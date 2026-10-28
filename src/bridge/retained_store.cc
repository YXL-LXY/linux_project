#include "bridge/retained_store.hpp"

#include "bridge_envelope.pb.h"

#include <chrono>
#include <cstdio>
#include <fstream>
#include <sstream>

namespace mq
{
namespace bridge
{

namespace
{

void setError(std::string *error, const std::string &message)
{
    if (error != nullptr)
    {
        *error = message;
    }
}

// 与持久化文件中的 updated_ms 使用同一种时间基准（Unix epoch 毫秒）。
// 使用系统时钟意味着人工改时间/时钟回拨会影响 TTL 判断；MVP 先接受这一点。
uint64_t nowMs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

bool isExpired(const RetainedStore::Entry &entry, uint64_t ttl_ms, uint64_t now_ms)
{
    // ttl_ms==0 表示关闭过期；now_ms < updated_ms 时说明时钟回拨，
    // 这时不把条目判为过期，避免刚写入的数据立刻消失。
    if (ttl_ms == 0)
    {
        return false;
    }
    return now_ms >= entry.updated_ms && now_ms - entry.updated_ms >= ttl_ms;
}

} // namespace

RetainedStore::RetainedStore(size_t capacity_per_topic,
                             uint64_t ttl_ms,
                             const std::string &persist_path)
    : _capacity_per_topic(capacity_per_topic == 0 ? 1 : capacity_per_topic),
      _ttl_ms(ttl_ms),
      _persist_path(persist_path)
{
    // 容量 0 会让 put() 的淘汰循环永远成立，因此在入口处规范化为 1。
}

RetainedStore::~RetainedStore()
{
}

bool RetainedStore::put(const std::string &topic,
                        const std::string &encoded_envelope,
                        uint64_t now_ms,
                        std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (topic.empty())
    {
        setError(error, "retained topic must not be empty");
        return false;
    }
    if (encoded_envelope.empty())
    {
        setError(error, "retained envelope must not be empty");
        return false;
    }

    // 追加到尾部，容量超限才淘汰最旧条目。注意这里不按 updated_ms 排序：
    // 调用者应保证 now_ms 单调递增；即使不递增，get() 仍只返回最后写入的一条。
    std::lock_guard<std::mutex> lock(_mutex);
    std::deque<Entry> &queue = _entries[topic];
    queue.push_back(Entry{encoded_envelope, now_ms});
    while (queue.size() > _capacity_per_topic)
    {
        queue.pop_front();
    }

    // 每次 put 都全量写盘，简单但 O(topic 数 * 容量)；MVP 数据量很小，
    // 后续如果 retained 很多，应改成批量/增量持久化。
    if (!_persist_path.empty())
    {
        return saveLocked(error);
    }
    return true;
}

bool RetainedStore::get(const std::string &topic,
                        std::string *encoded_envelope,
                        uint64_t *updated_ms,
                        uint64_t now_ms,
                        std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (encoded_envelope == nullptr)
    {
        setError(error, "retained output must not be null");
        return false;
    }

    std::lock_guard<std::mutex> lock(_mutex);
    std::map<std::string, std::deque<Entry> >::iterator it = _entries.find(topic);
    if (it == _entries.end())
    {
        return false;
    }

    // get() 会顺手做过期清理，因此它不是纯读操作；这也解释了为什么它需要
    // 非 const 的 mutex 和 _entries。
    std::deque<Entry> &queue = it->second;
    while (!queue.empty() && isExpired(queue.front(), _ttl_ms, now_ms))
    {
        queue.pop_front();
    }
    if (queue.empty())
    {
        _entries.erase(it);
        return false;
    }

    // 正常情况下 deque 按写入顺序排列，清理完头部后 latest 必然最新；这里仍
    // 单独检查 latest，作为防御性判断，避免未来改变存储顺序后漏掉过期条目。
    const Entry &latest = queue.back();
    if (isExpired(latest, _ttl_ms, now_ms))
    {
        _entries.erase(it);
        return false;
    }

    *encoded_envelope = latest.envelope;
    if (updated_ms != nullptr)
    {
        *updated_ms = latest.updated_ms;
    }
    return true;
}

bool RetainedStore::erase(const std::string &topic)
{
    std::lock_guard<std::mutex> lock(_mutex);
    return _entries.erase(topic) > 0;
}

size_t RetainedStore::size() const
{
    std::lock_guard<std::mutex> lock(_mutex);
    size_t total = 0;
    for (const auto &entry : _entries)
    {
        total += entry.second.size();
    }
    return total;
}

void RetainedStore::clear()
{
    std::lock_guard<std::mutex> lock(_mutex);
    _entries.clear();
}

bool RetainedStore::save(std::string *error) const
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (_persist_path.empty())
    {
        return true;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    return saveLocked(error);
}

bool RetainedStore::saveLocked(std::string *error) const
{
    // 先把所有 topic 的顺序展开成 protobuf 记录。文件格式只由 bridge 使用，
    // broker 不读取它。
    RetainedState state;
    for (const auto &topic_entry : _entries)
    {
        for (const auto &entry : topic_entry.second)
        {
            RetainedRecord *record = state.add_records();
            record->set_topic(topic_entry.first);
            record->set_envelope(entry.envelope);
            record->set_updated_ms(entry.updated_ms);
        }
    }

    std::string bytes;
    if (!state.SerializeToString(&bytes))
    {
        setError(error, "failed to serialize retained state");
        return false;
    }

    // 写临时文件再 rename，保证目标文件不会出现“写了一半”的状态。
    // 注意：这里只 flush，没有 fsync；机器掉电时可能丢失最后一次写入，
    // 但不会把旧文件破坏成半截。MVP 接受这个边界。
    const std::string temp_path = _persist_path + ".tmp";
    {
        std::ofstream output(temp_path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        if (!output.is_open())
        {
            setError(error, "failed to open retained state file: " + temp_path);
            return false;
        }
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        output.flush();
        if (!output.good())
        {
            setError(error, "failed to write retained state file: " + temp_path);
            return false;
        }
    }

    // rename 在同一目录内是原子替换；失败时清掉临时文件，避免留下垃圾。
    if (std::rename(temp_path.c_str(), _persist_path.c_str()) != 0)
    {
        setError(error, "failed to replace retained state file: " + _persist_path);
        std::remove(temp_path.c_str());
        return false;
    }
    return true;
}

bool RetainedStore::load(std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (_persist_path.empty())
    {
        return true;
    }

    std::ifstream input(_persist_path.c_str(), std::ios::in | std::ios::binary);
    if (!input.is_open())
    {
        // 首次启动时文件不存在是正常情况。
        return true;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (!input.good() && !input.eof())
    {
        setError(error, "failed to read retained state file: " + _persist_path);
        return false;
    }

    RetainedState state;
    if (!state.ParseFromString(buffer.str()))
    {
        setError(error, "failed to parse retained state file: " + _persist_path);
        return false;
    }

    // 文件是 bridge 自己的状态快照；加载时用当前时间做一次 TTL 清理，
    // 并丢弃字段不完整（topic/envelope 为空）的记录。
    const uint64_t now = nowMs();
    std::lock_guard<std::mutex> lock(_mutex);
    _entries.clear();
    for (int i = 0; i < state.records_size(); ++i)
    {
        const RetainedRecord &record = state.records(i);
        if (record.topic().empty() || record.envelope().empty())
        {
            continue;
        }
        Entry entry{record.envelope(), record.updated_ms()};
        if (isExpired(entry, _ttl_ms, now))
        {
            continue;
        }
        // 文件按 topic 分组、按写入顺序保存；这里仍按容量截断，兼容手工修改
        // 过或旧版本写出的文件。
        std::deque<Entry> &queue = _entries[record.topic()];
        queue.push_back(entry);
        while (queue.size() > _capacity_per_topic)
        {
            queue.pop_front();
        }
    }
    return true;
}

} // namespace bridge
} // namespace mq
