#ifndef MQ_BRIDGE_RETAINED_STORE_HPP
#define MQ_BRIDGE_RETAINED_STORE_HPP

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <string>

namespace mq
{
namespace bridge
{

// bridge 自己的“最近状态”缓存。
//
// 设计要点：
// - 每个 topic 用一个 deque 保存最近若干条 BridgeEnvelope 字节；
//   put() 从尾部追加，超过容量从头部淘汰；
// - get() 只返回最新一条，并在读取时顺便清理过期条目；
// - ttl_ms==0 表示不按时间过期；
// - 所有访问 _entries 的方法都由同一把 mutex 保护，因此可以跨线程调用；
//   但 put() 持锁期间会写盘，磁盘慢时会阻塞其他读写；
// - retained store 不参与 broker 队列的 ACK/重投语义，只是 bridge 的辅助
//   状态，broker 重启或 bridge 崩溃不会影响队列内的消息；
// - bridge 启动时会回放最新状态；TRANSIENT_LOCAL publisher 自身还会为晚加入
//   的同类订阅者保留 DDS 历史，两者不是同一个机制。
class RetainedStore
{
public:
    // 保存一条完整的 BridgeEnvelope 字节。updated_ms 只用于 TTL 判断，
    // 不参与 envelope 本身的语义。
    struct Entry
    {
        std::string envelope;
        uint64_t updated_ms;
    };

    // capacity_per_topic 为 0 时会规范化为 1；persist_path 为空表示只存内存。
    RetainedStore(size_t capacity_per_topic,
                  uint64_t ttl_ms,
                  const std::string &persist_path = std::string());
    ~RetainedStore();

    RetainedStore(const RetainedStore &) = delete;
    RetainedStore &operator=(const RetainedStore &) = delete;

    // 写入一条最新状态；超过容量时淘汰最旧条目。
    // 如果配置了 persist_path，会立即尝试原子写盘。注意：写盘失败时内存中的
    // 新值已经生效，函数返回 false 只是告诉调用者持久化失败，调用者可以选择
    // 记录日志后继续运行。
    bool put(const std::string &topic,
             const std::string &encoded_envelope,
             uint64_t now_ms,
             std::string *error);

    // 读取最新一条。以下情况返回 false 且 error 为空：
    // - topic 不存在；
    // - 所有条目都已过期（读取过程中会删除空 topic）。
    // 返回 true 时，如果 updated_ms 非空，会写回该条的更新时间。
    bool get(const std::string &topic,
             std::string *encoded_envelope,
             uint64_t *updated_ms,
             uint64_t now_ms,
             std::string *error);

    bool erase(const std::string &topic);
    size_t size() const;
    void clear();

    // 从 persist_path 恢复状态。文件不存在视为首次启动并返回成功；
    // 文件存在但无法解析/读取时返回 false。load() 会替换当前内存状态，
    // 并跳过已过期、字段为空或超过容量的记录。
    bool load(std::string *error);

    // 把当前所有 topic 写回 persist_path；空路径直接返回成功。
    // 实现方式见 saveLocked()：先写临时文件，再 rename 覆盖目标文件。
    bool save(std::string *error) const;

    size_t capacityPerTopic() const { return _capacity_per_topic; }
    uint64_t ttlMs() const { return _ttl_ms; }
    const std::string &persistPath() const { return _persist_path; }

private:
    // 调用者必须已经持有 _mutex。
    bool saveLocked(std::string *error) const;

    size_t _capacity_per_topic;
    uint64_t _ttl_ms;
    std::string _persist_path;

    // 保护 _entries；mutable 让 const 的 size()/save() 也能加锁。
    mutable std::mutex _mutex;
    // topic -> 按写入顺序排列的条目；每个 deque 的长度不超过容量。
    std::map<std::string, std::deque<Entry> > _entries;
};

} // namespace bridge
} // namespace mq

#endif
