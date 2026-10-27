#ifndef __M_MSG_H__
#define __M_MSG_H__
#include "common/mq_logger.hpp"
#include "common/mq_helper.hpp"
#include "mq_msg.pb.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <list>
#include <vector>
#include <algorithm>
#include <stdint.h>
#include <chrono>

namespace mq
{
#define DATAFILE_SUBFIX ".mqd"
#define TMPFILE_SUBFIX ".mqd.tmp"
    using MessagePtr = std::shared_ptr<mq::Message>;

    // 消息管理分为三层：
    // - MessageMapper：单个队列的 .mqd 文件读写、垃圾回收；
    // - QueueMessage：单个队列的内存状态（就绪 / wait-ack / 持久化索引）；
    // - MessageManager：按队列名找到 QueueMessage，并转发操作。
    //
    // 只有“队列 durable 且消息 delivery_mode=DURABLE”的消息才会落盘。
    // 非持久化消息只存在于 _msgs/_waitack_msgs 中，broker 重启后丢失。

    // 使用 steady_clock，避免系统时间调整影响 ACK 超时判断。
    inline uint64_t mqNowMs()
    {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }

    // 队列数据文件的读写封装。
    //
    // 文件格式（追加写）：
    //   [size_t 长度][protobuf Message::Payload 字节] [size_t 长度][...]
    //
    // ACK/丢弃时不删除中间的字节，而是把 Payload.valid 从 "1" 改成 "0"；
    // 因为 protobuf 的字段长度不变，可以原地覆盖，避免移动后续数据。
    // 当无效记录占比过高时，gc() 会把有效记录重写到 .tmp 文件再原子替换。
    class MessageMapper
    {
    public:
        MessageMapper(std::string &basedir, const std::string &qname) : _qname(qname)
        {
            if (basedir.back() != '/')
                basedir.push_back('/');
            _datafile = basedir + qname + DATAFILE_SUBFIX;
            _tmpfile = basedir + qname + TMPFILE_SUBFIX;
            if (FileHelper(basedir).exists() == false)
            {
                assert(FileHelper::createDirectory(basedir));
            }
            createMsgFile();
        }
        bool createMsgFile()
        {
            if (FileHelper(_datafile).exists() == true)
            {
                return true;
            }
            bool ret = FileHelper::createFile(_datafile);
            if (ret == false)
            {
                DLOG("创建队列数据文件 %s 失败！", _datafile.c_str());
                return false;
            }
            return true;
        }
        void removeMsgFile()
        {
            FileHelper::removeFile(_datafile);
            FileHelper::removeFile(_tmpfile);
        }
        bool insert(MessagePtr &msg)
        {
            return insert(_datafile, msg);
        }
        bool remove(MessagePtr &msg)
        {
            // 注意：不能直接序列化内存中的 msg。投递后 delivery_count 会变化，
            // 序列化长度可能和落盘时的长度不同。这里读取原始记录，只把 valid
            // 从 "1" 改成 "0"，字段长度不变，才能安全地原地覆盖。
            FileHelper helper(_datafile);
            std::string body(msg->length(), '\0');
            if (helper.read(&body[0], msg->offset(), msg->length()) == false)
            {
                DLOG("读取待确认消息的持久化数据失败！");
                return false;
            }
            mq::Message::Payload payload;
            if (payload.ParseFromString(body) == false)
            {
                DLOG("反序列化待确认消息的持久化数据失败！");
                return false;
            }
            payload.set_valid("0");
            std::string new_body = payload.SerializeAsString();
            if (new_body.size() != msg->length())
            {
                DLOG("不能修改文件中的数据信息，因为新生成的数据与原数据长度不一致!");
                return false;
            }
            bool ret = helper.write(new_body.c_str(), msg->offset(), new_body.size());
            if (ret == false)
            {
                DLOG("向队列数据文件写入数据失败！");
                return false;
            }
            msg->mutable_payload()->set_valid("0");
            return true;
        }
        std::list<MessagePtr> gc()
        {
            // 1. 加载出文件中所有的有效数据；  存储格式 4字节长度|数据|4字节长度|数据.....
            bool ret;
            std::list<MessagePtr> result;
            ret = load(result);
            if (ret == false)
            {
                DLOG("加载有效数据失败！\n");
                return result;
            }
            // DLOG("垃圾回收，得到有效消息数量：%d", result.size());
            // 2. 将有效数据，进行序列化存储到临时文件中
            FileHelper::createFile(_tmpfile);
            for (auto &msg : result)
            {
                DLOG("向临时文件写入数据: %s", msg->payload().body().c_str());
                ret = insert(_tmpfile, msg);
                if (ret == false)
                {
                    DLOG("向临时文件写入消息数据失败！！");
                    return result;
                }
            }
            // DLOG("垃圾回收后，向临时文件写入数据完毕，临时文件大小: %ld", FileHelper(_tmpfile).size());
            // 3. 删除源文件
            ret = FileHelper::removeFile(_datafile);
            if (ret == false)
            {
                DLOG("删除源文件失败！");
                return result;
            }
            // 4. 修改临时文件名，为源文件名称
            ret = FileHelper(_tmpfile).rename(_datafile);
            if (ret == false)
            {
                DLOG("修改临时文件名称失败！");
                return result;
            }
            // 5. 返回新的有效数据
            return result;
        }

    private:
        bool load(std::list<MessagePtr> &result)
        {
            // 加载出文件中所有的有效数据；  存储格式 4字节长度|数据|4字节长度|数据.....
            FileHelper data_file_helper(_datafile);
            size_t offset = 0, msg_size;
            size_t fsize = data_file_helper.size();
            // DLOG("准备开始加载持久化数据，当前文件大小: %ld", data_file_helper.size());
            bool ret;
            while (offset < fsize)
            {
                ret = data_file_helper.read((char *)&msg_size, offset, sizeof(size_t));
                if (ret == false)
                {
                    DLOG("读取消息长度失败！");
                    return false;
                }
                offset += sizeof(size_t);
                if (msg_size > fsize - offset)
                {
                    DLOG("消息长度 %zu 超出数据文件剩余大小 %zu，数据文件可能损坏！",
                         msg_size, fsize - offset);
                    return false;
                }
                std::string msg_body(msg_size, '\0');
                ret = data_file_helper.read(&msg_body[0], offset, msg_size);
                if (ret == false)
                {
                    DLOG("读取消息数据失败！");
                    return false;
                }
                offset += msg_size;
                MessagePtr msgp = std::make_shared<Message>();
                if (msgp->mutable_payload()->ParseFromString(msg_body) == false)
                {
                    DLOG("反序列化消息失败，数据文件可能损坏！");
                    return false;
                }
                // DLOG("加载到有效数据：%s", msgp->payload().body().c_str());
                // 如果是无效消息，则直接处理下一个
                if (msgp->payload().valid() == "0")
                {
                    DLOG("加载到无效消息：%s", msgp->payload().body().c_str());
                    continue;
                }
                // 有效消息则保存起来
                result.push_back(msgp);
            }
            return true;
        }
        bool insert(const std::string &filename, MessagePtr &msg)
        {
            // 新增数据都是添加在文件末尾的
            // 1. 进行消息的序列化，获取到格式化后的消息
            std::string body = msg->payload().SerializeAsString();
            // 2. 获取文件长度
            FileHelper helper(filename);
            size_t fsize = helper.size();
            size_t msg_size = body.size();
            // 写入逻辑：1. 先写入4/8字节数据长度， 2， 再写入指定长度数据
            bool ret = helper.write((char *)&msg_size, fsize, sizeof(size_t));
            if (ret == false)
            {
                DLOG("向队列数据文件写入数据长度失败！");
                return false;
            }
            // 3. 将数据写入文件的指定位置
            ret = helper.write(body.c_str(), fsize + sizeof(size_t), body.size());
            if (ret == false)
            {
                DLOG("向队列数据文件写入数据失败！");
                return false;
            }
            // 4. 更新msg中的实际存储信息
            msg->set_offset(fsize + sizeof(size_t));
            msg->set_length(body.size());
            return true;
        }

    private:
        std::string _qname;
        std::string _datafile;
        std::string _tmpfile;
    };

    // 单个队列的内存消息状态。
    //
    // 三个集合分别表示：
    // - _msgs：就绪消息，等待被 basicConsume 取走；
    // - _waitack_msgs：已投递给消费者、尚未 ACK/Reject 的消息；
    // - _durable_msgs：已落盘消息的 id -> Message 索引，用于更新 offset/length。
    //
    // 一条消息在任意时刻只应处于“就绪”或“wait-ack”之一。_mutex 保护
    // 这三个集合以及计数；所有业务判断（ACK、Reject、超时、淘汰）都在锁内
    // 完成，避免取消息和 ACK 并发时出现重复入队。
    class QueueMessage
    {
        // wait-ack 里除了消息本身，还要记住是哪个消费者取走的。
        // 消费者断线时，只能把它自己的消息重新入队，不能影响其他消费者。
        struct WaitAckEntry
        {
            WaitAckEntry(const MessagePtr &m, const std::string &tag, uint64_t seq,
                         uint64_t deadline)
                : msg(m), consumer_tag(tag), sequence(seq), deadline_ms(deadline) {}
            MessagePtr msg;
            std::string consumer_tag;
            uint64_t sequence;    // 保持重新入队时的原始投递顺序
            uint64_t deadline_ms; // 0 表示不启用 ACK 超时
        };

    public:
        using ptr = std::shared_ptr<QueueMessage>;

        // 拒绝/超时处理结果：found 表示消息确实在 wait-ack 中；
        // dead_letter=true 表示这条消息不应重新入队，而应交给死信交换机。
        struct RejectResult
        {
            RejectResult() : found(false), dead_letter(false) {}
            bool found;
            std::string consumer_tag;
            MessagePtr msg;
            bool dead_letter;
        };
        // 超时扫描时，一条 wait-ack 消息的处理结果。
        struct ExpiredEntry
        {
            ExpiredEntry(const std::string &tag, const MessagePtr &m, bool dead)
                : consumer_tag(tag), msg(m), dead_letter(dead) {}
            std::string consumer_tag;
            MessagePtr msg;
            bool dead_letter;
        };

        QueueMessage(std::string &basedir, const std::string &qname, size_t max_size = 0)
            : _qname(qname),
              _valid_count(0),
              _total_count(0),
              _max_size(max_size),
              _mapper(basedir, qname),
              _waitack_seq(0) {}

        // 声明队列时支持动态修改 x-max-length。
        // 调小上限时会立刻淘汰旧消息。
        void set_max_size(size_t v)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_max_size == v)
            {
                return;
            }
            _max_size = v;
            trim_locked();
        }
        size_t get_max_size()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _max_size;
        }
        // broker 启动/队列首次创建时从 .mqd 恢复消息。
        // 恢复出来的一律是“就绪”消息；上次进程退出时未 ACK 的消息没有
        // wait-ack owner，会被当作普通就绪消息重新投递。
        bool recovery()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _msgs = _mapper.gc();
            for (auto &msg : _msgs)
            {
                _durable_msgs.insert(std::make_pair(msg->payload().properties().id(), msg));
            }
            _valid_count = _total_count = _msgs.size();

            // 恢复后如果就绪消息超过上限，保留最新消息，丢弃最旧的。
            trim_locked();
            return true;
        }

        // 发布一条消息到队列。
        //
        // 消息是否落盘由“队列 durable + 消息 delivery_mode”共同决定：
        // - 队列不 durable：强制按 UNDURABLE 处理；
        // - 队列 durable 且消息 DURABLE：先写 .mqd，再进入内存队列；
        // - 非持久化消息只进内存。
        // 入队后统一执行 x-max-length 淘汰，超限时丢最旧的就绪消息。
        bool insert(const BasicProperties *bp, const std::string &body, bool queue_is_durable)
        {
            // 1. 构造消息对象
            MessagePtr msg = std::make_shared<Message>();
            msg->mutable_payload()->set_body(body);
            if (bp != nullptr)
            {
                DeliveryMode mode = queue_is_durable ? bp->delivery_mode() : DeliveryMode::UNDURABLE;
                // 消息 id 是 wait-ack / 持久化索引的 key，不能为空，否则多条消息会互相覆盖
                std::string msg_id = bp->id().empty() ? UUIDHelper::uuid() : bp->id();
                msg->mutable_payload()->mutable_properties()->set_id(msg_id);
                msg->mutable_payload()->mutable_properties()->set_delivery_mode(mode);
                msg->mutable_payload()->mutable_properties()->set_routing_key(bp->routing_key());
            }
            else
            {
                DeliveryMode mode = queue_is_durable ? DeliveryMode::DURABLE : DeliveryMode::UNDURABLE;
                msg->mutable_payload()->mutable_properties()->set_id(UUIDHelper::uuid());
                msg->mutable_payload()->mutable_properties()->set_delivery_mode(mode);
                msg->mutable_payload()->mutable_properties()->set_routing_key("");
            }
            // 新消息总是从 0 开始计数；重投次数由 broker 在每次投递时维护，
            // 不允许发布端伪造。
            msg->mutable_payload()->mutable_properties()->set_delivery_count(0);
            std::unique_lock<std::mutex> lock(_mutex);
            // 2. 判断消息是否需要持久化
            if (msg->payload().properties().delivery_mode() == DeliveryMode::DURABLE)
            {
                msg->mutable_payload()->set_valid("1"); // 在持久化存储中表示数据有效
                // 3. 进行持久化存储
                bool ret = _mapper.insert(msg);
                if (ret == false)
                {
                    DLOG("持久化存储消息：%s 失败了！", body.c_str());
                    return false;
                }
                _valid_count += 1; // 持久化信息中的数据量+1
                _total_count += 1;
                _durable_msgs.insert(std::make_pair(msg->payload().properties().id(), msg));
            }
            // 4. 内存的管理：先入队，再按 x-max-length 淘汰最旧消息
            _msgs.push_back(msg);
            trim_locked();
            return true;
        }
        // consumer_tag 用来记录 wait-ack 消息的归属，断线重入队时使用。
        // ack_timeout_ms=0 表示这条消息不启用 ACK 超时。
        MessagePtr front(const std::string &consumer_tag = "", uint64_t ack_timeout_ms = 0)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_msgs.size() == 0)
            {
                return MessagePtr();
            }
            // 获取一条队首消息：从_msgs中取出数据
            MessagePtr msg = _msgs.front();
            _msgs.pop_front();
            // 每次投递都累加 delivery_count，重投后仍保留在原消息里。
            BasicProperties *props = msg->mutable_payload()->mutable_properties();
            props->set_delivery_count(props->delivery_count() + 1);
            uint64_t deadline = 0;
            if (ack_timeout_ms > 0)
            {
                deadline = mqNowMs() + ack_timeout_ms;
            }
            // 将该消息对象，向待确认的hash表中添加一份，等到收到消息确认后进行删除
            _waitack_msgs.insert(std::make_pair(
                msg->payload().properties().id(),
                WaitAckEntry(msg, consumer_tag, _waitack_seq++, deadline)));
            return msg;
        }
        // 确认消息（basicAck）。
        //
        // 只接受仍在 wait-ack 中的消息：从 _waitack_msgs 删除、持久化消息
        // 标记为无效，并返回原 owner（consumer_tag）供上层减少 prefetch 计数。
        // 找不到消息或 owner 为空时返回空字符串，调用方据此判断 ACK 是否有效。
        std::string remove(const std::string &msg_id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            // 1. 从待确认队列中查找消息
            auto it = _waitack_msgs.find(msg_id);
            if (it == _waitack_msgs.end())
            {
                DLOG("没有找到要删除的消息：%s!", msg_id.c_str());
                return std::string();
            }
            MessagePtr msg = it->second.msg;
            std::string consumer_tag = it->second.consumer_tag;
            // 2. 根据消息的持久化模式，决定是否删除持久化信息
            discard_locked(msg);
            // 3. 删除内存中的信息
            _waitack_msgs.erase(msg_id);
            // DLOG("确认消息后，删除消息的管理成功：%s", msg->payload().body().c_str());
            return consumer_tag;
        }

        // 拒绝一条 wait-ack 消息。
        // requeue=true 且未超过最大投递次数时放回队首；
        // requeue=false 或超过最大投递次数时标记为 dead_letter，
        // 由 VirtualHost 决定路由到死信交换机还是直接丢弃。
        RejectResult reject(const std::string &msg_id, bool requeue,
                            uint32_t max_delivery_count)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            RejectResult result;
            auto it = _waitack_msgs.find(msg_id);
            if (it == _waitack_msgs.end())
            {
                return result;
            }
            result.found = true;
            result.consumer_tag = it->second.consumer_tag;
            result.msg = it->second.msg;
            _waitack_msgs.erase(it);

            if (requeue)
            {
                uint32_t delivery_count =
                    result.msg->payload().properties().delivery_count();
                if (max_delivery_count == 0 || delivery_count < max_delivery_count)
                {
                    _msgs.push_front(result.msg);
                    trim_locked();
                    return result;
                }
            }

            // 不再重投：持久化消息要从原队列中标记为无效。
            result.dead_letter = true;
            discard_locked(result.msg);
            return result;
        }

        // 扫描已超时的 wait-ack 消息。
        //
        // 所有 deadline_ms <= now_ms 的消息都会先移出 wait-ack：
        // - 未达到 x-max-delivery-count：按原投递顺序放回队首；
        // - 达到上限：标记为 dead_letter，并从持久化文件中删除。
        // 返回处理的条目；dead_letter=true 的消息需要 VirtualHost 路由到 DLX。
        size_t requeueExpired(uint64_t now_ms, uint32_t max_delivery_count,
                              std::vector<ExpiredEntry> &expired)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            std::vector<WaitAckEntry> candidates;
            for (auto it = _waitack_msgs.begin(); it != _waitack_msgs.end();)
            {
                if (it->second.deadline_ms != 0 && it->second.deadline_ms <= now_ms)
                {
                    candidates.push_back(it->second);
                    it = _waitack_msgs.erase(it);
                }
                else
                {
                    ++it;
                }
            }
            if (candidates.empty())
            {
                return 0;
            }

            // 与断线重入队一致，按原投递顺序放回队首。
            std::sort(candidates.begin(), candidates.end(),
                      [](const WaitAckEntry &a, const WaitAckEntry &b) {
                          return a.sequence < b.sequence;
                      });
            for (auto it = candidates.rbegin(); it != candidates.rend(); ++it)
            {
                uint32_t delivery_count =
                    it->msg->payload().properties().delivery_count();
                bool dead = (max_delivery_count != 0 &&
                             delivery_count >= max_delivery_count);
                if (dead)
                {
                    discard_locked(it->msg);
                }
                else
                {
                    _msgs.push_front(it->msg);
                }
                expired.push_back(ExpiredEntry(it->consumer_tag, it->msg, dead));
            }
            trim_locked();
            return candidates.size();
        }

        // 消费者断线/取消订阅时，把它取走但尚未 ACK 的消息放回队首。
        // 返回值是重新入队的消息数量。
        size_t requeueByConsumer(const std::string &consumer_tag)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            std::vector<WaitAckEntry> requeued;
            for (auto it = _waitack_msgs.begin(); it != _waitack_msgs.end();)
            {
                if (it->second.consumer_tag == consumer_tag)
                {
                    requeued.push_back(it->second);
                    it = _waitack_msgs.erase(it);
                }
                else
                {
                    ++it;
                }
            }

            // 按原投递顺序放回队首，保证 requeue 后不会把任务顺序打乱。
            std::sort(requeued.begin(), requeued.end(),
                      [](const WaitAckEntry &a, const WaitAckEntry &b) {
                          return a.sequence < b.sequence;
                      });
            for (auto it = requeued.rbegin(); it != requeued.rend(); ++it)
            {
                _msgs.push_front(it->msg);
            }
            // 重新入队后这些消息又变成“就绪消息”，仍要遵守 x-max-length。
            trim_locked();
            return requeued.size();
        }
        // 当前可投递（就绪）消息数，不包含 wait-ack 中的消息。
        size_t getable_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msgs.size();
        }
        size_t total_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _total_count;
        }
        size_t durable_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _durable_msgs.size();
        }
        size_t waitack_count()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _waitack_msgs.size();
        }
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _mapper.removeMsgFile();
            _msgs.clear();
            _durable_msgs.clear();
            _waitack_msgs.clear();
            _waitack_seq = 0;
            _valid_count = 0;
            _total_count = 0;
        }

    private:
        // 调用方必须持有 _mutex。
        // 消息完成 ACK、被拒绝且不重投、或因超过最大投递次数进入死信时调用。
        void discard_locked(MessagePtr msg)
        {
            if (msg->payload().properties().delivery_mode() != DeliveryMode::DURABLE)
            {
                return;
            }
            _mapper.remove(msg);
            _durable_msgs.erase(msg->payload().properties().id());
            if (_valid_count > 0)
            {
                _valid_count -= 1;
            }
            gc();
        }

        // 调用方必须持有 _mutex。
        // 上限只约束“待推送”消息；已经被消费者取走但未 ack 的消息不在这里淘汰。
        void trim_locked()
        {
            if (_max_size == 0)
            {
                return;
            }

            bool need_gc = false;
            while (_msgs.size() > _max_size)
            {
                MessagePtr discard_msg = _msgs.front();
                _msgs.pop_front();
                DLOG("队列[%s]达到最大长度 %zu，丢弃最旧就绪消息, msg_id:%s",
                     _qname.c_str(), _max_size, discard_msg->payload().properties().id().c_str());

                // 持久化消息需要标记为无效，否则重启后会被恢复回来。
                if (discard_msg->payload().properties().delivery_mode() == DeliveryMode::DURABLE)
                {
                    _mapper.remove(discard_msg);
                    _durable_msgs.erase(discard_msg->payload().properties().id());
                    if (_valid_count > 0)
                    {
                        _valid_count--;
                    }
                    need_gc = true;
                }
            }

            if (need_gc)
            {
                gc();
            }
        }

        bool GCCheck()
        {
            // 持久化的消息总量大于2000， 且其中有效比例低于50%则需要持久化
            if (_total_count > 2000 && _valid_count * 10 / _total_count < 5)
            {
                return true;
            }
            return false;
        }
        void gc()
        {
            // 1. 进行垃圾回收，获取到垃圾回收后，有效的消息信息链表
            if (GCCheck() == false)
                return;
            std::list<MessagePtr> msgs = _mapper.gc();
            for (auto &msg : msgs)
            {
                auto it = _durable_msgs.find(msg->payload().properties().id());
                if (it == _durable_msgs.end())
                {
                    DLOG("垃圾回收后，有一条持久化消息，在内存中没有进行管理!");
                    _msgs.push_back(msg); /// 做法：重新添加到推送链表的末尾
                    _durable_msgs.insert(std::make_pair(msg->payload().properties().id(), msg));
                    continue;
                }
                // 2. 更新每一条消息的实际存储位置
                it->second->set_offset(msg->offset());
                it->second->set_length(msg->length());
            }
            // 3. 更新当前的有效消息数量 & 总的持久化消息数量
            _valid_count = _total_count = msgs.size();
        }

    private:
        std::mutex _mutex;
        std::string _qname;
        size_t _valid_count;                                       // 有效消息数量
        size_t _total_count;                                       // 总的持久化消息数量
        size_t _max_size;                                          // 0 = 无上限
        MessageMapper _mapper;                                     // 持久化管理消息map
        std::list<MessagePtr> _msgs;                               // 待推送消息
        std::unordered_map<std::string, MessagePtr> _durable_msgs; // 持久化消息hash
        std::unordered_map<std::string, WaitAckEntry> _waitack_msgs; // 待确认消息hash
        uint64_t _waitack_seq;                                     // wait-ack 投递序号
    };

    // 队列消息注册表：负责按队列名找到 QueueMessage。
    //
    // _mutex 只保护 _queue_msgs 这个 map。每个方法先在锁内复制一份
    // QueueMessage::ptr，再释放锁调用队列自身的方法，避免全局锁和
    // QueueMessage::_mutex 嵌套，也避免删除队列时阻塞所有队列的读写。
    class MessageManager
    {
    public:
        using ptr = std::shared_ptr<MessageManager>;
        MessageManager(const std::string &basedir) : _basedir(basedir) {}
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (auto &qmsg : _queue_msgs)
            {
                qmsg.second->clear();
            }
        }
        // 队列声明时确保 QueueMessage 存在。
        //
        // 首次创建会立刻 recovery() 读取历史 .mqd；对已存在的队列，只有
        // apply_max_size=true（本次声明显式带了 x-max-length）才更新上限，
        // 避免重复声明时用空 args 把已有配置清掉。
        void initQueueMessage(const std::string &qname, size_t max_size = 0, bool apply_max_size = false)
        {
            QueueMessage::ptr qmp;
            bool created = false;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it != _queue_msgs.end())
                {
                    qmp = it->second;
                }
                else
                {
                    qmp = std::make_shared<QueueMessage>(_basedir, qname,
                                                         apply_max_size ? max_size : 0);
                    _queue_msgs.insert(std::make_pair(qname, qmp));
                    created = true;
                }
            }

            if (created)
            {
                qmp->recovery();
                return;
            }

            // 队列已存在：只有本次声明显式带了 x-max-length 才更新，
            // 避免其他客户端用空 args 重复声明时把已有上限清掉。
            if (apply_max_size)
            {
                qmp->set_max_size(max_size);
            }
        }

        // 删除队列：先从注册表摘除，再清理消息文件和内存状态。
        // 先摘除可以避免新消息继续进入一个正在销毁的队列。
        void destroyQueueMessage(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    return;
                }
                qmp = it->second;
                _queue_msgs.erase(it);
            }
            qmp->clear();
        }
        bool insert(const std::string &qname, BasicProperties *bp, const std::string &body, bool queue_is_durable)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("向队列%s新增消息失败：没有找到消息管理句柄!", qname.c_str());
                    return false;
                }
                qmp = it->second;
            }
            return qmp->insert(bp, body, queue_is_durable);
        }
        MessagePtr front(const std::string &qname, const std::string &consumer_tag = "",
                         uint64_t ack_timeout_ms = 0)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s队首消息失败：没有找到消息管理句柄!", qname.c_str());
                    return MessagePtr();
                }
                qmp = it->second;
            }
            return qmp->front(consumer_tag, ack_timeout_ms);
        }
        // 某个消费者断线后，把它未确认的消息重新放回队列。
        size_t requeueByConsumer(const std::string &qname, const std::string &consumer_tag)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("重新入队队列%s的消息失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->requeueByConsumer(consumer_tag);
        }
        std::string ack(const std::string &qname, const std::string &msg_id)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("确认队列%s消息%s失败：没有找到消息管理句柄!", qname.c_str(), msg_id.c_str());
                    return std::string();
                }
                qmp = it->second;
            }
            return qmp->remove(msg_id);
        }
        QueueMessage::RejectResult reject(const std::string &qname,
                                          const std::string &msg_id,
                                          bool requeue,
                                          uint32_t max_delivery_count)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("拒绝队列%s消息%s失败：没有找到消息管理句柄!", qname.c_str(), msg_id.c_str());
                    return QueueMessage::RejectResult();
                }
                qmp = it->second;
            }
            return qmp->reject(msg_id, requeue, max_delivery_count);
        }
        size_t requeueExpired(const std::string &qname, uint64_t now_ms,
                              uint32_t max_delivery_count,
                              std::vector<QueueMessage::ExpiredEntry> &expired)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->requeueExpired(now_ms, max_delivery_count, expired);
        }
        size_t getable_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s待推送消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->getable_count();
        }
        size_t total_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s总持久化消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->total_count();
        }
        size_t durable_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s有效持久化消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->durable_count();
        }
        size_t waitack_count(const std::string &qname)
        {
            QueueMessage::ptr qmp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _queue_msgs.find(qname);
                if (it == _queue_msgs.end())
                {
                    DLOG("获取队列%s待确认消息数量失败：没有找到消息管理句柄!", qname.c_str());
                    return 0;
                }
                qmp = it->second;
            }
            return qmp->waitack_count();
        }

    private:
        std::mutex _mutex;
        std::string _basedir;
        std::unordered_map<std::string, QueueMessage::ptr> _queue_msgs;
    };

}
#endif