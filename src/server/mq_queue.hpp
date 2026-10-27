#ifndef __M_QUEUE_H__
#define __M_QUEUE_H__
#include "common/mq_logger.hpp"
#include "common/mq_helper.hpp"
#include "mq_msg.pb.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <cstdlib>
#include <cerrno>

namespace mq
{
    // 队列元数据。durable=true 的队列会把元数据写入 SQLite，
    // 并在启动时恢复；exclusive/auto_delete 目前只作为字段保存，
    // 尚未实现独占连接和自动删除语义。
    struct MsgQueue
    {
        using ptr = std::shared_ptr<MsgQueue>;
        std::string name;
        bool durable;
        bool exclusive; // 是否独占（字段已保存，行为尚未实现）
        bool auto_delete; // 是否自动删除（字段已保存，行为尚未实现）
        google::protobuf::Map<std::string, std::string> args;

        MsgQueue() {}
        MsgQueue(const std::string &qname,
                 bool qdurable,
                 bool qexclusive,
                 bool qauto_delete,
                 const google::protobuf::Map<std::string, std::string> &qargs) : 
                                                                              name(qname), durable(qdurable), exclusive(qexclusive),
                                                                              auto_delete(qauto_delete), args(qargs)
        {
        }

        // 数据库中的 args 以 "k1=v1&k2=v2&" 的字符串保存。
        // 注意：当前实现不做 URL 编码，参数值中不能包含 '&'。
        void setArgs(const std::string &str_args)
        {
            std::vector<std::string> sub_args;
            StrHelper::split(str_args, "&", sub_args);
            for (auto &str : sub_args)
            {
                size_t pos = str.find("=");
                if (pos == std::string::npos)
                {
                    continue;
                }
                std::string key = str.substr(0, pos);
                std::string val = str.substr(pos + 1);
                args[key] = val;
            }
        }
        std::string getArgs()
        {
            std::string result;
            for (auto start = args.begin(); start != args.end(); ++start)
            {
                result += start->first + "=" + start->second + "&";
            }
            return result;
        }
    };

    // 解析队列参数中的 x-max-length。
    // 返回 true 表示参数存在且合法，max_length 为有效值；
    // 返回 false 表示未设置或值非法，调用方应保持原有/默认的无上限行为。
    inline bool parseQueueMaxLength(const google::protobuf::Map<std::string, std::string> &args,
                                    size_t &max_length)
    {
        auto it = args.find("x-max-length");
        if (it == args.end())
        {
            return false;
        }
        const std::string &text = it->second;
        if (text.empty())
        {
            DLOG("队列参数 x-max-length 为空，按无上限处理");
            return false;
        }

        errno = 0;
        char *end = nullptr;
        const char *begin = text.c_str();
        while (*begin == ' ' || *begin == '\t')
        {
            ++begin;
        }
        // strtoull 会接受 "-1" 并回绕成很大的正数，这里显式拒绝负号。
        if (*begin == '-')
        {
            DLOG("队列参数 x-max-length=%s 非法，按无上限处理", text.c_str());
            return false;
        }
        unsigned long long value = std::strtoull(text.c_str(), &end, 10);
        if (errno != 0 || end == text.c_str() || *end != '\0' || value == 0)
        {
            DLOG("队列参数 x-max-length=%s 非法或为 0，按无上限处理", text.c_str());
            return false;
        }
        max_length = static_cast<size_t>(value);
        return true;
    }

    // 未显式配置时，服务端每秒扫描一次 wait-ack，超过该时间仍未确认就重新入队。
    const uint64_t MQ_DEFAULT_ACK_TIMEOUT_MS = 30000;

    // 读取无符号整数队列参数。allow_zero=false 时 0 也视为非法。
    inline bool parseQueueUnsignedArg(const google::protobuf::Map<std::string, std::string> &args,
                                      const std::string &key,
                                      uint64_t &value,
                                      bool allow_zero)
    {
        auto it = args.find(key);
        if (it == args.end())
        {
            return false;
        }
        const std::string &text = it->second;
        if (text.empty())
        {
            DLOG("队列参数 %s 为空，按未设置处理", key.c_str());
            return false;
        }

        const char *begin = text.c_str();
        while (*begin == ' ' || *begin == '\t')
        {
            ++begin;
        }
        // 显式拒绝负号，避免 strtoull 回绕成很大的正数。
        if (*begin == '-')
        {
            DLOG("队列参数 %s=%s 非法，按未设置处理", key.c_str(), text.c_str());
            return false;
        }

        errno = 0;
        char *end = nullptr;
        unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
        if (errno != 0 || end == text.c_str() || *end != '\0' ||
            (!allow_zero && parsed == 0))
        {
            DLOG("队列参数 %s=%s 非法，按未设置处理", key.c_str(), text.c_str());
            return false;
        }
        value = static_cast<uint64_t>(parsed);
        return true;
    }

    // x-ack-timeout-ms：0 表示关闭 ACK 超时；未设置时调用方使用默认值。
    inline bool parseQueueAckTimeout(const google::protobuf::Map<std::string, std::string> &args,
                                     uint64_t &timeout_ms)
    {
        return parseQueueUnsignedArg(args, "x-ack-timeout-ms", timeout_ms, true);
    }

    // x-max-delivery-count：消息最多投递多少次；0/未设置表示不限制。
    inline bool parseQueueMaxDeliveryCount(const google::protobuf::Map<std::string, std::string> &args,
                                           uint32_t &count)
    {
        uint64_t value = 0;
        if (parseQueueUnsignedArg(args, "x-max-delivery-count", value, false) == false)
        {
            return false;
        }
        count = static_cast<uint32_t>(value);
        return true;
    }

    inline std::string queueArgValue(const google::protobuf::Map<std::string, std::string> &args,
                                     const std::string &key)
    {
        auto it = args.find(key);
        if (it == args.end())
        {
            return std::string();
        }
        return it->second;
    }

    using QueueMap = std::unordered_map<std::string, MsgQueue::ptr>;

    // 队列元数据的 SQLite 持久化。
    // queue_table 只保存 durable 队列；非持久化队列仅存在于内存。
    class MsgQueueMapper
    {
    public:
        MsgQueueMapper(const std::string &dbfile) : _sql_helper(dbfile)
        {
            std::string path = FileHelper::parentDirectory(dbfile);
            FileHelper::createDirectory(path);
            _sql_helper.open();
            createTable();
        }
        void createTable()
        {
            std::stringstream sql;
            sql << "create table if not exists queue_table(";
            sql << "name varchar(32) primary key, ";
            sql << "durable int, ";
            sql << "exclusive int, ";
            sql << "auto_delete int, ";
            sql << "args varchar(128));";
            assert(_sql_helper.exec(sql.str(), nullptr, nullptr));
        }
        void removeTable()
        {
            std::string sql = "drop table if exists queue_table;";
            _sql_helper.exec(sql, nullptr, nullptr);
        }
        bool insert(MsgQueue::ptr &queue)
        {
            // insert into queue_table values('queue1', true, false, false, "k1=v1&k2=v2&");
            std::stringstream sql;
            sql << "insert into queue_table values(";
            sql << "'" << queue->name << "', ";
            sql << queue->durable << ", ";
            sql << queue->exclusive << ", ";
            sql << queue->auto_delete << ", ";
            sql << "'" << queue->getArgs() << "');";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        void remove(const std::string &name)
        {
            // delete from queue_table where name='queue1';
            std::stringstream sql;
            sql << "delete from queue_table where name=";
            sql << "'" << name << "';";
            _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        bool updateArgs(MsgQueue::ptr &queue)
        {
            std::stringstream sql;
            sql << "update queue_table set args='";
            sql << queue->getArgs();
            sql << "' where name='" << queue->name << "';";
            return _sql_helper.exec(sql.str(), nullptr, nullptr);
        }
        QueueMap recovery()
        {
            QueueMap result;
            std::string sql = "select name, durable, exclusive, auto_delete, args from queue_table;";
            _sql_helper.exec(sql, selectCallback, &result);
            return result;
        }

    private:
        static int selectCallback(void *arg, int numcol, char **row, char **fields)
        {
            QueueMap *result = (QueueMap *)arg;
            MsgQueue::ptr mqp = std::make_shared<MsgQueue>();
            mqp->name = row[0];
            mqp->durable = (bool)std::stoi(row[1]);
            mqp->exclusive = (bool)std::stoi(row[2]);
            mqp->auto_delete = (bool)std::stoi(row[3]);
            if (row[4])
                mqp->setArgs(row[4]);
            result->insert(std::make_pair(mqp->name, mqp));
            return 0;
        }

    private:
        SqliteHelper _sql_helper;
    };

    // 队列元数据管理器。
    //
    // declareQueue 是幂等的：队列已存在时只更新本次声明显式携带的合法参数，
    // 不会清空消息，也不会用空 args 覆盖已有配置。只有 durable 队列的
    // 参数变化才写回 SQLite。
    class MsgQueueManager
    {
    public:
        using ptr = std::shared_ptr<MsgQueueManager>;
        MsgQueueManager(const std::string &dbfile) : _mapper(dbfile)
        {
            _msg_queues = _mapper.recovery();
        }
        bool declareQueue(const std::string &qname,
                          bool qdurable,
                          bool qexclusive,
                          bool qauto_delete,
                          const google::protobuf::Map<std::string, std::string> &qargs) 
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _msg_queues.find(qname);
            if (it != _msg_queues.end())
            {
                // 已存在的队列只更新本次声明显式携带的合法参数，
                // 避免空 args 的重复声明把已有上限/超时/死信配置清掉。
                bool changed = false;
                auto set_arg = [&](const std::string &key, const std::string &value) {
                    auto old = it->second->args.find(key);
                    if (old == it->second->args.end() || old->second != value)
                    {
                        it->second->args[key] = value;
                        changed = true;
                    }
                };

                size_t max_length = 0;
                if (parseQueueMaxLength(qargs, max_length))
                {
                    set_arg("x-max-length", std::to_string(max_length));
                }

                uint64_t ack_timeout_ms = 0;
                if (parseQueueAckTimeout(qargs, ack_timeout_ms))
                {
                    set_arg("x-ack-timeout-ms", std::to_string(ack_timeout_ms));
                }

                uint32_t max_delivery_count = 0;
                if (parseQueueMaxDeliveryCount(qargs, max_delivery_count))
                {
                    set_arg("x-max-delivery-count", std::to_string(max_delivery_count));
                }

                // 死信交换机/路由键是字符串参数，显式传入空值可以关闭死信。
                auto dlx = qargs.find("x-dead-letter-exchange");
                if (dlx != qargs.end())
                {
                    set_arg("x-dead-letter-exchange", dlx->second);
                }
                auto dlrk = qargs.find("x-dead-letter-routing-key");
                if (dlrk != qargs.end())
                {
                    set_arg("x-dead-letter-routing-key", dlrk->second);
                }

                if (changed && it->second->durable == true)
                {
                    _mapper.updateArgs(it->second);
                }
                return true;
            }
            MsgQueue::ptr mqp = std::make_shared<MsgQueue>();
            mqp->name = qname;
            mqp->durable = qdurable;
            mqp->exclusive = qexclusive;
            mqp->auto_delete = qauto_delete;
            mqp->args = qargs;
            if (qdurable == true)
            {
                bool ret = _mapper.insert(mqp);
                if (ret == false)
                    return false;
            }
            _msg_queues.insert(std::make_pair(qname, mqp));
            return true;
        }
        void deleteQueue(const std::string &name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _msg_queues.find(name);
            if (it == _msg_queues.end())
            {
                return;
            }
            if (it->second->durable == true)
                _mapper.remove(name);
            _msg_queues.erase(name);
        }
        // 返回 shared_ptr，调用方在锁外仍可安全读取队列元数据；
        // 删除队列只会把 map 中的条目摘掉，不会让正在使用的对象失效。
        MsgQueue::ptr selectQueue(const std::string &name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _msg_queues.find(name);
            if (it == _msg_queues.end())
            {
                return MsgQueue::ptr();
            }
            return it->second;
        }
        // 返回一份队列表快照，供 ACK 超时扫描等需要遍历的场景使用。
        QueueMap allQueues()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msg_queues;
        }
        bool exists(const std::string &name)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _msg_queues.find(name);
            if (it == _msg_queues.end())
            {
                return false;
            }
            return true;
        }
        size_t size()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _msg_queues.size();
        }
        void clear()
        {
            _mapper.removeTable();
            _msg_queues.clear();
        }

    private:
        std::mutex _mutex;
        MsgQueueMapper _mapper;
        QueueMap _msg_queues;
    };
}
#endif