#ifndef __M_CONSUMER_H__
#define __M_CONSUMER_H__
#include "common/mq_logger.hpp"
#include "common/mq_helper.hpp"
#include "mq_msg.pb.h"
#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <vector>
#include <functional>
#include <atomic>

namespace mq
{
    using ConsumerCallback = std::function<void(const std::string, const BasicProperties *bp, const std::string)>;

    // 一个 Consumer 对应一个 basicConsume 订阅。
    //
    // tag 在“同一队列”内唯一，是 wait-ack 消息的 owner，也是断线重投时
    // 精确筛选消息的依据；不同队列可以使用相同的 tag。
    //
    // 线程模型：
    // - create/remove/choose/QoS 调整会从 IO 线程或调度线程进入；
    // - callback 在 QueueDispatcher 的线程池中执行；
    // - ACK/Reject 又从 IO 线程回到调度器。
    // 因此 prefetch_count/unacked_count 必须是原子变量，不能依赖外部大锁。
    struct Consumer
    {
        using ptr = std::shared_ptr<Consumer>;
        std::string tag;   // 消费者标识（同一队列内唯一）
        std::string qname; // 消费者订阅的队列名称
        bool auto_ack;     // true：回调返回后由 broker 自动 ACK
        ConsumerCallback callback;
        // prefetch_count=0 表示不限制；unacked_count 是已经投递但还没 ACK 的数量。
        // 这两个值共同决定 hasCapacity()，也就是消费者还能不能再收消息。
        std::atomic<uint32_t> prefetch_count;
        std::atomic<size_t> unacked_count;

        Consumer() : prefetch_count(0), unacked_count(0)
        {
            DLOG("new Consumer: %p", this);
        }
        Consumer(const std::string &ctag, const std::string &queue_name, bool ack_flag,
                 const ConsumerCallback &cb, uint32_t prefetch = 0)
            : tag(ctag), qname(queue_name), auto_ack(ack_flag), callback(cb),
              prefetch_count(prefetch), unacked_count(0)
        {
            DLOG("new Consumer: %p", this);
        }
        ~Consumer()
        {
            DLOG("del Consumer: %p", this);
        }

        bool hasCapacity() const
        {
            uint32_t limit = prefetch_count.load();
            return limit == 0 || unacked_count.load() < limit;
        }
        void incUnacked()
        {
            unacked_count.fetch_add(1);
        }
        void decUnacked()
        {
            // 防止重复 ACK 或异常路径把计数减成负数。
            size_t old = unacked_count.load();
            while (old > 0 && !unacked_count.compare_exchange_weak(old, old - 1))
            {
            }
        }
        void setPrefetch(uint32_t prefetch)
        {
            prefetch_count.store(prefetch);
        }
    };

    // 以队列为单元的消费者管理结构。
    //
    // 每个队列维护一个消费者列表和一个轮转游标 _rr_seq。choose() 从上次
    // 的位置继续向后找，保证有多个消费者时消息大致均匀分配。
    // 锁只保护 _consumers 和 _rr_seq，不保护 Consumer 内部原子计数；
    // 因此持有本锁时不会阻塞正在等待 ACK 的消费者。
    class QueueConsumer
    {
    public:
        using ptr = std::shared_ptr<QueueConsumer>;
        QueueConsumer(const std::string &qname) : _qname(qname), _rr_seq(0) {}
        // 队列新增消费者；同队列内 tag 重复时返回空指针，由调用方判错。
        Consumer::ptr create(const std::string &ctag, const std::string &queue_name, bool ack_flag,
                             const ConsumerCallback &cb, uint32_t prefetch = 0)
        {
            // 1. 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 2. 判断消费者是否重复
            for (auto &consumer : _consumers)
            {
                if (consumer->tag == ctag)
                {
                    return Consumer::ptr();
                }
            }
            // 3. 没有重复则新增--构造对象
            auto consumer = std::make_shared<Consumer>(ctag, queue_name, ack_flag, cb, prefetch);
            // 4. 添加管理后返回对象
            _consumers.push_back(consumer);
            return consumer;
        }
        // 队列移除消费者。
        // 注意：这里只删除订阅关系，不处理该消费者的 wait-ack 消息。
        // 重新入队由 Channel 在 remove 之前调用 requeueByConsumer() 完成。
        void remove(const std::string &ctag)
        {
            // 1. 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 2. 遍历查找-删除
            for (auto it = _consumers.begin(); it != _consumers.end(); ++it)
            {
                if ((*it)->tag == ctag)
                {
                    _consumers.erase(it);
                    return;
                }
            }
            return;
        }
        // 队列获取消费者：RR 轮转获取，跳过已经达到 prefetch 上限的消费者。
        // 返回空指针表示“当前没有可投递对象”，可能是没有消费者，也可能是
        // 所有消费者都在等待 ACK；QueueDispatcher 会据此结束本轮 drain。
        Consumer::ptr choose()
        {
            // 1. 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            size_t count = _consumers.size();
            if (count == 0)
            {
                return Consumer::ptr();
            }
            // 2. 从上次轮转位置开始找第一个还有额度的消费者
            size_t start = static_cast<size_t>(_rr_seq % count);
            for (size_t i = 0; i < count; ++i)
            {
                size_t idx = (start + i) % count;
                if (_consumers[idx]->hasCapacity())
                {
                    // 下一次从该消费者之后开始，保持公平轮转。
                    _rr_seq += i + 1;
                    return _consumers[idx];
                }
            }
            // 所有消费者都在等待 ACK，暂时没有可投递对象。
            return Consumer::ptr();
        }
        // ACK 后释放一个在途名额。
        bool onAck(const std::string &ctag)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (auto &consumer : _consumers)
            {
                if (consumer->tag == ctag)
                {
                    consumer->decUnacked();
                    return true;
                }
            }
            return false;
        }
        // 消费者已存在时动态调整 QoS。
        bool setPrefetch(const std::string &ctag, uint32_t prefetch)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            for (auto &consumer : _consumers)
            {
                if (consumer->tag == ctag)
                {
                    consumer->setPrefetch(prefetch);
                    return true;
                }
            }
            return false;
        }
        // 是否为空
        bool empty()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            return _consumers.size() == 0;
        }
        // 判断指定消费者是否存在
        bool exists(const std::string &ctag)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            // 2. 遍历查找
            for (auto it = _consumers.begin(); it != _consumers.end(); ++it)
            {
                if ((*it)->tag == ctag)
                {
                    return true;
                }
            }
            return false;
        }
        // 清理所有消费者
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _consumers.clear();
            _rr_seq = 0;
        }

    private:
        std::string _qname;
        std::mutex _mutex;
        uint64_t _rr_seq; // 轮转序号
        std::vector<Consumer::ptr> _consumers;
    };

    // 全局消费者注册表：queue_name -> QueueConsumer。
    //
    // 锁顺序固定为 ConsumerManager::_mutex -> QueueConsumer::_mutex。
    // 各方法先在 Manager 锁内取出 QueueConsumer 的 shared_ptr，再释放 Manager
    // 锁并调用 QueueConsumer 的方法，因此不会长期持有全局锁，也不会反向加锁。
    class ConsumerManager
    {
    public:
        using ptr = std::shared_ptr<ConsumerManager>;
        ConsumerManager() {}
        // 队列声明时创建它的消费者管理单元；重复调用是幂等的。
        void initQueueConsumer(const std::string &qname)
        {
            // 1. 加锁
            std::unique_lock<std::mutex> lock(_mutex);
            // 2. 重复判断
            auto it = _qconsumers.find(qname);
            if (it != _qconsumers.end())
            {
                return;
            }
            // 3. 新增
            auto qconsumers = std::make_shared<QueueConsumer>(qname);
            _qconsumers.insert(std::make_pair(qname, qconsumers));
        }
        // 队列删除时销毁它的消费者管理单元。
        // 队列删除流程会先让 Channel 取消消费者并把 wait-ack 消息恢复/清理，
        // 因此这里只负责移除注册表项。
        void destroyQueueConsumer(const std::string &qname)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _qconsumers.erase(qname);
        }
        // 在指定队列上创建消费者；队列尚未初始化时返回空指针。
        Consumer::ptr create(const std::string &ctag, const std::string &queue_name, bool ack_flag,
                             const ConsumerCallback &cb, uint32_t prefetch = 0)
        {
            // 获取队列的消费者管理单元句柄，通过句柄完成新建
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                    return Consumer::ptr();
                }
                qcp = it->second;
            }
            return qcp->create(ctag, queue_name, ack_flag, cb, prefetch);
        }
        // 消息被 ACK/拒绝/超时后，归还该消费者的一个在途名额。
        bool onAck(const std::string &queue_name, const std::string &ctag)
        {
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    return false;
                }
                qcp = it->second;
            }
            return qcp->onAck(ctag);
        }
        // basicQos 可能发生在 basicConsume 之后，需要动态更新已有消费者。
        bool setPrefetch(const std::string &queue_name, const std::string &ctag, uint32_t prefetch)
        {
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    return false;
                }
                qcp = it->second;
            }
            return qcp->setPrefetch(ctag, prefetch);
        }
        void remove(const std::string &ctag, const std::string &queue_name)
        {
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                    return;
                }
                qcp = it->second;
            }
            return qcp->remove(ctag);
        }
        Consumer::ptr choose(const std::string &queue_name)
        {
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                    return Consumer::ptr();
                }
                qcp = it->second;
            }
            return qcp->choose();
        }
        bool empty(const std::string &queue_name)
        {
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                    return false;
                }
                qcp = it->second;
            }
            return qcp->empty();
        }
        bool exists(const std::string &ctag, const std::string &queue_name)
        {
            QueueConsumer::ptr qcp;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _qconsumers.find(queue_name);
                if (it == _qconsumers.end())
                {
                    DLOG("没有找到队列 %s 的消费者管理句柄！", queue_name.c_str());
                    return false;
                }
                qcp = it->second;
            }
            return qcp->exists(ctag);
        }
        void clear()
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _qconsumers.clear();
        }

    private:
        std::mutex _mutex;
        std::unordered_map<std::string, QueueConsumer::ptr> _qconsumers;
    };
}

#endif