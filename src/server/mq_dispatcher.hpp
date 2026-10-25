#ifndef __M_DISPATCHER_H__
#define __M_DISPATCHER_H__
#include "mq_host.hpp"
#include "mq_consumer.hpp"
#include "common/mq_threadpool.hpp"
#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace mq
{
    // 队列调度器：负责把队列里的消息按轮转方式投递给消费者。
    //
    // 设计要点：
    // 1. 每个队列同一时刻最多只有一个 drain 任务，避免多个发布者/订阅者
    //    各自启动任务后互相抢消息。
    // 2. notify() 只负责“唤醒”；如果队列已经在 drain，就只记录 pending，
    //    由当前任务处理完当前批次后重新投递任务，避免丢唤醒。
    // 3. 必须通过 std::make_shared<QueueDispatcher>() 创建，notify() 内部
    //    使用 shared_from_this() 延长任务执行期间的生命周期。
    class QueueDispatcher : public std::enable_shared_from_this<QueueDispatcher>
    {
    public:
        using ptr = std::shared_ptr<QueueDispatcher>;

        QueueDispatcher(const VirtualHost::ptr &host,
                        const ConsumerManager::ptr &cmp,
                        const threadpool::ptr &pool)
            : _host(host), _cmp(cmp), _pool(pool) {}

        // 发布消息、订阅消息、消息确认、消费者断线后都可以调用。
        // 同一个队列重复调用会被合并，不会创建大量重复任务。
        void notify(const std::string &qname)
        {
            threadpool::ptr pool = _pool.lock();
            if (!pool || pool->stopped())
            {
                return;
            }

            // 没有消费者时不需要调度。等 basicConsume 成功后会再次 notify。
            if (_cmp->empty(qname))
            {
                return;
            }

            {
                std::unique_lock<std::mutex> lock(_mutex);
                QueueState &state = _states[qname];
                state.pending = true;
                if (state.running)
                {
                    return;
                }
                state.running = true;
            }

            // 任务持有 self，保证 drain 期间 host/cmp 不会被提前释放。
            // _pool 只保存 weak_ptr，避免 dispatcher 与线程池形成循环引用。
            auto self = shared_from_this();
            pool->push([self, qname]() { self->drainLoop(qname); });
        }

        // 由 Server 的定时器每秒调用一次：把超时未 ACK 的消息重新入队，
        // 超过最大投递次数的消息转死信，并唤醒相关队列。
        void checkAckTimeout()
        {
            checkAckTimeout(mqNowMs());
        }
        void checkAckTimeout(uint64_t now_ms)
        {
            std::vector<VirtualHost::TimeoutResult> results =
                _host->requeueExpired(now_ms);
            for (auto &result : results)
            {
                // 超时的消息不再算在原消费者的在途数量里。
                for (auto &tag : result.consumer_tags)
                {
                    _cmp->onAck(result.queue_name, tag);
                }
                notify(result.queue_name);
                for (auto &target : result.notify_queues)
                {
                    notify(target);
                }
            }
        }

    private:
        struct QueueState
        {
            QueueState() : running(false), pending(false) {}
            bool running;
            bool pending;
        };

        // 每个任务只处理一个批次。如果队列还有消息，就重新投递一个任务，
        // 让线程池里排队的其他任务有机会先执行，避免单个大队列长期占用工作线程。
        void drainLoop(const std::string &qname)
        {
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _states.find(qname);
                if (it == _states.end())
                {
                    return;
                }
                if (!it->second.pending)
                {
                    it->second.running = false;
                    return;
                }
                it->second.pending = false;
            }

            drainBatch(qname);

            bool again = false;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                auto it = _states.find(qname);
                if (it == _states.end())
                {
                    return;
                }
                if (it->second.pending)
                {
                    again = true;
                }
                else
                {
                    it->second.running = false;
                }
            }

            if (!again)
            {
                return;
            }

            threadpool::ptr pool = _pool.lock();
            if (!pool || pool->stopped())
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _states[qname].running = false;
                return;
            }
            auto self = shared_from_this();
            pool->push([self, qname]() { self->drainLoop(qname); });
        }

        void drainBatch(const std::string &qname)
        {
            // 每批消息数有上限；超出的部分通过 pending 让下一个任务继续处理。
            static const size_t kBatchSize = 100;
            size_t available = _host->getable_count(qname);
            size_t max_count = std::min(available, kBatchSize);
            for (size_t i = 0; i < max_count; ++i)
            {
                Consumer::ptr cp = _cmp->choose(qname);
                if (!cp)
                {
                    return;
                }
                // choose 已经跳过满额消费者；这里再检查一次，防御并发调整 QoS。
                if (!cp->hasCapacity())
                {
                    return;
                }

                MessagePtr mp = _host->basicConsume(qname, cp->tag);
                if (!mp)
                {
                    return;
                }

                // 取消息和取消订阅之间可能发生竞争：如果消费者已经被移除，
                // 这条消息不能再丢在 wait-ack 里，立刻按 owner 重新入队。
                if (!_cmp->exists(cp->tag, qname))
                {
                    _host->requeueByConsumer(qname, cp->tag);
                    markPending(qname);
                    continue;
                }

                // 从这一刻起，消息占用消费者的一个 prefetch 名额。
                cp->incUnacked();
                cp->callback(cp->tag, mp->mutable_payload()->mutable_properties(),
                             mp->payload().body());

                if (cp->auto_ack)
                {
                    _host->basicAck(qname, mp->payload().properties().id());
                    cp->decUnacked();
                }
                else if (!_cmp->exists(cp->tag, qname))
                {
                    // 回调执行期间消费者被取消/断线：重新入队，等待下一个消费者。
                    _host->requeueByConsumer(qname, cp->tag);
                    cp->decUnacked();
                    markPending(qname);
                }
            }

            // 本批只处理了前 kBatchSize 条，后面还有消息。
            if (available > max_count)
            {
                markPending(qname);
            }
        }

        // 从 drain 内部再次标记 pending。drainLoop 会在本批结束后
        // 判断是否重新投递任务继续处理。
        void markPending(const std::string &qname)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _states[qname].pending = true;
        }

    private:
        VirtualHost::ptr _host;
        ConsumerManager::ptr _cmp;
        std::weak_ptr<threadpool> _pool;
        std::mutex _mutex;
        std::unordered_map<std::string, QueueState> _states;
    };
}
#endif
