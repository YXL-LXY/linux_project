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
    // 它在整个 broker 中承担三件事：
    // 1. 发布、订阅、ACK、Reject、ACK 超时后统一唤醒某个队列；
    // 2. 按 ConsumerManager 的轮转结果选择消费者，并遵守 prefetch 上限；
    // 3. 在消费者取消、连接断开等竞争条件下，把已经取出的消息安全地放回队列。
    //
    // 并发模型：
    // - notify()/checkAckTimeout() 可能由 muduo IO 线程或定时器线程调用；
    // - 真正的投递在 threadpool 工作线程中执行；
    // - _mutex 只保护 _states，队列消息本身的并发安全由 QueueMessage 自己负责。
    //
    // 每个队列有一个小的状态机：
    //   running=false, pending=false  空闲
    //   running=true,  pending=false  正在处理一批
    //   running=true,  pending=true   正在处理，同时又有新的唤醒
    //   running=false, pending=true   drainLoop 结束时发现还有工作，立即补一个任务
    //
    // 关键不变量：同一队列同一时刻最多只有一个 drain 任务。这样多个发布者
    // 同时 notify 时不会各自启动一个任务去抢同一批消息。
    //
    // 生命周期：本类必须通过 std::make_shared<QueueDispatcher>() 创建。
    // notify() 会捕获 shared_from_this()，保证 drain 期间 host/cmp 不会被释放；
    // 对线程池只保存 weak_ptr，避免 dispatcher 和 threadpool 形成循环引用。
    class QueueDispatcher : public std::enable_shared_from_this<QueueDispatcher>
    {
    public:
        using ptr = std::shared_ptr<QueueDispatcher>;

        QueueDispatcher(const VirtualHost::ptr &host,
                        const ConsumerManager::ptr &cmp,
                        const threadpool::ptr &pool)
            : _host(host), _cmp(cmp), _pool(pool) {}

        // 发布消息、订阅消息、确认/拒绝、消费者断线后都可以调用。
        //
        // 这个方法只做“标记 + 尝试启动一个任务”，不直接投递消息。
        // 同一个队列的重复调用会被合并：
        // - 如果已有 drain 任务在跑，只把 pending 置 true；
        // - 如果没有任务在跑，则启动一个 drainLoop。
        //
        // 注意：消费者已经全部取消时直接返回，等下一次 basicConsume 再唤醒。
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
        //
        // 无参版本使用 steady_clock 的当前时间；带 now_ms 的版本主要给测试
        // 注入“未来时间”，避免测试必须真的睡眠 30 秒。
        void checkAckTimeout()
        {
            checkAckTimeout(mqNowMs());
        }
        void checkAckTimeout(uint64_t now_ms)
        {
            // requeueExpired() 会逐个队列扫描 wait-ack，并返回：
            // - consumer_tags：哪些消费者失去了在途消息，需要归还 prefetch 名额；
            // - notify_queues：死信路由命中的目标队列，需要被唤醒。
            std::vector<VirtualHost::TimeoutResult> results =
                _host->requeueExpired(now_ms);
            for (auto &result : results)
            {
                // 超时/死信后消息已经不在原消费者的 wait-ack 里，
                // 必须同步减少它的 unacked_count，否则 prefetch 名额会泄漏。
                for (auto &tag : result.consumer_tags)
                {
                    _cmp->onAck(result.queue_name, tag);
                }
                // 源队列中可能已经有重新入队的消息，需要继续投递。
                notify(result.queue_name);
                // 死信消息进入目标队列后，同样要唤醒目标队列。
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

        // 单个 drain 任务的入口。
        //
        // 每个任务最多处理 kBatchSize 条消息；处理完后再检查 pending。
        // 如果还有消息，重新 push 一个任务，而不是在当前线程继续循环，
        // 这样线程池里的其他队列任务有机会先执行，避免大队列饿死其他队列。
        //
        // 这里维护 running/pending 的完整状态转换：
        // - 进来时若没有 pending，说明唤醒已被别的任务消费，直接退出；
        // - 本批结束后若 pending=true，保留 running=true 并再投递一个任务；
        // - 否则把 running 置 false，队列回到空闲状态。
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

        // 投递一个批次。调用方已经保证同一队列只有一个 drain 任务。
        //
        // 每次循环的执行顺序：
        // 1. choose()：轮转选一个还有 prefetch 额度的消费者；
        // 2. basicConsume()：从队列取出一条消息并放入 wait-ack；
        // 3. 检查消费者是否已取消，必要时回滚；
        // 4. 增加 unacked_count，调用 Channel 的推送回调；
        // 5. auto_ack 立即确认；手动 ACK 则等客户端后续 basicAck/basicReject。
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
                    // 队列没有消费者，或所有消费者都达到 prefetch 上限。
                    // 不需要 markPending：ACK/Reject/取消订阅/QoS 调整会再次 notify。
                    return;
                }
                // choose 已经跳过满额消费者；这里再检查一次，防御并发调整 QoS
                // 或另一个线程刚刚增加了 unacked_count 的窗口。
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
                // consumer_tag 就是这里的“owner”，不会误伤其他消费者的消息。
                if (!_cmp->exists(cp->tag, qname))
                {
                    _host->requeueByConsumer(qname, cp->tag);
                    markPending(qname);
                    continue;
                }

                // 从这一刻起，消息占用消费者的一个 prefetch 名额。
                // 必须先 inc 再调用 callback：callback 内部可能同步触发 ACK/Reject，
                // 这样计数不会出现“先减后加”的负数窗口。
                cp->incUnacked();
                cp->callback(cp->tag, mp->mutable_payload()->mutable_properties(),
                             mp->payload().body());

                if (cp->auto_ack)
                {
                    // auto_ack 的语义是“回调返回即确认”，即使 Channel 已断开也不回滚。
                    _host->basicAck(qname, mp->payload().properties().id());
                    cp->decUnacked();
                }
                else if (!_cmp->exists(cp->tag, qname))
                {
                    // 回调执行期间消费者被取消/断线：消息还在 wait-ack，
                    // 把它放回队首，并归还刚占用的 prefetch 名额。
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
