#include "bridge/bridge_runtime.hpp"

#include "bridge/envelope_codec.hpp"
#include "bridge/qos_mapper.hpp"
#include "bridge/retained_store.hpp"
#include "common/mq_helper.hpp"
#include "common/mq_logger.hpp"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <set>
#include <thread>

namespace mq
{
namespace bridge
{

namespace
{

// 防止 ROS2 发布速度远高于 MQ 时无限堆积任务。达到上限时回调返回失败：
// MQ -> ROS2 会走 reject(requeue)，ROS2 -> MQ 则只能记录后丢弃（当前客户端
// 没有 Publisher Confirm，无法提供更强的保证）。
const size_t kMaxPendingTasks = 4096;

void setError(std::string *error, const std::string &message)
{
    if (error != nullptr)
    {
        *error = message;
    }
}

std::string topicLabel(const TopicConfig &topic, size_t index)
{
    if (!topic.name().empty())
    {
        return topic.name();
    }
    return "topics[" + std::to_string(index) + "]";
}

} // namespace

// ---------------------------------------------------------------------------
// 串行任务队列
// ---------------------------------------------------------------------------
//
// 一个线程足以串行化两个方向的跨域操作：
// - MQ adapter 的 publish/ack/reject 都是同步网络请求；
// - ROS2 generic publisher 的 publish 很快，但也不应该在 executor 回调里
//   与 MQ 网络等待混在一起。
//
// stop() 的语义是“停止接收新任务并丢弃尚未开始的任务”。正在执行的任务会
// 被 join 等待完成；MQ 侧尚未开始的任务不会被 ACK，随后 MqClientAdapter::stop
// 的 basicCancel 会让 broker 重新投递它们。
class BridgeRuntime::TaskQueue
{
public:
    TaskQueue() : _running(false), _stopping(false) {}

    ~TaskQueue()
    {
        stop();
    }

    TaskQueue(const TaskQueue &) = delete;
    TaskQueue &operator=(const TaskQueue &) = delete;

    bool start(std::string *error)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_running)
        {
            return true;
        }

        _stopping = false;
        try
        {
            _thread = std::thread(&TaskQueue::run, this);
        }
        catch (const std::exception &e)
        {
            setError(error, std::string("failed to start bridge task thread: ") + e.what());
            return false;
        }
        _running = true;
        return true;
    }

    void stop()
    {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (!_running && !_thread.joinable())
            {
                return;
            }
            // 先置位再清空队列；worker 醒来后会直接退出，不会继续处理已经
            // 排队的任务。未处理的 MQ delivery 由 adapter.stop() 统一归还。
            _stopping = true;
            _running = false;
            _tasks.clear();
        }
        _cv.notify_all();

        if (_thread.joinable())
        {
            // stop() 只能由外部线程（main/测试）调用；不要从任务内部调用，
            // 否则 join 自己会死锁。正常运行路径不会出现这种自调用。
            _thread.join();
        }
    }

    bool submit(std::function<void()> task)
    {
        if (!task)
        {
            return false;
        }

        std::lock_guard<std::mutex> lock(_mutex);
        if (_stopping || !_running || _tasks.size() >= kMaxPendingTasks)
        {
            return false;
        }
        _tasks.push_back(std::move(task));
        _cv.notify_one();
        return true;
    }

private:
    void run()
    {
        for (;;)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                _cv.wait(lock, [this]() { return _stopping || !_tasks.empty(); });
                if (_stopping)
                {
                    return;
                }
                task = std::move(_tasks.front());
                _tasks.pop_front();
            }

            try
            {
                task();
            }
            catch (const std::exception &e)
            {
                ELOG("bridge task thread caught exception: %s", e.what());
            }
            catch (...)
            {
                ELOG("bridge task thread caught unknown exception");
            }
        }
    }

    bool _running;
    bool _stopping;
    std::thread _thread;
    std::mutex _mutex;
    std::condition_variable _cv;
    std::deque<std::function<void()> > _tasks;
};

// ---------------------------------------------------------------------------
// 生命周期
// ---------------------------------------------------------------------------

BridgeRuntime::BridgeRuntime() : _running(false)
{
}

BridgeRuntime::~BridgeRuntime()
{
    stop();
}

bool BridgeRuntime::running() const
{
    return _running.load();
}

bool BridgeRuntime::start(const BridgeConfig &config, std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }

    std::lock_guard<std::mutex> lifecycle_lock(_lifecycle_mutex);
    if (_running.load() || _tasks.get() != nullptr || _ros2.get() != nullptr)
    {
        setError(error, "bridge runtime is already started");
        return false;
    }

    // main 已经通过 BridgeConfigLoader 加载配置；这里再校验一次，使
    // BridgeRuntime 也可以被测试或其它入口直接复用，不依赖调用者遗漏默认值。
    _config = config;
    BridgeConfigLoader::applyDefaults(&_config);
    if (!BridgeConfigLoader::validate(_config, error))
    {
        return false;
    }

    // 1. 连接 broker。connect() 会阻塞到 TCP 建立完成，必须发生在 spin 之前。
    if (!_mq.connect(_config.mq_host(), static_cast<int>(_config.mq_port()), error))
    {
        stopInternal();
        return false;
    }

    // 2. 声明 exchange/queue/binding。拓扑声明失败就不能继续消费或发布，
    // 否则 broker 会返回“exchange 不存在”等难以定位的运行时错误。
    if (!declareTopology(error))
    {
        stopInternal();
        return false;
    }

    // 3. retained store 是 bridge 自己的可选状态。加载失败时直接拒绝启动，
    // 避免用户以为持久化状态已经恢复。
    if (_config.retained_store().enabled())
    {
        _retained = std::make_shared<RetainedStore>(
            static_cast<size_t>(_config.retained_store().capacity_per_topic()),
            static_cast<uint64_t>(_config.retained_store().ttl_sec()) * 1000ULL,
            _config.retained_store().persist_path());
        if (!_retained->load(error))
        {
            stopInternal();
            return false;
        }
        ILOG("bridge retained store loaded, entries=%zu", _retained->size());
    }

    // 4. 先启动串行任务线程，再创建 ROS2 端点。虽然 start() 返回前不会
    // spin，但这样可以让后续所有回调都遵守同一个线程模型。
    try
    {
        _tasks.reset(new TaskQueue());
    }
    catch (const std::exception &e)
    {
        setError(error, std::string("failed to allocate bridge task queue: ") + e.what());
        stopInternal();
        return false;
    }
    if (!_tasks->start(error))
    {
        stopInternal();
        return false;
    }

    // 5. 创建 ROS2 node、publisher、subscription。回调只把消息投递给任务线程，
    // 不在 executor 线程里做 MQ 网络等待。
    _ros2 = createRos2Bridge();
    _ros2->setMessageHandler(
        [this](const TopicConfig &topic, const std::string &cdr, uint64_t stamp_ns)
        {
            onRos2Message(topic, cdr, stamp_ns);
        });
    if (!_ros2->start(_config, error))
    {
        stopInternal();
        return false;
    }

    // 6. 在开始消费前回放 retained 状态，让 MQ_TO_ROS2 的晚加入订阅者拿到
    // 最近一条。失败只记录日志，不阻断主数据面（retained 是辅助状态）。
    replayRetained();

    // 7. 启动 MQ 消费者。先置 running=true，避免第一条消息在 consume() 返回
    // 与状态位设置之间被误判为“正在停止”。
    _running.store(true);
    for (int i = 0; i < _config.topics_size(); ++i)
    {
        const TopicConfig &topic = _config.topics(i);
        if (!BridgeConfigLoader::includesMqToRos2(topic.direction()))
        {
            continue;
        }

        const MqQos qos = QosMapper::toMq(topic);
        // 手动 ACK 时用 depth 作为近似 prefetch：既限制在途任务数量，又不
        // 让 bridge 在 ROS2 侧积压过多消息。auto_ack 不需要额外限额。
        const uint32_t prefetch =
            qos.auto_ack ? 0 : (topic.depth() > 0 ? topic.depth() : 1);
        const std::string consumer_tag =
            "mq_bridge_" + std::to_string(i) + "_" + mq::UUIDHelper::uuid();

        std::string detail;
        if (!_mq.consume(
                topic.queue(),
                consumer_tag,
                prefetch,
                qos.auto_ack,
                [this, i](const MqDelivery &delivery)
                {
                    onMqMessage(static_cast<size_t>(i), delivery);
                },
                &detail))
        {
            setError(error,
                     "topic " + topicLabel(topic, static_cast<size_t>(i)) +
                         ": " + detail);
            stopInternal();
            return false;
        }
        ILOG("bridge consuming queue=%s topic=%s auto_ack=%d prefetch=%u",
             topic.queue().c_str(), topic.name().c_str(),
             qos.auto_ack ? 1 : 0, prefetch);
    }

    ILOG("bridge runtime started: broker=%s:%u topics=%d",
         _config.mq_host().c_str(), _config.mq_port(), _config.topics_size());
    return true;
}

void BridgeRuntime::spin()
{
    // 取一份 shared_ptr，避免 stop() 在另一个线程重置成员时让 spin 内部
    // 访问悬空指针。正常约定仍然是：spin() 返回后才调用 stop()。
    std::shared_ptr<Ros2Bridge> ros2;
    {
        std::lock_guard<std::mutex> lock(_lifecycle_mutex);
        ros2 = _ros2;
    }
    if (ros2.get() != nullptr)
    {
        ros2->spin();
    }
}

void BridgeRuntime::stop()
{
    std::lock_guard<std::mutex> lifecycle_lock(_lifecycle_mutex);
    stopInternal();
}

void BridgeRuntime::stopInternal()
{
    // 先阻止新回调投递任务，并等待正在执行的任务结束；此时 MQ adapter 和
    // ROS2 node 仍然有效，in-flight 任务可以安全完成 publish/ack。
    //
    // 注意：这里只 stop()、不立刻 reset()。MqClientAdapter::stop() 内部销毁
    // worker 时会 join 仍在执行的 MQ 回调，而这些回调仍可能读取 _tasks；如果
    // 提前 reset，unique_ptr 会和回调线程形成数据竞争。等 MQ/ROS2 侧都停止后
    // 再释放任务队列。
    _running.store(false);
    if (_tasks.get() != nullptr)
    {
        _tasks->stop();
    }

    // 取消消费者并关闭连接。basicCancel 会把未 ACK 的消息交还 broker；
    // 任务队列里已经丢掉、尚未 ACK 的消息也依赖这一步恢复。stop() 还会
    // join AsyncWorker 的线程池，确保不再有 MQ 回调访问 _tasks。
    _mq.stop();

    // 最后销毁 ROS2 端点。stop() 约定在 spin() 返回后调用，因此这里不会
    // 与 executor 回调并发。
    if (_ros2.get() != nullptr)
    {
        _ros2->stop();
        _ros2.reset();
    }

    // MQ 回调和 ROS2 回调都已停止，此时释放任务队列才安全。
    _tasks.reset();
    _retained.reset();
}

// ---------------------------------------------------------------------------
// 拓扑声明
// ---------------------------------------------------------------------------

mq::ExchangeType BridgeRuntime::mapExchangeType(BridgeExchangeType type)
{
    // 配置层枚举和 broker 协议枚举保持分离；未知值回退到 TOPIC，与配置
    // 默认值一致。真正的非法值在 BridgeConfigLoader::validate() 已拒绝。
    switch (type)
    {
    case EXCHANGE_DIRECT:
        return mq::DIRECT;
    case EXCHANGE_FANOUT:
        return mq::FANOUT;
    case EXCHANGE_TOPIC:
    default:
        return mq::TOPIC;
    }
}

bool BridgeRuntime::declareTopology(std::string *error)
{
    // 多个 topic 可以引用同一个 exchange/queue；broker 的声明接口虽然是
    // 幂等的，但重复发起同步请求没有意义，也容易掩盖配置冲突。这里按名字
    // 去重，只声明一次。
    std::set<std::string> declared_exchanges;
    std::set<std::string> declared_queues;
    std::set<std::string> declared_bindings;

    for (int i = 0; i < _config.topics_size(); ++i)
    {
        const TopicConfig &topic = _config.topics(i);
        const std::string label = topicLabel(topic, static_cast<size_t>(i));
        const MqQos qos = QosMapper::toMq(topic);
        const bool mq_to_ros2 = BridgeConfigLoader::includesMqToRos2(topic.direction());
        const bool ros2_to_mq = BridgeConfigLoader::includesRos2ToMq(topic.direction());

        // MQ -> ROS2 只有在显式配置 exchange 时才需要声明/绑定；队列本身
        // 可能由其它客户端预先绑定好。ROS2 -> MQ 则必须声明发布目标。
        const bool need_exchange =
            ros2_to_mq || (mq_to_ros2 && !topic.exchange().empty());
        const bool need_queue =
            (mq_to_ros2 && !topic.queue().empty()) ||
            (ros2_to_mq && !topic.queue().empty());

        if (need_exchange)
        {
            if (declared_exchanges.insert(topic.exchange()).second)
            {
                // exchange 的 args 目前没有配置入口，保留空 map 便于以后扩展。
                google::protobuf::Map<std::string, std::string> exchange_args;
                std::string detail;
                if (!_mq.declareExchange(topic.exchange(),
                                         mapExchangeType(topic.exchange_type()),
                                         qos.durable,
                                         false,
                                         exchange_args,
                                         &detail))
                {
                    setError(error, label + ": " + detail);
                    return false;
                }
                ILOG("bridge declared exchange=%s type=%d durable=%d",
                     topic.exchange().c_str(),
                     static_cast<int>(mapExchangeType(topic.exchange_type())),
                     qos.durable ? 1 : 0);
            }
        }

        if (need_queue)
        {
            if (declared_queues.insert(topic.queue()).second)
            {
                std::string detail;
                if (!_mq.declareQueue(topic.queue(),
                                      qos.durable,
                                      false,
                                      false,
                                      QosMapper::toProtobufMap(qos.queue_args),
                                      &detail))
                {
                    setError(error, label + ": " + detail);
                    return false;
                }
                ILOG("bridge declared queue=%s durable=%d args=%zu",
                     topic.queue().c_str(), qos.durable ? 1 : 0,
                     qos.queue_args.size());
            }
        }

        // 只有同时有 exchange 和 queue 才能建立绑定。MQ_TO_ROS2 未配置
        // exchange 时假定 queue 已有外部绑定；ROS2_TO_MQ 的 queue 可选，
        // 为空时只发布到 exchange，由已有绑定决定路由目标。
        if (need_exchange && need_queue && !topic.exchange().empty() &&
            !topic.queue().empty())
        {
            const std::string binding_key =
                topic.exchange() + "\x1f" + topic.queue() + "\x1f" +
                topic.routing_key();
            if (declared_bindings.insert(binding_key).second)
            {
                std::string detail;
                if (!_mq.bindQueue(topic.exchange(), topic.queue(),
                                   topic.routing_key(), &detail))
                {
                    setError(error, label + ": " + detail);
                    return false;
                }
                ILOG("bridge bound exchange=%s queue=%s routing_key=%s",
                     topic.exchange().c_str(), topic.queue().c_str(),
                     topic.routing_key().c_str());
            }
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// retained state
// ---------------------------------------------------------------------------

uint64_t BridgeRuntime::nowMs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

void BridgeRuntime::replayRetained()
{
    if (_retained.get() == nullptr || _ros2.get() == nullptr)
    {
        return;
    }

    size_t replayed = 0;
    for (int i = 0; i < _config.topics_size(); ++i)
    {
        const TopicConfig &topic = _config.topics(i);
        if (!BridgeConfigLoader::includesMqToRos2(topic.direction()) ||
            !QosMapper::shouldRetain(topic))
        {
            continue;
        }

        std::string bytes;
        uint64_t updated_ms = 0;
        std::string detail;
        if (!_retained->get(topic.name(), &bytes, &updated_ms, nowMs(), &detail))
        {
            // 没有记录是正常情况；只有持久化/读取错误才需要提示。
            if (!detail.empty())
            {
                ELOG("bridge retained replay failed for topic %s: %s",
                     topic.name().c_str(), detail.c_str());
            }
            continue;
        }

        BridgeEnvelope envelope;
        if (!EnvelopeCodec::decode(bytes, &envelope, &detail))
        {
            ELOG("bridge retained envelope invalid for topic %s: %s",
                 topic.name().c_str(), detail.c_str());
            continue;
        }
        // 配置可能在两次运行之间改过；只回放到当前配置仍然匹配的 topic。
        if (envelope.topic() != topic.name() ||
            envelope.ros_type() != topic.ros_type())
        {
            ELOG("bridge retained envelope mismatch for topic %s (stored=%s/%s)",
                 topic.name().c_str(), envelope.topic().c_str(),
                 envelope.ros_type().c_str());
            continue;
        }

        if (!_ros2->publish(topic, envelope.cdr(), &detail))
        {
            ELOG("bridge retained replay publish failed for topic %s: %s",
                 topic.name().c_str(), detail.c_str());
            continue;
        }
        ++replayed;
    }

    if (replayed > 0)
    {
        ILOG("bridge replayed %zu retained ROS2 publisher(s)", replayed);
    }
}

// ---------------------------------------------------------------------------
// MQ -> ROS2
// ---------------------------------------------------------------------------

void BridgeRuntime::onMqMessage(size_t topic_index, const MqDelivery &delivery)
{
    if (topic_index >= static_cast<size_t>(_config.topics_size()))
    {
        rejectDelivery(delivery, true, "invalid topic index");
        return;
    }
    if (!_running.load())
    {
        // stop() 已经清位；此时不应再 ACK，交还给 adapter.stop() 重投。
        rejectDelivery(delivery, true, "bridge is stopping");
        return;
    }

    const TopicConfig topic = _config.topics(static_cast<int>(topic_index));
    if (_tasks.get() == nullptr ||
        !_tasks->submit(
            [this, topic, delivery]()
            {
                processMqMessage(topic, delivery);
            }))
    {
        rejectDelivery(delivery, true, "bridge task queue is full or stopping");
    }
}

void BridgeRuntime::rejectDelivery(const MqDelivery &delivery,
                                   bool requeue,
                                   const std::string &reason)
{
    ELOG("bridge reject MQ message id=%s requeue=%d: %s",
         delivery.properties.id().c_str(), requeue ? 1 : 0, reason.c_str());
    if (delivery.reject)
    {
        delivery.reject(requeue);
    }
    else
    {
        // auto_ack 或 broker 未返回 message id 时没有 Reject 句柄；消息已经
        // 被 broker 自动确认，bridge 无法把它放回队列。
        ELOG("bridge cannot reject MQ message id=%s (no reject handle)",
             delivery.properties.id().c_str());
    }
}

void BridgeRuntime::processMqMessage(const TopicConfig &topic,
                                     const MqDelivery &delivery)
{
    BridgeEnvelope envelope;
    std::string detail;
    if (!EnvelopeCodec::decode(delivery.body, &envelope, &detail))
    {
        // 无法解析的消息永远不会变得可解析，requeue=true 只会造成毒消息
        // 循环；如果有死信配置则交给 broker 的 DLX，否则直接丢弃。
        rejectDelivery(delivery, false, "invalid BridgeEnvelope: " + detail);
        return;
    }

    // 严格校验 envelope 与队列对应的配置。这样同一个队列被错误绑定到多个
    // bridge topic 时，不会把消息发布到未预期的 ROS2 topic。
    if (envelope.topic() != topic.name() ||
        envelope.ros_type() != topic.ros_type())
    {
        rejectDelivery(
            delivery, false,
            "envelope topic/type mismatch: got " + envelope.topic() + "/" +
                envelope.ros_type() + ", expected " + topic.name() + "/" +
                topic.ros_type());
        return;
    }

    if (_ros2.get() == nullptr ||
        !_ros2->publish(topic, envelope.cdr(), &detail))
    {
        // ROS2 发布失败通常是瞬时问题（executor/node 状态、DDS 资源等），
        // 重新入队让 broker 按自己的重投/超时策略处理。
        rejectDelivery(delivery, true, "ROS2 publish failed: " + detail);
        return;
    }

    // retained 是辅助状态：写盘失败只记录日志，不改变主消息的 ACK 语义。
    // 否则磁盘故障会让已经成功发布到 ROS2 的消息被重复投递。
    if (_retained.get() != nullptr && QosMapper::shouldRetain(topic))
    {
        if (!_retained->put(topic.name(), delivery.body, nowMs(), &detail))
        {
            ELOG("bridge retained put failed for topic %s: %s",
                 topic.name().c_str(), detail.c_str());
        }
    }

    if (delivery.ack)
    {
        delivery.ack();
    }
    else if (!QosMapper::toMq(topic).auto_ack)
    {
        // 手动 ACK 模式下 broker 应返回 message id；缺失时无法精确确认，
        // 消息会在 broker 的 ACK 超时后重投。这里显式记录，避免静默卡住。
        ELOG("bridge cannot ACK MQ message id=%s topic=%s",
             delivery.properties.id().c_str(), topic.name().c_str());
    }
}

// ---------------------------------------------------------------------------
// ROS2 -> MQ
// ---------------------------------------------------------------------------

void BridgeRuntime::onRos2Message(const TopicConfig &topic,
                                  const std::string &cdr,
                                  uint64_t stamp_ns)
{
    if (!_running.load())
    {
        return;
    }
    if (_tasks.get() == nullptr ||
        !_tasks->submit(
            [this, topic, cdr, stamp_ns]()
            {
                publishRos2Message(topic, cdr, stamp_ns);
            }))
    {
        // ROS2 侧没有确认机制，任务队列满或正在停止时只能丢弃并记录。
        ELOG("bridge dropped ROS2 -> MQ message topic=%s: task queue unavailable",
             topic.name().c_str());
    }
}

void BridgeRuntime::publishRos2Message(const TopicConfig &topic,
                                       const std::string &cdr,
                                       uint64_t stamp_ns)
{
    BridgeEnvelope envelope;
    envelope.set_version(EnvelopeCodec::kCurrentVersion);
    envelope.set_topic(topic.name());
    envelope.set_ros_type(topic.ros_type());
    envelope.set_stamp_ns(stamp_ns);
    envelope.set_cdr(cdr);
    envelope.set_source(_config.node_name());
    envelope.set_correlation_id(mq::UUIDHelper::uuid());
    envelope.set_reliability(topic.reliability());
    envelope.set_durability(topic.durability());
    envelope.set_depth(topic.depth());

    std::string body;
    std::string detail;
    if (!EnvelopeCodec::encode(envelope, &body, &detail))
    {
        ELOG("bridge failed to encode ROS2 -> MQ envelope topic=%s: %s",
             topic.name().c_str(), detail.c_str());
        return;
    }

    const MqQos qos = QosMapper::toMq(topic);
    mq::BasicProperties properties;
    // 使用 envelope 的 correlation id 作为 broker 消息 id，便于把 bridge
    // 日志和 broker 侧的 wait-ack/死信记录关联起来。
    properties.set_id(envelope.correlation_id());
    properties.set_delivery_mode(qos.durable ? mq::DURABLE : mq::UNDURABLE);
    properties.set_routing_key(topic.routing_key());

    if (!_mq.publish(topic.exchange(), properties, body, &detail))
    {
        // 当前客户端没有 Publisher Confirm；到这里失败只表示请求没有得到
        // broker 的普通成功响应，不能证明消息一定没有落到某个队列。
        ELOG("bridge failed to publish ROS2 -> MQ topic=%s exchange=%s: %s",
             topic.name().c_str(), topic.exchange().c_str(), detail.c_str());
        return;
    }
}

} // namespace bridge
} // namespace mq
