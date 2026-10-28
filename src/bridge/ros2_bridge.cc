#include "bridge/ros2_bridge.hpp"

#include "bridge/bridge_config.hpp"
#include "bridge/qos_mapper.hpp"

#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <vector>

#ifdef MQ_WITH_ROS2
#include <rclcpp/generic_publisher.hpp>
#include <rclcpp/generic_subscription.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialized_message.hpp>
#endif

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

#ifdef MQ_WITH_ROS2

// 生成 BridgeEnvelope.stamp_ns 的时间基准：Unix epoch 纳秒。
// 使用系统时钟而不是 steady_clock，因为该值要跨进程/跨机器解释。
uint64_t nowNs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

rclcpp::QoS makeRos2Qos(const TopicConfig &topic)
{
    // 只映射 MVP 支持的三项：reliability、durability、history depth。
    // deadline/liveliness 等未配置，保持 rclcpp 默认值。
    const Ros2Qos mapped = QosMapper::toRos2(topic);
    rclcpp::QoS qos(static_cast<size_t>(mapped.depth));
    if (mapped.reliability == BEST_EFFORT)
    {
        qos.best_effort();
    }
    else
    {
        qos.reliable();
    }

    if (mapped.durability == TRANSIENT_LOCAL)
    {
        qos.transient_local();
    }
    else
    {
        qos.durability_volatile();
    }
    return qos;
}

// 基于 rclcpp generic pub/sub 的实现。
//
// 使用 generic 接口而不是为每种消息类型写 include/类型转换，意味着只依赖
// topic 名和 ros_type 字符串即可创建 publisher/subscription；消息体始终是
// 序列化字节，bridge 不需要知道 std_msgs/sensor_msgs 的字段。
class RclcppRos2Bridge : public Ros2Bridge
{
public:
    RclcppRos2Bridge() : _handler_mutex(), _handler() {}

    ~RclcppRos2Bridge() override
    {
        stop();
    }

    bool start(const BridgeConfig &config, std::string *error) override
    {
        if (error != nullptr)
        {
            error->clear();
        }
        // rclcpp::init() 必须先由 main 调用；这里只检查上下文是否可用。
        // _node 非空则说明 start() 被重复调用。
        if (!rclcpp::ok())
        {
            setError(error, "rclcpp is not initialized; call rclcpp::init first");
            return false;
        }
        if (_node.get() != nullptr)
        {
            setError(error, "ros2 bridge is already started");
            return false;
        }

        try
        {
            _node = std::make_shared<rclcpp::Node>(config.node_name());
            RCLCPP_INFO(_node->get_logger(), "mq_bridge ROS2 endpoints ready");

            // 按配置逐项创建 ROS2 端点：
            // - MQ_TO_ROS2：需要 generic publisher，后续把 MQ body 解出的
            //   cdr 发布到该 topic；
            // - ROS2_TO_MQ：需要 generic subscription，收到消息后调用 handler。
            // BIDIRECTIONAL 两项都会创建。
            for (int i = 0; i < config.topics_size(); ++i)
            {
                const TopicConfig &topic = config.topics(i);
                if (BridgeConfigLoader::includesMqToRos2(topic.direction()))
                {
                    rclcpp::GenericPublisher::SharedPtr publisher =
                        _node->create_generic_publisher(
                            topic.name(), topic.ros_type(), makeRos2Qos(topic));
                    _publishers[topic.name()] = publisher;
                }

                if (BridgeConfigLoader::includesRos2ToMq(topic.direction()))
                {
                    // topic 是 config 中的临时引用；回调可能在 start() 返回很久
                    // 之后才执行，因此按值捕获一份 TopicConfig 快照。
                    const TopicConfig topic_copy = topic;
                    rclcpp::GenericSubscription::SharedPtr subscription =
                        _node->create_generic_subscription(
                            topic.name(), topic.ros_type(), makeRos2Qos(topic),
                            [this, topic_copy](std::shared_ptr<rclcpp::SerializedMessage> message)
                            {
                                // 回调运行在 ROS2 executor 线程。这里只持锁复制
                                // handler，真正处理在锁外，避免 handler 调用
                                // setMessageHandler() 时死锁。
                                SerializedMessageHandler handler;
                                {
                                    std::lock_guard<std::mutex> lock(_handler_mutex);
                                    handler = _handler;
                                }
                                if (!handler || !message)
                                {
                                    return;
                                }

                                // generic subscription 给的是已经序列化好的 ROS2
                                // 消息；直接按字节复制，不解析具体消息类型。
                                const rcl_serialized_message_t &serialized =
                                    message->get_rcl_serialized_message();
                                std::string cdr;
                                if (serialized.buffer != nullptr && serialized.buffer_length > 0)
                                {
                                    cdr.assign(
                                        reinterpret_cast<const char *>(serialized.buffer),
                                        serialized.buffer_length);
                                }
                                // stamp 取 bridge 收到消息的时刻，而不是 ROS2
                                // header 里的时间；后者需要解析消息类型才能拿到。
                                handler(topic_copy, cdr, nowNs());
                            });
                    _subscriptions.push_back(subscription);
                }
            }
        }
        catch (const std::exception &e)
        {
            // 创建到一半失败时可能已经有 publisher/subscription，统一 stop()
            // 清理，保证对象状态可预测。
            stop();
            setError(error, std::string("failed to start ROS2 bridge: ") + e.what());
            return false;
        }
        return true;
    }

    bool publish(const TopicConfig &topic,
                 const std::string &cdr,
                 std::string *error) override
    {
        if (error != nullptr)
        {
            error->clear();
        }

        // MQ -> ROS2 的发布来自 bridge 任务线程，而 start()/stop() 由 main
        // 线程调用。publish 自身用锁保护 publisher 表的查找和 publish 调用；
        // stop() 的约定仍是“任务线程停止后再销毁 node”。
        std::lock_guard<std::mutex> lock(_publish_mutex);
        std::map<std::string, rclcpp::GenericPublisher::SharedPtr>::iterator it =
            _publishers.find(topic.name());
        if (it == _publishers.end() || it->second.get() == nullptr)
        {
            setError(error, "no ROS2 publisher configured for topic: " + topic.name());
            return false;
        }

        try
        {
            // SerializedMessage 会按 cdr.size() 分配缓冲区；把原始字节原样
            // 拷进去后 publish。generic publisher 不会再做类型转换。
            rclcpp::SerializedMessage message(cdr.size());
            rcl_serialized_message_t &serialized = message.get_rcl_serialized_message();
            if (!cdr.empty())
            {
                std::memcpy(serialized.buffer, cdr.data(), cdr.size());
            }
            serialized.buffer_length = cdr.size();
            it->second->publish(message);
        }
        catch (const std::exception &e)
        {
            setError(error, std::string("failed to publish ROS2 message: ") + e.what());
            return false;
        }
        return true;
    }

    void setMessageHandler(SerializedMessageHandler handler) override
    {
        // 只替换指针；已经进入回调的线程拿的是旧 handler 的副本，不会被
        // 这里修改到一半的函数对象影响。
        std::lock_guard<std::mutex> lock(_handler_mutex);
        _handler = handler;
    }

    void spin() override
    {
        // rclcpp::spin 会阻塞到 rclcpp::shutdown()；main 在它返回后才调用
        // stop()，避免在回调执行期间销毁 node。
        if (_node.get() != nullptr)
        {
            rclcpp::spin(_node);
        }
    }

    void stop() override
    {
        // 先销毁 subscription/publisher，再销毁 node。当前约定 stop() 在
        // spin() 返回之后调用，因此不会与正在执行的 ROS2 回调并发；这里仍
        // 持有 publish 锁，避免未来有线程在 stop 期间误调用 publish()。
        std::lock_guard<std::mutex> lock(_publish_mutex);
        _subscriptions.clear();
        _publishers.clear();
        _node.reset();
    }

private:
    rclcpp::Node::SharedPtr _node;
    std::map<std::string, rclcpp::GenericPublisher::SharedPtr> _publishers;
    std::vector<rclcpp::GenericSubscription::SharedPtr> _subscriptions;
    // 保护 _publishers 的查找和 publish；start() 完成后 publisher 表只读，
    // 但 publish 可能来自任务线程，加锁可以避免与 stop() 的清理竞争。
    std::mutex _publish_mutex;
    std::mutex _handler_mutex;
    SerializedMessageHandler _handler;
};

#else

// 没有 ROS2 时仍返回一个接口实现，让 main 可以正常构建、执行 --check-config，
// 并在 --run 时给出明确错误，而不是链接失败或抛异常。
class UnavailableRos2Bridge : public Ros2Bridge
{
public:
    bool start(const BridgeConfig &, std::string *error) override
    {
        setError(error,
                 "mq_bridge was built without ROS2 support; "
                 "reconfigure with -DMQ_BUILD_ROS2_BRIDGE=ON");
        return false;
    }

    bool publish(const TopicConfig &, const std::string &, std::string *error) override
    {
        setError(error, "mq_bridge was built without ROS2 support");
        return false;
    }

    void setMessageHandler(SerializedMessageHandler) override {}
    void spin() override {}
    void stop() override {}
};

#endif

} // namespace

std::shared_ptr<Ros2Bridge> createRos2Bridge()
{
#ifdef MQ_WITH_ROS2
    return std::make_shared<RclcppRos2Bridge>();
#else
    return std::make_shared<UnavailableRos2Bridge>();
#endif
}

bool ros2BridgeAvailable()
{
#ifdef MQ_WITH_ROS2
    return true;
#else
    return false;
#endif
}

} // namespace bridge
} // namespace mq
