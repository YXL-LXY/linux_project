#ifndef MQ_BRIDGE_BRIDGE_RUNTIME_HPP
#define MQ_BRIDGE_BRIDGE_RUNTIME_HPP

#include "bridge/bridge_config.hpp"
#include "bridge/mq_client_adapter.hpp"
#include "bridge/ros2_bridge.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace mq
{
namespace bridge
{

class RetainedStore;

// bridge 数据面编排器。
//
// 这个类把配置、MQ 适配器、ROS2 适配器和 retained store 串起来：
//   MQ 队列 --consume--> BridgeEnvelope 解码 --> ROS2 publish --> ACK/Reject
//   ROS2 topic --callback--> BridgeEnvelope 编码 --> MQ exchange publish
//
// 线程模型：
// - start()/stop() 由 main 线程调用；
// - MQ consume 回调运行在客户端 AsyncWorker 线程池；
// - ROS2 callback 运行在 rclcpp executor 线程；
// - 两个方向的实际跨域操作都会投递到同一个内部串行任务线程。这样 MQ/ROS2
//   回调都不用等待网络请求或 broker 响应，也避免多个线程同时调用
//   MqClientAdapter 的同步 API。
//
// 生命周期约束：
// - ROS2 的 spin() 会阻塞到 rclcpp::shutdown()；stop() 必须在 spin() 返回后
//   调用；
// - stop() 先停止任务线程，再取消 MQ 消费者，最后销毁 ROS2 node。这样
//   in-flight 任务仍能安全访问对象，未处理完的 MQ 消息也会由 basicCancel
//   交还给 broker。
class BridgeRuntime
{
public:
    BridgeRuntime();
    ~BridgeRuntime();

    BridgeRuntime(const BridgeRuntime &) = delete;
    BridgeRuntime &operator=(const BridgeRuntime &) = delete;

    // 连接 broker、声明拓扑、加载 retained store、创建 ROS2 端点并开始消费。
    // 失败时会清理已经创建的资源；error 返回可读原因。
    bool start(const BridgeConfig &config, std::string *error);

    // 阻塞运行 ROS2 executor，直到 rclcpp::shutdown()。未启动时立即返回。
    void spin();

    // 幂等停止。可在 start() 失败后重复调用，也可由析构函数再次调用。
    void stop();

    bool running() const;

private:
    // 串行任务队列。定义在 .cc 中，避免头文件暴露线程实现细节。
    class TaskQueue;

    bool declareTopology(std::string *error);
    void replayRetained();

    void onMqMessage(size_t topic_index, const MqDelivery &delivery);
    void processMqMessage(const TopicConfig &topic, const MqDelivery &delivery);
    void rejectDelivery(const MqDelivery &delivery,
                        bool requeue,
                        const std::string &reason);

    void onRos2Message(const TopicConfig &topic,
                       const std::string &cdr,
                       uint64_t stamp_ns);
    void publishRos2Message(const TopicConfig &topic,
                            const std::string &cdr,
                            uint64_t stamp_ns);

    void stopInternal();
    static mq::ExchangeType mapExchangeType(BridgeExchangeType type);
    static uint64_t nowMs();

    BridgeConfig _config;
    MqClientAdapter _mq;
    std::shared_ptr<Ros2Bridge> _ros2;
    std::shared_ptr<RetainedStore> _retained;
    std::unique_ptr<TaskQueue> _tasks;

    // 数据面是否可用。回调在投递任务前先检查它，stop() 先清位再停止线程。
    std::atomic<bool> _running;

    // 只保护 start()/stop() 的资源替换，不保护消息路径。
    std::mutex _lifecycle_mutex;
};

} // namespace bridge
} // namespace mq

#endif
