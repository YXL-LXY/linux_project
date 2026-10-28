#ifndef MQ_BRIDGE_ROS2_BRIDGE_HPP
#define MQ_BRIDGE_ROS2_BRIDGE_HPP

#include "bridge_config.pb.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace mq
{
namespace bridge
{

// ROS2 侧抽象。
//
// 目的：bridge_core 和 MQ 侧代码不需要 include rclcpp；没有安装 ROS2 的机器
// 也能构建 mq_bridge 并执行配置校验。启用 ROS2 时 createRos2Bridge() 返回
// RclcppRos2Bridge，未启用时返回一个明确报错的 stub。
class Ros2Bridge
{
public:
    // 收到一条 ROS2 消息时调用。
    // topic 是配置中的映射快照；cdr 是 ROS2 序列化后的原始字节（通常带
    // CDR encapsulation），bridge 不解析；stamp_ns 是 bridge 收到消息的时间。
    // 回调运行在 ROS2 executor 线程（通常就是 spin() 所在线程），因此不能
    // 在里面做长时间阻塞操作。
    using SerializedMessageHandler = std::function<void(
        const TopicConfig &topic, const std::string &cdr, uint64_t stamp_ns)>;

    virtual ~Ros2Bridge() {}

    // 创建 node、publishers 和 subscriptions。调用前必须已经 rclcpp::init()。
    virtual bool start(const BridgeConfig &config, std::string *error) = 0;

    // 把 cdr 字节发布到 topic 对应的 generic publisher。
    // cdr 必须与配置的 ros_type 匹配；这里无法做运行时类型校验。
    virtual bool publish(const TopicConfig &topic,
                         const std::string &cdr,
                         std::string *error) = 0;

    // 设置 ROS2 -> MQ 的消息回调，可在 start() 前后调用；内部加锁。
    virtual void setMessageHandler(SerializedMessageHandler handler) = 0;

    // 阻塞执行 ROS2 executor，直到 rclcpp::shutdown()。
    // 注意：stop() 不是在另一个线程里打断 spin() 的手段。
    virtual void spin() = 0;

    // 释放 subscriptions/publishers/node。应在 spin() 返回后调用。
    virtual void stop() = 0;
};

// 未启用 ROS2 时返回一个明确报错的 stub，保证 mq_bridge 仍可构建和校验配置。
std::shared_ptr<Ros2Bridge> createRos2Bridge();

// 供 main 输出构建状态。
bool ros2BridgeAvailable();

} // namespace bridge
} // namespace mq

#endif
