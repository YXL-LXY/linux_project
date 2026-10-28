#ifndef MQ_BRIDGE_QOS_MAPPER_HPP
#define MQ_BRIDGE_QOS_MAPPER_HPP

#include "bridge_config.pb.h"

#include <cstdint>
#include <map>
#include <string>

namespace mq
{
namespace bridge
{

// bridge 内部的 QoS 中间表示。
//
// MQ 与 DDS 的 QoS 并不是一一对应的，这里只做 MVP 所需的最小近似，避免配置
// 看起来比实际保证更强。真正的 DDS 特性（deadline、liveliness、lease 等）
// 尚未实现。

// 从 TopicConfig 映射出的 ROS2 QoS 子集。
// 只保留 MVP 需要的 reliability、durability 和 history depth。
struct Ros2Qos
{
    BridgeReliability reliability;  // RELIABLE / BEST_EFFORT
    BridgeDurability durability;    // VOLATILE / TRANSIENT_LOCAL
    uint32_t depth;                 // history depth，至少为 1
};

// 从 TopicConfig 映射出的 MQ 侧近似策略。
//
// 注意：这不是完整的 DDS QoS 映射。RELIABLE 使用手动 ACK，BEST_EFFORT
// 使用 auto_ack；depth 只对 BEST_EFFORT 队列额外提供 x-max-length 的
// 默认上限，RELIABLE 任务队列不会被静默截断。
struct MqQos
{
    // consume 时是否由 broker 自动确认。true 时 MqDelivery 不会带 ack/reject。
    bool auto_ack;
    // 声明 MQ 队列/交换机时是否持久化。这里按可靠性做近似，不等价于 DDS 语义。
    bool durable;
    // 是否把最新一条写入 bridge 的 RetainedStore。store 启用时，bridge 会在
    // 启动阶段回放；更晚加入的订阅者是否收到还取决于 ROS2 durability。
    bool retained;
    // 额外的队列参数，例如 x-max-length、x-dead-letter-exchange。
    // 注意：x-ack-timeout-ms / x-max-delivery-count 目前不会自动注入。
    std::map<std::string, std::string> queue_args;
};

class QosMapper
{
public:
    // 把配置枚举转换成 rclcpp::QoS 需要的三个值；未填的 reliability/
    // durability 会回退到 RELIABLE + VOLATILE，depth 至少为 1。
    // 正常配置加载流程已经补过 depth=10，这里只是防御性兜底。
    static Ros2Qos toRos2(const TopicConfig &topic);

    // 把配置转换成 MQ 侧拓扑/消费参数。该函数不修改 topic。
    static MqQos toMq(const TopicConfig &topic);

    // TRANSIENT_LOCAL 或显式 retained=true 都应进入 bridge retained store。
    // 两者是“或”的关系：显式 retained 可以让 VOLATILE 也保留最近值。
    static bool shouldRetain(const TopicConfig &topic);

    // 将 bridge 的 std::map 转成客户端 API 需要的 protobuf map。
    // 客户端声明接口要求非 const map，因此这里返回一份可修改的副本。
    static google::protobuf::Map<std::string, std::string>
    toProtobufMap(const std::map<std::string, std::string> &args);
};

} // namespace bridge
} // namespace mq

#endif
