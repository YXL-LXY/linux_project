#include "bridge/qos_mapper.hpp"

#include <algorithm>

namespace mq
{
namespace bridge
{

Ros2Qos QosMapper::toRos2(const TopicConfig &topic)
{
    // 正常加载流程已经补过默认值；这里再兜底一次，方便测试或直接构造
    // TopicConfig 的调用者。
    Ros2Qos qos;
    qos.reliability = topic.reliability();
    if (qos.reliability == RELIABILITY_UNSPECIFIED)
    {
        qos.reliability = RELIABLE;
    }
    qos.durability = topic.durability();
    if (qos.durability == DURABILITY_UNSPECIFIED)
    {
        qos.durability = VOLATILE;
    }
    // rclcpp::QoS 的 history depth 必须至少为 1。
    qos.depth = std::max<uint32_t>(1, topic.depth());
    return qos;
}

bool QosMapper::shouldRetain(const TopicConfig &topic)
{
    // 两种来源都表示“保留最近状态”：DDS 的 TRANSIENT_LOCAL，或配置里显式
    // 打开的 retained。后者允许 VOLATILE topic 也使用 bridge 缓存。
    return topic.retained() || topic.durability() == TRANSIENT_LOCAL;
}

MqQos QosMapper::toMq(const TopicConfig &topic)
{
    // 这是 MVP 的近似映射，不是 DDS -> AMQP 的标准转换：
    // - RELIABLE 需要 bridge 手动 ACK，避免任务消息在 bridge 处理前丢失；
    // - BEST_EFFORT 允许 broker auto_ack，适合只关心最新值的遥测；
    // - durable 跟着 reliable 走，让队列/交换机的拓扑声明可以持久化
    //   （消息本身是否落盘还取决于 publish 的 delivery_mode）。
    MqQos qos;
    const bool reliable = topic.reliability() != BEST_EFFORT;

    qos.auto_ack = !reliable;
    qos.durable = reliable;
    qos.retained = shouldRetain(topic);
    // x-ack-timeout-ms / x-max-delivery-count 不在这里自动注入：它们需要
    // 与 broker 的重投策略一起决定，目前由用户通过 queue_args 显式配置，
    // 或留到拓扑声明阶段统一补默认值。

    // 先复制用户参数，后面的 depth 推断不能覆盖显式配置。
    for (const auto &entry : topic.queue_args())
    {
        qos.queue_args[entry.first] = entry.second;
    }

    // depth 在这里只是一个近似：对 best-effort 遥测队列补一个默认上限，
    // 但用户显式配置的 x-max-length 始终优先。reliable 任务队列不自动截断，
    // 否则 broker 会在 bridge 确认前静默丢弃最旧任务。
    const uint32_t depth = topic.depth();
    if (!reliable && depth > 0 &&
        qos.queue_args.find("x-max-length") == qos.queue_args.end())
    {
        qos.queue_args["x-max-length"] = std::to_string(depth);
    }

    return qos;
}

google::protobuf::Map<std::string, std::string>
QosMapper::toProtobufMap(const std::map<std::string, std::string> &args)
{
    // 逐项复制而不是引用，避免调用方在异步声明前修改 std::map。
    google::protobuf::Map<std::string, std::string> result;
    for (const auto &entry : args)
    {
        result[entry.first] = entry.second;
    }
    return result;
}

} // namespace bridge
} // namespace mq
