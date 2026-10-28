#include "bridge/bridge_config.hpp"

#include <google/protobuf/text_format.h>

#include <fstream>
#include <map>
#include <set>
#include <sstream>

namespace mq
{
namespace bridge
{

namespace
{

// error 允许调用者传 nullptr；这样内部校验函数不用到处判空。
void setError(std::string *error, const std::string &message)
{
    if (error != nullptr)
    {
        *error = message;
    }
}

bool isPortValid(uint32_t port)
{
    return port > 0 && port <= 65535;
}

// 校验失败时尽量指出具体 topic。name 为空时退化为下标，避免错误信息里
// 只剩一个空字符串。
std::string topicLabel(const TopicConfig &topic, size_t index)
{
    if (!topic.name().empty())
    {
        return topic.name();
    }
    return "topics[" + std::to_string(index) + "]";
}

} // namespace

// BIDIRECTIONAL 同时包含两个方向，因此不能只做 == 判断。
bool BridgeConfigLoader::includesMqToRos2(BridgeDirection direction)
{
    return direction == MQ_TO_ROS2 || direction == BIDIRECTIONAL;
}

bool BridgeConfigLoader::includesRos2ToMq(BridgeDirection direction)
{
    return direction == ROS2_TO_MQ || direction == BIDIRECTIONAL;
}

void BridgeConfigLoader::applyDefaults(BridgeConfig *config)
{
    if (config == nullptr)
    {
        return;
    }

    // 全局连接和节点默认值。mq_port 用 0 表示未配置；真正的端口范围检查在
    // validate() 中完成。
    if (config->node_name().empty())
    {
        config->set_node_name("mq_bridge");
    }
    if (config->mq_host().empty())
    {
        config->set_mq_host("127.0.0.1");
    }
    if (config->mq_port() == 0)
    {
        config->set_mq_port(8085);
    }

    // retained store 即使 enabled=false 也补容量，避免关闭状态下打印出
    // capacity=0 这种容易误解的计划摘要。
    RetainedStoreConfig *retained = config->mutable_retained_store();
    if (retained->capacity_per_topic() == 0)
    {
        retained->set_capacity_per_topic(1);
    }

    // topic 级默认值：
    // - reliability 缺省为 RELIABLE（MQ 侧对应手动 ACK）；
    // - durability 缺省为 VOLATILE，只有显式 TRANSIENT_LOCAL 才保留状态；
    // - depth 缺省为 10，同时用于 ROS2 history 和 best-effort 的队列上限。
    for (int i = 0; i < config->topics_size(); ++i)
    {
        TopicConfig *topic = config->mutable_topics(i);
        if (topic->reliability() == RELIABILITY_UNSPECIFIED)
        {
            topic->set_reliability(RELIABLE);
        }
        if (topic->durability() == DURABILITY_UNSPECIFIED)
        {
            topic->set_durability(VOLATILE);
        }
        if (topic->depth() == 0)
        {
            topic->set_depth(10);
        }
        // 绝大多数 MQ 路由使用 topic exchange；显式配置 DIRECT/FANOUT 时才
        // 覆盖这个默认值。配置层保留独立枚举，数据面再映射到 mq::ExchangeType。
        if (topic->exchange_type() == EXCHANGE_UNSPECIFIED)
        {
            topic->set_exchange_type(EXCHANGE_TOPIC);
        }
    }
}

bool BridgeConfigLoader::validate(const BridgeConfig &config, std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }

    // 全局字段：节点名、broker 地址和端口。
    if (config.node_name().empty())
    {
        setError(error, "node_name must not be empty");
        return false;
    }
    if (config.mq_host().empty())
    {
        setError(error, "mq_host must not be empty");
        return false;
    }
    if (!isPortValid(config.mq_port()))
    {
        setError(error, "mq_port must be between 1 and 65535");
        return false;
    }
    // 开启 retained store 时容量不能为 0；关闭时容量值不会生效。
    if (config.retained_store().enabled() &&
        config.retained_store().capacity_per_topic() == 0)
    {
        setError(error, "retained_store.capacity_per_topic must be greater than 0");
        return false;
    }
    // bridge 没有“空配置也能启动”的场景，至少需要一条 topic 映射。
    if (config.topics_size() == 0)
    {
        setError(error, "at least one topics entry is required");
        return false;
    }

    // 先检查单字段，再检查字段组合。这里假设 applyDefaults() 已经执行过，
    // 因此 reliability/durability/depth 还处于 UNSPECIFIED/0 就说明调用者
    // 绕过了正常加载流程，直接报错而不是再次补默认值。
    std::set<std::string> topic_names;
    // 同一个 exchange 可能被多个 topic 引用。它们必须使用相同类型，否则
    // 数据面先声明谁就决定路由行为，配置会变得不可预测。
    std::map<std::string, BridgeExchangeType> exchange_types;
    for (int i = 0; i < config.topics_size(); ++i)
    {
        const TopicConfig &topic = config.topics(i);
        const std::string label = topicLabel(topic, static_cast<size_t>(i));

        if (topic.name().empty())
        {
            setError(error, label + ": name must not be empty");
            return false;
        }
        // 同一个 ROS2 topic 只允许一个配置项，否则两个方向/队列的映射会产生
        // 歧义（尤其是 generic publisher 按 topic 名索引时）。
        if (!topic_names.insert(topic.name()).second)
        {
            setError(error, label + ": duplicate topic name");
            return false;
        }
        if (topic.ros_type().empty())
        {
            setError(error, label + ": ros_type must not be empty");
            return false;
        }
        if (topic.direction() == DIRECTION_UNSPECIFIED)
        {
            setError(error, label + ": direction must be set");
            return false;
        }
        // applyDefaults() 会把未填的 exchange_type 补成 EXCHANGE_TOPIC。
        // 这里仍显式检查未知值，避免手工构造的 TopicConfig 绕过配置入口。
        if (topic.exchange_type() != EXCHANGE_DIRECT &&
            topic.exchange_type() != EXCHANGE_FANOUT &&
            topic.exchange_type() != EXCHANGE_TOPIC)
        {
            setError(error, label + ": exchange_type must be DIRECT, FANOUT or TOPIC");
            return false;
        }
        if (!topic.exchange().empty())
        {
            std::map<std::string, BridgeExchangeType>::iterator existing =
                exchange_types.find(topic.exchange());
            if (existing == exchange_types.end())
            {
                exchange_types[topic.exchange()] = topic.exchange_type();
            }
            else if (existing->second != topic.exchange_type())
            {
                setError(error,
                         label + ": exchange " + topic.exchange() +
                             " is declared with conflicting exchange_type");
                return false;
            }
        }
        // MQ -> ROS2 必须知道从哪个队列消费；ROS2 -> MQ 必须知道发到哪个
        // exchange。BIDIRECTIONAL 同时包含两侧，因此两个字段都必需。
        if (includesMqToRos2(topic.direction()) && topic.queue().empty())
        {
            setError(error, label + ": queue is required for MQ_TO_ROS2");
            return false;
        }
        if (includesRos2ToMq(topic.direction()) && topic.exchange().empty())
        {
            setError(error, label + ": exchange is required for ROS2_TO_MQ");
            return false;
        }
        // retained 是“MQ 消息最近值 -> ROS2 transient_local”的概念；只有
        // 从 MQ 到 ROS2 的方向才有意义。
        if (topic.retained() && !includesMqToRos2(topic.direction()))
        {
            setError(error, label + ": retained is only valid for MQ_TO_ROS2");
            return false;
        }
        if (topic.depth() == 0)
        {
            setError(error, label + ": depth must be greater than 0");
            return false;
        }
        if (topic.reliability() == RELIABILITY_UNSPECIFIED)
        {
            setError(error, label + ": reliability must be set");
            return false;
        }
        if (topic.durability() == DURABILITY_UNSPECIFIED)
        {
            setError(error, label + ": durability must be set");
            return false;
        }

        // queue_args 会原样传给 broker；空 key 无法形成合法参数。
        for (const auto &entry : topic.queue_args())
        {
            if (entry.first.empty())
            {
                setError(error, label + ": queue_args key must not be empty");
                return false;
            }
        }
    }

    return true;
}

bool BridgeConfigLoader::parseFromString(const std::string &text,
                                          BridgeConfig *config,
                                          std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (config == nullptr)
    {
        setError(error, "config output must not be null");
        return false;
    }

    // 先解析到局部对象，避免解析/校验失败时污染调用者已有的 config。
    BridgeConfig parsed;
    if (!google::protobuf::TextFormat::ParseFromString(text, &parsed))
    {
        setError(error, "failed to parse bridge config as protobuf text format");
        return false;
    }

    // 顺序不能反：validate() 依赖 applyDefaults() 补好的可靠性/深度等字段。
    applyDefaults(&parsed);
    if (!validate(parsed, error))
    {
        return false;
    }

    // Swap 是 O(1) 的，并且保证成功返回时调用者拿到的是完整的新配置。
    config->Swap(&parsed);
    return true;
}

bool BridgeConfigLoader::loadFromFile(const std::string &path,
                                      BridgeConfig *config,
                                      std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (path.empty())
    {
        setError(error, "config path must not be empty");
        return false;
    }

    // 配置是 UTF-8 文本，但用 binary 方式读取可以避免平台换行转换影响
    // TextFormat 的解析结果。
    std::ifstream input(path.c_str(), std::ios::in | std::ios::binary);
    if (!input.is_open())
    {
        setError(error, "failed to open config file: " + path);
        return false;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();
    if (!input.good() && !input.eof())
    {
        setError(error, "failed to read config file: " + path);
        return false;
    }

    if (!parseFromString(buffer.str(), config, error))
    {
        // 文件级错误补上路径，方便 CLI 用户定位是哪个配置写错了。
        if (error != nullptr)
        {
            *error = path + ": " + *error;
        }
        return false;
    }
    return true;
}

} // namespace bridge
} // namespace mq
