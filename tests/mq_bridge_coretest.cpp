#include "bridge/bridge_config.hpp"
#include "bridge/bridge_runtime.hpp"
#include "bridge/envelope_codec.hpp"
#include "bridge/qos_mapper.hpp"
#include "bridge/retained_store.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <string>

// 这些用例只覆盖不依赖 ROS2 的 bridge 核心：
// - protobuf text 配置解析、默认值和交叉校验；
// - BridgeEnvelope 的版本/序列化；
// - QoS 近似映射；
// - retained store 的容量、TTL 和持久化；
// - BridgeRuntime 启动前的配置校验失败路径。
// ROS2 generic pub/sub 与 MQ adapter 需要单独的集成测试。
namespace
{

const char *kValidConfig = R"(
node_name: "test_bridge"
mq_host: "127.0.0.1"
mq_port: 8085

topics {
    name: "/chatter"
    ros_type: "std_msgs/msg/String"
    direction: MQ_TO_ROS2
    exchange: "exchange1"
    exchange_type: EXCHANGE_TOPIC
    routing_key: "news.music.pop"
    queue: "queue2"
    reliability: RELIABLE
    durability: VOLATILE
    depth: 5
    retained: true
    queue_args { key: "x-ack-timeout-ms" value: "30000" }
}
)";

mq::bridge::BridgeEnvelope makeEnvelope()
{
    mq::bridge::BridgeEnvelope envelope;
    envelope.set_version(1);
    envelope.set_topic("/chatter");
    envelope.set_ros_type("std_msgs/msg/String");
    envelope.set_stamp_ns(123456789);
    envelope.set_source("coretest");
    envelope.set_correlation_id("corr-1");
    envelope.set_reliability(mq::bridge::RELIABLE);
    envelope.set_durability(mq::bridge::VOLATILE);
    envelope.set_depth(10);
    (*envelope.mutable_headers())["frame_id"] = "base_link";

    std::string cdr;
    cdr.push_back('\0');
    cdr.push_back('\x01');
    cdr.push_back('\x02');
    envelope.set_cdr(cdr);
    return envelope;
}

} // namespace

TEST(bridge_config_test, parses_valid_config_and_applies_defaults)
{
    mq::bridge::BridgeConfig config;
    std::string error;
    ASSERT_TRUE(mq::bridge::BridgeConfigLoader::parseFromString(
        kValidConfig, &config, &error)) << error;

    EXPECT_EQ(config.node_name(), "test_bridge");
    ASSERT_EQ(config.topics_size(), 1);
    const mq::bridge::TopicConfig &topic = config.topics(0);
    EXPECT_EQ(topic.name(), "/chatter");
    EXPECT_EQ(topic.direction(), mq::bridge::MQ_TO_ROS2);
    EXPECT_EQ(topic.queue(), "queue2");
    EXPECT_EQ(topic.exchange_type(), mq::bridge::EXCHANGE_TOPIC);
    EXPECT_EQ(topic.depth(), 5);
    EXPECT_TRUE(topic.retained());
    ASSERT_EQ(topic.queue_args().count("x-ack-timeout-ms"), 1u);
    EXPECT_EQ(topic.queue_args().at("x-ack-timeout-ms"), "30000");
}

TEST(bridge_config_test, fills_optional_defaults)
{
    const std::string text = R"(
topics {
    name: "/task"
    ros_type: "std_msgs/msg/String"
    direction: ROS2_TO_MQ
    exchange: "tasks"
}
)";

    mq::bridge::BridgeConfig config;
    std::string error;
    ASSERT_TRUE(mq::bridge::BridgeConfigLoader::parseFromString(text, &config, &error))
        << error;

    EXPECT_EQ(config.node_name(), "mq_bridge");
    EXPECT_EQ(config.mq_host(), "127.0.0.1");
    EXPECT_EQ(config.mq_port(), 8085);
    EXPECT_EQ(config.retained_store().capacity_per_topic(), 1u);
    EXPECT_EQ(config.topics(0).reliability(), mq::bridge::RELIABLE);
    EXPECT_EQ(config.topics(0).durability(), mq::bridge::VOLATILE);
    EXPECT_EQ(config.topics(0).depth(), 10u);
    EXPECT_EQ(config.topics(0).exchange_type(), mq::bridge::EXCHANGE_TOPIC);
}

TEST(bridge_config_test, rejects_conflicting_exchange_types)
{
    const std::string text = R"(
topics {
    name: "/one"
    ros_type: "std_msgs/msg/String"
    direction: ROS2_TO_MQ
    exchange: "shared"
    exchange_type: EXCHANGE_TOPIC
}
topics {
    name: "/two"
    ros_type: "std_msgs/msg/String"
    direction: ROS2_TO_MQ
    exchange: "shared"
    exchange_type: EXCHANGE_DIRECT
}
)";

    mq::bridge::BridgeConfig config;
    std::string error;
    EXPECT_FALSE(mq::bridge::BridgeConfigLoader::parseFromString(text, &config, &error));
    EXPECT_NE(error.find("conflicting exchange_type"), std::string::npos) << error;
}

TEST(bridge_config_test, accepts_direct_and_fanout_exchange_types)
{
    const std::string text = R"(
topics {
    name: "/one"
    ros_type: "std_msgs/msg/String"
    direction: ROS2_TO_MQ
    exchange: "direct_exchange"
    exchange_type: EXCHANGE_DIRECT
    routing_key: "exact"
}
topics {
    name: "/two"
    ros_type: "std_msgs/msg/String"
    direction: ROS2_TO_MQ
    exchange: "fanout_exchange"
    exchange_type: EXCHANGE_FANOUT
}
)";

    mq::bridge::BridgeConfig config;
    std::string error;
    ASSERT_TRUE(mq::bridge::BridgeConfigLoader::parseFromString(text, &config, &error))
        << error;
    EXPECT_EQ(config.topics(0).exchange_type(), mq::bridge::EXCHANGE_DIRECT);
    EXPECT_EQ(config.topics(1).exchange_type(), mq::bridge::EXCHANGE_FANOUT);
}

TEST(bridge_config_test, rejects_missing_mq_to_ros2_queue)
{
    const std::string text = R"(
topics {
    name: "/chatter"
    ros_type: "std_msgs/msg/String"
    direction: MQ_TO_ROS2
}
)";

    mq::bridge::BridgeConfig config;
    std::string error;
    EXPECT_FALSE(mq::bridge::BridgeConfigLoader::parseFromString(text, &config, &error));
    EXPECT_NE(error.find("queue is required"), std::string::npos) << error;
}

TEST(bridge_config_test, rejects_retained_on_ros2_to_mq)
{
    const std::string text = R"(
topics {
    name: "/task"
    ros_type: "std_msgs/msg/String"
    direction: ROS2_TO_MQ
    exchange: "tasks"
    retained: true
}
)";

    mq::bridge::BridgeConfig config;
    std::string error;
    EXPECT_FALSE(mq::bridge::BridgeConfigLoader::parseFromString(text, &config, &error));
    EXPECT_NE(error.find("retained is only valid"), std::string::npos) << error;
}

TEST(bridge_config_test, rejects_duplicate_topic_names)
{
    const std::string text = R"(
topics {
    name: "/chatter"
    ros_type: "std_msgs/msg/String"
    direction: MQ_TO_ROS2
    queue: "q1"
}
topics {
    name: "/chatter"
    ros_type: "std_msgs/msg/String"
    direction: MQ_TO_ROS2
    queue: "q2"
}
)";

    mq::bridge::BridgeConfig config;
    std::string error;
    EXPECT_FALSE(mq::bridge::BridgeConfigLoader::parseFromString(text, &config, &error));
    EXPECT_NE(error.find("duplicate topic name"), std::string::npos) << error;
}

TEST(envelope_codec_test, round_trips_all_fields)
{
    const mq::bridge::BridgeEnvelope original = makeEnvelope();

    std::string bytes;
    std::string error;
    ASSERT_TRUE(mq::bridge::EnvelopeCodec::encode(original, &bytes, &error)) << error;

    mq::bridge::BridgeEnvelope decoded;
    ASSERT_TRUE(mq::bridge::EnvelopeCodec::decode(bytes, &decoded, &error)) << error;
    EXPECT_EQ(decoded.version(), 1u);
    EXPECT_EQ(decoded.topic(), original.topic());
    EXPECT_EQ(decoded.ros_type(), original.ros_type());
    EXPECT_EQ(decoded.stamp_ns(), original.stamp_ns());
    EXPECT_EQ(decoded.cdr(), original.cdr());
    EXPECT_EQ(decoded.source(), original.source());
    EXPECT_EQ(decoded.correlation_id(), original.correlation_id());
    EXPECT_EQ(decoded.reliability(), mq::bridge::RELIABLE);
    EXPECT_EQ(decoded.durability(), mq::bridge::VOLATILE);
    EXPECT_EQ(decoded.depth(), 10u);
    EXPECT_EQ(decoded.headers().at("frame_id"), "base_link");
}

TEST(envelope_codec_test, fills_version_when_zero)
{
    mq::bridge::BridgeEnvelope envelope = makeEnvelope();
    envelope.set_version(0);

    std::string bytes;
    std::string error;
    ASSERT_TRUE(mq::bridge::EnvelopeCodec::encode(envelope, &bytes, &error)) << error;

    mq::bridge::BridgeEnvelope decoded;
    ASSERT_TRUE(mq::bridge::EnvelopeCodec::decode(bytes, &decoded, &error)) << error;
    EXPECT_EQ(decoded.version(), mq::bridge::EnvelopeCodec::kCurrentVersion);
}

TEST(envelope_codec_test, rejects_future_version)
{
    mq::bridge::BridgeEnvelope envelope = makeEnvelope();
    envelope.set_version(2);

    std::string bytes;
    ASSERT_TRUE(envelope.SerializeToString(&bytes));

    mq::bridge::BridgeEnvelope decoded;
    std::string error;
    EXPECT_FALSE(mq::bridge::EnvelopeCodec::decode(bytes, &decoded, &error));
    EXPECT_NE(error.find("unsupported envelope version"), std::string::npos) << error;
}

TEST(envelope_codec_test, accepts_empty_cdr)
{
    mq::bridge::BridgeEnvelope envelope = makeEnvelope();
    envelope.clear_cdr();

    std::string bytes;
    std::string error;
    ASSERT_TRUE(mq::bridge::EnvelopeCodec::encode(envelope, &bytes, &error)) << error;

    mq::bridge::BridgeEnvelope decoded;
    ASSERT_TRUE(mq::bridge::EnvelopeCodec::decode(bytes, &decoded, &error)) << error;
    EXPECT_TRUE(decoded.cdr().empty());
}

TEST(qos_mapper_test, reliable_maps_to_manual_ack)
{
    mq::bridge::TopicConfig topic;
    topic.set_name("/task");
    topic.set_ros_type("std_msgs/msg/String");
    topic.set_direction(mq::bridge::ROS2_TO_MQ);
    topic.set_reliability(mq::bridge::RELIABLE);
    topic.set_durability(mq::bridge::VOLATILE);
    topic.set_depth(10);

    const mq::bridge::MqQos qos = mq::bridge::QosMapper::toMq(topic);
    EXPECT_FALSE(qos.auto_ack);
    EXPECT_TRUE(qos.durable);
    EXPECT_FALSE(qos.retained);
    EXPECT_EQ(qos.queue_args.count("x-max-length"), 0u);
}

TEST(qos_mapper_test, best_effort_maps_to_auto_ack_and_depth_limit)
{
    mq::bridge::TopicConfig topic;
    topic.set_name("/imu/data");
    topic.set_ros_type("sensor_msgs/msg/Imu");
    topic.set_direction(mq::bridge::MQ_TO_ROS2);
    topic.set_reliability(mq::bridge::BEST_EFFORT);
    topic.set_durability(mq::bridge::VOLATILE);
    topic.set_depth(5);

    const mq::bridge::MqQos qos = mq::bridge::QosMapper::toMq(topic);
    EXPECT_TRUE(qos.auto_ack);
    EXPECT_FALSE(qos.durable);
    EXPECT_EQ(qos.queue_args.at("x-max-length"), "5");
}

TEST(qos_mapper_test, explicit_queue_args_win_and_transient_local_retains)
{
    mq::bridge::TopicConfig topic;
    topic.set_name("/state");
    topic.set_ros_type("std_msgs/msg/String");
    topic.set_direction(mq::bridge::MQ_TO_ROS2);
    topic.set_reliability(mq::bridge::BEST_EFFORT);
    topic.set_durability(mq::bridge::TRANSIENT_LOCAL);
    topic.set_depth(5);
    (*topic.mutable_queue_args())["x-max-length"] = "1";

    const mq::bridge::MqQos qos = mq::bridge::QosMapper::toMq(topic);
    EXPECT_EQ(qos.queue_args.at("x-max-length"), "1");
    EXPECT_TRUE(qos.retained);

    const mq::bridge::Ros2Qos ros_qos = mq::bridge::QosMapper::toRos2(topic);
    EXPECT_EQ(ros_qos.reliability, mq::bridge::BEST_EFFORT);
    EXPECT_EQ(ros_qos.durability, mq::bridge::TRANSIENT_LOCAL);
    EXPECT_EQ(ros_qos.depth, 5u);
}

TEST(retained_store_test, capacity_keeps_newest_entries)
{
    mq::bridge::RetainedStore store(2, 0);
    std::string error;

    ASSERT_TRUE(store.put("/state", "one", 1000, &error)) << error;
    ASSERT_TRUE(store.put("/state", "two", 2000, &error)) << error;
    ASSERT_TRUE(store.put("/state", "three", 3000, &error)) << error;
    EXPECT_EQ(store.size(), 2u);

    std::string value;
    uint64_t updated = 0;
    ASSERT_TRUE(store.get("/state", &value, &updated, 3000, &error)) << error;
    EXPECT_EQ(value, "three");
    EXPECT_EQ(updated, 3000u);

    EXPECT_TRUE(store.get("/state", &value, &updated, 3000, &error));
    EXPECT_EQ(value, "three");
    EXPECT_FALSE(store.get("/missing", &value, &updated, 3000, &error));
}

TEST(retained_store_test, ttl_expires_entries)
{
    mq::bridge::RetainedStore store(1, 100);
    std::string error;
    ASSERT_TRUE(store.put("/state", "value", 1000, &error)) << error;

    std::string value;
    uint64_t updated = 0;
    EXPECT_TRUE(store.get("/state", &value, &updated, 1099, &error)) << error;
    EXPECT_FALSE(store.get("/state", &value, &updated, 1100, &error));
    EXPECT_EQ(store.size(), 0u);
}

TEST(retained_store_test, persists_and_reloads)
{
    const std::string path = "./data/bridge-retained-coretest.db";
    std::remove(path.c_str());

    {
        mq::bridge::RetainedStore store(1, 0, path);
        std::string error;
        ASSERT_TRUE(store.put("/state", "persisted", 1234, &error)) << error;
    }

    mq::bridge::RetainedStore restored(1, 0, path);
    std::string error;
    ASSERT_TRUE(restored.load(&error)) << error;

    std::string value;
    uint64_t updated = 0;
    ASSERT_TRUE(restored.get("/state", &value, &updated, 2000, &error)) << error;
    EXPECT_EQ(value, "persisted");
    EXPECT_EQ(updated, 1234u);

    std::remove(path.c_str());
}

TEST(bridge_runtime_test, validates_before_connecting)
{
    // 空配置在 validate() 阶段就会被拒绝，不需要 broker/ROS2；这个用例同时
    // 覆盖 BridgeRuntime 的构造/析构和 start() 失败路径。
    mq::bridge::BridgeRuntime runtime;
    mq::bridge::BridgeConfig config;
    std::string error;
    EXPECT_FALSE(runtime.start(config, &error));
    EXPECT_NE(error.find("at least one topics"), std::string::npos) << error;
    EXPECT_FALSE(runtime.running());
    runtime.stop();
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
