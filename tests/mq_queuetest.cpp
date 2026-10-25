#include "server/mq_queue.hpp"
#include <gtest/gtest.h>

mq::MsgQueueManager::ptr mqmp;

class QueueTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            mqmp = std::make_shared<mq::MsgQueueManager>("./data/meta.db");

            // The committed fixture only contains exchanges and bindings.
            // Create the queues this test expects so it is self-contained.
            google::protobuf::Map<std::string, std::string> args;
            args["k1"] = "v1";
            mqmp->declareQueue("queue3", true, false, false, args);
            mqmp->declareQueue("queue4", true, false, false, args);
        }
        virtual void TearDown() override {
            mqmp->clear();
        }
};

// TEST(queue_test, insert_test) {
//     std::unordered_map<std::string, std::string> map = {{"k1", "v1"}};
//     mqmp->declareQueue("queue1", true, false, false, map);
//     mqmp->declareQueue("queue2", true, false, false, map);
//     mqmp->declareQueue("queue3", true, false, false, map);
//     mqmp->declareQueue("queue4", true, false, false, map);
//     ASSERT_EQ(mqmp->size(), 4);
// }

TEST(queue_test, select_test) {
    ASSERT_EQ(mqmp->exists("queue1"), false);
    ASSERT_EQ(mqmp->exists("queue2"), false);
    ASSERT_EQ(mqmp->exists("queue3"), true);
    ASSERT_EQ(mqmp->exists("queue4"), true);

    mq::MsgQueue::ptr mqp = mqmp->selectQueue("queue3");
    ASSERT_NE(mqp.get(), nullptr);
    ASSERT_EQ(mqp->name, "queue3");
    ASSERT_EQ(mqp->durable, true);
    ASSERT_EQ(mqp->exclusive, false);
    ASSERT_EQ(mqp->auto_delete, false);
    ASSERT_EQ(mqp->getArgs(), std::string("k1=v1&"));
}

TEST(queue_test, parse_max_length_arg_test) {
    google::protobuf::Map<std::string, std::string> args;
    size_t max_length = 0;

    // 未设置 x-max-length
    ASSERT_FALSE(mq::parseQueueMaxLength(args, max_length));

    // 正常正整数
    args["x-max-length"] = "10";
    ASSERT_TRUE(mq::parseQueueMaxLength(args, max_length));
    ASSERT_EQ(max_length, 10);

    // 0 表示不限制
    args["x-max-length"] = "0";
    ASSERT_FALSE(mq::parseQueueMaxLength(args, max_length));

    // 负数、非数字、尾随字符都按非法处理
    args["x-max-length"] = "-1";
    ASSERT_FALSE(mq::parseQueueMaxLength(args, max_length));
    args["x-max-length"] = "abc";
    ASSERT_FALSE(mq::parseQueueMaxLength(args, max_length));
    args["x-max-length"] = "12x";
    ASSERT_FALSE(mq::parseQueueMaxLength(args, max_length));
}

TEST(queue_test, declare_max_length_update_test) {
    google::protobuf::Map<std::string, std::string> args;
    args["x-max-length"] = "5";
    ASSERT_TRUE(mqmp->declareQueue("queue5", true, false, false, args));

    mq::MsgQueue::ptr q = mqmp->selectQueue("queue5");
    ASSERT_NE(q.get(), nullptr);
    ASSERT_EQ(q->args["x-max-length"], std::string("5"));

    // 已存在的队列再次声明时，合法的新上限应更新到元数据中
    args["x-max-length"] = "2";
    ASSERT_TRUE(mqmp->declareQueue("queue5", true, false, false, args));
    q = mqmp->selectQueue("queue5");
    ASSERT_EQ(q->args["x-max-length"], std::string("2"));

    // 空 args 不能清掉已有上限
    google::protobuf::Map<std::string, std::string> empty_args;
    ASSERT_TRUE(mqmp->declareQueue("queue5", true, false, false, empty_args));
    q = mqmp->selectQueue("queue5");
    ASSERT_EQ(q->args["x-max-length"], std::string("2"));

    mqmp->deleteQueue("queue5");
}

TEST(queue_test, parse_qos_and_dead_letter_args_test) {
    google::protobuf::Map<std::string, std::string> args;
    uint64_t timeout_ms = 0;
    uint32_t max_delivery = 0;

    // 未设置时由调用方使用默认值
    ASSERT_FALSE(mq::parseQueueAckTimeout(args, timeout_ms));
    ASSERT_FALSE(mq::parseQueueMaxDeliveryCount(args, max_delivery));

    // x-ack-timeout-ms 允许 0（关闭超时），正整数有效，负数/非数字非法
    args["x-ack-timeout-ms"] = "0";
    ASSERT_TRUE(mq::parseQueueAckTimeout(args, timeout_ms));
    ASSERT_EQ(timeout_ms, 0u);
    args["x-ack-timeout-ms"] = "1500";
    ASSERT_TRUE(mq::parseQueueAckTimeout(args, timeout_ms));
    ASSERT_EQ(timeout_ms, 1500u);
    args["x-ack-timeout-ms"] = "-1";
    ASSERT_FALSE(mq::parseQueueAckTimeout(args, timeout_ms));

    // x-max-delivery-count 只接受正整数
    args["x-max-delivery-count"] = "3";
    ASSERT_TRUE(mq::parseQueueMaxDeliveryCount(args, max_delivery));
    ASSERT_EQ(max_delivery, 3u);
    args["x-max-delivery-count"] = "0";
    ASSERT_FALSE(mq::parseQueueMaxDeliveryCount(args, max_delivery));

    // 字符串参数原样读取
    ASSERT_EQ(mq::queueArgValue(args, "x-dead-letter-exchange"), std::string());
    args["x-dead-letter-exchange"] = "dlx";
    ASSERT_EQ(mq::queueArgValue(args, "x-dead-letter-exchange"), std::string("dlx"));
}

TEST(queue_test, declare_qos_args_update_test) {
    google::protobuf::Map<std::string, std::string> args;
    args["x-ack-timeout-ms"] = "2000";
    args["x-max-delivery-count"] = "2";
    args["x-dead-letter-exchange"] = "dlx";
    args["x-dead-letter-routing-key"] = "dead";
    ASSERT_TRUE(mqmp->declareQueue("queue_qos", true, false, false, args));

    mq::MsgQueue::ptr q = mqmp->selectQueue("queue_qos");
    ASSERT_NE(q.get(), nullptr);
    ASSERT_EQ(q->args["x-ack-timeout-ms"], std::string("2000"));
    ASSERT_EQ(q->args["x-max-delivery-count"], std::string("2"));
    ASSERT_EQ(q->args["x-dead-letter-exchange"], std::string("dlx"));

    // 已存在队列显式更新参数，空 args 不会清掉旧配置
    args["x-max-delivery-count"] = "5";
    ASSERT_TRUE(mqmp->declareQueue("queue_qos", true, false, false, args));
    google::protobuf::Map<std::string, std::string> empty_args;
    ASSERT_TRUE(mqmp->declareQueue("queue_qos", true, false, false, empty_args));
    q = mqmp->selectQueue("queue_qos");
    ASSERT_EQ(q->args["x-max-delivery-count"], std::string("5"));
    ASSERT_EQ(q->args["x-ack-timeout-ms"], std::string("2000"));

    mqmp->deleteQueue("queue_qos");
}

TEST(queue_test, remove_test) {
    mqmp->deleteQueue("queue3");
    ASSERT_EQ(mqmp->exists("queue3"), false);
}

int main(int argc,char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new QueueTest);
    return RUN_ALL_TESTS();
}