#include <gtest/gtest.h>
#include "server/mq_host.hpp"

// 单元测试
class HostTest : public testing::Test {
    public:
        void SetUp() override {
            google::protobuf::Map<std::string, std::string> empty_map;
            _host = std::make_shared<mq::VirtualHost>("host1", "./data/host1/message/", "./data/host1/host1.db");
            _host->declareExchange("exchange1", mq::ExchangeType::DIRECT, true, false, empty_map);
            _host->declareExchange("exchange2", mq::ExchangeType::DIRECT, true, false, empty_map);
            _host->declareExchange("exchange3", mq::ExchangeType::DIRECT, true, false, empty_map);

            _host->declareQueue("queue1", true, false, false, empty_map);
            _host->declareQueue("queue2", true, false, false, empty_map);
            _host->declareQueue("queue3", true, false, false, empty_map);

            _host->bind("exchange1", "queue1", "news.music.#");
            _host->bind("exchange1", "queue2", "news.music.#");
            _host->bind("exchange1", "queue3", "news.music.#");
            
            _host->bind("exchange2", "queue1", "news.music.#");
            _host->bind("exchange2", "queue2", "news.music.#");
            _host->bind("exchange2", "queue3", "news.music.#");

            _host->bind("exchange3", "queue1", "news.music.#");
            _host->bind("exchange3", "queue2", "news.music.#");
            _host->bind("exchange3", "queue3", "news.music.#");

            _host->basicPublish("queue1", nullptr, "Hello World-1");
            _host->basicPublish("queue1", nullptr, "Hello World-2");
            _host->basicPublish("queue1", nullptr, "Hello World-3");
            
            _host->basicPublish("queue2", nullptr, "Hello World-1");
            _host->basicPublish("queue2", nullptr, "Hello World-2");
            _host->basicPublish("queue2", nullptr, "Hello World-3");
            
            _host->basicPublish("queue3", nullptr, "Hello World-1");
            _host->basicPublish("queue3", nullptr, "Hello World-2");
            _host->basicPublish("queue3", nullptr, "Hello World-3");
        }
        void TearDown() override {
            _host->clear();
        }
    public:
        mq::VirtualHost::ptr _host;
};


TEST_F(HostTest, init_test) {
    ASSERT_EQ(_host->existsExchange("exchange1"), true);
    ASSERT_EQ(_host->existsExchange("exchange2"), true);
    ASSERT_EQ(_host->existsExchange("exchange3"), true);
    
    ASSERT_EQ(_host->existsQueue("queue1"), true);
    ASSERT_EQ(_host->existsQueue("queue2"), true);
    ASSERT_EQ(_host->existsQueue("queue3"), true);
    
    ASSERT_EQ(_host->existsBinding("exchange1", "queue1"), true);
    ASSERT_EQ(_host->existsBinding("exchange1", "queue2"), true);
    ASSERT_EQ(_host->existsBinding("exchange1", "queue3"), true);
    
    ASSERT_EQ(_host->existsBinding("exchange2", "queue1"), true);
    ASSERT_EQ(_host->existsBinding("exchange2", "queue2"), true);
    ASSERT_EQ(_host->existsBinding("exchange2", "queue3"), true);

    ASSERT_EQ(_host->existsBinding("exchange3", "queue1"), true);
    ASSERT_EQ(_host->existsBinding("exchange3", "queue2"), true);
    ASSERT_EQ(_host->existsBinding("exchange3", "queue3"), true);

    mq::MessagePtr msg1 = _host->basicConsume("queue1");
    ASSERT_EQ(msg1->payload().body(), std::string("Hello World-1"));
    mq::MessagePtr msg2 = _host->basicConsume("queue1");
    ASSERT_EQ(msg2->payload().body(), std::string("Hello World-2"));
    mq::MessagePtr msg3 = _host->basicConsume("queue1");
    ASSERT_EQ(msg3->payload().body(), std::string("Hello World-3"));
    mq::MessagePtr msg4 = _host->basicConsume("queue1");
    ASSERT_EQ(msg4.get(), nullptr);
}


TEST_F(HostTest, max_length) {
    google::protobuf::Map<std::string, std::string> args;
    args["x-max-length"] = "2";
    ASSERT_TRUE(_host->declareQueue("queue_max", true, false, false, args));

    ASSERT_TRUE(_host->basicPublish("queue_max", nullptr, "max-1"));
    ASSERT_TRUE(_host->basicPublish("queue_max", nullptr, "max-2"));
    ASSERT_TRUE(_host->basicPublish("queue_max", nullptr, "max-3"));

    ASSERT_EQ(_host->getable_count("queue_max"), 2);

    mq::MessagePtr msg1 = _host->basicConsume("queue_max");
    ASSERT_NE(msg1.get(), nullptr);
    ASSERT_EQ(msg1->payload().body(), std::string("max-2"));
    _host->basicAck("queue_max", msg1->payload().properties().id());

    mq::MessagePtr msg2 = _host->basicConsume("queue_max");
    ASSERT_NE(msg2.get(), nullptr);
    ASSERT_EQ(msg2->payload().body(), std::string("max-3"));
    _host->basicAck("queue_max", msg2->payload().properties().id());

    ASSERT_EQ(_host->getable_count("queue_max"), 0);
    _host->deleteQueue("queue_max");
}

TEST_F(HostTest, max_length_recovery) {
    google::protobuf::Map<std::string, std::string> args;
    args["x-max-length"] = "2";
    ASSERT_TRUE(_host->declareQueue("queue_recover", true, false, false, args));

    ASSERT_TRUE(_host->basicPublish("queue_recover", nullptr, "recover-1"));
    ASSERT_TRUE(_host->basicPublish("queue_recover", nullptr, "recover-2"));
    ASSERT_TRUE(_host->basicPublish("queue_recover", nullptr, "recover-3"));
    ASSERT_TRUE(_host->basicPublish("queue_recover", nullptr, "recover-4"));
    ASSERT_EQ(_host->getable_count("queue_recover"), 2);

    // 重新构造一个 VirtualHost，模拟服务重启：上限应从队列元数据中恢复
    mq::VirtualHost::ptr recovered =
        std::make_shared<mq::VirtualHost>("host_recover", "./data/host1/message/", "./data/host1/host1.db");
    ASSERT_EQ(recovered->getable_count("queue_recover"), 2);

    mq::MessagePtr msg1 = recovered->basicConsume("queue_recover");
    ASSERT_NE(msg1.get(), nullptr);
    ASSERT_EQ(msg1->payload().body(), std::string("recover-3"));
    recovered->basicAck("queue_recover", msg1->payload().properties().id());

    mq::MessagePtr msg2 = recovered->basicConsume("queue_recover");
    ASSERT_NE(msg2.get(), nullptr);
    ASSERT_EQ(msg2->payload().body(), std::string("recover-4"));
    recovered->basicAck("queue_recover", msg2->payload().properties().id());
}

TEST_F(HostTest, durable_ack_after_delivery_count_change) {
    google::protobuf::Map<std::string, std::string> empty;
    ASSERT_TRUE(_host->declareQueue("queue_durable_ack", true, false, false, empty));

    mq::BasicProperties properties;
    properties.set_id("durable-ack-msg");
    properties.set_delivery_mode(mq::DeliveryMode::DURABLE);
    properties.set_routing_key("test");
    ASSERT_TRUE(_host->basicPublish("queue_durable_ack", &properties, "durable-body"));

    mq::MessagePtr msg = _host->basicConsume("queue_durable_ack", "durable-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(msg->payload().properties().delivery_count(), 1u);
    _host->basicAck("queue_durable_ack", msg->payload().properties().id());
    ASSERT_EQ(_host->getable_count("queue_durable_ack"), 0u);

    // delivery_count 在投递时从 0 变成 1，ACK 仍然必须正确地把落盘消息标记为无效。
    mq::VirtualHost::ptr recovered =
        std::make_shared<mq::VirtualHost>("host_recover_ack", "./data/host1/message/",
                                          "./data/host1/host1.db");
    EXPECT_EQ(recovered->getable_count("queue_durable_ack"), 0u);

    recovered->deleteQueue("queue_durable_ack");
}

TEST_F(HostTest, remove_exchange) {
    _host->deleteExchange("exchange1");
    ASSERT_EQ(_host->existsBinding("exchange1", "queue1"), false);
    ASSERT_EQ(_host->existsBinding("exchange1", "queue2"), false);
    ASSERT_EQ(_host->existsBinding("exchange1", "queue3"), false);
}

TEST_F(HostTest, remove_queue) {
    _host->deleteQueue("queue1");
    ASSERT_EQ(_host->existsBinding("exchange1", "queue1"), false);
    ASSERT_EQ(_host->existsBinding("exchange2", "queue1"), false);
    ASSERT_EQ(_host->existsBinding("exchange3", "queue1"), false);
    
    mq::MessagePtr msg1 = _host->basicConsume("queue1");
    ASSERT_EQ(msg1.get(), nullptr);
}


TEST_F(HostTest, ack_message) {
    mq::MessagePtr msg1 = _host->basicConsume("queue1");
    ASSERT_EQ(msg1->payload().body(), std::string("Hello World-1"));
    _host->basicAck(std::string("queue1"), msg1->payload().properties().id());

    mq::MessagePtr msg2 = _host->basicConsume("queue1");
    ASSERT_EQ(msg2->payload().body(), std::string("Hello World-2"));
    _host->basicAck(std::string("queue1"), msg2->payload().properties().id());

    mq::MessagePtr msg3 = _host->basicConsume("queue1");
    ASSERT_EQ(msg3->payload().body(), std::string("Hello World-3"));
    _host->basicAck(std::string("queue1"), msg3->payload().properties().id());

}



TEST_F(HostTest, reject_requeue_increments_delivery_count) {
    google::protobuf::Map<std::string, std::string> args;
    args["x-max-delivery-count"] = "3";
    ASSERT_TRUE(_host->declareQueue("queue_reject", false, false, false, args));

    mq::BasicProperties properties;
    properties.set_id("reject-msg");
    properties.set_delivery_mode(mq::DeliveryMode::UNDURABLE);
    properties.set_routing_key("test");
    ASSERT_TRUE(_host->basicPublish("queue_reject", &properties, "reject-body"));

    mq::MessagePtr msg = _host->basicConsume("queue_reject", "reject-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(msg->payload().properties().delivery_count(), 1u);

    mq::VirtualHost::RejectResult result =
        _host->basicReject("queue_reject", "reject-msg", true);
    ASSERT_TRUE(result.found);
    EXPECT_EQ(result.consumer_tag, "reject-consumer");
    EXPECT_TRUE(result.notify_queues.empty());
    ASSERT_EQ(_host->waitack_count("queue_reject"), 0u);
    ASSERT_EQ(_host->getable_count("queue_reject"), 1u);

    // 重新投递后 delivery_count 增加，供最大投递次数判断使用。
    msg = _host->basicConsume("queue_reject", "reject-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(msg->payload().properties().delivery_count(), 2u);
    _host->basicAck("queue_reject", msg->payload().properties().id());

    _host->deleteQueue("queue_reject");
}

TEST_F(HostTest, ack_timeout_requeues_message) {
    google::protobuf::Map<std::string, std::string> args;
    args["x-ack-timeout-ms"] = "1";
    ASSERT_TRUE(_host->declareQueue("queue_timeout", false, false, false, args));

    mq::BasicProperties properties;
    properties.set_id("timeout-msg");
    properties.set_delivery_mode(mq::DeliveryMode::UNDURABLE);
    properties.set_routing_key("test");
    ASSERT_TRUE(_host->basicPublish("queue_timeout", &properties, "timeout-body"));

    mq::MessagePtr msg = _host->basicConsume("queue_timeout", "timeout-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(_host->waitack_count("queue_timeout"), 1u);

    std::vector<mq::VirtualHost::TimeoutResult> results =
        _host->requeueExpired(mq::mqNowMs() + 1000);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].queue_name, "queue_timeout");
    ASSERT_EQ(results[0].consumer_tags.size(), 1u);
    EXPECT_EQ(results[0].consumer_tags[0], "timeout-consumer");
    ASSERT_EQ(_host->waitack_count("queue_timeout"), 0u);
    ASSERT_EQ(_host->getable_count("queue_timeout"), 1u);

    msg = _host->basicConsume("queue_timeout", "timeout-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(msg->payload().properties().delivery_count(), 2u);
    _host->basicAck("queue_timeout", msg->payload().properties().id());

    _host->deleteQueue("queue_timeout");
}

TEST_F(HostTest, dead_letter_after_max_delivery_count) {
    google::protobuf::Map<std::string, std::string> empty;
    ASSERT_TRUE(_host->declareExchange("dlx", mq::ExchangeType::DIRECT,
                                       false, false, empty));
    ASSERT_TRUE(_host->declareQueue("queue_dlq", false, false, false, empty));
    ASSERT_TRUE(_host->bind("dlx", "queue_dlq", "dead"));

    google::protobuf::Map<std::string, std::string> args;
    args["x-dead-letter-exchange"] = "dlx";
    args["x-dead-letter-routing-key"] = "dead";
    args["x-max-delivery-count"] = "1";
    ASSERT_TRUE(_host->declareQueue("queue_dlx_src", false, false, false, args));

    mq::BasicProperties properties;
    properties.set_id("dlx-msg");
    properties.set_delivery_mode(mq::DeliveryMode::UNDURABLE);
    properties.set_routing_key("original");
    ASSERT_TRUE(_host->basicPublish("queue_dlx_src", &properties, "dlx-body"));

    mq::MessagePtr msg = _host->basicConsume("queue_dlx_src", "dlx-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(msg->payload().properties().delivery_count(), 1u);

    // 最大投递次数为 1：本次 requeue 请求会转为死信，而不是再次入队。
    mq::VirtualHost::RejectResult result =
        _host->basicReject("queue_dlx_src", "dlx-msg", true);
    ASSERT_TRUE(result.found);
    ASSERT_EQ(result.notify_queues.size(), 1u);
    EXPECT_EQ(result.notify_queues[0], "queue_dlq");
    ASSERT_EQ(_host->getable_count("queue_dlx_src"), 0u);
    ASSERT_EQ(_host->waitack_count("queue_dlx_src"), 0u);
    ASSERT_EQ(_host->getable_count("queue_dlq"), 1u);

    mq::MessagePtr dead = _host->basicConsume("queue_dlq", "dlq-consumer");
    ASSERT_NE(dead.get(), nullptr);
    EXPECT_EQ(dead->payload().body(), std::string("dlx-body"));
    EXPECT_EQ(dead->payload().properties().id(), std::string("dlx-msg"));
    EXPECT_EQ(dead->payload().properties().delivery_count(), 1u);
    _host->basicAck("queue_dlq", dead->payload().properties().id());

    _host->deleteQueue("queue_dlx_src");
    _host->deleteQueue("queue_dlq");
    _host->deleteExchange("dlx");
}

TEST_F(HostTest, ack_timeout_dead_letters_after_max_delivery_count) {
    google::protobuf::Map<std::string, std::string> empty;
    ASSERT_TRUE(_host->declareExchange("timeout_dlx", mq::ExchangeType::DIRECT,
                                       false, false, empty));
    ASSERT_TRUE(_host->declareQueue("queue_timeout_dlq", false, false, false, empty));
    ASSERT_TRUE(_host->bind("timeout_dlx", "queue_timeout_dlq", "dead"));

    google::protobuf::Map<std::string, std::string> args;
    args["x-ack-timeout-ms"] = "1";
    args["x-max-delivery-count"] = "1";
    args["x-dead-letter-exchange"] = "timeout_dlx";
    args["x-dead-letter-routing-key"] = "dead";
    ASSERT_TRUE(_host->declareQueue("queue_timeout_src", false, false, false, args));

    mq::BasicProperties properties;
    properties.set_id("timeout-dlx-msg");
    properties.set_delivery_mode(mq::DeliveryMode::UNDURABLE);
    properties.set_routing_key("original");
    ASSERT_TRUE(_host->basicPublish("queue_timeout_src", &properties,
                                    "timeout-dlx-body"));

    mq::MessagePtr msg = _host->basicConsume("queue_timeout_src", "timeout-dlx-consumer");
    ASSERT_NE(msg.get(), nullptr);
    ASSERT_EQ(msg->payload().properties().delivery_count(), 1u);

    std::vector<mq::VirtualHost::TimeoutResult> results =
        _host->requeueExpired(mq::mqNowMs() + 1000);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].queue_name, "queue_timeout_src");
    ASSERT_EQ(results[0].notify_queues.size(), 1u);
    EXPECT_EQ(results[0].notify_queues[0], "queue_timeout_dlq");
    ASSERT_EQ(_host->getable_count("queue_timeout_src"), 0u);
    ASSERT_EQ(_host->waitack_count("queue_timeout_src"), 0u);
    ASSERT_EQ(_host->getable_count("queue_timeout_dlq"), 1u);

    mq::MessagePtr dead = _host->basicConsume("queue_timeout_dlq", "timeout-dlq-consumer");
    ASSERT_NE(dead.get(), nullptr);
    EXPECT_EQ(dead->payload().body(), std::string("timeout-dlx-body"));
    EXPECT_EQ(dead->payload().properties().delivery_count(), 1u);
    _host->basicAck("queue_timeout_dlq", dead->payload().properties().id());

    _host->deleteQueue("queue_timeout_src");
    _host->deleteQueue("queue_timeout_dlq");
    _host->deleteExchange("timeout_dlx");
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}