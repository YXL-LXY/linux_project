#include "server/mq_message.hpp"
#include <gtest/gtest.h>

mq::MessageManager::ptr mmp;

class MessageTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            mmp = std::make_shared<mq::MessageManager>("./data/message/");
            mmp->initQueueMessage("queue1");
        }
        virtual void TearDown() override {
            //mmp->clear();
        }
};

//新增消息测试：新增消息，然后观察可获取消息数量，以及持久化消息数量
TEST(message_test, insert_test) {
    mq::BasicProperties properties;
    properties.set_id(mq::UUIDHelper::uuid());
    properties.set_delivery_mode(mq::DeliveryMode::DURABLE);
    properties.set_routing_key("news.music.pop");
    mmp->insert("queue1", &properties, "Hello World-1", true);
    mmp->insert("queue1", nullptr, "Hello World-2", true);
    mmp->insert("queue1", nullptr, "Hello World-3", true);
    mmp->insert("queue1", nullptr, "Hello World-4", true);
    mmp->insert("queue1", nullptr, "Hello World-5", false);
    ASSERT_EQ(mmp->getable_count("queue1"), 5);
    ASSERT_EQ(mmp->total_count("queue1"), 4);
    ASSERT_EQ(mmp->durable_count("queue1"), 4);
    ASSERT_EQ(mmp->waitack_count("queue1"), 0);
}

//恢复and获取消息测试：获取一条消息，然后在不进行确认的情况下，查看可获取消息数量，待确认消息数量，以及测试消息获取的顺序
// TEST(message_test, select_test) {
//     ASSERT_EQ(mmp->getable_count("queue1"), 4);
//     mq::MessagePtr msg1 = mmp->front("queue1");
//     ASSERT_NE(msg1.get(), nullptr);
//     ASSERT_EQ(msg1->payload().body(), std::string("Hello World-1"));
//     ASSERT_EQ(mmp->getable_count("queue1"), 3);
//     ASSERT_EQ(mmp->waitack_count("queue1"), 1);

//     mq::MessagePtr msg2 = mmp->front("queue1");
//     ASSERT_NE(msg2.get(), nullptr);
//     ASSERT_EQ(msg2->payload().body(), std::string("Hello World-2"));
//     ASSERT_EQ(mmp->getable_count("queue1"), 2);
//     ASSERT_EQ(mmp->waitack_count("queue1"), 2);

//     mq::MessagePtr msg3 = mmp->front("queue1");
//     ASSERT_NE(msg3.get(), nullptr);
//     ASSERT_EQ(msg3->payload().body(), std::string("Hello World-3"));
//     ASSERT_EQ(mmp->getable_count("queue1"), 1);
//     ASSERT_EQ(mmp->waitack_count("queue1"), 3);

//     mq::MessagePtr msg4 = mmp->front("queue1");
//     ASSERT_NE(msg4.get(), nullptr);
//     ASSERT_EQ(msg4->payload().body(), std::string("Hello World-4"));
//     ASSERT_EQ(mmp->getable_count("queue1"), 0);
//     ASSERT_EQ(mmp->waitack_count("queue1"), 4);

//     mq::MessagePtr msg5 = mmp->front("queue1");
//     ASSERT_EQ(msg5.get(), nullptr);
// }

//删除消息测试：确认一条消息，查看持久化消息数量，待确认消息数量
// TEST(message_test, delete_test) {
//     ASSERT_EQ(mmp->getable_count("queue1"), 5);
//     mq::MessagePtr msg1 = mmp->front("queue1");
//     ASSERT_NE(msg1.get(), nullptr);
//     ASSERT_EQ(msg1->payload().body(), std::string("Hello World-1"));
//     ASSERT_EQ(mmp->getable_count("queue1"), 4);
//     ASSERT_EQ(mmp->waitack_count("queue1"), 1);
//     mmp->ack("queue1", msg1->payload().properties().id());
//     ASSERT_EQ(mmp->waitack_count("queue1"), 0);
//     ASSERT_EQ(mmp->durable_count("queue1"), 3);
//     ASSERT_EQ(mmp->total_count("queue1"), 4);
// }
// x-max-length 测试：超过上限时丢弃最旧的就绪消息；调小上限立即裁剪；
// 重复声明不带 x-max-length 时不能清掉已有上限。
TEST(message_test, max_length_test) {
    mmp->initQueueMessage("queue_max", 3, true);

    for (int i = 0; i < 5; ++i) {
        mq::BasicProperties properties;
        properties.set_id("max-id-" + std::to_string(i));
        properties.set_delivery_mode(mq::DeliveryMode::DURABLE);
        properties.set_routing_key("test");
        ASSERT_TRUE(mmp->insert("queue_max", &properties,
                                "msg-" + std::to_string(i), true));
    }
    ASSERT_EQ(mmp->getable_count("queue_max"), 3);
    ASSERT_EQ(mmp->durable_count("queue_max"), 3);

    // 保留最新的 msg-2、msg-3、msg-4
    for (int i = 2; i < 5; ++i) {
        mq::MessagePtr msg = mmp->front("queue_max");
        ASSERT_NE(msg.get(), nullptr);
        ASSERT_EQ(msg->payload().body(), "msg-" + std::to_string(i));
        mmp->ack("queue_max", msg->payload().properties().id());
    }
    ASSERT_EQ(mmp->getable_count("queue_max"), 0);

    // 调小上限时立即裁剪
    for (int i = 0; i < 3; ++i) {
        mq::BasicProperties properties;
        properties.set_id("trim-id-" + std::to_string(i));
        properties.set_delivery_mode(mq::DeliveryMode::DURABLE);
        properties.set_routing_key("test");
        ASSERT_TRUE(mmp->insert("queue_max", &properties,
                                "trim-" + std::to_string(i), true));
    }
    ASSERT_EQ(mmp->getable_count("queue_max"), 3);

    mmp->initQueueMessage("queue_max", 1, true);
    ASSERT_EQ(mmp->getable_count("queue_max"), 1);

    // 空 args 重复声明不能清掉已有上限：再插入两条，仍只保留一条
    mmp->initQueueMessage("queue_max", 0, false);
    for (int i = 0; i < 2; ++i) {
        mq::BasicProperties properties;
        properties.set_id("no-clear-id-" + std::to_string(i));
        properties.set_delivery_mode(mq::DeliveryMode::DURABLE);
        properties.set_routing_key("test");
        ASSERT_TRUE(mmp->insert("queue_max", &properties,
                                "no-clear-" + std::to_string(i), true));
    }
    ASSERT_EQ(mmp->getable_count("queue_max"), 1);

    mq::MessagePtr last = mmp->front("queue_max");
    ASSERT_NE(last.get(), nullptr);
    ASSERT_EQ(last->payload().body(), std::string("no-clear-1"));
    mmp->ack("queue_max", last->payload().properties().id());

    mmp->destroyQueueMessage("queue_max");
}

// 断线重新入队测试：只把指定消费者的 wait-ack 消息放回队列，
// 其他消费者的未确认消息不能受影响，并且恢复原来的投递顺序。
TEST(message_test, requeue_by_consumer_test) {
    mmp->initQueueMessage("queue_owner");

    for (int i = 0; i < 4; ++i) {
        mq::BasicProperties properties;
        properties.set_id("owner-id-" + std::to_string(i));
        properties.set_delivery_mode(mq::DeliveryMode::UNDURABLE);
        properties.set_routing_key("test");
        ASSERT_TRUE(mmp->insert("queue_owner", &properties,
                                "owner-" + std::to_string(i), false));
    }

    mq::MessagePtr msg1 = mmp->front("queue_owner", "consumer-1");
    mq::MessagePtr msg2 = mmp->front("queue_owner", "consumer-1");
    mq::MessagePtr msg3 = mmp->front("queue_owner", "consumer-2");
    ASSERT_NE(msg1.get(), nullptr);
    ASSERT_NE(msg2.get(), nullptr);
    ASSERT_NE(msg3.get(), nullptr);
    ASSERT_EQ(mmp->getable_count("queue_owner"), 1);
    ASSERT_EQ(mmp->waitack_count("queue_owner"), 3);

    // consumer-1 断线：只重入它自己的 msg1、msg2。
    ASSERT_EQ(mmp->requeueByConsumer("queue_owner", "consumer-1"), 2);
    ASSERT_EQ(mmp->getable_count("queue_owner"), 3);
    ASSERT_EQ(mmp->waitack_count("queue_owner"), 1);

    // consumer-2 确认自己的消息后，它的 wait-ack 才清空。
    mmp->ack("queue_owner", msg3->payload().properties().id());
    ASSERT_EQ(mmp->waitack_count("queue_owner"), 0);

    mq::MessagePtr requeued1 = mmp->front("queue_owner", "consumer-3");
    mq::MessagePtr requeued2 = mmp->front("queue_owner", "consumer-3");
    ASSERT_NE(requeued1.get(), nullptr);
    ASSERT_NE(requeued2.get(), nullptr);
    ASSERT_EQ(requeued1->payload().body(), std::string("owner-0"));
    ASSERT_EQ(requeued2->payload().body(), std::string("owner-1"));
    mmp->ack("queue_owner", requeued1->payload().properties().id());
    mmp->ack("queue_owner", requeued2->payload().properties().id());

    mq::MessagePtr last = mmp->front("queue_owner", "consumer-3");
    ASSERT_NE(last.get(), nullptr);
    ASSERT_EQ(last->payload().body(), std::string("owner-3"));
    mmp->ack("queue_owner", last->payload().properties().id());

    mmp->destroyQueueMessage("queue_owner");
}

//销毁测试
TEST(message_test, clear) {
    mmp->destroyQueueMessage("queue1");
}

int main(int argc,char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new MessageTest);
    return RUN_ALL_TESTS();
}




