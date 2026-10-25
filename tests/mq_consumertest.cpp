#include "server/mq_consumer.hpp"
#include <gtest/gtest.h>

mq::ConsumerManager::ptr cmp;

class ConsumerTest : public testing::Environment {
    public:
        virtual void SetUp() override {
            cmp = std::make_shared<mq::ConsumerManager>();
            cmp->initQueueConsumer("queue1");
        }
        virtual void TearDown() override {
            cmp->clear();
        }
};

void cb(const std::string &tag, const mq::BasicProperties *bp, const std::string &body) {
    std::cout << tag << " 消费了消息:" << body << std::endl;
}

TEST(consumer_test, insert_test) {
    cmp->create("consumer1", "queue1", false, cb);
    cmp->create("consumer2", "queue1", false, cb);
    cmp->create("consumer3", "queue1", false, cb);

    ASSERT_EQ(cmp->exists("consumer1", "queue1"), true);
    ASSERT_EQ(cmp->exists("consumer2", "queue1"), true);
    ASSERT_EQ(cmp->exists("consumer3", "queue1"), true);
}
TEST(consumer_test, remove_test) {
    cmp->remove("consumer1", "queue1");

    ASSERT_EQ(cmp->exists("consumer1", "queue1"), false);
    ASSERT_EQ(cmp->exists("consumer2", "queue1"), true);
    ASSERT_EQ(cmp->exists("consumer3", "queue1"), true);
}


TEST(consumer_test, choose_test) {
    mq::Consumer::ptr cp = cmp->choose("queue1");
    ASSERT_NE(cp.get(), nullptr);
    ASSERT_EQ(cp->tag, "consumer2");

    
    cp = cmp->choose("queue1");
    ASSERT_NE(cp.get(), nullptr);
    ASSERT_EQ(cp->tag, "consumer3");

    
    cp = cmp->choose("queue1");
    ASSERT_NE(cp.get(), nullptr);
    ASSERT_EQ(cp->tag, "consumer2");
}

// prefetch=1 时，每个消费者最多只能有一条未 ACK 消息；
// ACK 归还名额后，调度器才能继续选择它。
TEST(consumer_test, prefetch_capacity_test) {
    cmp->initQueueConsumer("queue_capacity");
    mq::Consumer::ptr c1 = cmp->create("capacity-1", "queue_capacity", false, cb, 1);
    mq::Consumer::ptr c2 = cmp->create("capacity-2", "queue_capacity", false, cb, 1);
    ASSERT_NE(c1.get(), nullptr);
    ASSERT_NE(c2.get(), nullptr);

    mq::Consumer::ptr first = cmp->choose("queue_capacity");
    ASSERT_NE(first.get(), nullptr);
    EXPECT_EQ(first->tag, "capacity-1");
    first->incUnacked();

    mq::Consumer::ptr second = cmp->choose("queue_capacity");
    ASSERT_NE(second.get(), nullptr);
    EXPECT_EQ(second->tag, "capacity-2");
    second->incUnacked();

    // 两个消费者都到上限，没有可投递对象。
    EXPECT_EQ(cmp->choose("queue_capacity").get(), nullptr);

    // ACK 后 capacity-1 恢复一个名额。
    ASSERT_TRUE(cmp->onAck("queue_capacity", "capacity-1"));
    mq::Consumer::ptr third = cmp->choose("queue_capacity");
    ASSERT_NE(third.get(), nullptr);
    EXPECT_EQ(third->tag, "capacity-1");

    // 动态调大 prefetch 后，capacity-2 也可以继续投递。
    ASSERT_TRUE(cmp->setPrefetch("queue_capacity", "capacity-2", 2));
    mq::Consumer::ptr fourth = cmp->choose("queue_capacity");
    ASSERT_NE(fourth.get(), nullptr);
    EXPECT_EQ(fourth->tag, "capacity-2");

    cmp->onAck("queue_capacity", "capacity-1");
    cmp->onAck("queue_capacity", "capacity-2");
    cmp->onAck("queue_capacity", "capacity-2");
    cmp->destroyQueueConsumer("queue_capacity");
}



int main(int argc,char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ConsumerTest);
    return RUN_ALL_TESTS();
}