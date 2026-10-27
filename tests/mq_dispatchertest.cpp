#include <gtest/gtest.h>
#include "server/mq_dispatcher.hpp"
#include "server/mq_host.hpp"
#include "server/mq_consumer.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// QueueDispatcher 的并发行为测试：
// - 多消费者竞争时消息不重复、分配公平；
// - 消费者断线后 wait-ack 消息按原顺序重新入队；
// - prefetch 限制在途消息数量；
// - ACK 超时后重投并递增 delivery_count。
struct DeliveryRecord {
    std::string consumer_tag;
    std::string message_id;
    uint32_t delivery_count;
};

class DispatcherTest : public testing::Test {
public:
    void SetUp() override {
        _host = std::make_shared<mq::VirtualHost>(
            "dispatcher_host",
            "./data/host1/message/",
            "./data/host1/host1.db");
        _cmp = std::make_shared<mq::ConsumerManager>();
        _pool = std::make_shared<threadpool>();
        _qdispatcher = std::make_shared<mq::QueueDispatcher>(_host, _cmp, _pool);

        google::protobuf::Map<std::string, std::string> empty_map;
        ASSERT_TRUE(_host->declareQueue("dispatcher_compete", false, false, false, empty_map));
        ASSERT_TRUE(_host->declareQueue("dispatcher_requeue", false, false, false, empty_map));
        ASSERT_TRUE(_host->declareQueue("dispatcher_prefetch", false, false, false, empty_map));

        google::protobuf::Map<std::string, std::string> timeout_args;
        timeout_args["x-ack-timeout-ms"] = "1";
        ASSERT_TRUE(_host->declareQueue("dispatcher_timeout", false, false, false, timeout_args));

        _cmp->initQueueConsumer("dispatcher_compete");
        _cmp->initQueueConsumer("dispatcher_requeue");
        _cmp->initQueueConsumer("dispatcher_prefetch");
        _cmp->initQueueConsumer("dispatcher_timeout");
    }

    void TearDown() override {
        _cmp->destroyQueueConsumer("dispatcher_compete");
        _cmp->destroyQueueConsumer("dispatcher_requeue");
        _cmp->destroyQueueConsumer("dispatcher_prefetch");
        _cmp->destroyQueueConsumer("dispatcher_timeout");
        _host->deleteQueue("dispatcher_compete");
        _host->deleteQueue("dispatcher_requeue");
        _host->deleteQueue("dispatcher_prefetch");
        _host->deleteQueue("dispatcher_timeout");
    }

    void publish(const std::string &qname, const std::string &body) {
        mq::BasicProperties properties;
        properties.set_id(body);
        properties.set_delivery_mode(mq::DeliveryMode::UNDURABLE);
        properties.set_routing_key("test");
        ASSERT_TRUE(_host->basicPublish(qname, &properties, body));
    }

public:
    mq::VirtualHost::ptr _host;
    mq::ConsumerManager::ptr _cmp;
    threadpool::ptr _pool;
    mq::QueueDispatcher::ptr _qdispatcher;
};

// 两个消费者竞争同一个队列：消息应该被分摊，且每条只投递一次。
TEST_F(DispatcherTest, two_consumers_share_messages_without_duplicates) {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::string> first;
    std::vector<std::string> second;

    auto first_cb = [&](const std::string, const mq::BasicProperties *, const std::string &body) {
        std::unique_lock<std::mutex> lock(mutex);
        first.push_back(body);
        cv.notify_all();
    };
    auto second_cb = [&](const std::string, const mq::BasicProperties *, const std::string &body) {
        std::unique_lock<std::mutex> lock(mutex);
        second.push_back(body);
        cv.notify_all();
    };

    ASSERT_TRUE(_cmp->create("compete-1", "dispatcher_compete", true, first_cb));
    ASSERT_TRUE(_cmp->create("compete-2", "dispatcher_compete", true, second_cb));

    for (int i = 0; i < 10; ++i) {
        publish("dispatcher_compete", "compete-" + std::to_string(i));
    }
    _qdispatcher->notify("dispatcher_compete");

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
        return first.size() + second.size() >= 10;
    }));

    EXPECT_EQ(first.size(), 5u);
    EXPECT_EQ(second.size(), 5u);

    std::set<std::string> all(first.begin(), first.end());
    all.insert(second.begin(), second.end());
    EXPECT_EQ(all.size(), 10u);
}

// 消费者断线：未 ACK 的消息应该重新入队，并能被后续消费者收到。
TEST_F(DispatcherTest, disconnect_requeues_unacked_messages) {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::string> first;
    std::vector<std::string> second;

    auto first_cb = [&](const std::string, const mq::BasicProperties *, const std::string &body) {
        std::unique_lock<std::mutex> lock(mutex);
        first.push_back(body);
        cv.notify_all();
        // 故意不 ack：模拟消费者收到消息后立刻断线。
    };
    auto second_cb = [&](const std::string, const mq::BasicProperties *, const std::string &body) {
        std::unique_lock<std::mutex> lock(mutex);
        second.push_back(body);
        cv.notify_all();
    };

    ASSERT_TRUE(_cmp->create("requeue-1", "dispatcher_requeue", false, first_cb));
    for (int i = 0; i < 3; ++i) {
        publish("dispatcher_requeue", "requeue-" + std::to_string(i));
    }
    _qdispatcher->notify("dispatcher_requeue");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
            return first.size() >= 3;
        }));
    }
    ASSERT_EQ(_host->waitack_count("dispatcher_requeue"), 3u);
    ASSERT_EQ(_host->getable_count("dispatcher_requeue"), 0u);

    // 模拟 Channel 析构 / basicCancel：先移除消费者，再把它未 ACK 的消息还回队列。
    _cmp->remove("requeue-1", "dispatcher_requeue");
    ASSERT_EQ(_host->requeueByConsumer("dispatcher_requeue", "requeue-1"), 3u);
    ASSERT_EQ(_host->waitack_count("dispatcher_requeue"), 0u);
    ASSERT_EQ(_host->getable_count("dispatcher_requeue"), 3u);

    // 新消费者应该能重新收到这三条消息，顺序保持不变。
    ASSERT_TRUE(_cmp->create("requeue-2", "dispatcher_requeue", true, second_cb));
    _qdispatcher->notify("dispatcher_requeue");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
            return second.size() >= 3;
        }));
    }
    ASSERT_EQ(second.size(), 3u);
    EXPECT_EQ(second[0], "requeue-0");
    EXPECT_EQ(second[1], "requeue-1");
    EXPECT_EQ(second[2], "requeue-2");
    EXPECT_EQ(_host->getable_count("dispatcher_requeue"), 0u);
    EXPECT_EQ(_host->waitack_count("dispatcher_requeue"), 0u);
}

// prefetch=1：两个消费者各自最多持有 1 条未 ACK 消息。
TEST_F(DispatcherTest, prefetch_limits_inflight_messages) {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<DeliveryRecord> delivered;

    auto cb = [&](const std::string tag, const mq::BasicProperties *bp, const std::string &) {
        std::unique_lock<std::mutex> lock(mutex);
        DeliveryRecord record;
        record.consumer_tag = tag;
        record.message_id = bp->id();
        record.delivery_count = bp->delivery_count();
        delivered.push_back(record);
        cv.notify_all();
    };

    ASSERT_TRUE(_cmp->create("prefetch-1", "dispatcher_prefetch", false, cb, 1));
    ASSERT_TRUE(_cmp->create("prefetch-2", "dispatcher_prefetch", false, cb, 1));
    for (int i = 0; i < 4; ++i) {
        publish("dispatcher_prefetch", "prefetch-" + std::to_string(i));
    }
    _qdispatcher->notify("dispatcher_prefetch");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
            return delivered.size() >= 2;
        }));
    }
    // 两个消费者都在等待 ACK，不应该继续投递。
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    {
        std::unique_lock<std::mutex> lock(mutex);
        EXPECT_EQ(delivered.size(), 2u);
    }

    DeliveryRecord first;
    {
        std::unique_lock<std::mutex> lock(mutex);
        first = delivered[0];
    }
    std::string owner = _host->basicAck("dispatcher_prefetch", first.message_id);
    ASSERT_EQ(owner, first.consumer_tag);
    ASSERT_TRUE(_cmp->onAck("dispatcher_prefetch", owner));
    _qdispatcher->notify("dispatcher_prefetch");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
            return delivered.size() >= 3;
        }));
        EXPECT_EQ(delivered.size(), 3u);
    }

    // 清理已经投递但尚未 ACK 的消息。
    for (auto &record : delivered) {
        std::string tag = _host->basicAck("dispatcher_prefetch", record.message_id);
        if (!tag.empty()) {
            _cmp->onAck("dispatcher_prefetch", tag);
        }
    }
}

// ACK 超时：消息重新投递，delivery_count 增加；超时同时释放 prefetch 名额。
TEST_F(DispatcherTest, ack_timeout_requeues_and_redelivers) {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<DeliveryRecord> delivered;

    auto cb = [&](const std::string tag, const mq::BasicProperties *bp, const std::string &) {
        std::unique_lock<std::mutex> lock(mutex);
        DeliveryRecord record;
        record.consumer_tag = tag;
        record.message_id = bp->id();
        record.delivery_count = bp->delivery_count();
        delivered.push_back(record);
        cv.notify_all();
    };

    ASSERT_TRUE(_cmp->create("timeout-1", "dispatcher_timeout", false, cb));
    publish("dispatcher_timeout", "timeout-body");
    _qdispatcher->notify("dispatcher_timeout");

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
            return delivered.size() >= 1;
        }));
    }

    // 队列配置了 1ms 超时；传入一个更晚的时间让扫描确定命中。
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    _qdispatcher->checkAckTimeout(mq::mqNowMs() + 1000);

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&]() {
            return delivered.size() >= 2;
        }));
        ASSERT_EQ(delivered[1].delivery_count, 2u);
        ASSERT_EQ(delivered[1].message_id, delivered[0].message_id);
    }

    std::string owner = _host->basicAck("dispatcher_timeout", delivered[1].message_id);
    ASSERT_EQ(owner, delivered[1].consumer_tag);
    ASSERT_TRUE(_cmp->onAck("dispatcher_timeout", owner));
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
