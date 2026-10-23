#include "server/mq_exchange.hpp"
#include <gtest/gtest.h>

mq::ExchangeManager::ptr emp;

class ExchangeTest : public testing::Environment
{
public:
    virtual void SetUp() override
    {
        emp = std::make_shared<mq::ExchangeManager>("./data/meta.db");
    }
    virtual void TearDown() override
    {
        //emp->clear();
        std::cout << "最后的清理！！\n";
    }
};

// TEST(exchange_test, insert_test)
// {
//     std::unordered_map<std::string, std::string> map = {{"k1", "v1"}, {"k2", "v2"}};
//     emp->declareExchange("exchange1", mq::ExchangeType::DIRECT, true, false, map);
//     emp->declareExchange("exchange2", mq::ExchangeType::DIRECT, true, false, map);
//     emp->declareExchange("exchange3", mq::ExchangeType::DIRECT, true, false, map);
//     emp->declareExchange("exchange4", mq::ExchangeType::DIRECT, true, false, map);
//     ASSERT_EQ(emp->size(), 4);
// }

TEST(exchange_test, select_test)
{
    ASSERT_EQ(emp->exists("exchange1"), true);
    ASSERT_EQ(emp->exists("exchange2"), false);
    ASSERT_EQ(emp->exists("exchange3"), true);
    ASSERT_EQ(emp->exists("exchange4"), true);
    mq::Exchange::ptr exp = emp->selectExchange("exchange3");
    ASSERT_NE(exp.get(), nullptr);
    ASSERT_EQ(exp->name, "exchange3");
    ASSERT_EQ(exp->durable, true);
    ASSERT_EQ(exp->auto_delete, false);
    ASSERT_EQ(exp->type, mq::ExchangeType::DIRECT);
    // protobuf::Map is unordered, so only check that both key/value pairs exist.
    const std::string args = exp->getArgs();
    ASSERT_NE(args.find("k1=v1&"), std::string::npos);
    ASSERT_NE(args.find("k2=v2&"), std::string::npos);
}

TEST(exchange_test, remove_test)
{
    emp->deleteExchange("exchange2");
    mq::Exchange::ptr exp = emp->selectExchange("exchange2");
    ASSERT_EQ(exp.get(), nullptr);
    ASSERT_EQ(emp->exists("exchange2"), false);
}

int main(int argc, char *argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new ExchangeTest);
    return RUN_ALL_TESTS();
}