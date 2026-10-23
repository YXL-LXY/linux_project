#include "server/mq_connection.hpp"

int main()
{
    mq::ConnectionManager::ptr cmp = std::make_shared<mq::ConnectionManager>();
    cmp->newConnection(std::make_shared<mq::VirtualHost>("host1", "./data/host1/message/", "./data/host1/host1.db"),
        std::make_shared<mq::ConsumerManager>(),
        mq::ProtobufCodecPtr(),
        muduo::net::TcpConnectionPtr(),
        threadpool::ptr());
    return 0;
}