#ifndef __M_WORKER_H__
#define __M_WORKER_H__
#include "muduo/net/EventLoopThread.h"
#include "common/mq_logger.hpp"
#include "common/mq_helper.hpp"
#include "common/mq_threadpool.hpp"
namespace mq
{
    // 客户端异步执行环境：
    // - loopthread：muduo TcpClient 的 IO 线程，负责收发和解析响应；
    // - pool：消费回调线程池，用户可以在这里做耗时处理或同步 ACK/Reject。
    // 网络线程与回调线程分离，避免慢消费者阻塞心跳/其他信道响应。
    class AsyncWorker
    {
    public:
        using ptr = std::shared_ptr<AsyncWorker>;
        muduo::net::EventLoopThread loopthread;
        threadpool pool;
    };
}
#endif