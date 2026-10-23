#ifndef __M_WORKER_H__
#define __M_WORKER_H__
#include "muduo/net/EventLoopThread.h"
#include "common/mq_logger.hpp"
#include "common/mq_helper.hpp"
#include "common/mq_threadpool.hpp"
namespace mq
{
    class AsyncWorker
    {
    public:
        using ptr = std::shared_ptr<AsyncWorker>;
        muduo::net::EventLoopThread loopthread;
        threadpool pool;
    };
}
#endif