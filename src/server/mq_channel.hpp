#ifndef __M_CHANNEL_H__
#define __M_CHANNEL_H__
#include "muduo/net/TcpConnection.h"
#include "muduo/proto/codec.h"
#include "muduo/proto/dispatcher.h"
#include "common/mq_logger.hpp"
#include "common/mq_helper.hpp"
#include "mq_msg.pb.h"
#include "mq_proto.pb.h"
#include "mq_consumer.hpp"
#include "mq_dispatcher.hpp"
#include "mq_host.hpp"
#include "mq_route.hpp"
#include <memory>

namespace mq
{
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;
    using openChannelRequestPtr = std::shared_ptr<openChannelRequest>;
    using closeChannelRequestPtr = std::shared_ptr<closeChannelRequest>;
    using declareExchangeRequestPtr = std::shared_ptr<declareExchangeRequest>;
    using deleteExchangeRequestPtr = std::shared_ptr<deleteExchangeRequest>;
    using declareQueueRequestPtr = std::shared_ptr<declareQueueRequest>;
    using deleteQueueRequestPtr = std::shared_ptr<deleteQueueRequest>;
    using queueBindRequestPtr = std::shared_ptr<queueBindRequest>;
    using queueUnBindRequestPtr = std::shared_ptr<queueUnBindRequest>;
    using basicPublishRequestPtr = std::shared_ptr<basicPublishRequest>;
    using basicAckRequestPtr = std::shared_ptr<basicAckRequest>;
    using basicQosRequestPtr = std::shared_ptr<basicQosRequest>;
    using basicRejectRequestPtr = std::shared_ptr<basicRejectRequest>;
    using basicConsumeRequestPtr = std::shared_ptr<basicConsumeRequest>;
    using basicCancelRequestPtr = std::shared_ptr<basicCancelRequest>;

    class Channel : public std::enable_shared_from_this<Channel>
    {
    public:
        using ptr = std::shared_ptr<Channel>;
        Channel(const std::string &id,
                const VirtualHost::ptr &host,
                const ConsumerManager::ptr &cmp,
                const ProtobufCodecPtr &codec,
                const muduo::net::TcpConnectionPtr &conn,
                const QueueDispatcher::ptr &qdispatcher) : _cid(id),
                                                           _conn(conn),
                                                           _codec(codec),
                                                           _cmp(cmp),
                                                           _host(host),
                                                           _qdispatcher(qdispatcher),
                                                           _prefetch_count(0)
        {
            DLOG("new Channel: %p", this);
        }
        ~Channel()
        {
            if (_consumer.get() != nullptr)
            {
                // 连接/信道断开时，消费者已经取走但未 ACK 的消息必须还回队列，
                // 否则这些消息会永远留在 wait-ack 里。
                std::string qname = _consumer->qname;
                std::string tag = _consumer->tag;
                _cmp->remove(tag, qname);
                _host->requeueByConsumer(qname, tag);
                if (_qdispatcher)
                {
                    _qdispatcher->notify(qname);
                }
            }
            DLOG("del Channel: %p", this);
        }
        // 交换机的声明与删除
        void declareExchange(const declareExchangeRequestPtr &req)
        {
            bool ret = _host->declareExchange(req->exchange_name(),
                                              req->exchange_type(), req->durable(),
                                              req->auto_delete(), req->args());
            return basicResponse(ret, req->rid(), req->cid());
        }
        void deleteExchange(const deleteExchangeRequestPtr &req)
        {
            _host->deleteExchange(req->exchange_name());
            return basicResponse(true, req->rid(), req->cid());
        }
        // 队列的声明与删除
        void declareQueue(const declareQueueRequestPtr &req)
        {
            bool ret = _host->declareQueue(req->queue_name(),
                                           req->durable(), req->exclusive(),
                                           req->auto_delete(), req->args());
            if (ret == false)
            {
                return basicResponse(false, req->rid(), req->cid());
            }
            _cmp->initQueueConsumer(req->queue_name()); // 初始化队列的消费者管理句柄
            return basicResponse(true, req->rid(), req->cid());
        }
        void deleteQueue(const deleteQueueRequestPtr &req)
        {
            _cmp->destroyQueueConsumer(req->queue_name());
            _host->deleteQueue(req->queue_name());
            return basicResponse(true, req->rid(), req->cid());
        }
        // 队列的绑定与解除绑定
        void queueBind(const queueBindRequestPtr &req)
        {
            bool ret = _host->bind(req->exchange_name(),
                                   req->queue_name(), req->binding_key());
            return basicResponse(ret, req->rid(), req->cid());
        }
        void queueUnBind(const queueUnBindRequestPtr &req)
        {
            _host->unBind(req->exchange_name(), req->queue_name());
            return basicResponse(true, req->rid(), req->cid());
        }
        // 消息的发布
        void basicPublish(const basicPublishRequestPtr &req)
        {
            // 1. 判断交换机是否存在
            auto ep = _host->selectExchange(req->exchange_name());
            if (ep.get() == nullptr)
            {
                return basicResponse(false, req->rid(), req->cid());
            }
            // 2. 进行交换路由，判断消息可以发布到交换机绑定的哪个队列中
            MsgQueueBindingMap mqbm = _host->exchangeBindings(req->exchange_name());
            BasicProperties *properties = nullptr;
            std::string routing_key;
            if (req->has_properties())
            {
                properties = req->mutable_properties();
                routing_key = properties->routing_key();
            }

            bool ok = true;
            for (auto &binding : mqbm)
            {
                if (Router::route(ep->type, routing_key, binding.second->binding_key))
                {
                    // 3. 将消息添加到队列中（添加消息的管理）
                    if (_host->basicPublish(binding.first, properties, req->body()) == false)
                    {
                        ok = false;
                        continue;
                    }

                    // 4. 通知队列调度器：队列里有新消息，可以尝试投递。
                    // 同一个队列的多次通知会被合并，不需要为每次发布创建任务。
                    if (_qdispatcher)
                    {
                        _qdispatcher->notify(binding.first);
                    }
                }
            }
            return basicResponse(ok, req->rid(), req->cid());
        }

        // 消息的确认
        void basicAck(const basicAckRequestPtr &req)
        {
            std::string owner = _host->basicAck(req->queue_name(), req->message_id());
            if (!owner.empty())
            {
                _cmp->onAck(req->queue_name(), owner);
            }
            // ACK 之后消费者的在途名额被释放，可以继续投递。
            if (_qdispatcher)
            {
                _qdispatcher->notify(req->queue_name());
            }
            return basicResponse(true, req->rid(), req->cid());
        }
        // 设置当前信道的 prefetch。0 表示不限制。
        void basicQos(const basicQosRequestPtr &req)
        {
            _prefetch_count = req->prefetch_count();
            if (_consumer)
            {
                _cmp->setPrefetch(_consumer->qname, _consumer->tag,
                                  _prefetch_count);
                if (_qdispatcher)
                {
                    _qdispatcher->notify(_consumer->qname);
                }
            }
            return basicResponse(true, req->rid(), req->cid());
        }
        // 消费失败时拒绝消息：requeue=true 重投，false 丢弃/死信。
        void basicReject(const basicRejectRequestPtr &req)
        {
            VirtualHost::RejectResult result =
                _host->basicReject(req->queue_name(), req->message_id(),
                                   req->requeue());
            if (result.found && !result.consumer_tag.empty())
            {
                _cmp->onAck(req->queue_name(), result.consumer_tag);
            }
            if (_qdispatcher)
            {
                _qdispatcher->notify(req->queue_name());
                for (auto &target : result.notify_queues)
                {
                    _qdispatcher->notify(target);
                }
            }
            return basicResponse(result.found, req->rid(), req->cid());
        }
        // 订阅队列消息
        void basicConsume(const basicConsumeRequestPtr &req)
        {
            // 1. 判断队列是否存在
            bool ret = _host->existsQueue(req->queue_name());
            if (ret == false)
            {
                return basicResponse(false, req->rid(), req->cid());
            }
            // 一个信道只允许一个消费者，否则旧消费者会残留在 ConsumerManager 中
            if (_consumer)
            {
                DLOG("信道 %s 已经订阅了队列 %s，拒绝重复订阅",
                     _cid.c_str(), _consumer->qname.c_str());
                return basicResponse(false, req->rid(), req->cid());
            }
            // 2. 创建队列的消费者。
            // 回调同样只持有 weak_ptr：即使消费者比 Channel 活得更久，
            // 回调时也不会访问已经析构的 Channel。
            std::weak_ptr<Channel> weak_self = shared_from_this();
            auto cb = [weak_self](const std::string tag, const BasicProperties *bp, const std::string &body)
            {
                auto sp = weak_self.lock();
                if (!sp)
                    return;
                sp->callback(tag, bp, body);
            };
            // 创建了消费者之后，当前的channel角色就是个消费者；
            // 把当前信道通过 basicQos 设置的上限带给消费者。
            _consumer = _cmp->create(req->consumer_tag(), req->queue_name(),
                                     req->auto_ack(), cb, _prefetch_count);
            // create 可能因为消费者 tag 重复而失败
            if (!_consumer)
            {
                DLOG("创建消费者失败，队列 %s 消费者 tag %s 重复",
                     req->queue_name().c_str(), req->consumer_tag().c_str());
                return basicResponse(false, req->rid(), req->cid());
            }

            // 先回复订阅成功，再让调度器按正常队列语义投递。
            // 新消费者不会“独占”历史消息，而是和其他消费者一起竞争队列里的消息。
            basicResponse(true, req->rid(), req->cid());
            if (_qdispatcher)
            {
                _qdispatcher->notify(req->queue_name());
            }
        }
        // 取消订阅
        void basicCancel(const basicCancelRequestPtr &req)
        {
            // 只允许取消当前信道自己的消费者，避免误删其他连接的订阅。
            if (!_consumer ||
                _consumer->tag != req->consumer_tag() ||
                _consumer->qname != req->queue_name())
            {
                DLOG("取消订阅失败：信道 %s 没有消费者 %s",
                     _cid.c_str(), req->consumer_tag().c_str());
                return basicResponse(false, req->rid(), req->cid());
            }

            std::string qname = _consumer->qname;
            std::string tag = _consumer->tag;
            _cmp->remove(tag, qname);
            // 把这个消费者未 ACK 的消息放回队首，再唤醒队列。
            _host->requeueByConsumer(qname, tag);
            _consumer.reset();
            if (_qdispatcher)
            {
                _qdispatcher->notify(qname);
            }
            return basicResponse(true, req->rid(), req->cid());
        }

    private:
        void callback(const std::string tag, const BasicProperties *bp, const std::string &body)
        {
            // 针对参数组织出推送消息请求，将消息推送给channel对应的客户端
            basicConsumeResponse resp;
            resp.set_cid(_cid);
            resp.set_body(body);
            resp.set_consumer_tag(tag);
            if (bp)
            {
                resp.mutable_properties()->set_id(bp->id());
                resp.mutable_properties()->set_delivery_mode(bp->delivery_mode());
                resp.mutable_properties()->set_routing_key(bp->routing_key());
                resp.mutable_properties()->set_delivery_count(bp->delivery_count());
            }
            _codec->send(_conn, resp);
        }
        void basicResponse(bool ok, const std::string &rid, const std::string &cid)
        {
            basicCommonResponse resp;
            resp.set_rid(rid);
            resp.set_cid(cid);
            resp.set_ok(ok);
            _codec->send(_conn, resp);
        }
    private:
        std::string _cid;
        Consumer::ptr _consumer;
        muduo::net::TcpConnectionPtr _conn;
        ProtobufCodecPtr _codec;
        ConsumerManager::ptr _cmp;
        VirtualHost::ptr _host;
        QueueDispatcher::ptr _qdispatcher;
        uint32_t _prefetch_count;
    };

    class ChannelManager
    {
    public:
        using ptr = std::shared_ptr<ChannelManager>;
        ChannelManager() {}
        bool openChannel(const std::string &id,
                         const VirtualHost::ptr &host,
                         const ConsumerManager::ptr &cmp,
                         const ProtobufCodecPtr &codec,
                         const muduo::net::TcpConnectionPtr &conn,
                         const QueueDispatcher::ptr &qdispatcher)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _channels.find(id);
            if (it != _channels.end())
            {
                DLOG("信道：%s 已经存在!", id.c_str());
                return false;
            }
            auto channel = std::make_shared<Channel>(id, host, cmp, codec, conn, qdispatcher);
            _channels.insert(std::make_pair(id, channel));
            return true;
        }
        void closeChannel(const std::string &id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _channels.erase(id);
        }
        Channel::ptr getChannel(const std::string &id)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _channels.find(id);
            if (it == _channels.end())
            {
                return Channel::ptr();
            }
            return it->second;
        }

    private:
        std::mutex _mutex;
        std::unordered_map<std::string, Channel::ptr> _channels;
    };
}
#endif