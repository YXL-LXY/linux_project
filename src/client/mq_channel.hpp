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
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <unordered_map>

namespace mq
{
    typedef std::shared_ptr<google::protobuf::Message> MessagePtr;
    using ProtobufCodecPtr = std::shared_ptr<ProtobufCodec>;
    using basicConsumeResponsePtr = std::shared_ptr<basicConsumeResponse>;
    using basicCommonResponsePtr = std::shared_ptr<basicCommonResponse>;

    // 客户端信道。
    //
    // 调用线程通过 _codec->send() 发出请求，然后在 _basic_resp 条件变量上
    // 等待对应 rid 的 basicCommonResponse，最多 5 秒。网络线程收到响应后
    // 调用 putBasicResponse() 唤醒等待者。
    //
    // _consumer 单独用 _consumer_mutex 保护，因为：
    // - consume() 在网络线程读取 _consumer 并执行回调；
    // - basicConsume()/basicCancel()/析构函数可能在用户线程修改它。
    // 回调本身由调用方（如 AsyncWorker 的线程池）执行，不在信道锁内。
    class Channel
    {
    public:
        using ptr = std::shared_ptr<Channel>;
        Channel(const muduo::net::TcpConnectionPtr &conn, const ProtobufCodecPtr &codec) : _cid(UUIDHelper::uuid()), _conn(conn), _codec(codec) {}
        // 析构时尽力取消订阅，让 broker 归还 wait-ack 消息。
        ~Channel() { basicCancel(); }
        std::string cid() { return _cid; }
        bool openChannel()
        {
            std::string rid = UUIDHelper::uuid();
            openChannelRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            _codec->send(_conn, req);
            return waitResponseOk(rid, "打开信道");
        }
        void closeChannel()
        {
            std::string rid = UUIDHelper::uuid();
            closeChannelRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            _codec->send(_conn, req);
            waitResponseOk(rid, "关闭信道");
            return;
        }
        bool declareExchange(
            const std::string &name,
            ExchangeType type,
            bool durable,
            bool auto_delete,
            google::protobuf::Map<std::string, std::string> &args)
        {
            // 构造一个声明虚拟机的请求对象，
            std::string rid = UUIDHelper::uuid();
            declareExchangeRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(name);
            req.set_exchange_type(type);
            req.set_durable(durable);
            req.set_auto_delete(auto_delete);
            req.mutable_args()->swap(args);
            // 然后向服务器发送请求
            _codec->send(_conn, req);
            // 等待服务器的响应（超时返回 false）
            return waitResponseOk(rid, "声明交换机");
        }
        void deleteExchange(const std::string &name)
        {
            std::string rid = UUIDHelper::uuid();
            deleteExchangeRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(name);
            _codec->send(_conn, req);
            waitResponseOk(rid, "删除交换机");
            return;
        }

        bool declareQueue(
            const std::string &qname,
            bool qdurable,
            bool qexclusive,
            bool qauto_delete,
            google::protobuf::Map<std::string, std::string> &qargs)
        {
            std::string rid = UUIDHelper::uuid();
            declareQueueRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(qname);
            req.set_durable(qdurable);
            req.set_auto_delete(qauto_delete);
            req.set_exclusive(qexclusive);
            req.mutable_args()->swap(qargs);
            _codec->send(_conn, req);
            return waitResponseOk(rid, "声明队列");
        }
        void deleteQueue(const std::string &qname)
        {
            std::string rid = UUIDHelper::uuid();
            deleteQueueRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(qname);
            _codec->send(_conn, req);
            waitResponseOk(rid, "删除队列");
            return;
        }

        bool queueBind(
            const std::string &ename,
            const std::string &qname,
            const std::string &key)
        {
            std::string rid = UUIDHelper::uuid();
            queueBindRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(ename);
            req.set_queue_name(qname);
            req.set_binding_key(key);
            _codec->send(_conn, req);
            return waitResponseOk(rid, "绑定队列");
        }
        void queueUnBind(const std::string &ename, const std::string &qname)
        {
            std::string rid = UUIDHelper::uuid();
            queueUnBindRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_exchange_name(ename);
            req.set_queue_name(qname);
            _codec->send(_conn, req);
            waitResponseOk(rid, "解绑队列");
            return;
        }

        // 发布消息。
        // 当前只等待 broker 的“路由处理完成”响应，不提供 Publisher Confirm：
        // 服务端持久化失败等错误不会通过返回值区分。
        void basicPublish(
            const std::string &ename,
            const BasicProperties *bp,
            const std::string &body)
        {
            std::string rid = UUIDHelper::uuid();
            basicPublishRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_body(body);
            req.set_exchange_name(ename);
            if (bp != nullptr)
            {
                req.mutable_properties()->set_id(bp->id());
                req.mutable_properties()->set_delivery_mode(bp->delivery_mode());
                req.mutable_properties()->set_routing_key(bp->routing_key());
                req.mutable_properties()->set_delivery_count(bp->delivery_count());
            }
            _codec->send(_conn, req);
            waitResponseOk(rid, "发布消息");
            return;
        }

        void basicAck(const std::string &msgid)
        {
            Consumer::ptr consumer;
            {
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                consumer = _consumer;
            }
            if (consumer.get() == nullptr)
            {
                DLOG("消息确认时，找不到消费者信息！");
                return;
            }
            std::string rid = UUIDHelper::uuid();
            basicAckRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(consumer->qname);
            req.set_message_id(msgid);
            _codec->send(_conn, req);
            waitResponseOk(rid, "确认消息");
            return;
        }

        // 设置当前信道的 prefetch。0 表示不限制。
        bool basicQos(uint32_t prefetch_count)
        {
            std::string rid = UUIDHelper::uuid();
            basicQosRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_prefetch_count(prefetch_count);
            _codec->send(_conn, req);
            return waitResponseOk(rid, "设置 QoS");
        }

        // 消费失败时拒绝消息；requeue=true 会让 broker 重新投递。
        bool basicReject(const std::string &msgid, bool requeue)
        {
            Consumer::ptr consumer;
            {
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                consumer = _consumer;
            }
            if (consumer.get() == nullptr)
            {
                DLOG("拒绝消息时，找不到消费者信息！");
                return false;
            }
            std::string rid = UUIDHelper::uuid();
            basicRejectRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(consumer->qname);
            req.set_message_id(msgid);
            req.set_requeue(requeue);
            _codec->send(_conn, req);
            return waitResponseOk(rid, "拒绝消息");
        }

        void basicCancel()
        {
            Consumer::ptr consumer;
            {
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                consumer = _consumer;
            }
            if (consumer.get() == nullptr)
            {
                return;
            }
            std::string rid = UUIDHelper::uuid();
            basicCancelRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(consumer->qname);
            req.set_consumer_tag(consumer->tag);
            _codec->send(_conn, req);
            waitResponseOk(rid, "取消订阅");
            {
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                if (_consumer == consumer)
                {
                    _consumer.reset();
                }
            }
            return;
        }

        bool basicConsume( // 添加订阅
            const std::string &consumer_tag,
            const std::string &queue_name,
            bool auto_ack,
            const ConsumerCallback &cb)
        {
            {
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                if (_consumer.get() != nullptr)
                {
                    DLOG("当前信道已订阅其他队列消息！");
                    return false;
                }
                // 必须在发送请求之前安装消费者：服务端订阅成功后会立刻开始
                // 正常投递，网络线程可能在 waitResponseOk 返回前就收到消息。
                _consumer = std::make_shared<Consumer>(consumer_tag, queue_name, auto_ack, cb);
            }

            std::string rid = UUIDHelper::uuid();
            basicConsumeRequest req;
            req.set_rid(rid);
            req.set_cid(_cid);
            req.set_queue_name(queue_name);
            req.set_consumer_tag(consumer_tag);
            req.set_auto_ack(auto_ack);
            _codec->send(_conn, req);
            if (waitResponseOk(rid, "订阅队列") == false)
            {
                DLOG("添加订阅失败或等待响应超时！");
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                _consumer.reset();
                return false;
            }
            return true;
        }

    public:
        // 连接收到基础响应后，向hash_map中添加响应
        void putBasicResponse(const basicCommonResponsePtr &resp)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _basic_resp.insert(std::make_pair(resp->rid(), resp));
            _cv.notify_all();
        }

        // 连接收到消息推送后，需要通过信道找到对应的消费者对象，通过回调函数进行消息处理
        void consume(const basicConsumeResponsePtr &resp)
        {
            Consumer::ptr consumer;
            {
                std::unique_lock<std::mutex> lock(_consumer_mutex);
                consumer = _consumer;
            }
            if (consumer.get() == nullptr)
            {
                DLOG("消息处理时，未找到订阅者信息！");
                return;
            }
            if (consumer->tag != resp->consumer_tag())
            {
                DLOG("收到的推送消息中的消费者标识，与当前信道消费者标识不一致！");
                return;
            }
            consumer->callback(resp->consumer_tag(), resp->mutable_properties(), resp->body());
        }

    private:
        // 等待响应并检查服务端返回的 ok 字段；超时会记录日志并返回 false。
        bool waitResponseOk(const std::string &rid, const char *action)
        {
            basicCommonResponsePtr resp = waitResponse(rid);
            if (!resp)
            {
                DLOG("%s：等待服务器响应超时", action);
                return false;
            }
            return resp->ok();
        }

        // 最多等待 5 秒，避免网络抖动时永久阻塞。
        basicCommonResponsePtr waitResponse(const std::string &rid)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto ok = _cv.wait_for(lock, std::chrono::seconds(5), [&rid, this]()
                                   { return _basic_resp.count(rid) > 0; });
            if (!ok)
            {
                return nullptr;
            }
            auto resp = _basic_resp[rid];
            _basic_resp.erase(rid);
            return resp;
        }

    private:
        std::string _cid;
        muduo::net::TcpConnectionPtr _conn;
        ProtobufCodecPtr _codec;
        Consumer::ptr _consumer;
        std::mutex _consumer_mutex;
        std::mutex _mutex;
        std::condition_variable _cv;
        std::unordered_map<std::string, basicCommonResponsePtr> _basic_resp;
    };

    // 客户端连接内的信道表。cid 由客户端生成，服务端原样带回。
    class ChannelManager
    {
    public:
        using ptr = std::shared_ptr<ChannelManager>;
        ChannelManager() {}
        Channel::ptr create(const muduo::net::TcpConnectionPtr &conn,
                            const ProtobufCodecPtr &codec)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto channel = std::make_shared<Channel>(conn, codec);
            _channels.insert(std::make_pair(channel->cid(), channel));
            return channel;
        }
        void remove(const std::string &cid)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _channels.erase(cid);
        }
        Channel::ptr get(const std::string &cid)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _channels.find(cid);
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