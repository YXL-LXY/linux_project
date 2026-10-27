#include "mq_channel.hpp"

namespace mq
{
    // 服务端连接对象：持有该 TCP 连接上的所有 Channel。
    // 连接断开时 Connection 被销毁，其 ChannelManager 中的 Channel 也会
    // 逐个析构，由 Channel 负责归还 wait-ack 消息并摘除消费者。
    class Connection
    {
    public:
        using ptr = std::shared_ptr<Connection>;
        Connection(const VirtualHost::ptr &host,
                   const ConsumerManager::ptr &cmp,
                   const ProtobufCodecPtr &codec,
                   const muduo::net::TcpConnectionPtr &conn,
                   const QueueDispatcher::ptr &qdispatcher) : _conn(conn),
                                                              _codec(codec),
                                                              _cmp(cmp),
                                                              _host(host),
                                                              _qdispatcher(qdispatcher),
                                                              _channels(std::make_shared<ChannelManager>()) {}
        void openChannel(const openChannelRequestPtr &req)
        {
            // 1. 判断信道ID是否重复,创建信道
            bool ret = _channels->openChannel(req->cid(), _host, _cmp, _codec, _conn, _qdispatcher);
            if (ret == false)
            {
                DLOG("创建信道的时候，信道ID重复了");
                return basicResponse(false, req->rid(), req->cid());
            }
            DLOG("%s 信道创建成功！", req->cid().c_str());
            // 3. 给客户端进行回复
            return basicResponse(true, req->rid(), req->cid());
        }
        void closeChannel(const closeChannelRequestPtr &req)
        {
            _channels->closeChannel(req->cid());
            return basicResponse(true, req->rid(), req->cid());
        }
        Channel::ptr getChannel(const std::string &cid)
        {
            return _channels->getChannel(cid);
        }

    private:
        void basicResponse(bool ok, const std::string &rid, const std::string &cid)
        {
            basicCommonResponse resp;
            resp.set_rid(rid);
            resp.set_cid(cid);
            resp.set_ok(ok);
            _codec->send(_conn, resp);
        }

    private:
        muduo::net::TcpConnectionPtr _conn;
        ProtobufCodecPtr _codec;
        ConsumerManager::ptr _cmp;
        VirtualHost::ptr _host;
        QueueDispatcher::ptr _qdispatcher;
        ChannelManager::ptr _channels;
    };

    // TCP 连接 -> Connection 的映射。断连时仅从表中删除，最后一个
    // shared_ptr 释放后触发 Connection/Channel 的析构清理。
    class ConnectionManager
    {
    public:
        using ptr = std::shared_ptr<ConnectionManager>;
        ConnectionManager() {}
        void newConnection(const VirtualHost::ptr &host,
                           const ConsumerManager::ptr &cmp,
                           const ProtobufCodecPtr &codec,
                           const muduo::net::TcpConnectionPtr &conn,
                           const QueueDispatcher::ptr &qdispatcher)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _conns.find(conn);
            if (it != _conns.end())
            {
                return;
            }
            Connection::ptr self_conn = std::make_shared<Connection>(host, cmp, codec, conn, qdispatcher);
            _conns.insert(std::make_pair(conn, self_conn));
        }
        void delConnection(const muduo::net::TcpConnectionPtr &conn)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            _conns.erase(conn);
        }
        Connection::ptr getConnection(const muduo::net::TcpConnectionPtr &conn)
        {
            std::unique_lock<std::mutex> lock(_mutex);
            auto it = _conns.find(conn);
            if (it == _conns.end())
            {
                return Connection::ptr();
            }
            return it->second;
        }

    private:
        std::mutex _mutex;
        std::unordered_map<muduo::net::TcpConnectionPtr, Connection::ptr> _conns;
    };
}