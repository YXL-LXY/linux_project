#include "bridge/mq_client_adapter.hpp"

#include "client/mq_connection.hpp"
#include "common/mq_logger.hpp"

#include <mutex>
#include <vector>

namespace mq
{
namespace bridge
{

namespace
{

void setError(std::string *error, const std::string &message)
{
    if (error != nullptr)
    {
        *error = message;
    }
}

} // namespace

struct MqClientAdapter::Impl
{
    // AsyncWorker 提供网络 IO 线程和用户回调线程池；必须比 Connection/Channel
    // 活得更久。
    mq::AsyncWorker::ptr worker;
    mq::Connection::ptr connection;

    // 拓扑声明和发布共用一条信道。信道本身不是线程安全的，所有访问都在
    // mutex 下串行化。
    mq::Channel::ptr publish_channel;

    // 每条消费队列单独一条信道，便于独立设置 prefetch、独立取消。stop() 会
    // 把它们从 vector 中换出后统一 basicCancel。
    std::vector<mq::Channel::ptr> consume_channels;

    // 保护上述指针和 vector。注意：declare/publish 会在持锁期间等待 broker
    // 响应（最长约 5 秒），所以这些方法不要从高频实时线程调用。
    mutable std::mutex mutex;
};

MqClientAdapter::MqClientAdapter() : _impl(new Impl())
{
}

MqClientAdapter::~MqClientAdapter()
{
    stop();
}

bool MqClientAdapter::connected() const
{
    // 仅检查本地是否还持有连接对象；broker 已断开但尚未触发清理时，这里
    // 仍可能返回 true。它不是健康检查接口。
    std::lock_guard<std::mutex> lock(_impl->mutex);
    return _impl->connection.get() != nullptr;
}

bool MqClientAdapter::connect(const std::string &host, int port, std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (host.empty())
    {
        setError(error, "mq host must not be empty");
        return false;
    }
    if (port <= 0 || port > 65535)
    {
        setError(error, "mq port must be between 1 and 65535");
        return false;
    }

    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->connection.get() != nullptr)
    {
        setError(error, "mq client is already connected");
        return false;
    }

    try
    {
        // Connection 构造函数会阻塞到 TCP 连接建立；当前客户端没有连接超时参数。
        // 因此 connect() 失败时可能等待较久，调用方不要在 ROS2 回调线程里调用。
        _impl->worker = std::make_shared<mq::AsyncWorker>();
        _impl->connection = std::make_shared<mq::Connection>(host, port, _impl->worker);
        _impl->publish_channel = _impl->connection->openChannel();
        if (_impl->publish_channel.get() == nullptr)
        {
            // 失败时把半初始化状态清干净，后续仍可重新 connect()。
            _impl->connection.reset();
            _impl->worker.reset();
            setError(error, "failed to open mq publish channel");
            return false;
        }
    }
    catch (const std::exception &e)
    {
        _impl->connection.reset();
        _impl->worker.reset();
        _impl->publish_channel.reset();
        setError(error, std::string("failed to connect to mq broker: ") + e.what());
        return false;
    }
    return true;
}

void MqClientAdapter::stop()
{
    std::vector<mq::Channel::ptr> channels;

    // 先把所有信道从共享状态里换出，再释放锁做网络操作。这样 basicCancel()
    // 阻塞时不会一直占着 mutex；换出后新的 declare/publish 也会因信道为空
    // 而立即失败，而不是继续往正在关闭的连接上发请求。
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        channels.swap(_impl->consume_channels);
        if (_impl->publish_channel.get() != nullptr)
        {
            channels.push_back(_impl->publish_channel);
            _impl->publish_channel.reset();
        }
    }

    // 在连接、worker 仍有效时取消消费者，这样 Channel 析构时不会再次
    // 等待一个已经关闭连接上的 basicCancel 响应；同时让 broker 立即归还
    // 未 ACK 的消息，而不是等到连接断开。
    for (const auto &channel : channels)
    {
        if (channel.get() != nullptr)
        {
            channel->basicCancel();
        }
    }

    // 先断连接再销毁 worker；局部 channels 在此之后析构，Connection/worker
    // 仍被本函数持有，不会出现悬空引用。
    {
        std::lock_guard<std::mutex> lock(_impl->mutex);
        _impl->connection.reset();
        _impl->worker.reset();
    }
}

bool MqClientAdapter::declareExchange(
    const std::string &name,
    mq::ExchangeType type,
    bool durable,
    bool auto_delete,
    const google::protobuf::Map<std::string, std::string> &args,
    std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (name.empty())
    {
        setError(error, "exchange name must not be empty");
        return false;
    }

    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->publish_channel.get() == nullptr)
    {
        setError(error, "mq client is not connected");
        return false;
    }

    // 客户端接口通过非 const 引用“swap”参数，因此先复制一份，避免修改
    // 调用者的 map。
    google::protobuf::Map<std::string, std::string> mutable_args = args;
    if (!_impl->publish_channel->declareExchange(name, type, durable, auto_delete, mutable_args))
    {
        setError(error, "failed to declare exchange: " + name);
        return false;
    }
    return true;
}

bool MqClientAdapter::declareQueue(
    const std::string &name,
    bool durable,
    bool exclusive,
    bool auto_delete,
    const google::protobuf::Map<std::string, std::string> &args,
    std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (name.empty())
    {
        setError(error, "queue name must not be empty");
        return false;
    }

    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->publish_channel.get() == nullptr)
    {
        setError(error, "mq client is not connected");
        return false;
    }

    google::protobuf::Map<std::string, std::string> mutable_args = args;
    if (!_impl->publish_channel->declareQueue(name, durable, exclusive, auto_delete, mutable_args))
    {
        setError(error, "failed to declare queue: " + name);
        return false;
    }
    return true;
}

bool MqClientAdapter::bindQueue(const std::string &exchange,
                                const std::string &queue,
                                const std::string &routing_key,
                                std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (exchange.empty() || queue.empty())
    {
        setError(error, "exchange and queue must not be empty");
        return false;
    }

    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->publish_channel.get() == nullptr)
    {
        setError(error, "mq client is not connected");
        return false;
    }
    // 绑定也走 publish channel：它在拓扑声明阶段与 declare 串行执行，
    // 真正消费时再使用独立信道。
    if (!_impl->publish_channel->queueBind(exchange, queue, routing_key))
    {
        setError(error, "failed to bind queue to exchange");
        return false;
    }
    return true;
}

bool MqClientAdapter::publish(const std::string &exchange,
                              const mq::BasicProperties &properties,
                              const std::string &body,
                              std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (exchange.empty())
    {
        setError(error, "exchange must not be empty");
        return false;
    }

    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->publish_channel.get() == nullptr)
    {
        setError(error, "mq client is not connected");
        return false;
    }

    // basicPublish 返回的是 broker 普通响应是否成功，不是 Publisher Confirm；
    // 也没有 mandatory/return。消息是否已持久化、是否真的路由到队列，仍需
    // 未来扩展协议后才能确认。
    if (!_impl->publish_channel->basicPublish(exchange, &properties, body))
    {
        setError(error, "failed to publish to exchange: " + exchange);
        return false;
    }
    return true;
}

bool MqClientAdapter::consume(const std::string &queue,
                              const std::string &consumer_tag,
                              uint32_t prefetch_count,
                              bool auto_ack,
                              MqMessageHandler handler,
                              std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (queue.empty())
    {
        setError(error, "queue must not be empty");
        return false;
    }
    if (consumer_tag.empty())
    {
        setError(error, "consumer_tag must not be empty");
        return false;
    }
    if (!handler)
    {
        setError(error, "message handler must not be empty");
        return false;
    }

    std::lock_guard<std::mutex> lock(_impl->mutex);
    if (_impl->connection.get() == nullptr)
    {
        setError(error, "mq client is not connected");
        return false;
    }

    // 每个队列使用独立信道：prefetch 和 basicCancel 都是信道级状态，复用
    // publish channel 会互相影响。
    mq::Channel::ptr channel = _impl->connection->openChannel();
    if (channel.get() == nullptr)
    {
        setError(error, "failed to open mq consume channel");
        return false;
    }
    if (prefetch_count > 0 && !channel->basicQos(prefetch_count))
    {
        setError(error, "failed to set mq prefetch");
        return false;
    }

    // 回调捕获 channel 的 shared_ptr，保证回调执行期间信道仍然存活。
    // Connection::consumeResponse 已经把回调投递到 AsyncWorker 线程池，因此
    // 这里的 handler 不在网络 IO 线程执行；但仍应尽量短，避免占满线程池。
    mq::ConsumerCallback callback =
        [handler, channel, auto_ack](const std::string,
                                     const mq::BasicProperties *properties,
                                     const std::string body)
    {
        MqDelivery delivery;
        if (properties != nullptr)
        {
            delivery.properties = *properties;
        }
        delivery.body = body;

        // 手动确认依赖 broker 返回 message id：没有 id 就无法精确 ACK/Reject。
        // auto_ack=true 时消息已经被 broker 自动确认，不需要也不应该再 ACK。
        const std::string message_id =
            properties != nullptr ? properties->id() : std::string();
        if (!auto_ack && !message_id.empty())
        {
            delivery.ack = [channel, message_id]()
            {
                channel->basicAck(message_id);
            };
            delivery.reject = [channel, message_id](bool requeue)
            {
                channel->basicReject(message_id, requeue);
            };
        }

        try
        {
            handler(delivery);
        }
        catch (const std::exception &e)
        {
            // 抛异常时尽量把消息退回队列，避免它停在 wait-ack 直到超时。
            // 如果 auto_ack=true 或没有 message id，reject 为空，只能依赖
            // broker 的 ACK 超时机制。
            ELOG("bridge mq handler exception: %s", e.what());
            if (delivery.reject)
            {
                delivery.reject(true);
            }
        }
    };

    if (!channel->basicConsume(consumer_tag, queue, auto_ack, callback))
    {
        setError(error, "failed to consume queue: " + queue);
        return false;
    }

    // 保存信道，stop() 时统一取消；否则 Connection 析构时可能已经在关闭
    // 的连接上等待 basicCancel 响应。
    _impl->consume_channels.push_back(channel);
    return true;
}

} // namespace bridge
} // namespace mq
