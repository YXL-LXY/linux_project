#ifndef MQ_BRIDGE_MQ_CLIENT_ADAPTER_HPP
#define MQ_BRIDGE_MQ_CLIENT_ADAPTER_HPP

#include "mq_msg.pb.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace mq
{
namespace bridge
{

// 交给 bridge 消费回调的一条 MQ 消息。
//
// ack/reject 内部捕获了信道和消息 id，可在回调线程直接调用。它们是同步的
// 网络请求，可能阻塞；每条消息只应调用 ack 或 reject 一次。
// auto_ack=true 或 broker 没有返回 message id 时，这两个 std::function 为空，
// 调用前必须先判断。
struct MqDelivery
{
    mq::BasicProperties properties;  // 原始属性，包含 id、routing_key、delivery_mode 等
    std::string body;                // 原始消息体，bridge 层可自行解析
    std::function<void()> ack;       // 手动确认：从 wait-ack 中移除
    std::function<void(bool)> reject; // requeue=true 重新入队，false 丢弃/死信
};

using MqMessageHandler = std::function<void(const MqDelivery &)>;

// 对现有 mq 客户端库的薄封装。
//
// 这个类只负责连接、声明拓扑、发布和消费，不包含任何 ROS2/CDR 逻辑。
// 线程与生命周期：
// - connect() 会阻塞到 TCP 连接建立完成；
// - 一个 publish_channel 负责 declare/bind/publish；
// - 每个 consume 队列额外开一个 channel，以便独立设置 prefetch 和取消；
// - consume 回调由 AsyncWorker 的线程池执行，不在网络 IO 线程；
// - 公开方法通过内部 mutex 串行化，stop() 可重复调用。
//
// 可靠性边界：当前客户端的 basicPublish 没有 Publisher Confirm，因此
// publish() 只能表示请求已发出并收到 broker 的普通响应，不能表示消息已经
// 持久化。BridgeRuntime 目前对 ROS2 -> MQ 采用 at-most-once：发布失败只
// 记录日志，不会保留待确认状态重试。
class MqClientAdapter
{
public:
    MqClientAdapter();
    ~MqClientAdapter();

    MqClientAdapter(const MqClientAdapter &) = delete;
    MqClientAdapter &operator=(const MqClientAdapter &) = delete;

    // 建立连接并打开 publish channel。重复调用会失败；当前没有自动重连。
    bool connect(const std::string &host, int port, std::string *error);

    // 取消所有消费者、释放信道/连接/worker。可重复调用。
    void stop();

    // 只表示当前是否持有 Connection 指针，不探测 TCP 是否仍然存活。
    bool connected() const;

    bool declareExchange(const std::string &name,
                         mq::ExchangeType type,
                         bool durable,
                         bool auto_delete,
                         const google::protobuf::Map<std::string, std::string> &args,
                         std::string *error);

    bool declareQueue(const std::string &name,
                      bool durable,
                      bool exclusive,
                      bool auto_delete,
                      const google::protobuf::Map<std::string, std::string> &args,
                      std::string *error);

    bool bindQueue(const std::string &exchange,
                   const std::string &queue,
                   const std::string &routing_key,
                   std::string *error);

    // 发布到 exchange。body 和 properties 由调用者决定，adapter 不改写。
    bool publish(const std::string &exchange,
                 const mq::BasicProperties &properties,
                 const std::string &body,
                 std::string *error);

    // 为 queue 打开独立信道并开始消费。
    // prefetch_count==0 表示不限制；auto_ack=true 时由 broker 自动确认，
    // 否则 handler 需要通过 MqDelivery.ack/reject 手动处理。
    bool consume(const std::string &queue,
                 const std::string &consumer_tag,
                 uint32_t prefetch_count,
                 bool auto_ack,
                 MqMessageHandler handler,
                 std::string *error);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace bridge
} // namespace mq

#endif
