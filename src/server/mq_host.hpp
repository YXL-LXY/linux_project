#ifndef __M_HOST_H__
#define __M_HOST_H__
#include "mq_exchange.hpp"
#include "mq_queue.hpp"
#include "mq_binding.hpp"
#include "mq_message.hpp"
#include "mq_route.hpp"
#include <vector>

namespace mq
{
    class VirtualHost
    {
    public:
        using ptr = std::shared_ptr<VirtualHost>;
        // basicReject 的处理结果。notify_queues 是死信路由命中的队列，
        // 调用方需要唤醒这些队列的调度器。
        struct RejectResult
        {
            RejectResult() : found(false) {}
            bool found;
            std::string consumer_tag;
            std::vector<std::string> notify_queues;
        };
        // ACK 超时扫描后，一个队列里发生的重投/死信结果。
        struct TimeoutResult
        {
            std::string queue_name;
            std::vector<std::string> consumer_tags;
            std::vector<std::string> notify_queues;
        };
        VirtualHost(const std::string &hname, const std::string &basedir, const std::string &dbfile) : 
            _host_name(hname),
            _emp(std::make_shared<ExchangeManager>(dbfile)),
            _mqmp(std::make_shared<MsgQueueManager>(dbfile)),
            _bmp(std::make_shared<BindingManager>(dbfile)),
            _mmp(std::make_shared<MessageManager>(basedir))
        {
            // 获取到所有的队列信息，通过队列名称恢复历史消息数据
            QueueMap qm = _mqmp->allQueues();
            for (auto &q : qm)
            {
                size_t max_length = 0;
                bool has_max_length = parseQueueMaxLength(q.second->args, max_length);
                _mmp->initQueueMessage(q.first, max_length, has_max_length);
            }
        }
        bool declareExchange(const std::string &name,
                             ExchangeType type, bool durable, bool auto_delete,
                             const google::protobuf::Map<std::string, std::string> &args)
        {
            return _emp->declareExchange(name, type, durable, auto_delete, args);
        }
        void deleteExchange(const std::string &name)
        {
            // 删除交换机的时候，需要将交换机相关的绑定信息也删除掉。
            _bmp->removeExchangeBindings(name);
            return _emp->deleteExchange(name);
        }
        bool existsExchange(const std::string &name)
        {
            return _emp->exists(name);
        }
        Exchange::ptr selectExchange(const std::string &ename)
        {
            return _emp->selectExchange(ename);
        }

        bool declareQueue(const std::string &qname,
                          bool qdurable,
                          bool qexclusive,
                          bool qauto_delete,
                          const google::protobuf::Map<std::string, std::string> &qargs)
        {
            // 从队列参数中解析 x-max-length。
            // 只有本次声明显式带了该参数才更新已有队列的上限。
            size_t max_length = 0;
            bool has_max_length = parseQueueMaxLength(qargs, max_length);

            // 初始化队列的消息句柄（消息的存储管理）
            // 队列的创建
            _mmp->initQueueMessage(qname, max_length, has_max_length);
            return _mqmp->declareQueue(qname, qdurable, qexclusive, qauto_delete, qargs);
        }
        void deleteQueue(const std::string &name)
        {
            // 删除的时候队列相关的数据有两个：队列的消息，队列的绑定信息
            _mmp->destroyQueueMessage(name);
            _bmp->removeMsgQueueBindings(name);
            return _mqmp->deleteQueue(name);
        }
        bool existsQueue(const std::string &name)
        {
            return _mqmp->exists(name);
        }
        QueueMap allQueues()
        {
            return _mqmp->allQueues();
        }

        bool bind(const std::string &ename, const std::string &qname, const std::string &key)
        {
            Exchange::ptr ep = _emp->selectExchange(ename);
            if (ep.get() == nullptr)
            {
                DLOG("进行队列绑定失败，交换机%s不存在！", ename.c_str());
                return false;
            }
            MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
            if (mqp.get() == nullptr)
            {
                DLOG("进行队列绑定失败，队列%s不存在！", qname.c_str());
                return false;
            }
            return _bmp->bind(ename, qname, key, ep->durable && mqp->durable);
        }
        void unBind(const std::string &ename, const std::string &qname)
        {
            return _bmp->unBind(ename, qname);
        }
        MsgQueueBindingMap exchangeBindings(const std::string &ename)
        {
            return _bmp->getExchangeBindings(ename);
        }
        bool existsBinding(const std::string &ename, const std::string &qname)
        {
            return _bmp->exists(ename, qname);
        }

        bool basicPublish(const std::string &qname, BasicProperties *bp, const std::string &body)
        {
            MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
            if (mqp.get() == nullptr)
            {
                DLOG("发布消息失败，队列%s不存在！", qname.c_str());
                return false;
            }
            return _mmp->insert(qname, bp, body, mqp->durable);
        }
        // consumer_tag 记录 wait-ack 归属，断线时用于精确重新入队。
        MessagePtr basicConsume(const std::string &qname, const std::string &consumer_tag = "")
        {
            return _mmp->front(qname, consumer_tag, queueAckTimeout(qname));
        }
        size_t requeueByConsumer(const std::string &qname, const std::string &consumer_tag)
        {
            return _mmp->requeueByConsumer(qname, consumer_tag);
        }
        // 确认消息，返回消息的 owner 标签，便于上层减少消费者的在途计数。
        std::string basicAck(const std::string &qname, const std::string &msgid)
        {
            return _mmp->ack(qname, msgid);
        }
        // 拒绝消息：requeue=true 重投，false 丢弃/死信。
        RejectResult basicReject(const std::string &qname, const std::string &msgid,
                                 bool requeue)
        {
            QueueMessage::RejectResult qr = _mmp->reject(
                qname, msgid, requeue, queueMaxDeliveryCount(qname));
            RejectResult result;
            result.found = qr.found;
            result.consumer_tag = qr.consumer_tag;
            if (qr.dead_letter)
            {
                result.notify_queues = routeDeadLetter(qname, qr.msg);
            }
            return result;
        }
        // 扫描所有队列中超时的 wait-ack 消息，重新入队或转入死信。
        std::vector<TimeoutResult> requeueExpired(uint64_t now_ms)
        {
            std::vector<TimeoutResult> results;
            QueueMap queues = _mqmp->allQueues();
            for (auto &q : queues)
            {
                std::vector<QueueMessage::ExpiredEntry> expired;
                uint32_t max_delivery_count = 0;
                parseQueueMaxDeliveryCount(q.second->args, max_delivery_count);
                size_t count = _mmp->requeueExpired(
                    q.first, now_ms, max_delivery_count, expired);
                if (count == 0)
                {
                    continue;
                }

                TimeoutResult result;
                result.queue_name = q.first;
                for (auto &entry : expired)
                {
                    result.consumer_tags.push_back(entry.consumer_tag);
                    if (entry.dead_letter)
                    {
                        std::vector<std::string> targets =
                            routeDeadLetter(q.first, entry.msg);
                        result.notify_queues.insert(result.notify_queues.end(),
                                                     targets.begin(), targets.end());
                    }
                }
                results.push_back(result);
            }
            return results;
        }
        size_t getable_count(const std::string &qname)
        {
            return _mmp->getable_count(qname);
        }
        size_t waitack_count(const std::string &qname)
        {
            return _mmp->waitack_count(qname);
        }
        uint64_t queueAckTimeout(const std::string &qname)
        {
            MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
            uint64_t timeout_ms = MQ_DEFAULT_ACK_TIMEOUT_MS;
            if (mqp && parseQueueAckTimeout(mqp->args, timeout_ms))
            {
                return timeout_ms;
            }
            return timeout_ms;
        }
        uint32_t queueMaxDeliveryCount(const std::string &qname)
        {
            MsgQueue::ptr mqp = _mqmp->selectQueue(qname);
            uint32_t count = 0;
            if (mqp)
            {
                parseQueueMaxDeliveryCount(mqp->args, count);
            }
            return count;
        }
        void clear()
        {
            _emp->clear();
            _mqmp->clear();
            _bmp->clear();
            _mmp->clear();
        }

    private:
        // 按源队列的 x-dead-letter-exchange 把消息重新发布到死信交换机。
        // 返回实际收到消息的队列名，调用方需要唤醒这些队列。
        std::vector<std::string> routeDeadLetter(const std::string &source_qname,
                                                 const MessagePtr &msg)
        {
            std::vector<std::string> targets;
            MsgQueue::ptr source = _mqmp->selectQueue(source_qname);
            if (!source)
            {
                return targets;
            }
            std::string dlx = queueArgValue(source->args, "x-dead-letter-exchange");
            if (dlx.empty())
            {
                DLOG("队列[%s]的消息没有配置死信交换机，直接丢弃 msg_id:%s",
                     source_qname.c_str(),
                     msg->payload().properties().id().c_str());
                return targets;
            }
            Exchange::ptr ep = _emp->selectExchange(dlx);
            if (!ep)
            {
                DLOG("队列[%s]配置的死信交换机 %s 不存在，消息被丢弃",
                     source_qname.c_str(), dlx.c_str());
                return targets;
            }

            std::string routing_key =
                queueArgValue(source->args, "x-dead-letter-routing-key");
            if (routing_key.empty())
            {
                routing_key = msg->payload().properties().routing_key();
            }
            BasicProperties properties = msg->payload().properties();
            properties.set_delivery_count(0);
            properties.set_routing_key(routing_key);

            MsgQueueBindingMap bindings = _bmp->getExchangeBindings(dlx);
            for (auto &binding : bindings)
            {
                if (Router::route(ep->type, routing_key,
                                  binding.second->binding_key) == false)
                {
                    continue;
                }
                if (basicPublish(binding.first, &properties,
                                 msg->payload().body()))
                {
                    targets.push_back(binding.first);
                }
            }
            if (targets.empty())
            {
                DLOG("死信交换机 %s 没有匹配到目标队列，消息被丢弃", dlx.c_str());
            }
            return targets;
        }

    private:
        std::string _host_name;
        ExchangeManager::ptr _emp;
        MsgQueueManager::ptr _mqmp;
        BindingManager::ptr _bmp;
        MessageManager::ptr _mmp;
    };
}
#endif