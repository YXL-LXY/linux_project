#include "mq_connection.hpp"
#include <cstdlib>

void cb(mq::Channel::ptr &channel, const std::string consumer_tag, 
    const mq::BasicProperties *bp, const std::string &body)
{
    std::cout << consumer_tag << "消费了消息：" << body << std::endl;
    channel->basicAck(bp->id());
}
int main(int argc, char *argv[])
{
    if (argc < 2 || argc > 4) {
        std::cout << "usage: ./consume_client queue1 [consumer_tag] [prefetch]\n";
        return -1;
    }
    // 同一队列可以启动多个消费者；不同消费者要使用不同的 tag。
    const std::string consumer_tag = (argc >= 3) ? argv[2] : "consumer1";
    unsigned long prefetch = 0;
    if (argc == 4) {
        char *end = nullptr;
        prefetch = std::strtoul(argv[3], &end, 10);
        if (end == argv[3] || *end != '\0') {
            std::cerr << "prefetch 必须是大于等于 0 的整数\n";
            return -1;
        }
    }
    //1. 实例化异步工作线程对象
    mq::AsyncWorker::ptr awp = std::make_shared<mq::AsyncWorker>();
    //2. 实例化连接对象
    mq::Connection::ptr conn = std::make_shared<mq::Connection>("127.0.0.1", 8085, awp);
    //3. 通过连接创建信道
    mq::Channel::ptr channel = conn->openChannel();
    if (!channel)
    {
        std::cerr << "打开信道失败，请确认 mqserver 已启动并监听 8085 端口" << std::endl;
        return -1;
    }
    //4. 通过信道提供的服务完成所需
    //  1. 声明一个交换机exchange1, 交换机类型为广播模式
    google::protobuf::Map<std::string, std::string> tmp_map;
    channel->declareExchange("exchange1", mq::ExchangeType::TOPIC, true, false, tmp_map);
    //  2. 声明一个队列queue1
    channel->declareQueue("queue1", true, false, false, tmp_map);
    //  3. 声明一个队列queue2
    channel->declareQueue("queue2", true, false, false, tmp_map);
    //  4. 绑定queue1-exchange1，且binding_key设置为queue1
    channel->queueBind("exchange1", "queue1", "queue1");
    //  5. 绑定queue2-exchange1，且binding_key设置为news.music.#
    channel->queueBind("exchange1", "queue2", "news.music.#");

    auto functor = std::bind(cb, channel, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    if (argc == 4) {
        if (channel->basicQos(static_cast<uint32_t>(prefetch)) == false) {
            std::cerr << "设置 prefetch 失败\n";
            return -1;
        }
    }
    channel->basicConsume(consumer_tag, argv[1], false, functor);

    while(1) std::this_thread::sleep_for(std::chrono::seconds(3));
    conn->closeChannel(channel);

    return 0;
}