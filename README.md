# mq

一个用 C++11 写的小型消息队列 broker，接口和概念模仿 RabbitMQ（交换机、队列、绑定、ACK、死信、prefetch），但规模小得多，设计优先考虑可读和易学，生产功能做了取舍。网络层用 muduo，协议用 protobuf，元数据存 SQLite，队列消息存 append-only 文件。

项目自带一个原型客户端库和示例程序，另有一个可选的 ROS2 桥接模块（详见下文「ROS2 bridge」）。

设计动机和实现细节见 [PROJECT.md](PROJECT.md)，本文只讲怎么用和会发生什么。

## 目录

- [快速开始](#快速开始)
- [客户端用法](#客户端用法)
- [核心概念](#核心概念)
- [消息的可靠性语义](#消息的可靠性语义)
- [持久化](#持久化)
- [ROS2 bridge](#ros2-bridge)
- [测试](#测试)
- [已知限制](#已知限制)

## 快速开始

### 依赖

- CMake >= 3.16，C++11 编译器（g++ 或 clang++ 均可）
- protobuf、SQLite3、zlib 的开发包
- 一个 C++ 标准库自带的 pthread；muduo 已经以静态库形式放在 `third_party/muduo/` 里，无需安装
- ROS2（只有构建 bridge 运行模式需要；不装也能编译 broker）

Ubuntu 上大致是：

```bash
sudo apt install build-essential cmake libprotobuf-dev protobuf-compiler \
    libsqlite3-dev zlib1g-dev
```

### 构建与运行

```bash
mkdir build && cmake -S . -B build && cmake --build build -j4
```

构建产物在 `build/bin/` 下：`mqserver`（broker）、`publish_client`、`consume_client`、`mq_bridge`。

启动 broker 并跑通最简单的发布/消费流程：

```bash
# 终端 1：默认数据目录 ./data/，默认端口 8085；都可以通过参数改
./build/bin/mqserver

# 终端 2：消费者先挂着
./build/bin/consume_client queue1 consumer1

# 终端 3：发布消息
./build/bin/publish_client
```

`consume_client` 的第 3 个参数可以设置 prefetch（同一消费者最多未确认消息数），例如 `./build/bin/consume_client queue1 consumer1 1`。

### 构建选项

| 选项 | 说明 |
| --- | --- |
| `MQ_BUILD_TESTS`（默认 ON） | 构建 gtest 单元测试 |
| `MQ_ENABLE_ASAN`（默认 OFF） | 打开 AddressSanitizer |
| `MQ_BUILD_ROS2_BRIDGE`（默认 OFF） | bridge 的 ROS2 运行模式；只有该目标会用 C++17 |

开发调试建议另开一个 ASan 构建目录：

```bash
cmake -S . -B build-asan -DMQ_ENABLE_ASAN=ON && cmake --build build-asan -j4
```

## 客户端用法

客户端只有一个连接类加一个信道类，同步阻塞式 API（每个请求最多等 5 秒响应）：

```cpp
#include "mq_connection.hpp"

// 1. 连接（AsyncWorker 里跑 muduo 事件循环，一条连接一个线程）
mq::AsyncWorker::ptr worker = std::make_shared<mq::AsyncWorker>();
mq::Connection::ptr conn = std::make_shared<mq::Connection>("127.0.0.1", 8085, worker);

// 2. 打开信道。上层操作都挂在信道上，返回的 shared_ptr 在订阅期间要持有
mq::Channel::ptr ch = conn->openChannel();
if (!ch) { /* broker 不在线或超时 */ }

// 3. 声明拓扑
google::protobuf::Map<std::string, std::string> args;
ch->declareExchange("exchange1", mq::ExchangeType::TOPIC, true, false, args);
ch->declareQueue("queue1", true, false, false, args);
ch->queueBind("exchange1", "queue1", "news.music.#");

// 4. 发布。第二个参数传 nullptr 时 broker 负责生成消息 id
mq::BasicProperties props;
props.set_id("demo-1");                  // 客户端自定义 id 时必须唯一
props.set_delivery_mode(mq::DeliveryMode::DURABLE);
props.set_routing_key("news.music.pop");
ch->basicPublish("exchange1", &props, "hello");
```

订阅是这样的：

```cpp
// 回调由客户端的线程池执行；手动 ACK 时回调返回前 ACK 已经发出，不会阻塞投递
void on_msg(mq::Channel::ptr &ch, const std::string &tag,
            const mq::BasicProperties *props, const std::string &body)
{
    // ... 处理 body ...
    ch->basicAck(props->id());
}

auto functor = std::bind(on_msg, ch, std::placeholders::_1,
                         std::placeholders::_2, std::placeholders::_3);
ch->basicQos(1);                                 // 可选：限流
ch->basicConsume("c1", "queue1", false, functor);// 第三个参数 false = 手动 ACK
```

注意几个容易踩的坑：

- 同一信道同一时刻只能有一个订阅者，换队列要先 `basicCancel()`。
- 消费失败可以 `basicReject(msg_id, /*requeue=*/true)` 归还队列，或 `false` 走死信。
- 信道由调用方持有引用，析构时会自动向 broker 发送取消订阅（不再等待响应）。
- 目前没有 Publisher Confirm：`basicPublish` 返回 true 只表示 broker 收到了请求并完成了路由判断，不代表消息一定已落盘、也不代表一定进了某个队列。

## 核心概念

交换机（exchange）、队列（queue）、绑定关系（binding）三件套和 AMQP 相同：消息发布到交换机，broker 按交换机类型和 binding key 匹配投递到队列。

交换机类型只有三种，路由匹配用动态规划的编辑距离式判定实现：

| 类型 | 匹配规则 |
| --- | --- |
| DIRECT | routing key 与 binding key 逐字节相等 |
| FANOUT | 忽略 key，投给所有绑定队列 |
| TOPIC | key 按 `.` 分段，`*` 一段任意、`#` 任意多段 |

合法字符集（DIRECT 交换机的 key 除外，它不转义）：

- **binding key** 只允许字母数字、`_`，以及分段通配符 `*`、`#`；格式错误（比如 `news.*a.foo`）会在 `queueBind` 时被服务端直接拒绝。
- **routing key** 只允许字母数字、`_`、`.`，不能含通配符。

每个 (exchange, queue) 对只允许一个 binding key。重复声明相同的 key 是幂等成功；声明不同的 key 会返回失败，需要先 `queueUnBind` 再重新绑定——这是和 RabbitMQ 多键共存语义的显式偏差，见 PROJECT.md 的取舍说明。

队列支持这些参数（声明时放进 `args`，key=value 用 `&` 连接）：

| 参数 | 作用 |
| --- | --- |
| `x-max-length` | 就绪消息条数上限，超过丢最旧 |
| `x-ack-timeout-ms` | ACK 超时毫秒数，0 表示关闭；默认 30000 |
| `x-dead-letter-exchange` | 死信交换机 |
| `x-dead-letter-routing-key` | 死信时的替换 routing key，缺省沿用原 key |
| `x-max-delivery-count` | 同一条消息最多投递次数，超过进死信 |

## 消息的可靠性语义

- **持久化由 `delivery_mode` 决定。** DURABLE 消息先写 `.mqd` 再入内存队列，broker 重启后恢复；不指定就是 UNDURABLE，只存在于内存中。
- **手动 ACK 模式下消息在 wait-ack 区间持有 harness**：消费端 ACK 前，消息既不在就绪队列也不会丢；ACK 超时（默认 30 秒，队列参数可改）会自动重投。
- **发布响应是同步语义**：broker 把请求处理完（含 DURABLE 消息写入 `.mqd`）才回复，路由到的某个队列写失败（磁盘满、消息 id 重复等）会让本次发布返回 false；交换机不匹配到任何队列时消息直接被丢弃，不会报错。依然没有 Publisher Confirm——写入不带 fsync，进程崩溃不丢、断电可能丢最后几条。
- **Reject(`requeue=true`) 不涉及死信**，消息原样放回队首等待重新投递。
- **Reject(`requeue=false`) 和 ACK 超时重投达到上限都会转入死信**：按队列的 `x-dead-letter-exchange` 重新发布；没配死信交换机就直接丢弃（只留日志）。死信转发时 `delivery_count` 归零，避免消息在新队列里立刻再次变成死信。
- **`x-max-length` 淘汰最旧消息时不走死信**，直接标记无效并记日志。
- **auto_ack 模式风险自理**：服务端发出即确认，崩溃/断线时这条消息不会重新投递。
- **顺序不保证**：重投（消费者断线、ACK 超时、Reject requeue）把消息放回队首，竞争消费时会乱序。单消费者手动 ACK 场景下顺序基本守恒，但不做承诺。

投递的吞吐上限由 dispatcher 批次大小（每队列一次最多 100 条）和消费者线程池决定；竞争消费用轮转 + prefetch 配额实现，消费者多次 ACK 后重新参与调度。

## 持久化

Broker 落盘的数据都放在数据目录下（默认 `./data/`，启动参数第一个是目录）：`<data_dir>/<queue>.mqd` 存队列消息，`<data_dir>/<queue>.mqd.tmp` 是 GC 时的临时文件，`meta.db` 是 SQLite 交换机/队列/绑定元数据。

`.mqd` 是 append-only 的 protobuf 消息序列，每条前面是平台的 `size_t` 长度前缀。文件内部没有索引，offset 全部保存在内存里，启动时重放整个文件来重建。写入不做 fsync：进程崩溃数据都在页缓存里不丢，断电可能丢最后几条——这正是没有 Publisher Confirm 的语义边界。

删除消息采用「无效标记 + GC 压实」策略：ACK/丢弃时服务端改写记录的 `valid` 字段为 "0"，有效记录的 offset 前移到垃圾比率高或文件超大时用一次性 GC（写临时文件 → 删旧文件 → 改名）回收空间。GC 失败时原文件和内存索引都保持原状，等下一轮再试，尽量避免因为 GC 中途失败而丢消息。

## ROS2 bridge

`mq_bridge` 进程可以把 MQ 话题桥接到 ROS2 话题（双向）。这个模块是可选的：

- 只做配置校验或纯正 MQ 模式时不需要 ROS2：`./build/bin/mq_bridge --config config/bridge.conf.example --check-config`
- 真正跑 ROS2 模式要用 `-DMQ_BUILD_ROS2_BRIDGE=ON` 构建，然后 `--run`。

配置文件是 protobuf text format，`config/bridge.conf.example` 里有完整注释，覆盖了 MQ↔ROS2 双方向、QoS 映射、retained store 的用法。

## 测试

```bash
ctest --test-dir build        # 13 个测试套件，覆盖存储、队列、路由、调度、连接、host 全流程
```

`mq_msgtest`、`mq_hosttest` 等测试会在当前目录读写 `data/`，请从项目根目录运行。CD 默认是构建目录时用 `ctest --test-dir build` 即可，无需手工 cd。

## 已知限制

这是一个学习/骨架项目，下面这些是有意为之的边界，不是 bug：

- **没有 Publisher Confirm**，客户端无法区分「broker 收到了」和「broker 持久化成功」。
- **文件损坏时孤儿队列可能丢消息**：`.mqd` 只做长度前缀包裹，断电时写到一半的记录在恢复时按长度解析，无法校验内容完整性（无 CRC）。
- **多种故障模式下的窗口**：broker 宕机的边界、断线期间的投递、重投与死信的时序等，没有做端到端形式化验证。
- **系统 `system("rm -rf")` 删目录**：路径会做字符白名单过滤，但只在小规模数据目录上验证过。
- **单 broker**，无集群、无主从、无权限控制，连接层不做认证。
- ROS2 bridge 依赖运行时查到的 `ros2` 环境，不是纯 C++ 链接依赖，机器上没装 rclcpp 也能编译其他目标。
