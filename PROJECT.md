# mq 项目完整文档

> 本文档介绍整个项目的设计、实现、运行方式和后续规划。
> 项目快速上手请参考 [README.md](README.md)，本文更侧重架构、语义和边界。
>
> 当前状态：
>
> - 第 1 步：队列调度器、竞争消费、断线重投——**已实现**
> - 第 2 步：prefetch/QoS、Reject、ACK 超时、死信队列、队列参数持久化——**已实现**
> - 第 3 步：ROS2 Bridge MVP——**双向数据面已实现，稳定运行能力仍有限**
>
> 独立 `mq_bridge` 会声明 MQ 拓扑、消费队列并发布到 ROS2，也会把 ROS2 消息
> 封装成 `BridgeEnvelope` 后发布到 MQ。配置、QoS 近似映射、retained 启动回放、
> 手动 ACK/Reject 和串行任务线程均已接通；自动重连、Publisher Confirm、
> 监控指标和自动化 ROS2 集成测试仍未实现。

---

## 目录

1. [项目定位](#1-项目定位)
2. [功能总览](#2-功能总览)
3. [总体架构](#3-总体架构)
4. [数据模型与协议](#4-数据模型与协议)
5. [消息生命周期](#5-消息生命周期)
6. [核心业务流程](#6-核心业务流程)
7. [QoS 与可靠性语义](#7-qos-与可靠性语义)
8. [并发模型与锁](#8-并发模型与锁)
9. [持久化](#9-持久化)
10. [构建、运行与测试](#10-构建运行与测试)
11. [代码导读](#11-代码导读)
12. [已知限制](#12-已知限制)
13. [路线图与 ROS2 Bridge MVP](#13-路线图与-ros2-bridge-mvp)
14. [总结](#14-总结)

---

## 1. 项目定位

`mq` 是一个使用 **C++11 + muduo + protobuf + SQLite** 实现的轻量级消息队列，核心模型参考 RabbitMQ：

- 交换机（Exchange）
- 队列（Queue）
- 绑定（Binding）
- 信道（Channel）
- 发布 / 订阅
- 手动 ACK / Reject
- prefetch（QoS）
- ACK 超时重投
- 死信队列

### 1.1 项目目标

这个项目的目标不是复刻一个生产级 RabbitMQ，而是把消息队列最核心的链路完整跑通，并让每个关键设计都能被解释和验证：

1. 客户端如何通过 TCP + protobuf 与 broker 通信；
2. 发布消息如何经过 exchange 路由到一个或多个队列；
3. 多个消费者如何竞争同一个队列，既不重复消费，也不让某个消费者饿死；
4. 消息从“就绪”到“投递中”再到“确认/重投/死信”的完整状态变化；
5. broker 重启后，队列元数据和持久化消息如何恢复；
6. 在多线程环境下，如何通过锁、原子变量和 `weak_ptr` 控制并发与生命周期。

### 1.2 非目标

以下内容不是当前阶段的目标：

- 生产级高可用、集群、副本和故障转移；
- 完整的 AMQP 协议兼容；
- exactly-once 语义；
- 完整的 ROS2 / DDS 能力；
- 管理控制台、监控大盘、鉴权和 TLS；
- 大包零拷贝、共享内存、RDMA 等高性能传输。

### 1.3 当前实现边界

当前版本适合作为：

- C++ 网络编程 / 后端方向的完整作品集项目；
- 学习消息队列核心机制的可运行样例；
- 演示 broker 与 ROS2 bridge 解耦、独立部署的示例。

当前版本不适合：

- 直接承载生产环境的关键任务；
- 需要严格顺序、事务或 exactly-once 的业务；
- 对外网暴露的服务。

---

## 2. 功能总览

### 2.1 已实现

| 能力 | 说明 |
| --- | --- |
| TCP + protobuf | 服务端基于 muduo，请求/响应使用 protobuf |
| Channel | 打开、关闭；一个连接可拥有多个信道 |
| Exchange | `DIRECT`、`FANOUT`、`TOPIC` 三种路由 |
| Queue | 声明、删除、持久化元数据 |
| Binding | 绑定、解绑、routing key 匹配 |
| Publish | 发布到 exchange，按绑定关系路由到一个或多个队列 |
| Consume | 订阅队列；同一队列支持多个消费者竞争消费 |
| ACK | 手动确认；确认后释放 prefetch 名额 |
| Reject | `requeue=true` 重投；`requeue=false` 丢弃或死信 |
| prefetch/QoS | 限制单个消费者同时在途的未 ACK 消息数 |
| 断线重投 | 消费者取消/连接断开时，按原投递顺序归还 wait-ack 消息 |
| ACK 超时 | 服务端每秒扫描；超时后重投或转死信 |
| delivery_count | 每次投递自增，用于最大投递次数和死信判断 |
| 死信队列 | 通过 `x-dead-letter-exchange` 路由死信 |
| x-max-length | 就绪消息超限时丢弃最旧消息 |
| 持久化 | SQLite 保存元数据，`.mqd` 文件保存消息体 |
| 二进制消息体 | `Message.Payload.body` / publish / consume 的 body 使用 protobuf `bytes`，可承载 CDR/envelope |
| 客户端超时 | 请求最多等待 5 秒，避免永久阻塞 |
| ROS2 Bridge 数据面 | 独立 `mq_bridge`：拓扑声明、MQ ↔ ROS2 双向转发、手动 ACK/Reject、retained 启动回放 |
| bridge 执行模型 | MQ/ROS2 回调只投递任务，跨域 publish/ACK 在单独串行线程执行 |
| 单元测试 | GoogleTest + CTest，测试使用独立数据目录 |

### 2.2 尚未实现

| 能力 | 状态 |
| --- | --- |
| bridge 自动重连 | 未实现；broker 断开后需要重启 bridge |
| bridge 自动化集成测试 | 未实现；当前用真实 broker + ROS2 手工 smoke test |
| Publisher Confirm / mandatory / return | 未实现 |
| basicNack / 批量 ACK | 未实现 |
| 消息 TTL、优先级队列 | 未实现 |
| 延迟队列、重试队列模板 | 未实现 |
| 死信循环检测 | 未实现 |
| 独占队列、自动删除队列 | 字段已保存，行为未实现 |
| 客户端自动重连 | 未实现 |
| TLS、鉴权、权限控制 | 未实现 |
| 监控指标 / Prometheus | 未实现 |
| 多 VirtualHost / 集群 | 未实现 |

---

## 3. 总体架构

### 3.1 架构图

```text
┌──────────────────┐        TCP + protobuf        ┌──────────────────────────────────────┐
│  publish_client  │ ───────────────────────────▶ │               mqserver               │
└──────────────────┘                              │                                      │
                                                  │  muduo TcpServer / EventLoop         │
┌──────────────────┐        TCP + protobuf        │        │                             │
│  consume_client  │ ◀─────────────────────────── │        ▼                             │
└──────────────────┘                              │  ConnectionManager                   │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  Connection                          │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  ChannelManager                      │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  Channel                             │
                                                  │   ├── 声明/删除 exchange             │
                                                  │   ├── 声明/删除 queue                │
                                                  │   ├── bind/unbind                    │
                                                  │   ├── publish                        │
                                                  │   ├── consume/cancel                 │
                                                  │   ├── ack/reject/qos                 │
                                                  │   └── 推送 basicConsumeResponse      │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  VirtualHost                         │
                                                  │   ├── ExchangeManager                │
                                                  │   ├── MsgQueueManager                │
                                                  │   ├── BindingManager                 │
                                                  │   └── MessageManager                 │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  ConsumerManager                     │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  QueueDispatcher ──▶ threadpool      │
                                                  │        │                             │
                                                  │        ▼                             │
                                                  │  SQLite(meta.db) + *.mqd             │
                                                  └──────────────────────────────────────┘
```

### 3.2 组件职责

| 组件 | 职责 |
| --- | --- |
| `mqserver` | 进程入口，解析数据目录和端口，启动 `Server` |
| `Server` | 拥有网络服务器、协议分发器、VirtualHost、消费者表、线程池和调度器 |
| `ConnectionManager` | `TcpConnectionPtr -> Connection` 的映射 |
| `Connection` | 管理一条 TCP 连接上的多个 `Channel` |
| `Channel` | 处理一个信道上的所有业务请求；最多绑定一个消费者 |
| `ProtobufCodec` | 帧解析、protobuf 序列化/反序列化 |
| `VirtualHost` | 组合四个管理器，对 Channel 提供统一业务接口 |
| `ExchangeManager` | 交换机元数据、类型、持久化 |
| `MsgQueueManager` | 队列元数据、队列参数、持久化 |
| `BindingManager` | exchange 与 queue 的绑定关系 |
| `MessageManager` | 按队列名管理消息存储句柄 |
| `QueueMessage` | 单个队列的内存消息状态（就绪 / wait-ack / 持久化索引） |
| `MessageMapper` | 单个队列 `.mqd` 文件的读写和 GC |
| `ConsumerManager` | 全局消费者注册表，按队列管理消费者 |
| `QueueDispatcher` | 队列到消费者的调度、prefetch 控制、竞争消费、超时唤醒 |
| `threadpool` | 执行消费者回调；默认 1 个工作线程 |
| `mq_bridge` | 独立进程；不链接进 broker，按需启用 rclcpp |
| `BridgeRuntime` | bridge 编排：拓扑声明、双向转发、retained 回放、任务队列和生命周期 |
| `MqClientAdapter` | 复用现有客户端库，封装 connect/declare/publish/consume，不感知 ROS2 |
| `RclcppRos2Bridge` | generic publisher/subscriber，只处理序列化字节 |

### 3.3 目录结构

```text
.
├── CMakeLists.txt
├── PROJECT.md                  # 本文档
├── README.md                   # 快速开始
├── LICENSE
├── cmake/
│   └── RunTest.cmake           # 为每个 CTest 目标准备独立数据目录
├── proto/
│   ├── mq_msg.proto            # Message、Payload、BasicProperties、ExchangeType
│   ├── mq_proto.proto          # 客户端/服务端请求与响应
│   ├── bridge_config.proto     # bridge 配置、方向、QoS、retained store
│   └── bridge_envelope.proto   # BridgeEnvelope 与 retained 持久化记录
├── config/
│   └── bridge.conf.example     # bridge 的 protobuf text 配置示例
├── src/
│   ├── common/
│   │   ├── mq_helper.hpp       # 文件、SQLite、UUID、字符串工具
│   │   ├── mq_logger.hpp       # 日志宏
│   │   └── mq_threadpool.hpp   # 线程池
│   ├── server/
│   │   ├── mq_server.cc        # broker 进程入口
│   │   ├── mq_broker.hpp       # Server：网络、定时器、请求分发
│   │   ├── mq_connection.hpp   # Connection / ConnectionManager
│   │   ├── mq_channel.hpp      # 服务端 Channel / ChannelManager
│   │   ├── mq_host.hpp         # VirtualHost 门面
│   │   ├── mq_exchange.hpp     # Exchange / ExchangeManager
│   │   ├── mq_queue.hpp        # MsgQueue / MsgQueueManager / 参数解析
│   │   ├── mq_binding.hpp      # Binding / BindingManager
│   │   ├── mq_message.hpp      # QueueMessage / MessageManager / MessageMapper
│   │   ├── mq_consumer.hpp     # Consumer / QueueConsumer / ConsumerManager
│   │   ├── mq_dispatcher.hpp   # QueueDispatcher
│   │   └── mq_route.hpp        # DIRECT / FANOUT / TOPIC 路由
│   ├── client/
│       ├── mq_connection.hpp   # 客户端 Connection / Channel 管理
│       ├── mq_channel.hpp      # 客户端 Channel：同步请求、消费者、ACK
│       ├── mq_worker.hpp       # 客户端 IO 线程 + 回调线程池
│       ├── publish_client.cc   # 发布示例
│       └── consume_client.cc   # 消费示例
│   └── bridge/
│       ├── main.cc             # --check-config / --run
│       ├── bridge_config.*     # protobuf text 解析、默认值、校验
│       ├── bridge_runtime.*    # 拓扑、双向转发、retained 回放、任务队列
│       ├── envelope_codec.*    # BridgeEnvelope 编解码/版本检查
│       ├── qos_mapper.*        # ROS2 QoS <-> MQ 近似映射
│       ├── retained_store.*    # 线程安全的最近状态缓存
│       ├── mq_client_adapter.* # 现有 MQ 客户端薄封装
│       └── ros2_bridge.*       # rclcpp generic pub/sub 或 stub
├── tests/                      # GoogleTest 单元测试与测试数据
├── third_party/
│   └── muduo/                  # 预编译 muduo 头文件和静态库
└── data/                       # 运行时生成，已被 .gitignore 忽略
```

---

## 4. 数据模型与协议

### 4.1 消息结构

核心消息定义在 `proto/mq_msg.proto`：

```proto
message BasicProperties {
    string id = 1;
    DeliveryMode delivery_mode = 2;
    string routing_key = 3;
    uint32 delivery_count = 4;
}

message Message {
    message Payload {
        BasicProperties properties = 1;
        bytes body = 2;
        string valid = 3;
    };
    Payload payload = 1;
    uint32 offset = 2;
    uint32 length = 3;
}
```

字段语义：

| 字段 | 说明 |
| --- | --- |
| `id` | broker 内消息唯一 id；为空时 broker 生成 UUID |
| `delivery_mode` | `UNDURABLE` 或 `DURABLE`；最终是否落盘还取决于目标队列 |
| `routing_key` | 发布时用于匹配 binding key |
| `delivery_count` | 该消息被投递的次数；broker 维护，发布端设置会被覆盖为 0 |
| `body` | 消息体，protobuf `bytes`；broker 按字节透传，不解释内容，可承载二进制 CDR/envelope |
| `valid` | 持久化文件中 `"1"` 有效、`"0"` 无效（待 GC） |
| `offset` / `length` | 消息 payload 在 `.mqd` 文件中的位置，由 `MessageMapper` 维护 |

> 重要：消息是否持久化不是只看 `delivery_mode`。
> 只有“目标队列 `durable=true` 且消息 `DURABLE`”才会写入 `.mqd`；
> 队列不 durable 时，消息会被强制当作 `UNDURABLE`。

> `body` 从 `string` 改为 `bytes` 是 wire-compatible 的：两者都使用
> length-delimited 编码，tag 和长度不变；但 `bytes` 不会做 UTF-8 校验，
> 因此 BridgeEnvelope 中的二进制 CDR 可以安全通过 publish/consume。

### 4.2 交换机与路由

| 类型 | 路由规则 |
| --- | --- |
| `DIRECT` | `routing_key == binding_key` |
| `FANOUT` | 忽略 routing key，投递给所有绑定队列 |
| `TOPIC` | `*` 匹配一个单词，`#` 匹配零个或多个单词 |

`TOPIC` 示例：

```text
binding key: news.music.#
routing key: news.music.pop      -> 匹配
routing key: news.music          -> 匹配
routing key: news.sport          -> 不匹配
```

一次发布可以命中多个队列。每个命中队列都会得到一份独立的消息记录；
消费者之间仍然是竞争关系，不会因为多个消费者订阅同一队列而重复投递。

### 4.3 请求与响应

`proto/mq_proto.proto` 定义了客户端和服务端之间的协议。

| 请求 | 响应 | 作用 |
| --- | --- | --- |
| `openChannelRequest` | `basicCommonResponse` | 打开信道 |
| `closeChannelRequest` | `basicCommonResponse` | 关闭信道 |
| `declareExchangeRequest` | `basicCommonResponse` | 声明交换机 |
| `deleteExchangeRequest` | `basicCommonResponse` | 删除交换机 |
| `declareQueueRequest` | `basicCommonResponse` | 声明队列 |
| `deleteQueueRequest` | `basicCommonResponse` | 删除队列 |
| `queueBindRequest` | `basicCommonResponse` | 绑定队列 |
| `queueUnBindRequest` | `basicCommonResponse` | 解绑队列 |
| `basicPublishRequest` | `basicCommonResponse` | 发布消息 |
| `basicConsumeRequest` | `basicCommonResponse` + 持续推送 `basicConsumeResponse` | 订阅队列 |
| `basicCancelRequest` | `basicCommonResponse` | 取消订阅 |
| `basicAckRequest` | `basicCommonResponse` | 确认消息 |
| `basicRejectRequest` | `basicCommonResponse` | 拒绝消息 |
| `basicQosRequest` | `basicCommonResponse` | 设置 prefetch |

### 4.4 `rid`、`cid`、`consumer_tag`

| 字段 | 作用 |
| --- | --- |
| `rid` | 请求 id。客户端等待相同 `rid` 的 `basicCommonResponse`，用于请求/响应匹配 |
| `cid` | 信道 id。服务端通过它找到对应 `Connection` 下的 `Channel` |
| `consumer_tag` | 同一队列内唯一，标识某次订阅；也是 wait-ack 消息的 owner |

补充说明：

- `cid` 由客户端生成，服务端原样带回。
- 一个 `Channel` 最多一个消费者；重复 `basicConsume` 会被拒绝。
- 同一队列的不同消费者必须使用不同 `consumer_tag`。
- 客户端请求最多等待 5 秒；超时返回失败，但服务端可能已经执行了请求。

---

## 5. 消息生命周期

### 5.1 状态图

```text
                         basicPublish
                              │
                              ▼
                    ┌──────────────────┐
                    │  就绪 _msgs      │
                    │  (ready)         │
                    └────────┬─────────┘
                             │ basicConsume
                             ▼
                    ┌──────────────────┐
                    │ wait-ack         │
                    │ _waitack_msgs    │
                    └───┬──────┬───────┘
                        │      │
                 basicAck│      │basicReject / ACK timeout
                        │      │
                        ▼      ▼
                   ┌────────┐  ┌──────────────────────┐
                   │ 已确认 │  │ requeue=true?        │
                   │ 删除   │  └──────┬───────────────┘
                   └────────┘         │
                                      ├── 是且未超最大次数 ──▶ 回到 ready
                                      │
                                      └── 否 / 已超最大次数
                                                │
                                                ▼
                                       ┌──────────────────┐
                                       │ 死信路由         │
                                       │ DLX / 丢弃       │
                                       └──────────────────┘
```

### 5.2 状态说明

| 状态 | 存放位置 | 说明 |
| --- | --- | --- |
| 就绪 | `QueueMessage::_msgs` | 可以被 `basicConsume` 取走 |
| wait-ack | `QueueMessage::_waitack_msgs` | 已投递，等待 ACK/Reject/超时/断线 |
| 持久化索引 | `QueueMessage::_durable_msgs` | `id -> Message`，用于更新 offset/length 和 GC |
| 已确认 | 不在集合中 | 持久化文件中的记录被标记 `valid="0"` |
| 死信 | 目标队列的 ready 状态 | 在新队列中 `delivery_count` 重置为 0 |

关键不变量：

1. 一条消息在任意时刻最多处于 `_msgs` 或 `_waitack_msgs` 之一。
2. `delivery_count` 在每次投递时加一，重投后保留。
3. `basicAck` 只对仍在 wait-ack 中的消息有效。
4. `basicReject` 和 ACK 超时会让消息离开 wait-ack，并归还消费者的 prefetch 名额。
5. 消费者取消/连接断开时，只归还该 tag 自己的 wait-ack 消息。

---

## 6. 核心业务流程

### 6.1 发布消息

```text
publish_client
  └─ basicPublish(exchange, properties, body)
       └─ Channel::basicPublish
            ├─ 检查 exchange 是否存在
            ├─ 读取 exchange 的所有 binding
            ├─ 对每个 binding 调用 Router::route()
            ├─ 命中的队列调用 VirtualHost::basicPublish()
            │    └─ MessageManager::insert()
            │         └─ QueueMessage::insert()
            │              ├─ 生成/校验消息 id
            │              ├─ 队列 durable && 消息 DURABLE -> 写 .mqd
            │              ├─ 加入 _msgs
            │              └─ 按 x-max-length 淘汰最旧就绪消息
            └─ 对每个命中队列调用 QueueDispatcher::notify()
```

路由结果：

- 没有命中任何队列：当前实现仍返回 `ok=true`，消息被丢弃。
- 命中多个队列：每个队列各有一份消息。
- exchange 不存在：返回 `ok=false`。
- 某个队列写入失败：整体 `ok=false`，但其他队列可能已经写入。

### 6.2 订阅与竞争消费

```text
consume_client
  └─ basicConsume(queue, consumer_tag, auto_ack, callback)
       └─ Channel::basicConsume
            ├─ 检查队列存在
            ├─ 检查当前 Channel 没有消费者
            ├─ 创建 Consumer（带 prefetch）
            ├─ 回复订阅成功
            └─ QueueDispatcher::notify(queue)

QueueDispatcher
  ├─ 同一队列同一时刻只允许一个 drain 任务
  ├─ ConsumerManager::choose() 轮转选择消费者
  ├─ 跳过达到 prefetch 上限的消费者
  ├─ QueueMessage::front() 取出队首消息并放入 wait-ack
  ├─ 调用 Channel 的推送回调
  └─ auto_ack ? 立即 ACK : 等待客户端 ACK/Reject
```

竞争消费语义：

- 同一队列的多个消费者是**竞争关系**，每条消息只投递给其中一个消费者。
- 新消费者不会获得历史消息的“补发副本”；它只会参与后续队列消息的竞争。
- 轮转选择会跳过已经达到 prefetch 上限的消费者，避免某个慢消费者占住队列。
- 每批最多处理 100 条，处理完后如果还有工作，会重新投递一个 drain 任务，避免大队列长期占用一个工作线程。

### 6.3 ACK

```text
消费端 callback
  └─ basicAck(message_id)
       └─ Channel::basicAck
            ├─ VirtualHost::basicAck()
            │    └─ QueueMessage::remove()
            │         ├─ 从 _waitack_msgs 删除
            │         ├─ 持久化消息标记 valid="0"
            │         └─ 返回 owner consumer_tag
            ├─ ConsumerManager::onAck() 归还 prefetch 名额
            └─ QueueDispatcher::notify() 唤醒队列
```

特点：

- ACK 是幂等的：重复 ACK、ACK 已超时/已重投的消息不会使 broker 崩溃。
- 服务端对 ACK 请求通常返回 `ok=true`，即使消息已经不在 wait-ack 中。
- 消费端如果使用 `auto_ack=true`，broker 会在回调返回后自动 ACK，消费端不需要再调用 `basicAck`。
- 手动 ACK 模式下，消费端必须在处理成功后显式 ACK，否则消息会在超时后重投。

### 6.4 Reject

```text
消费端失败
  └─ basicReject(message_id, requeue)
       └─ Channel::basicReject
            ├─ VirtualHost::basicReject()
            │    ├─ QueueMessage::reject()
            │    │    ├─ 从 _waitack_msgs 移除
            │    │    ├─ requeue=true 且未达 x-max-delivery-count -> 放回队首
            │    │    └─ 否则 -> dead_letter=true
            │    └─ 如果是死信 -> routeDeadLetter()
            ├─ ConsumerManager::onAck() 归还 prefetch 名额
            └─ notify(源队列) + notify(死信目标队列)
```

语义：

| 条件 | 结果 |
| --- | --- |
| `requeue=true` 且未达到最大投递次数 | 放回队首，等待再次投递 |
| `requeue=true` 但已达到最大投递次数 | 转死信 |
| `requeue=false` | 直接转死信；没有 DLX 则丢弃 |
| 消息不在 wait-ack 中 | 返回 `found=false` |

### 6.5 取消订阅与连接断开

```text
basicCancel / Channel 析构
  ├─ ConsumerManager::remove(tag, queue)
  ├─ VirtualHost::requeueByConsumer(queue, tag)
  │    └─ 只挑选 owner == tag 的 wait-ack 消息
  │         ├─ 按原投递 sequence 排序
  │         ├─ 依次 push_front 回 _msgs
  │         └─ 重新应用 x-max-length
  └─ QueueDispatcher::notify(queue)
```

为什么先移除消费者再 requeue：

- 防止调度器在 requeue 期间又把消息投给一个已经取消的消费者。
- 如果调度器已经取出消息但消费者刚好被移除，`QueueDispatcher` 会检测
  `ConsumerManager::exists()`，把消息按 tag 回滚，避免留在 wait-ack。

### 6.6 ACK 超时

服务端在 `Server::start()` 中注册每秒执行一次的定时器：

```cpp
_baseloop.runEvery(1.0, std::bind(&Server::onAckTimeout, this));
```

流程：

```text
每秒
  └─ QueueDispatcher::checkAckTimeout()
       └─ VirtualHost::requeueExpired(now_ms)
            └─ 遍历所有队列
                 └─ QueueMessage::requeueExpired()
                      ├─ 找出 deadline_ms <= now_ms 的 wait-ack 消息
                      ├─ 按 sequence 排序
                      ├─ 未达最大投递次数 -> 放回队首
                      └─ 达到最大投递次数 -> 标记死信
       ├─ ConsumerManager::onAck() 归还每个超时消息的 prefetch 名额
       ├─ notify(源队列)
       └─ notify(死信目标队列)
```

配置：

- `x-ack-timeout-ms` 默认 `30000`（30 秒）。
- `x-ack-timeout-ms=0` 表示关闭 ACK 超时。
- 超时扫描粒度是 1 秒，因此实际超时时间可能在配置值之后最多约 1 秒。
- 超时重投会保留原 `message_id`，并递增 `delivery_count`。

### 6.7 死信队列

死信来源：

1. `basicReject(requeue=false)`；
2. `basicReject(requeue=true)` 但达到 `x-max-delivery-count`；
3. ACK 超时且达到 `x-max-delivery-count`。

路由规则：

```text
源队列 args:
  x-dead-letter-exchange = "dlx"
  x-dead-letter-routing-key = "dead"   # 可选

死信消息:
  routing_key = x-dead-letter-routing-key
                或原消息的 routing_key
  delivery_count = 0
```

然后按 `dlx` 的类型和绑定关系重新发布：

- `DIRECT`：精确匹配；
- `FANOUT`：所有绑定队列；
- `TOPIC`：通配符匹配。

如果 DLX 不存在、没有匹配队列，或消息写入目标队列失败，消息会被丢弃。
当前没有死信跳数/循环检测：如果 DLX 又路由回源队列，可能形成反复投递。

### 6.8 `x-max-length`

`x-max-length` 只约束“就绪消息”（`_msgs`），不约束 wait-ack 消息。

触发时机：

- 新消息入队后；
- 消息重新入队后；
- 消费者断线/取消后归还消息后；
- 队列恢复后；
- 动态调小上限后。

淘汰策略：

- 丢弃最旧的就绪消息（队首）；
- 非持久化消息直接从内存删除；
- 持久化消息在 `.mqd` 中标记 `valid="0"`，并触发 GC 检查。

因此，如果消费者长期不 ACK，队列中的总消息量仍可能超过 `x-max-length`，
因为 wait-ack 不计入上限。

---

## 7. QoS 与可靠性语义

### 7.1 prefetch

`basicQos(prefetch_count)` 是信道级设置：

- `0`：不限制；
- `N > 0`：当前消费者最多同时持有 N 条未 ACK 消息。

实现方式：

- `Consumer::prefetch_count` 和 `unacked_count` 是原子变量；
- `Consumer::hasCapacity()` 判断是否还能接收消息；
- `QueueConsumer::choose()` 轮转时跳过满额消费者；
- ACK/Reject/超时/断线后归还名额，并唤醒队列。

### 7.2 自动 ACK 与手动 ACK

| 模式 | 行为 | 适用场景 |
| --- | --- | --- |
| `auto_ack=true` | 回调返回后 broker 立即 ACK | 允许丢失的遥测、日志、状态广播 |
| `auto_ack=false` | 客户端显式 `basicAck` / `basicReject` | 任务、需要失败重试的业务 |

手动 ACK 模式下，消费者必须保证：

1. 处理成功后再 ACK；
2. 处理失败时调用 `basicReject(requeue=true)` 或按业务转入死信；
3. 回调要幂等，因为超时/断线可能造成重复投递。

### 7.3 投递语义

当前实现是 **at-least-once**：

- 消费者处理成功但 ACK 丢失：消息超时后重投；
- 消费者实际处理失败：Reject 后重投；
- 消费者断线：wait-ack 消息重新入队；
- 服务端或客户端崩溃：持久化消息可以恢复，但 `delivery_count` 会重置。

不是 exactly-once，也没有事务。需要幂等的业务应在消费端做去重。

### 7.4 顺序

- 单队列内，正常情况下按入队顺序投递。
- 重投消息会按原投递 `sequence` 排序后放回队首，因此不会把同一批消息的顺序打乱。
- 死信消息进入新队列后，顺序由新队列的入队顺序决定。
- 多个消费者并行消费时，业务层面的处理完成顺序不保证与入队顺序一致。

### 7.5 `delivery_count`

- 发布时由 broker 重置为 0；
- 每次 `basicConsume` 投递时加一；
- 通过 `basicConsumeResponse.properties.delivery_count` 传给消费者；
- 用于 `x-max-delivery-count` 判断；
- 死信消息进入目标队列时重置为 0；
- **不跨重启持久化**：消息体会恢复，但重投次数从 0 开始。

### 7.6 队列参数

| 参数 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `x-max-length` | 正整数 | 无上限 | 就绪消息上限；0 或非法值按无上限 |
| `x-ack-timeout-ms` | 非负整数 | `30000` | ACK 超时；0 表示关闭 |
| `x-max-delivery-count` | 正整数 | 不限制 | 最大投递次数；0/非法值按不限制 |
| `x-dead-letter-exchange` | 字符串 | 空 | 死信交换机名 |
| `x-dead-letter-routing-key` | 字符串 | 空 | 死信 routing key；空则沿用原 key |

参数更新规则：

- 队列已存在时，只有本次声明显式携带的合法参数才会更新；
- 空 `args` 的重复声明不会清掉已有参数；
- 只有 durable 队列的参数变化会写回 SQLite。

---

## 8. 并发模型与锁

### 8.1 线程模型

服务端：

```text
main thread
  └─ Server::start()
       ├─ _baseloop.loop()        # muduo EventLoop
       │    ├─ accept / IO / 请求 handler
       │    └─ 每秒 ACK 超时定时器
       └─ threadpool               # 默认 1 个工作线程
            └─ QueueDispatcher::drainLoop()
                 └─ 消费者 callback
```

- 请求解析和大部分元数据操作在 muduo IO 线程执行。
- 消费者回调在线程池执行，避免慢回调阻塞网络 IO。
- `QueueDispatcher::notify()` 可以在 IO 线程调用，实际投递在工作线程。
- `BasicProperties` 指针只在回调期间有效；服务端推送时会复制字段。

客户端：

```text
AsyncWorker
  ├─ loopthread   # TcpClient IO 线程
  └─ pool         # 默认 1 个工作线程，执行消费回调
```

客户端收到 `basicConsumeResponse` 后，把 `channel->consume()` 投递到线程池，
用户回调不在网络线程执行。这样回调中的同步 ACK/Reject 不会阻塞 IO。

bridge：

```text
MQ AsyncWorker pool ──onMqMessage──┐
                                  ├──▶ BridgeRuntime::TaskQueue（单线程串行）
ROS2 executor ──onRos2Message─────┘
                                         │
                                         ├─ ROS2 publish
                                         ├─ MQ publish
                                         └─ ACK / Reject
```

两个方向的回调都只做轻量复制和入队，不在 MQ 回调线程或 ROS2 executor
线程里执行同步网络请求。任务队列上限 4096；满时 MQ → ROS2 走
`reject(requeue=true)`，ROS2 → MQ 只能记录后丢弃（没有 Publisher Confirm）。

### 8.2 主要锁

| 锁 | 保护对象 | 说明 |
| --- | --- | --- |
| `ConsumerManager::_mutex` | `_qconsumers` | 全局消费者注册表 |
| `QueueConsumer::_mutex` | `_consumers`、`_rr_seq` | 单队列消费者列表和轮转游标 |
| `MessageManager::_mutex` | `_queue_msgs` | 队列名到 QueueMessage 的映射 |
| `QueueMessage::_mutex` | `_msgs`、`_waitack_msgs`、`_durable_msgs`、计数 | 单队列消息状态 |
| `QueueDispatcher::_mutex` | `_states` | 每个队列的 running/pending 状态 |
| `ChannelManager::_mutex` | `_channels` | 连接内信道表 |
| `ConnectionManager::_mutex` | `_conns` | TCP 连接表 |
| 客户端 `Channel::_mutex` | `_basic_resp` | 请求响应等待 |
| 客户端 `Channel::_consumer_mutex` | `_consumer` | 消费者生命周期 |
| `BridgeRuntime::TaskQueue::_mutex` | 任务 deque / running 状态 | 串行任务队列 |
| `MqClientAdapter::Impl::mutex` | 连接、信道、消费者信道 | 同步网络请求串行化 |
| `RclcppRos2Bridge::_publish_mutex` | publisher 表 | 任务线程发布 |
| `RclcppRos2Bridge::_handler_mutex` | 消息 handler | 设置/读取回调 |

### 8.3 锁顺序与避免嵌套

代码遵循“先取注册表锁，再操作具体对象，但不嵌套持有”的原则：

```text
ConsumerManager::_mutex
  └─ 取出 QueueConsumer::ptr，释放锁
       └─ QueueConsumer::_mutex

MessageManager::_mutex
  └─ 取出 QueueMessage::ptr，释放锁
       └─ QueueMessage::_mutex
```

这样可以避免：

- 全局锁和单队列锁互相等待；
- 删除队列时长时间阻塞其他队列；
- 消费者 ACK 和队列调度之间形成锁环。

### 8.4 生命周期与 `weak_ptr`

- `QueueDispatcher` 必须通过 `std::make_shared` 创建；`notify()` 捕获
  `shared_from_this()`，保证 drain 期间对象存活。
- `QueueDispatcher` 对线程池使用 `weak_ptr`，避免 dispatcher 与 threadpool
  形成循环引用。
- 服务端消费者的 callback 捕获 `weak_ptr<Channel>`；Channel 析构后回调直接返回。
- 客户端 `consumeResponse` 捕获 `shared_ptr<Channel>` 和 `shared_ptr<message>`，
  保证线程池任务执行期间对象有效。
- `ConnectionManager` 删除连接后，最后一个 `shared_ptr` 释放会触发
  `Connection -> Channel` 的析构清理。
- `BridgeRuntime::stop()` 先清 `_running`、停止任务线程，再 `basicCancel`
  取消 MQ 消费者，最后销毁 ROS2 node；这样 in-flight 任务仍能安全访问
  适配器，未处理的 MQ 消息由 broker 重新投递。
- `RclcppRos2Bridge` 的 subscription 回调按值捕获 `TopicConfig`，不持有
  `BridgeConfig` 的引用；运行时回调通过 `BridgeRuntime*` 回到编排器。

### 8.5 调度器状态机

每个队列在 `QueueDispatcher` 中有一个状态：

```text
running=false, pending=false   空闲
running=true,  pending=false   正在处理一批
running=true,  pending=true    正在处理，同时有新唤醒
running=false, pending=true    处理结束发现还有工作，补一个任务
```

规则：

1. 同一队列同一时刻最多一个 drain 任务；
2. `pending` 只表示“还有工作”，不会创建无限任务；
3. 每批最多 100 条，剩余工作通过 pending 继续；
4. 消费者全部取消时 `notify()` 直接返回，等下次订阅再唤醒。

---

## 9. 持久化

### 9.1 存储布局

```text
data/
├── meta.db              # SQLite：exchange / queue / binding 元数据
└── message/
    ├── queue1.mqd       # queue1 的消息记录
    ├── queue2.mqd
    └── *.mqd.tmp        # GC 过程中的临时文件
```

SQLite 表：

| 表 | 内容 |
| --- | --- |
| `exchange_table` | 交换机名、类型、durable、auto_delete、args |
| `queue_table` | 队列名、durable、exclusive、auto_delete、args |
| `binding_table` | exchange 名、queue 名、binding key |

说明：

- 只有 durable 的 exchange/queue/binding 会持久化。
- binding 的持久化条件是 `exchange.durable && queue.durable`。
- 非 durable 队列只存在于内存，重启后消失。

### 9.2 `.mqd` 文件格式

```text
[size_t 长度][Message::Payload 字节][size_t 长度][Message::Payload 字节]...
```

- 长度字段是 `sizeof(size_t)`，在 x86-64 上为 8 字节。
- 新消息总是追加到文件末尾。
- ACK/丢弃时不移动后续数据，而是把 Payload 中的 `valid` 从 `"1"` 改成 `"0"`。
- 因为只改一个字段值，protobuf 序列化长度不变，可以安全地原地覆盖。
- `Message.offset` 指向 payload 起始位置，`Message.length` 是 payload 长度。

为什么不能在 ACK 时直接重新序列化整条消息：

- 投递后 `delivery_count` 会变化，序列化长度可能和落盘时不同；
- 直接覆盖会破坏文件结构；
- 因此 ACK 时读取原始记录，只修改 `valid` 字段。

### 9.3 恢复流程

```text
VirtualHost 构造
  └─ MsgQueueManager 从 SQLite 恢复队列元数据
       └─ 对每个队列：
            ├─ 解析 x-max-length
            ├─ MessageManager::initQueueMessage()
            │    └─ QueueMessage::recovery()
            │         ├─ MessageMapper::gc()
            │         │    ├─ 读取 .mqd 中所有有效记录
            │         │    ├─ 写入 .tmp
            │         │    ├─ 删除旧文件
            │         │    └─ 重命名为 .mqd
            │         ├─ 加入 _msgs
            │         ├─ 建立 _durable_msgs 索引
            │         └─ 按 x-max-length 裁剪
            └─ ConsumerManager::initQueueConsumer()
```

恢复后的消息全部是“就绪”状态；上次进程退出时未 ACK 的消息没有 owner，
会重新参与投递。

### 9.4 垃圾回收（GC）

`MessageMapper::gc()` 会：

1. 读取 `.mqd` 中所有 `valid != "0"` 的记录；
2. 写入 `.mqd.tmp`；
3. 删除旧 `.mqd`；
4. 将临时文件重命名为正式文件；
5. 返回有效消息列表。

触发条件：

- 每次队列恢复时强制执行一次；
- 消息被 ACK/丢弃后，如果 `_total_count > 2000` 且有效比例低于 50%，
  触发一次 GC。

### 9.5 持久化边界

会持久化的内容：

- durable 的 exchange/queue/binding 元数据；
- durable 队列中的 DURABLE 消息 body 和 properties；
- 队列参数（`x-max-length` 等）。

不会持久化的内容：

- wait-ack 的 owner 和 deadline；
- `delivery_count` 的运行时值；
- 非 durable 队列及其消息；
- 消费者的订阅关系；
- 死信路由过程中的临时状态。

因此，broker 重启后：

- 消息体会恢复；
- 消费关系需要客户端重新建立；
- 未 ACK 消息会变成就绪消息；
- `delivery_count` 从 0 开始。

---

## 10. 构建、运行与测试

### 10.1 依赖

Ubuntu / Debian：

```bash
sudo apt update
sudo apt install -y \
    build-essential cmake \
    libprotobuf-dev protobuf-compiler \
    libsqlite3-dev zlib1g-dev \
    libgtest-dev
```

要求：

- CMake >= 3.16
- 支持 C++11 的编译器（GCC 7+ / Clang 6+）
- protobuf >= 3.x
- `third_party/muduo` 中已包含 muduo 头文件和静态库

> 预编译 muduo 是 x86-64 Linux 版本。ARM / macOS 需要自行重新编译 muduo，
> 并替换 `third_party/muduo/lib`。

### 10.2 构建

```bash
cmake -S . -B build
cmake --build build -j
```

构建产物：

```text
build/bin/mqserver
build/bin/publish_client
build/bin/consume_client
build/bin/mq_bridge
build/bin/mq_*test
```

常用选项：

```bash
# 不构建测试
cmake -S . -B build -DMQ_BUILD_TESTS=OFF

# ASan 构建
cmake -S . -B build -DMQ_ENABLE_ASAN=ON
cmake --build build -j

# Release
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
```

ROS2 bridge 默认不链接 rclcpp，需要显式打开，并让 CMake 找到 ROS2：

```bash
# 已安装 ROS2 Humble 时
cmake -S . -B build-ros2 \
    -DMQ_BUILD_ROS2_BRIDGE=ON \
    -DCMAKE_PREFIX_PATH=/opt/ros/humble
cmake --build build-ros2 -j
```

关闭时 `mq_bridge` 仍会构建，但只支持 `--check-config`；
`--run` 会明确提示没有编译 ROS2 支持。打开后 bridge 目标使用 C++17，
broker 目标仍保持 C++11。

protobuf 生成的 `.pb.cc/.pb.h` 在构建目录中，不提交到仓库。

### 10.3 运行

终端 1：启动 broker

```bash
./build/bin/mqserver
# 指定数据目录和端口
./build/bin/mqserver ./data/ 8085
```

终端 2：订阅

```bash
./build/bin/consume_client queue2
./build/bin/consume_client queue2 consumer-A
./build/bin/consume_client queue2 consumer-B 1
```

第三个参数是 prefetch：

```bash
# 每个消费者最多同时持有 1 条未 ACK 消息
./build/bin/consume_client queue2 consumer-A 1
```

终端 3：发布

```bash
./build/bin/publish_client
```

默认示例：

- `exchange1` 是 `TOPIC` 交换机；
- `queue1` 绑定 key `queue1`；
- `queue2` 绑定 key `news.music.#`；
- `publish_client` 使用 `news.music.pop`，消息进入 `queue2`。

### 10.4 ROS2 bridge 数据面

校验配置并打印 topic 计划：

```bash
./build/bin/mq_bridge --config config/bridge.conf.example --check-config
```

打开 ROS2 支持后，先启动 broker，再启动双向数据面：

```bash
./build/bin/mqserver ./data/ 8085
./build-ros2/bin/mq_bridge --config config/bridge.conf.example --run
```

`--run` 的实际行为：

- 连接 MQ broker，按 topic 配置声明 exchange、queue 和 binding；
- `MQ_TO_ROS2`：创建 generic publisher，消费 MQ 队列，解码
  `BridgeEnvelope`，把 `cdr` 原样发布到 ROS2；
- `ROS2_TO_MQ`：创建 generic subscription，把 CDR 封装成
  `BridgeEnvelope`，发布到配置的 exchange/routing key；
- 启动时加载 retained store，并在创建 ROS2 端点后回放最近状态；
- MQ 手动 ACK 模式下，只有 ROS2 publish 成功才 ACK；失败
  `reject(requeue=true)`；无法解析或 topic/type 不匹配的毒消息
  `reject(requeue=false)`；
- 两个方向统一投递到 bridge 串行任务线程，避免阻塞 ROS2 executor 或
  MQ 回调线程。

配置文件使用 protobuf text format，示例见
[`config/bridge.conf.example`](config/bridge.conf.example)。`exchange_type`
支持 `EXCHANGE_DIRECT`、`EXCHANGE_FANOUT`、`EXCHANGE_TOPIC`，未填时默认
`EXCHANGE_TOPIC`；同一个 exchange 被多个 topic 引用时必须保持一致。

当前边界：

- ROS2 → MQ 没有 Publisher Confirm，是 at-most-once；发布请求失败只记录日志；
- broker 断开后不会自动重连；
- bridge 的 retained store 是 bridge 自己的辅助状态，不参与 broker
  ACK/重投；
- 当前没有自动化真实 ROS2 集成测试，使用手工 smoke test 验证。

### 10.5 测试

```bash
ctest --test-dir build --output-on-failure
```

只跑某个测试：

```bash
ctest --test-dir build -R mq_routetest --output-on-failure
```

每个测试都会在 `build/test-run/<test_name>/` 下获得一份独立的
`tests/data` 副本，避免 SQLite 状态互相污染。

当前 CTest 共有 13 个目标：

| 测试 | 覆盖内容 |
| --- | --- |
| `mq_bindingtest` | 绑定持久化/恢复 |
| `mq_channeltest` | 信道管理 smoke test |
| `mq_connectiontest` | 连接管理 smoke test |
| `mq_consumertest` | 消费者插入/删除/轮转/prefetch 容量 |
| `mq_dispatchertest` | 竞争消费、断线重投、prefetch、ACK 超时 |
| `mq_exchangetest` | 交换机基本操作 |
| `mq_filetest` | 文件工具 smoke test |
| `mq_hosttest` | VirtualHost、max-length、durable ACK、Reject、超时、死信、二进制 body |
| `mq_msgtest` | 消息插入、持久化、max-length |
| `mq_queuetest` | 队列参数解析、声明更新 |
| `mq_routetest` | DIRECT/TOPIC 路由 key 合法性 |
| `mq_bridge_coretest` | 配置解析/校验、exchange_type、envelope 往返、QoS 映射、retained store、BridgeRuntime 启动前校验 |
| `mq_bridge_config_check` | 示例配置通过 CLI `--check-config` |

说明：

- `mq_channeltest`、`mq_connectiontest`、`mq_filetest` 目前更接近 smoke test，
  不是完整的行为测试。
- 核心行为覆盖主要在 `mq_dispatchertest` 和 `mq_hosttest`。
- `mq_bridge_coretest` 只覆盖纯内存/协议部分；真实 MQ ↔ ROS2 链路目前用
  手工 smoke test 验证，自动化集成测试待补。

### 10.6 当前验证结果

在 ROS2 bridge 双向数据面落地后：

- 普通构建（`build`）：成功；
- `ctest --test-dir build --output-on-failure`：13/13 通过；
- ASan 构建（`build-asan-tests`）：成功；
- `ctest --test-dir build-asan-tests --output-on-failure`：13/13 通过；
- `MQ_BUILD_ROS2_BRIDGE=ON` 构建（`build-ros2`）：成功；
- `ctest --test-dir build-ros2 --output-on-failure`：13/13 通过；
- 真实 broker + ROS2 Humble 手工 smoke test：
  - ROS2 → MQ：`/bridge/task` 的 `std_msgs/msg/String` 进入 MQ 队列，
    `BridgeEnvelope` 中的 CDR 内容为 `hello from ros2`；
  - MQ → ROS2：注入 envelope 后 `/chatter` 收到 `hello from mq`；
  - retained：持久化文件生成，bridge 重启后回放成功。

---

## 11. 代码导读

### 11.1 服务端请求链路

```text
TcpServer
  └─ ProtobufCodec::onMessage
       └─ ProtobufDispatcher
            └─ Server::onXxx()
                 ├─ ConnectionManager::getConnection(conn)
                 ├─ Connection::getChannel(cid)
                 └─ Channel::xxx(req)
                      ├─ VirtualHost / ConsumerManager / QueueDispatcher
                      └─ ProtobufCodec::send(response)
```

### 11.2 客户端链路

```text
Connection 构造
  ├─ AsyncWorker::loopthread.startLoop()
  ├─ TcpClient::connect()
  └─ 等待连接建立

Channel::xxx()
  ├─ 生成 rid
  ├─ 构造 protobuf 请求
  ├─ _codec->send()
  └─ waitResponse(rid) 最多 5 秒

网络线程收到响应
  └─ Connection::basicResponse()
       └─ Channel::putBasicResponse()
            └─ 条件变量唤醒等待线程
```

消费消息：

```text
网络线程收到 basicConsumeResponse
  └─ Connection::consumeResponse()
       └─ AsyncWorker::pool.push()
            └─ Channel::consume()
                 └─ Consumer::callback()
```

### 11.3 关键不变量

阅读代码时，可以优先抓住这些不变量：

1. `QueueDispatcher` 对同一队列最多一个 drain 任务；
2. `QueueMessage` 的一条消息不能同时处于 ready 和 wait-ack；
3. `Consumer::unacked_count` 不会因为重复 ACK 变成负数；
4. `ConsumerManager` / `MessageManager` 不在持有全局锁时调用具体对象；
5. 消费者取消时，先 remove 再 requeue，最后 notify；
6. `basicConsume` 先安装客户端消费者，再发送请求；
7. 服务端 `basicConsume` 先回复成功，再 notify 队列；
8. `basicAck`/`basicReject`/超时都会归还 prefetch 名额；
9. 持久化消息 ACK 时只把 `valid` 改成 `"0"`，不重新序列化整条消息；
10. 线程池任务持有必要的 `shared_ptr`，回调持有 `weak_ptr<Channel>`。

### 11.4 bridge 数据面链路

```text
mq_bridge --check-config
  ├─ BridgeConfigLoader::loadFromFile()
  │    ├─ protobuf TextFormat 解析
  │    ├─ applyDefaults()
  │    └─ validate()
  └─ printSummary() / QosMapper::toMq()

mq_bridge --run（打开 MQ_BUILD_ROS2_BRIDGE 时）
  ├─ rclcpp::init()
  ├─ BridgeRuntime::start()
  │    ├─ MqClientAdapter::connect()
  │    ├─ declareTopology(): exchange / queue / binding
  │    ├─ RetainedStore::load()
  │    ├─ createRos2Bridge(): generic pub/sub
  │    ├─ replayRetained()
  │    └─ consume() for MQ_TO_ROS2 topics
  ├─ rclcpp::spin(node)
  └─ BridgeRuntime::stop()
       ├─ 停止串行任务线程
       ├─ MqClientAdapter::stop()：basicCancel + 归还 wait-ack
       └─ 销毁 ROS2 node
```

各模块的职责边界：

- `BridgeRuntime` 只做编排，不解析 CDR，也不直接操作 muduo；
- `EnvelopeCodec` 只处理 `BridgeEnvelope`，不解析 CDR；
- `QosMapper` 只做近似参数映射，不改变 broker 队列语义；
- `RetainedStore` 只保存最近状态，不参与 ACK/重投；
- `MqClientAdapter` 只封装现有 MQ 客户端，不包含 ROS2 类型；
- `Ros2Bridge` 只处理 ROS2 序列化消息，不直接访问 broker。

MQ → ROS2 与 ROS2 → MQ 都由 `TaskQueue` 串行执行。回调线程只负责复制
消息并入队，因此 `basicPublish`、`basicAck`、`basicReject` 这些可能等待
broker 响应的同步请求不会卡住 ROS2 executor。

---

## 12. 已知限制

以下是当前版本刻意保留的边界，不是“忘了做”：

### 12.1 协议与可靠性

- **没有 Publisher Confirm / mandatory / return**：
  发布端无法确认 broker 是否收到、是否路由到队列；没有匹配队列时仍可能返回成功。
- **只实现单条 `basicReject`**：
  没有 `basicNack(multiple)`，也不支持批量确认；消费端需要自己记录失败消息 id。
- **没有 `basicGet`**：
  只能通过 `basicConsume` 订阅，不能同步拉取单条消息。
- **at-least-once**：
  超时、断线、Reject 都可能重投，不是 exactly-once；消费端必须幂等。
- **请求超时不等于请求没执行**：
  客户端 5 秒没收到响应会返回失败，但服务端可能已经处理了请求。
- **ACK 对未知消息不报错**：
  服务端通常返回 `ok=true`，便于客户端重试；调试时不能只依赖响应判断消息是否存在。

### 12.2 队列与调度

- **没有 per-subscriber 历史回放**：
  队列是工作队列，消费即出队；新订阅者不会获得历史消息副本。
  ROS2 latched/retained 语义由 bridge 的 retained store 实现，不改变队列语义。
- **`x-max-length` 只约束就绪消息**：
  已被取走但未 ACK 的消息不计入上限；消费者长期不 ACK 时总量仍可能超限。
- **`delivery_count` 不持久化**：
  服务重启后从 0 开始；持久化消息体会恢复，但重投次数不会。
- **最大投递次数只在 Reject/超时检查**：
  消费者反复断线导致的重投不会立即检查 `x-max-delivery-count`。
- **死信环路未检测**：
  DLX 如果又把消息路由回源队列，可能形成反复投递。
- **没有消息 TTL、优先级、延迟队列**。
- **没有死信 header（如 x-death）**：
  死信消息只保留原属性并重置 `delivery_count`。

### 12.3 队列/连接管理

- **`exclusive` / `auto_delete` 字段未真正生效**：
  字段会被保存和传递，但当前没有独占连接限制，也不会在最后一个消费者退出后自动删除队列。
- **一个 Channel 最多一个消费者**：
  不能在同一信道上订阅多个队列。
- **没有客户端自动重连**：
  连接断开后需要重新创建 `Connection`、`Channel` 和订阅。
- **客户端连接没有超时/失败返回**：
  `Connection` 构造会阻塞等待 TCP 连接建立；broker 未启动时可能一直等待。
- **没有服务端命名队列**：
  队列名必须由客户端提供。
- **重复声明已存在队列不会更新 `durable`/`exclusive`/`auto_delete`**：
  只更新显式携带的合法 args。

### 12.4 存储与安全

- **SQL 通过字符串拼接**：
  队列名、交换机名和参数没有做 SQL 转义；当前只适合可信输入。
- **队列名直接参与 `.mqd` 文件路径**：
  不建议使用包含路径分隔符的队列名。
- **args 使用 `&` 和 `=` 拼接，未做 URL 编码**：
  参数值中不能包含 `&`；否则解析会出错。
- **没有 TLS、鉴权、权限控制**：
  默认只适合本地学习。
- **没有监控面板 / Prometheus**：
  目前只有日志，没有队列深度、消费者数、超时/死信计数等指标。
- **没有集群 / 高可用 / 副本**。
- **muduo 是预编译静态库**：
  换架构需要自行重新编译。

### 12.5 性能

- **消息体统一走 protobuf + TCP**：
  没有大包零拷贝、共享内存或 RDMA。
- **默认单工作线程**：
  `Server` 的线程池默认 1 个线程；消费者回调是串行执行的。需要更高并发时需要调整线程池大小并重新评估锁竞争。
- **`.mqd` 追加写 + 周期性 GC**：
  文件会先增长，再在阈值触发时重写；写放大和磁盘占用需要根据业务评估。

### 12.6 ROS2 bridge

- **没有自动重连 / 连接超时**：
  broker 断开后 `mq_bridge` 不会重新连接、重新声明拓扑或恢复 consumer；
  当前需要重启 bridge。客户端 `Connection` 构造函数没有连接超时，broker
  不可达时启动可能阻塞，因此应先启动 broker。连接对象也没有健康检查接口。
- **ROS2 → MQ 没有 Publisher Confirm**：
  发布请求失败会记录日志，但无法证明消息一定没有进入队列；
  因此该方向是 at-most-once。
- **背压有限**：
  串行任务队列上限 4096；满时 MQ → ROS2 会 requeue，ROS2 → MQ 只能丢弃。
  MQ 侧 prefetch 使用 `depth` 近似，但多 topic 时总在途量仍可能较大。
- **双向配置可能形成回环**：
  如果 ROS2 → MQ 的发布目标队列又被同一 bridge 的 MQ → ROS2 消费，消息会在
  两个 topic 之间循环；BIDIRECTIONAL 使用同一个 topic 时也会回环。示例配置
  因此为两个方向使用不同的 routing key 和队列。
- **没有 bridge 指标**：
  只有日志，没有转发计数、延迟、丢弃/重投计数。
- **QoS 只是子集**：
  只映射 reliability/durability/history depth；不会自动给已有队列补
  `x-ack-timeout-ms` / `x-max-delivery-count`，需要用户在 `queue_args` 中显式配置。
- **自动化集成测试缺失**：
  核心测试不启动 ROS2；真实链路目前靠手工 smoke test。
- **bridge 目标使用 C++17**：
  rclcpp 要求更高标准；broker 本身仍保持 C++11。

---

## 13. 路线图与 ROS2 Bridge MVP

### 13.1 已完成阶段

#### 第 1 步：竞争消费与调度

- 引入 `QueueDispatcher`；
- 同一队列同一时刻只允许一个 drain 任务；
- 发布、订阅、ACK、断线后统一唤醒队列；
- 多消费者轮转分摊；
- 消费者断线后按原顺序重新入队；
- 每批 100 条，避免大队列饿死其他队列。

#### 第 2 步：QoS、Reject、超时与死信

- `basicQos(prefetch_count)`；
- `Consumer::unacked_count` 原子计数；
- `basicReject(requeue)`；
- ACK 超时扫描与重投；
- `delivery_count`；
- `x-max-delivery-count`、`x-ack-timeout-ms`；
- 死信交换机路由；
- 队列参数持久化；
- 对应单元测试。

### 13.2 第 3 步：ROS2 Bridge MVP（双向数据面已实现）

> 当前仓库已有独立 `mq_bridge`、`MQ_BUILD_ROS2_BRIDGE` 选项和完整的
> MQ ↔ ROS2 双向转发；broker 仍不依赖任何 ROS2 库。MVP 的数据面已经
> 可以运行，但自动重连、Publisher Confirm、指标和自动化 ROS2 集成测试
> 仍属于后续稳定化工作。

#### 13.2.1 设计目标

在独立进程中把 ROS2 topic 与当前 broker 连接起来：

1. **MQ → ROS2**：把 MQ 中的消息发布到 ROS2 topic；
2. **ROS2 → MQ**：把 ROS2 topic 的消息发布到 MQ exchange/queue；
3. 支持常见传感器/任务场景的 QoS 近似映射；
4. 为 latched / transient-local 类需求提供 retained-state 缓存；
5. broker 继续保持 ROS2 无关，不把 `rclcpp`、DDS 类型或 CDR 解析逻辑放进 broker。

#### 13.2.2 非目标

MVP 不包含：

- 完整 DDS QoS 语义（deadline、lifespan、liveliness、lease duration 等）；
- ROS2 service / action；
- 多机器人命名空间自动发现；
- 内省/动态发现所有 topic；
- exactly-once；
- 跨机时钟同步；
- 大包零拷贝。

#### 13.2.3 进程边界

```text
┌─────────────────────┐        ┌──────────────────────────┐        ┌──────────────────┐
│ ROS2 world           │        │ mq_bridge (新进程)       │        │ mq broker         │
│                      │        │                          │        │                  │
│ rclcpp pub/sub  ◀───┼───────▶│  ROS2 adapter            │        │  TCP + protobuf  │
│                      │        │  BridgeEnvelope codec    │◀──────▶│  Exchange/Queue  │
│                      │        │  QoS mapper              │        │  ACK/Reject      │
│                      │        │  Retained store          │        │  DLX             │
└─────────────────────┘        └──────────────────────────┘        └──────────────────┘
```

关键原则：

- broker 不引入 `rclcpp`，不解析 CDR；
- bridge 使用现有客户端库连接 broker；
- CDR 二进制放在 `Message.body` 中，broker 只按字节透传；协议字段因此使用
  protobuf `bytes`，避免 UTF-8 校验拒绝二进制 envelope；
- retained store 属于 bridge，不改变 broker 的队列语义；
- 通过 `MQ_BUILD_ROS2_BRIDGE=ON` 条件编译，默认关闭，保证不装 ROS2 也能构建 broker。

#### 13.2.4 `BridgeEnvelope`

bridge 需要一个稳定的消息封装。实际文件在
[`proto/bridge_envelope.proto`](proto/bridge_envelope.proto)，由 bridge 自己生成和解析，
不进入 broker 的协议依赖。

实际字段：

```proto
syntax = "proto3";
package mq.bridge;

import "bridge_config.proto";

message BridgeEnvelope {
    uint32 version = 1;              // 从 1 开始
    string topic = 2;                // ROS2 topic 名
    string ros_type = 3;             // 例如 sensor_msgs/msg/Imu
    uint64 stamp_ns = 4;             // bridge 接收/生成时间
    bytes cdr = 5;                   // ROS2 序列化后的 CDR 字节
    string source = 6;               // 来源标识
    string correlation_id = 7;       // 关联 id / 去重键
    BridgeReliability reliability = 8;
    BridgeDurability durability = 9;
    uint32 depth = 10;               // 建议的 ROS2 history depth
    map<string, string> headers = 11;
}
```

`mq::bridge::EnvelopeCodec` 负责 `encode/decode/validate`：version 为 0 时
自动填入当前版本，未来版本会被拒绝；cdr 字节不被 bridge 修改。

说明：

- `cdr` 对 broker 完全不透明；
- `ros_type` 用于 bridge 选择反序列化类型；
- `correlation_id` 可用于消费端幂等或日志追踪；
- `headers` 可携带 `frame_id`、机器人 id、QoS 原始配置等扩展信息。

#### 13.2.5 retained store

为什么不能直接让 broker 做 retained：

- broker 的队列是工作队列，消费即出队；
- 如果为每个新订阅者回放历史消息，会改变现有队列语义；
- retained 需求只属于 bridge 的 ROS2 侧。

实际实现：

- 类：`mq::bridge::RetainedStore`，线程安全；
- key：`topic`（后续可扩展为 `topic + key`）；
- value：序列化后的 `BridgeEnvelope` 和 `updated_ms`；
- 每个 topic 默认只保留 1 条，可配置容量；
- 支持 TTL；
- 内存为主，配置 `persist_path` 时用 protobuf 文件原子写盘；
- `load()` 在 bridge 启动时恢复 retained 状态；
- `BridgeRuntime::start()` 在 MQ 消费开始前调用 `replayRetained()`，把最新
  envelope 的 CDR 发布到对应 ROS2 publisher；
- ROS2 侧是配置的 durability：`TRANSIENT_LOCAL` 时 publisher 自己也会缓存
  样本，晚加入的 transient-local 订阅者可以拿到；`VOLATILE` 时只有 bridge
  启动回放，不能保证 bridge 启动后才加入的订阅者收到历史样本；
- retained store 不参与任务队列的 ACK/重投语义。

典型场景：

```text
/imu/data       -> 只关心最新姿态       -> retained 1 条
/robot/status   -> 只关心最新状态       -> retained 1 条
/task           -> 每条任务都要处理     -> 不做 retained，走 MQ 工作队列
```

#### 13.2.6 QoS 近似映射

ROS2/DDS QoS 策略很多，MVP 只映射最常用的三项：reliability、durability、depth。

| ROS2 QoS | MQ 侧近似 | 说明 |
| --- | --- | --- |
| `RELIABLE` | 手动 ACK；队列/交换机 durable=true | 允许重投；`x-ack-timeout-ms` / `x-max-delivery-count` 需在 `queue_args` 显式配置 |
| `BEST_EFFORT` | `auto_ack=true`；队列/交换机 durable=false | 允许丢失，不重投 |
| `TRANSIENT_LOCAL` | retained store（store 启用时）+ transient_local publisher | bridge 启动时回放；晚加入的 transient-local 订阅者可拿到 DDS 缓存 |
| `VOLATILE` | 默认不启用 retained | 只接收在线期间的消息；显式 `retained: true` 可额外使用 bridge 缓存 |
| `depth=N` | ROS2 history depth；best-effort 可补 `x-max-length` | 不是严格的 DDS history，reliable 任务队列不自动截断 |
| `deadline` | 不支持 | 需要 DDS 层能力 |
| `lifespan` | 不支持 | 后续可用消息 TTL 近似 |
| `liveliness` | 不支持 | 需要 DDS 层能力 |

注意：

- 这是“近似映射”，不是完整 QoS 兼容；
- 任务队列必须使用手动 ACK，不能为了性能静默丢任务；
- 遥测队列可以使用 `auto_ack` + `x-max-length=1` 保留最新状态；
- retained store 只保存最近状态，不能替代 broker 队列的可靠投递。

#### 13.2.7 两种典型 profile

**遥测 / 状态（MQ → ROS2）**

```text
ROS2 订阅者（晚加入）
        ▲
        │ transient_local / retained
        │
   mq_bridge
        │  BEST_EFFORT: consume + auto_ack
        │  RELIABLE: consume + 手动 ACK
        ▼
MQ queue: telemetry.imu
  x-max-length = 1
  x-dead-letter-exchange = 可选
```

特点：

- 只关心最新值；
- 旧值可以被 `x-max-length` 淘汰；
- BEST_EFFORT 由 broker auto_ack，发布失败无法重投；需要至少一次时改用
  RELIABLE，bridge 会在 ROS2 publish 成功后才 ACK；
- retained store 为 bridge 启动回放提供最近状态，`TRANSIENT_LOCAL` 还能让
  晚加入的同类订阅者拿到 DDS 缓存。

**任务（ROS2 → MQ）**

```text
ROS2 task publisher
        │
        ▼
   mq_bridge
        │  basicPublish
        ▼
MQ exchange/queue: task.queue
  durable = true
  x-ack-timeout-ms = 30000
  x-max-delivery-count = 3
  x-dead-letter-exchange = dlx
        │
        ▼
任务消费者（手动 ACK）
```

特点：

- 每条任务都必须处理；
- 手动 ACK，失败重投；
- 达到最大次数进入死信；
- 不使用 retained store。

#### 13.2.8 配置

实际配置使用 protobuf text format，由
[`proto/bridge_config.proto`](proto/bridge_config.proto) 定义，
[`BridgeConfigLoader`](src/bridge/bridge_config.hpp) 负责解析、补齐默认值和语义校验。
这样不需要为了配置再引入 YAML/JSON 依赖。

实际示例（节选，完整文件见
[`config/bridge.conf.example`](config/bridge.conf.example)）：

```text
node_name: "mq_bridge"
mq_host: "127.0.0.1"
mq_port: 8085

retained_store {
    enabled: true
    capacity_per_topic: 1
    ttl_sec: 0
    persist_path: "./data/bridge-retained.db"
}

topics {
    name: "/chatter"
    ros_type: "std_msgs/msg/String"
    direction: MQ_TO_ROS2
    exchange: "exchange1"
    exchange_type: EXCHANGE_TOPIC
    routing_key: "news.music.pop"
    queue: "queue2"
    reliability: RELIABLE
    durability: VOLATILE
    depth: 10
    retained: true
}
```

校验规则包括：

- `node_name`、`mq_host`、`mq_port` 合法；
- `MQ_TO_ROS2` 必须有 `queue`，`ROS2_TO_MQ` 必须有 `exchange`；
- topic 名不能重复；
- `retained` 只能用于包含 MQ → ROS2 的方向；
- `exchange_type` 只能是 `EXCHANGE_DIRECT` / `EXCHANGE_FANOUT` /
  `EXCHANGE_TOPIC`，未填时默认 `EXCHANGE_TOPIC`；
- 同一个 exchange 被多个 topic 引用时，`exchange_type` 必须一致；
- reliability、durability、depth、exchange_type 会补默认值
  （RELIABLE / VOLATILE / 10 / TOPIC）。

#### 13.2.9 模块划分

```text
proto/
├── bridge_config.proto        # 配置、方向、QoS、exchange_type、retained store
└── bridge_envelope.proto      # BridgeEnvelope + retained 持久化格式

src/bridge/
├── main.cc                    # --check-config / --run 生命周期
├── bridge_config.hpp/.cc      # protobuf text 配置解析、默认值、校验
├── bridge_runtime.hpp/.cc     # 拓扑声明、双向转发、retained 回放、任务队列
├── envelope_codec.hpp/.cc     # BridgeEnvelope 编解码/版本校验
├── qos_mapper.hpp/.cc         # ROS2 QoS <-> MQ 近似参数
├── retained_store.hpp/.cc     # 线程安全最近状态缓存 + 可选文件持久化
├── mq_client_adapter.hpp/.cc  # 复用现有 MQ 客户端库
├── ros2_bridge.hpp/.cc        # 可选 rclcpp generic pub/sub 适配器
└── ...

config/
└── bridge.conf.example        # 可直接 --check-config 的示例
```

CMake 实际结构：

```cmake
option(MQ_BUILD_ROS2_BRIDGE "Build mq_bridge with ROS2/rclcpp support" OFF)

protobuf_generate_cpp(BRIDGE_PROTO_SRCS BRIDGE_PROTO_HDRS
    proto/bridge_config.proto
    proto/bridge_envelope.proto)

add_library(mq_bridge_core STATIC
    ${BRIDGE_PROTO_SRCS}
    src/bridge/bridge_config.cc
    src/bridge/bridge_runtime.cc
    src/bridge/envelope_codec.cc
    src/bridge/mq_client_adapter.cc
    src/bridge/qos_mapper.cc
    src/bridge/retained_store.cc
    src/bridge/ros2_bridge.cc
)
target_link_libraries(mq_bridge_core PUBLIC mq_core)

if(MQ_BUILD_ROS2_BRIDGE)
    find_package(rclcpp REQUIRED)
    target_compile_features(mq_bridge_core PUBLIC cxx_std_17)
    target_compile_definitions(mq_bridge_core PUBLIC MQ_WITH_ROS2=1)
    target_link_libraries(mq_bridge_core PUBLIC rclcpp::rclcpp)
endif()

add_executable(mq_bridge src/bridge/main.cc)
target_link_libraries(mq_bridge PRIVATE mq_bridge_core)
```

说明：

- `mq_bridge_core` 始终构建，因此不装 ROS2 也能校验配置和跑核心测试；
- `MQ_BUILD_ROS2_BRIDGE=OFF` 时 `ros2_bridge.cc` 编译为明确报错的 stub；
- 只有打开选项时 bridge 目标才使用 C++17 并链接 `rclcpp`；
- broker 的 `mq_core` 仍只依赖 C++11/muduo/protobuf/SQLite。

#### 13.2.10 消息流程（已实现）

**MQ → ROS2**

```text
1. BridgeRuntime::start()
   a. MqClientAdapter::connect()
   b. 按 topic 配置声明 exchange / queue / binding
   c. RetainedStore::load()
   d. 创建 ROS2 generic publisher/subscriber
   e. replayRetained()
   f. 对 MQ_TO_ROS2 topic 调用 consume()
2. MQ 回调只把 MqDelivery 投递到 TaskQueue
3. 任务线程：
   a. 解析 BridgeEnvelope
   b. 校验 version / topic / ros_type
   c. 把 envelope.cdr 原样交给 generic publisher
   d. 如果 retained 启用，保存原始 envelope 字节
   e. publish 成功 -> ACK；失败 -> reject(requeue=true)
   f. envelope 无法解析或 topic/type 不匹配 -> reject(requeue=false)
4. bridge 启动时 retained 回放发生在消费开始前；TRANSIENT_LOCAL publisher
   还会为晚加入的 transient-local 订阅者保留最新样本
```

注意：bridge 不反序列化 CDR，也不理解 ROS2 消息字段；它只校验 envelope
和配置的 topic/type 是否匹配，然后透传序列化字节。

**ROS2 → MQ**

```text
1. ROS2 subscriber 回调只复制 SerializedMessage 的字节并投递到 TaskQueue
2. 任务线程：
   a. 生成 BridgeEnvelope（cdr、topic、ros_type、stamp、source、
      correlation_id、QoS 快照）
   b. EnvelopeCodec::encode()
   c. 构造 BasicProperties（id、delivery_mode、routing_key）并调用
      MqClientAdapter::publish()
3. MQ 消费端手动 ACK/Reject
4. 失败任务按队列的 x-ack-timeout-ms / x-max-delivery-count / DLX 处理
```

当前客户端没有 Publisher Confirm，因此第 2.c 步只能确认 publish 请求得到
broker 的普通响应，不能确认消息已经持久化或路由到队列。

#### 13.2.11 里程碑

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| 3.0 | 独立进程、配置校验、envelope、QoS 映射、retained store、MQ/ROS2 适配器框架 | **已完成** |
| 3A | MQ → ROS2 单 topic、envelope、retained store、QoS 映射、手动 ACK/Reject | **已完成**：真实 broker + `std_msgs/msg/String` 手工 smoke test |
| 3B | ROS2 → MQ 单 topic、CDR、durable task queue、BridgeEnvelope 编码 | **已完成**：真实 ROS2 publisher + MQ consumer 手工 smoke test |
| 3C | 多 topic 配置、拓扑声明、串行任务队列、retained 持久化与启动回放 | **已完成**：多 topic 配置结构和启动回放已接入 |
| 3D | 自动重连、指标、Publisher Confirm、自动化 ROS2 集成测试 | **未完成** |

#### 13.2.12 验收标准

MVP 已满足：

1. 不打开 `MQ_BUILD_ROS2_BRIDGE` 时，broker 构建和测试完全不受影响；
2. broker 源码不包含 `rclcpp` / DDS 头文件；
3. `mq_bridge --config ... --check-config` 能拒绝非法配置并打印计划；
4. envelope、QoS 映射、retained store、exchange_type 有单元测试；
5. MQ → ROS2 的 CDR 字节不被修改，`std_msgs/msg/String` 手工 smoke test 通过；
6. ROS2 → MQ 能从真实 ROS2 publisher 收到消息，并在 MQ 队列中看到完整
   `BridgeEnvelope`；
7. reliable MQ 消费在 ROS2 publish 成功后才 ACK，失败走 Reject/requeue；
8. retained store 可持久化，bridge 重启后 `replayRetained()` 能重新发布最近状态。

仍需稳定化：

1. broker 断开后自动重连并恢复 consumer/拓扑；
2. bridge 指标（转发、重投、丢弃、延迟）；
3. Publisher Confirm / mandatory 返回；
4. 自动化真实 ROS2 集成测试和长时间压力测试。

#### 13.2.13 测试计划

已实现：

- `mq_bridge_coretest`：envelope 编解码/版本兼容、QoS 映射、retained store
  容量/TTL/重启恢复、配置解析/默认值/非法配置、exchange_type 冲突、
  BridgeRuntime 启动前校验失败路径；
- `mq_hosttest`：额外覆盖含 NUL/非 UTF-8 字节的二进制消息体；
- `mq_bridge_config_check`：示例配置通过 CLI `--check-config`；
- 手工 smoke test（真实 broker + ROS2 Humble）：
  - MQ → ROS2：注入 `BridgeEnvelope`，`ros2 topic echo` 收到 `std_msgs/msg/String`；
  - ROS2 → MQ：`ros2 topic pub` 后 MQ 队列收到完整 envelope，CDR 内容一致；
  - retained：持久化文件生成，重启 bridge 后回放成功。

待补：

- 真实 broker + ROS2 的自动化集成测试；
- 拓扑声明、ACK/Reject、断线重连测试；
- 100Hz IMU + 图像大包 demo，观察延迟和丢包。

#### 13.2.14 风险与对策

| 风险 | 对策 |
| --- | --- |
| CDR 对齐/字节序 | 使用 ROS2 已经序列化好的 `SerializedMessage`，不在 bridge 手写 CDR 解析 |
| QoS 语义无法完全映射 | 明确“近似映射”，在配置和文档中标注 |
| retained store 内存增长 | 每 topic 限制条数 + TTL + 监控 |
| 回调线程与 ROS2 executor 竞争 | MQ 回调和 ROS2 executor 都只复制消息并投递到串行任务队列；publish/ACK 不在回调线程执行 |
| task queue 无界增长 | 上限 4096；MQ → ROS2 满时 requeue，ROS2 → MQ 记录后丢弃 |
| bridge 重启丢状态 | retained store 可选持久化；MQ 消息依赖 broker 持久化 |
| 重复投递 | 使用 `correlation_id` + 消费端幂等 |
| 大包阻塞 | 串行任务队列会顺序处理大包；后续可拆分队列/限流 |
| broker 断线 | 当前需要重启 bridge；后续补自动重连和拓扑恢复 |

#### 13.2.15 为什么采用独立进程

- **解耦**：broker 不依赖 ROS2，不装 ROS2 也能构建；
- **可替换**：未来可以换 DDS 实现或增加 MQTT bridge；
- **故障隔离**：bridge 崩溃不影响 broker；
- **部署灵活**：broker 可以部署在服务器，bridge 部署在机器人端；
- **语义清晰**：队列语义和 ROS2 QoS 语义分别由各自组件负责，避免把 retained 逻辑塞进 broker。

### 13.3 后续可选方向

在 ROS2 Bridge MVP 之后，可以按优先级考虑：

1. **bridge 自动重连**：broker 断开后重新连接、恢复 topology 和 consumer；
2. **bridge 集成测试**：真实 broker + ROS2 双向链路、retained 回放、失败重投；
3. **Publisher Confirm**：让发布端知道 broker 是否成功落盘/入队；
4. **basicNack(multiple) / 批量 ACK**：降低高频消费的协议开销；
5. **消息 TTL / 延迟队列**：支持超时取消、延迟重试；
6. **监控指标**：队列深度、消费者数、wait-ack 数、超时数、死信数、bridge 转发数；
7. **更好的持久化重投计数**：把 `delivery_count` 写入持久化记录；
8. **死信环路检测**：限制最大跳数；
9. **TLS / 鉴权 / 权限控制**；
10. **多 VirtualHost / 集群 / 副本**。

---

## 14. 总结

这个项目的核心价值在于把消息队列的关键链路做完整，而不是堆功能：

- 用 `Exchange + Binding + Queue` 解释消息如何路由；
- 用 `wait-ack` 和 `basicAck` 解释可靠投递；
- 用 `prefetch` 解释消费者流控；
- 用 `delivery_count + ACK 超时 + Reject + DLX` 解释失败处理；
- 用 `QueueDispatcher` 解释多消费者竞争和并发调度；
- 用 SQLite + `.mqd` 解释元数据和消息体的持久化；
- 用 `PROJECT.md` 中的规划说明如何在不污染 broker 的前提下接入 ROS2。

当前第 1/2 步已经形成可运行、可测试的闭环；第 3 步的 ROS2 Bridge
双向数据面也已接通，可以用真实 broker + ROS2 做手工验证。
继续演进时应坚持两个边界：

1. **broker 不依赖 ROS2**；
2. **retained state 属于 bridge，不改变队列的工作队列语义**。

接下来最有价值的工作不是继续堆功能，而是把 bridge 的重连、指标和集成测试
补齐，让它在断线、背压和长时间运行下也可观测、可恢复。

---

## 附录 A：常用命令速查

```bash
# 构建
cmake -S . -B build
cmake --build build -j

# 测试
ctest --test-dir build --output-on-failure

# ASan
cmake -S . -B build-asan -DMQ_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

# ROS2 bridge 构建（Humble）
source /opt/ros/humble/setup.bash
cmake -S . -B build-ros2 -DMQ_BUILD_ROS2_BRIDGE=ON
cmake --build build-ros2 -j
ctest --test-dir build-ros2 --output-on-failure

# 启动 broker
./build/bin/mqserver ./data/ 8085

# 消费
./build/bin/consume_client queue2 consumer-A 1

# 发布
./build/bin/publish_client

# bridge 配置检查 / 运行
./build/bin/mq_bridge --config config/bridge.conf.example --check-config
./build-ros2/bin/mq_bridge --config config/bridge.conf.example --run
```

## 附录 B：术语表

| 术语 | 含义 |
| --- | --- |
| Exchange | 交换机，负责按类型和 binding 路由消息 |
| Binding | exchange 与 queue 的绑定关系 |
| Routing key | 发布时用于匹配 binding 的 key |
| Binding key | 绑定时设置的匹配模式 |
| Ready | 就绪消息，等待投递 |
| wait-ack | 已投递但未确认的消息 |
| prefetch | 消费者允许的未 ACK 消息上限 |
| ACK | 确认消息已处理 |
| Reject | 拒绝消息，可选择重投或死信 |
| DLX | Dead Letter Exchange，死信交换机 |
| delivery_count | 消息被投递的次数 |
| retained store | bridge 侧保存的最近状态，用于 ROS2 late joiner |
| CDR | ROS2/DDS 的序列化格式 |
