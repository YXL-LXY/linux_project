# mq

一个用 **C++11 + muduo + protobuf** 实现的轻量级消息队列，核心模型参考 RabbitMQ：
交换机（Exchange）、队列（Queue）、绑定（Binding）、信道（Channel）、发布/订阅与手动 ACK。

项目目标是**把消息队列的核心链路讲清楚、跑起来**，而不是做一个生产级中间件。
当前版本适合作为 C++ 网络编程 / 后端实习项目的作品集。

## 架构

```text
┌──────────────┐        TCP + protobuf        ┌─────────────────────────────────────┐
│ publish_client│ ───────────────────────────▶ │                mqserver              │
└──────────────┘                              │                                     │
                                              │  Connection / Channel               │
┌──────────────┐        TCP + protobuf        │        │                            │
│ consume_client│ ◀─────────────────────────── │        ▼                            │
└──────────────┘                              │  VirtualHost                        │
                                              │   ├── ExchangeManager  (交换机)      │
                                              │   ├── MsgQueueManager  (队列元数据)  │
                                              │   ├── BindingManager   (绑定关系)    │
                                              │   └── MessageManager   (消息存储)    │
                                              │          │                          │
                                              │          ├── SQLite: data/meta.db    │
                                              │          └── 文件:   data/*.mqd      │
                                              └─────────────────────────────────────┘
```

一条消息的路径：

```text
publish
  └─▶ Exchange ──(routing key)──▶ Binding ──▶ Queue
                                                │
                                       QueueDispatcher
                                                │  多个消费者轮转竞争 + prefetch 限制
                                                ▼
                                       consumer callback
                                                │
                                 basicAck / basicReject
                                                │
                             ACK 超时扫描 → 重投 / 死信队列
```

支持三种交换机：

| 类型 | 路由规则 |
| --- | --- |
| `DIRECT` | routing key 与 binding key 完全相等 |
| `FANOUT` | 忽略 routing key，投递给所有绑定队列 |
| `TOPIC` | `*` 匹配一个单词，`#` 匹配零个或多个单词，例如 `news.music.#` |

## 目录结构

```text
.
├── CMakeLists.txt          # 顶层构建脚本
├── cmake/
│   └── RunTest.cmake       # 为每个测试复制独立的数据目录
├── proto/
│   ├── mq_msg.proto        # 消息、属性、交换机类型
│   └── mq_proto.proto      # 客户端与服务器之间的请求/响应
├── src/
│   ├── common/             # 日志、文件/SQLite 工具、线程池
│   ├── server/             # broker：连接、信道、交换机、队列、绑定、消息存储
│   └── client/             # 客户端库 + publish/consume 示例
├── tests/                  # GoogleTest 单元测试 + SQLite 测试数据
├── third_party/
│   └── muduo/              # 预编译的 muduo 头文件与静态库（x86-64 Linux）
├── data/                   # 运行时生成，已被 .gitignore 忽略
└── README.md
```

## 依赖

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
- `third_party/muduo` 中自带 muduo 头文件和静态库，不需要单独安装 muduo

> muduo 静态库目前是 **x86-64 Linux** 下编译的。如果你在 ARM 或 macOS 上构建，
> 需要自己重新编译 muduo，并替换 `third_party/muduo/lib`。

## 构建

```bash
cmake -S . -B build
cmake --build build -j
```

构建产物在 `build/bin/`：

```text
build/bin/mqserver
build/bin/publish_client
build/bin/consume_client
build/bin/mq_*test
```

常用选项：

```bash
# 不构建测试，只构建 broker 和客户端
cmake -S . -B build -DMQ_BUILD_TESTS=OFF

# 打开 AddressSanitizer（排查 use-after-free / 越界）
cmake -S . -B build -DMQ_ENABLE_ASAN=ON
cmake --build build -j

# Release 构建
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
```

protobuf 的 `.pb.cc/.pb.h` 在构建时生成到 `build/`，不提交到仓库。

## 运行

准备三个终端，都在仓库根目录执行。

终端 1：启动 broker（默认监听 `0.0.0.0:8085`，数据写到 `./data/`）

```bash
./build/bin/mqserver
# 也可以指定数据目录和端口
./build/bin/mqserver ./data/ 8085
```

终端 2：订阅 `queue2`

```bash
./build/bin/consume_client queue2
```

同一个队列可以启动多个消费者，用不同的 consumer tag 分摊消息：

```bash
./build/bin/consume_client queue2 consumer-A
./build/bin/consume_client queue2 consumer-B
```

第三个可选参数是 prefetch：每个消费者最多同时持有多少条未 ACK 消息。

```bash
# 每个消费者同一时间最多处理 1 条，ACK 后才继续投递
./build/bin/consume_client queue2 consumer-A 1
```

终端 3：发布消息

```bash
./build/bin/publish_client
```

`consume_client` 会打印：

```text
consumer1消费了消息：Hello World-0
consumer1消费了消息：Hello World-1
...
consumer1消费了消息：Hello Bite
```

默认示例里：

- `exchange1` 是 `TOPIC` 交换机
- `queue1` 的 binding key 是 `queue1`
- `queue2` 的 binding key 是 `news.music.#`
- `publish_client` 使用的 routing key 是 `news.music.pop`，所以消息会进入 `queue2`

## QoS、失败重投和死信

`basicQos(prefetch_count)` 是信道级设置，作用于该信道上的消费者：

- `0`：不限制（默认）
- `N > 0`：消费者最多同时持有 N 条未 ACK 消息；达到上限后，`QueueDispatcher` 会跳过它，把消息投给其他还有额度的消费者
- ACK、`basicReject` 或 ACK 超时归还名额后，队列会被重新唤醒

消费端处理失败时调用：

```cpp
// 重新入队，等待再次投递
channel->basicReject(message_id, true);

// 不再重投：有死信交换机就进入死信队列，否则直接丢弃
channel->basicReject(message_id, false);
```

队列支持这些参数：

| 参数 | 含义 |
| --- | --- |
| `x-max-length` | 就绪消息上限，超出时丢弃最旧的 |
| `x-ack-timeout-ms` | 等待 ACK 的毫秒数，默认 `30000`；`0` 表示关闭超时 |
| `x-max-delivery-count` | 最多投递次数；达到后 `requeue=true` 也会转死信；未设置表示不限制 |
| `x-dead-letter-exchange` | 死信交换机名；未设置时拒绝/超时的消息直接丢弃 |
| `x-dead-letter-routing-key` | 可选，死信路由 key；不设置时沿用原消息的 routing key |

服务端每秒扫描一次 wait-ack。超时消息会按原投递顺序重新入队，`delivery_count` 加一；
如果达到 `x-max-delivery-count`，则按死信交换机路由。死信消息进入目标队列后，
`delivery_count` 会重置为 0。

`BasicProperties.delivery_count` 会随消息一起推送给消费者，消费端可以据此判断是否已经重投过。
当前 `delivery_count` 是运行时计数，服务重启后从 0 开始；消息本身仍会持久化。

示例：

```cpp
google::protobuf::Map<std::string, std::string> args;
args["x-max-length"] = "1000";
args["x-ack-timeout-ms"] = "5000";
args["x-max-delivery-count"] = "3";
args["x-dead-letter-exchange"] = "dlx";
args["x-dead-letter-routing-key"] = "dead";
channel->declareQueue("task_queue", true, false, false, args);
channel->basicQos(1);
```

## 测试

```bash
ctest --test-dir build --output-on-failure
```

每个测试都会在 `build/test-run/<test_name>/` 下拿到一份独立的 `tests/data` 副本，
避免 SQLite 状态在测试之间互相污染。

只跑某个测试：

```bash
ctest --test-dir build -R mq_routetest --output-on-failure
```

## 已实现

- 基于 muduo 的 TCP 服务端 / 客户端，protobuf 编解码
- `Channel` 的打开与关闭
- `Exchange` 的声明、删除，支持 `DIRECT` / `FANOUT` / `TOPIC`
- `Queue` 的声明、删除，支持持久化元数据
- `Binding` 的绑定 / 解绑与 routing key 匹配
- `basicPublish` / `basicConsume` / `basicCancel` / `basicAck` / `basicQos` / `basicReject`
- 队列采用 RabbitMQ 式竞争消费：同一队列的多个消费者按轮转分摊消息，每条消息只投递给其中一个消费者；新消费者不会把历史消息“独占补发”一遍
- `QueueDispatcher` 按队列合并唤醒：发布、订阅、确认、拒绝、超时后触发正常 drain；每批最多处理 100 条，剩余消息重新排队，避免大队列长期占用单工作线程
- `basicQos` 支持 prefetch：消费者达到在途消息上限后会被跳过，ACK/拒绝/超时后恢复投递
- 消费失败可用 `basicReject(requeue)` 重新入队或转入死信；`delivery_count` 随消息传递
- 消费者断线或 `basicCancel` 时，未 ACK 消息按原投递顺序重新入队，等待其他/后续消费者
- 服务端每秒扫描一次 ACK 超时；超时消息自动重投，达到 `x-max-delivery-count` 后进入死信交换机
- 消息落盘：SQLite 保存元数据，`.mqd` 文件保存消息体
- 未确认消息（wait-ack）与确认后回收
- 无消费者时消息保留在队列中，不会先取出再卡在 wait-ack
- 支持队列参数 `x-max-length`、`x-ack-timeout-ms`、`x-max-delivery-count`、`x-dead-letter-exchange`、`x-dead-letter-routing-key`；durable 队列会随元数据持久化这些参数
- 消费者回调使用 `weak_ptr`，避免 Channel 析构后的悬空指针；调度任务持有 dispatcher 的 `shared_ptr`，线程池使用 `weak_ptr` 避免循环引用
- 客户端请求等待响应最多 5 秒，超时返回失败而不是永久阻塞
- GoogleTest 单元测试与 CTest 集成

## 已知限制

这些是刻意保留的边界，不是“忘了做”：

- **没有 Publisher Confirm / mandatory / return**：发布端无法确认 broker 是否收到消息。
- **只实现了单条 `basicReject`**：没有 `basicNack(multiple)`，也不支持批量确认；消费端需要自己记录失败消息 id。
- **ACK 超时和死信是 at-least-once**：超时/断线都会重投，不是 exactly-once；消费端仍需要保证操作幂等。
- **`delivery_count` 没有持久化**：它保存在消息运行时属性里，服务重启后会从 0 开始；持久化消息体会恢复，但重投次数不会。
- **没有 latched / retained 语义**：队列是“消费即出队”的工作队列，不会为每个新订阅者保留最近一条或最近 N 条副本。ROS2 latched topic 这类“最新状态”需求应放到未来的 bridge 中用 retained-state 缓存实现，而不是把队列改成回放日志。
- **`x-max-length` 只约束待推送消息**：已经被消费者取走但尚未 ACK 的消息不计入上限，所以消费者长期不 ACK 时，队列里的消息总量仍可能超过上限。
- **死信配置要避免环路**：如果死信交换机又把消息路由回源队列，可能形成反复投递；当前没有死信跳数/循环检测。
- **没有大包零拷贝 / 共享内存**：消息体统一走 protobuf 和 TCP。
- **没有 ROS2 bridge**：还没有和 `rclcpp` 的 QoS 做映射。
- **没有监控面板**：没有 Prometheus / Grafana，只有日志。
- **请求超时不等于请求没执行**：客户端 5 秒没收到响应会返回失败，但服务端可能已经处理了请求。
- **没有 TLS、鉴权、权限控制**：默认只适合本地学习。
- **muduo 是预编译静态库**：换架构需要自行重新编译。

## 下一步

按优先级建议继续做：

1. 把 retained-state（最近状态）、ROS2 QoS 映射和 topic/service 桥接放到独立的 bridge 进程；broker 继续保持 ROS2 无关
2. 写一个 100Hz IMU + 大包图像的 demo，并补一张吞吐/延迟表
3. 可选：补监控指标（队列深度、消费者数、未确认数、超时/死信计数）
4. 可选：补 `basicNack(multiple)`、Publisher Confirm 和更有针对性的持久化重投计数

## License

MIT，见 [LICENSE](LICENSE)。
