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
                          basicConsume ◀────────┘
                                │
                                ▼
                          consumer callback
                                │
                            basicAck
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
- `basicPublish` / `basicConsume` / `basicCancel` / `basicAck`
- 消息落盘：SQLite 保存元数据，`.mqd` 文件保存消息体
- 未确认消息（wait-ack）与确认后回收
- 服务端线程池异步投递消息
- GoogleTest 单元测试与 CTest 集成

## 已知限制

这些是刻意保留的边界，不是“忘了做”：

- **没有 Publisher Confirm / mandatory / return**：发布端无法确认 broker 是否收到消息。
- **没有 prefetch / QoS**：服务端会尽可能快地把消息推给消费者。
- **没有重投、死信队列、去重**：消费失败或超时后不会自动重新入队。
- **没有 latched / 历史消息回放**：新订阅者只能收到订阅之后发布的消息。
- **没有大包零拷贝 / 共享内存**：消息体统一走 protobuf 和 TCP。
- **没有 ROS2 bridge**：还没有和 `rclcpp` 的 QoS 做映射。
- **没有监控面板**：没有 Prometheus / Grafana，只有日志。
- **没有 TLS、鉴权、权限控制**：默认只适合本地学习。
- **muduo 是预编译静态库**：换架构需要自行重新编译。

## 下一步

按优先级建议继续做：

1. 修复“没有消费者时消息卡在 wait-ack”的问题
2. 消费任务捕获 `weak_ptr`，消除 `Channel` 关闭后的 use-after-free
3. 客户端 `waitResponse` 增加超时，避免网络抖动时永久阻塞
4. 实现 latched topic：新消费者订阅时投递队列中的历史消息
5. 用 `x-max-length` 实现 `KEEP_LAST` 有界队列
6. 写一个 100Hz IMU + 大包图像的 demo，并补一张吞吐/延迟表

## License

MIT，见 [LICENSE](LICENSE)。
