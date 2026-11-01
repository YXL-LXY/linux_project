# mq 设计笔记

这是项目的第一手设计文档，目标读者是想读懂或修改代码的人——包括未来的我自己。这里解释「为什么这么写」，具体的用法和接口说明在 [README.md](README.md) 里。

新代码修改前请先看「关键取舍」一节，很多看似奇怪的做法是有理由的。

## 目录

- [项目定位](#项目定位)
- [总体结构与线程模型](#总体结构与线程模型)
- [关键取舍](#关键取舍)
- [发布/投递/ACK 的完整链路](#发布投递ack-的完整链路)
- [调度器：一次投递的决策过程](#调度器一次投递的决策过程)
- [持久化设计](#持久化设计)
- [断线重连、超时与死信](#断线重连超时与死信)
- [客户端设计](#客户端设计)
- [ROS2 bridge](#ros2-bridge)
- [测试](#测试)
- [已知问题与后续方向](#已知问题与后续方向)

## 项目定位

核心目标是把 muduo 网络库、protobuf、SQLite、线程池这几样东西在一个中等复杂度的真实业务里串起来，同时让最终的产物真的像一个消息中间件，而不是课设级别的 echo server。因此：

- **语义上努力向 AMQP 0-9-1 靠拢**：交换机/队列/绑定、`wait_ack`、prefetch、死信队列这些核心概念都有，并且按 AMQP 的直觉使用。
- **实现上刻意保持小**：没有插件、没有集群、没有持久化 journal，重视可通读、可调试。
- **性能不做第一优先级**：单机中小流量是目标场景。一些 O(锁) 的粗粒度做法（比如队列级大锁、磁盘路径上的 `std::mutex`）在这里是可接受甚至故意的，先易后难。

明确不做的事（这些是设计决定，不是待办）：集群与主从、认证与授权、Publisher Confirm、跨队列事务、消息回溯。

## 总体结构与线程模型

### 代码分层

一个 broker 里所有可独立验证的单元基本都能在 `src/` 顶层对应到文件：

```
common/   mq_logger.hpp      打日志（printf 备注）
          mq_helper.hpp      文件、字符串、UUID、线程池等小工具
proto/    mq_msg.proto       消息体定义（含 BasicProperties、最终 hook 的 dead letter 路由结果等）
          mq_proto.proto     请求/响应协议、拓扑声明、发布、订阅、ACK、取消订阅……
server/   mq_broker.hpp      Server 类：muduo acceptor + 注册所有请求 handler
          mq_connection.hpp  连接对象（持有信道表）
          mq_channel.hpp     信道：所有请求的真正处理入口，一个连接可以有多个信道
          mq_host.hpp        VirtualHost：拓扑(交换机/队列/绑定) + 消息存储的整合门面
          mq_exchange.hpp / mq_queue.hpp / mq_binding.hpp   三张元数据表
          mq_route.hpp       Router：DIRECT / FANOUT / TOPIC 的匹配算法
          mq_message.hpp     MessageManager / QueueMessage / MessageMapper：消息的内存态 + 磁盘态
          mq_consumer.hpp    ConsumerManager / Consumer：订阅表
          mq_dispatcher.hpp  QueueDispatcher：把「有消息可投」变成「投出去 + 等 ACK」
client/   mq_connection.hpp  客户端 Connection：管理 TCP 连接、连接级异步 worker
          mq_channel.hpp     客户端 Channel：同步 RPC + 消费回调分发
          mq_worker.hpp      AsyncWorker：专属的 muduo EventLoopThread + 线程池
          publish_client.cc / consume_client.cc 两个可用的示例程序
bridge/   可选的 ROS2 桥接，见下文
```

### 线程模型

一条 TCP 连接对应 muduo 的工作线程。`Server` 的策略：

- **主 IO 线程**跑 muduo 事件循环。所有请求（声明、绑定、发布、ACK、拒绝、取消订阅）都在这里同步处理，避免给元数据加读写锁——一个 DB 请求串行处理比多线程快且不会踩到 SQLite 连接的线程安全边界。
- **消费者回调**可能阻塞（用户代码），丢进线程池执行。这里引入了两处复杂度：回调线程与 IO 线程并发访问 Consumer 的 prefetch 计数（用 `Consumer` 内部的 mutex 保护），以及「回调还没执行消费者就被 cancel」的竞态（见下文调度器）。
- **ACK 超时扫描**是主 IO 线程里每秒一次的定时器任务。30 秒的默认超时来自 `MQ_DEFAULT_ACK_TIMEOUT_MS`，队列参数 `x-ack-timeout-ms` 可以按队列覆盖。

客户端同样是一条连接一个 `AsyncWorker`（EventLoopThread + 线程池），用户代码和事件循环线程可以在回调里同时触碰 `Consumer` 对象，所以客户端的消费者也用独立的 mutex 保护。

### 为什么 VirtualHost 是一个类

交换机、队列、绑定三张表各自有独立的 Mapper（对应 SQLite），但所有业务操作几乎都要同时触碰多张表，还需要在删除时做级联清理（删交换机要删绑定、删队列要删历史消息文件）。把它们放进一个 `VirtualHost` 门面里，上层的 `Channel` 就可以只依赖这一个对象。缺点是 `mq_host.hpp` 越来越大，但目前还没到需要拆分的程度。

## 关键取舍

这一节按「如果不知道为什么这么写，看起来会很奇怪」的顺序列出几个设计决定。

### 1. 消息文件 append-only + `valid` 标记字段 + 定期 GC

不用 WAL/journal/LSM 这类重型方案，每队列落成单个 `.mqd` 文件，消息按顺序追加。淘汰消息时不重写文件，而是改写原始记录里 `payload.properties.valid` 字段为 "0"（这也是为什么消息对象在内存里保留 offset/length），GC 时再把有效记录压实到新的文件。

好处：发布时的磁盘开销排除了「随机块场面」，只有 append；实现量很小。
代价：文件会暂时变大（失效记录占着地方）、GC 是一次性 stop-the-world 的（每个队列在 GC 锁内搬完整个文件）、GC 失败要小心不能把内存索引搞脏——见下文。

### 2. GC 的原子性与失败语义

`MessageMapper::gc()` 的签名是 `bool gc(std::list<MessagePtr> &result)`，流程是：

1. 重放 `.mqd` 载入有效消息；
2. 全部序列化写入 `.mqd.tmp`（此时改写这些新对象的 offset/length，指向 tmp 中的位置）;
3. 删除旧 `.mqd`、把 `.mqd.tmp` 改名成 `.mqd`；此时 offset 的语义才真正对应正式文件；
4. 只有以上全部成功才返回 true。

失败时返回 false（上层必须原样保留计数与索引，不许把 `result` 当真），并尝试清掉 tmp 文件。上层 `QueueMessage::gc()` 只在成功路径上调换索引、更新 `_valid_count`/`_total_count`；失败时什么都不改，靠下一次触发再试。旧实现「half-success 也返回列表」的写法一旦在 rename 前崩溃或删除失败，会拿一份 offset 全错的索引继续运行，等于把好数据的指针弄脏——这个 bug 的修复可以说是整个项目里最值得之一。

### 3. 队列大锁：一个消息队列一把 mutex

`QueueMessage` 上的操作（insert / front / ack / reject / requeue / trim / gc）共用一把 mutex。没有用读写锁或分段锁，因为：

- 单队列的吞吐瓶颈在实践中先出现在磁盘 IO，锁竞争不痛；
- 吞吐敏感的 read（`front()`）会修改内部状态（delivery_count、wait-ack 表），本来就是写操作，读写锁帮不上忙。

代价是 GC（stop-the-world）期间同队列所有发布/投递都阻塞。因为 `x-max-length` 触发 GC 时机被设计成可配置（见 `x-max-length`），这在 MVP 里是可以接受的。

### 4. 发布响应是同步的，但仍然没有 Publisher Confirm

客户端 `basicPublish` 会等 broker 的响应，而响应是在发布请求**处理完之后**才发出：交换机不存在、路由命中队列的 insert 失败（含 DURABLE 消息写 `.mqd` 失败、消息 id 重复被拒），客户端都会拿到 false。所以严格说「写入失败客户端完全无从得知」并不成立——能感知到的部分都通过同步响应上报了。

没做的部分是 Confirm 的「确认投递/确认刷盘」语义：

- DURABLE 写入走 ofstream，**没有 fsync**，进程崩溃时数据都在页缓存不丢，断电可能丢最后几条；
- 一个消息没有路由到任何队列时响应仍是 true，只是消息被丢弃（与 AMQP « unroutable » 语义一致但无 alternate-exchange 兜底）；
- 响应只覆盖发布动作本身，与后续的投递成功与否无关。

引入真正的 Confirm 需要每条消息的 unconfirmed 表和刷盘时机管理，在第一优先级之外；同步响应已经消掉了最大的一块不确定性。文档里如实写清了这三条边界（本仓库 README 已写）。

### 5. (exchange, queue) 一对一绑定 key

RabbitMQ 允许一对 (exchange, queue) 用多个 binding key 绑定，中间有同时删除/替换的复杂性。这里把「一对多 key」砍掉，理由是：

- `BindingManager` 的删除粒度是 (exchange, queue)，多 key 会让「删除」变成「部分删除」，现有数据结构（二层 unordered_map）不好表达；
- MVP 阶段没有看到「一个队列必须同时匹配两种 pattern」的真实需求。

行为定义：重复 bind 相同 key → 幂等成功；bind 不同 key → 显式失败（客户端需要先 `queueUnBind` 再走 bind）。这比起旧实现「静默忽略但返回成功」要友善，旧做法会让客户端以为新 key 已生效。

### 6. binding key 合法性在服务端校验，routing key 不校验

`*` 和 `#` 在 binding key 里是位置敏感的通配符（`#` 不能出现在段中间等），Router 的算法对格式非法的 key 会产生无意义匹配，所以 `queueBind` 在服务端直接用 `Router::isLegalBindingKey()` 校验、不合法就拒绝（不再入库）。routing key 则相反：`*`/`#` 在发布端IANA 没有必要保留，AMQP 没硬性规定，很多真实系统的 key 里还会出现连字符之类，因此发布端不做白名单校验，由用户自己约定。

### 7. 消息 id 的重复在队列层面拒绝

消息 id 是客户端可以提供的；id 在 wait-ack 表和持久化索引里都是 key，重复 id 会让两次投递互相覆盖、ACK 时错误地淘汰另一条记录。所以 `QueueMessage::insert()` 在加锁后检查 `_durable_msgs` 和 `_waitack_msgs`，命中即拒绝（返回 false，客户把日志里的错误当失败处理，换个 id 重新发布即可）。检查只做 O(1) 的哈希表查询，不扫就绪链表——就绪态重复（两次没投递的 UNDURABLE）的后果只是「双条消息」，不会破坏文件和索引。

### 8. 投递时给回调的是快照

`QueueDispatcher::drainBatch()` 从队列取到消息后，先把 `properties` 和 `body` **拷贝**一份再调消费者回调。直接传 `mutable_payload()` 的指针在回调执行期间可能被人修改（同一时刻另一线程可能 ACK 掉这条消息、或重投把它移动起来），数据竞争在 ASan 下就可能翻车，拷贝的成本可以忽略。

### 9. 客户端同步 RPC 的 5 秒上限

客户端 `Channel` 的所有等待都用条件变量限制最多 5 秒。服务端不会让用户挂到永久（每次响应必然有回音），5 秒意味着 broker 不在线/慢死锁时的行为是可预期的降级。析构路径是个例外：`~Channel()` **不等响应**，只把 cancel 请求尽力发出去（连接活着就发），随后清理本地订阅者。析构时再同步等待 5 秒纯粹是给关闭流程添乱，代价是析构后可能有极小概率收到一条已取消消费者的推送（客户端按「找不到订阅者」静默丢弃，无害）。

### 10. 定长 `size_t` 长度前缀

`.mqd` 的记录格式是 `[size_t 长度][protobuf bytes]`，长度用 `size_t` 而不是 4 字节。 protobuf 单条消息本体可以任意，但长度字段按平台宽度直接一次 read，去掉序列化/字节序转换的边际成本。代价是文件不是跨平台二进制兼容的（32 位与 64 位平台的文件不能互换），MVP 阶段可以接受。

## 发布/投递/ACK 的完整链路

**发布**：客户端发 `basicPublishRequest` → Server 的 IO 线程 `Channel::basicPublish()` → `selectExchange` 找交换机（不存在则回 false）→ `exchangeBindings()` 拿绑定表，逐条用 `Router::route(type, rkey, bkey)` 筛选命中的队列 → 命中后 `VirtualHost::basicPublish(qname, ...)`，消息进入 `QueueMessage::insert()`：

- DURABLE 消息先 append 到 `.mqd`（写失败返回 false，本次发布对这条队列不生效）；
- 然后进内存就绪链表 `_msgs`；
- 最后按 `x-max-length` 淘汰超限的最旧就绪消息；
- 触发该队列的 notify（调度器接受到这条消息才会真正被投出）。

**投递（调度）**：见下一节。

**ACK**：客户端 `basicAck(msg_id)` → 服务端从 `wait-ack` 查到消息 → DURABLE 的改写磁盘记录 valid 字段 → 从内存移除（`_durable_msgs` / `_waitack_msgs` / 就绪链表都不再有它）→ 消费者的 `unacked` 计数减一，队列 notify 继续投下一条。

**Reject**：`basicReject(msg_id, requeue)` → 从 wait-ack 摘下 → 标 valid（视为本次投递终结）→ `requeue=true` 直接放回就绪链表队首，`false` 走 `routeDeadLetter()` 按 `x-dead-letter-exchange` 重新发布（新队列里 delivery_count 归零）。

## 调度器：一次投递的决策过程

`QueueDispatcher` 是整个服务端里并发最敏感的一块，它对每个队列维护一个状态机字段（`ready` / `pending` / `draining`），并在消费者线程池上执行 `drainBatch()`。单批最多投 100 条（`kBatchSize`），超出的部分通过 pending 状态在下一批继续，避免一个队列风暴占死线程池。

每条消息的投递流程：

1. `ConsumerManager::choose()` 按轮转挑一个**还有 prefetch 余量**的消费者；
2. `QueueMessage::front(consumer_tag, ack_timeout_ms)`：把队首消息移入 wait-ack，记下 deadline，`delivery_count` 加 1；
3. 竞争检查：`choose` 与「消费者刚好被 cancel」之间有窗口，存在竞态就 `requeueByConsumer(tag)` 把这条消息还给 owner（按 consumer_tag requeue，而不是全局 requeue，避免误伤其他消费者）；
4. `cp->incUnacked()` 先于回调——回调里若同步 ACK，计数不会出现先减后加的负数窗口；
5. 调用消费者回调（传快照，见取舍 8）；
6. `auto_ack` 模式：回调返回即 `basicAck`；手动模式等客户端 ACK；回调执行期间消费者被移除 → requeue、归还配额。

ACK 超时的重投、`x-max-delivery-count` 的 Retry 上限判断也由调度器的定时器入口（`checkAckTimeout`）触发，改成死信时还需要闭包 notify 受影响队列。

## 持久化设计

### 数据布局

```
<data_dir>/                    # mqserver 的第一个参数，默认 ./data/
  meta.db                      # SQLite：exchange_table / queue_table / binding_table
  <queue>.mqd                  # 每个持久化队列一个消息文件
  <queue>.mqd.tmp              # GC 用的临时文件，正常状态不存在
```

`.mqd` 内容是 `[size_t 长度][Message::Payload 字节]` 的连续序列，`Payload` 里含 `BasicProperties`（id、routing_key、delivery_mode、delivery_count、valid 等字段）。没有 CRC、没有分代日志，重启恢复就是从文件头开始按长度逐条解析，遇到「有效数据 Das Kapitel」的记录（valid="1"）放进消息链表。

### 交换机/队列/绑定的恢复

三个 Mapper 在构造时读全量 SQLite 表，在内存里维护 unordered_map + (对绑定) 双层 map 索引，写操作先落库再改内存。SQLite 相关操作失败的处理策略是：启动期的建表/open 失败直接 abort（宁可拒绝服务，也不能带着空元数据「假装成功」运行下去）；运行期的单次操作失败打日志并返回失败，让上层决定。

## 断线重连、超时与死信

### 连接断开

muduo 的 onConnection 回调里处理：遍历连接对象挂的所有信道，每个信道的消费者走 `requeueByConsumer(tag)`（消息放回就绪链表队首——重投偏好），连接级数据（信道表）随后销毁。

### ACK 超时

`x-ack-timeout-ms`（默认 30000ms，0 表示关闭）决定 wait-ack 的 deadline。定时扫描：超时消息按 `x-max-delivery-count` 决定去留——未达上限的 requeue，达到上限的 routeDeadLetter。这里是支持「消费者拿到消息但永远不回（bug / 崩溃）」场景的关键防线。

### 死信

死信的两个触发源（Reject `requeue=false`、ACK 超时达到重投上限）最终都汇聚到 `VirtualHost::routeDeadLetter()`：

- 源队列没配 `x-dead-letter-exchange`、或配置的交换机不存在，消息直接丢弃（日志留痕）；
- routing key 用 `x-dead-letter-routing-key` 覆盖（没设就沿用原 key）；
- 转发是新一次发布流程（含 Router 匹配），delivery_count 归零。

`x-max-length` 淘汰最旧就绪消息**不走**死信，只做标记无效 + 记日志——这条被淘汰的消息已经排在队首很久没人消费，再投死信意义不大，且淘汰路径在调度器锁内，多一步路由会放大停顿。想要「旧消息进死信」的场合应改用 TTL 类方案（尚未实现）。

## 客户端设计

客户端的价值在「把同步 API 建立在异步网络库上」：

- **request/response 对齐**：每个请求带 `rid`，响应回来对号入座；等待用条件变量 + 5 秒上限。`cid` 标识信道，服务端的 ChannelManager 按它路由响应。
- **消费推送**：服务端推 `basicConsumeResponse`，客户端按 `cid` 找到信道、按 `consumer_tag` 找到订阅者，把回调丢给线程池。
- **订阅并发**：`basicConsume` 必须**先**把订阅者装进 ConsumerManager 再发请求——服务端订阅成功后可能立刻开始推消息，顺序反了会丢最初的几条。
- **取消订阅**：`basicCancel()` 同步等响应；析构路径 `~Channel()` 走 fire-and-forget（见取舍 9）。

没有连接重连——断线后 `Connection` 不会自动恢复，应用层需要自己重建连接和拓扑。这是留给后续的改进点，不是疏忽：无重连时行为是完全确定的，加上重连反而引入了「重连后拓扑不一致」的一类新问题要先想清楚。

## ROS2 bridge

bridge 是独立的进程，`--check-config` 模式不依赖 ROS2，`--run` 模式要求 ROS2 环境（`MQ_BUILD_ROS2_BRIDGE=ON` 构建）。需要理解 bridge 时的三个入口：

- `bridge_config`：protobuf text format 配置的解析、默认值补全、校验，会判定哪些 topic 需要 rclcpp；
- `bridge_runtime` / `ros2_bridge`：双向数据面的编排——MQ 侧用 `mq_client_adapter` 连 broker（RELIABLE 映射为手动 ACK，ROS2 publish 成功才 ACK）；
- `envelope_codec` / `retained_store`：CDR 消息封装与 retained 内存/文件缓存。

bridge 与 broker 的解耦依赖「bridge 只是另一个 MQ 客户端」这一事实，所有协议兼容性问题都能用运行 broker 的单测来验。

## 测试

`tests/` 下的 13 个套件按层次覆盖：

- 存储层：`mq_filetest`、`mq_msgtest`（消息文件读写/GC/恢复）、`mq_queuetest` / `mq_bindingtest` / `mq_exchangetest`（三张元数据表）
- 逻辑层：`mq_routetest`（Router 匹配）、`mq_dispatchertest`（调度器状态机）、`mq_hosttest`（VirtualHost 全流程，含恢复）
- 协议层：`mq_channeltest`、`mq_connectiontest`、`mq_consumertest`（真实的 TCP 往返）
- bridge：`mq_bridge_coretest`（不依赖 rclcpp 的部分）、`mq_bridge_config_check`

跑法：`ctest --test-dir build`，13/13 通过；开发期间推荐再开一个 ASan 构建目录并行跑同一套测试，能提前暴露共享指针的生命周期问题（本项目长达数千行，靠 review 很难全部抓到）。

## 已知问题与后续方向

按「下一个最值得做的事」排序：

1. **Publisher Confirm**——把发布响应挪到真正落盘之后，是提升「可靠性故事完整性」收益最大的单项。
2. **`.mqd` 恢复的健壮性**——加一个 magic/CRC 头，把「文件读到一半损坏」从静默丢数据变成显式报错。
3. **连接自动重连 + 订阅/拓扑恢复**——用「重连后重放拓扑声明」这种显式策略比隐式恢复好实现也好理解。
4. **GC 的并发优化**——目前 stop-the-world，队列大时停顿明显；可考虑双缓冲或增量压实。
5. **Dispatcher 的公平性和可观测性**——当前批次大小固定 100，队列多、消费者饥饿的场景没有专门处理；也没有 metrics 暴露。
