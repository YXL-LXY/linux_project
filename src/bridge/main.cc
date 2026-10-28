#include "bridge/bridge_config.hpp"
#include "bridge/bridge_runtime.hpp"
#include "bridge/qos_mapper.hpp"
#include "bridge/ros2_bridge.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

#ifdef MQ_WITH_ROS2
#include <rclcpp/rclcpp.hpp>
#endif

// mq_bridge 进程入口。
//
// 两种模式：
// - --check-config：只解析配置、打印计划，不需要 ROS2，默认模式；
// - --run：连接 MQ broker、声明拓扑、创建 ROS2 generic pub/sub，
//   然后运行 MQ <-> ROS2 数据面。
//
// 关闭 MQ_BUILD_ROS2_BRIDGE 时仍然可以构建并执行 --check-config；--run 会
// 明确报告没有 ROS2 支持，而不是静默降级。
namespace
{

const char *directionName(mq::bridge::BridgeDirection direction)
{
    switch (direction)
    {
    case mq::bridge::MQ_TO_ROS2:
        return "MQ_TO_ROS2";
    case mq::bridge::ROS2_TO_MQ:
        return "ROS2_TO_MQ";
    case mq::bridge::BIDIRECTIONAL:
        return "BIDIRECTIONAL";
    default:
        return "UNSPECIFIED";
    }
}

const char *exchangeTypeName(mq::bridge::BridgeExchangeType type)
{
    switch (type)
    {
    case mq::bridge::EXCHANGE_DIRECT:
        return "DIRECT";
    case mq::bridge::EXCHANGE_FANOUT:
        return "FANOUT";
    case mq::bridge::EXCHANGE_TOPIC:
        return "TOPIC";
    default:
        return "UNSPECIFIED";
    }
}

const char *reliabilityName(mq::bridge::BridgeReliability reliability)
{
    switch (reliability)
    {
    case mq::bridge::BEST_EFFORT:
        return "BEST_EFFORT";
    case mq::bridge::RELIABLE:
        return "RELIABLE";
    default:
        return "UNSPECIFIED";
    }
}

const char *durabilityName(mq::bridge::BridgeDurability durability)
{
    switch (durability)
    {
    case mq::bridge::VOLATILE:
        return "VOLATILE";
    case mq::bridge::TRANSIENT_LOCAL:
        return "TRANSIENT_LOCAL";
    default:
        return "UNSPECIFIED";
    }
}

void printUsage(const char *program)
{
    std::cout
        << "Usage: " << program << " --config PATH [--check-config | --run]\n"
        << "\n"
        << "  --config PATH    protobuf text format bridge config\n"
        << "  --check-config   validate and print the plan, then exit (default)\n"
        << "  --run            connect MQ and run the ROS2 <-> MQ data plane\n"
        << "  --help           show this help\n"
        << "\n"
        << "--run requires a build with -DMQ_BUILD_ROS2_BRIDGE=ON and a\n"
        << "reachable MQ broker at config.mq_host:config.mq_port.\n";
}

// 打印配置解析后的“计划”。这里会调用 QosMapper::toMq() 展示派生结果，
// 但不会声明拓扑或连接 broker，因此 --check-config 可以离线运行。
void printSummary(const mq::bridge::BridgeConfig &config)
{
    std::cout << "mq_bridge config OK\n"
              << "  node: " << config.node_name() << "\n"
              << "  broker: " << config.mq_host() << ":" << config.mq_port() << "\n"
              << "  retained_store: "
              << (config.retained_store().enabled() ? "enabled" : "disabled")
              << " (capacity=" << config.retained_store().capacity_per_topic()
              << ", ttl=" << config.retained_store().ttl_sec() << "s)\n"
              << "  topics: " << config.topics_size() << "\n";

    for (int i = 0; i < config.topics_size(); ++i)
    {
        const mq::bridge::TopicConfig &topic = config.topics(i);
        const mq::bridge::MqQos mq_qos = mq::bridge::QosMapper::toMq(topic);
        std::cout << "    " << topic.name()
                  << " [" << directionName(topic.direction()) << "]"
                  << " type=" << topic.ros_type()
                  << " exchange=" << topic.exchange()
                  << " exchange_type=" << exchangeTypeName(topic.exchange_type())
                  << " routing_key=" << topic.routing_key()
                  << " queue=" << topic.queue()
                  << " qos=" << reliabilityName(topic.reliability())
                  << "/" << durabilityName(topic.durability())
                  << " depth=" << topic.depth()
                  << " retained=" << (mq_qos.retained ? "yes" : "no")
                  << " mq_auto_ack=" << (mq_qos.auto_ack ? "yes" : "no")
                  << "\n";
    }
}

struct Options
{
    std::string config_path;
    bool check_only;
    bool run;
};

bool parseArgs(int argc, char **argv, Options *options, std::string *error)
{
    // 不指定模式时默认做配置检查；只有显式 --run 才启动数据面。
    // 这样在 CI 或没有 ROS2 的机器上直接执行也不会意外阻塞。
    options->check_only = false;
    options->run = false;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--config")
        {
            if (i + 1 >= argc)
            {
                *error = "--config requires a path";
                return false;
            }
            options->config_path = argv[++i];
        }
        else if (arg == "--check-config")
        {
            options->check_only = true;
        }
        else if (arg == "--run")
        {
            options->run = true;
        }
        else if (arg == "--help" || arg == "-h")
        {
            printUsage(argv[0]);
            std::exit(0);
        }
        else
        {
            *error = "unknown argument: " + arg;
            return false;
        }
    }

    if (options->config_path.empty())
    {
        *error = "--config PATH is required";
        return false;
    }
    if (options->check_only && options->run)
    {
        *error = "--check-config and --run are mutually exclusive";
        return false;
    }
    // 默认（既没有 --check-config 也没有 --run）走检查模式。
    if (!options->run)
    {
        options->check_only = true;
    }
    return true;
}

} // namespace

int main(int argc, char **argv)
{
    Options options;
    std::string error;
    if (!parseArgs(argc, argv, &options, &error))
    {
        std::cerr << "mq_bridge: " << error << "\n";
        printUsage(argv[0]);
        return 1;
    }

    // 配置是唯一入口：解析、补默认值、校验都在 loadFromFile() 内完成。
    mq::bridge::BridgeConfig config;
    if (!mq::bridge::BridgeConfigLoader::loadFromFile(options.config_path, &config, &error))
    {
        std::cerr << "mq_bridge: " << error << "\n";
        return 1;
    }

    // 打印实际生效的值（包括默认值），方便确认配置没有被拼写错误绕过。
    printSummary(config);
    std::cout << "  ros2_support: "
              << (mq::bridge::ros2BridgeAvailable() ? "enabled" : "disabled")
              << "\n";

    // 配置检查到此结束；不初始化 rclcpp，也不连接 broker。
    if (options.check_only)
    {
        return 0;
    }

    // --run 必须真的编译了 ROS2，否则 createRos2Bridge() 只能返回 stub。
    if (!mq::bridge::ros2BridgeAvailable())
    {
        std::cerr << "mq_bridge: ROS2 support is not compiled in; "
                  << "reconfigure with -DMQ_BUILD_ROS2_BRIDGE=ON\n";
        return 2;
    }

#ifdef MQ_WITH_ROS2
    // 必须先 init，RclcppRos2Bridge::start() 里的 rclcpp::ok() 才会为 true。
    rclcpp::init(argc, argv);
#endif

    // BridgeRuntime 负责 MQ 连接、拓扑声明、retained 回放、双向转发和清理。
    // main 只负责进程级生命周期：rclcpp::init/shutdown 和退出码。
    mq::bridge::BridgeRuntime runtime;
    if (!runtime.start(config, &error))
    {
        std::cerr << "mq_bridge: " << error << "\n";
        // start() 已经在失败路径清理资源；这里仍显式 stop，保证析构前
        // ROS2 node 先于 rclcpp::shutdown() 释放。
        runtime.stop();
#ifdef MQ_WITH_ROS2
        rclcpp::shutdown();
#endif
        return 1;
    }

    std::cout << "mq_bridge running: MQ <-> ROS2 data plane active; "
              << "press Ctrl-C to stop.\n";
    // spin() 会阻塞到 Ctrl-C / rclcpp::shutdown()。返回后按“先停 bridge，
    // 再关 ROS2 上下文”的顺序清理，避免 node 在 shutdown 之后析构。
    runtime.spin();
    runtime.stop();

#ifdef MQ_WITH_ROS2
    rclcpp::shutdown();
#endif
    return 0;
}
