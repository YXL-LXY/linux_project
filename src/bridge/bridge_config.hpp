#ifndef MQ_BRIDGE_BRIDGE_CONFIG_HPP
#define MQ_BRIDGE_BRIDGE_CONFIG_HPP

#include "bridge_config.pb.h"

#include <string>

namespace mq
{
namespace bridge
{

// bridge 配置的唯一入口。
//
// 配置使用 protobuf text format（不是 YAML/JSON），这样可以直接复用项目已经
// 依赖的 protobuf，不额外引入配置库。示例见 config/bridge.conf.example。
//
// 处理顺序固定为：
//   1. TextFormat 解析到临时 BridgeConfig；
//   2. applyDefaults() 补 node_name/mq_host/mq_port/QoS/depth/exchange_type 等缺省值；
//   3. validate() 检查字段组合和跨字段约束；
//   4. 全部成功后才 Swap 到调用者对象。
// 因此失败时不会留下半份配置。所有接口都通过 error 返回可读错误，不抛异常，
// 方便 CLI 和测试直接复用。
class BridgeConfigLoader
{
public:
    // 读取整个文件并解析。失败时 error 会带文件路径前缀。
    static bool loadFromFile(const std::string &path,
                             BridgeConfig *config,
                             std::string *error);

    // 从内存中的文本解析。成功后才替换 *config；*config 不会出现部分更新。
    static bool parseFromString(const std::string &text,
                                BridgeConfig *config,
                                std::string *error);

    // 补齐缺省值。proto3 标量没有可靠的“字段是否出现”语义，因此这里把
    // 空字符串/0 统一当作未配置。该函数只补默认值，不做合法性判断；
    // 重复调用结果不变。
    static void applyDefaults(BridgeConfig *config);

    // 不修改 config，只检查字段组合是否可以用作 bridge 配置。
    // 通常应在 applyDefaults() 之后调用；直接传入原始配置时，未填的枚举
    // 会被当成错误，而不是自动补默认值。
    static bool validate(const BridgeConfig &config, std::string *error);

    // 供 main 和测试复用：判断方向是否包含某一侧。
    static bool includesMqToRos2(BridgeDirection direction);
    static bool includesRos2ToMq(BridgeDirection direction);
};

} // namespace bridge
} // namespace mq

#endif
