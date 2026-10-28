#ifndef MQ_BRIDGE_ENVELOPE_CODEC_HPP
#define MQ_BRIDGE_ENVELOPE_CODEC_HPP

#include "bridge_envelope.pb.h"

#include <cstdint>
#include <string>

namespace mq
{
namespace bridge
{

// bridge 写入 MQ 的稳定消息体格式。
//
// MQ 侧仍然只是普通的 BasicProperties + body；bridge 把 ROS2 topic、类型、
// 时间戳和 CDR 字节封装进 body 的 BridgeEnvelope。broker 不解析这个 body，
// 所以 broker 始终不需要 ROS2/DDS 类型，也不知道 CDR 的结构。
//
// 版本策略：
// - version 从 1 开始；encode() 遇到 0 会自动填入 kCurrentVersion；
// - decode() 只接受 1..kCurrentVersion，拒绝未来版本，避免旧 bridge 按旧
//   语义误读新消息；
// - 以后扩展时只新增字段，不能复用已有字段编号或改变既有字段含义。
class EnvelopeCodec
{
public:
    static const uint32_t kCurrentVersion = 1;

    // 校验并序列化。不修改输入 envelope；version 为 0 时只在副本上补版本。
    // 序列化失败或字段非法时返回 false，不保证 *bytes 的内容。
    static bool encode(const BridgeEnvelope &envelope,
                       std::string *bytes,
                       std::string *error);

    // 反序列化并校验。空 cdr 是合法的（例如空消息体），但 topic/ros_type
    // 不能为空。成功后才替换 *envelope，失败时不会留下部分结果。
    static bool decode(const std::string &bytes,
                       BridgeEnvelope *envelope,
                       std::string *error);

    // 只做字段检查，不自动补 version；encode()/decode() 内部都会调用。
    // 这里不解析 cdr，保持 bridge 对 ROS2 序列化格式的透明。
    static bool validate(const BridgeEnvelope &envelope, std::string *error);
};

} // namespace bridge
} // namespace mq

#endif
