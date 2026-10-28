#include "bridge/envelope_codec.hpp"

namespace mq
{
namespace bridge
{

const uint32_t EnvelopeCodec::kCurrentVersion;

namespace
{

void setError(std::string *error, const std::string &message)
{
    if (error != nullptr)
    {
        *error = message;
    }
}

} // namespace

bool EnvelopeCodec::validate(const BridgeEnvelope &envelope, std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }

    // version=0 只允许出现在 encode() 的输入里；validate() 本身不补版本，
    // 因此这里要求调用者已经明确版本。
    if (envelope.version() == 0)
    {
        setError(error, "envelope version must not be 0");
        return false;
    }
    // 只接受当前和更旧的版本。未来版本的字段语义可能不同，旧 bridge 不能
    // 猜测着继续解析。
    if (envelope.version() > kCurrentVersion)
    {
        setError(error, "unsupported envelope version: " +
                            std::to_string(envelope.version()));
        return false;
    }
    // topic/ros_type 是消费端重建 ROS2 消息的必要信息。cdr 故意不检查：
    // 空消息体是合法的，而且 bridge 不应该理解 CDR 的内部结构。
    if (envelope.topic().empty())
    {
        setError(error, "envelope topic must not be empty");
        return false;
    }
    if (envelope.ros_type().empty())
    {
        setError(error, "envelope ros_type must not be empty");
        return false;
    }
    return true;
}

bool EnvelopeCodec::encode(const BridgeEnvelope &envelope,
                           std::string *bytes,
                           std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (bytes == nullptr)
    {
        setError(error, "output bytes must not be null");
        return false;
    }

    // 在副本上补版本，避免 encode() 偷偷修改调用者传入的 envelope。
    BridgeEnvelope copy = envelope;
    if (copy.version() == 0)
    {
        copy.set_version(kCurrentVersion);
    }
    if (!validate(copy, error))
    {
        return false;
    }
    if (!copy.SerializeToString(bytes))
    {
        setError(error, "failed to serialize BridgeEnvelope");
        return false;
    }
    return true;
}

bool EnvelopeCodec::decode(const std::string &bytes,
                           BridgeEnvelope *envelope,
                           std::string *error)
{
    if (error != nullptr)
    {
        error->clear();
    }
    if (envelope == nullptr)
    {
        setError(error, "envelope output must not be null");
        return false;
    }

    // 先解析到局部对象：protobuf 的 ParseFromString 可能部分写入，校验失败
    // 时不能把半成品暴露给调用者。
    BridgeEnvelope parsed;
    if (!parsed.ParseFromString(bytes))
    {
        setError(error, "failed to parse BridgeEnvelope");
        return false;
    }
    if (!validate(parsed, error))
    {
        return false;
    }

    // 成功后才替换输出。Swap 同时避免一次大字符串/字段拷贝。
    envelope->Swap(&parsed);
    return true;
}

} // namespace bridge
} // namespace mq
