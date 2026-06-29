using System.Text.Json;

namespace Signalynx.Messaging;

public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public string ContentType => "application/json";

    public byte[] Serialize<TMessage>(TMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return JsonSerializer.SerializeToUtf8Bytes(message, _options);
    }

    public object Deserialize(ReadOnlyMemory<byte> body, Type messageType) =>
        JsonSerializer.Deserialize(body.Span, messageType, _options)
        ?? throw new SignalynxMessagingException(
            $"Message body could not be deserialized as '{messageType}'.");
}
