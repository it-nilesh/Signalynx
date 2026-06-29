namespace Signalynx.Messaging;

public sealed class SignalynxMessagingOptions
{
    internal Dictionary<string, MessageRegistration> Registrations { get; } =
        new(StringComparer.Ordinal);

    public int OutboxBatchSize { get; set; } = 100;

    public TimeSpan OutboxPollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan OutboxLockDuration { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxDeliveryAttempts { get; set; } = 5;

    public TimeSpan BaseRetryDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    public bool EnableOutboxWorker { get; set; } = true;

    public bool EnableReceiverWorker { get; set; } = true;

    public SignalynxMessagingOptions RegisterMessage<TMessage>(string? name = null)
    {
        var messageName = name ?? MessageTypeName.For<TMessage>();
        if (!Registrations.TryAdd(
                messageName,
                new MessageRegistration(
                    typeof(TMessage),
                    static (services, envelope, attempt, cancellationToken) =>
                        MessageHandlerInvoker<TMessage>.InvokeAsync(
                            services,
                            envelope,
                            attempt,
                            cancellationToken))))
        {
            throw new InvalidOperationException($"Message name '{messageName}' is already registered.");
        }

        return this;
    }
}

internal sealed record MessageRegistration(
    Type MessageType,
    Func<IServiceProvider, MessageEnvelope, int, CancellationToken, ValueTask> Invoke);

internal static class MessageTypeName
{
    public static string For<TMessage>() =>
        typeof(TMessage).AssemblyQualifiedName
        ?? typeof(TMessage).FullName
        ?? typeof(TMessage).Name;
}
