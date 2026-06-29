namespace Signalynx.Messaging;

public sealed class SignalynxMessageBus : ISignalynxMessageBus
{
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    private readonly IOutboxStore _outbox;
    private readonly IMessageSerializer _serializer;
    private readonly TimeProvider _timeProvider;

    public SignalynxMessageBus(
        IOutboxStore outbox,
        IMessageSerializer serializer,
        TimeProvider timeProvider)
    {
        _outbox = outbox;
        _serializer = serializer;
        _timeProvider = timeProvider;
    }

    public ValueTask<Guid> EnqueueAsync<TMessage>(
        TMessage message,
        string destination = "default",
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default) =>
        ScheduleAsync(
            message,
            _timeProvider.GetUtcNow(),
            destination,
            headers,
            cancellationToken);

    public async ValueTask<Guid> ScheduleAsync<TMessage>(
        TMessage message,
        DateTimeOffset deliverAt,
        string destination = "default",
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);

        var now = _timeProvider.GetUtcNow();
        var id = Guid.NewGuid();
        var envelope = new MessageEnvelope(
            id,
            MessageTypeName.For<TMessage>(),
            destination,
            _serializer.ContentType,
            _serializer.Serialize(message),
            headers ?? EmptyHeaders,
            now,
            deliverAt > now ? deliverAt : null);

        await _outbox.EnqueueAsync(
            new OutboxMessage(envelope, 0, deliverAt),
            cancellationToken).ConfigureAwait(false);
        SignalynxMessagingDiagnostics.Enqueued.Add(1);
        return id;
    }
}
