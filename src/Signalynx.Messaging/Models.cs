namespace Signalynx.Messaging;

public sealed record MessageEnvelope(
    Guid Id,
    string MessageType,
    string Destination,
    string ContentType,
    ReadOnlyMemory<byte> Body,
    IReadOnlyDictionary<string, string> Headers,
    DateTimeOffset CreatedAt,
    DateTimeOffset? DeliverAt,
    Guid? CorrelationId = null,
    Guid? CausationId = null);

public sealed record OutboxMessage(
    MessageEnvelope Envelope,
    int Attempt,
    DateTimeOffset NextAttempt,
    DateTimeOffset? LockedUntil = null,
    string? LastError = null);

public sealed record DeadLetterMessage(
    MessageEnvelope Envelope,
    int Attempt,
    string Error,
    DateTimeOffset FailedAt,
    string Source);

public sealed record MessageContext(
    MessageEnvelope Envelope,
    int Attempt,
    CancellationToken CancellationToken)
{
    public Guid MessageId => Envelope.Id;

    public IReadOnlyDictionary<string, string> Headers => Envelope.Headers;
}

public abstract class TransportDelivery
{
    protected TransportDelivery(MessageEnvelope envelope, int attempt)
    {
        Envelope = envelope;
        Attempt = attempt;
    }

    public MessageEnvelope Envelope { get; }

    public int Attempt { get; }

    public abstract ValueTask CompleteAsync(CancellationToken cancellationToken = default);

    public abstract ValueTask RetryAsync(
        TimeSpan delay,
        CancellationToken cancellationToken = default);

    public abstract ValueTask DeadLetterAsync(
        string error,
        CancellationToken cancellationToken = default);
}
