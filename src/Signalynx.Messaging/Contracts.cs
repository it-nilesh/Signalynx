namespace Signalynx.Messaging;

public interface IMessageHandler<in TMessage>
{
    ValueTask HandleAsync(TMessage message, MessageContext context);
}

public interface ISignalynxMessageBus
{
    ValueTask<Guid> EnqueueAsync<TMessage>(
        TMessage message,
        string destination = "default",
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);

    ValueTask<Guid> ScheduleAsync<TMessage>(
        TMessage message,
        DateTimeOffset deliverAt,
        string destination = "default",
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);
}

public interface IMessageSerializer
{
    string ContentType { get; }

    byte[] Serialize<TMessage>(TMessage message);

    object Deserialize(ReadOnlyMemory<byte> body, Type messageType);
}

public interface IMessageTransport
{
    ValueTask SendAsync(MessageEnvelope envelope, CancellationToken cancellationToken);

    IAsyncEnumerable<TransportDelivery> ReceiveAsync(CancellationToken cancellationToken);
}

public interface IOutboxStore
{
    ValueTask EnqueueAsync(OutboxMessage message, CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<OutboxMessage>> LockDueAsync(
        int maxCount,
        DateTimeOffset now,
        TimeSpan lockDuration,
        CancellationToken cancellationToken);

    ValueTask MarkDeliveredAsync(Guid messageId, CancellationToken cancellationToken);

    ValueTask RescheduleAsync(
        Guid messageId,
        int attempt,
        DateTimeOffset nextAttempt,
        string error,
        CancellationToken cancellationToken);

    ValueTask MoveToDeadLetterAsync(
        Guid messageId,
        int attempt,
        string error,
        CancellationToken cancellationToken);
}

public interface IBatchOutboxStore
{
    ValueTask EnqueueBatchAsync(
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken);

    ValueTask MarkDeliveredBatchAsync(
        IReadOnlyList<Guid> messageIds,
        CancellationToken cancellationToken);

    ValueTask RescheduleBatchAsync(
        IReadOnlyList<OutboxReschedule> messages,
        CancellationToken cancellationToken);

    ValueTask MoveToDeadLetterBatchAsync(
        IReadOnlyList<OutboxDeadLetter> messages,
        CancellationToken cancellationToken);
}

public interface IInboxStore
{
    ValueTask<bool> TryStartAsync(
        Guid messageId,
        DateTimeOffset receivedAt,
        CancellationToken cancellationToken);

    ValueTask CompleteAsync(Guid messageId, CancellationToken cancellationToken);

    ValueTask FailAsync(Guid messageId, string error, CancellationToken cancellationToken);
}

public interface IBatchInboxStore
{
    ValueTask<IReadOnlyList<Guid>> TryStartBatchAsync(
        IReadOnlyList<InboxStart> messages,
        CancellationToken cancellationToken);

    ValueTask CompleteBatchAsync(
        IReadOnlyList<Guid> messageIds,
        CancellationToken cancellationToken);

    ValueTask FailBatchAsync(
        IReadOnlyList<InboxFailure> messages,
        CancellationToken cancellationToken);
}

public interface IDeadLetterStore
{
    ValueTask AddAsync(DeadLetterMessage message, CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<DeadLetterMessage>> GetAsync(
        int maxCount,
        CancellationToken cancellationToken);

    ValueTask RemoveAsync(Guid messageId, CancellationToken cancellationToken);
}

public interface IBatchDeadLetterStore
{
    ValueTask AddBatchAsync(
        IReadOnlyList<DeadLetterMessage> messages,
        CancellationToken cancellationToken);

    ValueTask RemoveBatchAsync(
        IReadOnlyList<Guid> messageIds,
        CancellationToken cancellationToken);
}

public interface IRetryPolicy
{
    bool ShouldRetry(int attempt, Exception exception, out TimeSpan delay);
}

public interface IMessageOperations
{
    ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
        int maxCount = 100,
        CancellationToken cancellationToken = default);

    ValueTask ReplayDeadLetterAsync(
        Guid messageId,
        CancellationToken cancellationToken = default);
}
