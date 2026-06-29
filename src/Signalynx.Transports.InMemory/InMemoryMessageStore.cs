using System.Collections.Concurrent;

namespace Signalynx.Messaging.InMemory;

public sealed class InMemoryMessageStore :
    IOutboxStore,
    IInboxStore,
    IDeadLetterStore
{
    private readonly object _outboxLock = new();
    private readonly Dictionary<Guid, OutboxMessage> _outbox = [];
    private readonly ConcurrentDictionary<Guid, InboxEntry> _inbox = new();
    private readonly ConcurrentQueue<DeadLetterMessage> _deadLetters = new();

    public int PendingOutboxCount
    {
        get
        {
            lock (_outboxLock)
            {
                return _outbox.Count;
            }
        }
    }

    public IReadOnlyList<DeadLetterMessage> DeadLetters => _deadLetters.ToArray();

    public ValueTask EnqueueAsync(
        OutboxMessage message,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_outboxLock)
        {
            if (!_outbox.TryAdd(message.Envelope.Id, message))
            {
                throw new InvalidOperationException(
                    $"Outbox message '{message.Envelope.Id}' already exists.");
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<OutboxMessage>> LockDueAsync(
        int maxCount,
        DateTimeOffset now,
        TimeSpan lockDuration,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (maxCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCount));
        }

        var messages = new List<OutboxMessage>(Math.Min(maxCount, 32));
        lock (_outboxLock)
        {
            foreach (var pair in _outbox)
            {
                if (messages.Count == maxCount)
                {
                    break;
                }

                var message = pair.Value;
                if (message.NextAttempt > now ||
                    message.LockedUntil is { } lockedUntil && lockedUntil > now)
                {
                    continue;
                }

                var locked = message with { LockedUntil = now + lockDuration };
                _outbox[pair.Key] = locked;
                messages.Add(locked);
            }
        }

        return ValueTask.FromResult<IReadOnlyList<OutboxMessage>>(messages);
    }

    public ValueTask MarkDeliveredAsync(Guid messageId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_outboxLock)
        {
            _outbox.Remove(messageId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask RescheduleAsync(
        Guid messageId,
        int attempt,
        DateTimeOffset nextAttempt,
        string error,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_outboxLock)
        {
            if (_outbox.TryGetValue(messageId, out var message))
            {
                _outbox[messageId] = message with
                {
                    Attempt = attempt,
                    NextAttempt = nextAttempt,
                    LockedUntil = null,
                    LastError = error
                };
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask MoveToDeadLetterAsync(
        Guid messageId,
        int attempt,
        string error,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_outboxLock)
        {
            _outbox.Remove(messageId);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TryStartAsync(
        Guid messageId,
        DateTimeOffset receivedAt,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        while (true)
        {
            if (!_inbox.TryGetValue(messageId, out var current))
            {
                if (_inbox.TryAdd(messageId, new InboxEntry(InboxStatus.Processing, null)))
                {
                    return ValueTask.FromResult(true);
                }

                continue;
            }

            if (current.Status is InboxStatus.Completed or InboxStatus.Processing)
            {
                return ValueTask.FromResult(false);
            }

            if (_inbox.TryUpdate(
                    messageId,
                    new InboxEntry(InboxStatus.Processing, null),
                    current))
            {
                return ValueTask.FromResult(true);
            }
        }
    }

    public ValueTask CompleteAsync(Guid messageId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _inbox[messageId] = new InboxEntry(InboxStatus.Completed, null);
        return ValueTask.CompletedTask;
    }

    public ValueTask FailAsync(
        Guid messageId,
        string error,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _inbox[messageId] = new InboxEntry(InboxStatus.Failed, error);
        return ValueTask.CompletedTask;
    }

    public ValueTask AddAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _deadLetters.Enqueue(message);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<DeadLetterMessage>> GetAsync(
        int maxCount,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (maxCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCount));
        }

        return ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>(
            _deadLetters.Take(maxCount).ToArray());
    }

    public ValueTask RemoveAsync(Guid messageId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var retained = new List<DeadLetterMessage>();
        while (_deadLetters.TryDequeue(out var message))
        {
            if (message.Envelope.Id != messageId)
            {
                retained.Add(message);
            }
        }

        for (var i = 0; i < retained.Count; i++)
        {
            _deadLetters.Enqueue(retained[i]);
        }

        return ValueTask.CompletedTask;
    }

    private sealed record InboxEntry(InboxStatus Status, string? Error);

    private enum InboxStatus
    {
        Processing,
        Completed,
        Failed
    }
}
