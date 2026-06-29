namespace Signalynx.Messaging;

public sealed class MessageOperations : IMessageOperations
{
    private readonly IDeadLetterStore _deadLetters;
    private readonly IOutboxStore _outbox;
    private readonly TimeProvider _timeProvider;

    public MessageOperations(
        IDeadLetterStore deadLetters,
        IOutboxStore outbox,
        TimeProvider timeProvider)
    {
        _deadLetters = deadLetters;
        _outbox = outbox;
        _timeProvider = timeProvider;
    }

    public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
        int maxCount = 100,
        CancellationToken cancellationToken = default)
    {
        if (maxCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCount));
        }

        return _deadLetters.GetAsync(maxCount, cancellationToken);
    }

    public async ValueTask ReplayDeadLetterAsync(
        Guid messageId,
        CancellationToken cancellationToken = default)
    {
        var entries = await _deadLetters
            .GetAsync(int.MaxValue, cancellationToken)
            .ConfigureAwait(false);
        DeadLetterMessage? selected = null;
        for (var i = 0; i < entries.Count; i++)
        {
            if (entries[i].Envelope.Id == messageId)
            {
                selected = entries[i];
                break;
            }
        }

        if (selected is null)
        {
            throw new KeyNotFoundException($"Dead-letter message '{messageId}' was not found.");
        }

        var now = _timeProvider.GetUtcNow();
        await _outbox.EnqueueAsync(
            new OutboxMessage(
                selected.Envelope with { DeliverAt = null },
                0,
                now),
            cancellationToken).ConfigureAwait(false);
        await _deadLetters.RemoveAsync(messageId, cancellationToken).ConfigureAwait(false);
    }
}
