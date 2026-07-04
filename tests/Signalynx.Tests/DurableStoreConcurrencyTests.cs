using Signalynx.Messaging;
using Signalynx.Messaging.InMemory;

namespace Signalynx.Tests;

public sealed class DurableStoreConcurrencyTests
{
    [Fact]
    public async Task Lease_races_return_each_due_outbox_message_once()
    {
        var store = new InMemoryMessageStore();
        var now = DateTimeOffset.UtcNow;
        var messages = Enumerable
            .Range(0, 100)
            .Select(_ => new OutboxMessage(Envelope(), 0, now))
            .ToArray();

        for (var i = 0; i < messages.Length; i++)
        {
            await store.EnqueueAsync(messages[i], CancellationToken.None);
        }

        var workers = Enumerable
            .Range(0, 16)
            .Select(_ => store.LockDueAsync(10, now, TimeSpan.FromMinutes(1), CancellationToken.None).AsTask())
            .ToArray();

        var locked = (await Task.WhenAll(workers)).SelectMany(static batch => batch).ToArray();

        Assert.Equal(messages.Length, locked.Select(static message => message.Envelope.Id).Distinct().Count());
        Assert.All(locked, static message => Assert.True(message.LockedUntil.HasValue));
    }

    [Fact]
    public async Task Duplicate_delivery_races_allow_only_one_inbox_start()
    {
        var store = new InMemoryMessageStore();
        var messageId = Guid.NewGuid();
        var starts = Enumerable
            .Range(0, 64)
            .Select(_ => store.TryStartAsync(messageId, DateTimeOffset.UtcNow, CancellationToken.None).AsTask())
            .ToArray();

        var results = await Task.WhenAll(starts);

        Assert.Single(results, static started => started);
    }

    [Fact]
    public async Task Retry_races_preserve_single_outbox_entry()
    {
        var store = new InMemoryMessageStore();
        var envelope = Envelope();
        await store.EnqueueAsync(
            new OutboxMessage(envelope, 0, DateTimeOffset.UtcNow),
            CancellationToken.None);

        var retries = Enumerable
            .Range(1, 32)
            .Select(attempt => store.RescheduleAsync(
                envelope.Id,
                attempt,
                DateTimeOffset.UtcNow.AddSeconds(attempt),
                $"retry-{attempt}",
                CancellationToken.None).AsTask())
            .ToArray();

        await Task.WhenAll(retries);
        var locked = await store.LockDueAsync(
            10,
            DateTimeOffset.UtcNow.AddMinutes(1),
            TimeSpan.FromSeconds(30),
            CancellationToken.None);

        Assert.Single(locked);
        Assert.Equal(envelope.Id, locked[0].Envelope.Id);
        Assert.InRange(locked[0].Attempt, 1, 32);
    }

    [Fact]
    public async Task Dead_letter_replay_moves_message_back_to_outbox_once()
    {
        var store = new InMemoryMessageStore();
        var operations = new MessageOperations(store, store, TimeProvider.System);
        var envelope = Envelope();
        await store.AddAsync(
            new DeadLetterMessage(envelope, 3, "failed", DateTimeOffset.UtcNow, "receiver"),
            CancellationToken.None);

        await operations.ReplayDeadLetterAsync(envelope.Id, CancellationToken.None);

        var deadLetters = await operations.GetDeadLettersAsync(cancellationToken: CancellationToken.None);
        var locked = await store.LockDueAsync(
            10,
            DateTimeOffset.UtcNow.AddSeconds(1),
            TimeSpan.FromSeconds(30),
            CancellationToken.None);

        Assert.Empty(deadLetters);
        Assert.Single(locked);
        Assert.Equal(envelope.Id, locked[0].Envelope.Id);
        Assert.Equal(0, locked[0].Attempt);
    }

    private static MessageEnvelope Envelope() =>
        new(
            Guid.NewGuid(),
            typeof(StoreConcurrencyMessage).AssemblyQualifiedName!,
            "orders",
            "application/json",
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, string>(),
            DateTimeOffset.UtcNow,
            null);

    private sealed record StoreConcurrencyMessage;
}
