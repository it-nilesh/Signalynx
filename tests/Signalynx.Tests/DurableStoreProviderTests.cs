using Microsoft.Extensions.DependencyInjection;
using Signalynx.Messaging;
using Signalynx.Messaging.PostgreSql;
using Signalynx.Messaging.SqlServer;

namespace Signalynx.Tests;

public sealed class DurableStoreProviderTests
{
    [Fact]
    public async Task Sql_Server_store_delegates_outbox_inbox_and_dead_letter_operations()
    {
        var client = new FakeSqlServerClient();
        var store = new SqlServerMessageStore(
            client,
            new SqlServerMessageStoreOptions
            {
                Schema = "messaging",
                OutboxTable = "outbox",
                InboxTable = "inbox",
                DeadLetterTable = "dead_letters"
            });
        var outbox = (IOutboxStore)store;
        var inbox = (IInboxStore)store;
        var deadLetters = (IDeadLetterStore)store;
        var outboxMessage = Outbox();
        var deadLetter = DeadLetter(outboxMessage.Envelope);

        await outbox.EnqueueAsync(outboxMessage, CancellationToken.None);
        await outbox.LockDueAsync(10, DateTimeOffset.UtcNow, TimeSpan.FromSeconds(30), CancellationToken.None);
        await outbox.MarkDeliveredAsync(outboxMessage.Envelope.Id, CancellationToken.None);
        await outbox.RescheduleAsync(outboxMessage.Envelope.Id, 2, DateTimeOffset.UtcNow, "retry", CancellationToken.None);
        await outbox.MoveToDeadLetterAsync(outboxMessage.Envelope.Id, 3, "failed", CancellationToken.None);
        Assert.True(await inbox.TryStartAsync(outboxMessage.Envelope.Id, DateTimeOffset.UtcNow, CancellationToken.None));
        await inbox.CompleteAsync(outboxMessage.Envelope.Id, CancellationToken.None);
        await inbox.FailAsync(outboxMessage.Envelope.Id, "bad", CancellationToken.None);
        await deadLetters.AddAsync(deadLetter, CancellationToken.None);
        await deadLetters.GetAsync(5, CancellationToken.None);
        await deadLetters.RemoveAsync(outboxMessage.Envelope.Id, CancellationToken.None);

        Assert.Equal(
            [
                "enqueue-outbox",
                "lock-outbox",
                "mark-delivered",
                "reschedule",
                "move-to-dead-letter",
                "try-start-inbox",
                "complete-inbox",
                "fail-inbox",
                "add-dead-letter",
                "get-dead-letters",
                "remove-dead-letter"
            ],
            client.Operations);
        Assert.Equal("messaging", client.LastOptions?.Schema);
        Assert.Equal("outbox", client.LastOptions?.OutboxTable);
    }

    [Fact]
    public async Task PostgreSQL_store_delegates_outbox_inbox_and_dead_letter_operations()
    {
        var client = new FakePostgreSqlClient();
        var store = new PostgreSqlMessageStore(
            client,
            new PostgreSqlMessageStoreOptions
            {
                Schema = "messaging",
                OutboxTable = "outbox",
                InboxTable = "inbox",
                DeadLetterTable = "dead_letters"
            });
        var outbox = (IOutboxStore)store;
        var inbox = (IInboxStore)store;
        var deadLetters = (IDeadLetterStore)store;
        var outboxMessage = Outbox();
        var deadLetter = DeadLetter(outboxMessage.Envelope);

        await outbox.EnqueueAsync(outboxMessage, CancellationToken.None);
        await outbox.LockDueAsync(10, DateTimeOffset.UtcNow, TimeSpan.FromSeconds(30), CancellationToken.None);
        await outbox.MarkDeliveredAsync(outboxMessage.Envelope.Id, CancellationToken.None);
        await outbox.RescheduleAsync(outboxMessage.Envelope.Id, 2, DateTimeOffset.UtcNow, "retry", CancellationToken.None);
        await outbox.MoveToDeadLetterAsync(outboxMessage.Envelope.Id, 3, "failed", CancellationToken.None);
        Assert.True(await inbox.TryStartAsync(outboxMessage.Envelope.Id, DateTimeOffset.UtcNow, CancellationToken.None));
        await inbox.CompleteAsync(outboxMessage.Envelope.Id, CancellationToken.None);
        await inbox.FailAsync(outboxMessage.Envelope.Id, "bad", CancellationToken.None);
        await deadLetters.AddAsync(deadLetter, CancellationToken.None);
        await deadLetters.GetAsync(5, CancellationToken.None);
        await deadLetters.RemoveAsync(outboxMessage.Envelope.Id, CancellationToken.None);

        Assert.Equal(
            [
                "enqueue-outbox",
                "lock-outbox",
                "mark-delivered",
                "reschedule",
                "move-to-dead-letter",
                "try-start-inbox",
                "complete-inbox",
                "fail-inbox",
                "add-dead-letter",
                "get-dead-letters",
                "remove-dead-letter"
            ],
            client.Operations);
        Assert.Equal("messaging", client.LastOptions?.Schema);
        Assert.Equal("dead_letters", client.LastOptions?.DeadLetterTable);
    }

    [Fact]
    public async Task SQL_Server_store_delegates_batch_operations()
    {
        var client = new FakeSqlServerClient();
        var store = new SqlServerMessageStore(client, new SqlServerMessageStoreOptions());
        var outbox = (IBatchOutboxStore)store;
        var inbox = (IBatchInboxStore)store;
        var deadLetters = (IBatchDeadLetterStore)store;
        var message = Outbox();
        var messageId = message.Envelope.Id;

        await outbox.EnqueueBatchAsync([message], CancellationToken.None);
        await outbox.MarkDeliveredBatchAsync([messageId], CancellationToken.None);
        await outbox.RescheduleBatchAsync(
            [new OutboxReschedule(messageId, 2, DateTimeOffset.UtcNow, "retry")],
            CancellationToken.None);
        await outbox.MoveToDeadLetterBatchAsync(
            [new OutboxDeadLetter(messageId, 3, "failed")],
            CancellationToken.None);
        var started = await inbox.TryStartBatchAsync(
            [new InboxStart(messageId, DateTimeOffset.UtcNow)],
            CancellationToken.None);
        await inbox.CompleteBatchAsync([messageId], CancellationToken.None);
        await inbox.FailBatchAsync([new InboxFailure(messageId, "bad")], CancellationToken.None);
        await deadLetters.AddBatchAsync([DeadLetter(message.Envelope)], CancellationToken.None);
        await deadLetters.RemoveBatchAsync([messageId], CancellationToken.None);

        Assert.Equal([messageId], started);
        Assert.Equal(
            [
                "enqueue-outbox-batch",
                "mark-delivered-batch",
                "reschedule-batch",
                "move-to-dead-letter-batch",
                "try-start-inbox-batch",
                "complete-inbox-batch",
                "fail-inbox-batch",
                "add-dead-letter-batch",
                "remove-dead-letter-batch"
            ],
            client.Operations);
    }

    [Fact]
    public async Task PostgreSQL_store_delegates_batch_operations()
    {
        var client = new FakePostgreSqlClient();
        var store = new PostgreSqlMessageStore(client, new PostgreSqlMessageStoreOptions());
        var outbox = (IBatchOutboxStore)store;
        var inbox = (IBatchInboxStore)store;
        var deadLetters = (IBatchDeadLetterStore)store;
        var message = Outbox();
        var messageId = message.Envelope.Id;

        await outbox.EnqueueBatchAsync([message], CancellationToken.None);
        await outbox.MarkDeliveredBatchAsync([messageId], CancellationToken.None);
        await outbox.RescheduleBatchAsync(
            [new OutboxReschedule(messageId, 2, DateTimeOffset.UtcNow, "retry")],
            CancellationToken.None);
        await outbox.MoveToDeadLetterBatchAsync(
            [new OutboxDeadLetter(messageId, 3, "failed")],
            CancellationToken.None);
        var started = await inbox.TryStartBatchAsync(
            [new InboxStart(messageId, DateTimeOffset.UtcNow)],
            CancellationToken.None);
        await inbox.CompleteBatchAsync([messageId], CancellationToken.None);
        await inbox.FailBatchAsync([new InboxFailure(messageId, "bad")], CancellationToken.None);
        await deadLetters.AddBatchAsync([DeadLetter(message.Envelope)], CancellationToken.None);
        await deadLetters.RemoveBatchAsync([messageId], CancellationToken.None);

        Assert.Equal([messageId], started);
        Assert.Equal(
            [
                "enqueue-outbox-batch",
                "mark-delivered-batch",
                "reschedule-batch",
                "move-to-dead-letter-batch",
                "try-start-inbox-batch",
                "complete-inbox-batch",
                "fail-inbox-batch",
                "add-dead-letter-batch",
                "remove-dead-letter-batch"
            ],
            client.Operations);
    }

    [Fact]
    public void Registers_SQL_Server_store_roles_with_DI()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ISqlServerMessageStoreClient, FakeSqlServerClient>();
        services.AddSignalynxSqlServerStores(options => options.Schema = "messaging");

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<SqlServerMessageStore>();
        Assert.Same(store, provider.GetRequiredService<IOutboxStore>());
        Assert.Same(store, provider.GetRequiredService<IInboxStore>());
        Assert.Same(store, provider.GetRequiredService<IDeadLetterStore>());
        Assert.Same(store, provider.GetRequiredService<IBatchOutboxStore>());
        Assert.Same(store, provider.GetRequiredService<IBatchInboxStore>());
        Assert.Same(store, provider.GetRequiredService<IBatchDeadLetterStore>());
        Assert.Equal("messaging", provider.GetRequiredService<SqlServerMessageStoreOptions>().Schema);
    }

    [Fact]
    public void Registers_PostgreSQL_store_roles_with_DI()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IPostgreSqlMessageStoreClient, FakePostgreSqlClient>();
        services.AddSignalynxPostgreSqlStores(options => options.Schema = "messaging");

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<PostgreSqlMessageStore>();
        Assert.Same(store, provider.GetRequiredService<IOutboxStore>());
        Assert.Same(store, provider.GetRequiredService<IInboxStore>());
        Assert.Same(store, provider.GetRequiredService<IDeadLetterStore>());
        Assert.Same(store, provider.GetRequiredService<IBatchOutboxStore>());
        Assert.Same(store, provider.GetRequiredService<IBatchInboxStore>());
        Assert.Same(store, provider.GetRequiredService<IBatchDeadLetterStore>());
        Assert.Equal("messaging", provider.GetRequiredService<PostgreSqlMessageStoreOptions>().Schema);
    }

    private static OutboxMessage Outbox()
    {
        var envelope = new MessageEnvelope(
            Guid.NewGuid(),
            typeof(StoreMessage).AssemblyQualifiedName!,
            "orders",
            "application/json",
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, string>(),
            DateTimeOffset.UtcNow,
            null);
        return new OutboxMessage(envelope, 0, DateTimeOffset.UtcNow);
    }

    private static DeadLetterMessage DeadLetter(MessageEnvelope envelope) =>
        new(envelope, 1, "failed", DateTimeOffset.UtcNow, "receiver");

    private sealed record StoreMessage;

    private sealed class FakeSqlServerClient : ISqlServerMessageStoreClient
    {
        public List<string> Operations { get; } = [];

        public SqlServerMessageStoreOptions? LastOptions { get; private set; }

        public ValueTask EnqueueOutboxAsync(
            OutboxMessage message,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("enqueue-outbox", options);

        public ValueTask EnqueueOutboxBatchAsync(
            IReadOnlyList<OutboxMessage> messages,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("enqueue-outbox-batch", options);

        public ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(
            int maxCount,
            DateTimeOffset now,
            TimeSpan lockDuration,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("lock-outbox", options);
            return ValueTask.FromResult<IReadOnlyList<OutboxMessage>>([]);
        }

        public ValueTask MarkOutboxDeliveredAsync(
            Guid messageId,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("mark-delivered", options);

        public ValueTask MarkOutboxDeliveredBatchAsync(
            IReadOnlyList<Guid> messageIds,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("mark-delivered-batch", options);

        public ValueTask RescheduleOutboxAsync(
            Guid messageId,
            int attempt,
            DateTimeOffset nextAttempt,
            string error,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("reschedule", options);

        public ValueTask RescheduleOutboxBatchAsync(
            IReadOnlyList<OutboxReschedule> messages,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("reschedule-batch", options);

        public ValueTask MoveOutboxToDeadLetterAsync(
            Guid messageId,
            int attempt,
            string error,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("move-to-dead-letter", options);

        public ValueTask MoveOutboxToDeadLetterBatchAsync(
            IReadOnlyList<OutboxDeadLetter> messages,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("move-to-dead-letter-batch", options);

        public ValueTask<bool> TryStartInboxAsync(
            Guid messageId,
            DateTimeOffset receivedAt,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("try-start-inbox", options);
            return ValueTask.FromResult(true);
        }

        public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(
            IReadOnlyList<InboxStart> messages,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("try-start-inbox-batch", options);
            return ValueTask.FromResult<IReadOnlyList<Guid>>(
                messages.Select(static message => message.MessageId).ToArray());
        }

        public ValueTask CompleteInboxAsync(
            Guid messageId,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("complete-inbox", options);

        public ValueTask CompleteInboxBatchAsync(
            IReadOnlyList<Guid> messageIds,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("complete-inbox-batch", options);

        public ValueTask FailInboxAsync(
            Guid messageId,
            string error,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("fail-inbox", options);

        public ValueTask FailInboxBatchAsync(
            IReadOnlyList<InboxFailure> messages,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("fail-inbox-batch", options);

        public ValueTask AddDeadLetterAsync(
            DeadLetterMessage message,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("add-dead-letter", options);

        public ValueTask AddDeadLetterBatchAsync(
            IReadOnlyList<DeadLetterMessage> messages,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("add-dead-letter-batch", options);

        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
            int maxCount,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("get-dead-letters", options);
            return ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>([]);
        }

        public ValueTask RemoveDeadLetterAsync(
            Guid messageId,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("remove-dead-letter", options);

        public ValueTask RemoveDeadLetterBatchAsync(
            IReadOnlyList<Guid> messageIds,
            SqlServerMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("remove-dead-letter-batch", options);

        private ValueTask TrackAsync(string operation, SqlServerMessageStoreOptions options)
        {
            Track(operation, options);
            return ValueTask.CompletedTask;
        }

        private void Track(string operation, SqlServerMessageStoreOptions options)
        {
            Operations.Add(operation);
            LastOptions = options;
        }
    }

    private sealed class FakePostgreSqlClient : IPostgreSqlMessageStoreClient
    {
        public List<string> Operations { get; } = [];

        public PostgreSqlMessageStoreOptions? LastOptions { get; private set; }

        public ValueTask EnqueueOutboxAsync(
            OutboxMessage message,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("enqueue-outbox", options);

        public ValueTask EnqueueOutboxBatchAsync(
            IReadOnlyList<OutboxMessage> messages,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("enqueue-outbox-batch", options);

        public ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(
            int maxCount,
            DateTimeOffset now,
            TimeSpan lockDuration,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("lock-outbox", options);
            return ValueTask.FromResult<IReadOnlyList<OutboxMessage>>([]);
        }

        public ValueTask MarkOutboxDeliveredAsync(
            Guid messageId,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("mark-delivered", options);

        public ValueTask MarkOutboxDeliveredBatchAsync(
            IReadOnlyList<Guid> messageIds,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("mark-delivered-batch", options);

        public ValueTask RescheduleOutboxAsync(
            Guid messageId,
            int attempt,
            DateTimeOffset nextAttempt,
            string error,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("reschedule", options);

        public ValueTask RescheduleOutboxBatchAsync(
            IReadOnlyList<OutboxReschedule> messages,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("reschedule-batch", options);

        public ValueTask MoveOutboxToDeadLetterAsync(
            Guid messageId,
            int attempt,
            string error,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("move-to-dead-letter", options);

        public ValueTask MoveOutboxToDeadLetterBatchAsync(
            IReadOnlyList<OutboxDeadLetter> messages,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("move-to-dead-letter-batch", options);

        public ValueTask<bool> TryStartInboxAsync(
            Guid messageId,
            DateTimeOffset receivedAt,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("try-start-inbox", options);
            return ValueTask.FromResult(true);
        }

        public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(
            IReadOnlyList<InboxStart> messages,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("try-start-inbox-batch", options);
            return ValueTask.FromResult<IReadOnlyList<Guid>>(
                messages.Select(static message => message.MessageId).ToArray());
        }

        public ValueTask CompleteInboxAsync(
            Guid messageId,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("complete-inbox", options);

        public ValueTask CompleteInboxBatchAsync(
            IReadOnlyList<Guid> messageIds,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("complete-inbox-batch", options);

        public ValueTask FailInboxAsync(
            Guid messageId,
            string error,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("fail-inbox", options);

        public ValueTask FailInboxBatchAsync(
            IReadOnlyList<InboxFailure> messages,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("fail-inbox-batch", options);

        public ValueTask AddDeadLetterAsync(
            DeadLetterMessage message,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("add-dead-letter", options);

        public ValueTask AddDeadLetterBatchAsync(
            IReadOnlyList<DeadLetterMessage> messages,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("add-dead-letter-batch", options);

        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
            int maxCount,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            Track("get-dead-letters", options);
            return ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>([]);
        }

        public ValueTask RemoveDeadLetterAsync(
            Guid messageId,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("remove-dead-letter", options);

        public ValueTask RemoveDeadLetterBatchAsync(
            IReadOnlyList<Guid> messageIds,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken) =>
            TrackAsync("remove-dead-letter-batch", options);

        private ValueTask TrackAsync(string operation, PostgreSqlMessageStoreOptions options)
        {
            Track(operation, options);
            return ValueTask.CompletedTask;
        }

        private void Track(string operation, PostgreSqlMessageStoreOptions options)
        {
            Operations.Add(operation);
            LastOptions = options;
        }
    }
}
