using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.SqlServer;

public sealed class SqlServerMessageStoreOptions
{
    public string Schema { get; set; } = "dbo";

    public string OutboxTable { get; set; } = "SignalynxOutbox";

    public string InboxTable { get; set; } = "SignalynxInbox";

    public string DeadLetterTable { get; set; } = "SignalynxDeadLetters";
}

public interface ISqlServerMessageStoreClient
{
    ValueTask EnqueueOutboxAsync(
        OutboxMessage message,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask EnqueueOutboxBatchAsync(
        IReadOnlyList<OutboxMessage> messages,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(
        int maxCount,
        DateTimeOffset now,
        TimeSpan lockDuration,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MarkOutboxDeliveredAsync(
        Guid messageId,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MarkOutboxDeliveredBatchAsync(
        IReadOnlyList<Guid> messageIds,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RescheduleOutboxAsync(
        Guid messageId,
        int attempt,
        DateTimeOffset nextAttempt,
        string error,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RescheduleOutboxBatchAsync(
        IReadOnlyList<OutboxReschedule> messages,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MoveOutboxToDeadLetterAsync(
        Guid messageId,
        int attempt,
        string error,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MoveOutboxToDeadLetterBatchAsync(
        IReadOnlyList<OutboxDeadLetter> messages,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<bool> TryStartInboxAsync(
        Guid messageId,
        DateTimeOffset receivedAt,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(
        IReadOnlyList<InboxStart> messages,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask CompleteInboxAsync(
        Guid messageId,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask CompleteInboxBatchAsync(
        IReadOnlyList<Guid> messageIds,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask FailInboxAsync(
        Guid messageId,
        string error,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask FailInboxBatchAsync(
        IReadOnlyList<InboxFailure> messages,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask AddDeadLetterAsync(
        DeadLetterMessage message,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask AddDeadLetterBatchAsync(
        IReadOnlyList<DeadLetterMessage> messages,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
        int maxCount,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RemoveDeadLetterAsync(
        Guid messageId,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RemoveDeadLetterBatchAsync(
        IReadOnlyList<Guid> messageIds,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken);
}

public sealed class SqlServerMessageStore :
    IOutboxStore,
    IBatchOutboxStore,
    IInboxStore,
    IBatchInboxStore,
    IDeadLetterStore,
    IBatchDeadLetterStore
{
    private readonly ISqlServerMessageStoreClient _client;
    private readonly SqlServerMessageStoreOptions _options;

    public SqlServerMessageStore(
        ISqlServerMessageStoreClient client,
        SqlServerMessageStoreOptions options)
    {
        _client = client;
        _options = options;
    }

    public ValueTask EnqueueAsync(
        OutboxMessage message,
        CancellationToken cancellationToken) =>
        _client.EnqueueOutboxAsync(message, _options, cancellationToken);

    public ValueTask EnqueueBatchAsync(
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken) =>
        _client.EnqueueOutboxBatchAsync(messages, _options, cancellationToken);

    public ValueTask<IReadOnlyList<OutboxMessage>> LockDueAsync(
        int maxCount,
        DateTimeOffset now,
        TimeSpan lockDuration,
        CancellationToken cancellationToken) =>
        _client.LockDueOutboxAsync(
            maxCount,
            now,
            lockDuration,
            _options,
            cancellationToken);

    public ValueTask MarkDeliveredAsync(
        Guid messageId,
        CancellationToken cancellationToken) =>
        _client.MarkOutboxDeliveredAsync(messageId, _options, cancellationToken);

    public ValueTask MarkDeliveredBatchAsync(
        IReadOnlyList<Guid> messageIds,
        CancellationToken cancellationToken) =>
        _client.MarkOutboxDeliveredBatchAsync(messageIds, _options, cancellationToken);

    public ValueTask RescheduleAsync(
        Guid messageId,
        int attempt,
        DateTimeOffset nextAttempt,
        string error,
        CancellationToken cancellationToken) =>
        _client.RescheduleOutboxAsync(
            messageId,
            attempt,
            nextAttempt,
            error,
            _options,
            cancellationToken);

    public ValueTask RescheduleBatchAsync(
        IReadOnlyList<OutboxReschedule> messages,
        CancellationToken cancellationToken) =>
        _client.RescheduleOutboxBatchAsync(messages, _options, cancellationToken);

    public ValueTask MoveToDeadLetterAsync(
        Guid messageId,
        int attempt,
        string error,
        CancellationToken cancellationToken) =>
        _client.MoveOutboxToDeadLetterAsync(
            messageId,
            attempt,
            error,
            _options,
            cancellationToken);

    public ValueTask MoveToDeadLetterBatchAsync(
        IReadOnlyList<OutboxDeadLetter> messages,
        CancellationToken cancellationToken) =>
        _client.MoveOutboxToDeadLetterBatchAsync(messages, _options, cancellationToken);

    public ValueTask<bool> TryStartAsync(
        Guid messageId,
        DateTimeOffset receivedAt,
        CancellationToken cancellationToken) =>
        _client.TryStartInboxAsync(messageId, receivedAt, _options, cancellationToken);

    public ValueTask<IReadOnlyList<Guid>> TryStartBatchAsync(
        IReadOnlyList<InboxStart> messages,
        CancellationToken cancellationToken) =>
        _client.TryStartInboxBatchAsync(messages, _options, cancellationToken);

    public ValueTask CompleteAsync(
        Guid messageId,
        CancellationToken cancellationToken) =>
        _client.CompleteInboxAsync(messageId, _options, cancellationToken);

    public ValueTask CompleteBatchAsync(
        IReadOnlyList<Guid> messageIds,
        CancellationToken cancellationToken) =>
        _client.CompleteInboxBatchAsync(messageIds, _options, cancellationToken);

    public ValueTask FailAsync(
        Guid messageId,
        string error,
        CancellationToken cancellationToken) =>
        _client.FailInboxAsync(messageId, error, _options, cancellationToken);

    public ValueTask FailBatchAsync(
        IReadOnlyList<InboxFailure> messages,
        CancellationToken cancellationToken) =>
        _client.FailInboxBatchAsync(messages, _options, cancellationToken);

    public ValueTask AddAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken) =>
        _client.AddDeadLetterAsync(message, _options, cancellationToken);

    public ValueTask AddBatchAsync(
        IReadOnlyList<DeadLetterMessage> messages,
        CancellationToken cancellationToken) =>
        _client.AddDeadLetterBatchAsync(messages, _options, cancellationToken);

    public ValueTask<IReadOnlyList<DeadLetterMessage>> GetAsync(
        int maxCount,
        CancellationToken cancellationToken) =>
        _client.GetDeadLettersAsync(maxCount, _options, cancellationToken);

    public ValueTask RemoveAsync(
        Guid messageId,
        CancellationToken cancellationToken) =>
        _client.RemoveDeadLetterAsync(messageId, _options, cancellationToken);

    public ValueTask RemoveBatchAsync(
        IReadOnlyList<Guid> messageIds,
        CancellationToken cancellationToken) =>
        _client.RemoveDeadLetterBatchAsync(messageIds, _options, cancellationToken);
}

public static class SqlServerServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxSqlServerStores(
        this IServiceCollection services,
        Action<SqlServerMessageStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new SqlServerMessageStoreOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<SqlServerMessageStore>();
        services.TryAddSingleton<IOutboxStore>(
            static provider => provider.GetRequiredService<SqlServerMessageStore>());
        services.TryAddSingleton<IInboxStore>(
            static provider => provider.GetRequiredService<SqlServerMessageStore>());
        services.TryAddSingleton<IDeadLetterStore>(
            static provider => provider.GetRequiredService<SqlServerMessageStore>());
        services.TryAddSingleton<IBatchOutboxStore>(
            static provider => provider.GetRequiredService<SqlServerMessageStore>());
        services.TryAddSingleton<IBatchInboxStore>(
            static provider => provider.GetRequiredService<SqlServerMessageStore>());
        services.TryAddSingleton<IBatchDeadLetterStore>(
            static provider => provider.GetRequiredService<SqlServerMessageStore>());
        return services;
    }
}
