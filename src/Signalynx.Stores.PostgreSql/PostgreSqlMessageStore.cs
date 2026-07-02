using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.PostgreSql;

public sealed class PostgreSqlMessageStoreOptions
{
    public string Schema { get; set; } = "public";

    public string OutboxTable { get; set; } = "signalynx_outbox";

    public string InboxTable { get; set; } = "signalynx_inbox";

    public string DeadLetterTable { get; set; } = "signalynx_dead_letters";
}

public interface IPostgreSqlMessageStoreClient
{
    ValueTask EnqueueOutboxAsync(
        OutboxMessage message,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask EnqueueOutboxBatchAsync(
        IReadOnlyList<OutboxMessage> messages,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(
        int maxCount,
        DateTimeOffset now,
        TimeSpan lockDuration,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MarkOutboxDeliveredAsync(
        Guid messageId,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MarkOutboxDeliveredBatchAsync(
        IReadOnlyList<Guid> messageIds,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RescheduleOutboxAsync(
        Guid messageId,
        int attempt,
        DateTimeOffset nextAttempt,
        string error,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RescheduleOutboxBatchAsync(
        IReadOnlyList<OutboxReschedule> messages,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MoveOutboxToDeadLetterAsync(
        Guid messageId,
        int attempt,
        string error,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask MoveOutboxToDeadLetterBatchAsync(
        IReadOnlyList<OutboxDeadLetter> messages,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<bool> TryStartInboxAsync(
        Guid messageId,
        DateTimeOffset receivedAt,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(
        IReadOnlyList<InboxStart> messages,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask CompleteInboxAsync(
        Guid messageId,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask CompleteInboxBatchAsync(
        IReadOnlyList<Guid> messageIds,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask FailInboxAsync(
        Guid messageId,
        string error,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask FailInboxBatchAsync(
        IReadOnlyList<InboxFailure> messages,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask AddDeadLetterAsync(
        DeadLetterMessage message,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask AddDeadLetterBatchAsync(
        IReadOnlyList<DeadLetterMessage> messages,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(
        int maxCount,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RemoveDeadLetterAsync(
        Guid messageId,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);

    ValueTask RemoveDeadLetterBatchAsync(
        IReadOnlyList<Guid> messageIds,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken);
}

public sealed class PostgreSqlMessageStore :
    IOutboxStore,
    IBatchOutboxStore,
    IInboxStore,
    IBatchInboxStore,
    IDeadLetterStore,
    IBatchDeadLetterStore
{
    private readonly IPostgreSqlMessageStoreClient _client;
    private readonly PostgreSqlMessageStoreOptions _options;

    public PostgreSqlMessageStore(
        IPostgreSqlMessageStoreClient client,
        PostgreSqlMessageStoreOptions options)
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

public static class PostgreSqlServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxPostgreSqlStores(
        this IServiceCollection services,
        Action<PostgreSqlMessageStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new PostgreSqlMessageStoreOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<PostgreSqlMessageStore>();
        services.TryAddSingleton<IOutboxStore>(
            static provider => provider.GetRequiredService<PostgreSqlMessageStore>());
        services.TryAddSingleton<IInboxStore>(
            static provider => provider.GetRequiredService<PostgreSqlMessageStore>());
        services.TryAddSingleton<IDeadLetterStore>(
            static provider => provider.GetRequiredService<PostgreSqlMessageStore>());
        services.TryAddSingleton<IBatchOutboxStore>(
            static provider => provider.GetRequiredService<PostgreSqlMessageStore>());
        services.TryAddSingleton<IBatchInboxStore>(
            static provider => provider.GetRequiredService<PostgreSqlMessageStore>());
        services.TryAddSingleton<IBatchDeadLetterStore>(
            static provider => provider.GetRequiredService<PostgreSqlMessageStore>());
        return services;
    }
}
