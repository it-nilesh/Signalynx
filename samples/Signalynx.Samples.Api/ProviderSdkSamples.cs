#if SIGNALYNX_PROVIDER_SDK_SAMPLES
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Signalynx.Messaging;
using Signalynx.Messaging.Kafka;
using Signalynx.Messaging.PostgreSql;
using Signalynx.Messaging.RabbitMQ;
using Signalynx.Messaging.SqlServer;

namespace Signalynx.Samples.Api;

internal sealed class RabbitMqOfficialSdkClient : IRabbitMqTransportClient, IAsyncDisposable
{
    private readonly IChannel _channel;

    public RabbitMqOfficialSdkClient(IChannel channel)
    {
        _channel = channel;
    }

    public async ValueTask PublishAsync(
        MessageEnvelope envelope,
        string routingKey,
        CancellationToken cancellationToken)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(envelope);
        await _channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: routingKey,
            body: body,
            cancellationToken: cancellationToken);
    }

    public async IAsyncEnumerable<RabbitMqTransportMessage> ReceiveAsync(
        string queueName,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);
        var deliveries = new System.Threading.Channels.Channel<RabbitMqTransportMessage>();
        consumer.ReceivedAsync += async (_, args) =>
        {
            var envelope = JsonSerializer.Deserialize<MessageEnvelope>(args.Body.Span)!;
            await deliveries.Writer.WriteAsync(
                new RabbitMqTransportMessage(envelope, 1, args),
                cancellationToken);
        };

        await _channel.BasicConsumeAsync(queueName, autoAck: false, consumer, cancellationToken);
        await foreach (var delivery in deliveries.Reader.ReadAllAsync(cancellationToken))
        {
            yield return delivery;
        }
    }

    public ValueTask AcknowledgeAsync(RabbitMqTransportMessage message, CancellationToken cancellationToken)
    {
        var delivery = (BasicDeliverEventArgs)message.NativeMessage!;
        return _channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken);
    }

    public ValueTask RequeueAsync(RabbitMqTransportMessage message, TimeSpan delay, CancellationToken cancellationToken)
    {
        var delivery = (BasicDeliverEventArgs)message.NativeMessage!;
        return _channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, cancellationToken);
    }

    public ValueTask DeadLetterAsync(RabbitMqTransportMessage message, string error, CancellationToken cancellationToken)
    {
        var delivery = (BasicDeliverEventArgs)message.NativeMessage!;
        return _channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, cancellationToken);
    }

    public ValueTask DisposeAsync() => _channel.DisposeAsync();
}

internal sealed class KafkaOfficialSdkClient : IKafkaTransportClient, IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IConsumer<string, byte[]> _consumer;

    public KafkaOfficialSdkClient(
        IProducer<string, byte[]> producer,
        IConsumer<string, byte[]> consumer)
    {
        _producer = producer;
        _consumer = consumer;
    }

    public async ValueTask ProduceAsync(
        MessageEnvelope envelope,
        string topic,
        CancellationToken cancellationToken)
    {
        await _producer.ProduceAsync(
            topic,
            new Message<string, byte[]>
            {
                Key = envelope.Id.ToString("N"),
                Value = JsonSerializer.SerializeToUtf8Bytes(envelope)
            },
            cancellationToken);
    }

    public async IAsyncEnumerable<KafkaTransportMessage> ConsumeAsync(
        string topic,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = _consumer.Consume(cancellationToken);
            var envelope = JsonSerializer.Deserialize<MessageEnvelope>(result.Message.Value)!;
            yield return new KafkaTransportMessage(envelope, 1, result);
            await Task.Yield();
        }
    }

    public ValueTask CommitAsync(KafkaTransportMessage message, CancellationToken cancellationToken)
    {
        _consumer.Commit((ConsumeResult<string, byte[]>)message.NativeMessage!);
        return ValueTask.CompletedTask;
    }

    public ValueTask RetryAsync(KafkaTransportMessage message, TimeSpan delay, CancellationToken cancellationToken) =>
        ProduceAsync(message.Envelope, message.Envelope.Destination, cancellationToken);

    public ValueTask DeadLetterAsync(KafkaTransportMessage message, string error, CancellationToken cancellationToken) =>
        ProduceAsync(message.Envelope, $"{message.Envelope.Destination}.dead", cancellationToken);

    public void Dispose()
    {
        _consumer.Dispose();
        _producer.Dispose();
    }
}

internal sealed class SqlServerOfficialSdkClient : ISqlServerMessageStoreClient
{
    private readonly string _connectionString;

    public SqlServerOfficialSdkClient(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async ValueTask EnqueueOutboxAsync(
        OutboxMessage message,
        SqlServerMessageStoreOptions options,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            insert into [{options.Schema}].[{options.OutboxTable}]
                (Id, MessageType, Destination, ContentType, Body, CreatedAt, NextAttempt, Attempt)
            values (@id, @messageType, @destination, @contentType, @body, @createdAt, @nextAttempt, @attempt)
            """;
        command.Parameters.AddWithValue("@id", message.Envelope.Id);
        command.Parameters.AddWithValue("@messageType", message.Envelope.MessageType);
        command.Parameters.AddWithValue("@destination", message.Envelope.Destination);
        command.Parameters.AddWithValue("@contentType", message.Envelope.ContentType);
        command.Parameters.AddWithValue("@body", message.Envelope.Body.ToArray());
        command.Parameters.AddWithValue("@createdAt", message.Envelope.CreatedAt);
        command.Parameters.AddWithValue("@nextAttempt", message.NextAttempt);
        command.Parameters.AddWithValue("@attempt", message.Attempt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public ValueTask EnqueueOutboxBatchAsync(IReadOnlyList<OutboxMessage> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(int maxCount, DateTimeOffset now, TimeSpan lockDuration, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MarkOutboxDeliveredAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MarkOutboxDeliveredBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RescheduleOutboxAsync(Guid messageId, int attempt, DateTimeOffset nextAttempt, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RescheduleOutboxBatchAsync(IReadOnlyList<OutboxReschedule> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MoveOutboxToDeadLetterAsync(Guid messageId, int attempt, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MoveOutboxToDeadLetterBatchAsync(IReadOnlyList<OutboxDeadLetter> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> TryStartInboxAsync(Guid messageId, DateTimeOffset receivedAt, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(IReadOnlyList<InboxStart> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask CompleteInboxAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask CompleteInboxBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask FailInboxAsync(Guid messageId, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask FailInboxBatchAsync(IReadOnlyList<InboxFailure> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask AddDeadLetterAsync(DeadLetterMessage message, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask AddDeadLetterBatchAsync(IReadOnlyList<DeadLetterMessage> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(int maxCount, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RemoveDeadLetterAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RemoveDeadLetterBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
}

internal sealed class PostgreSqlOfficialSdkClient : IPostgreSqlMessageStoreClient
{
    private readonly string _connectionString;

    public PostgreSqlOfficialSdkClient(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(
        int maxCount,
        DateTimeOffset now,
        TimeSpan lockDuration,
        PostgreSqlMessageStoreOptions options,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            update "{options.Schema}"."{options.OutboxTable}"
            set "LockedUntil" = @lockedUntil
            where "Id" in (
                select "Id"
                from "{options.Schema}"."{options.OutboxTable}"
                where "NextAttempt" <= @now
                  and ("LockedUntil" is null or "LockedUntil" <= @now)
                order by "NextAttempt"
                for update skip locked
                limit @maxCount
            )
            returning "Id", "MessageType", "Destination", "ContentType", "Body", "CreatedAt", "NextAttempt", "Attempt", "LockedUntil", "LastError"
            """;
        command.Parameters.AddWithValue("@lockedUntil", now + lockDuration);
        command.Parameters.AddWithValue("@now", now);
        command.Parameters.AddWithValue("@maxCount", maxCount);

        var messages = new List<OutboxMessage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var envelope = new MessageEnvelope(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetFieldValue<byte[]>(4),
                new Dictionary<string, string>(),
                reader.GetFieldValue<DateTimeOffset>(5),
                null);
            messages.Add(new OutboxMessage(
                envelope,
                reader.GetInt32(7),
                reader.GetFieldValue<DateTimeOffset>(6),
                reader.GetFieldValue<DateTimeOffset>(8),
                reader.IsDBNull(9) ? null : reader.GetString(9)));
        }

        return messages;
    }

    public ValueTask EnqueueOutboxAsync(OutboxMessage message, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask EnqueueOutboxBatchAsync(IReadOnlyList<OutboxMessage> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MarkOutboxDeliveredAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MarkOutboxDeliveredBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RescheduleOutboxAsync(Guid messageId, int attempt, DateTimeOffset nextAttempt, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RescheduleOutboxBatchAsync(IReadOnlyList<OutboxReschedule> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MoveOutboxToDeadLetterAsync(Guid messageId, int attempt, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask MoveOutboxToDeadLetterBatchAsync(IReadOnlyList<OutboxDeadLetter> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> TryStartInboxAsync(Guid messageId, DateTimeOffset receivedAt, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(IReadOnlyList<InboxStart> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask CompleteInboxAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask CompleteInboxBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask FailInboxAsync(Guid messageId, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask FailInboxBatchAsync(IReadOnlyList<InboxFailure> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask AddDeadLetterAsync(DeadLetterMessage message, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask AddDeadLetterBatchAsync(IReadOnlyList<DeadLetterMessage> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(int maxCount, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RemoveDeadLetterAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RemoveDeadLetterBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => throw new NotImplementedException();
}
#endif
