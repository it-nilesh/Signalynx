using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Data.SqlClient;
using Npgsql;
using RabbitMQ.Client;
using Signalynx.Messaging;
using Signalynx.Messaging.Kafka;
using Signalynx.Messaging.PostgreSql;
using Signalynx.Messaging.RabbitMQ;
using Signalynx.Messaging.SqlServer;

namespace Signalynx.Performance;

[MemoryDiagnoser]
public class ProviderBackedMessagingLoadBenchmarks
{
    private const string OptInVariable = "SIGNALYNX_PROVIDER_LOAD_BENCHMARK";
    private const string RabbitQueue = "signalynx.provider.load";
    private const string KafkaTopic = "signalynx.provider.load";

    private ProviderHarness _harness = null!;
    private SystemTextJsonMessageSerializer _serializer = null!;
    private SignalynxMessageBus _bus = null!;
    private ExponentialBackoffRetryPolicy _retryPolicy = null!;
    private BenchmarkHandler _handler = null!;
    private BenchmarkTransportMessage _message = null!;

    [Params("RabbitMQ/PostgreSQL", "RabbitMQ/SQLServer", "Kafka/PostgreSQL", "Kafka/SQLServer")]
    public string ProviderPair { get; set; } = "RabbitMQ/PostgreSQL";

    [Params(10, 100)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        if (!ProviderLoadBenchmarkEnabled())
        {
            throw new InvalidOperationException(
                $"Set {OptInVariable}=1 before running provider-backed load benchmarks.");
        }

        _serializer = new SystemTextJsonMessageSerializer();
        _retryPolicy = new ExponentialBackoffRetryPolicy(
            new SignalynxMessagingOptions
            {
                MaxDeliveryAttempts = 2,
                BaseRetryDelay = TimeSpan.Zero
            });
        _handler = new BenchmarkHandler();
        _message = new BenchmarkTransportMessage(Guid.NewGuid(), "provider-backed");
        _harness = await ProviderHarness.CreateAsync(ProviderPair).ConfigureAwait(false);
        _bus = new SignalynxMessageBus(_harness.Outbox, _serializer, TimeProvider.System);
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_harness is not null)
        {
            await _harness.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async ValueTask<int> TransportOutboxInboxRetryAndHandlerExecution()
    {
        var handled = 0;
        for (var i = 0; i < MessageCount; i++)
        {
            var destination = _harness.TransportKind == TransportKind.RabbitMq
                ? RabbitQueue
                : KafkaTopic;

            await _bus.EnqueueAsync(_message, destination).ConfigureAwait(false);
            handled += await DrainOutboxAndReceiveAsync(i == 0).ConfigureAwait(false);
        }

        return handled;
    }

    private async ValueTask<int> DrainOutboxAndReceiveAsync(bool forceRetry)
    {
        var now = DateTimeOffset.UtcNow;
        var messages = await _harness.Outbox.LockDueAsync(
            32,
            now,
            TimeSpan.FromSeconds(30),
            CancellationToken.None).ConfigureAwait(false);

        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            try
            {
                await _harness.Transport.SendAsync(message.Envelope, CancellationToken.None).ConfigureAwait(false);
                await _harness.Outbox.MarkDeliveredAsync(message.Envelope.Id, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                var attempt = message.Attempt + 1;
                if (_retryPolicy.ShouldRetry(attempt, exception, out var delay))
                {
                    await _harness.Outbox.RescheduleAsync(
                        message.Envelope.Id,
                        attempt,
                        now + delay,
                        exception.Message,
                        CancellationToken.None).ConfigureAwait(false);
                    continue;
                }

                await _harness.DeadLetters.AddAsync(
                    new DeadLetterMessage(message.Envelope, attempt, exception.ToString(), now, "outbox"),
                    CancellationToken.None).ConfigureAwait(false);
                await _harness.Outbox.MoveToDeadLetterAsync(
                    message.Envelope.Id,
                    attempt,
                    exception.ToString(),
                    CancellationToken.None).ConfigureAwait(false);
            }
        }

        var handled = 0;
        var retried = false;
        await foreach (var delivery in _harness.Transport.ReceiveAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (!await _harness.Inbox.TryStartAsync(delivery.Envelope.Id, now, CancellationToken.None).ConfigureAwait(false))
            {
                await delivery.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            try
            {
                if (forceRetry && !retried)
                {
                    retried = true;
                    throw new InvalidOperationException("Forced provider-backed retry.");
                }

                var deserialized = (BenchmarkTransportMessage)_serializer.Deserialize(
                    delivery.Envelope.Body,
                    typeof(BenchmarkTransportMessage));
                await _handler.HandleAsync(
                    deserialized,
                    new MessageContext(delivery.Envelope, delivery.Attempt, CancellationToken.None)).ConfigureAwait(false);
                await _harness.Inbox.CompleteAsync(delivery.Envelope.Id, CancellationToken.None).ConfigureAwait(false);
                await delivery.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
                handled++;
            }
            catch (Exception exception)
            {
                await _harness.Inbox.FailAsync(delivery.Envelope.Id, exception.ToString(), CancellationToken.None).ConfigureAwait(false);
                if (_retryPolicy.ShouldRetry(delivery.Attempt, exception, out var delay))
                {
                    await delivery.RetryAsync(delay, CancellationToken.None).ConfigureAwait(false);
                    continue;
                }

                await _harness.DeadLetters.AddAsync(
                    new DeadLetterMessage(delivery.Envelope, delivery.Attempt, exception.ToString(), now, "receiver"),
                    CancellationToken.None).ConfigureAwait(false);
                await delivery.DeadLetterAsync(exception.ToString(), CancellationToken.None).ConfigureAwait(false);
            }
        }

        return handled;
    }

    private static bool ProviderLoadBenchmarkEnabled() =>
        string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "true", StringComparison.OrdinalIgnoreCase);

    public sealed record BenchmarkTransportMessage(Guid Id, string Value);

    private sealed class BenchmarkHandler : IMessageHandler<BenchmarkTransportMessage>
    {
        public ValueTask HandleAsync(
            BenchmarkTransportMessage message,
            MessageContext context) =>
            ValueTask.CompletedTask;
    }

    private enum TransportKind
    {
        RabbitMq,
        Kafka
    }

    private sealed class ProviderHarness : IAsyncDisposable
    {
        private readonly IAsyncDisposable? _asyncDisposable;
        private readonly IDisposable? _disposable;

        private ProviderHarness(
            TransportKind transportKind,
            IMessageTransport transport,
            IOutboxStore outbox,
            IInboxStore inbox,
            IDeadLetterStore deadLetters,
            IAsyncDisposable? asyncDisposable,
            IDisposable? disposable)
        {
            TransportKind = transportKind;
            Transport = transport;
            Outbox = outbox;
            Inbox = inbox;
            DeadLetters = deadLetters;
            _asyncDisposable = asyncDisposable;
            _disposable = disposable;
        }

        public TransportKind TransportKind { get; }

        public IMessageTransport Transport { get; }

        public IOutboxStore Outbox { get; }

        public IInboxStore Inbox { get; }

        public IDeadLetterStore DeadLetters { get; }

        public static async ValueTask<ProviderHarness> CreateAsync(string providerPair)
        {
            var useRabbit = providerPair.StartsWith("RabbitMQ/", StringComparison.OrdinalIgnoreCase);
            var usePostgreSql = providerPair.EndsWith("/PostgreSQL", StringComparison.OrdinalIgnoreCase);

            var store = usePostgreSql
                ? await CreatePostgreSqlStoreAsync().ConfigureAwait(false)
                : await CreateSqlServerStoreAsync().ConfigureAwait(false);
            var transport = useRabbit
                ? await CreateRabbitMqTransportAsync().ConfigureAwait(false)
                : await CreateKafkaTransportAsync().ConfigureAwait(false);

            return new ProviderHarness(
                useRabbit ? TransportKind.RabbitMq : TransportKind.Kafka,
                transport.Transport,
                store.Outbox,
                store.Inbox,
                store.DeadLetters,
                transport.AsyncDisposable,
                transport.Disposable);
        }

        public async ValueTask DisposeAsync()
        {
            if (_asyncDisposable is not null)
            {
                await _asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }

            _disposable?.Dispose();
        }

        private static async ValueTask<ProviderTransport> CreateRabbitMqTransportAsync()
        {
            var factory = new ConnectionFactory
            {
                HostName = Environment.GetEnvironmentVariable("SIGNALYNX_RABBITMQ_HOST") ?? "localhost",
                Port = int.Parse(Environment.GetEnvironmentVariable("SIGNALYNX_RABBITMQ_PORT") ?? "5672")
            };
            var connection = await factory.CreateConnectionAsync().ConfigureAwait(false);
            var channel = await connection.CreateChannelAsync().ConfigureAwait(false);
            await channel.QueueDeclareAsync(
                RabbitQueue,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null).ConfigureAwait(false);
            await channel.QueuePurgeAsync(RabbitQueue).ConfigureAwait(false);

            var client = new RabbitMqSdkClient(channel);
            return new ProviderTransport(
                new RabbitMqMessageTransport(client, new RabbitMqTransportOptions { QueueName = RabbitQueue }),
                new RabbitMqResources(client, channel, connection),
                null);
        }

        private static async ValueTask<ProviderTransport> CreateKafkaTransportAsync()
        {
            var bootstrapServers = Environment.GetEnvironmentVariable("SIGNALYNX_KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
            using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await admin.CreateTopicsAsync(
                        [new TopicSpecification { Name = KafkaTopic, NumPartitions = 1, ReplicationFactor = 1 }])
                        .ConfigureAwait(false);
                }
                catch (CreateTopicsException exception) when (exception.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
                {
                }
            }

            var producer = new ProducerBuilder<string, byte[]>(
                new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
            var consumer = new ConsumerBuilder<string, byte[]>(
                new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = $"signalynx-provider-load-{Guid.NewGuid():N}",
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    EnableAutoCommit = false
                }).Build();
            var client = new KafkaSdkClient(producer, consumer);
            return new ProviderTransport(
                new KafkaMessageTransport(client, new KafkaTransportOptions { Topic = KafkaTopic }),
                null,
                client);
        }

        private static async ValueTask<ProviderStore> CreatePostgreSqlStoreAsync()
        {
            var connectionString = Environment.GetEnvironmentVariable("SIGNALYNX_POSTGRESQL_CONNECTION_STRING")
                ?? "Host=localhost;Port=5432;Database=signalynx;Username=signalynx;Password=signalynx";
            var options = new PostgreSqlMessageStoreOptions
            {
                Schema = "public",
                OutboxTable = "signalynx_load_outbox",
                InboxTable = "signalynx_load_inbox",
                DeadLetterTable = "signalynx_load_dead_letters"
            };
            var client = new PostgreSqlSdkStoreClient(connectionString);
            await client.InitializeAsync(options, CancellationToken.None).ConfigureAwait(false);
            var store = new PostgreSqlMessageStore(client, options);
            return new ProviderStore(store, store, store);
        }

        private static async ValueTask<ProviderStore> CreateSqlServerStoreAsync()
        {
            var connectionString = Environment.GetEnvironmentVariable("SIGNALYNX_SQLSERVER_CONNECTION_STRING")
                ?? "Server=localhost,1433;Database=signalynx;User Id=sa;Password=Signalynx!2026;TrustServerCertificate=True;Encrypt=True";
            await SqlServerSdkStoreClient.EnsureDatabaseAsync(connectionString, CancellationToken.None).ConfigureAwait(false);
            var options = new SqlServerMessageStoreOptions
            {
                Schema = "dbo",
                OutboxTable = "SignalynxLoadOutbox",
                InboxTable = "SignalynxLoadInbox",
                DeadLetterTable = "SignalynxLoadDeadLetters"
            };
            var client = new SqlServerSdkStoreClient(connectionString);
            await client.InitializeAsync(options, CancellationToken.None).ConfigureAwait(false);
            var store = new SqlServerMessageStore(client, options);
            return new ProviderStore(store, store, store);
        }
    }

    private sealed record ProviderTransport(
        IMessageTransport Transport,
        IAsyncDisposable? AsyncDisposable,
        IDisposable? Disposable);

    private sealed record ProviderStore(
        IOutboxStore Outbox,
        IInboxStore Inbox,
        IDeadLetterStore DeadLetters);

    private sealed class RabbitMqResources(
        RabbitMqSdkClient client,
        IChannel channel,
        IConnection connection) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await client.DisposeAsync().ConfigureAwait(false);
            await channel.DisposeAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed class RabbitMqSdkClient(IChannel channel) : IRabbitMqTransportClient, IAsyncDisposable
    {
        public async ValueTask PublishAsync(
            MessageEnvelope envelope,
            string routingKey,
            CancellationToken cancellationToken)
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(envelope);
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: routingKey,
                body: body,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<RabbitMqTransportMessage> ReceiveAsync(
            string queueName,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await channel.BasicGetAsync(queueName, autoAck: false, cancellationToken)
                    .ConfigureAwait(false);
                if (result is null)
                {
                    yield break;
                }

                var envelope = JsonSerializer.Deserialize<MessageEnvelope>(result.Body.Span)!;
                yield return new RabbitMqTransportMessage(envelope, 1, result);
            }
        }

        public ValueTask AcknowledgeAsync(RabbitMqTransportMessage message, CancellationToken cancellationToken)
        {
            var delivery = (BasicGetResult)message.NativeMessage!;
            return channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken);
        }

        public ValueTask RequeueAsync(RabbitMqTransportMessage message, TimeSpan delay, CancellationToken cancellationToken)
        {
            var delivery = (BasicGetResult)message.NativeMessage!;
            return channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, cancellationToken);
        }

        public ValueTask DeadLetterAsync(RabbitMqTransportMessage message, string error, CancellationToken cancellationToken)
        {
            var delivery = (BasicGetResult)message.NativeMessage!;
            return channel.BasicRejectAsync(delivery.DeliveryTag, requeue: false, cancellationToken);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class KafkaSdkClient(
        IProducer<string, byte[]> producer,
        IConsumer<string, byte[]> consumer) : IKafkaTransportClient, IDisposable
    {
        private bool _subscribed;

        public async ValueTask ProduceAsync(
            MessageEnvelope envelope,
            string topic,
            CancellationToken cancellationToken)
        {
            await producer.ProduceAsync(
                topic,
                new Message<string, byte[]>
                {
                    Key = envelope.Id.ToString("N"),
                    Value = JsonSerializer.SerializeToUtf8Bytes(envelope)
                },
                cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<KafkaTransportMessage> ConsumeAsync(
            string topic,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (!_subscribed)
            {
                consumer.Subscribe(topic);
                _subscribed = true;
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
                if (result is null)
                {
                    yield break;
                }

                var envelope = JsonSerializer.Deserialize<MessageEnvelope>(result.Message.Value)!;
                yield return new KafkaTransportMessage(envelope, 1, result);
                await Task.Yield();
            }
        }

        public ValueTask CommitAsync(KafkaTransportMessage message, CancellationToken cancellationToken)
        {
            consumer.Commit((ConsumeResult<string, byte[]>)message.NativeMessage!);
            return ValueTask.CompletedTask;
        }

        public ValueTask RetryAsync(KafkaTransportMessage message, TimeSpan delay, CancellationToken cancellationToken) =>
            ProduceAsync(message.Envelope, message.Envelope.Destination, cancellationToken);

        public ValueTask DeadLetterAsync(KafkaTransportMessage message, string error, CancellationToken cancellationToken) =>
            ProduceAsync(message.Envelope, $"{message.Envelope.Destination}.dead", cancellationToken);

        public void Dispose()
        {
            consumer.Dispose();
            producer.Dispose();
        }
    }

    private sealed class PostgreSqlSdkStoreClient(string connectionString) : IPostgreSqlMessageStoreClient
    {
        public async ValueTask InitializeAsync(PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                create table if not exists "{options.Schema}"."{options.OutboxTable}" (
                    "Id" uuid primary key,
                    "MessageType" text not null,
                    "Destination" text not null,
                    "ContentType" text not null,
                    "Body" bytea not null,
                    "Headers" text not null,
                    "CreatedAt" timestamp with time zone not null,
                    "DeliverAt" timestamp with time zone null,
                    "CorrelationId" uuid null,
                    "CausationId" uuid null,
                    "Attempt" integer not null,
                    "NextAttempt" timestamp with time zone not null,
                    "LockedUntil" timestamp with time zone null,
                    "LastError" text null
                );
                create table if not exists "{options.Schema}"."{options.InboxTable}" (
                    "Id" uuid primary key,
                    "ReceivedAt" timestamp with time zone not null,
                    "CompletedAt" timestamp with time zone null,
                    "LastError" text null
                );
                create table if not exists "{options.Schema}"."{options.DeadLetterTable}" (
                    "Id" uuid primary key,
                    "MessageType" text not null,
                    "Destination" text not null,
                    "ContentType" text not null,
                    "Body" bytea not null,
                    "Headers" text not null,
                    "CreatedAt" timestamp with time zone not null,
                    "DeliverAt" timestamp with time zone null,
                    "CorrelationId" uuid null,
                    "CausationId" uuid null,
                    "Attempt" integer not null,
                    "Error" text not null,
                    "FailedAt" timestamp with time zone not null,
                    "Source" text not null
                );
                truncate table "{options.Schema}"."{options.OutboxTable}", "{options.Schema}"."{options.InboxTable}", "{options.Schema}"."{options.DeadLetterTable}";
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask EnqueueOutboxAsync(
            OutboxMessage message,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                insert into "{options.Schema}"."{options.OutboxTable}"
                    ("Id", "MessageType", "Destination", "ContentType", "Body", "Headers", "CreatedAt", "DeliverAt",
                     "CorrelationId", "CausationId", "Attempt", "NextAttempt", "LockedUntil", "LastError")
                values (@id, @messageType, @destination, @contentType, @body, @headers, @createdAt, @deliverAt,
                        @correlationId, @causationId, @attempt, @nextAttempt, @lockedUntil, @lastError)
                """;
            AddEnvelopeParameters(command, message.Envelope);
            command.Parameters.AddWithValue("@attempt", message.Attempt);
            command.Parameters.AddWithValue("@nextAttempt", message.NextAttempt);
            command.Parameters.AddWithValue("@lockedUntil", (object?)message.LockedUntil ?? DBNull.Value);
            command.Parameters.AddWithValue("@lastError", (object?)message.LastError ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(
            int maxCount,
            DateTimeOffset now,
            TimeSpan lockDuration,
            PostgreSqlMessageStoreOptions options,
            CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
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
                    limit @maxCount
                    for update skip locked
                )
                returning "Id", "MessageType", "Destination", "ContentType", "Body", "Headers", "CreatedAt", "DeliverAt",
                          "CorrelationId", "CausationId", "Attempt", "NextAttempt", "LockedUntil", "LastError";
                """;
            command.Parameters.AddWithValue("@lockedUntil", now + lockDuration);
            command.Parameters.AddWithValue("@now", now);
            command.Parameters.AddWithValue("@maxCount", maxCount);
            return await ReadOutboxAsync(command, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask MarkOutboxDeliveredAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await ExecuteAsync($"""delete from "{options.Schema}"."{options.OutboxTable}" where "Id" = @id""", messageId, cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask RescheduleOutboxAsync(Guid messageId, int attempt, DateTimeOffset nextAttempt, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                update "{options.Schema}"."{options.OutboxTable}"
                set "Attempt" = @attempt, "NextAttempt" = @nextAttempt, "LastError" = @error, "LockedUntil" = null
                where "Id" = @id
                """;
            command.Parameters.AddWithValue("@id", messageId);
            command.Parameters.AddWithValue("@attempt", attempt);
            command.Parameters.AddWithValue("@nextAttempt", nextAttempt);
            command.Parameters.AddWithValue("@error", error);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public ValueTask MoveOutboxToDeadLetterAsync(Guid messageId, int attempt, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) =>
            MarkOutboxDeliveredAsync(messageId, options, cancellationToken);

        public async ValueTask<bool> TryStartInboxAsync(Guid messageId, DateTimeOffset receivedAt, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                insert into "{options.Schema}"."{options.InboxTable}" ("Id", "ReceivedAt")
                values (@id, @receivedAt)
                on conflict ("Id") do nothing
                """;
            command.Parameters.AddWithValue("@id", messageId);
            command.Parameters.AddWithValue("@receivedAt", receivedAt);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }

        public async ValueTask CompleteInboxAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await ExecuteAsync($"""update "{options.Schema}"."{options.InboxTable}" set "CompletedAt" = now() where "Id" = @id""", messageId, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask FailInboxAsync(Guid messageId, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) =>
            ExecuteAsync($"""delete from "{options.Schema}"."{options.InboxTable}" where "Id" = @id""", messageId, cancellationToken);

        public async ValueTask AddDeadLetterAsync(DeadLetterMessage message, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                insert into "{options.Schema}"."{options.DeadLetterTable}"
                    ("Id", "MessageType", "Destination", "ContentType", "Body", "Headers", "CreatedAt", "DeliverAt",
                     "CorrelationId", "CausationId", "Attempt", "Error", "FailedAt", "Source")
                values (@id, @messageType, @destination, @contentType, @body, @headers, @createdAt, @deliverAt,
                        @correlationId, @causationId, @attempt, @error, @failedAt, @source)
                on conflict ("Id") do update set "Error" = excluded."Error", "FailedAt" = excluded."FailedAt"
                """;
            AddEnvelopeParameters(command, message.Envelope);
            command.Parameters.AddWithValue("@attempt", message.Attempt);
            command.Parameters.AddWithValue("@error", message.Error);
            command.Parameters.AddWithValue("@failedAt", message.FailedAt);
            command.Parameters.AddWithValue("@source", message.Source);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public ValueTask EnqueueOutboxBatchAsync(IReadOnlyList<OutboxMessage> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.EnqueueOutboxAsync(item, opts, token), cancellationToken);
        public ValueTask MarkOutboxDeliveredBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messageIds, options, static (client, item, opts, token) => client.MarkOutboxDeliveredAsync(item, opts, token), cancellationToken);
        public ValueTask RescheduleOutboxBatchAsync(IReadOnlyList<OutboxReschedule> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.RescheduleOutboxAsync(item.MessageId, item.Attempt, item.NextAttempt, item.Error, opts, token), cancellationToken);
        public ValueTask MoveOutboxToDeadLetterBatchAsync(IReadOnlyList<OutboxDeadLetter> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.MoveOutboxToDeadLetterAsync(item.MessageId, item.Attempt, item.Error, opts, token), cancellationToken);
        public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(IReadOnlyList<InboxStart> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => TryStartManyAsync(messages, options, cancellationToken);
        public ValueTask CompleteInboxBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messageIds, options, static (client, item, opts, token) => client.CompleteInboxAsync(item, opts, token), cancellationToken);
        public ValueTask FailInboxBatchAsync(IReadOnlyList<InboxFailure> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.FailInboxAsync(item.MessageId, item.Error, opts, token), cancellationToken);
        public ValueTask AddDeadLetterBatchAsync(IReadOnlyList<DeadLetterMessage> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.AddDeadLetterAsync(item, opts, token), cancellationToken);
        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(int maxCount, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>([]);
        public ValueTask RemoveDeadLetterAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteAsync($"""delete from "{options.Schema}"."{options.DeadLetterTable}" where "Id" = @id""", messageId, cancellationToken);
        public ValueTask RemoveDeadLetterBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messageIds, options, static (client, item, opts, token) => client.RemoveDeadLetterAsync(item, opts, token), cancellationToken);

        private async ValueTask ExecuteAsync(string commandText, Guid id, CancellationToken cancellationToken)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Parameters.AddWithValue("@id", id);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        private static void AddEnvelopeParameters(NpgsqlCommand command, MessageEnvelope envelope)
        {
            command.Parameters.AddWithValue("@id", envelope.Id);
            command.Parameters.AddWithValue("@messageType", envelope.MessageType);
            command.Parameters.AddWithValue("@destination", envelope.Destination);
            command.Parameters.AddWithValue("@contentType", envelope.ContentType);
            command.Parameters.AddWithValue("@body", envelope.Body.ToArray());
            command.Parameters.AddWithValue("@headers", JsonSerializer.Serialize(envelope.Headers));
            command.Parameters.AddWithValue("@createdAt", envelope.CreatedAt);
            command.Parameters.AddWithValue("@deliverAt", (object?)envelope.DeliverAt ?? DBNull.Value);
            command.Parameters.AddWithValue("@correlationId", (object?)envelope.CorrelationId ?? DBNull.Value);
            command.Parameters.AddWithValue("@causationId", (object?)envelope.CausationId ?? DBNull.Value);
        }

        private static async ValueTask<IReadOnlyList<OutboxMessage>> ReadOutboxAsync(NpgsqlCommand command, CancellationToken cancellationToken)
        {
            var messages = new List<OutboxMessage>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                messages.Add(ReadOutbox(reader));
            }

            return messages;
        }

        private static OutboxMessage ReadOutbox(NpgsqlDataReader reader)
        {
            var headers = JsonSerializer.Deserialize<IReadOnlyDictionary<string, string>>(reader.GetString(5)) ?? new Dictionary<string, string>();
            var envelope = new MessageEnvelope(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                (byte[])reader[4],
                headers,
                reader.GetFieldValue<DateTimeOffset>(6),
                reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8),
                reader.IsDBNull(9) ? null : reader.GetGuid(9));
            return new OutboxMessage(
                envelope,
                reader.GetInt32(10),
                reader.GetFieldValue<DateTimeOffset>(11),
                reader.IsDBNull(12) ? null : reader.GetFieldValue<DateTimeOffset>(12),
                reader.IsDBNull(13) ? null : reader.GetString(13));
        }

        private async ValueTask ExecuteManyAsync<T>(
            IReadOnlyList<T> items,
            PostgreSqlMessageStoreOptions options,
            Func<PostgreSqlSdkStoreClient, T, PostgreSqlMessageStoreOptions, CancellationToken, ValueTask> action,
            CancellationToken cancellationToken)
        {
            for (var i = 0; i < items.Count; i++)
            {
                await action(this, items[i], options, cancellationToken).ConfigureAwait(false);
            }
        }

        private async ValueTask<IReadOnlyList<Guid>> TryStartManyAsync(IReadOnlyList<InboxStart> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            var started = new List<Guid>();
            for (var i = 0; i < messages.Count; i++)
            {
                if (await TryStartInboxAsync(messages[i].MessageId, messages[i].ReceivedAt, options, cancellationToken).ConfigureAwait(false))
                {
                    started.Add(messages[i].MessageId);
                }
            }

            return started;
        }
    }

    private sealed class SqlServerSdkStoreClient(string connectionString) : ISqlServerMessageStoreClient
    {
        public static async ValueTask EnsureDatabaseAsync(string connectionString, CancellationToken cancellationToken)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            var database = builder.InitialCatalog;
            builder.InitialCatalog = "master";
            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"if db_id(N'{database.Replace("'", "''")}') is null create database [{database.Replace("]", "]]")}]";
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask InitializeAsync(SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                if object_id(N'[{options.Schema}].[{options.OutboxTable}]', N'U') is null
                create table [{options.Schema}].[{options.OutboxTable}] (
                    [Id] uniqueidentifier not null primary key,
                    [MessageType] nvarchar(512) not null,
                    [Destination] nvarchar(256) not null,
                    [ContentType] nvarchar(128) not null,
                    [Body] varbinary(max) not null,
                    [Headers] nvarchar(max) not null,
                    [CreatedAt] datetimeoffset not null,
                    [DeliverAt] datetimeoffset null,
                    [CorrelationId] uniqueidentifier null,
                    [CausationId] uniqueidentifier null,
                    [Attempt] int not null,
                    [NextAttempt] datetimeoffset not null,
                    [LockedUntil] datetimeoffset null,
                    [LastError] nvarchar(max) null
                );
                if object_id(N'[{options.Schema}].[{options.InboxTable}]', N'U') is null
                create table [{options.Schema}].[{options.InboxTable}] (
                    [Id] uniqueidentifier not null primary key,
                    [ReceivedAt] datetimeoffset not null,
                    [CompletedAt] datetimeoffset null,
                    [LastError] nvarchar(max) null
                );
                if object_id(N'[{options.Schema}].[{options.DeadLetterTable}]', N'U') is null
                create table [{options.Schema}].[{options.DeadLetterTable}] (
                    [Id] uniqueidentifier not null primary key,
                    [MessageType] nvarchar(512) not null,
                    [Destination] nvarchar(256) not null,
                    [ContentType] nvarchar(128) not null,
                    [Body] varbinary(max) not null,
                    [Headers] nvarchar(max) not null,
                    [CreatedAt] datetimeoffset not null,
                    [DeliverAt] datetimeoffset null,
                    [CorrelationId] uniqueidentifier null,
                    [CausationId] uniqueidentifier null,
                    [Attempt] int not null,
                    [Error] nvarchar(max) not null,
                    [FailedAt] datetimeoffset not null,
                    [Source] nvarchar(128) not null
                );
                delete from [{options.Schema}].[{options.OutboxTable}];
                delete from [{options.Schema}].[{options.InboxTable}];
                delete from [{options.Schema}].[{options.DeadLetterTable}];
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask EnqueueOutboxAsync(OutboxMessage message, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                insert into [{options.Schema}].[{options.OutboxTable}]
                    ([Id], [MessageType], [Destination], [ContentType], [Body], [Headers], [CreatedAt], [DeliverAt],
                     [CorrelationId], [CausationId], [Attempt], [NextAttempt], [LockedUntil], [LastError])
                values (@id, @messageType, @destination, @contentType, @body, @headers, @createdAt, @deliverAt,
                        @correlationId, @causationId, @attempt, @nextAttempt, @lockedUntil, @lastError)
                """;
            AddEnvelopeParameters(command, message.Envelope);
            command.Parameters.AddWithValue("@attempt", message.Attempt);
            command.Parameters.AddWithValue("@nextAttempt", message.NextAttempt);
            command.Parameters.AddWithValue("@lockedUntil", (object?)message.LockedUntil ?? DBNull.Value);
            command.Parameters.AddWithValue("@lastError", (object?)message.LastError ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public async ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(int maxCount, DateTimeOffset now, TimeSpan lockDuration, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                update top (@maxCount) [{options.Schema}].[{options.OutboxTable}]
                set [LockedUntil] = @lockedUntil
                output inserted.[Id], inserted.[MessageType], inserted.[Destination], inserted.[ContentType], inserted.[Body],
                       inserted.[Headers], inserted.[CreatedAt], inserted.[DeliverAt], inserted.[CorrelationId],
                       inserted.[CausationId], inserted.[Attempt], inserted.[NextAttempt], inserted.[LockedUntil], inserted.[LastError]
                where [NextAttempt] <= @now and ([LockedUntil] is null or [LockedUntil] <= @now)
                """;
            command.Parameters.AddWithValue("@maxCount", maxCount);
            command.Parameters.AddWithValue("@lockedUntil", now + lockDuration);
            command.Parameters.AddWithValue("@now", now);
            return await ReadOutboxAsync(command, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask MarkOutboxDeliveredAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) =>
            ExecuteAsync($"delete from [{options.Schema}].[{options.OutboxTable}] where [Id] = @id", messageId, cancellationToken);

        public async ValueTask RescheduleOutboxAsync(Guid messageId, int attempt, DateTimeOffset nextAttempt, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                update [{options.Schema}].[{options.OutboxTable}]
                set [Attempt] = @attempt, [NextAttempt] = @nextAttempt, [LastError] = @error, [LockedUntil] = null
                where [Id] = @id
                """;
            command.Parameters.AddWithValue("@id", messageId);
            command.Parameters.AddWithValue("@attempt", attempt);
            command.Parameters.AddWithValue("@nextAttempt", nextAttempt);
            command.Parameters.AddWithValue("@error", error);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public ValueTask MoveOutboxToDeadLetterAsync(Guid messageId, int attempt, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) =>
            MarkOutboxDeliveredAsync(messageId, options, cancellationToken);

        public async ValueTask<bool> TryStartInboxAsync(Guid messageId, DateTimeOffset receivedAt, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                if not exists (select 1 from [{options.Schema}].[{options.InboxTable}] where [Id] = @id)
                begin
                    insert into [{options.Schema}].[{options.InboxTable}] ([Id], [ReceivedAt]) values (@id, @receivedAt);
                    select 1;
                end
                else select 0;
                """;
            command.Parameters.AddWithValue("@id", messageId);
            command.Parameters.AddWithValue("@receivedAt", receivedAt);
            return (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0) == 1;
        }

        public ValueTask CompleteInboxAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) =>
            ExecuteAsync($"update [{options.Schema}].[{options.InboxTable}] set [CompletedAt] = sysdatetimeoffset() where [Id] = @id", messageId, cancellationToken);

        public ValueTask FailInboxAsync(Guid messageId, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) =>
            ExecuteAsync($"delete from [{options.Schema}].[{options.InboxTable}] where [Id] = @id", messageId, cancellationToken);

        public async ValueTask AddDeadLetterAsync(DeadLetterMessage message, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"""
                if exists (select 1 from [{options.Schema}].[{options.DeadLetterTable}] where [Id] = @id)
                    update [{options.Schema}].[{options.DeadLetterTable}] set [Error] = @error, [FailedAt] = @failedAt where [Id] = @id
                else
                    insert into [{options.Schema}].[{options.DeadLetterTable}]
                        ([Id], [MessageType], [Destination], [ContentType], [Body], [Headers], [CreatedAt], [DeliverAt],
                         [CorrelationId], [CausationId], [Attempt], [Error], [FailedAt], [Source])
                    values (@id, @messageType, @destination, @contentType, @body, @headers, @createdAt, @deliverAt,
                            @correlationId, @causationId, @attempt, @error, @failedAt, @source)
                """;
            AddEnvelopeParameters(command, message.Envelope);
            command.Parameters.AddWithValue("@attempt", message.Attempt);
            command.Parameters.AddWithValue("@error", message.Error);
            command.Parameters.AddWithValue("@failedAt", message.FailedAt);
            command.Parameters.AddWithValue("@source", message.Source);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public ValueTask EnqueueOutboxBatchAsync(IReadOnlyList<OutboxMessage> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.EnqueueOutboxAsync(item, opts, token), cancellationToken);
        public ValueTask MarkOutboxDeliveredBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messageIds, options, static (client, item, opts, token) => client.MarkOutboxDeliveredAsync(item, opts, token), cancellationToken);
        public ValueTask RescheduleOutboxBatchAsync(IReadOnlyList<OutboxReschedule> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.RescheduleOutboxAsync(item.MessageId, item.Attempt, item.NextAttempt, item.Error, opts, token), cancellationToken);
        public ValueTask MoveOutboxToDeadLetterBatchAsync(IReadOnlyList<OutboxDeadLetter> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.MoveOutboxToDeadLetterAsync(item.MessageId, item.Attempt, item.Error, opts, token), cancellationToken);
        public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(IReadOnlyList<InboxStart> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => TryStartManyAsync(messages, options, cancellationToken);
        public ValueTask CompleteInboxBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messageIds, options, static (client, item, opts, token) => client.CompleteInboxAsync(item, opts, token), cancellationToken);
        public ValueTask FailInboxBatchAsync(IReadOnlyList<InboxFailure> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.FailInboxAsync(item.MessageId, item.Error, opts, token), cancellationToken);
        public ValueTask AddDeadLetterBatchAsync(IReadOnlyList<DeadLetterMessage> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messages, options, static (client, item, opts, token) => client.AddDeadLetterAsync(item, opts, token), cancellationToken);
        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(int maxCount, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>([]);
        public ValueTask RemoveDeadLetterAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteAsync($"delete from [{options.Schema}].[{options.DeadLetterTable}] where [Id] = @id", messageId, cancellationToken);
        public ValueTask RemoveDeadLetterBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ExecuteManyAsync(messageIds, options, static (client, item, opts, token) => client.RemoveDeadLetterAsync(item, opts, token), cancellationToken);

        private async ValueTask ExecuteAsync(string commandText, Guid id, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Parameters.AddWithValue("@id", id);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        private static void AddEnvelopeParameters(SqlCommand command, MessageEnvelope envelope)
        {
            command.Parameters.AddWithValue("@id", envelope.Id);
            command.Parameters.AddWithValue("@messageType", envelope.MessageType);
            command.Parameters.AddWithValue("@destination", envelope.Destination);
            command.Parameters.AddWithValue("@contentType", envelope.ContentType);
            command.Parameters.AddWithValue("@body", envelope.Body.ToArray());
            command.Parameters.AddWithValue("@headers", JsonSerializer.Serialize(envelope.Headers));
            command.Parameters.AddWithValue("@createdAt", envelope.CreatedAt);
            command.Parameters.AddWithValue("@deliverAt", (object?)envelope.DeliverAt ?? DBNull.Value);
            command.Parameters.AddWithValue("@correlationId", (object?)envelope.CorrelationId ?? DBNull.Value);
            command.Parameters.AddWithValue("@causationId", (object?)envelope.CausationId ?? DBNull.Value);
        }

        private static async ValueTask<IReadOnlyList<OutboxMessage>> ReadOutboxAsync(SqlCommand command, CancellationToken cancellationToken)
        {
            var messages = new List<OutboxMessage>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                messages.Add(ReadOutbox(reader));
            }

            return messages;
        }

        private static OutboxMessage ReadOutbox(SqlDataReader reader)
        {
            var headers = JsonSerializer.Deserialize<IReadOnlyDictionary<string, string>>(reader.GetString(5)) ?? new Dictionary<string, string>();
            var envelope = new MessageEnvelope(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                (byte[])reader[4],
                headers,
                reader.GetFieldValue<DateTimeOffset>(6),
                reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8),
                reader.IsDBNull(9) ? null : reader.GetGuid(9));
            return new OutboxMessage(
                envelope,
                reader.GetInt32(10),
                reader.GetFieldValue<DateTimeOffset>(11),
                reader.IsDBNull(12) ? null : reader.GetFieldValue<DateTimeOffset>(12),
                reader.IsDBNull(13) ? null : reader.GetString(13));
        }

        private async ValueTask ExecuteManyAsync<T>(
            IReadOnlyList<T> items,
            SqlServerMessageStoreOptions options,
            Func<SqlServerSdkStoreClient, T, SqlServerMessageStoreOptions, CancellationToken, ValueTask> action,
            CancellationToken cancellationToken)
        {
            for (var i = 0; i < items.Count; i++)
            {
                await action(this, items[i], options, cancellationToken).ConfigureAwait(false);
            }
        }

        private async ValueTask<IReadOnlyList<Guid>> TryStartManyAsync(IReadOnlyList<InboxStart> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            var started = new List<Guid>();
            for (var i = 0; i < messages.Count; i++)
            {
                if (await TryStartInboxAsync(messages[i].MessageId, messages[i].ReceivedAt, options, cancellationToken).ConfigureAwait(false))
                {
                    started.Add(messages[i].MessageId);
                }
            }

            return started;
        }
    }
}
