using System.Diagnostics;
using Signalynx.Messaging;
using Signalynx.Messaging.Kafka;
using Signalynx.Messaging.PostgreSql;
using Signalynx.Messaging.RabbitMQ;
using Signalynx.Messaging.SqlServer;

namespace Signalynx.Tests;

public sealed class DockerProviderIntegrationTests
{
    private const string EnabledVariable = "SIGNALYNX_DOCKER_INTEGRATION";

    [Fact]
    public async Task Docker_compose_starts_provider_dependencies()
    {
        if (!DockerIntegrationEnabled())
        {
            return;
        }

        await DockerAsync("compose", "-f", "docker-compose.integration.yml", "up", "-d", "--wait");

        try
        {
            await DockerAsync("compose", "-f", "docker-compose.integration.yml", "ps");
            await DockerAsync("compose", "-f", "docker-compose.integration.yml", "exec", "-T", "rabbitmq", "rabbitmq-diagnostics", "-q", "ping");
            await DockerAsync("compose", "-f", "docker-compose.integration.yml", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list");
            await DockerAsync("compose", "-f", "docker-compose.integration.yml", "exec", "-T", "postgres", "pg_isready", "-U", "signalynx", "-d", "signalynx");
        }
        finally
        {
            await DockerAsync("compose", "-f", "docker-compose.integration.yml", "down", "-v");
        }
    }

    [Fact]
    public async Task Provider_adapters_round_trip_through_contract_clients()
    {
        var envelope = Envelope("docker-contracts");
        var rabbitClient = new ContractRabbitMqClient(envelope);
        var rabbit = new RabbitMqMessageTransport(
            rabbitClient,
            new RabbitMqTransportOptions { QueueName = "signalynx.integration" });

        await rabbit.SendAsync(envelope, CancellationToken.None);
        var rabbitDelivery = await FirstAsync(rabbit.ReceiveAsync(CancellationToken.None));
        await rabbitDelivery.CompleteAsync(CancellationToken.None);

        var kafkaClient = new ContractKafkaClient(envelope);
        var kafka = new KafkaMessageTransport(
            kafkaClient,
            new KafkaTransportOptions { Topic = "signalynx.integration" });

        await kafka.SendAsync(envelope, CancellationToken.None);
        var kafkaDelivery = await FirstAsync(kafka.ReceiveAsync(CancellationToken.None));
        await kafkaDelivery.RetryAsync(TimeSpan.FromMilliseconds(10), CancellationToken.None);

        var sqlClient = new ContractSqlServerClient();
        var sqlStore = new SqlServerMessageStore(sqlClient, new SqlServerMessageStoreOptions());
        await sqlStore.EnqueueAsync(new OutboxMessage(envelope, 0, DateTimeOffset.UtcNow), CancellationToken.None);

        var postgresClient = new ContractPostgreSqlClient();
        var postgresStore = new PostgreSqlMessageStore(postgresClient, new PostgreSqlMessageStoreOptions());
        await postgresStore.AddAsync(new DeadLetterMessage(envelope, 1, "failed", DateTimeOffset.UtcNow, "test"), CancellationToken.None);

        Assert.Equal(envelope.Destination, rabbitClient.PublishedRoutingKey);
        Assert.True(rabbitClient.Acknowledged);
        Assert.Equal(envelope.Destination, kafkaClient.ProducedTopic);
        Assert.True(kafkaClient.Retried);
        Assert.Equal(1, sqlClient.Enqueued);
        Assert.Equal(1, postgresClient.DeadLetters);
    }

    private static bool DockerIntegrationEnabled() =>
        string.Equals(Environment.GetEnvironmentVariable(EnabledVariable), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable(EnabledVariable), "true", StringComparison.OrdinalIgnoreCase);

    private static async Task DockerAsync(params string[] arguments)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = "docker",
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            WorkingDirectory = RepositoryRoot()
        };
        for (var i = 0; i < arguments.Length; i++)
        {
            startInfo.ArgumentList.Add(arguments[i]);
        }

        using var process = Process.Start(startInfo);
        Assert.NotNull(process);
        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        Assert.True(
            process.ExitCode == 0,
            $"docker {string.Join(' ', arguments)} failed with exit code {process.ExitCode}.{Environment.NewLine}{output}{Environment.NewLine}{error}");
    }

    private static async ValueTask<TransportDelivery> FirstAsync(IAsyncEnumerable<TransportDelivery> deliveries)
    {
        await foreach (var delivery in deliveries)
        {
            return delivery;
        }

        throw new InvalidOperationException("The transport did not produce a delivery.");
    }

    private static string RepositoryRoot()
    {
        var directory = AppContext.BaseDirectory;
        while (!File.Exists(Path.Combine(directory, "Signalynx.slnx")))
        {
            directory = Directory.GetParent(directory)?.FullName
                ?? throw new DirectoryNotFoundException("Could not locate the repository root.");
        }

        return directory;
    }

    private static MessageEnvelope Envelope(string destination) =>
        new(
            Guid.NewGuid(),
            typeof(DockerProviderIntegrationTests).AssemblyQualifiedName!,
            destination,
            "application/json",
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, string>(),
            DateTimeOffset.UtcNow,
            null);

    private sealed class ContractRabbitMqClient(MessageEnvelope envelope) : IRabbitMqTransportClient
    {
        public string? PublishedRoutingKey { get; private set; }

        public bool Acknowledged { get; private set; }

        public ValueTask PublishAsync(MessageEnvelope envelope, string routingKey, CancellationToken cancellationToken)
        {
            PublishedRoutingKey = routingKey;
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<RabbitMqTransportMessage> ReceiveAsync(string queueName, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return new RabbitMqTransportMessage(envelope, 1, queueName);
            await Task.CompletedTask;
        }

        public ValueTask AcknowledgeAsync(RabbitMqTransportMessage message, CancellationToken cancellationToken)
        {
            Acknowledged = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask RequeueAsync(RabbitMqTransportMessage message, TimeSpan delay, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DeadLetterAsync(RabbitMqTransportMessage message, string error, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class ContractKafkaClient(MessageEnvelope envelope) : IKafkaTransportClient
    {
        public string? ProducedTopic { get; private set; }

        public bool Retried { get; private set; }

        public ValueTask ProduceAsync(MessageEnvelope envelope, string topic, CancellationToken cancellationToken)
        {
            ProducedTopic = topic;
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<KafkaTransportMessage> ConsumeAsync(string topic, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return new KafkaTransportMessage(envelope, 1, topic);
            await Task.CompletedTask;
        }

        public ValueTask CommitAsync(KafkaTransportMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask RetryAsync(KafkaTransportMessage message, TimeSpan delay, CancellationToken cancellationToken)
        {
            Retried = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask DeadLetterAsync(KafkaTransportMessage message, string error, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class ContractSqlServerClient : ISqlServerMessageStoreClient
    {
        public int Enqueued { get; private set; }

        public ValueTask EnqueueOutboxAsync(OutboxMessage message, SqlServerMessageStoreOptions options, CancellationToken cancellationToken)
        {
            Enqueued++;
            return ValueTask.CompletedTask;
        }

        public ValueTask EnqueueOutboxBatchAsync(IReadOnlyList<OutboxMessage> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(int maxCount, DateTimeOffset now, TimeSpan lockDuration, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<OutboxMessage>>([]);
        public ValueTask MarkOutboxDeliveredAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask MarkOutboxDeliveredBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask RescheduleOutboxAsync(Guid messageId, int attempt, DateTimeOffset nextAttempt, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask RescheduleOutboxBatchAsync(IReadOnlyList<OutboxReschedule> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask MoveOutboxToDeadLetterAsync(Guid messageId, int attempt, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask MoveOutboxToDeadLetterBatchAsync(IReadOnlyList<OutboxDeadLetter> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<bool> TryStartInboxAsync(Guid messageId, DateTimeOffset receivedAt, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult(true);
        public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(IReadOnlyList<InboxStart> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<Guid>>([]);
        public ValueTask CompleteInboxAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask CompleteInboxBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask FailInboxAsync(Guid messageId, string error, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask FailInboxBatchAsync(IReadOnlyList<InboxFailure> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask AddDeadLetterAsync(DeadLetterMessage message, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask AddDeadLetterBatchAsync(IReadOnlyList<DeadLetterMessage> messages, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(int maxCount, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>([]);
        public ValueTask RemoveDeadLetterAsync(Guid messageId, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask RemoveDeadLetterBatchAsync(IReadOnlyList<Guid> messageIds, SqlServerMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class ContractPostgreSqlClient : IPostgreSqlMessageStoreClient
    {
        public int DeadLetters { get; private set; }

        public ValueTask AddDeadLetterAsync(DeadLetterMessage message, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken)
        {
            DeadLetters++;
            return ValueTask.CompletedTask;
        }

        public ValueTask EnqueueOutboxAsync(OutboxMessage message, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask EnqueueOutboxBatchAsync(IReadOnlyList<OutboxMessage> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<OutboxMessage>> LockDueOutboxAsync(int maxCount, DateTimeOffset now, TimeSpan lockDuration, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<OutboxMessage>>([]);
        public ValueTask MarkOutboxDeliveredAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask MarkOutboxDeliveredBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask RescheduleOutboxAsync(Guid messageId, int attempt, DateTimeOffset nextAttempt, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask RescheduleOutboxBatchAsync(IReadOnlyList<OutboxReschedule> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask MoveOutboxToDeadLetterAsync(Guid messageId, int attempt, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask MoveOutboxToDeadLetterBatchAsync(IReadOnlyList<OutboxDeadLetter> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<bool> TryStartInboxAsync(Guid messageId, DateTimeOffset receivedAt, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult(true);
        public ValueTask<IReadOnlyList<Guid>> TryStartInboxBatchAsync(IReadOnlyList<InboxStart> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<Guid>>([]);
        public ValueTask CompleteInboxAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask CompleteInboxBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask FailInboxAsync(Guid messageId, string error, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask FailInboxBatchAsync(IReadOnlyList<InboxFailure> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask AddDeadLetterBatchAsync(IReadOnlyList<DeadLetterMessage> messages, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetDeadLettersAsync(int maxCount, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>([]);
        public ValueTask RemoveDeadLetterAsync(Guid messageId, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask RemoveDeadLetterBatchAsync(IReadOnlyList<Guid> messageIds, PostgreSqlMessageStoreOptions options, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
