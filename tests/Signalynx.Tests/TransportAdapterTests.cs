using Microsoft.Extensions.DependencyInjection;
using Signalynx.Messaging;
using Signalynx.Messaging.AmazonSqs;
using Signalynx.Messaging.AzureServiceBus;
using Signalynx.Messaging.Kafka;
using Signalynx.Messaging.RabbitMQ;

namespace Signalynx.Tests;

public sealed class TransportAdapterTests
{
    [Fact]
    public async Task RabbitMq_transport_delegates_send_and_settlement()
    {
        var envelope = Envelope("rabbit-orders");
        var client = new FakeRabbitMqClient(new RabbitMqTransportMessage(envelope, 2));
        var transport = new RabbitMqMessageTransport(
            client,
            new RabbitMqTransportOptions { QueueName = "rabbit-consume" });

        await transport.SendAsync(envelope, CancellationToken.None);
        var delivery = await ReadOneAsync(transport.ReceiveAsync(CancellationToken.None));
        await delivery.CompleteAsync();

        Assert.Equal("rabbit-orders", client.SentDestination);
        Assert.Equal("rabbit-consume", client.ReceiveSource);
        Assert.Equal("complete", client.Settlement);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await delivery.CompleteAsync());
    }

    [Fact]
    public async Task RabbitMq_transport_requeues_and_dead_letters()
    {
        var envelope = Envelope("rabbit-orders");
        var retryClient = new FakeRabbitMqClient(new RabbitMqTransportMessage(envelope, 1));
        var retryTransport = new RabbitMqMessageTransport(retryClient, new RabbitMqTransportOptions());
        var retryDelivery = await ReadOneAsync(retryTransport.ReceiveAsync(CancellationToken.None));

        await retryDelivery.RetryAsync(TimeSpan.FromSeconds(3));

        var deadLetterClient = new FakeRabbitMqClient(new RabbitMqTransportMessage(envelope, 1));
        var deadLetterTransport = new RabbitMqMessageTransport(deadLetterClient, new RabbitMqTransportOptions());
        var deadLetterDelivery = await ReadOneAsync(deadLetterTransport.ReceiveAsync(CancellationToken.None));

        await deadLetterDelivery.DeadLetterAsync("boom");

        Assert.Equal("retry", retryClient.Settlement);
        Assert.Equal(TimeSpan.FromSeconds(3), retryClient.RetryDelay);
        Assert.Equal("dead-letter", deadLetterClient.Settlement);
        Assert.Equal("boom", deadLetterClient.Error);
    }

    [Fact]
    public async Task Azure_Service_Bus_transport_delegates_send_and_settlement()
    {
        var envelope = Envelope("asb-orders");
        var client = new FakeAzureServiceBusClient(new AzureServiceBusTransportMessage(envelope, 3));
        var transport = new AzureServiceBusMessageTransport(
            client,
            new AzureServiceBusTransportOptions { EntityPath = "asb-consume" });

        await transport.SendAsync(envelope, CancellationToken.None);
        var delivery = await ReadOneAsync(transport.ReceiveAsync(CancellationToken.None));
        await delivery.RetryAsync(TimeSpan.FromSeconds(7));

        Assert.Equal("asb-orders", client.SentDestination);
        Assert.Equal("asb-consume", client.ReceiveSource);
        Assert.Equal("retry", client.Settlement);
        Assert.Equal(TimeSpan.FromSeconds(7), client.RetryDelay);
    }

    [Fact]
    public async Task Azure_Service_Bus_transport_dead_letters()
    {
        var envelope = Envelope("asb-orders");
        var client = new FakeAzureServiceBusClient(new AzureServiceBusTransportMessage(envelope, 1));
        var transport = new AzureServiceBusMessageTransport(client, new AzureServiceBusTransportOptions());
        var delivery = await ReadOneAsync(transport.ReceiveAsync(CancellationToken.None));

        await delivery.DeadLetterAsync("poison");

        Assert.Equal("dead-letter", client.Settlement);
        Assert.Equal("poison", client.Error);
    }

    [Fact]
    public async Task Amazon_SQS_transport_delegates_send_and_settlement()
    {
        var envelope = Envelope("sqs-orders");
        var client = new FakeAmazonSqsClient(new AmazonSqsTransportMessage(envelope, 4, "receipt"));
        var transport = new AmazonSqsMessageTransport(
            client,
            new AmazonSqsTransportOptions { QueueUrl = "sqs-consume" });

        await transport.SendAsync(envelope, CancellationToken.None);
        var delivery = await ReadOneAsync(transport.ReceiveAsync(CancellationToken.None));
        await delivery.CompleteAsync();

        Assert.Equal("sqs-orders", client.SentDestination);
        Assert.Equal("sqs-consume", client.ReceiveSource);
        Assert.Equal("complete", client.Settlement);
    }

    [Fact]
    public async Task Amazon_SQS_transport_retries_and_dead_letters()
    {
        var envelope = Envelope("sqs-orders");
        var retryClient = new FakeAmazonSqsClient(new AmazonSqsTransportMessage(envelope, 1, "receipt"));
        var retryTransport = new AmazonSqsMessageTransport(retryClient, new AmazonSqsTransportOptions());
        var retryDelivery = await ReadOneAsync(retryTransport.ReceiveAsync(CancellationToken.None));

        await retryDelivery.RetryAsync(TimeSpan.FromSeconds(9));

        var deadLetterClient = new FakeAmazonSqsClient(new AmazonSqsTransportMessage(envelope, 1, "receipt"));
        var deadLetterTransport = new AmazonSqsMessageTransport(deadLetterClient, new AmazonSqsTransportOptions());
        var deadLetterDelivery = await ReadOneAsync(deadLetterTransport.ReceiveAsync(CancellationToken.None));

        await deadLetterDelivery.DeadLetterAsync("bad");

        Assert.Equal("retry", retryClient.Settlement);
        Assert.Equal(TimeSpan.FromSeconds(9), retryClient.RetryDelay);
        Assert.Equal("dead-letter", deadLetterClient.Settlement);
        Assert.Equal("bad", deadLetterClient.Error);
    }

    [Fact]
    public async Task Kafka_transport_delegates_send_and_settlement()
    {
        var envelope = Envelope("kafka-orders");
        var client = new FakeKafkaClient(new KafkaTransportMessage(envelope, 5));
        var transport = new KafkaMessageTransport(
            client,
            new KafkaTransportOptions { Topic = "kafka-consume" });

        await transport.SendAsync(envelope, CancellationToken.None);
        var delivery = await ReadOneAsync(transport.ReceiveAsync(CancellationToken.None));
        await delivery.CompleteAsync();

        Assert.Equal("kafka-orders", client.SentDestination);
        Assert.Equal("kafka-consume", client.ReceiveSource);
        Assert.Equal("complete", client.Settlement);
    }

    [Fact]
    public async Task Kafka_transport_retries_and_dead_letters()
    {
        var envelope = Envelope("kafka-orders");
        var retryClient = new FakeKafkaClient(new KafkaTransportMessage(envelope, 1));
        var retryTransport = new KafkaMessageTransport(retryClient, new KafkaTransportOptions());
        var retryDelivery = await ReadOneAsync(retryTransport.ReceiveAsync(CancellationToken.None));

        await retryDelivery.RetryAsync(TimeSpan.FromSeconds(5));

        var deadLetterClient = new FakeKafkaClient(new KafkaTransportMessage(envelope, 1));
        var deadLetterTransport = new KafkaMessageTransport(deadLetterClient, new KafkaTransportOptions());
        var deadLetterDelivery = await ReadOneAsync(deadLetterTransport.ReceiveAsync(CancellationToken.None));

        await deadLetterDelivery.DeadLetterAsync("invalid");

        Assert.Equal("retry", retryClient.Settlement);
        Assert.Equal(TimeSpan.FromSeconds(5), retryClient.RetryDelay);
        Assert.Equal("dead-letter", deadLetterClient.Settlement);
        Assert.Equal("invalid", deadLetterClient.Error);
    }

    [Fact]
    public void Registers_transport_adapters_with_DI()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IRabbitMqTransportClient>(
            new FakeRabbitMqClient(new RabbitMqTransportMessage(Envelope("rabbit"), 1)));
        services.AddSignalynxRabbitMqTransport(options => options.QueueName = "rabbit");

        using var provider = services.BuildServiceProvider();

        Assert.IsType<RabbitMqMessageTransport>(provider.GetRequiredService<IMessageTransport>());
        Assert.Equal("rabbit", provider.GetRequiredService<RabbitMqTransportOptions>().QueueName);
    }

    private static MessageEnvelope Envelope(string destination) =>
        new(
            Guid.NewGuid(),
            typeof(AdapterMessage).AssemblyQualifiedName!,
            destination,
            "application/json",
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, string>(),
            DateTimeOffset.UtcNow,
            null);

    private static async Task<TransportDelivery> ReadOneAsync(
        IAsyncEnumerable<TransportDelivery> deliveries)
    {
        await foreach (var delivery in deliveries)
        {
            return delivery;
        }

        throw new InvalidOperationException("No delivery was available.");
    }

    private sealed record AdapterMessage;

    private sealed class FakeRabbitMqClient(RabbitMqTransportMessage message)
        : IRabbitMqTransportClient
    {
        public string? SentDestination { get; private set; }

        public string? ReceiveSource { get; private set; }

        public string? Settlement { get; private set; }

        public TimeSpan RetryDelay { get; private set; }

        public string? Error { get; private set; }

        public ValueTask PublishAsync(
            MessageEnvelope envelope,
            string routingKey,
            CancellationToken cancellationToken)
        {
            SentDestination = routingKey;
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<RabbitMqTransportMessage> ReceiveAsync(
            string queueName,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            ReceiveSource = queueName;
            yield return message;
            await Task.CompletedTask;
        }

        public ValueTask AcknowledgeAsync(
            RabbitMqTransportMessage message,
            CancellationToken cancellationToken)
        {
            Settlement = "complete";
            return ValueTask.CompletedTask;
        }

        public ValueTask RequeueAsync(
            RabbitMqTransportMessage message,
            TimeSpan delay,
            CancellationToken cancellationToken)
        {
            Settlement = "retry";
            RetryDelay = delay;
            return ValueTask.CompletedTask;
        }

        public ValueTask DeadLetterAsync(
            RabbitMqTransportMessage message,
            string error,
            CancellationToken cancellationToken)
        {
            Settlement = "dead-letter";
            Error = error;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeAzureServiceBusClient(AzureServiceBusTransportMessage message)
        : IAzureServiceBusTransportClient
    {
        public string? SentDestination { get; private set; }

        public string? ReceiveSource { get; private set; }

        public string? Settlement { get; private set; }

        public TimeSpan RetryDelay { get; private set; }

        public string? Error { get; private set; }

        public ValueTask SendAsync(
            MessageEnvelope envelope,
            string entityPath,
            CancellationToken cancellationToken)
        {
            SentDestination = entityPath;
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<AzureServiceBusTransportMessage> ReceiveAsync(
            string entityPath,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            ReceiveSource = entityPath;
            yield return message;
            await Task.CompletedTask;
        }

        public ValueTask CompleteAsync(
            AzureServiceBusTransportMessage message,
            CancellationToken cancellationToken)
        {
            Settlement = "complete";
            return ValueTask.CompletedTask;
        }

        public ValueTask AbandonAsync(
            AzureServiceBusTransportMessage message,
            TimeSpan delay,
            CancellationToken cancellationToken)
        {
            Settlement = "retry";
            RetryDelay = delay;
            return ValueTask.CompletedTask;
        }

        public ValueTask DeadLetterAsync(
            AzureServiceBusTransportMessage message,
            string error,
            CancellationToken cancellationToken)
        {
            Settlement = "dead-letter";
            Error = error;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeAmazonSqsClient(AmazonSqsTransportMessage message)
        : IAmazonSqsTransportClient
    {
        public string? SentDestination { get; private set; }

        public string? ReceiveSource { get; private set; }

        public string? Settlement { get; private set; }

        public TimeSpan RetryDelay { get; private set; }

        public string? Error { get; private set; }

        public ValueTask SendAsync(
            MessageEnvelope envelope,
            string queueUrl,
            CancellationToken cancellationToken)
        {
            SentDestination = queueUrl;
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<AmazonSqsTransportMessage> ReceiveAsync(
            string queueUrl,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            ReceiveSource = queueUrl;
            yield return message;
            await Task.CompletedTask;
        }

        public ValueTask DeleteAsync(
            AmazonSqsTransportMessage message,
            CancellationToken cancellationToken)
        {
            Settlement = "complete";
            return ValueTask.CompletedTask;
        }

        public ValueTask ChangeVisibilityAsync(
            AmazonSqsTransportMessage message,
            TimeSpan delay,
            CancellationToken cancellationToken)
        {
            Settlement = "retry";
            RetryDelay = delay;
            return ValueTask.CompletedTask;
        }

        public ValueTask MoveToDeadLetterAsync(
            AmazonSqsTransportMessage message,
            string error,
            CancellationToken cancellationToken)
        {
            Settlement = "dead-letter";
            Error = error;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FakeKafkaClient(KafkaTransportMessage message)
        : IKafkaTransportClient
    {
        public string? SentDestination { get; private set; }

        public string? ReceiveSource { get; private set; }

        public string? Settlement { get; private set; }

        public TimeSpan RetryDelay { get; private set; }

        public string? Error { get; private set; }

        public ValueTask ProduceAsync(
            MessageEnvelope envelope,
            string topic,
            CancellationToken cancellationToken)
        {
            SentDestination = topic;
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<KafkaTransportMessage> ConsumeAsync(
            string topic,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            ReceiveSource = topic;
            yield return message;
            await Task.CompletedTask;
        }

        public ValueTask CommitAsync(
            KafkaTransportMessage message,
            CancellationToken cancellationToken)
        {
            Settlement = "complete";
            return ValueTask.CompletedTask;
        }

        public ValueTask RetryAsync(
            KafkaTransportMessage message,
            TimeSpan delay,
            CancellationToken cancellationToken)
        {
            Settlement = "retry";
            RetryDelay = delay;
            return ValueTask.CompletedTask;
        }

        public ValueTask DeadLetterAsync(
            KafkaTransportMessage message,
            string error,
            CancellationToken cancellationToken)
        {
            Settlement = "dead-letter";
            Error = error;
            return ValueTask.CompletedTask;
        }
    }
}
