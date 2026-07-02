using System.Threading.Channels;
using Signalynx.Messaging;
using Signalynx.Messaging.AmazonSqs;
using Signalynx.Messaging.AzureServiceBus;
using Signalynx.Messaging.InMemory;
using Signalynx.Messaging.Kafka;
using Signalynx.Messaging.RabbitMQ;

namespace Signalynx.Samples.Api;

public static class MessagingTransportExamples
{
    public static IServiceCollection AddSampleMessagingTransport(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var provider = configuration["Signalynx:Transport:Provider"] ?? "in-memory";
        switch (provider.Trim().ToLowerInvariant())
        {
            case "in-memory":
                services.AddSignalynxInMemoryTransport();
                break;

            case "rabbitmq":
                services.AddSingleton<SampleBrokerTransportClient>();
                services.AddSingleton<IRabbitMqTransportClient>(
                    static serviceProvider => serviceProvider.GetRequiredService<SampleBrokerTransportClient>());
                services.AddSignalynxRabbitMqTransport(options =>
                {
                    options.QueueName = configuration["Signalynx:Transport:RabbitMQ:QueueName"] ?? "orders";
                });
                break;

            case "azure-service-bus":
                services.AddSingleton<SampleBrokerTransportClient>();
                services.AddSingleton<IAzureServiceBusTransportClient>(
                    static serviceProvider => serviceProvider.GetRequiredService<SampleBrokerTransportClient>());
                services.AddSignalynxAzureServiceBusTransport(options =>
                {
                    options.EntityPath = configuration["Signalynx:Transport:AzureServiceBus:EntityPath"] ?? "orders";
                });
                break;

            case "amazon-sqs":
                services.AddSingleton<SampleBrokerTransportClient>();
                services.AddSingleton<IAmazonSqsTransportClient>(
                    static serviceProvider => serviceProvider.GetRequiredService<SampleBrokerTransportClient>());
                services.AddSignalynxAmazonSqsTransport(options =>
                {
                    options.QueueUrl = configuration["Signalynx:Transport:AmazonSqs:QueueUrl"] ?? "orders";
                });
                break;

            case "kafka":
                services.AddSingleton<SampleBrokerTransportClient>();
                services.AddSingleton<IKafkaTransportClient>(
                    static serviceProvider => serviceProvider.GetRequiredService<SampleBrokerTransportClient>());
                services.AddSignalynxKafkaTransport(options =>
                {
                    options.Topic = configuration["Signalynx:Transport:Kafka:Topic"] ?? "orders";
                });
                break;

            default:
                throw new InvalidOperationException(
                    $"Unknown Signalynx transport provider '{provider}'.");
        }

        return services;
    }
}

public sealed class SampleBrokerTransportClient :
    IRabbitMqTransportClient,
    IAzureServiceBusTransportClient,
    IAmazonSqsTransportClient,
    IKafkaTransportClient
{
    private readonly Channel<QueuedEnvelope> _messages =
        Channel.CreateUnbounded<QueuedEnvelope>();

    ValueTask IRabbitMqTransportClient.PublishAsync(
        MessageEnvelope envelope,
        string routingKey,
        CancellationToken cancellationToken) =>
        EnqueueAsync(envelope, cancellationToken);

    ValueTask IAzureServiceBusTransportClient.SendAsync(
        MessageEnvelope envelope,
        string entityPath,
        CancellationToken cancellationToken) =>
        EnqueueAsync(envelope, cancellationToken);

    ValueTask IAmazonSqsTransportClient.SendAsync(
        MessageEnvelope envelope,
        string queueUrl,
        CancellationToken cancellationToken) =>
        EnqueueAsync(envelope, cancellationToken);

    ValueTask IKafkaTransportClient.ProduceAsync(
        MessageEnvelope envelope,
        string topic,
        CancellationToken cancellationToken) =>
        EnqueueAsync(envelope, cancellationToken);

    async IAsyncEnumerable<RabbitMqTransportMessage> IRabbitMqTransportClient.ReceiveAsync(
        string queueName,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var item in ReadAllAsync(cancellationToken))
        {
            yield return new RabbitMqTransportMessage(item.Envelope, item.Attempt);
        }
    }

    async IAsyncEnumerable<AzureServiceBusTransportMessage> IAzureServiceBusTransportClient.ReceiveAsync(
        string entityPath,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var item in ReadAllAsync(cancellationToken))
        {
            yield return new AzureServiceBusTransportMessage(item.Envelope, item.Attempt);
        }
    }

    async IAsyncEnumerable<AmazonSqsTransportMessage> IAmazonSqsTransportClient.ReceiveAsync(
        string queueUrl,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var item in ReadAllAsync(cancellationToken))
        {
            yield return new AmazonSqsTransportMessage(item.Envelope, item.Attempt, item.Envelope.Id.ToString());
        }
    }

    async IAsyncEnumerable<KafkaTransportMessage> IKafkaTransportClient.ConsumeAsync(
        string topic,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var item in ReadAllAsync(cancellationToken))
        {
            yield return new KafkaTransportMessage(item.Envelope, item.Attempt);
        }
    }

    ValueTask IRabbitMqTransportClient.AcknowledgeAsync(
        RabbitMqTransportMessage message,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IAzureServiceBusTransportClient.CompleteAsync(
        AzureServiceBusTransportMessage message,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IAmazonSqsTransportClient.DeleteAsync(
        AmazonSqsTransportMessage message,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IKafkaTransportClient.CommitAsync(
        KafkaTransportMessage message,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IRabbitMqTransportClient.RequeueAsync(
        RabbitMqTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken) =>
        RetryAsync(message.Envelope, message.Attempt, delay, cancellationToken);

    ValueTask IAzureServiceBusTransportClient.AbandonAsync(
        AzureServiceBusTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken) =>
        RetryAsync(message.Envelope, message.Attempt, delay, cancellationToken);

    ValueTask IAmazonSqsTransportClient.ChangeVisibilityAsync(
        AmazonSqsTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken) =>
        RetryAsync(message.Envelope, message.Attempt, delay, cancellationToken);

    ValueTask IKafkaTransportClient.RetryAsync(
        KafkaTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken) =>
        RetryAsync(message.Envelope, message.Attempt, delay, cancellationToken);

    ValueTask IRabbitMqTransportClient.DeadLetterAsync(
        RabbitMqTransportMessage message,
        string error,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IAzureServiceBusTransportClient.DeadLetterAsync(
        AzureServiceBusTransportMessage message,
        string error,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IAmazonSqsTransportClient.MoveToDeadLetterAsync(
        AmazonSqsTransportMessage message,
        string error,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    ValueTask IKafkaTransportClient.DeadLetterAsync(
        KafkaTransportMessage message,
        string error,
        CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    private ValueTask EnqueueAsync(
        MessageEnvelope envelope,
        CancellationToken cancellationToken) =>
        _messages.Writer.WriteAsync(new QueuedEnvelope(envelope, 1), cancellationToken);

    private async ValueTask RetryAsync(
        MessageEnvelope envelope,
        int attempt,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }

        await _messages.Writer
            .WriteAsync(new QueuedEnvelope(envelope, attempt + 1), cancellationToken)
            .ConfigureAwait(false);
    }

    private IAsyncEnumerable<QueuedEnvelope> ReadAllAsync(
        CancellationToken cancellationToken) =>
        _messages.Reader.ReadAllAsync(cancellationToken);

    private sealed record QueuedEnvelope(MessageEnvelope Envelope, int Attempt);
}
