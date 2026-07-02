using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.RabbitMQ;

public sealed class RabbitMqTransportOptions
{
    public string QueueName { get; set; } = "signalynx";
}

public sealed record RabbitMqTransportMessage(
    MessageEnvelope Envelope,
    int Attempt,
    object? NativeMessage = null);

public interface IRabbitMqTransportClient
{
    ValueTask PublishAsync(
        MessageEnvelope envelope,
        string routingKey,
        CancellationToken cancellationToken);

    IAsyncEnumerable<RabbitMqTransportMessage> ReceiveAsync(
        string queueName,
        CancellationToken cancellationToken);

    ValueTask AcknowledgeAsync(
        RabbitMqTransportMessage message,
        CancellationToken cancellationToken);

    ValueTask RequeueAsync(
        RabbitMqTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken);

    ValueTask DeadLetterAsync(
        RabbitMqTransportMessage message,
        string error,
        CancellationToken cancellationToken);
}

public sealed class RabbitMqMessageTransport : IMessageTransport
{
    private readonly IRabbitMqTransportClient _client;
    private readonly RabbitMqTransportOptions _options;

    public RabbitMqMessageTransport(
        IRabbitMqTransportClient client,
        RabbitMqTransportOptions options)
    {
        _client = client;
        _options = options;
    }

    public ValueTask SendAsync(
        MessageEnvelope envelope,
        CancellationToken cancellationToken) =>
        _client.PublishAsync(envelope, envelope.Destination, cancellationToken);

    public async IAsyncEnumerable<TransportDelivery> ReceiveAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var message in _client
                           .ReceiveAsync(_options.QueueName, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return new Delivery(_client, message);
        }
    }

    private sealed class Delivery : TransportDelivery
    {
        private readonly IRabbitMqTransportClient _client;
        private readonly RabbitMqTransportMessage _message;
        private int _settled;

        public Delivery(
            IRabbitMqTransportClient client,
            RabbitMqTransportMessage message)
            : base(message.Envelope, message.Attempt)
        {
            _client = client;
            _message = message;
        }

        public override ValueTask CompleteAsync(CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.AcknowledgeAsync(_message, cancellationToken);
        }

        public override ValueTask RetryAsync(
            TimeSpan delay,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.RequeueAsync(_message, delay, cancellationToken);
        }

        public override ValueTask DeadLetterAsync(
            string error,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.DeadLetterAsync(_message, error, cancellationToken);
        }

        private void Settle()
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                throw new InvalidOperationException("The transport delivery is already settled.");
            }
        }
    }
}

public static class RabbitMqServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxRabbitMqTransport(
        this IServiceCollection services,
        Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new RabbitMqTransportOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IMessageTransport, RabbitMqMessageTransport>();
        return services;
    }
}
