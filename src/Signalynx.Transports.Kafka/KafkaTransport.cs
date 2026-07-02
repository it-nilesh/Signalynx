using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.Kafka;

public sealed class KafkaTransportOptions
{
    public string Topic { get; set; } = "signalynx";
}

public sealed record KafkaTransportMessage(
    MessageEnvelope Envelope,
    int Attempt,
    object? NativeMessage = null);

public interface IKafkaTransportClient
{
    ValueTask ProduceAsync(
        MessageEnvelope envelope,
        string topic,
        CancellationToken cancellationToken);

    IAsyncEnumerable<KafkaTransportMessage> ConsumeAsync(
        string topic,
        CancellationToken cancellationToken);

    ValueTask CommitAsync(
        KafkaTransportMessage message,
        CancellationToken cancellationToken);

    ValueTask RetryAsync(
        KafkaTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken);

    ValueTask DeadLetterAsync(
        KafkaTransportMessage message,
        string error,
        CancellationToken cancellationToken);
}

public sealed class KafkaMessageTransport : IMessageTransport
{
    private readonly IKafkaTransportClient _client;
    private readonly KafkaTransportOptions _options;

    public KafkaMessageTransport(
        IKafkaTransportClient client,
        KafkaTransportOptions options)
    {
        _client = client;
        _options = options;
    }

    public ValueTask SendAsync(
        MessageEnvelope envelope,
        CancellationToken cancellationToken) =>
        _client.ProduceAsync(envelope, envelope.Destination, cancellationToken);

    public async IAsyncEnumerable<TransportDelivery> ReceiveAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var message in _client
                           .ConsumeAsync(_options.Topic, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return new Delivery(_client, message);
        }
    }

    private sealed class Delivery : TransportDelivery
    {
        private readonly IKafkaTransportClient _client;
        private readonly KafkaTransportMessage _message;
        private int _settled;

        public Delivery(
            IKafkaTransportClient client,
            KafkaTransportMessage message)
            : base(message.Envelope, message.Attempt)
        {
            _client = client;
            _message = message;
        }

        public override ValueTask CompleteAsync(CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.CommitAsync(_message, cancellationToken);
        }

        public override ValueTask RetryAsync(
            TimeSpan delay,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.RetryAsync(_message, delay, cancellationToken);
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

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxKafkaTransport(
        this IServiceCollection services,
        Action<KafkaTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new KafkaTransportOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IMessageTransport, KafkaMessageTransport>();
        return services;
    }
}
