using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.AmazonSqs;

public sealed class AmazonSqsTransportOptions
{
    public string QueueUrl { get; set; } = "signalynx";
}

public sealed record AmazonSqsTransportMessage(
    MessageEnvelope Envelope,
    int Attempt,
    string ReceiptHandle,
    object? NativeMessage = null);

public interface IAmazonSqsTransportClient
{
    ValueTask SendAsync(
        MessageEnvelope envelope,
        string queueUrl,
        CancellationToken cancellationToken);

    IAsyncEnumerable<AmazonSqsTransportMessage> ReceiveAsync(
        string queueUrl,
        CancellationToken cancellationToken);

    ValueTask DeleteAsync(
        AmazonSqsTransportMessage message,
        CancellationToken cancellationToken);

    ValueTask ChangeVisibilityAsync(
        AmazonSqsTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken);

    ValueTask MoveToDeadLetterAsync(
        AmazonSqsTransportMessage message,
        string error,
        CancellationToken cancellationToken);
}

public sealed class AmazonSqsMessageTransport : IMessageTransport
{
    private readonly IAmazonSqsTransportClient _client;
    private readonly AmazonSqsTransportOptions _options;

    public AmazonSqsMessageTransport(
        IAmazonSqsTransportClient client,
        AmazonSqsTransportOptions options)
    {
        _client = client;
        _options = options;
    }

    public ValueTask SendAsync(
        MessageEnvelope envelope,
        CancellationToken cancellationToken) =>
        _client.SendAsync(envelope, envelope.Destination, cancellationToken);

    public async IAsyncEnumerable<TransportDelivery> ReceiveAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var message in _client
                           .ReceiveAsync(_options.QueueUrl, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return new Delivery(_client, message);
        }
    }

    private sealed class Delivery : TransportDelivery
    {
        private readonly IAmazonSqsTransportClient _client;
        private readonly AmazonSqsTransportMessage _message;
        private int _settled;

        public Delivery(
            IAmazonSqsTransportClient client,
            AmazonSqsTransportMessage message)
            : base(message.Envelope, message.Attempt)
        {
            _client = client;
            _message = message;
        }

        public override ValueTask CompleteAsync(CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.DeleteAsync(_message, cancellationToken);
        }

        public override ValueTask RetryAsync(
            TimeSpan delay,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.ChangeVisibilityAsync(_message, delay, cancellationToken);
        }

        public override ValueTask DeadLetterAsync(
            string error,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.MoveToDeadLetterAsync(_message, error, cancellationToken);
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

public static class AmazonSqsServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxAmazonSqsTransport(
        this IServiceCollection services,
        Action<AmazonSqsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new AmazonSqsTransportOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IMessageTransport, AmazonSqsMessageTransport>();
        return services;
    }
}
