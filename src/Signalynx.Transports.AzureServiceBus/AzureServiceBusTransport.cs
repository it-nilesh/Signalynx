using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.AzureServiceBus;

public sealed class AzureServiceBusTransportOptions
{
    public string EntityPath { get; set; } = "signalynx";
}

public sealed record AzureServiceBusTransportMessage(
    MessageEnvelope Envelope,
    int Attempt,
    object? NativeMessage = null);

public interface IAzureServiceBusTransportClient
{
    ValueTask SendAsync(
        MessageEnvelope envelope,
        string entityPath,
        CancellationToken cancellationToken);

    IAsyncEnumerable<AzureServiceBusTransportMessage> ReceiveAsync(
        string entityPath,
        CancellationToken cancellationToken);

    ValueTask CompleteAsync(
        AzureServiceBusTransportMessage message,
        CancellationToken cancellationToken);

    ValueTask AbandonAsync(
        AzureServiceBusTransportMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken);

    ValueTask DeadLetterAsync(
        AzureServiceBusTransportMessage message,
        string error,
        CancellationToken cancellationToken);
}

public sealed class AzureServiceBusMessageTransport : IMessageTransport
{
    private readonly IAzureServiceBusTransportClient _client;
    private readonly AzureServiceBusTransportOptions _options;

    public AzureServiceBusMessageTransport(
        IAzureServiceBusTransportClient client,
        AzureServiceBusTransportOptions options)
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
                           .ReceiveAsync(_options.EntityPath, cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return new Delivery(_client, message);
        }
    }

    private sealed class Delivery : TransportDelivery
    {
        private readonly IAzureServiceBusTransportClient _client;
        private readonly AzureServiceBusTransportMessage _message;
        private int _settled;

        public Delivery(
            IAzureServiceBusTransportClient client,
            AzureServiceBusTransportMessage message)
            : base(message.Envelope, message.Attempt)
        {
            _client = client;
            _message = message;
        }

        public override ValueTask CompleteAsync(CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.CompleteAsync(_message, cancellationToken);
        }

        public override ValueTask RetryAsync(
            TimeSpan delay,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _client.AbandonAsync(_message, delay, cancellationToken);
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

public static class AzureServiceBusServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxAzureServiceBusTransport(
        this IServiceCollection services,
        Action<AzureServiceBusTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new AzureServiceBusTransportOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IMessageTransport, AzureServiceBusMessageTransport>();
        return services;
    }
}
