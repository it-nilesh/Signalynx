using System.Threading.Channels;

namespace Signalynx.Messaging.InMemory;

public sealed class InMemoryMessageTransport : IMessageTransport
{
    private readonly Channel<DeliveryItem> _channel;
    private readonly TimeProvider _timeProvider;

    public InMemoryMessageTransport(
        InMemoryMessagingOptions options,
        TimeProvider timeProvider)
    {
        if (options.Capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.Capacity),
                "Capacity must be positive.");
        }

        _timeProvider = timeProvider;
        _channel = Channel.CreateBounded<DeliveryItem>(
            new BoundedChannelOptions(options.Capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
    }

    public ValueTask SendAsync(
        MessageEnvelope envelope,
        CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(new DeliveryItem(envelope, 1), cancellationToken);

    public async IAsyncEnumerable<TransportDelivery> ReceiveAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        await foreach (var item in _channel.Reader
                           .ReadAllAsync(cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return new Delivery(this, item);
        }
    }

    private async ValueTask RetryAsync(
        DeliveryItem item,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
        }

        await _channel.Writer.WriteAsync(
            item with { Attempt = item.Attempt + 1 },
            cancellationToken).ConfigureAwait(false);
    }

    private sealed class Delivery : TransportDelivery
    {
        private readonly InMemoryMessageTransport _owner;
        private readonly DeliveryItem _item;
        private int _settled;

        public Delivery(InMemoryMessageTransport owner, DeliveryItem item)
            : base(item.Envelope, item.Attempt)
        {
            _owner = owner;
            _item = item;
        }

        public override ValueTask CompleteAsync(CancellationToken cancellationToken = default)
        {
            Settle();
            return ValueTask.CompletedTask;
        }

        public override ValueTask RetryAsync(
            TimeSpan delay,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return _owner.RetryAsync(_item, delay, cancellationToken);
        }

        public override ValueTask DeadLetterAsync(
            string error,
            CancellationToken cancellationToken = default)
        {
            Settle();
            return ValueTask.CompletedTask;
        }

        private void Settle()
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                throw new InvalidOperationException("The transport delivery is already settled.");
            }
        }
    }

    private sealed record DeliveryItem(MessageEnvelope Envelope, int Attempt);
}
