using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Signalynx.Messaging;

namespace Signalynx.Performance;

[MemoryDiagnoser]
[DisassemblyDiagnoser(
    maxDepth: 3,
    printSource: true,
    exportGithubMarkdown: true,
    exportHtml: true,
    exportCombinedDisassemblyReport: true)]
public class MessagingBenchmarks
{
    private ISignalynxMessageBus _bus = null!;
    private IMessageSerializer _serializer = null!;
    private BenchmarkTransportMessage _message = null!;

    [GlobalSetup]
    public void Setup()
    {
        _serializer = new SystemTextJsonMessageSerializer();
        _bus = new SignalynxMessageBus(
            new DiscardingOutboxStore(),
            _serializer,
            TimeProvider.System);
        _message = new BenchmarkTransportMessage(Guid.NewGuid(), "benchmark");
    }

    [Benchmark(Baseline = true)]
    public byte[] SerializeOnly() =>
        _serializer.Serialize(_message);

    [Benchmark]
    public ValueTask<Guid> SerializeAndEnqueue() =>
        _bus.EnqueueAsync(_message, "benchmarks");

    public sealed record BenchmarkTransportMessage(Guid Id, string Value);

    private sealed class DiscardingOutboxStore : IOutboxStore
    {
        public ValueTask EnqueueAsync(
            OutboxMessage message,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<IReadOnlyList<OutboxMessage>> LockDueAsync(
            int maxCount,
            DateTimeOffset now,
            TimeSpan lockDuration,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<IReadOnlyList<OutboxMessage>>([]);

        public ValueTask MarkDeliveredAsync(
            Guid messageId,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask RescheduleAsync(
            Guid messageId,
            int attempt,
            DateTimeOffset nextAttempt,
            string error,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask MoveToDeadLetterAsync(
            Guid messageId,
            int attempt,
            string error,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }
}

[MemoryDiagnoser]
public class EndToEndMessagingLoadBenchmarks
{
    private SystemTextJsonMessageSerializer _serializer = null!;
    private InMemoryDurableStore _store = null!;
    private LoopbackTransport _transport = null!;
    private BenchmarkHandler _handler = null!;
    private SignalynxMessageBus _bus = null!;
    private ExponentialBackoffRetryPolicy _retryPolicy = null!;
    private BenchmarkTransportMessage _message = null!;

    [Params(1, 100)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var options = new SignalynxMessagingOptions
        {
            MaxDeliveryAttempts = 2,
            BaseRetryDelay = TimeSpan.Zero
        };
        _serializer = new SystemTextJsonMessageSerializer();
        _store = new InMemoryDurableStore();
        _transport = new LoopbackTransport();
        _handler = new BenchmarkHandler();
        _bus = new SignalynxMessageBus(_store, _serializer, TimeProvider.System);
        _retryPolicy = new ExponentialBackoffRetryPolicy(options);
        _message = new BenchmarkTransportMessage(Guid.NewGuid(), "benchmark");
    }

    [Benchmark]
    public async ValueTask<int> TransportOutboxInboxRetryAndHandlerExecution()
    {
        var handled = 0;
        for (var i = 0; i < MessageCount; i++)
        {
            await _bus.EnqueueAsync(_message, "benchmarks").ConfigureAwait(false);
            handled += await DrainOutboxAndReceiveAsync().ConfigureAwait(false);
        }

        return handled;
    }

    private async ValueTask<int> DrainOutboxAndReceiveAsync()
    {
        var now = DateTimeOffset.UtcNow;
        var messages = await _store.LockDueAsync(
            32,
            now,
            TimeSpan.FromSeconds(30),
            CancellationToken.None).ConfigureAwait(false);

        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            try
            {
                await _transport.SendAsync(message.Envelope, CancellationToken.None).ConfigureAwait(false);
                await _store.MarkDeliveredAsync(message.Envelope.Id, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                var attempt = message.Attempt + 1;
                if (_retryPolicy.ShouldRetry(attempt, exception, out var delay))
                {
                    await _store.RescheduleAsync(
                        message.Envelope.Id,
                        attempt,
                        now + delay,
                        exception.Message,
                        CancellationToken.None).ConfigureAwait(false);
                    continue;
                }

                await _store.AddAsync(
                    new DeadLetterMessage(message.Envelope, attempt, exception.ToString(), now, "outbox"),
                    CancellationToken.None).ConfigureAwait(false);
                await _store.MoveToDeadLetterAsync(
                    message.Envelope.Id,
                    attempt,
                    exception.ToString(),
                    CancellationToken.None).ConfigureAwait(false);
            }
        }

        var handled = 0;
        await foreach (var delivery in _transport.ReceiveAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (!await _store.TryStartAsync(delivery.Envelope.Id, now, CancellationToken.None).ConfigureAwait(false))
            {
                await delivery.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            try
            {
                var deserialized = (BenchmarkTransportMessage)_serializer.Deserialize(
                    delivery.Envelope.Body,
                    typeof(BenchmarkTransportMessage));
                await _handler.HandleAsync(
                    deserialized,
                    new MessageContext(delivery.Envelope, delivery.Attempt, CancellationToken.None)).ConfigureAwait(false);
                await _store.CompleteAsync(delivery.Envelope.Id, CancellationToken.None).ConfigureAwait(false);
                await delivery.CompleteAsync(CancellationToken.None).ConfigureAwait(false);
                handled++;
            }
            catch (Exception exception)
            {
                await _store.FailAsync(delivery.Envelope.Id, exception.ToString(), CancellationToken.None).ConfigureAwait(false);
                if (_retryPolicy.ShouldRetry(delivery.Attempt, exception, out var delay))
                {
                    await delivery.RetryAsync(delay, CancellationToken.None).ConfigureAwait(false);
                    continue;
                }

                await _store.AddAsync(
                    new DeadLetterMessage(delivery.Envelope, delivery.Attempt, exception.ToString(), now, "receiver"),
                    CancellationToken.None).ConfigureAwait(false);
                await delivery.DeadLetterAsync(exception.ToString(), CancellationToken.None).ConfigureAwait(false);
            }
        }

        return handled;
    }

    public sealed record BenchmarkTransportMessage(Guid Id, string Value);

    private sealed class BenchmarkHandler : IMessageHandler<BenchmarkTransportMessage>
    {
        public ValueTask HandleAsync(
            BenchmarkTransportMessage message,
            MessageContext context) =>
            ValueTask.CompletedTask;
    }

    private sealed class LoopbackTransport : IMessageTransport
    {
        private readonly Queue<TransportDelivery> _deliveries = new();

        public ValueTask SendAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
        {
            _deliveries.Enqueue(new Delivery(envelope, 1));
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<TransportDelivery> ReceiveAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            while (_deliveries.Count > 0)
            {
                yield return _deliveries.Dequeue();
            }

            await Task.CompletedTask;
        }

        private sealed class Delivery(MessageEnvelope envelope, int attempt) :
            TransportDelivery(envelope, attempt)
        {
            public override ValueTask CompleteAsync(CancellationToken cancellationToken = default) =>
                ValueTask.CompletedTask;

            public override ValueTask RetryAsync(TimeSpan delay, CancellationToken cancellationToken = default) =>
                ValueTask.CompletedTask;

            public override ValueTask DeadLetterAsync(string error, CancellationToken cancellationToken = default) =>
                ValueTask.CompletedTask;
        }
    }

    private sealed class InMemoryDurableStore :
        IOutboxStore,
        IInboxStore,
        IDeadLetterStore
    {
        private readonly Dictionary<Guid, OutboxMessage> _outbox = [];
        private readonly HashSet<Guid> _inbox = [];
        private readonly List<DeadLetterMessage> _deadLetters = [];

        public ValueTask EnqueueAsync(
            OutboxMessage message,
            CancellationToken cancellationToken)
        {
            _outbox.Add(message.Envelope.Id, message);
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<OutboxMessage>> LockDueAsync(
            int maxCount,
            DateTimeOffset now,
            TimeSpan lockDuration,
            CancellationToken cancellationToken)
        {
            var due = _outbox.Values
                .Where(message => message.NextAttempt <= now)
                .Take(maxCount)
                .Select(message => message with { LockedUntil = now + lockDuration })
                .ToArray();
            for (var i = 0; i < due.Length; i++)
            {
                _outbox[due[i].Envelope.Id] = due[i];
            }

            return ValueTask.FromResult<IReadOnlyList<OutboxMessage>>(due);
        }

        public ValueTask MarkDeliveredAsync(Guid messageId, CancellationToken cancellationToken)
        {
            _outbox.Remove(messageId);
            return ValueTask.CompletedTask;
        }

        public ValueTask RescheduleAsync(
            Guid messageId,
            int attempt,
            DateTimeOffset nextAttempt,
            string error,
            CancellationToken cancellationToken)
        {
            if (_outbox.TryGetValue(messageId, out var message))
            {
                _outbox[messageId] = message with
                {
                    Attempt = attempt,
                    NextAttempt = nextAttempt,
                    LastError = error,
                    LockedUntil = null
                };
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask MoveToDeadLetterAsync(
            Guid messageId,
            int attempt,
            string error,
            CancellationToken cancellationToken)
        {
            _outbox.Remove(messageId);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> TryStartAsync(
            Guid messageId,
            DateTimeOffset receivedAt,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(_inbox.Add(messageId));

        public ValueTask CompleteAsync(Guid messageId, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask FailAsync(Guid messageId, string error, CancellationToken cancellationToken)
        {
            _inbox.Remove(messageId);
            return ValueTask.CompletedTask;
        }

        public ValueTask AddAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            _deadLetters.Add(message);
            return ValueTask.CompletedTask;
        }

        public ValueTask<IReadOnlyList<DeadLetterMessage>> GetAsync(
            int maxCount,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<IReadOnlyList<DeadLetterMessage>>(_deadLetters.Take(maxCount).ToArray());

        public ValueTask RemoveAsync(Guid messageId, CancellationToken cancellationToken)
        {
            _deadLetters.RemoveAll(message => message.Envelope.Id == messageId);
            return ValueTask.CompletedTask;
        }
    }
}
