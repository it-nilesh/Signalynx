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
