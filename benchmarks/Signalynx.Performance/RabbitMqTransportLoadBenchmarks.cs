using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Signalynx.Messaging;

namespace Signalynx.Performance;

[MemoryDiagnoser]
public class RabbitMqTransportLoadBenchmarks
{
    private const string OptInVariable = "SIGNALYNX_PROVIDER_LOAD_BENCHMARK";
    private const string QueueName = "signalynx.rabbitmq.transport.load";

    private IConnection _connection = null!;
    private IChannel _channel = null!;
    private byte[][] _payloads = null!;

    [Params(1000, 10000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        if (!ProviderLoadBenchmarkEnabled())
        {
            throw new InvalidOperationException(
                $"Set {OptInVariable}=1 before running RabbitMQ transport load benchmarks.");
        }

        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("SIGNALYNX_RABBITMQ_HOST") ?? "localhost",
            Port = int.Parse(Environment.GetEnvironmentVariable("SIGNALYNX_RABBITMQ_PORT") ?? "5672")
        };
        _connection = await factory.CreateConnectionAsync().ConfigureAwait(false);
        _channel = await _connection.CreateChannelAsync().ConfigureAwait(false);
        await _channel.QueueDeclareAsync(
            QueueName,
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null).ConfigureAwait(false);
        await _channel.QueuePurgeAsync(QueueName).ConfigureAwait(false);

        _payloads = new byte[MessageCount][];
        for (var i = 0; i < _payloads.Length; i++)
        {
            var envelope = new MessageEnvelope(
                Guid.NewGuid(),
                typeof(RabbitMqTransportMessage).AssemblyQualifiedName!,
                QueueName,
                "application/json",
                JsonSerializer.SerializeToUtf8Bytes(new RabbitMqTransportMessage(i, "rabbitmq-load")),
                new Dictionary<string, string>(),
                DateTimeOffset.UtcNow,
                null);
            _payloads[i] = JsonSerializer.SerializeToUtf8Bytes(envelope);
        }
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_channel is not null)
        {
            await _channel.QueuePurgeAsync(QueueName).ConfigureAwait(false);
            await _channel.DisposeAsync().ConfigureAwait(false);
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _channel.QueuePurgeAsync(QueueName).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async ValueTask<int> PublishAndConsume()
    {
        var consumed = 0;
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += (_, _) =>
        {
            var current = Interlocked.Increment(ref consumed);
            if (current == _payloads.Length)
            {
                completion.TrySetResult(current);
            }

            return Task.CompletedTask;
        };

        var consumerTag = await _channel.BasicConsumeAsync(
            QueueName,
            autoAck: true,
            consumer).ConfigureAwait(false);

        try
        {
            for (var i = 0; i < _payloads.Length; i++)
            {
                await _channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: QueueName,
                    body: _payloads[i]).ConfigureAwait(false);
            }

            return await completion.Task.WaitAsync(TimeSpan.FromSeconds(120)).ConfigureAwait(false);
        }
        finally
        {
            await _channel.BasicCancelAsync(consumerTag).ConfigureAwait(false);
        }
    }

    private static bool ProviderLoadBenchmarkEnabled() =>
        string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "true", StringComparison.OrdinalIgnoreCase);

    private sealed record RabbitMqTransportMessage(int Index, string Value);
}
