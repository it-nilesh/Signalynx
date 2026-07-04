using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Data.SqlClient;
using Npgsql;
using Signalynx.Messaging;

namespace Signalynx.Performance;

[MemoryDiagnoser]
public class KafkaTransportLoadBenchmarks
{
    private const string OptInVariable = "SIGNALYNX_PROVIDER_LOAD_BENCHMARK";

    private IProducer<string, byte[]> _producer = null!;
    private IConsumer<string, byte[]> _consumer = null!;
    private string _topic = null!;
    private (string Key, byte[] Payload)[] _messages = null!;
    private int _iteration;

    [Params(1000, 10000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        EnsureOptIn("Kafka");

        var bootstrapServers = Environment.GetEnvironmentVariable("SIGNALYNX_KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
        _topic = $"signalynx.kafka.transport.load.{Guid.NewGuid():N}";
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync(
                [new TopicSpecification { Name = _topic, NumPartitions = 1, ReplicationFactor = 1 }])
                .ConfigureAwait(false);
        }

        _producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Acks = Acks.Leader,
                LingerMs = 5,
                BatchNumMessages = 10000,
                QueueBufferingMaxMessages = 1000000
            }).Build();
        _consumer = new ConsumerBuilder<string, byte[]>(
            new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"signalynx-kafka-load-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            }).Build();
        _consumer.Subscribe(_topic);

        _messages = new (string Key, byte[] Payload)[MessageCount];
        for (var i = 0; i < _messages.Length; i++)
        {
            var envelope = new MessageEnvelope(
                Guid.NewGuid(),
                typeof(KafkaTransportMessage).AssemblyQualifiedName!,
                _topic,
                "application/json",
                JsonSerializer.SerializeToUtf8Bytes(new KafkaTransportMessage(i, "kafka-load")),
                new Dictionary<string, string>(),
                DateTimeOffset.UtcNow,
                null);
            _messages[i] = (envelope.Id.ToString("N"), JsonSerializer.SerializeToUtf8Bytes(envelope));
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _consumer?.Dispose();
        _producer?.Dispose();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _iteration++;
    }

    [Benchmark]
    public async ValueTask<int> ProduceAndConsume()
    {
        var iteration = _iteration.ToString();
        var delivered = 0;
        var deliveryCompletion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        for (var i = 0; i < _messages.Length; i++)
        {
            _producer.Produce(
                _topic,
                new Message<string, byte[]>
                {
                    Key = _messages[i].Key,
                    Value = _messages[i].Payload,
                    Headers = [new Header("iteration", System.Text.Encoding.UTF8.GetBytes(iteration))]
                },
                report =>
                {
                    if (report.Error.IsError)
                    {
                        deliveryCompletion.TrySetException(new KafkaException(report.Error));
                        return;
                    }

                    var current = Interlocked.Increment(ref delivered);
                    if (current == _messages.Length)
                    {
                        deliveryCompletion.TrySetResult(current);
                    }
                });
        }

        _producer.Flush(TimeSpan.FromSeconds(30));
        await deliveryCompletion.Task.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);

        var consumed = 0;
        while (consumed < _messages.Length)
        {
            var result = _consumer.Consume(TimeSpan.FromSeconds(10))
                ?? throw new TimeoutException("Kafka did not return a message within 10 seconds.");
            var header = result.Message.Headers.TryGetLastBytes("iteration", out var bytes)
                ? System.Text.Encoding.UTF8.GetString(bytes)
                : string.Empty;
            if (!string.Equals(header, iteration, StringComparison.Ordinal))
            {
                continue;
            }

            consumed++;
        }

        return consumed;
    }

    private sealed record KafkaTransportMessage(int Index, string Value);

    private static void EnsureOptIn(string provider)
    {
        if (!string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "1", StringComparison.Ordinal) &&
            !string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "true", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Set {OptInVariable}=1 before running {provider} load benchmarks.");
        }
    }
}

[MemoryDiagnoser]
public class PostgreSqlPrimitiveLoadBenchmarks
{
    private const string OptInVariable = "SIGNALYNX_PROVIDER_LOAD_BENCHMARK";

    private string _connectionString = null!;
    private PostgreSqlRow[] _rows = null!;

    [Params(1000, 10000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        EnsureOptIn("PostgreSQL");

        _connectionString = Environment.GetEnvironmentVariable("SIGNALYNX_POSTGRESQL_CONNECTION_STRING")
            ?? "Host=localhost;Port=5432;Database=signalynx;Username=signalynx;Password=signalynx";
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            create table if not exists signalynx_postgres_load (
                id uuid primary key,
                payload text not null
            );
            truncate table signalynx_postgres_load;
            """;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);

        _rows = Enumerable
            .Range(0, MessageCount)
            .Select(index => new PostgreSqlRow(Guid.NewGuid(), $"postgres-load-{index}"))
            .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        using var connection = new NpgsqlConnection(_connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "truncate table signalynx_postgres_load;";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public async ValueTask<int> InsertAndRead()
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using (var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false))
        {
            for (var i = 0; i < _rows.Length; i++)
            {
                await using var insert = connection.CreateCommand();
                insert.Transaction = transaction;
                insert.CommandText = "insert into signalynx_postgres_load (id, payload) values (@id, @payload);";
                insert.Parameters.AddWithValue("@id", _rows[i].Id);
                insert.Parameters.AddWithValue("@payload", _rows[i].Payload);
                await insert.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await transaction.CommitAsync().ConfigureAwait(false);
        }

        var read = 0;
        await using (var select = connection.CreateCommand())
        {
            select.CommandText = "select payload from signalynx_postgres_load;";
            await using var reader = await select.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                _ = reader.GetString(0);
                read++;
            }
        }

        return read;
    }

    private sealed record PostgreSqlRow(Guid Id, string Payload);

    private static void EnsureOptIn(string provider)
    {
        if (!string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "1", StringComparison.Ordinal) &&
            !string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "true", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Set {OptInVariable}=1 before running {provider} load benchmarks.");
        }
    }
}

[MemoryDiagnoser]
public class SqlServerPrimitiveLoadBenchmarks
{
    private const string OptInVariable = "SIGNALYNX_PROVIDER_LOAD_BENCHMARK";

    private string _connectionString = null!;
    private SqlServerRow[] _rows = null!;

    [Params(1000, 10000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        EnsureOptIn("SQL Server");

        _connectionString = Environment.GetEnvironmentVariable("SIGNALYNX_SQLSERVER_CONNECTION_STRING")
            ?? "Server=localhost,1433;Database=signalynx;User Id=sa;Password=Signalynx!2026;TrustServerCertificate=True;Encrypt=True";
        await EnsureDatabaseAsync(_connectionString).ConfigureAwait(false);
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            if object_id(N'dbo.SignalynxSqlServerLoad', N'U') is null
                create table dbo.SignalynxSqlServerLoad (
                    id uniqueidentifier not null primary key,
                    payload nvarchar(128) not null
                );
            delete from dbo.SignalynxSqlServerLoad;
            """;
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);

        _rows = Enumerable
            .Range(0, MessageCount)
            .Select(index => new SqlServerRow(Guid.NewGuid(), $"sqlserver-load-{index}"))
            .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "delete from dbo.SignalynxSqlServerLoad;";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public async ValueTask<int> InsertAndRead()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using (var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false))
        {
            for (var i = 0; i < _rows.Length; i++)
            {
                await using var insert = connection.CreateCommand();
                insert.Transaction = (SqlTransaction)transaction;
                insert.CommandText = "insert into dbo.SignalynxSqlServerLoad (id, payload) values (@id, @payload);";
                insert.Parameters.AddWithValue("@id", _rows[i].Id);
                insert.Parameters.AddWithValue("@payload", _rows[i].Payload);
                await insert.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await transaction.CommitAsync().ConfigureAwait(false);
        }

        var read = 0;
        await using (var select = connection.CreateCommand())
        {
            select.CommandText = "select payload from dbo.SignalynxSqlServerLoad;";
            await using var reader = await select.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                _ = reader.GetString(0);
                read++;
            }
        }

        return read;
    }

    private static async ValueTask EnsureDatabaseAsync(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        var database = builder.InitialCatalog;
        builder.InitialCatalog = "master";
        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"if db_id(N'{database.Replace("'", "''")}') is null create database [{database.Replace("]", "]]")}]";
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private sealed record SqlServerRow(Guid Id, string Payload);

    private static void EnsureOptIn(string provider)
    {
        if (!string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "1", StringComparison.Ordinal) &&
            !string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "true", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Set {OptInVariable}=1 before running {provider} load benchmarks.");
        }
    }
}
