# Signalynx

Signalynx is a high-performance mediator, dispatcher, and lightweight in-process messaging abstraction for .NET 8 and .NET 9. It supports CQRS commands, queries, request/response messages, notifications, domain events, pipeline behaviors, and bulk processing through a small, strongly typed API.

Signalynx is built from scratch. It does not depend on ASP.NET Core or an external messaging framework.

## Why another mediator?

Mediator overhead should not dominate inexpensive handlers. Signalynx keeps dispatch typed, uses `ValueTask`, discovers handlers once during startup, avoids `MethodInfo.Invoke` on the hot path, and keeps optional integrations in separate packages.

The performance goal is low dispatch overhead—not a claim that real business logic can process millions of records in a millisecond. Handler code, I/O, persistence, and serialization remain the primary real-world costs.

## Packages

| Package | Purpose |
| --- | --- |
| `Signalynx.Abstractions` | Messages, handlers, pipelines, and mediator contracts |
| `Signalynx.Core` | In-process mediator, publishers, registry, and bulk processor |
| `Signalynx.DependencyInjection` | Assembly scanning and Microsoft DI registration |
| `Signalynx.Validation` | Optional FluentValidation behaviors |
| `Signalynx.Logging` | Optional Microsoft.Extensions.Logging behaviors |
| `Signalynx.SourceGeneration` | Optional compile-time handler registration |
| `Signalynx.Messaging` | Durable messaging contracts, workers, retries, inbox/outbox, and operations |
| `Signalynx.Transports.InMemory` | Development/test transport and non-persistent stores |
| `Signalynx.Stores.SqlServer` | SQL Server durable inbox, outbox, and dead-letter stores |
| `Signalynx.Stores.PostgreSql` | PostgreSQL durable inbox, outbox, and dead-letter stores |

## Installation

When packages are published:

```bash
dotnet add package Signalynx.DependencyInjection
dotnet add package Signalynx.Logging
dotnet add package Signalynx.Validation
dotnet add package Signalynx.Messaging
dotnet add package Signalynx.Stores.SqlServer
dotnet add package Signalynx.Stores.PostgreSql
```

For local development, reference the projects in `src/`.

For production, keep API composition, application handlers, integration
contracts, and infrastructure adapters in separate projects:

```text
src/
  Orders.Api/              HTTP endpoints and composition root
  Orders.Application/      Commands, queries, handlers, validators
  Orders.Contracts/        Stable integration-message contracts
  Orders.Infrastructure/   Database, transport, inbox/outbox providers
```

Use in-process dispatch when the caller needs an immediate result. Use durable
messaging when work may be delayed, retried, processed by another service, or
completed after the original request ends.

## Quick start

Define a command and handler:

```csharp
public sealed record CreateOrderCommand(Guid CustomerId, decimal Amount)
    : ICommand<Guid>;

public sealed class CreateOrderHandler
    : ICommandHandler<CreateOrderCommand, Guid>
{
    public ValueTask<Guid> HandleAsync(
        CreateOrderCommand command,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(Guid.NewGuid());
}
```

Register Signalynx:

```csharp
builder.Services.AddSignalynx(options =>
{
    options.RegisterServicesFromAssembly(typeof(Program).Assembly);
    options.AddOpenBehavior(typeof(LoggingBehavior<,>));
    options.NotificationPublishStrategy = SignalynxPublishStrategy.Sequential;
});
```

Dispatch:

```csharp
var id = await signalynx.DispatchAsync<CreateOrderCommand, Guid>(
    command,
    cancellationToken);
```

## Messages and APIs

Use `ICommand` or `ICommand<TResult>` for state-changing operations, `IQuery<TResult>` for reads, and `IRequest<TResult>` for general request/response interactions.

Signalynx is async-only. APIs are `DispatchAsync`, `QueryAsync`, and `RequestAsync`; handlers expose `HandleAsync` and return `ValueTask` or `ValueTask<TResult>`.

Notifications and domain events allow multiple handlers:

```csharp
await signalynx.PublishAsync(new OrderCreated(orderId), cancellationToken);
await signalynx.PublishEventAsync(new OrderConfirmed(orderId), cancellationToken);
```

`Sequential` publishing executes registrations in order and stops at the first exception. `Parallel` starts handlers concurrently and aggregates failures.

## Pipeline behaviors

Implement `IPipelineBehavior<TRequest, TResult>`. Behaviors execute in registration order and can add validation, logging, authorization, metrics, or transactions.

```csharp
options.AddOpenBehavior(typeof(LoggingBehavior<,>));
```

Validation is opt-in:

```csharp
options.AddOpenBehavior(typeof(ValidationBehavior<,>));
```

Register your `IValidator<T>` implementations with the DI container. If no validators exist, the behavior immediately calls the next stage.

## Bulk processing

`ISignalynxBulkProcessor` is deliberately separate from mediator dispatch. It supports sequential and parallel asynchronous processing, batching, cancellation, maximum concurrency, and stop/continue/collect exception strategies.

```csharp
await bulk.ProcessParallelAsync(
    orders,
    static (order, token) => PersistAsync(order, token),
    maxDegreeOfParallelism: 8,
    cancellationToken);
```

Do not automatically dispatch every element of a million-item loop through the mediator. Benchmark the complete workload and use bulk APIs when per-item mediator semantics add no value.

## Durable messaging

`Signalynx.Messaging` adds asynchronous message delivery without coupling the
mediator hot path to a broker or database. It includes:

- message envelopes with headers, correlation, causation, destination, and scheduling;
- outbox and inbox persistence contracts;
- transport send, receive, acknowledgement, retry, and dead-letter contracts;
- hosted outbox and receiver workers;
- exponential-backoff retries;
- duplicate-delivery protection through the inbox;
- dead-letter browsing and replay through `IMessageOperations`;
- `System.Diagnostics.Metrics` counters under `Signalynx.Messaging`.

Define a message handler:

```csharp
public sealed record OrderSubmitted(Guid OrderId);

public sealed class OrderSubmittedHandler : IMessageHandler<OrderSubmitted>
{
    public ValueTask HandleAsync(OrderSubmitted message, MessageContext context)
    {
        // Application work. Use context.MessageId for idempotency/auditing.
        return ValueTask.CompletedTask;
    }
}
```

Configure the development transport:

```csharp
services.AddSignalynxInMemoryTransport();
services.AddSignalynxMessaging(options =>
{
    options.RegisterMessage<OrderSubmitted>();
    options.MaxDeliveryAttempts = 5;
});
services.AddSignalynxMessageHandler<OrderSubmitted, OrderSubmittedHandler>();
```

Enqueue or schedule:

```csharp
var id = await bus.EnqueueAsync(
    new OrderSubmitted(orderId),
    destination: "orders",
    cancellationToken: cancellationToken);

await bus.ScheduleAsync(
    new OrderSubmitted(orderId),
    DateTimeOffset.UtcNow.AddMinutes(5),
    destination: "orders",
    cancellationToken: cancellationToken);
```

The in-memory provider is intentionally for development and tests. It loses
messages when the process exits and is not a production durability guarantee.
Production deployments must provide persistent implementations of
`IOutboxStore`, `IInboxStore`, and `IDeadLetterStore`, plus an
`IMessageTransport` adapter for the chosen broker.

A true transactional outbox also requires the application data update and
outbox insert to participate in the same database transaction. The interfaces
support that architecture, but transaction enlistment belongs in each database
provider.

Signalynx ships durable store adapters for SQL Server and PostgreSQL. Each
store implements `IOutboxStore`, `IInboxStore`, and `IDeadLetterStore` over a
database-specific client abstraction so applications can bind raw ADO.NET,
Dapper, EF Core, or a transaction-enlisted implementation.

Example SQL Server registration:

```csharp
builder.Services.AddSingleton<ISqlServerMessageStoreClient, SqlServerStoreClient>();
builder.Services.AddSignalynxSqlServerStores(options =>
{
    options.Schema = "messaging";
    options.OutboxTable = "outbox";
    options.InboxTable = "inbox";
    options.DeadLetterTable = "dead_letters";
});
```

PostgreSQL uses the same shape through `IPostgreSqlMessageStoreClient` and
`AddSignalynxPostgreSqlStores`.

For high-throughput workloads, the SQL Server and PostgreSQL stores also
implement optional batch interfaces:

- `IBatchOutboxStore`
- `IBatchInboxStore`
- `IBatchDeadLetterStore`

Provider clients should implement these batch methods with bulk insert/update
commands, table-valued parameters, `COPY`, array parameters, partition-aware
queries, or equivalent database-specific primitives. Avoid one database
round-trip per message when processing large streams.

Signalynx does not yet ship RabbitMQ, Kafka, Azure Service Bus, Amazon SQS,
or other provider-specific transport adapters.

## Production Configuration

Register validation, logging, and handlers:

```csharp
builder.Services.AddValidatorsFromAssembly(
    typeof(CreateOrderValidator).Assembly);

builder.Services.AddSignalynx(options =>
{
    options.RegisterServicesFromAssembly(
        typeof(CreateOrderCommand).Assembly);
    options.AddOpenBehavior(typeof(ValidationBehavior<,>));
    options.AddOpenBehavior(typeof(LoggingBehavior<,>));
    options.NotificationPublishStrategy =
        SignalynxPublishStrategy.Sequential;
    options.EventPublishStrategy =
        SignalynxPublishStrategy.Sequential;
    options.ValidateHandlersOnStartup = true;
});
```

Sequential publishing is the safer default. Use parallel publishing only when
handlers are independent and thread-safe. Handlers are resolved through DI;
use scoped handlers for database contexts and request-scoped dependencies.

Register consumed messages with stable wire names:

```csharp
builder.Services.AddSignalynxMessaging(options =>
{
    options.RegisterMessage<OrderSubmitted>("orders.submitted.v1");
    options.OutboxBatchSize = 100;
    options.OutboxPollingInterval = TimeSpan.FromMilliseconds(250);
    options.OutboxLockDuration = TimeSpan.FromSeconds(30);
    options.MaxDeliveryAttempts = 5;
    options.BaseRetryDelay = TimeSpan.FromSeconds(1);
});

builder.Services
    .AddSignalynxMessageHandler<OrderSubmitted, OrderSubmittedHandler>();
```

Explicit wire names are recommended because assembly-qualified names can
change during refactoring.

Production deployments must register real providers:

```csharp
builder.Services.AddSingleton<IMessageTransport, ProductionTransport>();
builder.Services.AddSingleton<ISqlServerMessageStoreClient, SqlServerStoreClient>();
builder.Services.AddSignalynxSqlServerStores();
```

These implementations are application- or vendor-specific. Do not register
`Signalynx.Transports.InMemory` outside local development and tests.

## Idempotent Message Handlers

Most brokers provide at-least-once delivery, so handlers must tolerate
duplicates:

```csharp
public sealed class OrderSubmittedHandler(
    OrdersDbContext database,
    ILogger<OrderSubmittedHandler> logger)
    : IMessageHandler<OrderSubmitted>
{
    public async ValueTask HandleAsync(
        OrderSubmitted message,
        MessageContext context)
    {
        var alreadyProcessed = await database.ProcessedMessages
            .AnyAsync(
                x => x.MessageId == context.MessageId,
                context.CancellationToken);

        if (alreadyProcessed)
        {
            return;
        }

        await ApplyBusinessChangeAsync(
            message,
            context.CancellationToken);

        database.ProcessedMessages.Add(
            new ProcessedMessage(
                context.MessageId,
                DateTimeOffset.UtcNow));

        await database.SaveChangesAsync(context.CancellationToken);

        logger.LogInformation(
            "Processed message {MessageId} on attempt {Attempt}",
            context.MessageId,
            context.Attempt);
    }
}
```

Record the message ID and business update in one transaction. Signalynx's inbox
prevents completed messages from being handled twice by the same configured
store, but business-level idempotency remains necessary when handlers call
external systems.

## Transactional Outbox

For a true transactional outbox, the business write and outbox insert must
commit through the same database connection and transaction:

```csharp
await using var transaction =
    await database.Database.BeginTransactionAsync(cancellationToken);

database.Orders.Add(order);
await database.SaveChangesAsync(cancellationToken);

await messageBus.EnqueueAsync(
    new OrderSubmitted(order.Id),
    destination: "orders",
    headers: new Dictionary<string, string>
    {
        ["tenant-id"] = tenantId,
        ["schema-version"] = "1"
    },
    cancellationToken: cancellationToken);

await transaction.CommitAsync(cancellationToken);
```

This is atomic only when the selected `IOutboxStore` enlists in that same
transaction. A provider opening a separate connection is not transactional.

## Retries and Dead Letters

The default policy uses bounded exponential backoff. Replace `IRetryPolicy`
when permanent and transient exceptions require different handling:

```csharp
builder.Services.AddSingleton<IRetryPolicy, ApplicationRetryPolicy>();
```

Dead letters can be inspected and replayed:

```csharp
app.MapGet("/admin/messaging/dead-letters", async (
    IMessageOperations operations,
    CancellationToken cancellationToken) =>
    await operations.GetDeadLettersAsync(
        maxCount: 100,
        cancellationToken));

app.MapPost("/admin/messaging/dead-letters/{id:guid}/replay",
    async (
        Guid id,
        IMessageOperations operations,
        CancellationToken cancellationToken) =>
    {
        await operations.ReplayDeadLetterAsync(
            id,
            cancellationToken);
        return Results.Accepted();
    });
```

Protect these endpoints with administrative authorization and audit every
replay.

## Environment-Specific Providers

```csharp
if (builder.Environment.IsDevelopment())
{
    builder.Services.AddSignalynxInMemoryTransport(options =>
    {
        options.Capacity = 10_000;
    });
}
else
{
    // Application-defined extension that registers the selected production
    // IMessageTransport and persistent stores.
    builder.Services.AddProductionSignalynxTransport(
        builder.Configuration);
}
```

Integration tests should cover successful delivery, retries, retry exhaustion,
duplicate delivery, poison messages, scheduling, dead-letter replay, and
process restart behavior.

## Production Observability

Enable core mediator diagnostics when you want dispatch and publish telemetry:

```csharp
builder.Services.AddSignalynx(options =>
{
    options.RegisterServicesFromAssembly(typeof(Program).Assembly);
    options.EnableDiagnostics = true;
});
```

Signalynx emits dispatch and publish activities through the `Signalynx`
activity source and metrics through the `Signalynx` meter:

- `signalynx.dispatch.calls`
- `signalynx.dispatch.failures`
- `signalynx.dispatch.duration`
- `signalynx.publish.calls`
- `signalynx.publish.failures`
- `signalynx.publish.duration`

Signalynx emits these metrics through the `Signalynx.Messaging` meter:

- `signalynx.messaging.enqueued`
- `signalynx.messaging.sent`
- `signalynx.messaging.handled`
- `signalynx.messaging.retried`
- `signalynx.messaging.dead_lettered`
- `signalynx.messaging.handler.duration`

OpenTelemetry example:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing =>
    {
        tracing.AddSource(SignalynxDiagnostics.ActivitySourceName);
    })
    .WithMetrics(metrics =>
    {
        metrics.AddMeter(SignalynxDiagnostics.MeterName);
        metrics.AddMeter(SignalynxMessagingDiagnostics.MeterName);
        metrics.AddPrometheusExporter();
    });
```

This requires `OpenTelemetry.Extensions.Hosting` and the chosen exporter.
Alert on dead-letter growth, retry rate, outbox depth and age, handler latency,
and transport availability.

## Deployment and Scaling

- Run database migrations before enabling workers.
- Ensure only a worker holding the outbox lock publishes a row.
- Set lock duration above normal send latency and recover expired locks.
- Scale consumers according to partitioning and ordering requirements.
- Use graceful shutdown before terminating workers.
- Set broker acknowledgement timeouts above expected handler duration.
- Use bounded queues and broker quotas.
- Version contracts additively and deploy compatible consumers first.

## Security

- Use TLS and workload identity for broker and database connections.
- Never place credentials, access tokens, or unnecessary personal data in
  messages.
- Treat message headers as untrusted input.
- Validate tenant and authorization context inside handlers.
- Restrict dead-letter payload access and define retention policies.
- Keep Signalynx, .NET, serializers, broker clients, and providers patched.

## Production Readiness Checklist

- [ ] A persistent transport and inbox/outbox/dead-letter provider is installed.
- [ ] Business writes and outbox inserts share one transaction.
- [ ] Message wire names and schema versions are stable.
- [ ] Handlers are idempotent.
- [ ] Retry policy distinguishes transient and permanent failures.
- [ ] Dead-letter access and replay are authorized and audited.
- [ ] Metrics, logs, dashboards, and alerts are configured.
- [ ] Broker and database identities use least privilege.
- [ ] Retention, privacy, backup, and recovery policies are defined.
- [ ] Load, failure, duplicate-delivery, and restart tests have passed.
- [ ] Package versions are pinned and dependency licenses are reviewed.

Signalynx supplies the runtime and provider contracts. Production reliability
also depends on the selected broker, persistence provider, transaction design,
handler idempotency, deployment, and operations.

## Source generation

Add `Signalynx.SourceGeneration` as an analyzer to generate:

```csharp
services.AddSignalynxGeneratedHandlers();
```

The generator emits DI registrations, a static handler map, and duplicate single-handler diagnostic `SLX001`. Runtime assembly scanning remains fully supported. Compile-time pipeline composition and direct generated dispatch are roadmap items.

## Minimal APIs and ASP.NET Core

The sample in `samples/Signalynx.Samples.Api` shows asynchronous order endpoints. ASP.NET Core is not required by the libraries; it is only one possible host.

```csharp
app.MapPost("/orders", async (
    CreateOrderCommand command,
    ISignalynx signalynx,
    CancellationToken token) =>
    Results.Ok(await signalynx.DispatchAsync<CreateOrderCommand, Guid>(command, token)));
```

## Build, test, and benchmark

```bash
dotnet restore
dotnet build Signalynx.slnx -c Release
dotnet test tests/Signalynx.Tests -c Release
dotnet run -c Release --project benchmarks/Signalynx.Performance
dotnet run --project samples/Signalynx.Samples.Api
dotnet pack src/Signalynx.Core -c Release
```

BenchmarkDotNet scenarios include direct calls, cached delegates, reflection fallback, `ValueTask` dispatch, commands, queries, requests, notifications, events, diagnostics overhead, sequential/parallel publishing, one/three behavior pipelines, serialization, and enqueue cost. Dispatch, pipeline, diagnostics, and messaging benchmarks emit disassembly reports through BenchmarkDotNet. Always run benchmarks in Release mode without a debugger.

## Testing

Create a `ServiceCollection`, call `AddSignalynx`, and resolve `ISignalynx`. Tests should assert handler results, behavior ordering, cancellation flow, publisher strategy, and expected exceptions. The repository uses xUnit.

## Performance design

- Typed handler calls instead of reflection invocation during dispatch
- Startup assembly scanning and immutable handler metadata
- `ValueTask`-first async contracts
- No LINQ in core dispatch loops
- Async-only API with cancellation propagation
- Sequential publishing as the predictable low-overhead default
- Optional source-generated registration
- BenchmarkDotNet with allocation and GC measurements

`EnableDelegateCaching` reserves a stable configuration point for upcoming optimized dispatch caches. `EnableDiagnostics` turns on core dispatch and publish activities and metrics.

## Roadmap

- Generated direct-dispatch and pipeline delegates
- NativeAOT/trimming annotations and test matrix
- RabbitMQ, Azure Service Bus, Amazon SQS, and Kafka transport adapters
- .NET 10 target after the support baseline is adopted
- Signed packages, Source Link, API compatibility checks, and release automation

## License

Signalynx is distributed under the [MIT License](LICENSE). Third-party package
licenses are listed in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

The software is provided without warranty. Applications are responsible for
their own security, privacy, regulatory, and industry-specific compliance.
