export type PackageGroup = {
  name: string
  description: string
  packages: { name: string; purpose: string; note?: string; badge?: string }[]
}

export const site = {
  name: 'Signalynx',
  eyebrow: 'Mediator + durable messaging for modern .NET',
  headline: 'Fast in-process dispatch. Reliable messaging across boundaries.',
  description: 'A high-performance, strongly typed mediator, dispatcher, and durable messaging toolkit for .NET 8, .NET 9, and .NET 10.',
  install: 'dotnet add package Signalynx.Core',
  diInstall: 'dotnet add package Signalynx.DependencyInjection',
  nugetUrl: 'https://www.nuget.org/packages/Signalynx.Core',
  githubUrl: 'https://github.com/it-nilesh/Signalynx',
}

export const packageGroups: PackageGroup[] = [
  {
    name: 'Core',
    description: 'The essential runtime, contracts, and optional DI integration.',
    packages: [
      { name: 'Signalynx.Core', purpose: 'The main Signalynx runtime: mediator, publishers, handler registry, and bulk processor.', badge: 'Main package' },
      { name: 'Signalynx.Abstractions', purpose: 'Dependency-free contracts used by Core and shared contract projects.', badge: 'Included with Core' },
      { name: 'Signalynx.DependencyInjection', purpose: 'Microsoft DI registration and assembly scanning. Installs Core transitively.', badge: 'Optional integration' },
    ],
  },
  {
    name: 'Capabilities',
    description: 'Add only the application capabilities you need.',
    packages: [
      { name: 'Signalynx.Validation', purpose: 'Optional FluentValidation pipeline behavior.' },
      { name: 'Signalynx.Logging', purpose: 'Optional Microsoft.Extensions.Logging behavior.' },
      { name: 'Signalynx.SourceGeneration', purpose: 'Compile-time registration for trimming and NativeAOT.' },
      { name: 'Signalynx.Messaging', purpose: 'Inbox/outbox, workers, retries, schedules, and dead letters.' },
    ],
  },
  {
    name: 'Stores',
    description: 'Durable persistence adapters for production messaging.',
    packages: [
      { name: 'Signalynx.Stores.SqlServer', purpose: 'SQL Server inbox, outbox, and dead-letter stores.' },
      { name: 'Signalynx.Stores.PostgreSql', purpose: 'PostgreSQL inbox, outbox, and dead-letter stores.' },
    ],
  },
  {
    name: 'Transports',
    description: 'Connect durable messaging to the broker you already run.',
    packages: [
      { name: 'Signalynx.Transports.InMemory', purpose: 'Fast local transport and stores.', note: 'Development and tests only' },
      { name: 'Signalynx.Transports.RabbitMQ', purpose: 'RabbitMQ transport adapter.' },
      { name: 'Signalynx.Transports.AzureServiceBus', purpose: 'Azure Service Bus transport adapter.' },
      { name: 'Signalynx.Transports.AmazonSqs', purpose: 'Amazon SQS transport adapter.' },
      { name: 'Signalynx.Transports.Kafka', purpose: 'Kafka transport adapter.' },
    ],
  },
]

export const codeExamples = {
  command: `public sealed record CreateOrderCommand(Guid CustomerId, decimal Amount)
    : ICommand<Guid>;

public sealed class CreateOrderHandler
    : ICommandHandler<CreateOrderCommand, Guid>
{
    public ValueTask<Guid> HandleAsync(
        CreateOrderCommand command,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult(Guid.NewGuid());
}`,
  registration: `builder.Services.AddSignalynx(options =>
{
    options.RegisterServicesFromAssembly(typeof(Program).Assembly);
    options.AddOpenBehavior(typeof(LoggingBehavior<,>));
    options.NotificationPublishStrategy = SignalynxPublishStrategy.Sequential;
});`,
  dispatch: `var id = await signalynx.DispatchAsync<CreateOrderCommand, Guid>(
    command,
    cancellationToken);`,
  query: `public sealed record GetOrderQuery(Guid OrderId) : IQuery<OrderDto>;

var order = await signalynx.QueryAsync<GetOrderQuery, OrderDto>(
    new GetOrderQuery(orderId), cancellationToken);`,
  notification: `await signalynx.PublishAsync(
    new OrderCreated(orderId),
    cancellationToken);`,
  messaging: `services.AddSignalynxInMemoryTransport();
services.AddSignalynxMessaging(options =>
{
    options.RegisterMessage<OrderSubmitted>();
    options.MaxDeliveryAttempts = 5;
});
services.AddSignalynxMessageHandler<OrderSubmitted, OrderSubmittedHandler>();

var id = await bus.EnqueueAsync(
    new OrderSubmitted(orderId),
    destination: "orders",
    cancellationToken: cancellationToken);`,
}
