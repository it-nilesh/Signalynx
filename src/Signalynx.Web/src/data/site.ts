export type PackageGroup = {
  name: string
  description: string
  packages: {
    name: string
    purpose: string
    note?: string
    badge?: string
  }[]
}

export const site = {
  name: 'Signalynx',
  eyebrow: 'Mediator + durable messaging for modern .NET',
  headline: 'Fast in-process dispatch. Reliable messaging across boundaries.',
  description:
    'A high-performance, strongly typed mediator, dispatcher, and durable messaging toolkit for .NET 8, .NET 9, and .NET 10.',

  // Direct mediator runtime without Microsoft DI integration.
  install: 'dotnet add package Signalynx.Core',

  // Recommended for ASP.NET Core, Worker, console, and other Microsoft DI hosts.
  diInstall: 'dotnet add package Signalynx.DependencyInjection',

  nugetUrl: 'https://www.nuget.org/packages/Signalynx.Core',
  githubUrl: 'https://github.com/it-nilesh/Signalynx',
  websiteUrl: 'https://signalynx.inilesh.dev/',
}

export const packageGroups: PackageGroup[] = [
  {
    name: 'Core',
    description:
      'Choose the runtime, lightweight contracts, or Microsoft DI integration based on where the code runs.',
    packages: [
      {
        name: 'Signalynx.Core',
        purpose:
          'The mediator runtime for dispatch, publishing, handler execution, pipelines, and bulk processing. Use directly when you do not need the Microsoft DI integration.',
        badge: 'Runtime',
      },
      {
        name: 'Signalynx.Abstractions',
        purpose:
          'Dependency-free contracts for commands, queries, requests, notifications, domain events, handlers, pipelines, and mediator APIs. Reference directly from shared or application class libraries that do not need the runtime or Microsoft DI.',
        badge: 'Class libraries',
      },
      {
        name: 'Signalynx.DependencyInjection',
        purpose:
          'Microsoft DI registration and assembly scanning for ASP.NET Core, Worker, console, and other host applications. Installs Core and Abstractions transitively.',
        badge: 'Host applications',
      },
    ],
  },
  {
    name: 'Capabilities',
    description: 'Add only the application capabilities you need.',
    packages: [
      {
        name: 'Signalynx.Validation',
        purpose: 'Optional FluentValidation pipeline behavior.',
      },
      {
        name: 'Signalynx.Logging',
        purpose: 'Optional Microsoft.Extensions.Logging behavior.',
      },
      {
        name: 'Signalynx.SourceGeneration',
        purpose:
          'Compile-time registration for trimming and NativeAOT scenarios.',
      },
      {
        name: 'Signalynx.Messaging',
        purpose:
          'Durable messaging with inbox/outbox contracts, hosted workers, retries, scheduling, dead letters, and replay.',
      },
    ],
  },
  {
    name: 'Stores',
    description:
      'Durable persistence adapters for production messaging.',
    packages: [
      {
        name: 'Signalynx.Stores.SqlServer',
        purpose:
          'SQL Server inbox, outbox, and dead-letter store adapters.',
      },
      {
        name: 'Signalynx.Stores.PostgreSql',
        purpose:
          'PostgreSQL inbox, outbox, and dead-letter store adapters.',
      },
    ],
  },
  {
    name: 'Transports',
    description:
      'Connect durable messaging to the broker infrastructure your application already uses.',
    packages: [
      {
        name: 'Signalynx.Transports.InMemory',
        purpose:
          'Non-persistent in-memory transport for local development and testing.',
        note: 'Development and tests only — messages are lost when the process exits',
      },
      {
        name: 'Signalynx.Transports.RabbitMQ',
        purpose:
          'RabbitMQ IMessageTransport adapter over the RabbitMQ client abstraction.',
      },
      {
        name: 'Signalynx.Transports.AzureServiceBus',
        purpose:
          'Azure Service Bus IMessageTransport adapter over the Azure Service Bus client abstraction.',
      },
      {
        name: 'Signalynx.Transports.AmazonSqs',
        purpose:
          'Amazon SQS IMessageTransport adapter over the Amazon SQS client abstraction.',
      },
      {
        name: 'Signalynx.Transports.Kafka',
        purpose:
          'Kafka IMessageTransport adapter over the Kafka client abstraction.',
      },
    ],
  },
]

export const codeExamples = {
  command: `public sealed record CreateOrderCommand(
    Guid CustomerId,
    decimal Amount)
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

  query: `public sealed record GetOrderQuery(Guid OrderId)
    : IQuery<OrderDto>;

var order = await signalynx.QueryAsync<GetOrderQuery, OrderDto>(
    new GetOrderQuery(orderId),
    cancellationToken);`,

  notification: `await signalynx.PublishAsync(
    new OrderCreated(orderId),
    cancellationToken);`,

  messaging: `services.AddSignalynxInMemoryTransport();

services.AddSignalynxMessaging(options =>
{
    options.RegisterMessage<OrderSubmitted>("orders.submitted.v1");
    options.MaxDeliveryAttempts = 5;
});

services.AddSignalynxMessageHandler<
    OrderSubmitted,
    OrderSubmittedHandler>();

var id = await bus.EnqueueAsync(
    new OrderSubmitted(orderId),
    destination: "orders",
    cancellationToken: cancellationToken);`,
}