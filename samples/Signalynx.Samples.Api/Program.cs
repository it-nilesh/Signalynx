using Signalynx;
using Signalynx.Messaging;
using Signalynx.Messaging.InMemory;
using Signalynx.Samples.Api;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalynx(options =>
{
    options.RegisterServicesFromAssembly(typeof(Program).Assembly);
    options.AddOpenBehavior(typeof(LoggingBehavior<,>));
    options.NotificationPublishStrategy = SignalynxPublishStrategy.Sequential;
    options.EventPublishStrategy = SignalynxPublishStrategy.Sequential;
    options.ValidateHandlersOnStartup = true;
    options.EnableDelegateCaching = true;
});
builder.Services.AddSignalynxInMemoryTransport();
builder.Services.AddSignalynxMessaging(options =>
{
    options.RegisterMessage<OrderSubmitted>();
    options.MaxDeliveryAttempts = 5;
});
builder.Services.AddSignalynxMessageHandler<OrderSubmitted, OrderSubmittedHandler>();

var app = builder.Build();

app.MapPost("/orders", async (
    CreateOrderCommand command,
    ISignalynx signalynx,
    CancellationToken cancellationToken) =>
{
    var orderId = await signalynx.DispatchAsync<CreateOrderCommand, Guid>(
        command,
        cancellationToken);
    return Results.Ok(new { OrderId = orderId });
});

app.MapGet("/orders/{orderId:guid}", async (
    Guid orderId,
    ISignalynx signalynx,
    CancellationToken cancellationToken) =>
{
    var order = await signalynx.QueryAsync<GetOrderByIdQuery, OrderDto>(
        new GetOrderByIdQuery(orderId),
        cancellationToken);
    return Results.Ok(order);
});

app.MapPost("/orders/{orderId:guid}/submit", async (
    Guid orderId,
    ISignalynxMessageBus bus,
    CancellationToken cancellationToken) =>
{
    var messageId = await bus.EnqueueAsync(
        new OrderSubmitted(orderId),
        destination: "orders",
        cancellationToken: cancellationToken);
    return Results.Accepted(value: new { MessageId = messageId });
});

app.MapGet("/messaging/dead-letters", async (
    IMessageOperations operations,
    CancellationToken cancellationToken) =>
    Results.Ok(await operations.GetDeadLettersAsync(
        cancellationToken: cancellationToken)));

app.MapPost("/messaging/dead-letters/{messageId:guid}/replay", async (
    Guid messageId,
    IMessageOperations operations,
    CancellationToken cancellationToken) =>
{
    await operations.ReplayDeadLetterAsync(messageId, cancellationToken);
    return Results.Accepted();
});

app.Run();
