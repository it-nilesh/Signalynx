using Signalynx;
using Signalynx.Messaging;

namespace Signalynx.Samples.Api;

public sealed record CreateOrderCommand(Guid CustomerId, decimal Amount) : ICommand<Guid>;

public sealed record GetOrderByIdQuery(Guid OrderId) : IQuery<OrderDto>;

public sealed record OrderDto(Guid OrderId, string Name, decimal Amount);

public sealed record OrderSubmitted(Guid OrderId);

public sealed class CreateOrderCommandHandler : ICommandHandler<CreateOrderCommand, Guid>
{
    public ValueTask<Guid> HandleAsync(
        CreateOrderCommand command,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(Guid.NewGuid());
}

public sealed class GetOrderByIdQueryHandler : IQueryHandler<GetOrderByIdQuery, OrderDto>
{
    public ValueTask<OrderDto> HandleAsync(
        GetOrderByIdQuery query,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(new OrderDto(query.OrderId, "Sample Order", 100m));
}

public sealed class OrderSubmittedHandler(ILogger<OrderSubmittedHandler> logger)
    : IMessageHandler<OrderSubmitted>
{
    public ValueTask HandleAsync(OrderSubmitted message, MessageContext context)
    {
        logger.LogInformation(
            "Handled durable message {MessageId} for order {OrderId} on attempt {Attempt}",
            context.MessageId,
            message.OrderId,
            context.Attempt);
        return ValueTask.CompletedTask;
    }
}
