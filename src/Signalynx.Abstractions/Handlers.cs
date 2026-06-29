namespace Signalynx;

public interface ICommandHandler<in TCommand> where TCommand : ICommand
{
    ValueTask HandleAsync(TCommand command, CancellationToken cancellationToken = default);
}

public interface ICommandHandler<in TCommand, TResult> where TCommand : ICommand<TResult>
{
    ValueTask<TResult> HandleAsync(TCommand command, CancellationToken cancellationToken = default);
}

public interface IQueryHandler<in TQuery, TResult> where TQuery : IQuery<TResult>
{
    ValueTask<TResult> HandleAsync(TQuery query, CancellationToken cancellationToken = default);
}

public interface IRequestHandler<in TRequest, TResult> where TRequest : IRequest<TResult>
{
    ValueTask<TResult> HandleAsync(TRequest request, CancellationToken cancellationToken = default);
}

public interface INotificationHandler<in TNotification> where TNotification : INotification
{
    ValueTask HandleAsync(TNotification notification, CancellationToken cancellationToken = default);
}

public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    ValueTask HandleAsync(TEvent domainEvent, CancellationToken cancellationToken = default);
}
