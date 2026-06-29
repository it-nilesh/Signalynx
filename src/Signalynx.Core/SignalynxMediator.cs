namespace Signalynx;

public sealed class SignalynxMediator : ISignalynx
{
    private readonly SignalynxDispatcher _dispatcher;
    private readonly NotificationPublisher _notifications;
    private readonly EventPublisher _events;

    public SignalynxMediator(
        SignalynxDispatcher dispatcher,
        NotificationPublisher notifications,
        EventPublisher events)
    {
        _dispatcher = dispatcher;
        _notifications = notifications;
        _events = events;
    }

    public ValueTask DispatchAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        where TCommand : ICommand =>
        _dispatcher.DispatchAsync(command, cancellationToken);

    public ValueTask<TResult> DispatchAsync<TCommand, TResult>(
        TCommand command,
        CancellationToken cancellationToken = default)
        where TCommand : ICommand<TResult> =>
        _dispatcher.DispatchAsync<TCommand, TResult>(command, cancellationToken);

    public ValueTask<TResult> QueryAsync<TQuery, TResult>(
        TQuery query,
        CancellationToken cancellationToken = default)
        where TQuery : IQuery<TResult> =>
        _dispatcher.QueryAsync<TQuery, TResult>(query, cancellationToken);

    public ValueTask<TResult> RequestAsync<TRequest, TResult>(
        TRequest request,
        CancellationToken cancellationToken = default)
        where TRequest : IRequest<TResult> =>
        _dispatcher.RequestAsync<TRequest, TResult>(request, cancellationToken);

    public ValueTask PublishAsync<TNotification>(
        TNotification notification,
        CancellationToken cancellationToken = default)
        where TNotification : INotification =>
        _notifications.PublishAsync(notification, cancellationToken);

    public ValueTask PublishEventAsync<TEvent>(
        TEvent domainEvent,
        CancellationToken cancellationToken = default)
        where TEvent : IEvent =>
        _events.PublishAsync(domainEvent, cancellationToken);
}
