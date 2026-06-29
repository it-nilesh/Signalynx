namespace Signalynx;

public sealed class NotificationPublisher
{
    private readonly IServiceProvider _services;
    private readonly SignalynxOptions _options;

    public NotificationPublisher(IServiceProvider services, SignalynxOptions options)
    {
        _services = services;
        _options = options;
    }

    public ValueTask PublishAsync<TNotification>(
        TNotification notification,
        CancellationToken cancellationToken)
        where TNotification : INotification
    {
        var handlers = GetServices<INotificationHandler<TNotification>>();
        return _options.NotificationPublishStrategy == SignalynxPublishStrategy.Sequential
            ? PublishSequentialAsync(handlers, notification, cancellationToken)
            : PublishParallelAsync(handlers, notification, cancellationToken);
    }

    private IReadOnlyList<T> GetServices<T>() =>
        _services.GetService(typeof(IEnumerable<T>)) is IEnumerable<T> services
            ? services as IReadOnlyList<T> ?? services.ToArray()
            : Array.Empty<T>();

    private static async ValueTask PublishSequentialAsync<TNotification>(
        IReadOnlyList<INotificationHandler<TNotification>> handlers,
        TNotification notification,
        CancellationToken cancellationToken)
        where TNotification : INotification
    {
        for (var i = 0; i < handlers.Count; i++)
        {
            await handlers[i].HandleAsync(notification, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask PublishParallelAsync<TNotification>(
        IReadOnlyList<INotificationHandler<TNotification>> handlers,
        TNotification notification,
        CancellationToken cancellationToken)
        where TNotification : INotification
    {
        if (handlers.Count == 0)
        {
            return;
        }

        var tasks = new Task[handlers.Count];
        for (var i = 0; i < handlers.Count; i++)
        {
            tasks[i] = handlers[i].HandleAsync(notification, cancellationToken).AsTask();
        }

        var all = Task.WhenAll(tasks);
        try
        {
            await all.ConfigureAwait(false);
        }
        catch when (all.Exception is not null)
        {
            throw new AggregateException(all.Exception.InnerExceptions);
        }
    }
}

public sealed class EventPublisher
{
    private readonly IServiceProvider _services;
    private readonly SignalynxOptions _options;

    public EventPublisher(IServiceProvider services, SignalynxOptions options)
    {
        _services = services;
        _options = options;
    }

    public ValueTask PublishAsync<TEvent>(TEvent domainEvent, CancellationToken cancellationToken)
        where TEvent : IEvent
    {
        var handlers = GetServices<IEventHandler<TEvent>>();
        return _options.EventPublishStrategy == SignalynxPublishStrategy.Sequential
            ? PublishSequentialAsync(handlers, domainEvent, cancellationToken)
            : PublishParallelAsync(handlers, domainEvent, cancellationToken);
    }

    private IReadOnlyList<T> GetServices<T>() =>
        _services.GetService(typeof(IEnumerable<T>)) is IEnumerable<T> services
            ? services as IReadOnlyList<T> ?? services.ToArray()
            : Array.Empty<T>();

    private static async ValueTask PublishSequentialAsync<TEvent>(
        IReadOnlyList<IEventHandler<TEvent>> handlers,
        TEvent domainEvent,
        CancellationToken cancellationToken)
        where TEvent : IEvent
    {
        for (var i = 0; i < handlers.Count; i++)
        {
            await handlers[i].HandleAsync(domainEvent, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async ValueTask PublishParallelAsync<TEvent>(
        IReadOnlyList<IEventHandler<TEvent>> handlers,
        TEvent domainEvent,
        CancellationToken cancellationToken)
        where TEvent : IEvent
    {
        if (handlers.Count == 0)
        {
            return;
        }

        var tasks = new Task[handlers.Count];
        for (var i = 0; i < handlers.Count; i++)
        {
            tasks[i] = handlers[i].HandleAsync(domainEvent, cancellationToken).AsTask();
        }

        var all = Task.WhenAll(tasks);
        try
        {
            await all.ConfigureAwait(false);
        }
        catch when (all.Exception is not null)
        {
            throw new AggregateException(all.Exception.InnerExceptions);
        }
    }
}
