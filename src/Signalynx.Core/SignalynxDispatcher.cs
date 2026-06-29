namespace Signalynx;

public sealed class SignalynxDispatcher
{
    private readonly IServiceProvider _services;
    private readonly PipelineExecutor _pipelines;

    public SignalynxDispatcher(IServiceProvider services, PipelineExecutor pipelines)
    {
        _services = services;
        _pipelines = pipelines;
    }

    public ValueTask DispatchAsync<TCommand>(TCommand command, CancellationToken cancellationToken)
        where TCommand : ICommand
    {
        var handler = Required<ICommandHandler<TCommand>, TCommand>();
        var behaviors = Services<IPipelineBehavior<TCommand, Unit>>();
        return AwaitUnit(_pipelines.ExecuteAsync(
            command,
            behaviors,
            async () =>
            {
                await handler.HandleAsync(command, cancellationToken).ConfigureAwait(false);
                return Unit.Value;
            },
            cancellationToken));
    }

    public ValueTask<TResult> DispatchAsync<TCommand, TResult>(
        TCommand command,
        CancellationToken cancellationToken)
        where TCommand : ICommand<TResult> =>
        ExecuteAsync<TCommand, TResult, ICommandHandler<TCommand, TResult>>(
            command,
            static (handler, message, token) => handler.HandleAsync(message, token),
            cancellationToken);

    public ValueTask<TResult> QueryAsync<TQuery, TResult>(
        TQuery query,
        CancellationToken cancellationToken)
        where TQuery : IQuery<TResult> =>
        ExecuteAsync<TQuery, TResult, IQueryHandler<TQuery, TResult>>(
            query,
            static (handler, message, token) => handler.HandleAsync(message, token),
            cancellationToken);

    public ValueTask<TResult> RequestAsync<TRequest, TResult>(
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : IRequest<TResult> =>
        ExecuteAsync<TRequest, TResult, IRequestHandler<TRequest, TResult>>(
            request,
            static (handler, message, token) => handler.HandleAsync(message, token),
            cancellationToken);

    private ValueTask<TResult> ExecuteAsync<TMessage, TResult, THandler>(
        TMessage message,
        Func<THandler, TMessage, CancellationToken, ValueTask<TResult>> invoke,
        CancellationToken cancellationToken)
        where THandler : notnull
    {
        var handler = Required<THandler, TMessage>();
        var behaviors = Services<IPipelineBehavior<TMessage, TResult>>();
        return _pipelines.ExecuteAsync(
            message,
            behaviors,
            () => invoke(handler, message, cancellationToken),
            cancellationToken);
    }

    private THandler Required<THandler, TMessage>() where THandler : notnull =>
        _services.GetService(typeof(THandler)) is THandler handler
            ? handler
            : throw new HandlerNotFoundException(typeof(TMessage), typeof(THandler));

    private IReadOnlyList<T> Services<T>() =>
        _services.GetService(typeof(IEnumerable<T>)) is IEnumerable<T> services
            ? services as IReadOnlyList<T> ?? services.ToArray()
            : Array.Empty<T>();

    private static async ValueTask AwaitUnit(ValueTask<Unit> operation) =>
        await operation.ConfigureAwait(false);

    private readonly record struct Unit
    {
        public static Unit Value => default;
    }
}
