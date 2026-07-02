namespace Signalynx;

public sealed class SignalynxDispatcher
{
    private readonly IServiceProvider _services;
    private readonly PipelineExecutor _pipelines;
    private readonly SignalynxOptions _options;

    public SignalynxDispatcher(
        IServiceProvider services,
        PipelineExecutor pipelines,
        SignalynxOptions options)
    {
        _services = services;
        _pipelines = pipelines;
        _options = options;
    }

    public ValueTask DispatchAsync<TCommand>(TCommand command, CancellationToken cancellationToken)
        where TCommand : ICommand
    {
        if (_options.EnableDiagnostics)
        {
            return AwaitUnit(TrackAsync(
                () => ExecuteCommandAsync(command, cancellationToken),
                "command",
                typeof(TCommand)));
        }

        return AwaitUnit(ExecuteCommandAsync(command, cancellationToken));
    }

    private ValueTask<Unit> ExecuteCommandAsync<TCommand>(
        TCommand command,
        CancellationToken cancellationToken)
        where TCommand : ICommand
    {
        var handler = Required<ICommandHandler<TCommand>, TCommand>();
        var behaviors = Services<IPipelineBehavior<TCommand, Unit>>();
        var invoke = _options.EnableDelegateCaching
            ? CommandDispatchDelegateCache<TCommand>.Invoke
            : static async (ICommandHandler<TCommand> handler, TCommand command, CancellationToken token) =>
            {
                await handler.HandleAsync(command, token).ConfigureAwait(false);
                return Unit.Value;
            };

        return _pipelines.ExecuteAsync(
            command,
            behaviors,
            (handler, command, cancellationToken, invoke),
            static state => state.invoke(
                state.handler,
                state.command,
                state.cancellationToken),
            cancellationToken);
    }

    public ValueTask<TResult> DispatchAsync<TCommand, TResult>(
        TCommand command,
        CancellationToken cancellationToken)
        where TCommand : ICommand<TResult> =>
        ExecuteAsync<TCommand, TResult, ICommandHandler<TCommand, TResult>>(
            command,
            "command",
            _options.EnableDelegateCaching
                ? CommandDispatchDelegateCache<TCommand, TResult>.Invoke
                : static (handler, message, token) => handler.HandleAsync(message, token),
            cancellationToken);

    public ValueTask<TResult> QueryAsync<TQuery, TResult>(
        TQuery query,
        CancellationToken cancellationToken)
        where TQuery : IQuery<TResult> =>
        ExecuteAsync<TQuery, TResult, IQueryHandler<TQuery, TResult>>(
            query,
            "query",
            _options.EnableDelegateCaching
                ? QueryDispatchDelegateCache<TQuery, TResult>.Invoke
                : static (handler, message, token) => handler.HandleAsync(message, token),
            cancellationToken);

    public ValueTask<TResult> RequestAsync<TRequest, TResult>(
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : IRequest<TResult> =>
        ExecuteAsync<TRequest, TResult, IRequestHandler<TRequest, TResult>>(
            request,
            "request",
            _options.EnableDelegateCaching
                ? RequestDispatchDelegateCache<TRequest, TResult>.Invoke
                : static (handler, message, token) => handler.HandleAsync(message, token),
            cancellationToken);

    private ValueTask<TResult> ExecuteAsync<TMessage, TResult, THandler>(
        TMessage message,
        string operationName,
        DispatchHandlerInvoker<THandler, TMessage, TResult> invoke,
        CancellationToken cancellationToken)
        where THandler : notnull
    {
        return _options.EnableDiagnostics
            ? TrackAsync(
                () => ExecuteWithHandlerAsync(message, invoke, cancellationToken),
                operationName,
                typeof(TMessage))
            : ExecuteWithHandlerAsync(message, invoke, cancellationToken);
    }

    private ValueTask<TResult> ExecuteWithHandlerAsync<TMessage, TResult, THandler>(
        TMessage message,
        DispatchHandlerInvoker<THandler, TMessage, TResult> invoke,
        CancellationToken cancellationToken)
        where THandler : notnull
    {
        var handler = Required<THandler, TMessage>();
        var behaviors = Services<IPipelineBehavior<TMessage, TResult>>();
        return _pipelines.ExecuteAsync(
            message,
            behaviors,
            (handler, message, cancellationToken, invoke),
            static state => state.invoke(state.handler, state.message, state.cancellationToken),
            cancellationToken);
    }

    private async ValueTask<TResult> TrackAsync<TResult>(
        Func<ValueTask<TResult>> operation,
        string operationName,
        Type messageType)
    {
        var activity = SignalynxDiagnostics.StartActivity(
            _options.EnableDiagnostics,
            operationName,
            messageType);
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        Exception? exception = null;

        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (Exception caught)
        {
            exception = caught;
            throw;
        }
        finally
        {
            SignalynxDiagnostics.RecordDispatch(
                operationName,
                messageType,
                started,
                exception);
            SignalynxDiagnostics.CompleteActivity(activity, exception);
        }
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

    internal readonly record struct Unit
    {
        public static Unit Value => default;
    }
}
