namespace Signalynx;

internal delegate ValueTask<TResult> DispatchHandlerInvoker<in THandler, in TMessage, TResult>(
    THandler handler,
    TMessage message,
    CancellationToken cancellationToken);

internal static class CommandDispatchDelegateCache<TCommand, TResult>
    where TCommand : ICommand<TResult>
{
    public static readonly DispatchHandlerInvoker<ICommandHandler<TCommand, TResult>, TCommand, TResult> Invoke =
        static (handler, message, cancellationToken) =>
            handler.HandleAsync(message, cancellationToken);
}

internal static class QueryDispatchDelegateCache<TQuery, TResult>
    where TQuery : IQuery<TResult>
{
    public static readonly DispatchHandlerInvoker<IQueryHandler<TQuery, TResult>, TQuery, TResult> Invoke =
        static (handler, message, cancellationToken) =>
            handler.HandleAsync(message, cancellationToken);
}

internal static class RequestDispatchDelegateCache<TRequest, TResult>
    where TRequest : IRequest<TResult>
{
    public static readonly DispatchHandlerInvoker<IRequestHandler<TRequest, TResult>, TRequest, TResult> Invoke =
        static (handler, message, cancellationToken) =>
            handler.HandleAsync(message, cancellationToken);
}

internal static class CommandDispatchDelegateCache<TCommand>
    where TCommand : ICommand
{
    public static readonly DispatchHandlerInvoker<ICommandHandler<TCommand>, TCommand, SignalynxDispatcher.Unit> Invoke =
        static async (handler, command, cancellationToken) =>
        {
            await handler.HandleAsync(command, cancellationToken).ConfigureAwait(false);
            return SignalynxDispatcher.Unit.Value;
        };
}
