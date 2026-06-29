namespace Signalynx;

public delegate ValueTask<TResult> RequestHandlerDelegate<TResult>();

public interface IPipelineBehavior<in TRequest, TResult>
{
    ValueTask<TResult> HandleAsync(
        TRequest request,
        RequestHandlerDelegate<TResult> next,
        CancellationToken cancellationToken = default);
}
