namespace Signalynx;

public sealed class PipelineExecutor
{
    public ValueTask<TResult> ExecuteAsync<TRequest, TResult>(
        TRequest request,
        IReadOnlyList<IPipelineBehavior<TRequest, TResult>> behaviors,
        RequestHandlerDelegate<TResult> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(behaviors);
        ArgumentNullException.ThrowIfNull(handler);

        if (behaviors.Count == 0)
        {
            return handler();
        }

        return InvokeAsync(0);

        ValueTask<TResult> InvokeAsync(int index) =>
            index == behaviors.Count
                ? handler()
                : behaviors[index].HandleAsync(request, () => InvokeAsync(index + 1), cancellationToken);
    }

}
