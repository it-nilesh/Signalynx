namespace Signalynx;

public interface ISignalynxBulkProcessor
{
    ValueTask ProcessAsync<TSource>(
        IReadOnlyList<TSource> source,
        Func<TSource, CancellationToken, ValueTask> processor,
        CancellationToken cancellationToken = default);

    ValueTask ProcessParallelAsync<TSource>(
        IReadOnlyList<TSource> source,
        Func<TSource, CancellationToken, ValueTask> processor,
        int maxDegreeOfParallelism,
        CancellationToken cancellationToken = default);
}
