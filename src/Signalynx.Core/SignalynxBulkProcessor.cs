using System.Collections.Concurrent;

namespace Signalynx;

public sealed class SignalynxBulkProcessor : ISignalynxBulkProcessor
{
    private readonly SignalynxBulkOptions _options;

    public SignalynxBulkProcessor(SignalynxBulkOptions options)
    {
        _options = options;
    }

    public async ValueTask ProcessAsync<TSource>(
        IReadOnlyList<TSource> source,
        Func<TSource, CancellationToken, ValueTask> processor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(processor);

        List<Exception>? errors = null;
        for (var offset = 0; offset < source.Count; offset += ValidBatchSize())
        {
            var end = Math.Min(source.Count, offset + ValidBatchSize());
            for (var i = offset; i < end; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await processor(source[i], cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (Capture(exception, ref errors))
                {
                }
            }
        }

        ThrowCollected(errors);
    }

    public async ValueTask ProcessParallelAsync<TSource>(
        IReadOnlyList<TSource> source,
        Func<TSource, CancellationToken, ValueTask> processor,
        int maxDegreeOfParallelism,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(processor);
        ValidateDegree(maxDegreeOfParallelism);

        var errors = new ConcurrentQueue<Exception>();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            await Parallel.ForEachAsync(
                source,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = maxDegreeOfParallelism,
                    CancellationToken = stop.Token
                },
                async (item, token) =>
                {
                    try
                    {
                        await processor(item, token).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        if (_options.ExceptionStrategy == SignalynxBulkExceptionStrategy.StopOnFirstError)
                        {
                            stop.Cancel();
                            throw;
                        }

                        if (_options.ExceptionStrategy == SignalynxBulkExceptionStrategy.CollectErrors)
                        {
                            errors.Enqueue(exception);
                        }
                    }
                }).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && errors.IsEmpty)
        {
            throw;
        }

        ThrowCollected(errors.Count == 0 ? null : errors.ToList());
    }

    private int ValidBatchSize() => _options.BatchSize > 0
        ? _options.BatchSize
        : throw new ArgumentOutOfRangeException(nameof(_options.BatchSize), "Batch size must be positive.");

    private bool Capture(Exception exception, ref List<Exception>? errors)
    {
        if (_options.ExceptionStrategy == SignalynxBulkExceptionStrategy.StopOnFirstError)
        {
            return false;
        }

        if (_options.ExceptionStrategy == SignalynxBulkExceptionStrategy.CollectErrors)
        {
            (errors ??= []).Add(exception);
        }

        return true;
    }

    private static void ThrowCollected(List<Exception>? errors)
    {
        if (errors is { Count: > 0 })
        {
            throw new BulkProcessingException(errors);
        }
    }

    private static void ValidateDegree(int value)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "Max degree of parallelism must be positive.");
        }
    }
}
