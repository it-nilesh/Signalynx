using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Signalynx;

public sealed class LoggingBehavior<TRequest, TResult>
    : IPipelineBehavior<TRequest, TResult>
{
    private static readonly Action<ILogger, string, Exception?> Started =
        LoggerMessage.Define<string>(
            LogLevel.Debug,
            new EventId(1000, "SignalynxRequestStarted"),
            "Handling Signalynx request {RequestType}");

    private static readonly Action<ILogger, string, double, Exception?> Completed =
        LoggerMessage.Define<string, double>(
            LogLevel.Debug,
            new EventId(1001, "SignalynxRequestCompleted"),
            "Handled Signalynx request {RequestType} in {ElapsedMilliseconds} ms");

    private static readonly Action<ILogger, string, double, Exception?> Failed =
        LoggerMessage.Define<string, double>(
            LogLevel.Error,
            new EventId(1002, "SignalynxRequestFailed"),
            "Signalynx request {RequestType} failed after {ElapsedMilliseconds} ms");

    private readonly ILogger<LoggingBehavior<TRequest, TResult>> _logger;

    public LoggingBehavior(ILogger<LoggingBehavior<TRequest, TResult>> logger)
    {
        _logger = logger;
    }

    public async ValueTask<TResult> HandleAsync(
        TRequest request,
        RequestHandlerDelegate<TResult> next,
        CancellationToken cancellationToken = default)
    {
        var requestName = typeof(TRequest).Name;
        Started(_logger, requestName, null);
        var started = Stopwatch.GetTimestamp();
        try
        {
            var result = await next().ConfigureAwait(false);
            Completed(_logger, requestName, Stopwatch.GetElapsedTime(started).TotalMilliseconds, null);
            return result;
        }
        catch (Exception exception)
        {
            Failed(_logger, requestName, Stopwatch.GetElapsedTime(started).TotalMilliseconds, exception);
            throw;
        }
    }
}
