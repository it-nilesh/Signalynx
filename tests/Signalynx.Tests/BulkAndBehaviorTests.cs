using System.Collections.Concurrent;
using FluentValidation;
using Microsoft.Extensions.Logging;

namespace Signalynx.Tests;

public sealed class BulkAndBehaviorTests
{
    [Fact]
    public async Task Processes_bulk_items_asynchronously_and_in_parallel()
    {
        var processor = new SignalynxBulkProcessor(new SignalynxBulkOptions());
        var total = 0;

        await processor.ProcessAsync(
            new[] { 1, 2, 3 },
            (value, _) =>
            {
                Interlocked.Add(ref total, value);
                return ValueTask.CompletedTask;
            });
        await processor.ProcessParallelAsync(
            new[] { 1, 2, 3 },
            (value, _) =>
            {
                Interlocked.Add(ref total, value);
                return ValueTask.CompletedTask;
            },
            2);
        Assert.Equal(12, total);
    }

    [Fact]
    public async Task Validation_behavior_throws_for_invalid_request()
    {
        var behavior = new ValidationBehavior<ValidatedRequest, int>(
            [new ValidatedRequestValidator()]);

        await Assert.ThrowsAsync<ValidationException>(async () =>
            await behavior.HandleAsync(new ValidatedRequest(-1), () => ValueTask.FromResult(1)));
    }

    [Fact]
    public async Task Logging_behavior_logs_completion()
    {
        var logger = new RecordingLogger<LoggingBehavior<ValidatedRequest, int>>();
        var behavior = new LoggingBehavior<ValidatedRequest, int>(logger);

        var result = await behavior.HandleAsync(
            new ValidatedRequest(1),
            () => ValueTask.FromResult(2));

        Assert.Equal(2, result);
        Assert.Contains(logger.Events, entry => entry.Level == LogLevel.Debug);
    }

}

public sealed record ValidatedRequest(int Value);

public sealed class ValidatedRequestValidator : AbstractValidator<ValidatedRequest>
{
    public ValidatedRequestValidator() =>
        RuleFor(static request => request.Value).GreaterThan(0);
}

public sealed class RecordingLogger<T> : ILogger<T>
{
    public ConcurrentQueue<(LogLevel Level, string Message)> Events { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter) =>
        Events.Enqueue((logLevel, formatter(state, exception)));
}
