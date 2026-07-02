using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Signalynx.Tests;

public sealed class DiagnosticsTests
{
    [Fact]
    public async Task Emits_diagnostic_activity_when_enabled()
    {
        using var listener = new ActivityCollector();
        using var provider = CreateProvider(options => options.EnableDiagnostics = true);
        var mediator = provider.GetRequiredService<ISignalynx>();

        Assert.Equal("42", await mediator.QueryAsync<NumberQuery, string>(new NumberQuery(42)));

        var activity = Assert.Single(listener.Stopped);
        Assert.Equal("Signalynx.query", activity.OperationName);
        Assert.Equal("query", Tag(activity, "signalynx.operation"));
        Assert.Equal(typeof(NumberQuery).FullName, Tag(activity, "signalynx.message.type"));
        Assert.Equal(ActivityStatusCode.Unset, activity.Status);
    }

    [Fact]
    public async Task Emits_failed_diagnostic_activity_when_handler_fails()
    {
        using var listener = new ActivityCollector();
        using var provider = CreateProvider(options => options.EnableDiagnostics = true);
        var mediator = provider.GetRequiredService<ISignalynx>();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await mediator.RequestAsync<FailingRequest, int>(new FailingRequest()));

        var activity = Assert.Single(listener.Stopped);
        Assert.Equal("Signalynx.request", activity.OperationName);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal(typeof(InvalidOperationException).FullName, Tag(activity, "exception.type"));
    }

    [Fact]
    public async Task Emits_failed_diagnostic_activity_when_handler_is_missing()
    {
        using var listener = new ActivityCollector();
        using var provider = CreateProvider(options => options.EnableDiagnostics = true);
        var mediator = provider.GetRequiredService<ISignalynx>();

        await Assert.ThrowsAsync<HandlerNotFoundException>(async () =>
            await mediator.QueryAsync<MissingQuery, int>(new MissingQuery()));

        var activity = Assert.Single(listener.Stopped);
        Assert.Equal("Signalynx.query", activity.OperationName);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal(typeof(HandlerNotFoundException).FullName, Tag(activity, "exception.type"));
    }

    [Fact]
    public async Task Does_not_emit_diagnostic_activity_when_disabled()
    {
        using var listener = new ActivityCollector();
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        await mediator.QueryAsync<NumberQuery, string>(new NumberQuery(42));

        Assert.Empty(listener.Stopped);
    }

    [Fact]
    public async Task Emits_publish_diagnostic_activity_with_handler_count()
    {
        using var listener = new ActivityCollector();
        using var provider = CreateProvider(options => options.EnableDiagnostics = true);
        var mediator = provider.GetRequiredService<ISignalynx>();

        await mediator.PublishAsync(new ChangedNotification());

        var activity = Assert.Single(listener.Stopped);
        Assert.Equal("Signalynx.notification", activity.OperationName);
        Assert.Equal("notification", Tag(activity, "signalynx.operation"));
        Assert.Equal(2, Tag(activity, "signalynx.handler.count"));
    }

    private static ServiceProvider CreateProvider(Action<SignalynxOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Counter>();
        services.AddSingleton<Observations>();
        services.AddSignalynx(options =>
        {
            options.RegisterServicesFromAssembly(typeof(DiagnosticsTests).Assembly);
            options.ValidateHandlersOnStartup = false;
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    private static object? Tag(Activity activity, string key) =>
        activity.TagObjects.FirstOrDefault(tag => tag.Key == key).Value;
}

public sealed record FailingRequest : IRequest<int>;

public sealed class FailingRequestHandler : IRequestHandler<FailingRequest, int>
{
    public ValueTask<int> HandleAsync(FailingRequest request, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Expected test failure.");
}

public sealed class ActivityCollector : IDisposable
{
    private readonly ActivityListener _listener;

    public ActivityCollector()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = static source =>
                source.Name == SignalynxDiagnostics.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllData,
            ActivityStopped = activity => Stopped.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public ConcurrentBag<Activity> Stopped { get; } = [];

    public void Dispose() => _listener.Dispose();
}
