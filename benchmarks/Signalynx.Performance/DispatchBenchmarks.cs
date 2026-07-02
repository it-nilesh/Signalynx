using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace Signalynx.Performance;

[MemoryDiagnoser]
[DisassemblyDiagnoser(
    maxDepth: 3,
    printSource: true,
    exportGithubMarkdown: true,
    exportHtml: true,
    exportCombinedDisassemblyReport: true)]
[SimpleJob]
public class DispatchBenchmarks
{
    private ISignalynx _mediator = null!;
    private BenchmarkHandler _handler = null!;
    private BenchmarkCommand _command = null!;
    private BenchmarkNotification _notification = null!;
    private BenchmarkManyNotification _manyNotification = null!;
    private BenchmarkEvent _event = null!;
    private BenchmarkManyEvent _manyEvent = null!;
    private MethodInfo _reflectedMethod = null!;
    private Func<BenchmarkCommand, CancellationToken, ValueTask<int>> _cachedDelegate = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSignalynx(options =>
            options.RegisterServicesFromAssembly(typeof(DispatchBenchmarks).Assembly));
        var provider = services.BuildServiceProvider();
        _mediator = provider.GetRequiredService<ISignalynx>();
        _handler = (BenchmarkHandler)provider.GetRequiredService<ICommandHandler<BenchmarkCommand, int>>();
        _command = new BenchmarkCommand(42);
        _notification = new BenchmarkNotification();
        _manyNotification = new BenchmarkManyNotification();
        _event = new BenchmarkEvent();
        _manyEvent = new BenchmarkManyEvent();
        _reflectedMethod = typeof(BenchmarkHandler).GetMethod(nameof(BenchmarkHandler.HandleAsync))
            ?? throw new InvalidOperationException();
        _cachedDelegate = _handler.HandleAsync;
    }

    [Benchmark(Baseline = true)]
    public ValueTask<int> DirectCall() => _handler.HandleAsync(_command);

    [Benchmark]
    public ValueTask<int> CachedDelegateCall() => _cachedDelegate(_command, default);

    [Benchmark]
    public ValueTask<int> ReflectionInvoke() =>
        (ValueTask<int>)_reflectedMethod.Invoke(_handler, [_command, default(CancellationToken)])!;

    [Benchmark]
    public ValueTask AsyncCommandWithoutResult() =>
        _mediator.DispatchAsync(new BenchmarkVoidCommand());

    [Benchmark]
    public ValueTask<int> AsyncValueTaskCommand() =>
        _mediator.DispatchAsync<BenchmarkCommand, int>(_command);

    [Benchmark]
    public ValueTask<int> Query() =>
        _mediator.QueryAsync<BenchmarkQuery, int>(new BenchmarkQuery(42));

    [Benchmark]
    public ValueTask<int> Request() =>
        _mediator.RequestAsync<BenchmarkRequest, int>(new BenchmarkRequest(42));

    [Benchmark]
    public ValueTask NotificationOneHandler() =>
        _mediator.PublishAsync(_notification);

    [Benchmark]
    public ValueTask NotificationMultipleHandlers() =>
        _mediator.PublishAsync(_manyNotification);

    [Benchmark]
    public ValueTask EventOneHandler() =>
        _mediator.PublishEventAsync(_event);

    [Benchmark]
    public ValueTask EventMultipleHandlers() =>
        _mediator.PublishEventAsync(_manyEvent);
}

[MemoryDiagnoser]
[DisassemblyDiagnoser(
    maxDepth: 3,
    printSource: true,
    exportGithubMarkdown: true,
    exportHtml: true,
    exportCombinedDisassemblyReport: true)]
public class PipelineBenchmarks
{
    private ISignalynx _oneBehavior = null!;
    private ISignalynx _threeBehaviors = null!;
    private BenchmarkCommand _command = null!;

    [GlobalSetup]
    public void Setup()
    {
        _oneBehavior = Build(1);
        _threeBehaviors = Build(3);
        _command = new BenchmarkCommand(42);
    }

    [Benchmark]
    public ValueTask<int> OneBehavior() =>
        _oneBehavior.DispatchAsync<BenchmarkCommand, int>(_command);

    [Benchmark]
    public ValueTask<int> ThreeBehaviors() =>
        _threeBehaviors.DispatchAsync<BenchmarkCommand, int>(_command);

    private static ISignalynx Build(int behaviorCount)
    {
        var services = new ServiceCollection();
        services.AddSignalynx(options =>
        {
            options.RegisterServicesFromAssembly(typeof(PipelineBenchmarks).Assembly);
            for (var i = 0; i < behaviorCount; i++)
            {
                options.AddOpenBehavior(typeof(PassThroughBehavior<,>));
            }
        });
        return services.BuildServiceProvider().GetRequiredService<ISignalynx>();
    }
}

[MemoryDiagnoser]
[DisassemblyDiagnoser(
    maxDepth: 3,
    printSource: true,
    exportGithubMarkdown: true,
    exportHtml: true,
    exportCombinedDisassemblyReport: true)]
public class GeneratedDispatchBenchmarks
{
    private ISignalynx _cached = null!;
    private ISignalynx _uncached = null!;
    private ISignalynx _descriptorRegistered = null!;
    private BenchmarkHandler _handler = null!;
    private BenchmarkCommand _command = null!;

    [GlobalSetup]
    public void Setup()
    {
        _cached = BuildWithScanning(enableDelegateCaching: true);
        _uncached = BuildWithScanning(enableDelegateCaching: false);
        _descriptorRegistered = BuildWithDescriptors();
        _handler = new BenchmarkHandler();
        _command = new BenchmarkCommand(42);
    }

    [Benchmark(Baseline = true)]
    public ValueTask<int> DirectCall() =>
        _handler.HandleAsync(_command);

    [Benchmark]
    public ValueTask<int> CachedDelegateDispatch() =>
        _cached.DispatchAsync<BenchmarkCommand, int>(_command);

    [Benchmark]
    public ValueTask<int> UncachedDispatch() =>
        _uncached.DispatchAsync<BenchmarkCommand, int>(_command);

    [Benchmark]
    public ValueTask<int> DescriptorRegisteredDispatch() =>
        _descriptorRegistered.DispatchAsync<BenchmarkCommand, int>(_command);

    private static ISignalynx BuildWithScanning(bool enableDelegateCaching)
    {
        var services = new ServiceCollection();
        services.AddSignalynx(options =>
        {
            options.RegisterServicesFromAssembly(typeof(GeneratedDispatchBenchmarks).Assembly);
            options.EnableDelegateCaching = enableDelegateCaching;
        });
        return services.BuildServiceProvider().GetRequiredService<ISignalynx>();
    }

    private static ISignalynx BuildWithDescriptors()
    {
        var services = new ServiceCollection();
        services.AddSignalynx(
            [
                new HandlerDescriptor(
                    typeof(ICommandHandler<BenchmarkCommand, int>),
                    typeof(BenchmarkHandler),
                    AllowsMultiple: false)
            ]);
        return services.BuildServiceProvider().GetRequiredService<ISignalynx>();
    }
}

[MemoryDiagnoser]
[SimpleJob]
public class GeneratedDispatchLoadBenchmarks
{
    private const int Operations = 1_000_000;

    private ISignalynx _descriptorRegistered = null!;
    private BenchmarkCommand _command = null!;

    [GlobalSetup]
    public void Setup()
    {
        _descriptorRegistered = BuildWithDescriptors();
        _command = new BenchmarkCommand(1);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Operations)]
    public async ValueTask<int> SequentialGeneratedDispatch_OneMillion()
    {
        var total = 0;
        for (var i = 0; i < Operations; i++)
        {
            total += await _descriptorRegistered.DispatchAsync<BenchmarkCommand, int>(_command);
        }

        return total;
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    public async ValueTask<long> ParallelGeneratedDispatch_OneMillion()
    {
        long total = 0;
        await Parallel.ForAsync(
            0,
            Operations,
            async (_, _) =>
            {
                var result = await _descriptorRegistered.DispatchAsync<BenchmarkCommand, int>(_command);
                Interlocked.Add(ref total, result);
            });

        return total;
    }

    private static ISignalynx BuildWithDescriptors()
    {
        var services = new ServiceCollection();
        services.AddSignalynx(
            [
                new HandlerDescriptor(
                    typeof(ICommandHandler<BenchmarkCommand, int>),
                    typeof(BenchmarkHandler),
                    AllowsMultiple: false)
            ]);
        return services.BuildServiceProvider().GetRequiredService<ISignalynx>();
    }
}

[MemoryDiagnoser]
[DisassemblyDiagnoser(
    maxDepth: 3,
    printSource: true,
    exportGithubMarkdown: true,
    exportHtml: true,
    exportCombinedDisassemblyReport: true)]
public class DiagnosticsOverheadBenchmarks
{
    private ISignalynx _diagnosticsDisabled = null!;
    private ISignalynx _diagnosticsEnabled = null!;
    private BenchmarkCommand _command = null!;

    [GlobalSetup]
    public void Setup()
    {
        _diagnosticsDisabled = Build(enableDiagnostics: false);
        _diagnosticsEnabled = Build(enableDiagnostics: true);
        _command = new BenchmarkCommand(42);
    }

    [Benchmark(Baseline = true)]
    public ValueTask<int> DispatchDiagnosticsDisabled() =>
        _diagnosticsDisabled.DispatchAsync<BenchmarkCommand, int>(_command);

    [Benchmark]
    public ValueTask<int> DispatchDiagnosticsEnabled() =>
        _diagnosticsEnabled.DispatchAsync<BenchmarkCommand, int>(_command);

    private static ISignalynx Build(bool enableDiagnostics)
    {
        var services = new ServiceCollection();
        services.AddSignalynx(options =>
        {
            options.RegisterServicesFromAssembly(typeof(DiagnosticsOverheadBenchmarks).Assembly);
            options.EnableDiagnostics = enableDiagnostics;
        });
        return services.BuildServiceProvider().GetRequiredService<ISignalynx>();
    }
}

[MemoryDiagnoser]
public class PublisherStrategyBenchmarks
{
    private ISignalynx _sequential = null!;
    private ISignalynx _parallel = null!;
    private BenchmarkManyNotification _notification = null!;
    private BenchmarkManyEvent _event = null!;

    [GlobalSetup]
    public void Setup()
    {
        _sequential = Build(SignalynxPublishStrategy.Sequential);
        _parallel = Build(SignalynxPublishStrategy.Parallel);
        _notification = new BenchmarkManyNotification();
        _event = new BenchmarkManyEvent();
    }

    [Benchmark(Baseline = true)]
    public ValueTask SequentialNotificationMultipleHandlers() =>
        _sequential.PublishAsync(_notification);

    [Benchmark]
    public ValueTask ParallelNotificationMultipleHandlers() =>
        _parallel.PublishAsync(_notification);

    [Benchmark]
    public ValueTask SequentialEventMultipleHandlers() =>
        _sequential.PublishEventAsync(_event);

    [Benchmark]
    public ValueTask ParallelEventMultipleHandlers() =>
        _parallel.PublishEventAsync(_event);

    private static ISignalynx Build(SignalynxPublishStrategy strategy)
    {
        var services = new ServiceCollection();
        services.AddSignalynx(options =>
        {
            options.RegisterServicesFromAssembly(typeof(PublisherStrategyBenchmarks).Assembly);
            options.NotificationPublishStrategy = strategy;
            options.EventPublishStrategy = strategy;
        });
        return services.BuildServiceProvider().GetRequiredService<ISignalynx>();
    }
}

public sealed record BenchmarkCommand(int Value) : ICommand<int>;
public sealed record BenchmarkVoidCommand : ICommand;
public sealed record BenchmarkQuery(int Value) : IQuery<int>;
public sealed record BenchmarkRequest(int Value) : IRequest<int>;
public sealed record BenchmarkNotification : INotification;
public sealed record BenchmarkManyNotification : INotification;
public sealed record BenchmarkEvent : IEvent;
public sealed record BenchmarkManyEvent : IEvent;

public sealed class BenchmarkHandler : ICommandHandler<BenchmarkCommand, int>
{
    public ValueTask<int> HandleAsync(
        BenchmarkCommand command,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(command.Value);
}

public sealed class BenchmarkVoidHandler : ICommandHandler<BenchmarkVoidCommand>
{
    public ValueTask HandleAsync(
        BenchmarkVoidCommand command,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkQueryHandler : IQueryHandler<BenchmarkQuery, int>
{
    public ValueTask<int> HandleAsync(
        BenchmarkQuery query,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(query.Value);
}

public sealed class BenchmarkRequestHandler : IRequestHandler<BenchmarkRequest, int>
{
    public ValueTask<int> HandleAsync(
        BenchmarkRequest request,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(request.Value);
}

public sealed class BenchmarkManyNotificationHandlerOne : INotificationHandler<BenchmarkManyNotification>
{
    public ValueTask HandleAsync(
        BenchmarkManyNotification notification,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkManyNotificationHandlerTwo : INotificationHandler<BenchmarkManyNotification>
{
    public ValueTask HandleAsync(
        BenchmarkManyNotification notification,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkManyNotificationHandlerThree : INotificationHandler<BenchmarkManyNotification>
{
    public ValueTask HandleAsync(
        BenchmarkManyNotification notification,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkEventHandler : IEventHandler<BenchmarkEvent>
{
    public ValueTask HandleAsync(
        BenchmarkEvent domainEvent,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkManyEventHandlerOne : IEventHandler<BenchmarkManyEvent>
{
    public ValueTask HandleAsync(
        BenchmarkManyEvent domainEvent,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkManyEventHandlerTwo : IEventHandler<BenchmarkManyEvent>
{
    public ValueTask HandleAsync(
        BenchmarkManyEvent domainEvent,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkManyEventHandlerThree : IEventHandler<BenchmarkManyEvent>
{
    public ValueTask HandleAsync(
        BenchmarkManyEvent domainEvent,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class BenchmarkNotificationHandler : INotificationHandler<BenchmarkNotification>
{
    public ValueTask HandleAsync(
        BenchmarkNotification notification,
        CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

public sealed class PassThroughBehavior<TRequest, TResult> : IPipelineBehavior<TRequest, TResult>
{
    public ValueTask<TResult> HandleAsync(
        TRequest request,
        RequestHandlerDelegate<TResult> next,
        CancellationToken cancellationToken = default) =>
        next();
}
