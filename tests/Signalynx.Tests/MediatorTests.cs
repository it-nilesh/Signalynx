using System.Collections.Concurrent;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Signalynx.Tests;

public sealed class MediatorTests
{
    [Fact]
    public async Task Dispatches_commands()
    {
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        await mediator.DispatchAsync(new PingCommand());

        Assert.Equal(1, provider.GetRequiredService<Counter>().Value);
    }

    [Fact]
    public async Task Dispatches_commands_with_results()
    {
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        Assert.Equal(6, await mediator.DispatchAsync<MultiplyCommand, int>(new MultiplyCommand(2, 3)));
    }

    [Fact]
    public async Task Dispatches_queries_and_requests()
    {
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        Assert.Equal("42", await mediator.QueryAsync<NumberQuery, string>(new NumberQuery(42)));
        Assert.Equal(43, await mediator.RequestAsync<IncrementRequest, int>(new IncrementRequest(42)));
    }

    [Fact]
    public async Task Publishes_notifications_and_events_to_multiple_handlers()
    {
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();
        var observations = provider.GetRequiredService<Observations>();

        await mediator.PublishAsync(new ChangedNotification());
        await mediator.PublishEventAsync(new ChangedEvent());

        Assert.Equal(2, observations.NotificationCount);
        Assert.Equal(2, observations.EventCount);
    }

    [Fact]
    public async Task Executes_behaviors_in_registration_order()
    {
        using var provider = CreateProvider(options =>
        {
            options.AddBehavior<FirstBehavior>();
            options.AddBehavior<SecondBehavior>();
        });
        var mediator = provider.GetRequiredService<ISignalynx>();
        var observations = provider.GetRequiredService<Observations>();

        await mediator.QueryAsync<NumberQuery, string>(new NumberQuery(1));
        Assert.Equal(["async-1-before", "async-2-before", "async-2-after", "async-1-after"], observations.Order);

    }

    [Fact]
    public async Task Flows_cancellation_token_to_handler()
    {
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();
        using var source = new CancellationTokenSource();
        source.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await mediator.DispatchAsync(new CancelCommand(), source.Token));
    }

    [Fact]
    public async Task Throws_for_missing_handler()
    {
        using var provider = CreateProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        await Assert.ThrowsAsync<HandlerNotFoundException>(async () =>
            await mediator.QueryAsync<MissingQuery, int>(new MissingQuery()));
    }

    [Fact]
    public void Detects_duplicate_single_handlers_at_startup()
    {
        var services = BaseServices();

        Assert.Throws<DuplicateHandlerException>(() =>
            services.AddSignalynx(options =>
                options.RegisterServicesFromAssembly(typeof(MediatorTests).Assembly)));
    }

    [Fact]
    public async Task Supports_parallel_publishing()
    {
        using var provider = CreateProvider(options =>
        {
            options.NotificationPublishStrategy = SignalynxPublishStrategy.Parallel;
            options.EventPublishStrategy = SignalynxPublishStrategy.Parallel;
        });
        var mediator = provider.GetRequiredService<ISignalynx>();
        var observations = provider.GetRequiredService<Observations>();

        await mediator.PublishAsync(new ChangedNotification());
        await mediator.PublishEventAsync(new ChangedEvent());

        Assert.Equal(2, observations.NotificationCount);
        Assert.Equal(2, observations.EventCount);
    }

    private static ServiceProvider CreateProvider(Action<SignalynxOptions>? configure = null)
    {
        var services = BaseServices();
        services.AddSignalynx(options =>
        {
            options.RegisterServicesFromAssembly(typeof(MediatorTests).Assembly);
            options.ValidateHandlersOnStartup = false;
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    private static ServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Counter>();
        services.AddSingleton<Observations>();
        return services;
    }
}

public sealed record PingCommand : ICommand;
public sealed record MultiplyCommand(int Left, int Right) : ICommand<int>;
public sealed record NumberQuery(int Number) : IQuery<string>;
public sealed record IncrementRequest(int Number) : IRequest<int>;
public sealed record CancelCommand : ICommand;
public sealed record MissingQuery : IQuery<int>;
public sealed record DuplicateCommand : ICommand<int>;
public sealed record ChangedNotification : INotification;
public sealed record ChangedEvent : IEvent;

public sealed class Counter
{
    public int Value;
}

public sealed class Observations
{
    public int NotificationCount;
    public int EventCount;
    public List<string> Order { get; } = [];
}

public sealed class PingAsyncHandler(Counter counter) : ICommandHandler<PingCommand>
{
    public ValueTask HandleAsync(PingCommand command, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref counter.Value);
        return ValueTask.CompletedTask;
    }
}

public sealed class MultiplyAsyncHandler : ICommandHandler<MultiplyCommand, int>
{
    public ValueTask<int> HandleAsync(MultiplyCommand command, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(command.Left * command.Right);
}

public sealed class NumberAsyncHandler : IQueryHandler<NumberQuery, string>
{
    public ValueTask<string> HandleAsync(NumberQuery query, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(query.Number.ToString());
}

public sealed class IncrementAsyncHandler : IRequestHandler<IncrementRequest, int>
{
    public ValueTask<int> HandleAsync(IncrementRequest request, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(request.Number + 1);
}

public sealed class CancelHandler : ICommandHandler<CancelCommand>
{
    public ValueTask HandleAsync(CancelCommand command, CancellationToken cancellationToken = default) =>
        ValueTask.FromCanceled(cancellationToken);
}

public sealed class DuplicateHandlerOne : ICommandHandler<DuplicateCommand, int>
{
    public ValueTask<int> HandleAsync(DuplicateCommand command, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(1);
}

public sealed class DuplicateHandlerTwo : ICommandHandler<DuplicateCommand, int>
{
    public ValueTask<int> HandleAsync(DuplicateCommand command, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(2);
}

public sealed class NotificationHandlerOne(Observations observations)
    : INotificationHandler<ChangedNotification>
{
    public ValueTask HandleAsync(ChangedNotification notification, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref observations.NotificationCount);
        return ValueTask.CompletedTask;
    }
}

public sealed class NotificationHandlerTwo(Observations observations)
    : INotificationHandler<ChangedNotification>
{
    public ValueTask HandleAsync(ChangedNotification notification, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref observations.NotificationCount);
        return ValueTask.CompletedTask;
    }
}

public sealed class EventHandlerOne(Observations observations)
    : IEventHandler<ChangedEvent>
{
    public ValueTask HandleAsync(ChangedEvent domainEvent, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref observations.EventCount);
        return ValueTask.CompletedTask;
    }
}

public sealed class EventHandlerTwo(Observations observations)
    : IEventHandler<ChangedEvent>
{
    public ValueTask HandleAsync(ChangedEvent domainEvent, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref observations.EventCount);
        return ValueTask.CompletedTask;
    }
}

public sealed class FirstBehavior(Observations observations) : IPipelineBehavior<NumberQuery, string>
{
    public async ValueTask<string> HandleAsync(
        NumberQuery request,
        RequestHandlerDelegate<string> next,
        CancellationToken cancellationToken = default)
    {
        observations.Order.Add("async-1-before");
        var result = await next();
        observations.Order.Add("async-1-after");
        return result;
    }
}

public sealed class SecondBehavior(Observations observations) : IPipelineBehavior<NumberQuery, string>
{
    public async ValueTask<string> HandleAsync(
        NumberQuery request,
        RequestHandlerDelegate<string> next,
        CancellationToken cancellationToken = default)
    {
        observations.Order.Add("async-2-before");
        var result = await next();
        observations.Order.Add("async-2-after");
        return result;
    }
}
