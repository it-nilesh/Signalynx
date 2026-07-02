using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace Signalynx.Tests;

public sealed class GeneratedDispatchAndAotTests
{
    [Fact]
    public async Task Descriptor_registration_dispatches_without_assembly_scanning()
    {
        var services = new ServiceCollection();
        services.AddSingleton<GeneratedDispatchObservations>();
        services.AddSignalynx(
            [
                new HandlerDescriptor(
                    typeof(ICommandHandler<GeneratedVoidCommand>),
                    typeof(GeneratedVoidCommandHandler),
                    AllowsMultiple: false),
                new HandlerDescriptor(
                    typeof(ICommandHandler<GeneratedValueCommand, int>),
                    typeof(GeneratedValueCommandHandler),
                    AllowsMultiple: false),
                new HandlerDescriptor(
                    typeof(IQueryHandler<GeneratedQuery, string>),
                    typeof(GeneratedQueryHandler),
                    AllowsMultiple: false),
                new HandlerDescriptor(
                    typeof(IRequestHandler<GeneratedRequest, int>),
                    typeof(GeneratedRequestHandler),
                    AllowsMultiple: false)
            ]);

        using var provider = services.BuildServiceProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        await mediator.DispatchAsync(new GeneratedVoidCommand(3));
        var commandResult = await mediator.DispatchAsync<GeneratedValueCommand, int>(new GeneratedValueCommand(4));
        var queryResult = await mediator.QueryAsync<GeneratedQuery, string>(new GeneratedQuery(5));
        var requestResult = await mediator.RequestAsync<GeneratedRequest, int>(new GeneratedRequest(6));

        Assert.Equal(3, provider.GetRequiredService<GeneratedDispatchObservations>().Total);
        Assert.Equal(8, commandResult);
        Assert.Equal("5", queryResult);
        Assert.Equal(7, requestResult);
    }

    [Fact]
    public async Task Cached_dispatch_delegates_respect_pipeline_behaviors()
    {
        var services = new ServiceCollection();
        services.AddSingleton<GeneratedDispatchObservations>();
        services.AddSignalynx(
            [
                new HandlerDescriptor(
                    typeof(IQueryHandler<GeneratedQuery, string>),
                    typeof(GeneratedQueryHandler),
                    AllowsMultiple: false)
            ],
            options => options.AddBehavior<GeneratedQueryBehavior>());

        using var provider = services.BuildServiceProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        var result = await mediator.QueryAsync<GeneratedQuery, string>(new GeneratedQuery(9));

        Assert.Equal("9", result);
        Assert.Equal(["before", "after"], provider.GetRequiredService<GeneratedDispatchObservations>().Steps);
    }

    [Fact]
    public async Task Dispatch_still_works_when_delegate_caching_is_disabled()
    {
        var services = new ServiceCollection();
        services.AddSignalynx(
            [
                new HandlerDescriptor(
                    typeof(ICommandHandler<GeneratedValueCommand, int>),
                    typeof(GeneratedValueCommandHandler),
                    AllowsMultiple: false)
            ],
            options => options.EnableDelegateCaching = false);

        using var provider = services.BuildServiceProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();

        var result = await mediator.DispatchAsync<GeneratedValueCommand, int>(new GeneratedValueCommand(11));

        Assert.Equal(22, result);
    }

    [Fact]
    public void Descriptor_registration_still_detects_duplicate_single_handlers()
    {
        var services = new ServiceCollection();

        Assert.Throws<DuplicateHandlerException>(() =>
            services.AddSignalynx(
                [
                    new HandlerDescriptor(
                        typeof(IQueryHandler<GeneratedQuery, string>),
                        typeof(GeneratedQueryHandler),
                        AllowsMultiple: false),
                    new HandlerDescriptor(
                        typeof(IQueryHandler<GeneratedQuery, string>),
                        typeof(AlternateGeneratedQueryHandler),
                        AllowsMultiple: false)
                ]));
    }

    [Fact]
    public void Assembly_scanning_apis_are_marked_as_trimming_unsafe()
    {
        var addSignalynx = typeof(ServiceCollectionExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(method =>
                method.Name == nameof(ServiceCollectionExtensions.AddSignalynx) &&
                method.GetParameters() is [{ ParameterType: var first }, { ParameterType: var second }]
                && first == typeof(IServiceCollection)
                && second == typeof(Action<SignalynxOptions>));

        var registerAssembly = typeof(SignalynxOptions)
            .GetMethod(nameof(SignalynxOptions.RegisterServicesFromAssembly), [typeof(Assembly)]);

        Assert.NotNull(registerAssembly);
        Assert.NotNull(addSignalynx.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
        Assert.NotNull(registerAssembly.GetCustomAttribute<RequiresUnreferencedCodeAttribute>());
    }
}

public sealed record GeneratedVoidCommand(int Value) : ICommand;

public sealed record GeneratedValueCommand(int Value) : ICommand<int>;

public sealed record GeneratedQuery(int Value) : IQuery<string>;

public sealed record GeneratedRequest(int Value) : IRequest<int>;

public sealed class GeneratedDispatchObservations
{
    public int Total;

    public List<string> Steps { get; } = [];
}

public sealed class GeneratedVoidCommandHandler(GeneratedDispatchObservations observations)
    : ICommandHandler<GeneratedVoidCommand>
{
    public ValueTask HandleAsync(GeneratedVoidCommand command, CancellationToken cancellationToken = default)
    {
        observations.Total += command.Value;
        return ValueTask.CompletedTask;
    }
}

public sealed class GeneratedValueCommandHandler : ICommandHandler<GeneratedValueCommand, int>
{
    public ValueTask<int> HandleAsync(
        GeneratedValueCommand command,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(command.Value * 2);
}

public sealed class GeneratedQueryHandler : IQueryHandler<GeneratedQuery, string>
{
    public ValueTask<string> HandleAsync(
        GeneratedQuery query,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(query.Value.ToString());
}

public sealed class AlternateGeneratedQueryHandler : IQueryHandler<GeneratedQuery, string>
{
    public ValueTask<string> HandleAsync(
        GeneratedQuery query,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult((query.Value + 1).ToString());
}

public sealed class GeneratedRequestHandler : IRequestHandler<GeneratedRequest, int>
{
    public ValueTask<int> HandleAsync(
        GeneratedRequest request,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(request.Value + 1);
}

public sealed class GeneratedQueryBehavior(GeneratedDispatchObservations observations)
    : IPipelineBehavior<GeneratedQuery, string>
{
    public async ValueTask<string> HandleAsync(
        GeneratedQuery request,
        RequestHandlerDelegate<string> next,
        CancellationToken cancellationToken = default)
    {
        observations.Steps.Add("before");
        var result = await next().ConfigureAwait(false);
        observations.Steps.Add("after");
        return result;
    }
}
