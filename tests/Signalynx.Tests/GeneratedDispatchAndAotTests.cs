using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.DependencyInjection;
using Signalynx.SourceGeneration;

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
    public async Task Descriptor_registered_dispatch_handles_large_generated_load()
    {
        const int operations = 100_000;
        using var provider = CreateGeneratedDispatchProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();
        long total = 0;

        for (var i = 0; i < operations; i++)
        {
            total += await mediator.DispatchAsync<GeneratedValueCommand, int>(new GeneratedValueCommand(1));
        }

        Assert.Equal(operations * 2L, total);
    }

    [Fact]
    public async Task Descriptor_registered_dispatch_handles_parallel_generated_load()
    {
        const int operations = 100_000;
        using var provider = CreateGeneratedDispatchProvider();
        var mediator = provider.GetRequiredService<ISignalynx>();
        long total = 0;

        await Parallel.ForAsync(
            0,
            operations,
            async (_, _) =>
            {
                var result = await mediator.DispatchAsync<GeneratedValueCommand, int>(new GeneratedValueCommand(1));
                Interlocked.Add(ref total, result);
            });

        Assert.Equal(operations * 2L, total);
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

    [Fact]
    public void Source_generator_emits_aot_safe_registration_extension_and_descriptors()
    {
        var source = """
            using System.Threading;
            using System.Threading.Tasks;
            using Signalynx;

            namespace GeneratedCase
            {
                public sealed class GeneratedCaseCommand : ICommand<int>
                {
                    public GeneratedCaseCommand(int value) => Value = value;

                    public int Value { get; }
                }

                public sealed class GeneratedCaseHandler : ICommandHandler<GeneratedCaseCommand, int>
                {
                    public ValueTask<int> HandleAsync(
                        GeneratedCaseCommand command,
                        CancellationToken cancellationToken = default) =>
                        ValueTask.FromResult(command.Value);
                }
            }
            """;

        var compilation = CSharpCompilation.Create(
            "GeneratedCase",
            [CSharpSyntaxTree.ParseText(source)],
            GetGeneratorTestReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create([new SignalynxGenerator()]);
        driver = driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out _,
            out var diagnostics);

        var runResult = driver.GetRunResult();
        var generated = runResult
            .GeneratedTrees
            .Single(tree => tree.ToString().Contains("AddSignalynxGenerated(", StringComparison.Ordinal))
            .ToString();

        Assert.DoesNotContain(diagnostics, diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);
        Assert.Contains("AddSignalynxGenerated(", generated);
        Assert.Contains("new global::Signalynx.HandlerDescriptor", generated);
        Assert.Contains("typeof(global::Signalynx.ICommandHandler<global::GeneratedCase.GeneratedCaseCommand, int>)", generated);
        Assert.Contains("typeof(global::GeneratedCase.GeneratedCaseHandler)", generated);
    }

    private static IReadOnlyList<MetadataReference> GetGeneratorTestReferences()
    {
        var trustedPlatformAssemblies =
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? string.Empty;
        var paths = trustedPlatformAssemblies
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Append(typeof(ICommand<>).Assembly.Location)
            .Distinct(StringComparer.Ordinal);

        return paths
            .Select(static path => MetadataReference.CreateFromFile(path))
            .ToArray();
    }

    private static ServiceProvider CreateGeneratedDispatchProvider()
    {
        var services = new ServiceCollection();
        services.AddSignalynx(
            [
                new HandlerDescriptor(
                    typeof(ICommandHandler<GeneratedValueCommand, int>),
                    typeof(GeneratedValueCommandHandler),
                    AllowsMultiple: false)
            ]);

        return services.BuildServiceProvider();
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
