using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Signalynx;

public enum SignalynxPublishStrategy
{
    Sequential,
    Parallel
}

public sealed class SignalynxOptions
{
    public List<Assembly> Assemblies { get; } = [];

    public List<Type> PipelineBehaviors { get; } = [];

    public List<Type> OpenPipelineBehaviors { get; } = [];

    public SignalynxPublishStrategy NotificationPublishStrategy { get; set; }
        = SignalynxPublishStrategy.Sequential;

    public SignalynxPublishStrategy EventPublishStrategy { get; set; }
        = SignalynxPublishStrategy.Sequential;

    public bool ValidateHandlersOnStartup { get; set; } = true;

    public bool EnableDelegateCaching { get; set; } = true;

    public bool EnableDiagnostics { get; set; }

    [RequiresUnreferencedCode("Assembly scanning is not trimming-safe. Use source-generated handler registration for trimmed or NativeAOT applications.")]
    public SignalynxOptions RegisterServicesFromAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        Assemblies.Add(assembly);
        return this;
    }

    [RequiresUnreferencedCode("Assembly scanning is not trimming-safe. Use source-generated handler registration for trimmed or NativeAOT applications.")]
    public SignalynxOptions RegisterServicesFromAssemblies(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        for (var i = 0; i < assemblies.Length; i++)
        {
            RegisterServicesFromAssembly(assemblies[i]);
        }

        return this;
    }

    public SignalynxOptions AddBehavior<TBehavior>()
    {
        PipelineBehaviors.Add(typeof(TBehavior));
        return this;
    }

    public SignalynxOptions AddOpenBehavior(Type behaviorType)
    {
        ValidateOpenBehavior(behaviorType, nameof(behaviorType));
        OpenPipelineBehaviors.Add(behaviorType);
        return this;
    }

    private static void ValidateOpenBehavior(Type behaviorType, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(behaviorType, parameterName);
        if (!behaviorType.IsGenericTypeDefinition || behaviorType.GetGenericArguments().Length != 2)
        {
            throw new ArgumentException("An open behavior must be a generic type definition with two parameters.", parameterName);
        }
    }
}

public sealed class SignalynxBulkOptions
{
    public int BatchSize { get; set; } = 1000;

    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    public SignalynxBulkExceptionStrategy ExceptionStrategy { get; set; }
        = SignalynxBulkExceptionStrategy.StopOnFirstError;
}

public enum SignalynxBulkExceptionStrategy
{
    StopOnFirstError,
    ContinueOnError,
    CollectErrors
}
