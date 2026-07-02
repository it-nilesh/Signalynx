using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx;

public static class ServiceCollectionExtensions
{
    private static readonly HashSet<Type> HandlerDefinitions =
    [
        typeof(ICommandHandler<>),
        typeof(ICommandHandler<,>),
        typeof(IQueryHandler<,>),
        typeof(IRequestHandler<,>),
        typeof(INotificationHandler<>),
        typeof(IEventHandler<>)
    ];

    private static readonly HashSet<Type> MultipleHandlerDefinitions =
    [
        typeof(INotificationHandler<>),
        typeof(IEventHandler<>)
    ];

    [RequiresUnreferencedCode("Assembly scanning is not trimming-safe. Use AddSignalynx with generated HandlerDescriptor values for trimmed or NativeAOT applications.")]
    public static IServiceCollection AddSignalynx(
        this IServiceCollection services,
        Action<SignalynxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new SignalynxOptions();
        configure?.Invoke(options);

        var descriptors = Scan(options);
        return AddSignalynxCore(services, descriptors, options);
    }

    public static IServiceCollection AddSignalynx(
        this IServiceCollection services,
        IReadOnlyList<HandlerDescriptor> descriptors,
        Action<SignalynxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(descriptors);

        var options = new SignalynxOptions();
        configure?.Invoke(options);

        return AddSignalynxCore(services, descriptors, options);
    }

    public static IServiceCollection ConfigureSignalynxBulk(
        this IServiceCollection services,
        Action<SignalynxBulkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new SignalynxBulkOptions();
        configure(options);
        services.Replace(ServiceDescriptor.Singleton(options));
        return services;
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2072",
        Justification = "Generated descriptors preserve public constructors; reflection-scanned descriptors flow only from APIs marked RequiresUnreferencedCode.")]
    [UnconditionalSuppressMessage(
        "ReflectionAnalysis",
        "IL2072",
        Justification = "Generated descriptors preserve public constructors; reflection-scanned descriptors flow only from APIs marked RequiresUnreferencedCode.")]
    private static IServiceCollection AddSignalynxCore(
        IServiceCollection services,
        IReadOnlyList<HandlerDescriptor> descriptors,
        SignalynxOptions options)
    {
        Validate(descriptors, options);

        services.TryAddSingleton(options);
        services.TryAddSingleton(new HandlerRegistry(descriptors));
        services.TryAddSingleton(new SignalynxBulkOptions());
        services.TryAddTransient<PipelineExecutor>();
        services.TryAddTransient<SignalynxDispatcher>();
        services.TryAddTransient<NotificationPublisher>();
        services.TryAddTransient<EventPublisher>();
        services.TryAddTransient<ISignalynx, SignalynxMediator>();
        services.TryAddTransient<ISignalynxBulkProcessor, SignalynxBulkProcessor>();

        for (var i = 0; i < descriptors.Count; i++)
        {
            var descriptor = descriptors[i];
            var implementationType = descriptor.ImplementationType;
#pragma warning disable IL2072
            services.AddTransient(descriptor.ServiceType, implementationType);
#pragma warning restore IL2072
        }

        RegisterBehaviors(services, options);
        return services;
    }

    [RequiresUnreferencedCode("Assembly scanning is not trimming-safe. Use source-generated handler registration for trimmed or NativeAOT applications.")]
    private static List<HandlerDescriptor> Scan(SignalynxOptions options)
    {
        var descriptors = new List<HandlerDescriptor>();
        var seenAssemblies = new HashSet<Assembly>();

        for (var assemblyIndex = 0; assemblyIndex < options.Assemblies.Count; assemblyIndex++)
        {
            var assembly = options.Assemblies[assemblyIndex];
            if (!seenAssemblies.Add(assembly))
            {
                continue;
            }

            var types = GetLoadableTypes(assembly);
            for (var typeIndex = 0; typeIndex < types.Length; typeIndex++)
            {
                var implementationType = types[typeIndex];
                if (!implementationType.IsClass || implementationType.IsAbstract || implementationType.ContainsGenericParameters)
                {
                    continue;
                }

                var interfaces = implementationType.GetInterfaces();
                for (var interfaceIndex = 0; interfaceIndex < interfaces.Length; interfaceIndex++)
                {
                    var serviceType = interfaces[interfaceIndex];
                    if (!serviceType.IsGenericType)
                    {
                        continue;
                    }

                    var definition = serviceType.GetGenericTypeDefinition();
                    if (HandlerDefinitions.Contains(definition))
                    {
                        descriptors.Add(new HandlerDescriptor(
                            serviceType,
                            implementationType,
                            MultipleHandlerDefinitions.Contains(definition)));
                    }
                }
            }
        }

        return descriptors;
    }

    private static void Validate(IReadOnlyList<HandlerDescriptor> descriptors, SignalynxOptions options)
    {
        if (!options.ValidateHandlersOnStartup)
        {
            return;
        }

        var groups = descriptors.GroupBy(static descriptor => descriptor.ServiceType);
        foreach (var group in groups)
        {
            var entries = group.ToArray();
            if (entries.Length > 1 && !entries[0].AllowsMultiple)
            {
                throw new DuplicateHandlerException(
                    group.Key,
                    entries.Select(static descriptor => descriptor.ImplementationType).ToArray());
            }
        }
    }

    private static void RegisterBehaviors(IServiceCollection services, SignalynxOptions options)
    {
        for (var i = 0; i < options.OpenPipelineBehaviors.Count; i++)
        {
#pragma warning disable IL2072
            services.AddTransient(typeof(IPipelineBehavior<,>), options.OpenPipelineBehaviors[i]);
#pragma warning restore IL2072
        }

        RegisterClosedBehaviors(services, options.PipelineBehaviors, typeof(IPipelineBehavior<,>));
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2072",
        Justification = "Closed behavior types are explicit Type values supplied by application code or generated code and are registered directly with DI.")]
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Closed behavior discovery only inspects explicitly supplied behavior types.")]
    private static void RegisterClosedBehaviors(
        IServiceCollection services,
        IReadOnlyList<Type> behaviorTypes,
        Type interfaceDefinition)
    {
        for (var i = 0; i < behaviorTypes.Count; i++)
        {
            var behaviorType = behaviorTypes[i];
            var interfaces = behaviorType.GetInterfaces();
            var registered = false;
            for (var interfaceIndex = 0; interfaceIndex < interfaces.Length; interfaceIndex++)
            {
                var serviceType = interfaces[interfaceIndex];
                if (serviceType.IsGenericType &&
                    serviceType.GetGenericTypeDefinition() == interfaceDefinition)
                {
                    services.AddTransient(serviceType, behaviorType);
                    registered = true;
                }
            }

            if (!registered)
            {
                throw new ArgumentException(
                    $"Behavior '{behaviorType}' does not implement '{interfaceDefinition}'.");
            }
        }
    }

    [RequiresUnreferencedCode("Assembly.GetTypes is not trimming-safe. Use source-generated handler registration for trimmed or NativeAOT applications.")]
    private static Type[] GetLoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException exception)
        {
            return exception.Types.Where(static type => type is not null).Cast<Type>().ToArray();
        }
    }
}
