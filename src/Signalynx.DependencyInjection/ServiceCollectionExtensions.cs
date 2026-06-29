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

    public static IServiceCollection AddSignalynx(
        this IServiceCollection services,
        Action<SignalynxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new SignalynxOptions();
        configure?.Invoke(options);

        var descriptors = Scan(options);
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
            services.AddTransient(descriptor.ServiceType, descriptor.ImplementationType);
        }

        RegisterBehaviors(services, options);
        return services;
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

    private static void Validate(List<HandlerDescriptor> descriptors, SignalynxOptions options)
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
            services.AddTransient(typeof(IPipelineBehavior<,>), options.OpenPipelineBehaviors[i]);
        }

        RegisterClosedBehaviors(services, options.PipelineBehaviors, typeof(IPipelineBehavior<,>));
    }

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
