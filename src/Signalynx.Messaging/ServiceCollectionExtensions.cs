using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Signalynx.Messaging;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxMessaging(
        this IServiceCollection services,
        Action<SignalynxMessagingOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new SignalynxMessagingOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();
        services.TryAddSingleton<IRetryPolicy, ExponentialBackoffRetryPolicy>();
        services.TryAddSingleton<ISignalynxMessageBus, SignalynxMessageBus>();
        services.TryAddSingleton<IMessageOperations, MessageOperations>();

        if (options.EnableOutboxWorker)
        {
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IHostedService, OutboxWorker>());
        }

        if (options.EnableReceiverWorker)
        {
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IHostedService, ReceiverWorker>());
        }

        return services;
    }

    public static IServiceCollection AddSignalynxMessageHandler<TMessage, THandler>(
        this IServiceCollection services)
        where THandler : class, IMessageHandler<TMessage>
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddTransient<IMessageHandler<TMessage>, THandler>();
        return services;
    }
}
