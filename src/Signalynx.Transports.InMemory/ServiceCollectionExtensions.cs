using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Signalynx.Messaging.InMemory;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSignalynxInMemoryTransport(
        this IServiceCollection services,
        Action<InMemoryMessagingOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new InMemoryMessagingOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<InMemoryMessageStore>();
        services.TryAddSingleton<InMemoryMessageTransport>();
        services.TryAddSingleton<IOutboxStore>(
            static provider => provider.GetRequiredService<InMemoryMessageStore>());
        services.TryAddSingleton<IInboxStore>(
            static provider => provider.GetRequiredService<InMemoryMessageStore>());
        services.TryAddSingleton<IDeadLetterStore>(
            static provider => provider.GetRequiredService<InMemoryMessageStore>());
        services.TryAddSingleton<IMessageTransport>(
            static provider => provider.GetRequiredService<InMemoryMessageTransport>());
        return services;
    }
}
