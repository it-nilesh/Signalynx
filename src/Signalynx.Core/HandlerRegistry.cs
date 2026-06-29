using System.Collections.Frozen;

namespace Signalynx;

public sealed record HandlerDescriptor(Type ServiceType, Type ImplementationType, bool AllowsMultiple);

public sealed class HandlerRegistry
{
    private readonly FrozenDictionary<Type, HandlerDescriptor[]> _handlers;

    public HandlerRegistry(IEnumerable<HandlerDescriptor> descriptors)
    {
        ArgumentNullException.ThrowIfNull(descriptors);
        _handlers = descriptors
            .GroupBy(static descriptor => descriptor.ServiceType)
            .ToFrozenDictionary(
                static group => group.Key,
                static group => group.ToArray());
    }

    public static HandlerRegistry Empty { get; } = new([]);

    public ReadOnlySpan<HandlerDescriptor> GetHandlers(Type serviceType) =>
        _handlers.TryGetValue(serviceType, out var handlers) ? handlers : [];

    public bool Contains(Type serviceType) => _handlers.ContainsKey(serviceType);
}
