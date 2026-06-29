namespace Signalynx;

public class SignalynxException : Exception
{
    public SignalynxException(string message) : base(message)
    {
    }

    public SignalynxException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

public sealed class HandlerNotFoundException : SignalynxException
{
    public HandlerNotFoundException(Type messageType, Type handlerType)
        : base($"No handler implementing '{handlerType}' is registered for message '{messageType}'.")
    {
        MessageType = messageType;
        HandlerType = handlerType;
    }

    public Type MessageType { get; }

    public Type HandlerType { get; }
}

public sealed class DuplicateHandlerException : SignalynxException
{
    public DuplicateHandlerException(Type serviceType, IReadOnlyList<Type> implementationTypes)
        : base($"Multiple handlers are registered for single-handler service '{serviceType}': " +
               string.Join(", ", implementationTypes.Select(static type => type.FullName)))
    {
        ServiceType = serviceType;
        ImplementationTypes = implementationTypes;
    }

    public Type ServiceType { get; }

    public IReadOnlyList<Type> ImplementationTypes { get; }
}

public sealed class BulkProcessingException : SignalynxException
{
    public BulkProcessingException(IReadOnlyList<Exception> errors)
        : base($"Bulk processing completed with {errors.Count} error(s).")
    {
        Errors = errors;
    }

    public IReadOnlyList<Exception> Errors { get; }
}
