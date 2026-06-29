namespace Signalynx.Messaging;

public class SignalynxMessagingException : Exception
{
    public SignalynxMessagingException(string message) : base(message)
    {
    }

    public SignalynxMessagingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

public sealed class UnknownMessageTypeException : SignalynxMessagingException
{
    public UnknownMessageTypeException(string messageType)
        : base($"Message type '{messageType}' is not registered.")
    {
        MessageType = messageType;
    }

    public string MessageType { get; }
}
