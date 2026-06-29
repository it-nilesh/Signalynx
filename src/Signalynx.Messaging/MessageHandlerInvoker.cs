using Microsoft.Extensions.DependencyInjection;

namespace Signalynx.Messaging;

internal static class MessageHandlerInvoker<TMessage>
{
    public static async ValueTask InvokeAsync(
        IServiceProvider services,
        MessageEnvelope envelope,
        int attempt,
        CancellationToken cancellationToken)
    {
        var serializer = services.GetRequiredService<IMessageSerializer>();
        var message = (TMessage)serializer.Deserialize(envelope.Body, typeof(TMessage));
        var handlers = services.GetServices<IMessageHandler<TMessage>>();
        var context = new MessageContext(envelope, attempt, cancellationToken);
        var handled = false;

        foreach (var handler in handlers)
        {
            handled = true;
            await handler.HandleAsync(message, context).ConfigureAwait(false);
        }

        if (!handled)
        {
            throw new SignalynxMessagingException(
                $"No message handler is registered for '{typeof(TMessage)}'.");
        }
    }
}
