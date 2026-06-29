using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Signalynx.Messaging;

internal sealed class ReceiverWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMessageTransport _transport;
    private readonly IInboxStore _inbox;
    private readonly IDeadLetterStore _deadLetters;
    private readonly IRetryPolicy _retryPolicy;
    private readonly SignalynxMessagingOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ReceiverWorker> _logger;

    public ReceiverWorker(
        IServiceScopeFactory scopeFactory,
        IMessageTransport transport,
        IInboxStore inbox,
        IDeadLetterStore deadLetters,
        IRetryPolicy retryPolicy,
        SignalynxMessagingOptions options,
        TimeProvider timeProvider,
        ILogger<ReceiverWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _transport = transport;
        _inbox = inbox;
        _deadLetters = deadLetters;
        _retryPolicy = retryPolicy;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var delivery in _transport
                           .ReceiveAsync(stoppingToken)
                           .WithCancellation(stoppingToken)
                           .ConfigureAwait(false))
        {
            await HandleAsync(delivery, stoppingToken).ConfigureAwait(false);
        }
    }

    private async ValueTask HandleAsync(
        TransportDelivery delivery,
        CancellationToken cancellationToken)
    {
        var envelope = delivery.Envelope;
        if (!_options.Registrations.TryGetValue(envelope.MessageType, out var registration))
        {
            await DeadLetterAsync(
                delivery,
                new UnknownMessageTypeException(envelope.MessageType),
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!await _inbox.TryStartAsync(
                envelope.Id,
                _timeProvider.GetUtcNow(),
                cancellationToken).ConfigureAwait(false))
        {
            await delivery.CompleteAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var started = Stopwatch.GetTimestamp();
        try
        {
            using var scope = _scopeFactory.CreateScope();
            await registration.Invoke(
                scope.ServiceProvider,
                envelope,
                delivery.Attempt,
                cancellationToken).ConfigureAwait(false);
            await _inbox.CompleteAsync(envelope.Id, cancellationToken).ConfigureAwait(false);
            await delivery.CompleteAsync(cancellationToken).ConfigureAwait(false);
            SignalynxMessagingDiagnostics.Handled.Add(1);
            SignalynxMessagingDiagnostics.HandlerDuration.Record(
                Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            await _inbox.FailAsync(
                envelope.Id,
                exception.ToString(),
                cancellationToken).ConfigureAwait(false);

            if (_retryPolicy.ShouldRetry(delivery.Attempt, exception, out var delay))
            {
                await delivery.RetryAsync(delay, cancellationToken).ConfigureAwait(false);
                SignalynxMessagingDiagnostics.Retried.Add(1);
                _logger.LogWarning(
                    exception,
                    "Message {MessageId} handler failed on attempt {Attempt}; retrying in {Delay}",
                    envelope.Id,
                    delivery.Attempt,
                    delay);
                return;
            }

            await DeadLetterAsync(delivery, exception, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask DeadLetterAsync(
        TransportDelivery delivery,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var deadLetter = new DeadLetterMessage(
            delivery.Envelope,
            delivery.Attempt,
            exception.ToString(),
            _timeProvider.GetUtcNow(),
            "receiver");
        await _deadLetters.AddAsync(deadLetter, cancellationToken).ConfigureAwait(false);
        await delivery.DeadLetterAsync(exception.ToString(), cancellationToken).ConfigureAwait(false);
        SignalynxMessagingDiagnostics.DeadLettered.Add(1);
        _logger.LogError(
            exception,
            "Message {MessageId} moved to the dead-letter store after {Attempt} attempts",
            delivery.Envelope.Id,
            delivery.Attempt);
    }
}
