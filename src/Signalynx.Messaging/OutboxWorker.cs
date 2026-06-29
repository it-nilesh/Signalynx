using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Signalynx.Messaging;

internal sealed class OutboxWorker : BackgroundService
{
    private readonly IOutboxStore _outbox;
    private readonly IMessageTransport _transport;
    private readonly IDeadLetterStore _deadLetters;
    private readonly IRetryPolicy _retryPolicy;
    private readonly SignalynxMessagingOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<OutboxWorker> _logger;

    public OutboxWorker(
        IOutboxStore outbox,
        IMessageTransport transport,
        IDeadLetterStore deadLetters,
        IRetryPolicy retryPolicy,
        SignalynxMessagingOptions options,
        TimeProvider timeProvider,
        ILogger<OutboxWorker> logger)
    {
        _outbox = outbox;
        _transport = transport;
        _deadLetters = deadLetters;
        _retryPolicy = retryPolicy;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var messages = await _outbox.LockDueAsync(
                _options.OutboxBatchSize,
                _timeProvider.GetUtcNow(),
                _options.OutboxLockDuration,
                stoppingToken).ConfigureAwait(false);

            if (messages.Count == 0)
            {
                await Task.Delay(
                    _options.OutboxPollingInterval,
                    _timeProvider,
                    stoppingToken).ConfigureAwait(false);
                continue;
            }

            for (var i = 0; i < messages.Count; i++)
            {
                await DeliverAsync(messages[i], stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask DeliverAsync(
        OutboxMessage message,
        CancellationToken cancellationToken)
    {
        var attempt = message.Attempt + 1;
        try
        {
            await _transport.SendAsync(message.Envelope, cancellationToken).ConfigureAwait(false);
            await _outbox.MarkDeliveredAsync(
                message.Envelope.Id,
                cancellationToken).ConfigureAwait(false);
            SignalynxMessagingDiagnostics.Sent.Add(1);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            if (_retryPolicy.ShouldRetry(attempt, exception, out var delay))
            {
                await _outbox.RescheduleAsync(
                    message.Envelope.Id,
                    attempt,
                    _timeProvider.GetUtcNow() + delay,
                    exception.Message,
                    cancellationToken).ConfigureAwait(false);
                SignalynxMessagingDiagnostics.Retried.Add(1);
                _logger.LogWarning(
                    exception,
                    "Message {MessageId} transport send failed on attempt {Attempt}; retrying in {Delay}",
                    message.Envelope.Id,
                    attempt,
                    delay);
                return;
            }

            var deadLetter = new DeadLetterMessage(
                message.Envelope,
                attempt,
                exception.ToString(),
                _timeProvider.GetUtcNow(),
                "outbox");
            await _deadLetters.AddAsync(deadLetter, cancellationToken).ConfigureAwait(false);
            await _outbox.MoveToDeadLetterAsync(
                message.Envelope.Id,
                attempt,
                exception.ToString(),
                cancellationToken).ConfigureAwait(false);
            SignalynxMessagingDiagnostics.DeadLettered.Add(1);
            _logger.LogError(
                exception,
                "Message {MessageId} moved to the dead-letter store after {Attempt} attempts",
                message.Envelope.Id,
                attempt);
        }
    }
}
