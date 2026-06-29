namespace Signalynx.Messaging;

public sealed class ExponentialBackoffRetryPolicy : IRetryPolicy
{
    private readonly SignalynxMessagingOptions _options;

    public ExponentialBackoffRetryPolicy(SignalynxMessagingOptions options)
    {
        _options = options;
    }

    public bool ShouldRetry(int attempt, Exception exception, out TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (attempt >= _options.MaxDeliveryAttempts)
        {
            delay = default;
            return false;
        }

        var multiplier = Math.Pow(2, Math.Max(0, attempt - 1));
        var milliseconds = Math.Min(
            _options.BaseRetryDelay.TotalMilliseconds * multiplier,
            TimeSpan.FromMinutes(5).TotalMilliseconds);
        delay = TimeSpan.FromMilliseconds(milliseconds);
        return true;
    }
}
