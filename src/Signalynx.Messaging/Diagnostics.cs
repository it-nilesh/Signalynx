using System.Diagnostics.Metrics;

namespace Signalynx.Messaging;

public static class SignalynxMessagingDiagnostics
{
    public const string MeterName = "Signalynx.Messaging";

    public static readonly Meter Meter = new(MeterName, "1.0.0");

    internal static readonly Counter<long> Enqueued =
        Meter.CreateCounter<long>("signalynx.messaging.enqueued");

    internal static readonly Counter<long> Sent =
        Meter.CreateCounter<long>("signalynx.messaging.sent");

    internal static readonly Counter<long> Handled =
        Meter.CreateCounter<long>("signalynx.messaging.handled");

    internal static readonly Counter<long> Retried =
        Meter.CreateCounter<long>("signalynx.messaging.retried");

    internal static readonly Counter<long> DeadLettered =
        Meter.CreateCounter<long>("signalynx.messaging.dead_lettered");

    internal static readonly Histogram<double> HandlerDuration =
        Meter.CreateHistogram<double>("signalynx.messaging.handler.duration", "ms");
}
