using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Signalynx;

public static class SignalynxDiagnostics
{
    public const string ActivitySourceName = "Signalynx";

    public const string MeterName = "Signalynx";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName, "1.0.0");

    public static readonly Meter Meter = new(MeterName, "1.0.0");

    internal static readonly Counter<long> Dispatches =
        Meter.CreateCounter<long>("signalynx.dispatch.calls");

    internal static readonly Counter<long> DispatchFailures =
        Meter.CreateCounter<long>("signalynx.dispatch.failures");

    internal static readonly Histogram<double> DispatchDuration =
        Meter.CreateHistogram<double>("signalynx.dispatch.duration", "ms");

    internal static readonly Counter<long> Publishes =
        Meter.CreateCounter<long>("signalynx.publish.calls");

    internal static readonly Counter<long> PublishFailures =
        Meter.CreateCounter<long>("signalynx.publish.failures");

    internal static readonly Histogram<double> PublishDuration =
        Meter.CreateHistogram<double>("signalynx.publish.duration", "ms");

    internal static Activity? StartActivity(
        bool enabled,
        string operation,
        Type messageType)
    {
        if (!enabled)
        {
            return null;
        }

        var activity = ActivitySource.StartActivity(
            $"Signalynx.{operation}",
            ActivityKind.Internal);
        activity?.SetTag("signalynx.operation", operation);
        activity?.SetTag("signalynx.message.type", messageType.FullName ?? messageType.Name);
        return activity;
    }

    internal static void RecordDispatch(
        string operation,
        Type messageType,
        long started,
        Exception? exception)
    {
        var tags = Tags(operation, messageType);
        Dispatches.Add(1, tags);
        DispatchDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);

        if (exception is not null)
        {
            DispatchFailures.Add(1, tags);
        }
    }

    internal static void RecordPublish(
        string operation,
        Type messageType,
        int handlerCount,
        long started,
        Exception? exception)
    {
        var tags = Tags(operation, messageType);
        Publishes.Add(1, tags);
        PublishDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);

        if (exception is not null)
        {
            PublishFailures.Add(1, tags);
        }
    }

    internal static void CompleteActivity(
        Activity? activity,
        Exception? exception)
    {
        if (activity is null)
        {
            return;
        }

        if (exception is not null)
        {
            activity.SetStatus(ActivityStatusCode.Error, exception.Message);
            activity.SetTag("exception.type", exception.GetType().FullName);
            activity.SetTag("exception.message", exception.Message);
        }

        activity.Dispose();
    }

    private static TagList Tags(string operation, Type messageType)
    {
        var tags = new TagList
        {
            { "signalynx.operation", operation },
            { "signalynx.message.type", messageType.FullName ?? messageType.Name }
        };
        return tags;
    }
}
