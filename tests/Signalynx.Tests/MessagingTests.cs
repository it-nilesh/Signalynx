using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Signalynx.Messaging;
using Signalynx.Messaging.InMemory;

namespace Signalynx.Tests;

public sealed class MessagingTests
{
    [Fact]
    public async Task Delivers_outbox_message_to_registered_handler()
    {
        var state = new MessageHandlerState();
        await using var provider = CreateProvider<TestMessage, SuccessfulMessageHandler>(
            state,
            options => options.RegisterMessage<TestMessage>());
        var workers = provider.GetServices<IHostedService>().ToArray();
        await StartAsync(workers);

        var bus = provider.GetRequiredService<ISignalynxMessageBus>();
        var id = await bus.EnqueueAsync(new TestMessage("hello"));
        var handled = await state.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(id, handled.MessageId);
        Assert.Equal("hello", handled.Value);
        Assert.Equal(0, provider.GetRequiredService<InMemoryMessageStore>().PendingOutboxCount);

        await StopAsync(workers);
    }

    [Fact]
    public async Task Retries_failed_handler_and_then_completes()
    {
        var state = new MessageHandlerState { FailuresRemaining = 2 };
        await using var provider = CreateProvider<TestMessage, RetryingMessageHandler>(
            state,
            options =>
            {
                options.RegisterMessage<TestMessage>();
                options.MaxDeliveryAttempts = 3;
                options.BaseRetryDelay = TimeSpan.FromMilliseconds(1);
            });
        var workers = provider.GetServices<IHostedService>().ToArray();
        await StartAsync(workers);

        await provider.GetRequiredService<ISignalynxMessageBus>()
            .EnqueueAsync(new TestMessage("retry"));
        await state.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(3, Volatile.Read(ref state.Attempts));
        Assert.Empty(provider.GetRequiredService<InMemoryMessageStore>().DeadLetters);

        await StopAsync(workers);
    }

    [Fact]
    public async Task Moves_exhausted_message_to_dead_letter_store()
    {
        var state = new MessageHandlerState { FailuresRemaining = int.MaxValue };
        await using var provider = CreateProvider<TestMessage, RetryingMessageHandler>(
            state,
            options =>
            {
                options.RegisterMessage<TestMessage>();
                options.MaxDeliveryAttempts = 2;
                options.BaseRetryDelay = TimeSpan.FromMilliseconds(1);
            });
        var workers = provider.GetServices<IHostedService>().ToArray();
        await StartAsync(workers);

        var id = await provider.GetRequiredService<ISignalynxMessageBus>()
            .EnqueueAsync(new TestMessage("dead-letter"));
        var store = provider.GetRequiredService<InMemoryMessageStore>();

        await WaitUntilAsync(
            () => store.DeadLetters.Count == 1,
            TimeSpan.FromSeconds(5));

        Assert.Equal(id, store.DeadLetters[0].Envelope.Id);
        Assert.Equal("receiver", store.DeadLetters[0].Source);
        Assert.Equal(2, store.DeadLetters[0].Attempt);

        await StopAsync(workers);
    }

    [Fact]
    public async Task Replays_dead_letter_message()
    {
        var state = new MessageHandlerState { FailuresRemaining = 1 };
        await using var provider = CreateProvider<TestMessage, RetryingMessageHandler>(
            state,
            options =>
            {
                options.RegisterMessage<TestMessage>();
                options.MaxDeliveryAttempts = 1;
                options.BaseRetryDelay = TimeSpan.FromMilliseconds(1);
            });
        var workers = provider.GetServices<IHostedService>().ToArray();
        await StartAsync(workers);

        var id = await provider.GetRequiredService<ISignalynxMessageBus>()
            .EnqueueAsync(new TestMessage("replay"));
        var operations = provider.GetRequiredService<IMessageOperations>();
        await WaitUntilAsync(
            async () => (await operations.GetDeadLettersAsync()).Count == 1,
            TimeSpan.FromSeconds(5));

        await operations.ReplayDeadLetterAsync(id);
        var handled = await state.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(id, handled.MessageId);
        Assert.Empty(await operations.GetDeadLettersAsync());

        await StopAsync(workers);
    }

    [Fact]
    public async Task Supports_scheduled_delivery()
    {
        var state = new MessageHandlerState();
        await using var provider = CreateProvider<TestMessage, SuccessfulMessageHandler>(
            state,
            options =>
            {
                options.RegisterMessage<TestMessage>();
                options.OutboxPollingInterval = TimeSpan.FromMilliseconds(1);
            });
        var workers = provider.GetServices<IHostedService>().ToArray();
        await StartAsync(workers);

        var before = DateTimeOffset.UtcNow;
        await provider.GetRequiredService<ISignalynxMessageBus>().ScheduleAsync(
            new TestMessage("scheduled"),
            before + TimeSpan.FromMilliseconds(75));
        await state.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(DateTimeOffset.UtcNow - before >= TimeSpan.FromMilliseconds(50));

        await StopAsync(workers);
    }

    private static ServiceProvider CreateProvider<TMessage, THandler>(
        MessageHandlerState state,
        Action<SignalynxMessagingOptions> configure)
        where THandler : class, IMessageHandler<TMessage>
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(state);
        services.AddSignalynxInMemoryTransport(options => options.Capacity = 128);
        services.AddSignalynxMessaging(options =>
        {
            options.OutboxPollingInterval = TimeSpan.FromMilliseconds(1);
            configure(options);
        });
        services.AddSignalynxMessageHandler<TMessage, THandler>();
        return services.BuildServiceProvider();
    }

    private static async Task StartAsync(IReadOnlyList<IHostedService> workers)
    {
        for (var i = 0; i < workers.Count; i++)
        {
            await workers[i].StartAsync(CancellationToken.None);
        }
    }

    private static async Task StopAsync(IReadOnlyList<IHostedService> workers)
    {
        for (var i = workers.Count - 1; i >= 0; i--)
        {
            await workers[i].StopAsync(CancellationToken.None);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        while (!condition())
        {
            if (DateTime.UtcNow - started > timeout)
            {
                throw new TimeoutException("The expected messaging state was not reached.");
            }

            await Task.Delay(10);
        }
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        while (!await condition())
        {
            if (DateTime.UtcNow - started > timeout)
            {
                throw new TimeoutException("The expected messaging state was not reached.");
            }

            await Task.Delay(10);
        }
    }
}

public sealed record TestMessage(string Value);

public sealed class MessageHandlerState
{
    public TaskCompletionSource<(Guid MessageId, string Value)> Handled { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Attempts;

    public int FailuresRemaining;
}

public sealed class SuccessfulMessageHandler(MessageHandlerState state)
    : IMessageHandler<TestMessage>
{
    public ValueTask HandleAsync(TestMessage message, MessageContext context)
    {
        Interlocked.Increment(ref state.Attempts);
        state.Handled.TrySetResult((context.MessageId, message.Value));
        return ValueTask.CompletedTask;
    }
}

public sealed class RetryingMessageHandler(MessageHandlerState state)
    : IMessageHandler<TestMessage>
{
    public ValueTask HandleAsync(TestMessage message, MessageContext context)
    {
        Interlocked.Increment(ref state.Attempts);
        if (Interlocked.Decrement(ref state.FailuresRemaining) >= 0)
        {
            throw new InvalidOperationException("Expected test failure.");
        }

        state.Handled.TrySetResult((context.MessageId, message.Value));
        return ValueTask.CompletedTask;
    }
}
