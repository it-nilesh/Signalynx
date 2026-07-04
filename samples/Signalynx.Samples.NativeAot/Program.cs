using Microsoft.Extensions.DependencyInjection;
using Signalynx;

var services = new ServiceCollection();
services.AddSingleton<NativeAotCounter>();
services.AddSignalynx(
    [
        new HandlerDescriptor(
            typeof(ICommandHandler<IncrementCounterCommand>),
            typeof(IncrementCounterHandler),
            AllowsMultiple: false),
        new HandlerDescriptor(
            typeof(ICommandHandler<DoubleValueCommand, int>),
            typeof(DoubleValueHandler),
            AllowsMultiple: false),
        new HandlerDescriptor(
            typeof(IQueryHandler<GreetingQuery, string>),
            typeof(GreetingQueryHandler),
            AllowsMultiple: false)
    ]);

using var provider = services.BuildServiceProvider();
var signalynx = provider.GetRequiredService<ISignalynx>();

await signalynx.DispatchAsync(new IncrementCounterCommand(3));
var doubled = await signalynx.DispatchAsync<DoubleValueCommand, int>(new DoubleValueCommand(21));
var greeting = await signalynx.QueryAsync<GreetingQuery, string>(new GreetingQuery("Signalynx"));

var counter = provider.GetRequiredService<NativeAotCounter>();
if (counter.Value != 3 || doubled != 42 || greeting != "Hello, Signalynx")
{
    Console.Error.WriteLine(
        $"NativeAOT sample failed. Counter={counter.Value}; Doubled={doubled}; Greeting={greeting}");
    return 1;
}

Console.WriteLine("Signalynx NativeAOT sample completed successfully.");
return 0;

public sealed record IncrementCounterCommand(int Amount) : ICommand;

public sealed record DoubleValueCommand(int Value) : ICommand<int>;

public sealed record GreetingQuery(string Name) : IQuery<string>;

public sealed class NativeAotCounter
{
    public int Value;
}

public sealed class IncrementCounterHandler(NativeAotCounter counter)
    : ICommandHandler<IncrementCounterCommand>
{
    public ValueTask HandleAsync(
        IncrementCounterCommand command,
        CancellationToken cancellationToken = default)
    {
        counter.Value += command.Amount;
        return ValueTask.CompletedTask;
    }
}

public sealed class DoubleValueHandler : ICommandHandler<DoubleValueCommand, int>
{
    public ValueTask<int> HandleAsync(
        DoubleValueCommand command,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(command.Value * 2);
}

public sealed class GreetingQueryHandler : IQueryHandler<GreetingQuery, string>
{
    public ValueTask<string> HandleAsync(
        GreetingQuery query,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult($"Hello, {query.Name}");
}
