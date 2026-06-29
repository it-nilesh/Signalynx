using BenchmarkDotNet.Running;
using Signalynx.Performance;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
