# Repository Guidelines

## Project Structure & Module Organization

Production packages live under `src/`: contracts in `Signalynx.Abstractions`, dispatch in `Signalynx.Core`, durable messaging in `Signalynx.Messaging`, development transport/storage in `Signalynx.Transports.InMemory`, DI scanning in `Signalynx.DependencyInjection`, and optional logging, validation, and source-generation packages beside them. xUnit tests are in `tests/Signalynx.Tests`, BenchmarkDotNet scenarios in `benchmarks/Signalynx.Performance`, and the Minimal API example in `samples/Signalynx.Samples.Api`.

Keep public contracts dependency-free. Optional framework integrations belong in their dedicated packages. Generated output (`bin/`, `obj/`, benchmark artifacts, and coverage reports) must not be committed.

## Build, Test, and Development Commands

- `dotnet restore` restores targeting packs and NuGet dependencies.
- `dotnet build Signalynx.slnx -c Release` compiles all projects.
- `dotnet test tests/Signalynx.Tests -c Release` runs the xUnit suite.
- `dotnet run -c Release --project benchmarks/Signalynx.Performance` runs benchmarks.
- `dotnet run --project samples/Signalynx.Samples.Api` starts the sample API.
- `dotnet pack src/Signalynx.Core -c Release` validates package output.

## Coding Style & Naming Conventions

Use modern C# with nullable reference types enabled. Indent with four spaces. Use `PascalCase` for types and public members, `camelCase` for locals and parameters, and `_camelCase` for fields. Async methods end in `Async` and return `ValueTask` where synchronous completion is common. Keep reflection and LINQ out of dispatch hot paths; startup discovery code may use them. Run `dotnet format` before broad style changes.

## Testing Guidelines

Add xUnit tests for every behavior change and regression. Name tests as behavioral phrases, for example `Flows_cancellation_token_to_handler`. Cover success, cancellation, missing/duplicate handlers, pipelines, and publisher strategies. Tests must be deterministic and independent of live services. Performance claims require a BenchmarkDotNet scenario with a direct-call baseline and `[MemoryDiagnoser]`.

## Commit & Pull Request Guidelines

There is no Git history from which to infer an existing convention. Use concise, imperative Conventional Commit subjects, for example `feat: add signal ingestion endpoint` or `fix: handle empty payloads`.

Pull requests should include a clear summary, testing evidence, and links to relevant issues. Add screenshots or logs for visible UI and operational changes. Keep each pull request narrowly scoped, update documentation when behavior changes, and ensure all automated checks pass before requesting review.
