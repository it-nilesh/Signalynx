# Contributing to Signalynx

By submitting a contribution, you represent that you have the right to submit
it and agree that it may be distributed under the repository's MIT License.
Do not submit copied code or material whose license is incompatible with MIT.

## Development

```bash
dotnet restore
dotnet build Signalynx.slnx -c Release
dotnet test tests/Signalynx.Tests -c Release
```

Keep public contracts small and dependency-free. Add xUnit coverage for
behavior changes and BenchmarkDotNet evidence for performance claims. Avoid
reflection, LINQ, and avoidable allocations in dispatch hot paths.

Use concise Conventional Commit subjects, such as
`feat: add cached dispatch delegates`. Pull requests should explain the change,
include test evidence, and disclose any new dependency or copied/generated
material.
