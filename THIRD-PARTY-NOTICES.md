# Third-Party Notices

Signalynx is developed independently.

The repository uses the following direct third-party packages. Their licenses
apply to those packages, not to original Signalynx source code.

## Runtime dependencies

| Package | License | Used by |
| --- | --- | --- |
| FluentValidation 12.0.0 | Apache-2.0 | `Signalynx.Validation` |
| Microsoft.Extensions.DependencyInjection.Abstractions 9.0.6 | MIT | `Signalynx.DependencyInjection` |
| Microsoft.Extensions.Logging.Abstractions 9.0.6 | MIT | `Signalynx.Logging` |
| Microsoft.Extensions.Hosting.Abstractions 9.0.6 | MIT | `Signalynx.Messaging` |

## Build, test, and benchmark dependencies

| Package | License |
| --- | --- |
| BenchmarkDotNet 0.15.2 | MIT |
| Microsoft.CodeAnalysis.CSharp 4.14.0 | MIT |
| xUnit 2.9.3 | Apache-2.0 |
| Microsoft.NET.Test.Sdk 17.14.1 | MIT |
| coverlet.collector 6.0.4 | MIT |

Transitive dependencies remain subject to their respective licenses. Before a
release, review the resolved dependency graph with:

```bash
dotnet list Signalynx.slnx package --include-transitive
```

Package versions and license terms can change. This notice should be reviewed
whenever dependencies are added or upgraded.
