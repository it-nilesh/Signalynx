# Release Checklist

1. Confirm the `Signalynx` name and all intended NuGet package IDs are
   available. Availability is only guaranteed after NuGet accepts ownership.
2. Set `RepositoryUrl` to the real public repository URL during packing:

   ```bash
   dotnet pack Signalynx.slnx -c Release \
     -p:RepositoryUrl=https://github.com/OWNER/REPOSITORY
   ```

3. Review `LICENSE`, `THIRD-PARTY-NOTICES.md`, dependency licenses, and
   copyright ownership.
4. Run the Release build, tests, and benchmarks on supported .NET runtimes.
5. Keep performance statements tied to committed BenchmarkDotNet results and
   clearly identify hardware, runtime, and benchmark scope.
6. Inspect every `.nupkg` before signing or uploading.
7. Publish a pre-release version first and validate installation in a clean
   consumer project.
8. Configure a private security-reporting contact before making the repository
   public.
