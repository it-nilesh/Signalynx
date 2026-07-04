using System.Reflection;
using System.Text;
using Signalynx;
using Signalynx.Messaging;
using Signalynx.Messaging.Kafka;
using Signalynx.Messaging.PostgreSql;
using Signalynx.Messaging.RabbitMQ;
using Signalynx.Messaging.SqlServer;

namespace Signalynx.Tests;

public sealed class PublicApiApprovalTests
{
    public static IEnumerable<object[]> PublicAssemblies()
    {
        yield return [typeof(ISignalynx).Assembly, "Signalynx.Abstractions"];
        yield return [typeof(SignalynxDispatcher).Assembly, "Signalynx.Core"];
        yield return [typeof(Signalynx.Messaging.ServiceCollectionExtensions).Assembly, "Signalynx.Messaging"];
        yield return [typeof(RabbitMqMessageTransport).Assembly, "Signalynx.Transports.RabbitMQ"];
        yield return [typeof(KafkaMessageTransport).Assembly, "Signalynx.Transports.Kafka"];
        yield return [typeof(SqlServerMessageStore).Assembly, "Signalynx.Stores.SqlServer"];
        yield return [typeof(PostgreSqlMessageStore).Assembly, "Signalynx.Stores.PostgreSql"];
    }

    [Theory]
    [MemberData(nameof(PublicAssemblies))]
    public void Public_api_matches_approved_snapshot(Assembly assembly, string fileName)
    {
        var actual = BuildSnapshot(assembly);
        var approvalPath = Path.Combine(
            RepositoryRoot(),
            "tests",
            "Signalynx.Tests",
            "PublicApi",
            $"{fileName}.approved.txt");

        if (UpdateApprovalsEnabled())
        {
            Directory.CreateDirectory(Path.GetDirectoryName(approvalPath)!);
            File.WriteAllText(approvalPath, actual);
            return;
        }

        Assert.True(
            File.Exists(approvalPath),
            $"Missing public API approval file: {approvalPath}");
        Assert.Equal(File.ReadAllText(approvalPath), actual);
    }

    private static string BuildSnapshot(Assembly assembly)
    {
        var builder = new StringBuilder();
        builder.AppendLine($"# {assembly.GetName().Name}");
        builder.AppendLine();

        var types = assembly
            .GetExportedTypes()
            .OrderBy(static type => type.FullName, StringComparer.Ordinal)
            .ToArray();

        for (var i = 0; i < types.Length; i++)
        {
            var type = types[i];
            builder.AppendLine(TypeDeclaration(type));

            var constructors = type
                .GetConstructors(BindingFlags.Public | BindingFlags.Instance)
                .OrderBy(static member => member.ToString(), StringComparer.Ordinal);
            foreach (var constructor in constructors)
            {
                builder.AppendLine($"  ctor {FormatParameters(constructor.GetParameters())}");
            }

            var properties = type
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .OrderBy(static member => member.Name, StringComparer.Ordinal);
            foreach (var property in properties)
            {
                builder.AppendLine($"  property {FormatType(property.PropertyType)} {property.Name} {{ get;{(property.SetMethod?.IsPublic == true ? " set;" : string.Empty)} }}");
            }

            var methods = type
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Where(static method => !method.IsSpecialName)
                .OrderBy(static member => member.Name, StringComparer.Ordinal)
                .ThenBy(static member => member.ToString(), StringComparer.Ordinal);
            foreach (var method in methods)
            {
                builder.AppendLine($"  method {FormatType(method.ReturnType)} {method.Name}{FormatGenericParameters(method)}({FormatParameters(method.GetParameters())})");
            }

            builder.AppendLine();
        }

        return builder.ToString();
    }

    private static string TypeDeclaration(Type type)
    {
        var kind = type switch
        {
            { IsInterface: true } => "interface",
            { IsEnum: true } => "enum",
            { IsValueType: true } => "struct",
            { IsAbstract: true, IsSealed: true } => "static class",
            { IsAbstract: true } => "abstract class",
            _ => "class"
        };

        return $"{kind} {FormatType(type)}";
    }

    private static string FormatGenericParameters(MethodInfo method)
    {
        if (!method.IsGenericMethod)
        {
            return string.Empty;
        }

        return $"<{string.Join(", ", method.GetGenericArguments().Select(static parameter => parameter.Name))}>";
    }

    private static string FormatParameters(IEnumerable<ParameterInfo> parameters) =>
        string.Join(", ", parameters.Select(static parameter =>
            $"{FormatType(parameter.ParameterType)} {parameter.Name}{(parameter.HasDefaultValue ? " = default" : string.Empty)}"));

    private static string FormatType(Type type)
    {
        if (type.IsGenericParameter)
        {
            return type.Name;
        }

        if (type.IsArray)
        {
            return $"{FormatType(type.GetElementType()!)}[]";
        }

        if (!type.IsGenericType)
        {
            return type.FullName ?? type.Name;
        }

        var genericName = type.GetGenericTypeDefinition().FullName ?? type.Name;
        var tick = genericName.IndexOf('`', StringComparison.Ordinal);
        if (tick >= 0)
        {
            genericName = genericName[..tick];
        }

        return $"{genericName}<{string.Join(", ", type.GetGenericArguments().Select(FormatType))}>";
    }

    private static bool UpdateApprovalsEnabled() =>
        string.Equals(Environment.GetEnvironmentVariable("SIGNALYNX_UPDATE_PUBLIC_API"), "1", StringComparison.Ordinal) ||
        string.Equals(Environment.GetEnvironmentVariable("SIGNALYNX_UPDATE_PUBLIC_API"), "true", StringComparison.OrdinalIgnoreCase);

    private static string RepositoryRoot()
    {
        var directory = AppContext.BaseDirectory;
        while (!File.Exists(Path.Combine(directory, "Signalynx.slnx")))
        {
            directory = Directory.GetParent(directory)?.FullName
                ?? throw new DirectoryNotFoundException("Could not locate the repository root.");
        }

        return directory;
    }
}
