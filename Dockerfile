# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY Directory.Build.props Directory.Packages.props Signalynx.slnx ./
COPY src/Signalynx.Abstractions/Signalynx.Abstractions.csproj src/Signalynx.Abstractions/
COPY src/Signalynx.Core/Signalynx.Core.csproj src/Signalynx.Core/
COPY src/Signalynx.DependencyInjection/Signalynx.DependencyInjection.csproj src/Signalynx.DependencyInjection/
COPY src/Signalynx.Logging/Signalynx.Logging.csproj src/Signalynx.Logging/
COPY src/Signalynx.Messaging/Signalynx.Messaging.csproj src/Signalynx.Messaging/
COPY src/Signalynx.SourceGeneration/Signalynx.SourceGeneration.csproj src/Signalynx.SourceGeneration/
COPY src/Signalynx.Transports.AmazonSqs/Signalynx.Transports.AmazonSqs.csproj src/Signalynx.Transports.AmazonSqs/
COPY src/Signalynx.Transports.AzureServiceBus/Signalynx.Transports.AzureServiceBus.csproj src/Signalynx.Transports.AzureServiceBus/
COPY src/Signalynx.Transports.InMemory/Signalynx.Transports.InMemory.csproj src/Signalynx.Transports.InMemory/
COPY src/Signalynx.Transports.Kafka/Signalynx.Transports.Kafka.csproj src/Signalynx.Transports.Kafka/
COPY src/Signalynx.Transports.RabbitMQ/Signalynx.Transports.RabbitMQ.csproj src/Signalynx.Transports.RabbitMQ/
COPY samples/Signalynx.Samples.Api/Signalynx.Samples.Api.csproj samples/Signalynx.Samples.Api/

RUN dotnet restore samples/Signalynx.Samples.Api/Signalynx.Samples.Api.csproj

COPY src/ src/
COPY samples/Signalynx.Samples.Api/ samples/Signalynx.Samples.Api/

RUN dotnet publish samples/Signalynx.Samples.Api/Signalynx.Samples.Api.csproj \
    -c Release \
    --no-restore \
    -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Signalynx.Samples.Api.dll"]
