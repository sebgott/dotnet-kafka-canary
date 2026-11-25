using Azure.Identity;
using Confluent.Kafka;
using dotnet_kafka_canary;
using dotnet_kafka_canary.Domain.Tracing;
using Microsoft.Extensions.Options;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddOptions<KafkaOptions>()
    .BindConfiguration(KafkaOptions.SectionName)
    .ValidateOnStart();
builder.Services.AddSingleton<IValidateOptions<KafkaOptions>, KafkaOptionsValidator>();
builder.Services.AddHttpClient();
builder.Services.AddSingleton<IKafkaOAuthTokenProvider>(services =>
{
    var options = services.GetRequiredService<IOptions<KafkaOptions>>();
    return options.Value is { SaslMechanism: SaslMechanism.OAuthBearer, UseWorkloadIdentity: true }
        ? new KafkaWorkloadIdentityTokenProvider(new WorkloadIdentityCredential(), options)
        : ActivatorUtilities.CreateInstance<KafkaOAuthTokenProvider>(services);
});
builder.Services.AddSingleton<KafkaClientConfigurationFactory>();
builder.Services.AddSingleton<KafkaClientFactory>();
builder.Services.AddSingleton(serviceProvider =>
    serviceProvider.GetRequiredService<KafkaClientFactory>().Create());
builder.Services.AddHostedService<KafkaCanaryService>();

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(
        builder.Configuration["OTEL_SERVICE_NAME"] ?? "dotnet-kafka-canary"))
    .WithTracing(tracing =>
    {
        tracing
            .AddSource(KafkaCanaryTelemetry.ActivitySourceName)
            .AddAspNetCoreInstrumentation(options => options.Filter = context =>
                !context.Request.Path.StartsWithSegments("/metrics") &&
                !context.Request.Path.StartsWithSegments("/health"))
            .AddHttpClientInstrumentation();

        // Export only when an OTLP endpoint is configured via the standard OTEL_* environment variables.
        if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]) ||
            !string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]))
        {
            tracing.AddOtlpExporter();
        }
    });

builder.Services
    .AddHealthChecks()
    .AddCheck<KafkaCanaryHealthCheck>("kafka_canary");

var app = builder.Build();

app.MapHealthChecks("/health/ready");
app.UseHttpsRedirection();
app.MapMetrics();

app.Run();
