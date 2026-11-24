using Azure.Identity;
using Confluent.Kafka;
using dotnet_kafka_canary;
using Microsoft.Extensions.Options;
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
    .AddHealthChecks()
    .AddCheck<KafkaCanaryHealthCheck>("kafka_canary");

var app = builder.Build();

app.MapHealthChecks("/health/ready");
app.UseHttpsRedirection();
app.MapMetrics();

app.Run();
