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
builder.Services.AddSingleton<IKafkaOAuthTokenProvider, KafkaOAuthTokenProvider>();
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
