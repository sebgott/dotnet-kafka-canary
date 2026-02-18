using dotnet_kafka_canary;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHostedService<KafkaCanaryService>();

builder.Services
    .AddHealthChecks()
    .AddCheck<KafkaCanaryHealthCheck>("kafka_canary");

var app = builder.Build();

app.MapHealthChecks("/health/ready");
app.UseHttpsRedirection();
app.MapMetrics();

app.Run();