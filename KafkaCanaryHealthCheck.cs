namespace dotnet_kafka_canary;

using Microsoft.Extensions.Diagnostics.HealthChecks;

public class KafkaCanaryHealthCheck : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var lastSuccess = KafkaCanaryService.LastSuccessUtc;

        if (lastSuccess == DateTime.MinValue)
            return Task.FromResult(HealthCheckResult.Unhealthy("No successful roundtrip yet"));

        if (DateTime.UtcNow - lastSuccess > TimeSpan.FromSeconds(30))
            return Task.FromResult(HealthCheckResult.Unhealthy("No recent successful roundtrip"));

        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
