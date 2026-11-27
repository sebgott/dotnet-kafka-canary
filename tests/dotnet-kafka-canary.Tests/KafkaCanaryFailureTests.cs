using System.Reflection;
using System.Text;
using Confluent.Kafka;
using dotnet_kafka_canary.Domain.CloudEvents;
using dotnet_kafka_canary.Domain.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace dotnet_kafka_canary.Tests;

public sealed class KafkaCanaryFailureTests
{
    private const string Topic = "canary-topic";
    private const string MessageId = "7d1c4a8e-0f53-4a8e-9c7b-2d8f1e5a6b3c";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ProduceFailuresLogMessageIdAndReason(bool kafkaError)
    {
        var exception = kafkaError
            ? new ProduceException<string?, byte[]>(new Error(ErrorCode.Local_MsgTimedOut, "delivery timed out"),
                new DeliveryResult<string?, byte[]>())
            : (Exception)new InvalidOperationException("delivery\nfailed");
        var before = PrometheusMetrics.Failures.WithLabels(FailureStage.Produce).Value;
        var entry = await RunUntilFailure(
            (_, _) => throw exception,
            (_, args) => WaitForCancellation((CancellationToken)args![0]!));

        Assert.Contains("stage=produce", entry.Message);
        Assert.Matches(@"message_id=[0-9a-f-]{36}", entry.Message);
        Assert.Contains($"topic={Topic}", entry.Message);
        Assert.Contains(kafkaError ? "error_code=Local_MsgTimedOut" : "reason=delivery failed", entry.Message);
        Assert.DoesNotContain('\n', entry.Message);
        Assert.Same(exception, entry.Exception);
        Assert.Equal(before + 1, PrometheusMetrics.Failures.WithLabels(FailureStage.Produce).Value);
    }

    [Theory]
    [InlineData("record")]
    [InlineData("no_record")]
    [InlineData("unexpected")]
    public async Task ConsumeFailuresLogAvailableRecordDetails(string scenario)
    {
        var hasRecord = scenario == "record";
        var record = hasRecord ? new ConsumeResult<byte[], byte[]>
        {
            Topic = Topic,
            Partition = new Partition(2),
            Offset = new Offset(42),
            Message = new Message<byte[], byte[]> { Headers = HeadersWithId() }
        } : null;
        var exception = scenario == "unexpected"
            ? (Exception)new InvalidOperationException("consumer unavailable")
            : new ConsumeException(record, new Error(ErrorCode.Local_KeyDeserialization, "bad key"));
        var failed = false;
        var before = PrometheusMetrics.Failures.WithLabels(FailureStage.Consume).Value;
        var entry = await RunUntilFailure(
            (_, args) => WaitForProduceCancellation((CancellationToken)args![2]!),
            (_, args) =>
            {
                if (!failed)
                {
                    failed = true;
                    throw exception;
                }
                return WaitForCancellation((CancellationToken)args![0]!);
            });

        Assert.Contains("stage=consume", entry.Message);
        Assert.Contains($"message_id={(hasRecord ? MessageId : "unknown")}", entry.Message);
        Assert.Contains($"topic={Topic}", entry.Message);
        Assert.Contains(hasRecord ? "partition=2 offset=42" : "partition=unknown offset=unknown", entry.Message);
        Assert.Contains(scenario == "unexpected" ? "reason=consumer unavailable" : "reason=bad key", entry.Message);
        Assert.Equal(before + 1, PrometheusMetrics.Failures.WithLabels(FailureStage.Consume).Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ProcessAndCommitFailuresLogMessageAndLocation(bool malformed)
    {
        var message = CanaryCloudEvents.ToKafkaMessage(new CanaryMessage(
            "instance", Guid.Parse(MessageId), DateTime.UtcNow));
        if (malformed)
            message.Value = Encoding.UTF8.GetBytes("invalid json");

        var result = new ConsumeResult<string?, byte[]>
        {
            Topic = Topic,
            Partition = new Partition(3),
            Offset = new Offset(99),
            Message = message
        };
        var consumed = false;
        var before = PrometheusMetrics.Failures.WithLabels(FailureStage.Process).Value;
        var entry = await RunUntilFailure(
            (_, args) => WaitForProduceCancellation((CancellationToken)args![2]!),
            (method, args) =>
            {
                if (method.Name == nameof(IConsumer<string?, byte[]>.Commit))
                    throw new InvalidOperationException("commit failed");
                if (!consumed)
                {
                    consumed = true;
                    return result;
                }
                return WaitForCancellation((CancellationToken)args![0]!);
            });

        Assert.Contains("stage=process", entry.Message);
        Assert.Contains($"message_id={MessageId}", entry.Message);
        Assert.Contains($"topic={Topic} partition=3 offset=99", entry.Message);
        if (!malformed)
            Assert.Contains("reason=commit failed", entry.Message);
        else
            Assert.DoesNotContain("error_type=InvalidOperationException", entry.Message);
        Assert.Equal(before + 1, PrometheusMetrics.Failures.WithLabels(FailureStage.Process).Value);
    }

    private static async Task<LogEntry> RunUntilFailure(
        Func<MethodInfo, object?[]?, object?> producer,
        Func<MethodInfo, object?[]?, object?> consumer)
    {
        var logger = new FailureLogger();
        var producerProxy = DispatchProxy.Create<IProducer<string?, byte[]>, KafkaProxy>();
        ((KafkaProxy)(object)producerProxy).Handler = producer;
        var consumerProxy = DispatchProxy.Create<IConsumer<string?, byte[]>, KafkaProxy>();
        ((KafkaProxy)(object)consumerProxy).Handler = consumer;
        using var service = new KafkaCanaryService(logger, Options.Create(new KafkaOptions
        {
            Topic = Topic,
            ProduceIntervalMs = 60_000
        }), new KafkaClients(producerProxy, consumerProxy));

        await service.StartAsync(CancellationToken.None);
        try
        {
            return await logger.Failure.Task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    private static Headers HeadersWithId() => new() { { "ce_id", Encoding.UTF8.GetBytes(MessageId) } };

    private static object WaitForCancellation(CancellationToken token)
    {
        token.WaitHandle.WaitOne();
        throw new OperationCanceledException(token);
    }

    private static async Task<DeliveryResult<string?, byte[]>> WaitForProduceCancellation(CancellationToken token)
    {
        await Task.Delay(Timeout.Infinite, token);
        throw new InvalidOperationException("Unreachable");
    }

    public class KafkaProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Handler { get; set; } = null!;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
            Handler(targetMethod!, args);
    }

    private sealed record LogEntry(string Message, Exception? Exception);

    private sealed class FailureLogger : ILogger<KafkaCanaryService>
    {
        public TaskCompletionSource<LogEntry> Failure { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (message.StartsWith("Canary failure:", StringComparison.Ordinal))
                Failure.TrySetResult(new LogEntry(message, exception));
        }
    }
}
