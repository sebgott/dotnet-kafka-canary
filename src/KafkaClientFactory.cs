using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace dotnet_kafka_canary;

public sealed class KafkaClients : IDisposable
{
    public KafkaClients(IProducer<string?, byte[]> producer, IConsumer<string?, byte[]> consumer)
    {
        Producer = producer;
        Consumer = consumer;
    }

    public IProducer<string?, byte[]> Producer { get; }
    public IConsumer<string?, byte[]> Consumer { get; }

    public void Dispose()
    {
        Consumer.Close();
        Consumer.Dispose();
        Producer.Dispose();
    }
}

public sealed class KafkaClientFactory
{
    private readonly KafkaClientConfigurationFactory _configurationFactory;
    private readonly KafkaOptions _options;

    public KafkaClientFactory(
        KafkaClientConfigurationFactory configurationFactory,
        IOptions<KafkaOptions> options)
    {
        _configurationFactory = configurationFactory;
        _options = options.Value;
    }

    public KafkaClients Create()
    {
        var configuration = _configurationFactory.Create();

        var producerBuilder = new ProducerBuilder<string?, byte[]>(configuration.ProducerConfig);
        if (configuration.OAuthBearerTokenRefreshHandler is not null)
        {
            producerBuilder.SetOAuthBearerTokenRefreshHandler(
                (client, config) => configuration.OAuthBearerTokenRefreshHandler(client, config));
        }

        var consumerConfig = new ConsumerConfig(configuration.ClientConfig)
        {
            GroupId = _options.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        var consumerBuilder = new ConsumerBuilder<string?, byte[]>(consumerConfig);
        if (configuration.OAuthBearerTokenRefreshHandler is not null)
        {
            consumerBuilder.SetOAuthBearerTokenRefreshHandler(
                (client, config) => configuration.OAuthBearerTokenRefreshHandler(client, config));
        }

        var producer = producerBuilder.Build();
        try
        {
            var consumer = consumerBuilder.Build();
            consumer.Subscribe(_options.Topic);
            return new KafkaClients(producer, consumer);
        }
        catch
        {
            producer.Dispose();
            throw;
        }
    }
}
