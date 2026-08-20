using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace dotnet_kafka_canary;

public sealed class KafkaClients : IDisposable
{
    public KafkaClients(IProducer<string, string> producer, IConsumer<string, string> consumer)
    {
        Producer = producer;
        Consumer = consumer;
    }

    public IProducer<string, string> Producer { get; }
    public IConsumer<string, string> Consumer { get; }

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

        var producerBuilder = new ProducerBuilder<string, string>(configuration.ClientConfig);
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
        var consumerBuilder = new ConsumerBuilder<string, string>(consumerConfig);
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
