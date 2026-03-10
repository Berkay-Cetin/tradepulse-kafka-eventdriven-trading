using Confluent.Kafka;
using MarketSimulator.Service.Models;
using System.Text.Json;

namespace MarketSimulator.Service.Services;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _topic = config["Kafka:Topic"]!;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],

            // EXACTLY-ONCE için önemli ayarlar
            Acks = Acks.All,       // tüm replica'lar onaylasın
            EnableIdempotence = true,           // duplicate mesaj engelleyelim
            MessageTimeoutMs = 10000,
            RetryBackoffMs = 500,
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task PublishAsync(PriceFeedEvent priceFeed)
    {
        var message = new Message<string, string>
        {
            Key = priceFeed.Symbol,                        // aynı sembol → aynı partition
            Value = JsonSerializer.Serialize(priceFeed),
            Headers = new Headers
            {
                { "event-type", "PriceFeedEvent"u8.ToArray() },
                { "source",     "MarketSimulator"u8.ToArray() }
            }
        };

        var result = await _producer.ProduceAsync(_topic, message);

        _logger.LogInformation(
            "[KAFKA] {Symbol} → ${Price} | Partition: {Partition} | Offset: {Offset}",
            priceFeed.Symbol, priceFeed.Price,
            result.Partition.Value, result.Offset.Value);
    }

    public void Dispose() => _producer?.Dispose();
}