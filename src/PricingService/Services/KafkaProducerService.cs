using Confluent.Kafka;
using PricingService.Models;
using System.Text.Json;

namespace PricingService.Services;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _executionsTopic;
    private readonly string _deadLetterTopic;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _executionsTopic = config["Kafka:Topics:Executions"]!;
        _deadLetterTopic = config["Kafka:Topics:DeadLetter"]!;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            Acks = Acks.All,
            EnableIdempotence = true,
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task PublishExecutionAsync(TradeExecution execution)
    {
        var message = new Message<string, string>
        {
            Key = execution.Symbol,
            Value = JsonSerializer.Serialize(execution),
            Headers = new Headers
            {
                { "event-type", "TradeExecution"u8.ToArray() },
                { "source",     "PricingService"u8.ToArray() }
            }
        };

        var result = await _producer.ProduceAsync(_executionsTopic, message);
        _logger.LogInformation(
            "[PUBLISH] TradeExecution {Symbol} {Side} @${Price} | P:{Partition} O:{Offset}",
            execution.Symbol, execution.Side, execution.Price,
            result.Partition.Value, result.Offset.Value);
    }

    public async Task PublishToDeadLetterAsync(string key, string value, string originalTopic)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value,
            Headers = new Headers
            {
                { "original-topic", System.Text.Encoding.UTF8.GetBytes(originalTopic) },
                { "failed-at",      System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) }
            }
        };

        await _producer.ProduceAsync(_deadLetterTopic, message);
        _logger.LogWarning("[DLQ] Mesaj dead-letter'a gönderildi. Key: {Key}", key);
    }

    public void Dispose() => _producer?.Dispose();
}