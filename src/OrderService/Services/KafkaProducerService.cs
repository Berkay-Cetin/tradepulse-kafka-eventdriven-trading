using Confluent.Kafka;
using OrderService.Models.Events;
using System.Text.Json;

namespace OrderService.Services;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _ordersTopic;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _ordersTopic = config["Kafka:Topics:Orders"]!;

        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            Acks = Acks.All,
            EnableIdempotence = true,
        }).Build();
    }

    public async Task PublishOrderPlacedAsync(OrderPlacedEvent evt)
    {
        var message = new Message<string, string>
        {
            Key = evt.UserId.ToString(),   // user_id → aynı kullanıcı emirleri sıralı
            Value = JsonSerializer.Serialize(evt),
            Headers = new Headers
            {
                { "event-type", "OrderPlaced"u8.ToArray() },
                { "source",     "OrderService"u8.ToArray() }
            }
        };

        var result = await _producer.ProduceAsync(_ordersTopic, message);

        _logger.LogInformation(
            "[KAFKA] OrderPlaced published → {Symbol} {Type} | P:{Partition} O:{Offset}",
            evt.Symbol, evt.OrderType, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose() => _producer?.Dispose();
}