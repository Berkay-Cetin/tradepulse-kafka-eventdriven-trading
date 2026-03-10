using Confluent.Kafka;
using EventStoreService.Models;
using System.Text.Json;

namespace EventStoreService.Services;

public class EventConsumerService : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly EventStoreWriter _writer;
    private readonly ILogger<EventConsumerService> _logger;
    private readonly List<string> _topics;

    public EventConsumerService(
        IConfiguration config,
        EventStoreWriter writer,
        ILogger<EventConsumerService> logger)
    {
        _writer = writer;
        _logger = logger;
        _topics = new List<string>
        {
            config["Kafka:Topics:Orders"]!,
            config["Kafka:Topics:Executions"]!
        };

        _consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            GroupId = config["Kafka:ConsumerGroup"],
            AutoOffsetReset = AutoOffsetReset.Earliest, // en baştan — hiçbir event kaçırma
            EnableAutoCommit = false,
        })
        .SetErrorHandler((_, e) => logger.LogError("Consumer hatası: {Reason}", e.Reason))
        .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Birden fazla topic'i tek consumer ile dinle
        _consumer.Subscribe(_topics);
        _logger.LogInformation("EventStore dinleniyor → [{Topics}]", string.Join(", ", _topics));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result is null) continue;

                await ProcessAsync(result);
                _consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Consume hatası: {Reason}", ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _consumer.Close();
    }

    private async Task ProcessAsync(ConsumeResult<string, string> result)
    {
        // Topic'e göre aggregate type belirle
        var (aggregateType, eventType) = result.Topic switch
        {
            var t when t.Contains("orders") => ("Order", "OrderPlaced"),
            var t when t.Contains("executions") => ("Trade", "TradeExecution"),
            _ => ("Unknown", "Unknown")
        };

        // Payload'dan aggregateId çek
        var aggregateId = ExtractAggregateId(result.Message.Value, aggregateType);

        var evt = new StoredEvent
        {
            EventId = Guid.NewGuid(),
            AggregateId = aggregateId,
            AggregateType = aggregateType,
            EventType = eventType,
            Payload = result.Message.Value,
            Metadata = JsonSerializer.Serialize(new
            {
                Key = result.Message.Key,
                Timestamp = DateTime.UtcNow
            }),
            OccurredAt = DateTime.UtcNow,
            KafkaOffset = result.Offset.Value,
            KafkaPartition = result.Partition.Value,
            KafkaTopic = result.Topic
        };

        await _writer.AppendAsync(evt);
    }

    private static Guid ExtractAggregateId(string json, string aggregateType)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // OrderId veya UserId'yi bul
            if (root.TryGetProperty("orderId", out var orderId))
                return orderId.GetGuid();
            if (root.TryGetProperty("OrderId", out var orderId2))
                return orderId2.GetGuid();
        }
        catch { /* parse hatası → random id */ }

        return Guid.NewGuid();
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}