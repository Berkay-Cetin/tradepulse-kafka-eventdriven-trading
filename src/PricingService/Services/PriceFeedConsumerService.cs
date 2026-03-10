using Confluent.Kafka;
using PricingService.Models;
using System.Text.Json;

namespace PricingService.Services;

public class PriceFeedConsumerService : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly PriceCacheService _cache;
    private readonly KafkaProducerService _producer;
    private readonly ILogger<PriceFeedConsumerService> _logger;
    private readonly string _topic;
    private readonly string _deadLetterTopic;

    public PriceFeedConsumerService(
        IConfiguration config,
        PriceCacheService cache,
        KafkaProducerService producer,
        ILogger<PriceFeedConsumerService> logger)
    {
        _cache = cache;
        _producer = producer;
        _logger = logger;
        _topic = config["Kafka:Topics:PriceFeed"]!;
        _deadLetterTopic = config["Kafka:Topics:DeadLetter"]!;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            GroupId = config["Kafka:ConsumerGroup"],

            // En önemli ayar: ne zaman başlayacak?
            // Earliest → topic'in en başından (yeni group ilk kez başladığında)
            // Latest   → sadece yeni mesajları al
            AutoOffsetReset = AutoOffsetReset.Latest,

            // Manuel commit — işledikten SONRA commit et (veri kaybı önlenir)
            EnableAutoCommit = false,

            // Heartbeat — broker'a "hala hayattayım" sinyali
            HeartbeatIntervalMs = 3000,
            SessionTimeoutMs = 30000,
            MaxPollIntervalMs = 300000,
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) => logger.LogError("Kafka Consumer Hatası: {Reason}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                logger.LogInformation("Partition atandı: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                logger.LogWarning("Partition geri alındı: [{Partitions}]",
                    string.Join(", ", partitions.Select(p => p.Partition.Value)));
            })
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation("PricingService başladı → topic: {Topic}", _topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Non-blocking consume — 100ms timeout
                var result = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result is null) continue;

                await ProcessMessageAsync(result);

                // Manuel commit — mesajı başarıyla işledikten sonra
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

    private async Task ProcessMessageAsync(ConsumeResult<string, string> result)
    {
        try
        {
            var evt = JsonSerializer.Deserialize<PriceFeedEvent>(result.Message.Value);
            if (evt is null) return;

            _logger.LogInformation(
                "[CONSUME] {Symbol} ${Price} ({Change:+0.00;-0.00}%) | P:{Partition} O:{Offset}",
                evt.Symbol, evt.Price, evt.ChangePercent,
                result.Partition.Value, result.Offset.Value);

            // Redis'e yaz — Read Model güncelle
            var previous = await _cache.GetPriceAsync(evt.Symbol);
            await _cache.SetPriceAsync(new PriceCache
            {
                Symbol = evt.Symbol,
                CurrentPrice = evt.Price,
                PreviousPrice = previous?.CurrentPrice ?? evt.Price,
                Change = evt.Change,
                ChangePercent = evt.ChangePercent,
                UpdatedAt = DateTime.UtcNow,
            });

            // Büyük fiyat hareketi varsa trade execution simüle et (%0.5+)
            if (Math.Abs(evt.ChangePercent) >= 0.5m)
            {
                await _producer.PublishExecutionAsync(new TradeExecution
                {
                    OrderId = Guid.NewGuid().ToString(),
                    Symbol = evt.Symbol,
                    Quantity = 100,
                    Price = evt.Price,
                    Side = evt.Change > 0 ? "BUY" : "SELL",
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Mesaj işlenemedi, DLQ'ya gönderiliyor");
            await SendToDeadLetterAsync(result);
        }
    }

    private async Task SendToDeadLetterAsync(ConsumeResult<string, string> result)
    {
        await _producer.PublishToDeadLetterAsync(
            key: result.Message.Key,
            value: result.Message.Value,
            originalTopic: _topic
        );
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}