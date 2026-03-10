using MarketSimulator.Service.Models;
using MarketSimulator.Service.Services;

namespace MarketSimulator.Service;

public class Worker : BackgroundService
{
    private readonly KafkaProducerService _producer;
    private readonly ILogger<Worker> _logger;
    private readonly int _intervalMs;

    // Simüle edilecek hisse senetleri — başlangıç fiyatlarıyla
    private readonly Dictionary<string, decimal> _prices = new()
    {
        ["AAPL"] = 185.50m,
        ["MSFT"] = 420.00m,
        ["GOOGL"] = 175.00m,
        ["AMZN"] = 195.00m,
        ["NVDA"] = 875.00m,
        ["TSLA"] = 210.00m,
    };

    private readonly Random _random = new();

    public Worker(KafkaProducerService producer, IConfiguration config, ILogger<Worker> logger)
    {
        _producer = producer;
        _logger = logger;
        _intervalMs = config.GetValue<int>("Kafka:IntervalMs", 500);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("MarketSimulator başladı — {Count} sembol yayınlanıyor", _prices.Count);

        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var symbol in _prices.Keys)
            {
                var oldPrice = _prices[symbol];
                var newPrice = SimulatePrice(oldPrice);
                _prices[symbol] = newPrice;

                var change = newPrice - oldPrice;
                var changePercent = Math.Round(change / oldPrice * 100, 4);

                var evt = new PriceFeedEvent
                {
                    Symbol = symbol,
                    Price = newPrice,
                    Change = change,
                    ChangePercent = changePercent,
                    Volume = _random.NextInt64(100_000, 10_000_000),
                    Timestamp = DateTime.UtcNow,
                };

                await _producer.PublishAsync(evt);
            }

            await Task.Delay(_intervalMs, stoppingToken);
        }
    }

    // Gerçekçi fiyat simülasyonu — Geometric Brownian Motion (GBM)
    // Gerçek fintech'te kullanılan model!
    private decimal SimulatePrice(decimal currentPrice)
    {
        const double drift = 0.0001;  // küçük yukarı eğilim
        const double volatility = 0.002;  // %0.2 volatilite

        var randomShock = _random.NextDouble() * 2 - 1; // -1 ile 1 arası
        var changeRatio = 1 + drift + volatility * randomShock;

        return Math.Round(currentPrice * (decimal)changeRatio, 2);
    }
}