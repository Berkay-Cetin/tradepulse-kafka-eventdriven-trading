using StackExchange.Redis;
using PricingService.Models;
using System.Text.Json;

namespace PricingService.Services;

public class PriceCacheService
{
    private readonly IDatabase _redis;
    private readonly ILogger<PriceCacheService> _logger;
    private const string KEY_PREFIX = "price:";
    private const int TTL_SECONDS = 60;

    public PriceCacheService(IConnectionMultiplexer redis, ILogger<PriceCacheService> logger)
    {
        _redis = redis.GetDatabase();
        _logger = logger;
    }

    public async Task SetPriceAsync(PriceCache price)
    {
        var key = $"{KEY_PREFIX}{price.Symbol}";
        var value = JsonSerializer.Serialize(price);
        await _redis.StringSetAsync(key, value, TimeSpan.FromSeconds(TTL_SECONDS));

        // Sorted Set — tüm sembolleri score=fiyat olarak tut (leaderboard gibi)
        await _redis.SortedSetAddAsync("prices:all", price.Symbol, (double)price.CurrentPrice);
    }

    public async Task<PriceCache?> GetPriceAsync(string symbol)
    {
        var value = await _redis.StringGetAsync($"{KEY_PREFIX}{symbol}");
        if (value.IsNullOrEmpty) return null;
        return JsonSerializer.Deserialize<PriceCache>(value!);
    }

    public async Task<Dictionary<string, double>> GetAllPricesAsync()
    {
        var entries = await _redis.SortedSetRangeByRankWithScoresAsync("prices:all");
        return entries.ToDictionary(e => e.Element.ToString(), e => e.Score);
    }
}