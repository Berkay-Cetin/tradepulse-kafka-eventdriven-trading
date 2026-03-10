namespace PricingService.Models;

public record PriceFeedEvent
{
    public string Symbol { get; init; } = default!;
    public decimal Price { get; init; }
    public decimal Change { get; init; }
    public decimal ChangePercent { get; init; }
    public long Volume { get; init; }
    public DateTime Timestamp { get; init; }
    public string EventId { get; init; } = default!;
}