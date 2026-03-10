namespace PricingService.Models;

public record PriceCache
{
    public string Symbol { get; init; } = default!;
    public decimal CurrentPrice { get; init; }
    public decimal PreviousPrice { get; init; }
    public decimal Change { get; init; }
    public decimal ChangePercent { get; init; }
    public DateTime UpdatedAt { get; init; }
}