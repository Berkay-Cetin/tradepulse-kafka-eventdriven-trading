namespace PricingService.Models;

public record TradeExecution
{
    public string ExecutionId { get; init; } = Guid.NewGuid().ToString();
    public string OrderId { get; init; } = default!;
    public string Symbol { get; init; } = default!;
    public decimal Quantity { get; init; }
    public decimal Price { get; init; }
    public string Side { get; init; } = default!; // BUY | SELL
    public DateTime ExecutedAt { get; init; } = DateTime.UtcNow;
}