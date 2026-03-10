namespace OrderService.Models;

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public Guid UserId { get; set; }
    public string Symbol { get; set; } = default!;
    public string OrderType { get; set; } = default!; // BUY | SELL
    public decimal Quantity { get; set; }
    public decimal? Price { get; set; }             // null = market order
    public string Status { get; set; } = "PENDING";
    public decimal FilledQuantity { get; set; } = 0;
    public decimal? FilledPrice { get; set; }
    public int Version { get; set; } = 1;        // optimistic concurrency
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}