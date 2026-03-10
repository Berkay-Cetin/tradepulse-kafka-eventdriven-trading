namespace OrderService.Models.Events;

// Kafka'ya publish edilecek event
public record OrderPlacedEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString();
    public string EventType { get; init; } = "OrderPlaced";
    public Guid OrderId { get; init; }
    public Guid UserId { get; init; }
    public string Symbol { get; init; } = default!;
    public string OrderType { get; init; } = default!;
    public decimal Quantity { get; init; }
    public decimal? Price { get; init; }
    public DateTime OccuredAt { get; init; } = DateTime.UtcNow;
}