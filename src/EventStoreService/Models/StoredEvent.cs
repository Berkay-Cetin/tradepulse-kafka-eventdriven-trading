namespace EventStoreService.Models;

public class StoredEvent
{
    public long Id { get; set; }
    public Guid EventId { get; set; } = Guid.NewGuid();
    public Guid AggregateId { get; set; }
    public string AggregateType { get; set; } = default!;
    public string EventType { get; set; } = default!; // "OrderPlaced", "TradeExecution"
    public int EventVersion { get; set; } = 1;
    public string Payload { get; set; } = default!;
    public string? Metadata { get; set; }
    public DateTime OccurredAt { get; set; } = DateTime.UtcNow;
    public long? KafkaOffset { get; set; }
    public int? KafkaPartition { get; set; }
    public string? KafkaTopic { get; set; }
}