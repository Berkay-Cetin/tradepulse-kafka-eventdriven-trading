using EventStoreService.Data;
using EventStoreService.Models;
using Microsoft.EntityFrameworkCore;

namespace EventStoreService.Services;

public class EventStoreWriter
{
    private readonly IDbContextFactory<EventStoreDbContext> _dbFactory;
    private readonly ILogger<EventStoreWriter> _logger;

    public EventStoreWriter(IDbContextFactory<EventStoreDbContext> dbFactory, ILogger<EventStoreWriter> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    public async Task AppendAsync(StoredEvent evt)
    {
        await using var db = await _dbFactory.CreateDbContextAsync();

        // Aynı EventId daha önce yazıldıysa atla — idempotent
        var exists = await db.Events.AnyAsync(e => e.EventId == evt.EventId);
        if (exists)
        {
            _logger.LogWarning("[SKIP] Duplicate event: {EventId}", evt.EventId);
            return;
        }

        db.Events.Add(evt);
        await db.SaveChangesAsync();

        _logger.LogInformation(
            "[STORE] {EventType} | Aggregate: {AggregateId} | Offset: {Offset}",
            evt.EventType, evt.AggregateId, evt.KafkaOffset);
    }

    public async Task<List<StoredEvent>> GetAggregateHistoryAsync(Guid aggregateId)
    {
        await using var db = await _dbFactory.CreateDbContextAsync();
        return await db.Events
            .Where(e => e.AggregateId == aggregateId)
            .OrderBy(e => e.EventVersion)
            .AsNoTracking()
            .ToListAsync();
    }
}