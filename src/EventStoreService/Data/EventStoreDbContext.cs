using EventStoreService.Models;
using Microsoft.EntityFrameworkCore;

namespace EventStoreService.Data;

public class EventStoreDbContext : DbContext
{
    public EventStoreDbContext(DbContextOptions<EventStoreDbContext> options) : base(options) { }

    public DbSet<StoredEvent> Events => Set<StoredEvent>();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<StoredEvent>(e =>
        {
            e.ToTable("event_store");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).UseIdentityColumn();
            e.Property(x => x.AggregateType).HasMaxLength(100);
            e.Property(x => x.EventType).HasMaxLength(200);
            e.Property(x => x.Payload).HasColumnType("jsonb");
            e.Property(x => x.Metadata).HasColumnType("jsonb");
            e.HasIndex(x => new { x.AggregateId, x.EventVersion });
            e.HasIndex(x => x.EventType);
        });
    }
}