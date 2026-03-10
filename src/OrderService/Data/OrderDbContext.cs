using Microsoft.EntityFrameworkCore;
using OrderService.Models;

namespace OrderService.Data;

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options) { }

    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<Order>(e =>
        {
            e.ToTable("orders");
            e.HasKey(o => o.Id);
            e.Property(o => o.Symbol).HasMaxLength(20);
            e.Property(o => o.OrderType).HasMaxLength(10);
            e.Property(o => o.Status).HasMaxLength(20);
            e.Property(o => o.Quantity).HasPrecision(18, 8);
            e.Property(o => o.Price).HasPrecision(18, 4);
            e.Property(o => o.FilledQuantity).HasPrecision(18, 8);
            e.Property(o => o.FilledPrice).HasPrecision(18, 4);

            // Optimistic concurrency — aynı anda iki güncelleme çakışmasın
            e.Property(o => o.Version).IsConcurrencyToken();
        });
    }
}