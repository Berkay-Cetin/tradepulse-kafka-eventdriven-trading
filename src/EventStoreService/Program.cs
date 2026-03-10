using EventStoreService.Data;
using EventStoreService.Services;
using Microsoft.EntityFrameworkCore;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateBootstrapLogger();

var host = Host.CreateDefaultBuilder(args)
    .UseSerilog((ctx, services, config) =>
        config.ReadFrom.Configuration(ctx.Configuration))
    .ConfigureServices((ctx, services) =>
    {
        services.AddDbContextFactory<EventStoreDbContext>(opts =>
            opts.UseNpgsql(ctx.Configuration.GetConnectionString("Postgres")));

        services.AddSingleton<EventStoreWriter>();
        services.AddHostedService<EventConsumerService>();
    })
    .Build();

// Tabloları oluştur
using (var scope = host.Services.CreateScope())
{
    var factory = scope.ServiceProvider
        .GetRequiredService<IDbContextFactory<EventStoreDbContext>>();
    await using var db = await factory.CreateDbContextAsync();

    // Instead EnsureCreated, SQL — diğer tablolar varsa sorun çıkarmaz
    await db.Database.ExecuteSqlRawAsync(@"
        CREATE TABLE IF NOT EXISTS event_store (
            ""Id""             BIGSERIAL PRIMARY KEY,
            ""EventId""        UUID         NOT NULL UNIQUE DEFAULT gen_random_uuid(),
            ""AggregateId""    UUID         NOT NULL,
            ""AggregateType""  VARCHAR(100) NOT NULL,
            ""EventType""      VARCHAR(200) NOT NULL,
            ""EventVersion""   INT          NOT NULL DEFAULT 1,
            ""Payload""        JSONB        NOT NULL,
            ""Metadata""       JSONB,
            ""OccurredAt""     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            ""KafkaOffset""    BIGINT,
            ""KafkaPartition"" INT,
            ""KafkaTopic""     VARCHAR(200)
        );
        CREATE INDEX IF NOT EXISTS idx_event_store_aggregate ON event_store (""AggregateId"", ""EventVersion"");
        CREATE INDEX IF NOT EXISTS idx_event_store_type      ON event_store (""EventType"");
    ");

    Console.WriteLine("event_store tablosu hazır ✓");
}

await host.RunAsync();