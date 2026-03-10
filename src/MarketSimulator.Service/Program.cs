using MarketSimulator.Service;
using MarketSimulator.Service.Services;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateBootstrapLogger();

var host = Host.CreateDefaultBuilder(args)
    .UseSerilog((ctx, services, config) => config
        .ReadFrom.Configuration(ctx.Configuration))
    .ConfigureServices(services =>
    {
        services.AddSingleton<KafkaProducerService>();
        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();