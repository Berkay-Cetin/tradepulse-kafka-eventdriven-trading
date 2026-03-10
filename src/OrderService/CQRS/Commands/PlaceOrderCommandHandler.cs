using MediatR;
using OrderService.Data;
using OrderService.Models;
using OrderService.Models.Events;
using OrderService.Services;

namespace OrderService.CQRS.Commands;

public class PlaceOrderCommandHandler : IRequestHandler<PlaceOrderCommand, Order>
{
    private readonly OrderDbContext _db;
    private readonly KafkaProducerService _producer;
    private readonly ILogger<PlaceOrderCommandHandler> _logger;

    public PlaceOrderCommandHandler(
        OrderDbContext db,
        KafkaProducerService producer,
        ILogger<PlaceOrderCommandHandler> logger)
    {
        _db = db;
        _producer = producer;
        _logger = logger;
    }

    public async Task<Order> Handle(PlaceOrderCommand cmd, CancellationToken ct)
    {
        // 1. Order oluştur
        var order = new Order
        {
            UserId = cmd.UserId,
            Symbol = cmd.Symbol.ToUpper(),
            OrderType = cmd.OrderType.ToUpper(),
            Quantity = cmd.Quantity,
            Price = cmd.Price,
            Status = "PENDING"
        };

        // 2. PostgreSQL'e kaydet (Write DB)
        _db.Orders.Add(order);
        await _db.SaveChangesAsync(ct);

        _logger.LogInformation("[COMMAND] Order oluşturuldu: {OrderId} {Symbol} {Type}",
            order.Id, order.Symbol, order.OrderType);

        // 3. Kafka'ya event publish et — diğer servisler dinleyecek
        var evt = new OrderPlacedEvent
        {
            OrderId = order.Id,
            UserId = order.UserId,
            Symbol = order.Symbol,
            OrderType = order.OrderType,
            Quantity = order.Quantity,
            Price = order.Price,
        };

        await _producer.PublishOrderPlacedAsync(evt);

        return order;
    }
}