using MediatR;
using OrderService.Models;

namespace OrderService.CQRS.Commands;

public record PlaceOrderCommand : IRequest<Order>
{
    public Guid UserId { get; init; }
    public string Symbol { get; init; } = default!;
    public string OrderType { get; init; } = default!;
    public decimal Quantity { get; init; }
    public decimal? Price { get; init; }
}