using MediatR;
using OrderService.Models;

namespace OrderService.CQRS.Queries;

public record GetOrderQuery(Guid OrderId) : IRequest<Order?>;