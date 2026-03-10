using MediatR;
using OrderService.Data;
using OrderService.Models;
using Microsoft.EntityFrameworkCore;

namespace OrderService.CQRS.Queries;

public class GetOrderQueryHandler : IRequestHandler<GetOrderQuery, Order?>
{
    private readonly OrderDbContext _db;

    public GetOrderQueryHandler(OrderDbContext db) => _db = db;

    public async Task<Order?> Handle(GetOrderQuery query, CancellationToken ct)
    {
        // Query — sadece okur, asla write DB'yi değiştirmez
        return await _db.Orders
            .AsNoTracking()     // EF tracking kapalı — read için gereksiz
            .FirstOrDefaultAsync(o => o.Id == query.OrderId, ct);
    }
}