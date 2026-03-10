using MediatR;
using Microsoft.AspNetCore.Mvc;
using OrderService.CQRS.Commands;
using OrderService.CQRS.Queries;

namespace OrderService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly IMediator _mediator;

    public OrdersController(IMediator mediator) => _mediator = mediator;

    // COMMAND — yeni emir oluştur
    [HttpPost]
    public async Task<IActionResult> PlaceOrder([FromBody] PlaceOrderCommand cmd)
    {
        var order = await _mediator.Send(cmd);
        return CreatedAtAction(nameof(GetOrder), new { id = order.Id }, order);
    }

    // QUERY — emir sorgula
    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetOrder(Guid id)
    {
        var order = await _mediator.Send(new GetOrderQuery(id));
        return order is null ? NotFound() : Ok(order);
    }
}