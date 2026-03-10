using Microsoft.AspNetCore.Mvc;
using PricingService.Services;

namespace PricingService.Controllers;

[ApiController]
[Route("api/[controller]")]
public class PricesController : ControllerBase
{
    private readonly PriceCacheService _cache;

    public PricesController(PriceCacheService cache) => _cache = cache;

    [HttpGet("{symbol}")]
    public async Task<IActionResult> GetPrice(string symbol)
    {
        var price = await _cache.GetPriceAsync(symbol.ToUpper());
        return price is null ? NotFound() : Ok(price);
    }

    [HttpGet]
    public async Task<IActionResult> GetAllPrices()
    {
        var prices = await _cache.GetAllPricesAsync();
        return Ok(prices);
    }
}