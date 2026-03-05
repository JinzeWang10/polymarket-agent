import httpx
import respx

from src.api.clob import ClobClient

BASE = "https://clob.polymarket.com"


@respx.mock
def test_get_prices():
    respx.get(f"{BASE}/prices").mock(
        return_value=httpx.Response(200, json={"tok1": "0.55", "tok2": "0.45"})
    )
    client = ClobClient(BASE, httpx.Client())
    prices = client.get_prices(["tok1", "tok2"])
    assert prices == {"tok1": 0.55, "tok2": 0.45}


@respx.mock
def test_get_prices_empty():
    client = ClobClient(BASE, httpx.Client())
    assert client.get_prices([]) == {}


@respx.mock
def test_get_order_book():
    book = {
        "market": "tok1",
        "asset_id": "tok1",
        "bids": [{"price": "0.55", "size": "100"}],
        "asks": [{"price": "0.56", "size": "50"}],
    }
    respx.get(f"{BASE}/book").mock(return_value=httpx.Response(200, json=book))
    client = ClobClient(BASE, httpx.Client())
    result = client.get_order_book("tok1")
    assert result["bids"][0]["price"] == "0.55"
    assert result["asks"][0]["price"] == "0.56"
