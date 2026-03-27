import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.parse import urlencode

try:
    import requests
except ModuleNotFoundError:
    requests = None


class BinanceAPIError(RuntimeError):
    pass


def decimal_to_str(v: Decimal) -> str:
    return format(v.normalize(), "f")


class BinanceFuturesClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://fapi.binance.com") -> None:
        if requests is None:
            raise RuntimeError("Missing dependency: requests. Install with `pip install -r requirements.txt`.")
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"X-MBX-APIKEY": api_key})

    def _signed_params(self, params: Dict[str, str]) -> Dict[str, str]:
        params = dict(params)
        params["timestamp"] = str(int(time.time() * 1000))
        query = urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, str]] = None,
        signed: bool = False,
    ) -> Any:
        params = params or {}
        if signed:
            if not self.api_key or not self.api_secret:
                raise BinanceAPIError("signed endpoint requires BINANCE_API_KEY and BINANCE_API_SECRET")
            params = self._signed_params(params)

        url = f"{self.base_url}{path}"
        resp = self.session.request(method=method, url=url, params=params, timeout=10)
        if resp.status_code >= 400:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise BinanceAPIError(f"HTTP {resp.status_code} {path} failed: {detail}")

        data = resp.json()
        if isinstance(data, dict) and data.get("code", 0) not in (0, None):
            raise BinanceAPIError(f"API {path} failed: {data}")
        return data

    def get_exchange_info(self, symbol: str) -> Dict[str, Any]:
        info = self._request("GET", "/fapi/v1/exchangeInfo")
        for entry in info.get("symbols", []):
            if entry.get("symbol") == symbol:
                return entry
        raise BinanceAPIError(f"symbol not found in exchangeInfo: {symbol}")

    def get_server_time(self) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v1/time")

    def get_position_mode(self) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v1/positionSide/dual", signed=True)

    def get_account_info(self) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v2/account", signed=True)

    def get_mark_price(self, symbol: str) -> Decimal:
        data = self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return Decimal(str(data["markPrice"]))

    def set_hedge_mode(self, enabled: bool) -> None:
        value = "true" if enabled else "false"
        self._request("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": value}, signed=True)

    def set_leverage(self, symbol: str, leverage: int) -> None:
        self._request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": str(leverage)}, signed=True)

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        quantity: Decimal,
        price: Decimal,
        client_id: str,
    ) -> int:
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": decimal_to_str(quantity),
            "price": decimal_to_str(price),
            "newClientOrderId": client_id,
        }
        data = self._request("POST", "/fapi/v1/order", params=params, signed=True)
        return int(data["orderId"])

    def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        position_side: Optional[str] = None,
        reduce_only: bool = False,
        client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": decimal_to_str(quantity),
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        if client_id:
            params["newClientOrderId"] = client_id
        return self._request("POST", "/fapi/v1/order", params=params, signed=True)

    def place_test_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        position_side: Optional[str] = None,
        reduce_only: bool = False,
    ) -> None:
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": decimal_to_str(quantity),
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        self._request("POST", "/fapi/v1/order/test", params=params, signed=True)

    def get_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return self._request(
            "GET",
            "/fapi/v1/order",
            {"symbol": symbol, "orderId": str(order_id)},
            signed=True,
        )

    def get_open_orders(self, symbol: str) -> Any:
        return self._request(
            "GET",
            "/fapi/v1/openOrders",
            {"symbol": symbol},
            signed=True,
        )

    def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return self._request(
            "DELETE",
            "/fapi/v1/order",
            {"symbol": symbol, "orderId": str(order_id)},
            signed=True,
        )
