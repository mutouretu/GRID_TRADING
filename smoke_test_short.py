#!/usr/bin/env python3
import argparse
import os
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, Optional

from binance_client import BinanceAPIError, BinanceFuturesClient, decimal_to_str


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def round_step(value: Decimal, step: Decimal, up: bool = False) -> Decimal:
    if step <= 0:
        return value
    mode = ROUND_UP if up else ROUND_DOWN
    return (value / step).to_integral_value(rounding=mode) * step


def parse_symbol_filters(symbol_info: Dict[str, Any]):
    step_size = Decimal("0")
    min_qty = Decimal("0")
    min_notional = Decimal("0")

    for f in symbol_info.get("filters", []):
        ftype = f.get("filterType")
        if ftype == "LOT_SIZE":
            step_size = Decimal(str(f["stepSize"]))
            min_qty = Decimal(str(f["minQty"]))
        elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
            min_notional = Decimal(str(f.get("notional", f.get("minNotional", "0"))))

    if step_size <= 0:
        raise ValueError("invalid symbol LOT_SIZE.stepSize")

    return step_size, min_qty, min_notional


def calc_min_short_qty(mark_price: Decimal, step_size: Decimal, min_qty: Decimal, min_notional: Decimal, target_notional: Decimal) -> Decimal:
    qty_candidates = [min_qty]

    if target_notional > 0:
        qty_candidates.append(round_step(target_notional / mark_price, step_size, up=True))

    if min_notional > 0:
        qty_candidates.append(round_step(min_notional / mark_price, step_size, up=True))

    qty = max(qty_candidates)
    return round_step(qty, step_size, up=True)


def normalize_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() == "true"
    return bool(v)


def main() -> int:
    parser = argparse.ArgumentParser(description="Binance Futures connectivity and short-order smoke test")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--base-url", default="https://fapi.binance.com")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--notional", default="10", help="target USDT notional for test short order")
    parser.add_argument("--real-order", action="store_true", help="place a real MARKET short order")
    parser.add_argument(
        "--keep-position",
        action="store_true",
        help="do not auto-close the real short order after placing it",
    )
    args = parser.parse_args()

    load_env_file(args.env_file)

    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        raise RuntimeError("Missing BINANCE_API_KEY/BINANCE_API_SECRET. You can put them in .env")

    client = BinanceFuturesClient(api_key=api_key, api_secret=api_secret, base_url=args.base_url)

    print("[1/5] Checking public connectivity...")
    server_time = client.get_server_time()
    print(f"serverTime={server_time.get('serverTime')}")

    print("[2/5] Checking signed connectivity and account permissions...")
    account = client.get_account_info()
    can_trade = account.get("canTrade")
    assets = account.get("assets", [])
    usdt_asset = next((a for a in assets if a.get("asset") == "USDT"), None)
    wallet_balance = usdt_asset.get("walletBalance") if usdt_asset else "N/A"
    print(f"canTrade={can_trade}, USDT walletBalance={wallet_balance}")

    print("[3/5] Loading symbol filters and calculating minimum short quantity...")
    symbol_info = client.get_exchange_info(args.symbol)
    mark_price = client.get_mark_price(args.symbol)
    step_size, min_qty, min_notional = parse_symbol_filters(symbol_info)

    target_notional = Decimal(str(args.notional))
    qty = calc_min_short_qty(mark_price, step_size, min_qty, min_notional, target_notional)
    est_notional = qty * mark_price

    print(
        f"symbol={args.symbol}, mark={decimal_to_str(mark_price)}, qty={decimal_to_str(qty)}, "
        f"estimated_notional={decimal_to_str(est_notional)}"
    )

    pos_mode = client.get_position_mode()
    is_hedge_mode = normalize_bool(pos_mode.get("dualSidePosition"))
    position_side: Optional[str] = "SHORT" if is_hedge_mode else None

    print(f"position_mode={'HEDGE' if is_hedge_mode else 'ONE_WAY'}, order positionSide={position_side}")

    print("[4/5] Sending /order/test (no real fill)...")
    client.place_test_order(
        symbol=args.symbol,
        side="SELL",
        order_type="MARKET",
        quantity=qty,
        position_side=position_side,
    )
    print("order/test success")

    if not args.real_order:
        print("[5/5] Real order skipped (pass --real-order to place one real MARKET short order)")
        return 0

    print("[5/5] Placing real MARKET short order...")
    open_order = client.place_market_order(
        symbol=args.symbol,
        side="SELL",
        quantity=qty,
        position_side=position_side,
        client_id=f"smoke-open-{int(time.time())}",
    )
    print(
        f"OPEN success: orderId={open_order.get('orderId')}, status={open_order.get('status')}, "
        f"executedQty={open_order.get('executedQty')}"
    )

    if args.keep_position:
        print("Position kept as requested (--keep-position).")
        return 0

    print("Closing short position with MARKET BUY reduceOnly...")
    close_order = client.place_market_order(
        symbol=args.symbol,
        side="BUY",
        quantity=qty,
        position_side=position_side,
        reduce_only=True,
        client_id=f"smoke-close-{int(time.time())}",
    )
    print(
        f"CLOSE success: orderId={close_order.get('orderId')}, status={close_order.get('status')}, "
        f"executedQty={close_order.get('executedQty')}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BinanceAPIError as exc:
        print(f"Binance API error: {exc}")
        raise SystemExit(2)
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
