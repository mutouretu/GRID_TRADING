#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Optional

from binance_client import BinanceFuturesClient
from dual_trigger_grid import DualTriggerGrid, load_config, parse_filters, validate_config


def load_env_file(path: str) -> bool:
    if not os.path.exists(path):
        return False
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
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Binance Futures single-direction boundary-trigger grid strategy")
    parser.add_argument("--config", default="config.json", help="Path to JSON config")
    parser.add_argument("--base-url", default="https://fapi.binance.com", help="Binance Futures API base URL")
    parser.add_argument("--env-file", default=".env", help="Env file path, fallback to .env.example when missing")
    args = parser.parse_args()

    env_loaded: Optional[str] = None
    if load_env_file(args.env_file):
        env_loaded = args.env_file
    elif args.env_file == ".env" and load_env_file(".env.example"):
        env_loaded = ".env.example"

    cfg = load_config(args.config)
    validate_config(cfg)

    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        if env_loaded:
            print(f"Loaded env from {env_loaded}, but key vars are still missing", file=sys.stderr)
        print("Missing BINANCE_API_KEY/BINANCE_API_SECRET env vars", file=sys.stderr)
        return 1

    client = BinanceFuturesClient(api_key=api_key, api_secret=api_secret, base_url=args.base_url)
    symbol_info = client.get_exchange_info(cfg.symbol)
    filters = parse_filters(symbol_info)

    bot = DualTriggerGrid(client=client, cfg=cfg, filters=filters)
    bot.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
