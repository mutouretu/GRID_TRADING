import csv
import hashlib
import json
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, Optional, Tuple

from binance_client import BinanceAPIError, BinanceFuturesClient, decimal_to_str

getcontext().prec = 28


@dataclass
class Filters:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal


@dataclass
class CellState:
    idx: int
    lower: Decimal
    upper: Decimal

    long_armed: bool = False
    short_armed: bool = False

    long_entry: Optional[int] = None
    long_exit: Optional[int] = None
    short_entry: Optional[int] = None
    short_exit: Optional[int] = None


@dataclass
class StrategyConfig:
    symbol: str
    grids: int
    grid_ratio: Decimal
    order_qty: Decimal
    leverage: int
    mode: str
    poll_interval_sec: float
    status_interval_sec: float
    csv_path: str
    strategy_id: str
    lower_price: Optional[Decimal] = None
    upper_price: Optional[Decimal] = None


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def round_down(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


class CsvJournal:
    FIELDS = [
        "timestamp",
        "event",
        "symbol",
        "mode",
        "cell_idx",
        "direction",
        "order_role",
        "order_id",
        "client_order_id",
        "price",
        "qty",
        "status",
        "pnl_usdt",
        "total_trades",
        "total_profit_usdt",
        "note",
    ]

    def __init__(self, path: str) -> None:
        self.path = path
        self._ensure_header()

    def _ensure_header(self) -> None:
        dirpath = os.path.dirname(self.path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDS)
                writer.writeheader()

    def append(self, row: Dict[str, str]) -> None:
        payload = {k: "" for k in self.FIELDS}
        payload.update(row)
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDS)
            writer.writerow(payload)

    def load_totals(self) -> Tuple[int, Decimal]:
        if not os.path.exists(self.path):
            return 0, Decimal("0")

        trades = 0
        profit = Decimal("0")
        with open(self.path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("event") != "CYCLE_CLOSED":
                    continue
                trades += 1
                pnl = row.get("pnl_usdt", "0") or "0"
                profit += Decimal(str(pnl))
        return trades, profit


def parse_filters(symbol_info: Dict[str, object]) -> Filters:
    tick_size = Decimal("0.0")
    step_size = Decimal("0.0")
    min_qty = Decimal("0.0")
    min_notional = Decimal("0.0")

    for f in symbol_info.get("filters", []):
        ftype = f.get("filterType")
        if ftype == "PRICE_FILTER":
            tick_size = Decimal(str(f["tickSize"]))
        elif ftype == "LOT_SIZE":
            step_size = Decimal(str(f["stepSize"]))
            min_qty = Decimal(str(f["minQty"]))
        elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
            min_notional = Decimal(str(f.get("notional", f.get("minNotional", "0"))))

    if tick_size <= 0 or step_size <= 0:
        raise ValueError("symbol filters are missing valid tickSize/stepSize")

    return Filters(tick_size=tick_size, step_size=step_size, min_qty=min_qty, min_notional=min_notional)


def build_cells(cfg: StrategyConfig, tick_size: Decimal):
    growth = Decimal("1") + cfg.grid_ratio
    if growth <= Decimal("1"):
        raise ValueError("grid_ratio must be > 0")

    cells = []
    if cfg.mode == "short":
        if cfg.upper_price is None:
            raise ValueError("mode=short requires upper_price")
        upper = round_down(cfg.upper_price, tick_size)
        lower = round_down(upper / growth, tick_size)
        if lower >= upper:
            lower = upper - tick_size
        if lower <= 0:
            raise ValueError("invalid short anchor price for initial cell")
        cells.append(CellState(idx=0, lower=lower, upper=upper))
        return cells

    if cfg.mode == "long":
        if cfg.lower_price is None:
            raise ValueError("mode=long requires lower_price")
        lower = round_down(cfg.lower_price, tick_size)
        upper = round_down(lower * growth, tick_size)
        if upper <= lower:
            upper = lower + tick_size
        cells.append(CellState(idx=0, lower=lower, upper=upper))
        return cells

    if cfg.lower_price is None or cfg.upper_price is None:
        raise ValueError("mode=both requires lower_price and upper_price")
    if cfg.upper_price <= cfg.lower_price:
        raise ValueError("upper_price must be larger than lower_price")

    lower_anchor = cfg.lower_price
    levels = [lower_anchor]
    while levels[-1] < cfg.upper_price:
        levels.append(levels[-1] * growth)

    for i in range(len(levels) - 1):
        lo = round_down(levels[i], tick_size)
        up = round_down(levels[i + 1], tick_size)
        if up <= lo:
            up = lo + tick_size
        cells.append(CellState(idx=i, lower=lo, upper=up))
    return cells


def parse_client_order_id(client_order_id: str) -> Optional[Tuple[str, str, str, int]]:
    parts = client_order_id.split("-")
    if parts[0] != "dtg":
        return None

    # New format: dtg-{strategy_tag}-{side_tag}-{role_tag}-{idx}-{ts}
    if len(parts) >= 6:
        strategy_tag = parts[1]
        side_tag = parts[2]
        role_tag = parts[3]
        idx_part = parts[4]
    # Legacy format: dtg-{side_tag}-{role_tag}-{idx}-{ts}
    elif len(parts) == 5:
        strategy_tag = ""
        side_tag = parts[1]
        role_tag = parts[2]
        idx_part = parts[3]
    else:
        return None

    if side_tag not in ("l", "s"):
        return None
    if role_tag not in ("e", "x"):
        return None
    try:
        idx = int(idx_part)
    except ValueError:
        return None
    return strategy_tag, side_tag, role_tag, idx


def make_strategy_tag(strategy_id: str) -> str:
    raw = (strategy_id or "").strip().lower()
    cleaned = "".join(ch if (ch.isalnum() or ch in "_-") else "_" for ch in raw)
    if not cleaned:
        cleaned = "default"
    if len(cleaned) <= 10:
        return cleaned
    digest = hashlib.sha1(cleaned.encode("utf-8")).hexdigest()[:4]
    return f"{cleaned[:5]}{digest}"


class DualTriggerGrid:
    def __init__(self, client: BinanceFuturesClient, cfg: StrategyConfig, filters: Filters):
        self.client = client
        self.cfg = cfg
        self.filters = filters
        self.cells = build_cells(cfg, filters.tick_size)
        self.order_qty = round_down(cfg.order_qty, filters.step_size)
        self.growth = Decimal("1") + cfg.grid_ratio
        self.strategy_tag = make_strategy_tag(cfg.strategy_id)

        self.journal = CsvJournal(cfg.csv_path)
        self.total_trades = 0
        self.total_profit = Decimal("0")
        self.start_ts = time.time()
        self.last_status_ts = 0.0

    def _client_id(self, side_tag: str, role_tag: str, idx: int) -> str:
        return f"dtg-{self.strategy_tag}-{side_tag}-{role_tag}-{idx}-{int(time.time())}"

    def _active_count(self, direction: str) -> int:
        if direction == "LONG":
            return sum(1 for c in self.cells if c.long_entry is not None or c.long_exit is not None)
        if direction == "SHORT":
            return sum(1 for c in self.cells if c.short_entry is not None or c.short_exit is not None)
        raise ValueError(f"unknown direction: {direction}")

    def _append_short_cell(self) -> None:
        if not self.cells:
            return
        prev = self.cells[-1]
        upper = prev.lower
        lower = round_down(upper / self.growth, self.filters.tick_size)
        if lower >= upper:
            lower = upper - self.filters.tick_size
        if lower <= 0:
            return
        idx = len(self.cells)
        cell = CellState(idx=idx, lower=lower, upper=upper)
        self.cells.append(cell)
        self._write_event(
            event="CELL_EXPANDED",
            cell_idx=idx,
            note=f"lower={decimal_to_str(lower)},upper={decimal_to_str(upper)}",
        )
        log(f"cell expanded(short): {idx} [{decimal_to_str(lower)}, {decimal_to_str(upper)}]")

    def _append_long_cell(self) -> None:
        if not self.cells:
            return
        prev = self.cells[-1]
        lower = prev.upper
        upper = round_down(lower * self.growth, self.filters.tick_size)
        if upper <= lower:
            upper = lower + self.filters.tick_size
        idx = len(self.cells)
        cell = CellState(idx=idx, lower=lower, upper=upper)
        self.cells.append(cell)
        self._write_event(
            event="CELL_EXPANDED",
            cell_idx=idx,
            note=f"lower={decimal_to_str(lower)},upper={decimal_to_str(upper)}",
        )
        log(f"cell expanded(long): {idx} [{decimal_to_str(lower)}, {decimal_to_str(upper)}]")

    def _expand_to_index(self, target_idx: int) -> None:
        while target_idx >= len(self.cells):
            if self.cfg.mode == "short":
                self._append_short_cell()
            elif self.cfg.mode == "long":
                self._append_long_cell()
            else:
                # mode=both keeps static range for now
                break
            if len(self.cells) == 0:
                break

    def _find_cell_by_order_price(self, side_tag: str, role_tag: str, price: Decimal) -> Optional[CellState]:
        p = round_down(price, self.filters.tick_size)
        for c in self.cells:
            if side_tag == "s" and role_tag == "e" and c.upper == p:
                return c
            if side_tag == "s" and role_tag == "x" and c.lower == p:
                return c
            if side_tag == "l" and role_tag == "e" and c.lower == p:
                return c
            if side_tag == "l" and role_tag == "x" and c.upper == p:
                return c
        return None

    def _find_or_make_cell_for_order(self, side_tag: str, role_tag: str, price: Decimal, idx_hint: int) -> Optional[CellState]:
        # Prefer matching by order price to avoid legacy index drift after strategy logic changes.
        cell = self._find_cell_by_order_price(side_tag, role_tag, price)
        if cell is not None:
            return cell

        # For one-sided dynamic modes, expand first then try matching by price again.
        if self.cfg.mode == "short":
            target = round_down(price, self.filters.tick_size)
            while self.cells and (
                (side_tag == "s" and role_tag == "e" and target <= self.cells[-1].lower)
                or (side_tag == "s" and role_tag == "x" and target < self.cells[-1].lower)
            ):
                before = len(self.cells)
                self._append_short_cell()
                if len(self.cells) == before:
                    break
            cell = self._find_cell_by_order_price(side_tag, role_tag, price)
            if cell is not None:
                return cell
        elif self.cfg.mode == "long":
            target = round_down(price, self.filters.tick_size)
            while self.cells and (
                (side_tag == "l" and role_tag == "e" and target >= self.cells[-1].upper)
                or (side_tag == "l" and role_tag == "x" and target > self.cells[-1].upper)
            ):
                before = len(self.cells)
                self._append_long_cell()
                if len(self.cells) == before:
                    break
            cell = self._find_cell_by_order_price(side_tag, role_tag, price)
            if cell is not None:
                return cell

        # Fallback to index hint for backward compatibility.
        if idx_hint >= 0:
            if idx_hint >= len(self.cells):
                self._expand_to_index(idx_hint)
            if idx_hint < len(self.cells):
                return self.cells[idx_hint]
        return None

    def _expand_cells_for_price(self, price: Decimal) -> None:
        if not self.cells:
            return
        if self.cfg.mode == "short":
            while price <= self.cells[-1].lower:
                before = len(self.cells)
                self._append_short_cell()
                if len(self.cells) == before:
                    break
        elif self.cfg.mode == "long":
            while price >= self.cells[-1].upper:
                before = len(self.cells)
                self._append_long_cell()
                if len(self.cells) == before:
                    break

    def _write_event(
        self,
        event: str,
        cell_idx: Optional[int] = None,
        direction: str = "",
        order_role: str = "",
        order_id: Optional[int] = None,
        client_order_id: str = "",
        price: Optional[Decimal] = None,
        qty: Optional[Decimal] = None,
        status: str = "",
        pnl_usdt: Optional[Decimal] = None,
        note: str = "",
    ) -> None:
        row = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "event": event,
            "symbol": self.cfg.symbol,
            "mode": self.cfg.mode,
            "cell_idx": "" if cell_idx is None else str(cell_idx),
            "direction": direction,
            "order_role": order_role,
            "order_id": "" if order_id is None else str(order_id),
            "client_order_id": client_order_id,
            "price": "" if price is None else decimal_to_str(price),
            "qty": "" if qty is None else decimal_to_str(qty),
            "status": status,
            "pnl_usdt": "" if pnl_usdt is None else decimal_to_str(pnl_usdt),
            "total_trades": str(self.total_trades),
            "total_profit_usdt": decimal_to_str(self.total_profit),
            "note": note,
        }
        self.journal.append(row)

    def _record_cells(self) -> None:
        for c in self.cells:
            self._write_event(
                event="CELL",
                cell_idx=c.idx,
                note=f"lower={decimal_to_str(c.lower)},upper={decimal_to_str(c.upper)}",
            )

    def _recover_open_orders(self) -> None:
        recovered = 0
        skipped_foreign = 0
        open_orders = self.client.get_open_orders(self.cfg.symbol)
        for data in open_orders:
            client_order_id = str(data.get("clientOrderId", ""))
            parsed = parse_client_order_id(client_order_id)
            if parsed is None:
                continue

            parsed_strategy_tag, side_tag, role_tag, idx = parsed
            if parsed_strategy_tag and parsed_strategy_tag != self.strategy_tag:
                skipped_foreign += 1
                continue
            if not parsed_strategy_tag and self.strategy_tag != "default":
                # Ignore legacy unnamed orders unless this strategy explicitly uses default tag.
                skipped_foreign += 1
                continue
            if idx < 0:
                continue
            price = Decimal(str(data.get("price", "0")))
            qty = Decimal(str(data.get("origQty", "0")))
            cell = self._find_or_make_cell_for_order(side_tag, role_tag, price, idx)
            if cell is None:
                continue
            order_id = int(data["orderId"])
            status = str(data.get("status", ""))

            if side_tag == "l":
                if role_tag == "e":
                    cell.long_entry = order_id
                else:
                    cell.long_exit = order_id
                direction = "LONG"
            else:
                if role_tag == "e":
                    cell.short_entry = order_id
                else:
                    cell.short_exit = order_id
                direction = "SHORT"

            recovered += 1
            log(
                f"recovered open order: cell={cell.idx} direction={direction} "
                f"orderId={order_id} status={status} price={decimal_to_str(price)}"
            )
            self._write_event(
                event="OPEN_ORDER_RECOVERED",
                cell_idx=cell.idx,
                direction=direction,
                order_role="ENTRY" if role_tag == "e" else "EXIT",
                order_id=order_id,
                client_order_id=client_order_id,
                price=price,
                qty=qty,
                status=status,
            )

        if recovered == 0:
            log("recovered open order: none")
        else:
            log(f"recovered open order count: {recovered}")
        if skipped_foreign > 0:
            log(f"skipped foreign strategy open orders: {skipped_foreign}")

    def calc_qty(self, price: Decimal) -> Decimal:
        qty = self.order_qty
        if qty <= 0:
            return Decimal("0")
        if qty < self.filters.min_qty:
            return Decimal("0")
        if self.filters.min_notional > 0 and qty * price < self.filters.min_notional:
            return Decimal("0")
        return qty

    def run_forever(self) -> None:
        try:
            self.client.set_hedge_mode(True)
            log("Hedge mode set: True")
        except BinanceAPIError as exc:
            msg = str(exc)
            if "-4059" in msg:
                log("Hedge mode already configured")
            else:
                raise

        self.client.set_leverage(self.cfg.symbol, self.cfg.leverage)
        log(f"Leverage set to {self.cfg.leverage}x for {self.cfg.symbol}")

        self.total_trades, self.total_profit = self.journal.load_totals()
        log(
            f"Recovered stats from CSV: total_trades={self.total_trades}, "
            f"total_profit_usdt={decimal_to_str(self.total_profit)}"
        )

        self._record_cells()
        self._recover_open_orders()

        log(
            f"Strategy started: symbol={self.cfg.symbol}, mode={self.cfg.mode}, "
            f"max_active={self.cfg.grids}, cells={len(self.cells)}, grid_ratio={decimal_to_str(self.cfg.grid_ratio)}, "
            f"order_qty={decimal_to_str(self.order_qty)}, strategy={self.strategy_tag}, csv_path={self.cfg.csv_path}"
        )

        for c in self.cells:
            log(f"cell {c.idx}: [{decimal_to_str(c.lower)}, {decimal_to_str(c.upper)}]")

        while True:
            try:
                self.tick()
            except Exception as exc:
                log(f"loop error: {exc}")
            time.sleep(self.cfg.poll_interval_sec)

    def tick(self) -> None:
        price = self.client.get_mark_price(self.cfg.symbol)
        log(f"mark={decimal_to_str(price)}")

        self._expand_cells_for_price(price)
        self._arm_cells(price)
        self._sync_orders()
        self._maybe_place_entries()
        self._print_status(price)

    def _print_status(self, mark_price: Decimal) -> None:
        now = time.time()
        if now - self.last_status_ts < self.cfg.status_interval_sec:
            return
        self.last_status_ts = now

        long_armed = sum(1 for c in self.cells if c.long_armed)
        short_armed = sum(1 for c in self.cells if c.short_armed)
        long_entry_open = sum(1 for c in self.cells if c.long_entry is not None)
        short_entry_open = sum(1 for c in self.cells if c.short_entry is not None)
        long_exit_open = sum(1 for c in self.cells if c.long_exit is not None)
        short_exit_open = sum(1 for c in self.cells if c.short_exit is not None)

        uptime_sec = int(now - self.start_ts)
        hours = uptime_sec // 3600
        mins = (uptime_sec % 3600) // 60
        secs = uptime_sec % 60
        uptime = f"{hours:02d}:{mins:02d}:{secs:02d}"

        log(
            "STATUS "
            f"mark={decimal_to_str(mark_price)} "
            f"trades={self.total_trades} "
            f"profit_usdt={decimal_to_str(self.total_profit)} "
            f"armed(L/S)={long_armed}/{short_armed} "
            f"openEntry(L/S)={long_entry_open}/{short_entry_open} "
            f"openExit(L/S)={long_exit_open}/{short_exit_open} "
            f"uptime={uptime}"
        )

    def _arm_cells(self, price: Decimal) -> None:
        for c in self.cells:
            if (
                self.cfg.mode in ("both", "long")
                and c.long_entry is None
                and c.long_exit is None
                and not c.long_armed
                and price >= c.lower
            ):
                c.long_armed = True
                log(f"arm LONG: cell={c.idx} price={decimal_to_str(price)} >= lower={decimal_to_str(c.lower)}")
                self._write_event(event="ARM", cell_idx=c.idx, direction="LONG", price=price)

            if (
                self.cfg.mode in ("both", "short")
                and c.short_entry is None
                and c.short_exit is None
                and not c.short_armed
                and price <= c.upper
            ):
                c.short_armed = True
                log(f"arm SHORT: cell={c.idx} price={decimal_to_str(price)} <= upper={decimal_to_str(c.upper)}")
                self._write_event(event="ARM", cell_idx=c.idx, direction="SHORT", price=price)

    def _maybe_place_entries(self) -> None:
        long_active = self._active_count("LONG")
        short_active = self._active_count("SHORT")

        for c in self.cells:
            if self.cfg.mode in ("both", "long") and c.long_armed and c.long_entry is None and c.long_exit is None:
                if long_active >= self.cfg.grids:
                    continue
                qty = self.calc_qty(c.lower)
                if qty > 0:
                    client_id = self._client_id("l", "e", c.idx)
                    order_id = self.client.place_limit_order(
                        symbol=self.cfg.symbol,
                        side="BUY",
                        position_side="LONG",
                        quantity=qty,
                        price=c.lower,
                        client_id=client_id,
                    )
                    c.long_entry = order_id
                    c.long_armed = False
                    log(
                        f"LONG entry placed: cell={c.idx} orderId={order_id} qty={decimal_to_str(qty)} price={decimal_to_str(c.lower)}"
                    )
                    self._write_event(
                        event="ENTRY_PLACED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="ENTRY",
                        order_id=order_id,
                        client_order_id=client_id,
                        price=c.lower,
                        qty=qty,
                        status="NEW",
                    )
                    long_active += 1
                else:
                    log(f"LONG entry skipped(minQty/minNotional): cell={c.idx} price={decimal_to_str(c.lower)}")
                    self._write_event(
                        event="ENTRY_SKIPPED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="ENTRY",
                        price=c.lower,
                        status="SKIPPED",
                        note="minQty/minNotional",
                    )

            if self.cfg.mode in ("both", "short") and c.short_armed and c.short_entry is None and c.short_exit is None:
                if short_active >= self.cfg.grids:
                    continue
                qty = self.calc_qty(c.upper)
                if qty > 0:
                    client_id = self._client_id("s", "e", c.idx)
                    order_id = self.client.place_limit_order(
                        symbol=self.cfg.symbol,
                        side="SELL",
                        position_side="SHORT",
                        quantity=qty,
                        price=c.upper,
                        client_id=client_id,
                    )
                    c.short_entry = order_id
                    c.short_armed = False
                    log(
                        f"SHORT entry placed: cell={c.idx} orderId={order_id} qty={decimal_to_str(qty)} price={decimal_to_str(c.upper)}"
                    )
                    self._write_event(
                        event="ENTRY_PLACED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="ENTRY",
                        order_id=order_id,
                        client_order_id=client_id,
                        price=c.upper,
                        qty=qty,
                        status="NEW",
                    )
                    short_active += 1
                else:
                    log(f"SHORT entry skipped(minQty/minNotional): cell={c.idx} price={decimal_to_str(c.upper)}")
                    self._write_event(
                        event="ENTRY_SKIPPED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="ENTRY",
                        price=c.upper,
                        status="SKIPPED",
                        note="minQty/minNotional",
                    )

    def _sync_orders(self) -> None:
        for c in self.cells:
            if c.long_entry is not None:
                data = self.client.get_order(self.cfg.symbol, c.long_entry)
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    self._write_event(
                        event="ENTRY_FILLED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="ENTRY",
                        order_id=c.long_entry,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("avgPrice", data.get("price", c.lower)))),
                        qty=qty,
                        status=status,
                    )

                    exit_client_id = self._client_id("l", "x", c.idx)
                    exit_id = self.client.place_limit_order(
                        symbol=self.cfg.symbol,
                        side="SELL",
                        position_side="LONG",
                        quantity=qty,
                        price=c.upper,
                        client_id=exit_client_id,
                    )
                    log(
                        f"LONG entry filled: cell={c.idx} qty={decimal_to_str(qty)}; exit order={exit_id} @ {decimal_to_str(c.upper)}"
                    )
                    self._write_event(
                        event="EXIT_PLACED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="EXIT",
                        order_id=exit_id,
                        client_order_id=exit_client_id,
                        price=c.upper,
                        qty=qty,
                        status="NEW",
                    )
                    c.long_entry = None
                    c.long_exit = exit_id
                elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                    log(f"LONG entry ended({status}): cell={c.idx} orderId={c.long_entry}")
                    self._write_event(
                        event="ORDER_ENDED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="ENTRY",
                        order_id=c.long_entry,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("price", c.lower))),
                        qty=Decimal(str(data.get("origQty", "0"))),
                        status=status,
                    )
                    c.long_entry = None

            if c.long_exit is not None:
                data = self.client.get_order(self.cfg.symbol, c.long_exit)
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    pnl = qty * (c.upper - c.lower)
                    self.total_trades += 1
                    self.total_profit += pnl
                    log(
                        f"LONG cycle closed: cell={c.idx} exitOrder={c.long_exit} pnl={decimal_to_str(pnl)} "
                        f"total_trades={self.total_trades} total_profit={decimal_to_str(self.total_profit)}"
                    )
                    self._write_event(
                        event="CYCLE_CLOSED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="EXIT",
                        order_id=c.long_exit,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("avgPrice", data.get("price", c.upper)))),
                        qty=qty,
                        status=status,
                        pnl_usdt=pnl,
                    )
                    c.long_exit = None
                elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                    log(f"LONG exit ended({status}): cell={c.idx} orderId={c.long_exit}")
                    self._write_event(
                        event="ORDER_ENDED",
                        cell_idx=c.idx,
                        direction="LONG",
                        order_role="EXIT",
                        order_id=c.long_exit,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("price", c.upper))),
                        qty=Decimal(str(data.get("origQty", "0"))),
                        status=status,
                    )
                    c.long_exit = None

            if c.short_entry is not None:
                data = self.client.get_order(self.cfg.symbol, c.short_entry)
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    self._write_event(
                        event="ENTRY_FILLED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="ENTRY",
                        order_id=c.short_entry,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("avgPrice", data.get("price", c.upper)))),
                        qty=qty,
                        status=status,
                    )

                    exit_client_id = self._client_id("s", "x", c.idx)
                    exit_id = self.client.place_limit_order(
                        symbol=self.cfg.symbol,
                        side="BUY",
                        position_side="SHORT",
                        quantity=qty,
                        price=c.lower,
                        client_id=exit_client_id,
                    )
                    log(
                        f"SHORT entry filled: cell={c.idx} qty={decimal_to_str(qty)}; exit order={exit_id} @ {decimal_to_str(c.lower)}"
                    )
                    self._write_event(
                        event="EXIT_PLACED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="EXIT",
                        order_id=exit_id,
                        client_order_id=exit_client_id,
                        price=c.lower,
                        qty=qty,
                        status="NEW",
                    )
                    c.short_entry = None
                    c.short_exit = exit_id
                elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                    log(f"SHORT entry ended({status}): cell={c.idx} orderId={c.short_entry}")
                    self._write_event(
                        event="ORDER_ENDED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="ENTRY",
                        order_id=c.short_entry,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("price", c.upper))),
                        qty=Decimal(str(data.get("origQty", "0"))),
                        status=status,
                    )
                    c.short_entry = None

            if c.short_exit is not None:
                data = self.client.get_order(self.cfg.symbol, c.short_exit)
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    pnl = qty * (c.upper - c.lower)
                    self.total_trades += 1
                    self.total_profit += pnl
                    log(
                        f"SHORT cycle closed: cell={c.idx} exitOrder={c.short_exit} pnl={decimal_to_str(pnl)} "
                        f"total_trades={self.total_trades} total_profit={decimal_to_str(self.total_profit)}"
                    )
                    self._write_event(
                        event="CYCLE_CLOSED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="EXIT",
                        order_id=c.short_exit,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("avgPrice", data.get("price", c.lower)))),
                        qty=qty,
                        status=status,
                        pnl_usdt=pnl,
                    )
                    c.short_exit = None
                elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                    log(f"SHORT exit ended({status}): cell={c.idx} orderId={c.short_exit}")
                    self._write_event(
                        event="ORDER_ENDED",
                        cell_idx=c.idx,
                        direction="SHORT",
                        order_role="EXIT",
                        order_id=c.short_exit,
                        client_order_id=str(data.get("clientOrderId", "")),
                        price=Decimal(str(data.get("price", c.lower))),
                        qty=Decimal(str(data.get("origQty", "0"))),
                        status=status,
                    )
                    c.short_exit = None


def _opt_decimal(v: object) -> Optional[Decimal]:
    if v is None:
        return None
    return Decimal(str(v))


def load_config(path: str) -> StrategyConfig:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "grid_ratio" not in data:
        raise ValueError("config requires grid_ratio, e.g. 0.005 means 0.5% per grid")
    if "order_qty" not in data:
        raise ValueError("config requires order_qty, e.g. 0.002 means 0.002 BTC per order")

    csv_path = str(data.get("csv_path", "grid_trades.csv"))
    strategy_id = str(data.get("strategy_id", "")).strip()
    if not strategy_id:
        strategy_id = os.path.splitext(os.path.basename(csv_path))[0]

    return StrategyConfig(
        symbol=data["symbol"],
        grids=int(data["grids"]),
        grid_ratio=Decimal(str(data["grid_ratio"])),
        order_qty=Decimal(str(data["order_qty"])),
        leverage=int(data.get("leverage", 3)),
        mode=data.get("mode", "both"),
        poll_interval_sec=float(data.get("poll_interval_sec", 1.0)),
        status_interval_sec=float(data.get("status_interval_sec", 5.0)),
        csv_path=csv_path,
        strategy_id=strategy_id,
        lower_price=_opt_decimal(data.get("lower_price")),
        upper_price=_opt_decimal(data.get("upper_price")),
    )


def validate_config(cfg: StrategyConfig) -> None:
    if cfg.mode not in ("both", "long", "short"):
        raise ValueError("mode must be one of: both, long, short")
    if cfg.grids < 1:
        raise ValueError("grids(max active cycles) must be >= 1")
    if cfg.grid_ratio <= 0:
        raise ValueError("grid_ratio must be > 0")
    if cfg.order_qty <= 0:
        raise ValueError("order_qty must be > 0")
    if not cfg.csv_path.strip():
        raise ValueError("csv_path must not be empty")
    if not cfg.strategy_id.strip():
        raise ValueError("strategy_id must not be empty")
    if cfg.status_interval_sec < 0.5:
        raise ValueError("status_interval_sec should be >= 0.5")

    if cfg.mode == "long" and cfg.lower_price is None:
        raise ValueError("mode=long requires lower_price")
    if cfg.mode == "short" and cfg.upper_price is None:
        raise ValueError("mode=short requires upper_price")
    if cfg.mode == "both" and (cfg.lower_price is None or cfg.upper_price is None):
        raise ValueError("mode=both requires both lower_price and upper_price")

    if cfg.poll_interval_sec < 0.2:
        raise ValueError("poll_interval_sec should be >= 0.2 to avoid aggressive polling")
