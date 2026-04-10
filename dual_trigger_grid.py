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
LOG_PREFIX = ""


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
    long_open_qty: Decimal = Decimal("0")
    short_open_qty: Decimal = Decimal("0")


@dataclass
class StrategyConfig:
    symbol: str
    window_cells: int
    move_grid: bool
    grid_ratio: Decimal
    order_usdt: Decimal
    leverage: int
    mode: str
    poll_interval_sec: float
    status_interval_sec: float
    csv_path: str
    strategy_id: str
    anchor_price: Decimal = Decimal("0")


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    if LOG_PREFIX:
        print(f"[{ts}] [{LOG_PREFIX}] {msg}")
    else:
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
        target = cfg.window_cells
        upper = round_down(cfg.anchor_price, tick_size)
        for i in range(target):
            lower = round_down(upper / growth, tick_size)
            if lower >= upper:
                lower = upper - tick_size
            if lower <= 0:
                if i == 0:
                    raise ValueError("invalid short anchor price for initial cell")
                break
            cells.append(CellState(idx=i, lower=lower, upper=upper))
            upper = lower
        return cells

    if cfg.mode == "long":
        target = cfg.window_cells
        lower = round_down(cfg.anchor_price, tick_size)
        for i in range(target):
            upper = round_down(lower * growth, tick_size)
            if upper <= lower:
                upper = lower + tick_size
            cells.append(CellState(idx=i, lower=lower, upper=upper))
            lower = upper
        return cells

    raise ValueError("mode must be short or long")


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
        self.growth = Decimal("1") + cfg.grid_ratio
        self.strategy_tag = make_strategy_tag(cfg.strategy_id)

        self.journal = CsvJournal(cfg.csv_path)
        self.total_trades = 0
        self.total_profit = Decimal("0")
        self.start_ts = time.time()
        self.last_status_ts = 0.0
        self._initialized = False
        self.moved_cells_total = 0
        self.move_cells_limit_reached = False

    def _activate_log_prefix(self) -> None:
        global LOG_PREFIX
        LOG_PREFIX = f"{self.cfg.symbol}|{self.cfg.mode}|{self.strategy_tag}"

    def _client_id(self, side_tag: str, role_tag: str, idx: int) -> str:
        return f"dtg-{self.strategy_tag}-{side_tag}-{role_tag}-{idx}-{int(time.time())}"

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
                if self.cfg.move_grid and self.moved_cells_total >= self.cfg.window_cells:
                    if not self.move_cells_limit_reached:
                        self.move_cells_limit_reached = True
                        self._write_event(
                            event="MOVE_LIMIT_REACHED",
                            price=price,
                            status="LIMIT",
                            note=f"moved_cells_total={self.moved_cells_total},max_move_cells={self.cfg.window_cells}",
                        )
                        log(
                            f"move cells limit reached: moved_cells_total={self.moved_cells_total} "
                            f"max_move_cells={self.cfg.window_cells} price={decimal_to_str(price)}"
                        )
                    break
                before = len(self.cells)
                self._append_short_cell()
                if len(self.cells) == before:
                    break
                self._reclaim_far_cells()
                self.moved_cells_total += 1
        elif self.cfg.mode == "long":
            while price >= self.cells[-1].upper:
                if self.cfg.move_grid and self.moved_cells_total >= self.cfg.window_cells:
                    if not self.move_cells_limit_reached:
                        self.move_cells_limit_reached = True
                        self._write_event(
                            event="MOVE_LIMIT_REACHED",
                            price=price,
                            status="LIMIT",
                            note=f"moved_cells_total={self.moved_cells_total},max_move_cells={self.cfg.window_cells}",
                        )
                        log(
                            f"move cells limit reached: moved_cells_total={self.moved_cells_total} "
                            f"max_move_cells={self.cfg.window_cells} price={decimal_to_str(price)}"
                        )
                    break
                before = len(self.cells)
                self._append_long_cell()
                if len(self.cells) == before:
                    break
                self._reclaim_far_cells()
                self.moved_cells_total += 1

    def _cell_has_open_order(self, c: CellState) -> bool:
        return (
            c.long_entry is not None
            or c.long_exit is not None
            or c.short_entry is not None
            or c.short_exit is not None
        )

    def _remove_cell_at(self, idx: int) -> bool:
        if idx < 0 or idx >= len(self.cells):
            return False
        c = self.cells[idx]
        # 有逻辑持仓时不能删除该 cell，否则会丢失平仓闭环。
        if c.long_open_qty > 0 or c.short_open_qty > 0:
            return False
        # 不处理 exit 单，避免影响已成交后的平仓流程。
        if c.long_exit is not None or c.short_exit is not None:
            return False

        if self.cfg.mode == "long" and c.long_entry is not None:
            try:
                self.client.cancel_order(self.cfg.symbol, c.long_entry)
                self._write_event(
                    event="ENTRY_CANCELED",
                    cell_idx=c.idx,
                    direction="LONG",
                    order_role="ENTRY",
                    order_id=c.long_entry,
                    status="CANCELED",
                    note="reclaim_far_cell",
                )
                log(f"LONG entry canceled for reclaim: cell={c.idx} orderId={c.long_entry}")
                c.long_entry = None
                c.long_armed = False
            except Exception as exc:
                msg = str(exc)
                if "-2011" in msg or "Unknown order sent" in msg:
                    log(f"LONG reclaim cancel unknown, clear ref: cell={c.idx} orderId={c.long_entry}")
                    c.long_entry = None
                    c.long_armed = False
                else:
                    log(f"LONG reclaim cancel failed: cell={c.idx} orderId={c.long_entry} err={exc}")
                    return False

        if self.cfg.mode == "short" and c.short_entry is not None:
            try:
                self.client.cancel_order(self.cfg.symbol, c.short_entry)
                self._write_event(
                    event="ENTRY_CANCELED",
                    cell_idx=c.idx,
                    direction="SHORT",
                    order_role="ENTRY",
                    order_id=c.short_entry,
                    status="CANCELED",
                    note="reclaim_far_cell",
                )
                log(f"SHORT entry canceled for reclaim: cell={c.idx} orderId={c.short_entry}")
                c.short_entry = None
                c.short_armed = False
            except Exception as exc:
                msg = str(exc)
                if "-2011" in msg or "Unknown order sent" in msg:
                    log(f"SHORT reclaim cancel unknown, clear ref: cell={c.idx} orderId={c.short_entry}")
                    c.short_entry = None
                    c.short_armed = False
                else:
                    log(f"SHORT reclaim cancel failed: cell={c.idx} orderId={c.short_entry} err={exc}")
                    return False

        if self._cell_has_open_order(c):
            return False
        removed = self.cells.pop(idx)
        for i, cell in enumerate(self.cells):
            cell.idx = i
        self._write_event(
            event="CELL_REMOVED",
            cell_idx=removed.idx,
            note=f"lower={decimal_to_str(removed.lower)},upper={decimal_to_str(removed.upper)}",
        )
        log(f"cell removed: [{decimal_to_str(removed.lower)}, {decimal_to_str(removed.upper)}]")
        return True

    def _reclaim_far_cells(self) -> None:
        while len(self.cells) > self.cfg.window_cells:
            removed = False
            # short/long 都是删除远端（较老锚点端）的 cell，这里就是列表前端。
            for i in range(0, len(self.cells) - 1):
                if self._remove_cell_at(i):
                    removed = True
                    break
            if not removed:
                break

    def _maintain_cell_window(self, price: Decimal) -> None:
        # 先补近端，保证当前价格附近有 cell。
        self._expand_cells_for_price(price)
        # 再回收远端，回到目标窗口容量。
        self._reclaim_far_cells()

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
                    cell.long_open_qty = max(cell.long_open_qty, qty)
                direction = "LONG"
            else:
                if role_tag == "e":
                    cell.short_entry = order_id
                else:
                    cell.short_exit = order_id
                    cell.short_open_qty = max(cell.short_open_qty, qty)
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

    def _recover_open_qty_from_journal(self) -> None:
        if not os.path.exists(self.journal.path):
            return
        for c in self.cells:
            c.long_open_qty = Decimal("0")
            c.short_open_qty = Decimal("0")

        with open(self.journal.path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("symbol") != self.cfg.symbol:
                    continue
                direction = str(row.get("direction", "")).upper()
                event = str(row.get("event", ""))
                if event not in ("ENTRY_FILLED", "CYCLE_CLOSED"):
                    continue
                if self.cfg.mode == "short" and direction != "SHORT":
                    continue
                if self.cfg.mode == "long" and direction != "LONG":
                    continue

                qty_raw = row.get("qty", "")
                try:
                    qty = Decimal(str(qty_raw))
                except Exception:
                    continue
                if qty <= 0:
                    continue

                idx_hint = -1
                idx_raw = row.get("cell_idx", "")
                if str(idx_raw).strip():
                    try:
                        idx_hint = int(str(idx_raw).strip())
                    except Exception:
                        idx_hint = -1

                price = None
                price_raw = row.get("price", "")
                if str(price_raw).strip():
                    try:
                        price = Decimal(str(price_raw))
                    except Exception:
                        price = None

                cell = None
                if direction == "SHORT":
                    if price is not None:
                        if event == "ENTRY_FILLED":
                            cell = self._find_or_make_cell_for_order("s", "e", price, idx_hint)
                        else:
                            cell = self._find_or_make_cell_for_order("s", "x", price, idx_hint)
                    elif 0 <= idx_hint < len(self.cells):
                        cell = self.cells[idx_hint]

                    if cell is None:
                        continue
                    if event == "ENTRY_FILLED":
                        cell.short_open_qty += qty
                    else:
                        cell.short_open_qty = max(Decimal("0"), cell.short_open_qty - qty)

                elif direction == "LONG":
                    if price is not None:
                        if event == "ENTRY_FILLED":
                            cell = self._find_or_make_cell_for_order("l", "e", price, idx_hint)
                        else:
                            cell = self._find_or_make_cell_for_order("l", "x", price, idx_hint)
                    elif 0 <= idx_hint < len(self.cells):
                        cell = self.cells[idx_hint]

                    if cell is None:
                        continue
                    if event == "ENTRY_FILLED":
                        cell.long_open_qty += qty
                    else:
                        cell.long_open_qty = max(Decimal("0"), cell.long_open_qty - qty)

        short_total = sum((c.short_open_qty for c in self.cells), Decimal("0"))
        long_total = sum((c.long_open_qty for c in self.cells), Decimal("0"))
        log(
            f"recovered open qty from csv: short={decimal_to_str(short_total)} "
            f"long={decimal_to_str(long_total)}"
        )

    def calc_qty(self, price: Decimal) -> Decimal:
        if price <= 0:
            return Decimal("0")
        qty = round_down(self.cfg.order_usdt / price, self.filters.step_size)
        if qty <= 0:
            return Decimal("0")
        if qty < self.filters.min_qty:
            return Decimal("0")
        if self.filters.min_notional > 0 and qty * price < self.filters.min_notional:
            return Decimal("0")
        return qty

    def initialize(self) -> None:
        self._activate_log_prefix()
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
        self._recover_open_qty_from_journal()
        bootstrap_price = self.client.get_mark_price(self.cfg.symbol)
        log(f"bootstrap mark={decimal_to_str(bootstrap_price)}")
        if self.cfg.move_grid:
            self._maintain_cell_window(bootstrap_price)
        self._repair_short_missing_exits()
        self._arm_cells(bootstrap_price)
        self._maybe_place_entries()

        log(
            f"Strategy started: symbol={self.cfg.symbol}, mode={self.cfg.mode}, "
            f"window_cells={self.cfg.window_cells}, move_grid={self.cfg.move_grid}, "
            f"cells={len(self.cells)}, grid_ratio={decimal_to_str(self.cfg.grid_ratio)}, "
            f"order_usdt={decimal_to_str(self.cfg.order_usdt)}, strategy={self.strategy_tag}, csv_path={self.cfg.csv_path}"
        )

        for c in self.cells:
            log(f"cell {c.idx}: [{decimal_to_str(c.lower)}, {decimal_to_str(c.upper)}]")

        self._initialized = True

    def run_forever(self) -> None:
        if not self._initialized:
            self.initialize()

        while True:
            try:
                self._activate_log_prefix()
                self.tick()
            except Exception as exc:
                log(f"loop error: {exc}")
            time.sleep(self.cfg.poll_interval_sec)

    def tick(self) -> None:
        price = self.client.get_mark_price(self.cfg.symbol)
        log(f"mark={decimal_to_str(price)}")

        if self.cfg.move_grid:
            self._maintain_cell_window(price)
        self._arm_cells(price)
        self._sync_orders()
        self._repair_short_missing_exits()
        self._maybe_place_entries()
        self._print_status(price)

    def _repair_short_missing_exits(self) -> None:
        for c in self.cells:
            if self.cfg.mode == "short":
                if c.short_entry is None and c.short_exit is None:
                    qty = round_down(c.short_open_qty, self.filters.step_size)
                    if qty > 0:
                        client_id = self._client_id("s", "x", c.idx)
                        try:
                            exit_id = self.client.place_limit_order(
                                symbol=self.cfg.symbol,
                                side="BUY",
                                position_side="SHORT",
                                quantity=qty,
                                price=c.lower,
                                client_id=client_id,
                            )
                        except Exception as exc:
                            log(
                                f"SHORT exit repair failed: cell={c.idx} qty={decimal_to_str(qty)} "
                                f"price={decimal_to_str(c.lower)} err={exc}"
                            )
                        else:
                            c.short_exit = exit_id
                            log(
                                f"SHORT exit repaired: cell={c.idx} orderId={exit_id} "
                                f"qty={decimal_to_str(qty)} price={decimal_to_str(c.lower)}"
                            )
                            self._write_event(
                                event="EXIT_PLACED",
                                cell_idx=c.idx,
                                direction="SHORT",
                                order_role="EXIT",
                                order_id=exit_id,
                                client_order_id=client_id,
                                price=c.lower,
                                qty=qty,
                                status="NEW",
                                note="tick_exit_repair",
                            )

            if self.cfg.mode == "long":
                if c.long_entry is None and c.long_exit is None:
                    qty = round_down(c.long_open_qty, self.filters.step_size)
                    if qty > 0:
                        client_id = self._client_id("l", "x", c.idx)
                        try:
                            exit_id = self.client.place_limit_order(
                                symbol=self.cfg.symbol,
                                side="SELL",
                                position_side="LONG",
                                quantity=qty,
                                price=c.upper,
                                client_id=client_id,
                            )
                        except Exception as exc:
                            log(
                                f"LONG exit repair failed: cell={c.idx} qty={decimal_to_str(qty)} "
                                f"price={decimal_to_str(c.upper)} err={exc}"
                            )
                        else:
                            c.long_exit = exit_id
                            log(
                                f"LONG exit repaired: cell={c.idx} orderId={exit_id} "
                                f"qty={decimal_to_str(qty)} price={decimal_to_str(c.upper)}"
                            )
                            self._write_event(
                                event="EXIT_PLACED",
                                cell_idx=c.idx,
                                direction="LONG",
                                order_role="EXIT",
                                order_id=exit_id,
                                client_order_id=client_id,
                                price=c.upper,
                                qty=qty,
                                status="NEW",
                                note="tick_exit_repair",
                            )

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
                self.cfg.mode == "long"
                and c.long_entry is None
                and c.long_exit is None
                and not c.long_armed
                and c.long_open_qty <= 0
                and price >= c.lower
            ):
                c.long_armed = True
                log(f"arm LONG: cell={c.idx} price={decimal_to_str(price)} >= lower={decimal_to_str(c.lower)}")
                self._write_event(event="ARM", cell_idx=c.idx, direction="LONG", price=price)

            if (
                self.cfg.mode == "short"
                and c.short_entry is None
                and c.short_exit is None
                and not c.short_armed
                and c.short_open_qty <= 0
                and price <= c.upper
            ):
                c.short_armed = True
                log(f"arm SHORT: cell={c.idx} price={decimal_to_str(price)} <= upper={decimal_to_str(c.upper)}")
                self._write_event(event="ARM", cell_idx=c.idx, direction="SHORT", price=price)

    def _maybe_place_entries(self) -> None:
        for c in self.cells:
            if self.cfg.mode == "long" and c.long_armed and c.long_entry is None and c.long_exit is None:
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

            if (
                self.cfg.mode == "short"
                and c.short_armed
                and c.short_entry is None
                and c.short_exit is None
            ):
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
                try:
                    data = self.client.get_order(self.cfg.symbol, c.long_entry)
                except Exception as exc:
                    msg = str(exc)
                    if "-2011" in msg or "Unknown order sent" in msg:
                        log(f"LONG entry unknown on sync, clear ref: cell={c.idx} orderId={c.long_entry}")
                        c.long_entry = None
                        continue
                    raise
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    c.long_open_qty += qty
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
                try:
                    data = self.client.get_order(self.cfg.symbol, c.long_exit)
                except Exception as exc:
                    msg = str(exc)
                    if "-2011" in msg or "Unknown order sent" in msg:
                        log(f"LONG exit missing on sync, clear ref: cell={c.idx} orderId={c.long_exit}")
                        c.long_exit = None
                        continue
                    raise
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    c.long_open_qty = max(Decimal("0"), c.long_open_qty - qty)
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
                try:
                    data = self.client.get_order(self.cfg.symbol, c.short_entry)
                except Exception as exc:
                    msg = str(exc)
                    if "-2011" in msg or "Unknown order sent" in msg:
                        log(f"SHORT entry unknown on sync, clear ref: cell={c.idx} orderId={c.short_entry}")
                        c.short_entry = None
                        continue
                    raise
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    c.short_open_qty += qty
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
                    c.short_entry = None
                    c.short_exit = exit_id
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
                try:
                    data = self.client.get_order(self.cfg.symbol, c.short_exit)
                except Exception as exc:
                    msg = str(exc)
                    if "-2011" in msg or "Unknown order sent" in msg:
                        log(f"SHORT exit missing on sync, clear ref: cell={c.idx} orderId={c.short_exit}")
                        c.short_exit = None
                        continue
                    raise
                status = data["status"]
                if status == "FILLED":
                    qty = Decimal(str(data["executedQty"]))
                    c.short_open_qty = max(Decimal("0"), c.short_open_qty - qty)
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


def load_config_data(data: Dict[str, object], source_name: str = "") -> StrategyConfig:
    if "grid_ratio" not in data:
        raise ValueError("config requires grid_ratio, e.g. 0.005 means 0.5% per grid")
    if "order_usdt" not in data and "order_qty" not in data:
        raise ValueError("config requires order_usdt (or legacy order_qty)")

    mode = data.get("mode", "short")
    anchor_raw = data.get("anchor_price")
    if anchor_raw is None:
        if mode == "short":
            anchor_raw = data.get("upper_price")
        elif mode == "long":
            anchor_raw = data.get("lower_price")
    anchor_price = Decimal(str(anchor_raw)) if anchor_raw is not None else Decimal("0")

    if "order_usdt" in data:
        order_usdt = Decimal(str(data["order_usdt"]))
    else:
        # legacy compatibility: approximate by anchor price
        order_usdt = Decimal(str(data["order_qty"])) * anchor_price
    move_grid = bool(data.get("move_grid", True))

    csv_path = str(data.get("csv_path", "grid_trades.csv"))
    strategy_id = str(data.get("strategy_id", "")).strip()
    if not strategy_id:
        if source_name:
            strategy_id = source_name
        else:
            strategy_id = os.path.splitext(os.path.basename(csv_path))[0]

    return StrategyConfig(
        symbol=data["symbol"],
        window_cells=int(data.get("window_cells", data.get("grids", 20))),
        move_grid=move_grid,
        grid_ratio=Decimal(str(data["grid_ratio"])),
        order_usdt=order_usdt,
        leverage=int(data.get("leverage", 3)),
        mode=mode,
        poll_interval_sec=float(data.get("poll_interval_sec", 1.0)),
        status_interval_sec=float(data.get("status_interval_sec", 5.0)),
        csv_path=csv_path,
        strategy_id=strategy_id,
        anchor_price=anchor_price,
    )


def load_config(path: str) -> StrategyConfig:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return load_config_data(data, source_name=os.path.splitext(os.path.basename(path))[0])


def validate_config(cfg: StrategyConfig) -> None:
    if cfg.mode not in ("long", "short"):
        raise ValueError("mode must be one of: long, short")
    if cfg.window_cells < 1:
        raise ValueError("window_cells must be >= 1")
    if cfg.grid_ratio <= 0:
        raise ValueError("grid_ratio must be > 0")
    if cfg.order_usdt <= 0:
        raise ValueError("order_usdt must be > 0")
    if not cfg.csv_path.strip():
        raise ValueError("csv_path must not be empty")
    if not cfg.strategy_id.strip():
        raise ValueError("strategy_id must not be empty")
    if cfg.status_interval_sec < 0.5:
        raise ValueError("status_interval_sec should be >= 0.5")

    if cfg.anchor_price <= 0:
        raise ValueError("anchor_price must be > 0")

    if cfg.poll_interval_sec < 0.2:
        raise ValueError("poll_interval_sec should be >= 0.2 to avoid aggressive polling")
