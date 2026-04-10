import os
from decimal import Decimal

from dual_trigger_grid import DualTriggerGrid, Filters, StrategyConfig


class SimClient:
    def __init__(self, marks):
        self.marks = [Decimal(str(v)) for v in marks]
        self.current_mark = self.marks[0] if self.marks else Decimal("0")
        self.order_map = {}
        self.open_order_ids = []
        self._next_id = 1000

    def set_hedge_mode(self, enabled):
        return None

    def set_leverage(self, symbol, leverage):
        return None

    def get_mark_price(self, symbol):
        if self.marks:
            self.current_mark = self.marks.pop(0)
        return self.current_mark

    def place_limit_order(self, symbol, side, position_side, quantity, price, client_id):
        oid = self._next_id
        self._next_id += 1
        self.order_map[oid] = {
            "orderId": oid,
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "quantity": Decimal(str(quantity)),
            "price": Decimal(str(price)),
            "origQty": Decimal(str(quantity)),
            "executedQty": Decimal("0"),
            "avgPrice": Decimal(str(price)),
            "clientOrderId": client_id,
            "status": "NEW",
        }
        self.open_order_ids.append(oid)
        return oid

    def _should_fill(self, order):
        if order["status"] != "NEW":
            return False
        price = order["price"]
        side = order["side"]
        if side == "SELL":
            return self.current_mark <= price
        if side == "BUY":
            return self.current_mark <= price
        return False

    def get_order(self, symbol, order_id):
        order = self.order_map[order_id]
        if self._should_fill(order):
            order["status"] = "FILLED"
            order["executedQty"] = order["origQty"]
            if order_id in self.open_order_ids:
                self.open_order_ids.remove(order_id)
        return {
            "orderId": order["orderId"],
            "status": order["status"],
            "executedQty": str(order["executedQty"]),
            "avgPrice": str(order["avgPrice"]),
            "price": str(order["price"]),
            "origQty": str(order["origQty"]),
            "clientOrderId": order["clientOrderId"],
        }

    def get_open_orders(self, symbol):
        rows = []
        for oid in self.open_order_ids:
            order = self.order_map[oid]
            rows.append(
                {
                    "orderId": order["orderId"],
                    "clientOrderId": order["clientOrderId"],
                    "price": str(order["price"]),
                    "origQty": str(order["origQty"]),
                    "status": order["status"],
                }
            )
        return rows

    def cancel_order(self, symbol, order_id):
        order = self.order_map[order_id]
        order["status"] = "CANCELED"
        if order_id in self.open_order_ids:
            self.open_order_ids.remove(order_id)
        return {"status": "CANCELED"}


def run_simulation(csv_path: str):
    if os.path.exists(csv_path):
        os.remove(csv_path)
    marks = [
        "0.70",
        "0.64",
        "0.59",
        "0.54",
        "0.50",
        "0.46",
        "0.42",
        "0.39",
        "0.36",
        "0.33",
        "0.30",
        "0.27",
        "0.24",
        "0.21",
        "0.18",
        "0.15",
        "0.12",
        "0.10",
        "0.08",
        "0.06",
        "0.05",
    ]
    cfg = StrategyConfig(
        symbol="ARIAUSDT",
        window_cells=5,
        move_grid=True,
        grid_ratio=Decimal("0.08"),
        order_usdt=Decimal("20"),
        leverage=10,
        mode="short",
        poll_interval_sec=1.0,
        status_interval_sec=1000.0,
        csv_path=csv_path,
        strategy_id="aria_sim_drop",
        anchor_price=Decimal("0.7"),
    )
    filters = Filters(
        tick_size=Decimal("0.0001"),
        step_size=Decimal("0.1"),
        min_qty=Decimal("0.1"),
        min_notional=Decimal("0"),
    )
    client = SimClient(marks)
    bot = DualTriggerGrid(client=client, cfg=cfg, filters=filters)
    bot.initialize()

    while client.marks:
        bot.tick()

    return csv_path


if __name__ == "__main__":
    path = run_simulation("logs/aria_short_sim_drop.csv")
    print(path)
