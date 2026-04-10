import os
import tempfile
import unittest
from decimal import Decimal

from dual_trigger_grid import DualTriggerGrid, Filters, StrategyConfig


class FakeClient:
    def __init__(self, marks=None, open_orders=None):
        self.marks = list(marks or [])
        self.open_orders = list(open_orders or [])
        self.placed = []
        self.order_map = {}
        self._next_id = 1000

    def get_mark_price(self, symbol):
        if self.marks:
            return Decimal(str(self.marks.pop(0)))
        return Decimal("0")

    def place_limit_order(self, symbol, side, position_side, quantity, price, client_id):
        oid = self._next_id
        self._next_id += 1
        self.placed.append(
            {
                "orderId": oid,
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "quantity": Decimal(str(quantity)),
                "price": Decimal(str(price)),
                "clientOrderId": client_id,
            }
        )
        self.order_map[oid] = {
            "status": "NEW",
            "executedQty": str(quantity),
            "avgPrice": str(price),
            "price": str(price),
            "origQty": str(quantity),
            "clientOrderId": client_id,
        }
        return oid

    def get_order(self, symbol, order_id):
        data = dict(self.order_map[order_id])
        data["orderId"] = order_id
        return data

    def get_open_orders(self, symbol):
        return list(self.open_orders)


class GridStubTests(unittest.TestCase):
    def _make_cfg(self, csv_path):
        return StrategyConfig(
            symbol="BTCUSDT",
            window_cells=3,
            move_grid=False,
            grid_ratio=Decimal("0.002"),
            order_usdt=Decimal("140"),
            leverage=3,
            mode="short",
            poll_interval_sec=1.0,
            status_interval_sec=1000.0,
            csv_path=csv_path,
            strategy_id="default",
            anchor_price=Decimal("70000"),
        )

    def _make_filters(self):
        return Filters(
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("0"),
        )

    def test_short_places_entries_and_respects_grids_cap(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = self._make_cfg(os.path.join(d, "trades.csv"))
            client = FakeClient(marks=["68800"])
            bot = DualTriggerGrid(client=client, cfg=cfg, filters=self._make_filters())

            bot.tick()

            sells = [o for o in client.placed if o["side"] == "SELL"]
            self.assertEqual(len(sells), 3)
            self.assertEqual(sells[0]["price"], Decimal("70000"))
            self.assertEqual(sells[1]["price"], Decimal("69860.2"))
            self.assertEqual(sells[2]["price"], Decimal("69720.7"))

    def test_move_grid_starts_with_full_window(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = self._make_cfg(os.path.join(d, "trades.csv"))
            cfg.move_grid = True
            client = FakeClient()
            bot = DualTriggerGrid(client=client, cfg=cfg, filters=self._make_filters())

            self.assertEqual(len(bot.cells), 3)
            self.assertEqual(bot.cells[0].upper, Decimal("70000"))
            self.assertEqual(bot.cells[-1].lower, Decimal("69581.5"))

    def test_move_grid_shifts_by_one_cell(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = self._make_cfg(os.path.join(d, "trades.csv"))
            cfg.move_grid = True
            client = FakeClient()
            bot = DualTriggerGrid(client=client, cfg=cfg, filters=self._make_filters())

            bot._maintain_cell_window(Decimal("69581.5"))

            self.assertEqual(len(bot.cells), 3)
            self.assertEqual(bot.cells[0].upper, Decimal("69860.2"))
            self.assertEqual(bot.cells[0].lower, Decimal("69720.7"))
            self.assertEqual(bot.cells[-1].upper, Decimal("69581.5"))
            self.assertEqual(bot.cells[-1].lower, Decimal("69442.6"))
            self.assertEqual(bot.moved_cells_total, 1)

    def test_move_grid_stops_after_window_cells_moves(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = self._make_cfg(os.path.join(d, "trades.csv"))
            cfg.move_grid = True
            client = FakeClient()
            bot = DualTriggerGrid(client=client, cfg=cfg, filters=self._make_filters())

            bot._maintain_cell_window(Decimal("69165.5"))

            self.assertEqual(len(bot.cells), 3)
            self.assertEqual(bot.moved_cells_total, 3)
            self.assertTrue(bot.move_cells_limit_reached)
            self.assertEqual(bot.cells[0].upper, Decimal("69581.5"))
            self.assertEqual(bot.cells[-1].lower, Decimal("69165.5"))

    def test_recover_entry_prefers_price_mapping_not_legacy_idx(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = self._make_cfg(os.path.join(d, "trades.csv"))
            open_orders = [
                {
                    "orderId": 1,
                    "clientOrderId": "dtg-s-e-41-1700000000",
                    "price": "68752.0",
                    "origQty": "0.002",
                    "status": "NEW",
                }
            ]
            client = FakeClient(open_orders=open_orders)
            client.order_map[1] = {
                "status": "FILLED",
                "executedQty": "0.002",
                "avgPrice": "68752.0",
                "price": "68752.0",
                "origQty": "0.002",
                "clientOrderId": "dtg-s-e-41-1700000000",
            }
            bot = DualTriggerGrid(client=client, cfg=cfg, filters=self._make_filters())

            bot._recover_open_orders()
            # Filled entry should place BUY exit on matched cell.lower (~68614.7), not a far-away wrong level.
            bot._sync_orders()

            buys = [o for o in client.placed if o["side"] == "BUY"]
            self.assertEqual(len(buys), 1)
            self.assertEqual(buys[0]["price"], Decimal("68614.7"))



if __name__ == "__main__":
    unittest.main()
