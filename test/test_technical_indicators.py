import os
import shutil
import sys
import unittest
import uuid

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.database import StockDatabase
from src.technical_indicators import TechnicalSignalProvider


class TechnicalSignalProviderTests(unittest.TestCase):
    def _bars(self, closes, volumes=None):
        rows = []
        volumes = volumes or [1000] * len(closes)
        for idx, close in enumerate(closes):
            rows.append(
                {
                    "trade_date": f"2026-03-{idx + 1:02d}",
                    "open": close * 0.99,
                    "high": close * 1.01,
                    "low": close * 0.98,
                    "close": close,
                    "volume": volumes[idx],
                    "amount": close * volumes[idx],
                }
            )
        return pd.DataFrame(rows)

    def test_calculate_report_detects_breakout_and_levels(self):
        closes = [10.0] * 24 + [10.8]
        volumes = [1000] * 24 + [2500]
        bars = self._bars(closes, volumes)
        report = TechnicalSignalProvider().calculate_report("600519", bars)

        self.assertTrue(report.metrics["available"])
        self.assertIn("箱体放量突破", report.tags)
        self.assertGreater(report.metrics["volume_ratio"], 2.0)
        self.assertIsNotNone(report.metrics["atr14"])
        self.assertAlmostEqual(report.levels["support20"], 9.8, places=2)
        self.assertGreater(report.levels["breakout_trigger"], report.levels["resistance20"])
        self.assertIn("初始止损", TechnicalSignalProvider().format_report(report))

    def test_calculate_report_flags_high_volume_stall(self):
        closes = [10.0] * 20 + [10.1, 10.15, 10.2, 10.15, 10.05]
        volumes = [1000] * 24 + [3200]
        bars = self._bars(closes, volumes)
        bars.loc[len(bars) - 1, "open"] = 10.4
        bars.loc[len(bars) - 1, "high"] = 11.5
        bars.loc[len(bars) - 1, "low"] = 10.0
        bars.loc[len(bars) - 1, "close"] = 10.05

        report = TechnicalSignalProvider().calculate_report("000001", bars)

        self.assertIn("放量滞涨/上影派发", report.risk_flags)
        self.assertEqual(report.trade_plan["action_bias"], "avoid_chasing")

    def test_calculate_report_handles_insufficient_bars(self):
        report = TechnicalSignalProvider().calculate_report("000001", self._bars([10.0] * 10))

        self.assertFalse(report.metrics["available"])
        self.assertIn("K线样本不足", report.risk_flags)


    def test_load_daily_bars_overlays_same_day_quote_price(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"technical_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db = StockDatabase(db_path=os.path.join(temp_dir, "stock_lake.db"))
        try:
            bars = pd.DataFrame(
                [
                    {
                        "code": "000001",
                        "period": "daily",
                        "trade_date": "20260425",
                        "open": 10.0,
                        "high": 10.8,
                        "low": 9.8,
                        "close": 10.5,
                        "adj_close": 10.5,
                        "volume": 1000,
                        "amount": 10000,
                        "source": "unit_test",
                        "fetched_at": "2026-04-25 18:00:00",
                    }
                ]
            )
            quotes = pd.DataFrame(
                [
                    {
                        "code": "000001",
                        "trade_date": "20260425",
                        "price": 9.9,
                        "change_pct": -1.0,
                        "volume": 1000,
                        "amount": 10000,
                        "turnover_rate": 1.0,
                        "pe_ttm": 10,
                        "pb": 1,
                        "total_market_cap": 1000000000,
                    }
                ]
            )
            self.assertTrue(db.upsert_dataframe("market_bars", bars, key_columns=["code", "period", "trade_date"]))
            self.assertTrue(db.upsert_dataframe("daily_quotes", quotes, key_columns=["code", "trade_date"]))

            loaded = TechnicalSignalProvider(db=db).load_daily_bars("000001", lookback=5)

            self.assertEqual(float(loaded.iloc[-1]["close"]), 9.9)
            self.assertEqual(float(loaded.iloc[-1]["low"]), 9.8)
            self.assertEqual(float(loaded.iloc[-1]["high"]), 10.8)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
