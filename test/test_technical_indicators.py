import unittest

import pandas as pd

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


if __name__ == "__main__":
    unittest.main()
