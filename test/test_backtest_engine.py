import os
import shutil
import sys
import uuid
import unittest
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.agent.quick_filter_agent import QuickFilterAgent
from src.database import StockDatabase
from src.evaluation.backtest import BacktestEngine


class BacktestEngineTests(unittest.TestCase):
    def setUp(self):
        self.test_tmp_root = os.path.join(PROJECT_ROOT, ".test_tmp")
        os.makedirs(self.test_tmp_root, exist_ok=True)
        self.temp_dir = os.path.join(self.test_tmp_root, f"backtest_{uuid.uuid4().hex}")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        self.engine = BacktestEngine(
            db=self.db,
            output_path=os.path.join(self.temp_dir, "latest_backtest_report.json"),
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_records_signal_snapshot_and_builds_strategy_weight_reference(self):
        candidates = [
            {
                "code": "000001",
                "name": "trend sample",
                "strategy_tags": ["trend_breakout"],
                "strategy_confidence": 0.9,
                "technical_score": 80,
                "theme_score": 1,
                "key_metrics": {"ret5": 4.0},
            },
            {
                "code": "000002",
                "name": "value sample",
                "strategy_tags": ["value_bottom"],
                "strategy_confidence": 0.8,
                "technical_score": 75,
                "theme_score": 0,
                "key_metrics": {"ret5": -2.0},
            },
        ]
        self._seed_bars(
            "000001",
            ["20260420", "20260421", "20260422", "20260423", "20260424", "20260427"],
            [10, 10.2, 10.4, 10.8, 11.0, 11.5],
        )
        self._seed_bars(
            "000002",
            ["20260420", "20260421", "20260422", "20260423", "20260424", "20260427"],
            [10, 9.9, 9.8, 9.6, 9.5, 9.4],
        )

        saved = self.engine.record_signal_snapshot(candidates, signal_date="20260420")
        reference = self.engine.build_weight_reference(candidates, holding_days=(3, 5), min_samples=1)

        self.assertEqual(saved, 2)
        self.assertEqual(reference["sample_count"], 2)
        self.assertGreater(reference["strategy_weight_adjustments"]["trend_breakout"]["multiplier"], 1.0)
        self.assertLess(reference["strategy_weight_adjustments"]["value_bottom"]["multiplier"], 1.0)
        self.assertGreater(reference["candidate_adjustments"]["000001"]["score_bonus"], 0)
        self.assertLess(reference["candidate_adjustments"]["000002"]["score_bonus"], 0)

    def test_quick_filter_fallback_uses_calibrated_screener_score(self):
        agent = QuickFilterAgent()
        candidates = [
            {
                "code": "000001",
                "name": "weak adjusted",
                "strategy_tags": ["trend_breakout"],
                "strategy_confidence": 0.8,
                "technical_score": 75,
                "raw_technical_score": 80,
                "backtest_weight_bonus": -5,
            },
            {
                "code": "000002",
                "name": "strong adjusted",
                "strategy_tags": ["value_bottom"],
                "strategy_confidence": 0.8,
                "technical_score": 86,
                "raw_technical_score": 80,
                "backtest_weight_bonus": 6,
            },
        ]

        result = agent._rule_based_fallback(candidates, target_n=1)

        self.assertEqual(result["selected_codes"], ["000002"])

    @patch("src.stock_screener.StockScreener")
    def test_walk_forward_backtest_masks_future_data_by_as_of_date(self, mock_screener_class):
        self._seed_bars(
            "000001",
            ["20260420", "20260421", "20260422", "20260423", "20260424", "20260427"],
            [10, 10.2, 10.5, 10.8, 11.0, 11.4],
        )
        seen_as_of_dates = []
        mock_screener = mock_screener_class.return_value

        def run_screening(*, top_n, lookback_days, as_of_date, apply_backtest_weights):
            seen_as_of_dates.append(as_of_date)
            self.assertFalse(apply_backtest_weights)
            return [
                {
                    "code": "000001",
                    "name": "masked sample",
                    "strategy_tags": ["trend_breakout"],
                    "strategy_confidence": 0.9,
                    "technical_score": 80,
                }
            ]

        mock_screener.run_technical_screening.side_effect = run_screening

        report = self.engine.run_walk_forward_backtest(
            as_of_dates=["20260420"],
            holding_days=(3,),
            top_n=5,
        )

        self.assertEqual(seen_as_of_dates, ["20260420"])
        self.assertEqual(report["mode"], "walk_forward_masked")
        self.assertEqual(report["evaluated_count"], 1)
        self.assertEqual(report["evaluations"][0]["return_3d"], 8.0)

    def _seed_bars(self, code, dates, closes):
        rows = []
        for trade_date, close in zip(dates, closes):
            rows.append(
                {
                    "code": code,
                    "period": "daily",
                    "trade_date": trade_date,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000,
                    "amount": 10000,
                    "source": "unit_test",
                    "fetched_at": "2026-04-27 18:00:00",
                }
            )
        self.assertTrue(
            self.db.upsert_dataframe(
                "market_bars",
                pd.DataFrame(rows),
                key_columns=["code", "period", "trade_date"],
            )
        )


if __name__ == "__main__":
    unittest.main()
