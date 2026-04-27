import os
import shutil
import sys
import sqlite3
import uuid
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.stock_screener import StockScreener
from src.data_pipeline import DataPipeline
from src.database import StockDatabase
from src.evaluation.paper_trading import PaperTrading
from src.market_regime import MarketRegimeDetector


class StockDatabaseUpsertTests(unittest.TestCase):
    def setUp(self):
        self.test_tmp_root = os.path.join(PROJECT_ROOT, ".test_tmp")
        os.makedirs(self.test_tmp_root, exist_ok=True)
        self.temp_dir = os.path.join(self.test_tmp_root, f"t_{uuid.uuid4().hex}")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.db_path = os.path.join(self.temp_dir, "stock_lake.db")
        self.db = StockDatabase(db_path=self.db_path)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_upsert_stock_basic_updates_existing_row(self):
        initial_df = pd.DataFrame(
            [{"code": "000001", "name": "平安银行", "industry": "银行", "update_date": "2026-04-18"}]
        )
        updated_df = pd.DataFrame(
            [{"code": "000001", "name": "平安银行", "industry": "股份制银行", "update_date": "2026-04-19"}]
        )

        self.assertTrue(self.db.upsert_dataframe("stock_basic", initial_df, key_columns=["code"]))
        self.assertTrue(self.db.upsert_dataframe("stock_basic", updated_df, key_columns=["code"]))

        result = self.db.query_to_dataframe("SELECT code, industry, update_date FROM stock_basic")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["industry"], "股份制银行")
        self.assertEqual(result.iloc[0]["update_date"], "2026-04-19")

    def test_legacy_stock_basic_without_primary_key_gets_unique_index(self):
        legacy_db_path = os.path.join(self.temp_dir, "legacy_stock_lake.db")
        with sqlite3.connect(legacy_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE stock_basic (
                    code TEXT,
                    name TEXT,
                    industry TEXT,
                    update_date TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO stock_basic (code, name, industry, update_date) VALUES (?, ?, ?, ?)",
                ("000001", "old name", "old industry", "2026-04-18"),
            )
            conn.commit()

        db = StockDatabase(db_path=legacy_db_path)
        updated_df = pd.DataFrame(
            [{"code": "000001", "name": "new name", "industry": "new industry", "update_date": "2026-04-25"}]
        )

        self.assertTrue(db.upsert_dataframe("stock_basic", updated_df, key_columns=["code"]))

        result = db.query_to_dataframe("SELECT code, name, industry, update_date FROM stock_basic")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["name"], "new name")
        self.assertEqual(result.iloc[0]["industry"], "new industry")

    def test_upsert_daily_quotes_preserves_history_by_trade_date(self):
        quotes_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "trade_date": "20260418",
                    "price": 10.2,
                    "change_pct": 1.0,
                    "volume": 1000,
                    "amount": 10000,
                    "turnover_rate": 1.2,
                    "pe_ttm": 8.5,
                    "pb": 0.9,
                    "total_market_cap": 1000000000,
                },
                {
                    "code": "000001",
                    "trade_date": "20260419",
                    "price": 10.8,
                    "change_pct": 2.0,
                    "volume": 1200,
                    "amount": 12000,
                    "turnover_rate": 1.5,
                    "pe_ttm": 8.7,
                    "pb": 0.95,
                    "total_market_cap": 1010000000,
                },
            ]
        )

        self.assertTrue(self.db.upsert_dataframe("daily_quotes", quotes_df, key_columns=["code", "trade_date"]))

        result = self.db.query_to_dataframe("SELECT code, trade_date, price FROM daily_quotes ORDER BY trade_date")
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["trade_date"].tolist(), ["20260418", "20260419"])

    def test_upsert_market_bars_preserves_multiple_periods(self):
        bars_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "period": "daily",
                    "trade_date": "20260420",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "adj_close": 10.2,
                    "volume": 1000,
                    "amount": 10000,
                    "source": "unit_test",
                    "fetched_at": "2026-04-20 18:00:00",
                },
                {
                    "code": "000001",
                    "period": "weekly",
                    "trade_date": "20260420",
                    "open": 9.8,
                    "high": 10.5,
                    "low": 9.5,
                    "close": 10.2,
                    "adj_close": 10.2,
                    "volume": 5000,
                    "amount": 50000,
                    "source": "unit_test",
                    "fetched_at": "2026-04-20 18:00:00",
                },
            ]
        )

        self.assertTrue(
            self.db.upsert_dataframe(
                "market_bars",
                bars_df,
                key_columns=["code", "period", "trade_date"],
            )
        )

        result = self.db.query_to_dataframe("SELECT code, period, close FROM market_bars ORDER BY period")
        self.assertEqual(len(result), 2)
        self.assertListEqual(result["period"].tolist(), ["daily", "weekly"])


class DataPipelineNormalizationTests(unittest.TestCase):
    def setUp(self):
        self.test_tmp_root = os.path.join(PROJECT_ROOT, ".test_tmp")
        os.makedirs(self.test_tmp_root, exist_ok=True)
        self.temp_dir = os.path.join(self.test_tmp_root, f"t_{uuid.uuid4().hex}")
        os.makedirs(self.temp_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_to_yahoo_symbol_maps_a_share_exchange_suffix(self):
        pipeline = DataPipeline.__new__(DataPipeline)

        self.assertEqual(pipeline._to_yahoo_symbol("600000"), "600000.SS")
        self.assertEqual(pipeline._to_yahoo_symbol("000001"), "000001.SZ")
        self.assertEqual(pipeline._to_yahoo_symbol("300750"), "300750.SZ")
        self.assertEqual(pipeline._to_yahoo_symbol("830000"), "")

    def test_to_alpha_vantage_symbol_maps_a_share_exchange_suffix(self):
        pipeline = DataPipeline.__new__(DataPipeline)

        self.assertEqual(pipeline._to_alpha_vantage_symbol("600000"), "600000.SHH")
        self.assertEqual(pipeline._to_alpha_vantage_symbol("000001"), "000001.SHZ")
        self.assertEqual(pipeline._to_alpha_vantage_symbol("300750"), "300750.SHZ")
        self.assertEqual(pipeline._to_alpha_vantage_symbol("830000"), "")

    def test_parse_alpha_vantage_series_builds_market_bar_rows(self):
        pipeline = DataPipeline.__new__(DataPipeline)

        series = {
            "2026-04-25": {
                "1. open": "10.10",
                "2. high": "10.50",
                "3. low": "9.90",
                "4. close": "10.30",
                "5. adjusted close": "10.28",
                "6. volume": "123456",
            }
        }

        rows = pipeline._parse_alpha_vantage_series("000001", "daily", series, "2026-04-25 18:00:00")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["code"], "000001")
        self.assertEqual(rows[0]["period"], "daily")
        self.assertEqual(rows[0]["trade_date"], "20260425")
        self.assertEqual(rows[0]["source"], "alpha_vantage")
        self.assertEqual(rows[0]["adj_close"], "10.28")

    @patch("src.data_pipeline.time.sleep")
    @patch("src.data_pipeline.requests.get")
    def test_fetch_alpha_vantage_bars_returns_dataframe_for_small_batch(self, mock_get, _mock_sleep):
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.alpha_vantage_api_key = "test-key"
        pipeline.alpha_vantage_base_url = "https://www.alphavantage.co/query"
        pipeline.alpha_vantage_daily_limit = 5
        pipeline.alpha_vantage_request_interval = 0

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "Time Series (Daily)": {
                "2026-04-25": {
                    "1. open": "10.10",
                    "2. high": "10.50",
                    "3. low": "9.90",
                    "4. close": "10.30",
                    "5. adjusted close": "10.28",
                    "6. volume": "123456",
                }
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = pipeline._fetch_alpha_vantage_bars(["000001"], "daily")

        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["code"], "000001")
        self.assertEqual(df.iloc[0]["source"], "alpha_vantage")

    def test_fetch_alpha_vantage_bars_skips_when_batch_exceeds_limit(self):
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.alpha_vantage_api_key = "test-key"
        pipeline.alpha_vantage_daily_limit = 1
        pipeline.alpha_vantage_request_interval = 0

        df = pipeline._fetch_alpha_vantage_bars(["000001", "000002"], "daily")
        self.assertIsNone(df)

    @patch("src.data_pipeline.ak.stock_zh_index_daily")
    def test_sync_index_bars_writes_market_regime_indices(self, mock_index_daily):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db
        pipeline.market_bars_daily_retention_days = 540
        pipeline.market_bars_weekly_retention_days = 1825
        pipeline.market_bars_monthly_retention_days = 3650
        mock_index_daily.return_value = pd.DataFrame(
            [
                {"date": "2026-04-24", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
            ]
        )

        self.assertTrue(pipeline.sync_index_bars())
        rows = db.query_to_dataframe("SELECT code, period, trade_date, close, source FROM market_bars ORDER BY code")

        self.assertEqual(rows["code"].tolist(), ["sh000001", "sh000300", "sz399006"])
        self.assertEqual(rows["period"].unique().tolist(), ["daily"])
        self.assertEqual(rows["trade_date"].unique().tolist(), ["20260424"])
        self.assertEqual(rows["source"].unique().tolist(), ["akshare_index"])

    def test_filter_codes_needing_bars_skips_fresh_daily_records(self):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db

        expected_date = pipeline._expected_latest_trade_date("daily")
        bars_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "period": "daily",
                    "trade_date": expected_date,
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "adj_close": 10.5,
                    "volume": 1000,
                    "amount": None,
                    "source": "unit_test",
                    "fetched_at": "2026-04-25 18:00:00",
                }
            ]
        )
        self.assertTrue(db.upsert_dataframe("market_bars", bars_df, key_columns=["code", "period", "trade_date"]))

        pending = pipeline._filter_codes_needing_bars(["000001", "000002"], "daily")

        self.assertEqual(pending, ["000002"])

    def test_daily_quotes_fresh_when_latest_trade_date_exists(self):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db

        expected_date = pipeline._expected_latest_trade_date("daily")
        quotes_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "trade_date": expected_date,
                    "price": 10.2,
                    "change_pct": 1.0,
                    "volume": 1000,
                    "amount": 10000,
                    "turnover_rate": 1.2,
                    "pe_ttm": 8.5,
                    "pb": 0.9,
                    "total_market_cap": 1000000000,
                }
            ]
        )
        self.assertTrue(db.upsert_dataframe("daily_quotes", quotes_df, key_columns=["code", "trade_date"]))

        self.assertTrue(pipeline._daily_quotes_is_fresh())

    def test_enrich_daily_quote_valuations_uses_akshare_snapshot(self):
        pipeline = DataPipeline.__new__(DataPipeline)
        clean_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "trade_date": "20260425",
                    "price": 10.0,
                    "change_pct": 1.0,
                    "volume": 1000,
                    "amount": 1000000,
                    "turnover_rate": 1.2,
                    "pe_ttm": None,
                    "pb": None,
                    "total_market_cap": None,
                }
            ]
        )
        ak_df = pd.DataFrame(
            [
                {
                    "代码": "000001",
                    "最新价": 10.1,
                    "涨跌幅": 1.1,
                    "成交量": 1000,
                    "成交额": 1000000,
                    "换手率": 1.3,
                    "市盈率-动态": 9.8,
                    "市净率": 0.95,
                    "总市值": 12_000_000_000,
                }
            ]
        )
        pipeline._safe_fetch = MagicMock(return_value=ak_df)

        enriched = pipeline._enrich_daily_quote_valuations(clean_df, ["000001"])

        self.assertEqual(enriched.iloc[0]["pe_ttm"], 9.8)
        self.assertEqual(enriched.iloc[0]["pb"], 0.95)
        self.assertEqual(enriched.iloc[0]["total_market_cap"], 12_000_000_000)

    def test_stock_basic_fresh_when_updated_today(self):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db

        stock_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "name": "test",
                    "industry": "bank",
                    "update_date": pd.Timestamp.now().strftime("%Y-%m-%d"),
                }
            ]
        )
        self.assertTrue(db.upsert_dataframe("stock_basic", stock_df, key_columns=["code"]))

        self.assertTrue(pipeline._stock_basic_is_fresh())

    def test_sync_stock_basic_writes_fetched_codes(self):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db
        pipeline._safe_fetch = MagicMock(
            return_value=pd.DataFrame(
                [
                    {"code": "1", "name": "Ping An Bank"},
                    {"code": "600519", "name": "Kweichow Moutai"},
                ]
            )
        )

        self.assertTrue(pipeline.sync_stock_basic())

        result = db.query_to_dataframe("SELECT code, name, update_date FROM stock_basic ORDER BY code")
        self.assertEqual(result["code"].tolist(), ["000001", "600519"])
        self.assertEqual(result["name"].tolist(), ["Ping An Bank", "Kweichow Moutai"])
        self.assertTrue((result["update_date"] == pd.Timestamp.now().strftime("%Y-%m-%d")).all())

    @patch.object(DataPipeline, "_fetch_ak_bars", return_value=None)
    @patch.object(DataPipeline, "_fetch_alpha_vantage_bars", return_value=None)
    @patch.object(DataPipeline, "_fetch_yahoo_bars", return_value=None)
    def test_sync_market_bars_does_not_fetch_when_all_codes_are_fresh(
        self,
        mock_yahoo,
        mock_alpha,
        mock_ak,
    ):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db

        expected_date = pipeline._expected_latest_trade_date("daily")
        bars_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "period": "daily",
                    "trade_date": expected_date,
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "adj_close": 10.5,
                    "volume": 1000,
                    "amount": None,
                    "source": "unit_test",
                    "fetched_at": "2026-04-25 18:00:00",
                }
            ]
        )
        self.assertTrue(db.upsert_dataframe("market_bars", bars_df, key_columns=["code", "period", "trade_date"]))

        self.assertTrue(pipeline.sync_market_bars(codes=["000001"], periods=("daily",)))

        mock_yahoo.assert_not_called()
        mock_alpha.assert_not_called()
        mock_ak.assert_not_called()

    def test_prune_database_history_keeps_recent_analysis_window(self):
        db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))
        pipeline = DataPipeline.__new__(DataPipeline)
        pipeline.db = db
        pipeline.daily_quotes_retention_days = 30
        pipeline.market_bars_daily_retention_days = 365
        pipeline.market_bars_weekly_retention_days = 730
        pipeline.market_bars_monthly_retention_days = 3650
        pipeline.daily_lhb_retention_days = 90
        pipeline.paper_trades_retention_days = 365

        quotes_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "trade_date": "20240101",
                    "price": 10,
                    "change_pct": 0,
                    "volume": 100,
                    "amount": 1000,
                    "turnover_rate": 1,
                    "pe_ttm": 8,
                    "pb": 1,
                    "total_market_cap": 100000,
                },
                {
                    "code": "000001",
                    "trade_date": "20260424",
                    "price": 11,
                    "change_pct": 0,
                    "volume": 100,
                    "amount": 1000,
                    "turnover_rate": 1,
                    "pe_ttm": 8,
                    "pb": 1,
                    "total_market_cap": 100000,
                },
            ]
        )
        self.assertTrue(db.upsert_dataframe("daily_quotes", quotes_df, key_columns=["code", "trade_date"]))

        bars_df = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "period": "daily",
                    "trade_date": "20240101",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "adj_close": 10.5,
                    "volume": 1000,
                    "amount": None,
                    "source": "unit_test",
                    "fetched_at": "2026-04-25 18:00:00",
                },
                {
                    "code": "000001",
                    "period": "daily",
                    "trade_date": "20260424",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "adj_close": 10.5,
                    "volume": 1000,
                    "amount": None,
                    "source": "unit_test",
                    "fetched_at": "2026-04-25 18:00:00",
                },
            ]
        )
        self.assertTrue(db.upsert_dataframe("market_bars", bars_df, key_columns=["code", "period", "trade_date"]))

        with db.get_connection() as conn:
            conn.execute(
                "INSERT INTO paper_trades "
                "(code, name, recommend_date, recommend_price, recommend_reason, current_price, return_pct, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("000001", "old closed", "2024-01-01", 10, "test", 9, -10, "CLOSED"),
            )
            conn.execute(
                "INSERT INTO paper_trades "
                "(code, name, recommend_date, recommend_price, recommend_reason, current_price, return_pct, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("000002", "old hold", "2024-01-01", 10, "test", 9, -10, "HOLD"),
            )
            conn.commit()

        self.assertTrue(pipeline._prune_database_history())

        quotes = db.query_to_dataframe("SELECT trade_date FROM daily_quotes ORDER BY trade_date")
        bars = db.query_to_dataframe("SELECT trade_date FROM market_bars ORDER BY trade_date")
        trades = db.query_to_dataframe("SELECT code, status FROM paper_trades ORDER BY code")

        self.assertEqual(quotes["trade_date"].tolist(), ["20260424"])
        self.assertEqual(bars["trade_date"].tolist(), ["20260424"])
        self.assertEqual(trades[["code", "status"]].values.tolist(), [["000002", "HOLD"]])


class PaperTradingPostMarketTests(unittest.TestCase):
    def setUp(self):
        self.test_tmp_root = os.path.join(PROJECT_ROOT, ".test_tmp")
        os.makedirs(self.test_tmp_root, exist_ok=True)
        self.temp_dir = os.path.join(self.test_tmp_root, f"t_{uuid.uuid4().hex}")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.db = StockDatabase(db_path=os.path.join(self.temp_dir, "stock_lake.db"))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _seed_trade(self, code, name, recommend_price, latest_price):
        self.db.execute_non_query(
            """
            INSERT INTO paper_trades
            (code, name, recommend_date, recommend_price, recommend_reason, current_price, return_pct, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'HOLD')
            """,
            (code, name, "2026-04-20 15:30:00", recommend_price, "unit test", recommend_price, 0.0),
        )
        self.db.upsert_dataframe(
            "daily_quotes",
            pd.DataFrame(
                [
                    {
                        "code": code,
                        "trade_date": "20260425",
                        "price": latest_price,
                        "change_pct": 0,
                        "volume": 1000,
                        "amount": 1000000,
                        "turnover_rate": 1.0,
                        "pe_ttm": 10,
                        "pb": 1,
                        "total_market_cap": 1000000000,
                    }
                ]
            ),
            key_columns=["code", "trade_date"],
        )

    @patch("src.evaluation.paper_trading.fetch_latest_prices", return_value={})
    def test_post_market_diagnosis_outputs_actions_and_closes_stop_loss(self, _mock_prices):
        self._seed_trade("000001", "hold sample", 10, 10.2)
        self._seed_trade("000002", "wait sample", 10, 9.7)
        self._seed_trade("000003", "reduce sample", 10, 10.8)
        self._seed_trade("000004", "clear sample", 10, 9.3)
        paper = PaperTrading(db=self.db, rules={"stop_loss_pct": -5, "take_profit_pct": 10, "reduce_watch_pct": 6})

        report = paper.run_post_market_review()

        self.assertIn("继续持有", report)
        self.assertIn("等待确认", report)
        self.assertIn("减仓观察", report)
        self.assertIn("清仓退出", report)
        trades = self.db.query_to_dataframe("SELECT code, status, return_pct FROM paper_trades ORDER BY code")
        self.assertEqual(trades[trades["code"] == "000004"].iloc[0]["status"], "CLOSED")
        self.assertEqual(float(trades[trades["code"] == "000003"].iloc[0]["return_pct"]), 8.0)

    @patch("src.evaluation.paper_trading.fetch_latest_prices", return_value={"000001": 10.0})
    def test_add_trade_skips_duplicate_hold_position(self, _mock_prices):
        paper = PaperTrading(db=self.db)

        paper.add_trade("000001", "duplicate sample", "unit test")
        paper.add_trade("000001", "duplicate sample", "unit test")

        trades = self.db.query_to_dataframe("SELECT code, status FROM paper_trades WHERE code = '000001'")
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades.iloc[0]["status"], "HOLD")


class StockScreenerTests(unittest.TestCase):
    def setUp(self):
        self.screener = StockScreener()

    def _seed_index_bars(self, db, mode="offensive"):
        rows = []
        for code in ["sh000001", "sh000300", "sz399006"]:
            for i in range(70):
                if mode == "offensive":
                    close = 100 + i * 0.45
                elif mode == "defensive":
                    close = 130 - i * 0.28
                else:
                    close = 150 - i * 0.9
                    if i % 4 == 0:
                        close -= 5
                trade_date = f"2026{(i // 28) + 1:02d}{(i % 28) + 1:02d}"
                rows.append(
                    {
                        "code": code,
                        "period": "daily",
                        "trade_date": trade_date,
                        "open": close - 0.2,
                        "high": close + 0.5,
                        "low": close - 0.5,
                        "close": close,
                        "adj_close": close,
                        "volume": 1000000,
                        "amount": 100000000,
                        "source": "unit_test",
                        "fetched_at": "2026-04-25 18:00:00",
                    }
                )
        self.assertTrue(db.upsert_dataframe("market_bars", pd.DataFrame(rows), key_columns=["code", "period", "trade_date"]))

    def _write_market_regime_config(self, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        'default_profile: "balanced"',
                        "market_regime:",
                        "  enabled: true",
                        "  allow_empty_on_extreme_risk: true",
                        "  indices:",
                        '    sh000001: "sh"',
                        '    sh000300: "hs300"',
                        '    sz399006: "chinext"',
                        "profiles:",
                        "  aggressive:",
                        "    max_annual_vol20: 1.2",
                        "  conservative:",
                        "    max_annual_vol20: 0.5",
                    ]
                )
            )

    def _seed_basic_screening_case(self, db, codes):
        basics = pd.DataFrame(
            [
                {"code": code, "name": name, "industry": industry, "update_date": "2026-04-25"}
                for code, name, industry in codes
            ]
        )
        self.assertTrue(db.upsert_dataframe("stock_basic", basics, key_columns=["code"]))

        quotes = pd.DataFrame(
            [
                {
                    "code": code,
                    "trade_date": "20260425",
                    "price": 16.0,
                    "change_pct": 2.0,
                    "volume": 1_000_000,
                    "amount": 120_000_000,
                    "turnover_rate": 2.0,
                    "pe_ttm": 20,
                    "pb": 2,
                    "total_market_cap": 20_000_000_000,
                }
                for code, _name, _industry in codes
            ]
        )
        self.assertTrue(db.upsert_dataframe("daily_quotes", quotes, key_columns=["code", "trade_date"]))

        rows = []
        for code, _name, _industry in codes:
            for i in range(70):
                close = 10 + i * 0.06
                amount = 20_000_000 if code == "000003" else 100_000_000 + i * 1_000_000
                trade_date = f"2026{(i // 28) + 1:02d}{(i % 28) + 1:02d}"
                rows.append(
                    {
                        "code": code,
                        "period": "daily",
                        "trade_date": trade_date,
                        "open": close - 0.1,
                        "high": close + 0.2,
                        "low": close - 0.2,
                        "close": close,
                        "adj_close": close,
                        "volume": 1_000_000 + i * 1000,
                        "amount": amount,
                        "source": "unit_test",
                        "fetched_at": "2026-04-25 18:00:00",
                    }
                )
        bars = pd.DataFrame(rows)
        self.assertTrue(db.upsert_dataframe("market_bars", bars, key_columns=["code", "period", "trade_date"]))

    def test_technical_screening_excludes_chinext_and_star_market(self):
        db_path = os.path.join(PROJECT_ROOT, ".test_tmp", f"technical_{uuid.uuid4().hex}.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        db = StockDatabase(db_path=db_path)
        self.screener = StockScreener(db=db)
        self.screener.theme_scorer = MagicMock()
        self.screener.theme_scorer.score_candidates.side_effect = lambda candidates: candidates

        basics = pd.DataFrame(
            [
                {"code": "000001", "name": "main board", "industry": "bank", "update_date": "2026-04-25"},
                {"code": "600000", "name": "sh main", "industry": "bank", "update_date": "2026-04-25"},
                {"code": "300001", "name": "chinext", "industry": "tech", "update_date": "2026-04-25"},
                {"code": "688001", "name": "star", "industry": "tech", "update_date": "2026-04-25"},
            ]
        )
        self.assertTrue(db.upsert_dataframe("stock_basic", basics, key_columns=["code"]))

        quotes = pd.DataFrame(
            [
                {
                    "code": code,
                    "trade_date": "20260425",
                    "price": 16.0,
                    "change_pct": 2.0,
                    "volume": 1_000_000,
                    "amount": 120_000_000,
                    "turnover_rate": 2.0,
                    "pe_ttm": 20,
                    "pb": 2,
                    "total_market_cap": 20_000_000_000,
                }
                for code in ["000001", "600000", "300001", "688001"]
            ]
        )
        self.assertTrue(db.upsert_dataframe("daily_quotes", quotes, key_columns=["code", "trade_date"]))

        rows = []
        for code in ["000001", "600000", "300001", "688001"]:
            for i in range(70):
                close = 10 + i * (0.08 if code == "600000" else 0.06)
                trade_date = f"2026{(i // 28) + 1:02d}{(i % 28) + 1:02d}"
                rows.append(
                    {
                        "code": code,
                        "period": "daily",
                        "trade_date": trade_date,
                        "open": close - 0.1,
                        "high": close + 0.2,
                        "low": close - 0.2,
                        "close": close,
                        "adj_close": close,
                        "volume": 1_000_000 + i * 1000,
                        "amount": 100_000_000 + i * 1_000_000,
                        "source": "unit_test",
                        "fetched_at": "2026-04-25 18:00:00",
                    }
                )
        bars = pd.DataFrame(rows)
        self.assertTrue(db.upsert_dataframe("market_bars", bars, key_columns=["code", "period", "trade_date"]))

        result = self.screener.run_technical_screening(top_n=10)
        codes = [row["code"] for row in result]

        self.assertIn("000001", codes)
        self.assertIn("600000", codes)
        self.assertNotIn("300001", codes)
        self.assertNotIn("688001", codes)
        self.assertLessEqual(len(result), 10)

        os.remove(db_path)

    def test_technical_screening_allows_missing_industry_to_fill_top_n(self):
        ranked = [
                {
                    "code": f"60000{idx}",
                    "name": f"stock {idx}",
                    "industry": None,
                    "technical_score": 100 - idx,
                }
            for idx in range(5)
        ]
        candidates = self.screener._apply_industry_cap(ranked, top_n=5, max_per_industry=3)

        self.assertEqual(len(candidates), 5)

    def test_strategy_quota_keeps_multi_strategy_candidates(self):
        ranked = [
            {
                "code": f"60010{idx}",
                "name": f"trend {idx}",
                "industry": f"industry {idx}",
                "technical_score": 100 - idx,
                "strategy_confidence": 0.9,
                "strategy_tags": ["trend_breakout"],
            }
            for idx in range(6)
        ] + [
            {
                "code": "000001",
                "name": "value one",
                "industry": "bank",
                "technical_score": 70,
                "strategy_confidence": 0.8,
                "strategy_tags": ["value_bottom"],
            },
            {
                "code": "000002",
                "name": "panic one",
                "industry": "medicine",
                "technical_score": 65,
                "strategy_confidence": 0.78,
                "strategy_tags": ["panic_reversal"],
            },
        ]

        candidates = self.screener._apply_strategy_quota(ranked, top_n=6, max_per_industry=10)
        tags = [item["strategy_tags"][0] for item in candidates]

        self.assertIn("value_bottom", tags)
        self.assertIn("panic_reversal", tags)
        self.assertLess(tags.count("trend_breakout"), 6)

    def test_screener_backfills_valuation_from_previous_quote_snapshot(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"valuation_backfill_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db = StockDatabase(db_path=os.path.join(temp_dir, "stock_lake.db"))
        self.assertTrue(
            db.upsert_dataframe(
                "stock_basic",
                pd.DataFrame([{"code": "000001", "name": "valuation sample", "industry": "bank", "update_date": "2026-04-25"}]),
                key_columns=["code"],
            )
        )
        quotes = pd.DataFrame(
            [
                {
                    "code": "000001",
                    "trade_date": "20260424",
                    "price": 16.8,
                    "change_pct": 1.0,
                    "volume": 1_000_000,
                    "amount": 120_000_000,
                    "turnover_rate": 2.0,
                    "pe_ttm": 11.5,
                    "pb": 1.1,
                    "total_market_cap": 22_000_000_000,
                },
                {
                    "code": "000001",
                    "trade_date": "20260425",
                    "price": 16.9,
                    "change_pct": 2.0,
                    "volume": 1_100_000,
                    "amount": 130_000_000,
                    "turnover_rate": 2.2,
                    "pe_ttm": None,
                    "pb": None,
                    "total_market_cap": None,
                },
            ]
        )
        self.assertTrue(db.upsert_dataframe("daily_quotes", quotes, key_columns=["code", "trade_date"]))

        rows = []
        for i in range(70):
            close = 10 + i * 0.1
            rows.append(
                {
                    "code": "000001",
                    "period": "daily",
                    "trade_date": f"2026{(i // 28) + 1:02d}{(i % 28) + 1:02d}",
                    "open": close - 0.1,
                    "high": close + 0.2,
                    "low": close - 0.2,
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000 + i * 1000,
                    "amount": 100_000_000 + i * 1_000_000,
                    "source": "unit_test",
                    "fetched_at": "2026-04-25 18:00:00",
                }
            )
        self.assertTrue(db.upsert_dataframe("market_bars", pd.DataFrame(rows), key_columns=["code", "period", "trade_date"]))

        screener = StockScreener(db=db, profile="balanced")
        screener.theme_scorer = MagicMock()
        screener.theme_scorer.score_candidates.side_effect = lambda candidates: candidates

        result = screener.run_technical_screening(top_n=5)

        self.assertEqual([item["code"] for item in result], ["000001"])
        self.assertEqual(result[0]["pe_ttm"], 11.5)
        self.assertEqual(result[0]["pb"], 1.1)
        self.assertEqual(result[0]["total_market_cap"], 22_000_000_000)
        shutil.rmtree(temp_dir)

    def test_technical_screening_caps_same_industry_at_three(self):
        ranked = [
            {
                "code": f"60000{idx}",
                "name": f"stock {idx}",
                "industry": "bank",
                "technical_score": 100 - idx,
            }
            for idx in range(5)
        ]
        candidates = self.screener._apply_industry_cap(ranked, top_n=5, max_per_industry=3)

        self.assertEqual(len(candidates), 3)

    def test_screener_loads_default_profile_from_config(self):
        config_path = os.path.join(PROJECT_ROOT, ".test_tmp", f"stock_picking_{uuid.uuid4().hex}.yaml")
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        'default_profile: "conservative"',
                        "profiles:",
                        "  conservative:",
                        "    max_annual_vol20: 0.55",
                        "    max_per_industry: 1",
                        "output:",
                        "  top_n: 8",
                        "  max_top_n: 8",
                    ]
                )
            )

        screener = StockScreener(db=MagicMock(), config_path=config_path)

        self.assertEqual(screener.profile_name, "conservative")
        self.assertEqual(screener.screening_profile["max_annual_vol20"], 0.55)
        self.assertEqual(screener.screening_profile["max_per_industry"], 1)
        self.assertEqual(screener.screening_profile["top_n"], 8)

        os.remove(config_path)

    def test_screener_explicit_profile_overrides_config_default(self):
        config_path = os.path.join(PROJECT_ROOT, ".test_tmp", f"stock_picking_{uuid.uuid4().hex}.yaml")
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        'default_profile: "conservative"',
                        "profiles:",
                        "  aggressive:",
                        "    min_avg_amount20: 25000000",
                    ]
                )
            )

        screener = StockScreener(db=MagicMock(), profile="aggressive", config_path=config_path)

        self.assertEqual(screener.profile_name, "aggressive")
        self.assertEqual(screener.screening_profile["min_avg_amount20"], 25_000_000)

        os.remove(config_path)

    def test_market_regime_detector_maps_offensive_market_to_aggressive_profile(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"regime_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db = StockDatabase(db_path=os.path.join(temp_dir, "stock_lake.db"))
        self._seed_index_bars(db, mode="offensive")

        detector = MarketRegimeDetector(
            db=db,
            config={
                "enabled": True,
                "indices": {"sh000001": "sh", "sh000300": "hs300", "sz399006": "chinext"},
            },
        )
        result = detector.detect()

        self.assertEqual(result["regime"], "offensive")
        self.assertEqual(result["profile"], "aggressive")
        self.assertFalse(result["allow_empty"])
        shutil.rmtree(temp_dir)

    def test_screener_auto_selects_profile_from_market_regime(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"auto_profile_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db = StockDatabase(db_path=os.path.join(temp_dir, "stock_lake.db"))
        config_path = os.path.join(temp_dir, "stock_picking.yaml")
        self._write_market_regime_config(config_path)
        self._seed_index_bars(db, mode="offensive")

        screener = StockScreener(db=db, config_path=config_path)

        self.assertEqual(screener.market_regime["regime"], "offensive")
        self.assertEqual(screener.profile_name, "aggressive")
        self.assertEqual(screener.screening_profile["max_annual_vol20"], 1.2)
        shutil.rmtree(temp_dir)

    def test_screener_extreme_risk_returns_empty_candidates(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"extreme_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db = StockDatabase(db_path=os.path.join(temp_dir, "stock_lake.db"))
        config_path = os.path.join(temp_dir, "stock_picking.yaml")
        self._write_market_regime_config(config_path)
        self._seed_index_bars(db, mode="extreme")

        screener = StockScreener(db=db, config_path=config_path)
        result = screener.run_technical_screening(top_n=5)

        self.assertEqual(result, [])
        self.assertEqual(screener.market_regime["regime"], "extreme_risk")
        self.assertEqual(screener.last_audit["rule_reject_counts"]["market_extreme_risk"], 1)
        shutil.rmtree(temp_dir)

    def test_screener_uses_safe_defaults_for_missing_or_invalid_config(self):
        missing_path = os.path.join(PROJECT_ROOT, ".test_tmp", f"missing_{uuid.uuid4().hex}.yaml")

        screener = StockScreener(db=MagicMock(), profile="unknown", config_path=missing_path)

        self.assertEqual(screener.profile_name, "balanced")
        self.assertEqual(screener.screening_profile["max_annual_vol20"], 0.80)
        self.assertEqual(screener.screening_profile["max_per_industry"], 3)

        config_path = os.path.join(PROJECT_ROOT, ".test_tmp", f"bad_stock_picking_{uuid.uuid4().hex}.yaml")
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        'default_profile: "balanced"',
                        "profiles:",
                        "  balanced:",
                        "    max_annual_vol20: -1",
                        "    ret20_min: 30",
                        "    ret20_max: 10",
                        "    turnover_rate_min: 50",
                        "    turnover_rate_max: 2",
                    ]
                )
            )

        screener = StockScreener(db=MagicMock(), config_path=config_path)

        self.assertEqual(screener.screening_profile["max_annual_vol20"], 0.80)
        self.assertEqual(screener.screening_profile["ret20_min"], -5.0)
        self.assertEqual(screener.screening_profile["ret20_max"], 35.0)
        self.assertEqual(screener.screening_profile["turnover_rate_min"], 0.5)
        self.assertEqual(screener.screening_profile["turnover_rate_max"], 20.0)

        os.remove(config_path)

    def test_screener_audit_records_reject_reasons_and_candidates(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"audit_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db_path = os.path.join(temp_dir, "stock_lake.db")
        config_path = os.path.join(temp_dir, "stock_picking.yaml")
        audit_path = os.path.join(temp_dir, "screener_audit.json")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        'default_profile: "balanced"',
                        "output:",
                        "  top_n: 5",
                        "  max_top_n: 5",
                        "  save_audit: true",
                        f'  audit_path: "{audit_path.replace(os.sep, "/")}"',
                    ]
                )
            )

        db = StockDatabase(db_path=db_path)
        self._seed_basic_screening_case(
            db,
            [
                ("000001", "pass sample", "bank"),
                ("000003", "low amount sample", "tech"),
            ],
        )
        screener = StockScreener(db=db, config_path=config_path)

        result = screener.run_technical_screening(top_n=5)

        self.assertEqual([item["code"] for item in result], ["000001"])
        self.assertIn("key_metrics", result[0])
        self.assertIn("screen_reason", result[0])
        self.assertEqual(screener.last_audit["candidate_count"], 1)
        self.assertEqual(screener.last_audit["rule_reject_counts"]["avg_amount20_too_low"], 1)
        rejected = {item["code"]: item for item in screener.last_audit["rejected_stocks"]}
        self.assertEqual(rejected["000003"]["reason"], "avg_amount20_too_low")
        self.assertTrue(os.path.exists(audit_path))

        shutil.rmtree(temp_dir)

    def test_screener_applies_theme_score_only_after_hard_filters(self):
        temp_dir = os.path.join(PROJECT_ROOT, ".test_tmp", f"theme_{uuid.uuid4().hex}")
        os.makedirs(temp_dir, exist_ok=True)
        db = StockDatabase(db_path=os.path.join(temp_dir, "stock_lake.db"))
        self._seed_basic_screening_case(
            db,
            [
                ("000001", "pass sample", "人工智能"),
                ("000003", "low amount sample", "人工智能"),
            ],
        )
        screener = StockScreener(db=db)
        screener.theme_scorer = MagicMock()

        def score_side_effect(candidates):
            self.assertEqual([item["code"] for item in candidates], ["000001"])
            candidates[0]["theme_score"] = 8.0
            candidates[0]["technical_score"] += 8.0
            candidates[0]["key_metrics"]["theme_score"] = 8.0
            return candidates

        screener.theme_scorer.score_candidates.side_effect = score_side_effect

        result = screener.run_technical_screening(top_n=5)

        self.assertEqual([item["code"] for item in result], ["000001"])
        self.assertEqual(result[0]["theme_score"], 8.0)
        screener.theme_scorer.score_candidates.assert_called_once()
        shutil.rmtree(temp_dir)

    def test_strategy_detector_allows_value_bottom_below_ma20(self):
        matches = self.screener._detect_strategy_matches(
            profile=self.screener.screening_profile,
            close=9.5,
            ma5=9.6,
            ma10=9.8,
            ma20=10.0,
            ma60=12.0,
            ret3=-1.0,
            ret5=-3.0,
            ret10=-6.0,
            ret20=-10.0,
            change_pct=1.0,
            high60_distance=-25.0,
            low60_distance=6.0,
            vol_ratio=1.8,
            vol5_ratio=1.1,
            turnover_rate=2.0,
            pe_ttm=10.0,
            pb=0.9,
            latest_open=9.3,
            intraday_position=0.7,
        )

        self.assertIn("value_bottom", [item["tag"] for item in matches])

    def test_strategy_detector_allows_high_ret20_momentum_without_valuation(self):
        matches = self.screener._detect_strategy_matches(
            profile=self.screener.screening_profile,
            close=15.2,
            ma5=15.0,
            ma10=13.8,
            ma20=12.5,
            ma60=10.0,
            ret3=6.0,
            ret5=8.0,
            ret10=25.0,
            ret20=45.0,
            change_pct=3.0,
            high60_distance=-2.0,
            low60_distance=58.0,
            vol_ratio=1.9,
            vol5_ratio=1.25,
            turnover_rate=8.0,
            pe_ttm=None,
            pb=None,
            latest_open=14.8,
            intraday_position=0.75,
        )

        self.assertIn("momentum_leader", [item["tag"] for item in matches])

    def test_strategy_detector_rejects_high_volume_stalling_momentum(self):
        matches = self.screener._detect_strategy_matches(
            profile=self.screener.screening_profile,
            close=15.2,
            ma5=15.0,
            ma10=13.8,
            ma20=12.5,
            ma60=10.0,
            ret3=2.0,
            ret5=5.0,
            ret10=22.0,
            ret20=42.0,
            change_pct=-1.0,
            high60_distance=-2.0,
            low60_distance=58.0,
            vol_ratio=3.0,
            vol5_ratio=1.25,
            turnover_rate=9.0,
            pe_ttm=None,
            pb=None,
            latest_open=15.8,
            intraday_position=0.2,
        )

        self.assertNotIn("momentum_leader", [item["tag"] for item in matches])


if __name__ == "__main__":
    unittest.main()
