"""SQLite storage helpers for the local stock data lake."""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger


class StockDatabase:
    """Small SQLite wrapper with schema bootstrap and DataFrame upsert helpers."""

    _schema_init_lock = threading.Lock()
    _schema_initialized_paths: set[str] = set()

    def __init__(self, db_path: str = "data/stock_lake.db"):
        self.db_path = db_path
        self._ensure_db_dir()
        self._ensure_schema_initialized()

    def _ensure_db_dir(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=120)
        conn.execute("PRAGMA busy_timeout = 120000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -200000")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _ensure_schema_initialized(self) -> None:
        normalized_path = os.path.abspath(self.db_path)
        if normalized_path in self._schema_initialized_paths:
            return
        with self._schema_init_lock:
            if normalized_path in self._schema_initialized_paths:
                return
            if self.create_tables():
                self._schema_initialized_paths.add(normalized_path)

    def create_tables(self) -> bool:
        queries = [
            """
            CREATE TABLE IF NOT EXISTS stock_basic (
                code TEXT PRIMARY KEY,
                name TEXT,
                industry TEXT,
                update_date TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS daily_quotes (
                code TEXT,
                trade_date TEXT,
                price REAL,
                change_pct REAL,
                volume INTEGER,
                amount REAL,
                turnover_rate REAL,
                pe_ttm REAL,
                pb REAL,
                total_market_cap REAL,
                PRIMARY KEY (code, trade_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS market_bars (
                code TEXT,
                period TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume INTEGER,
                amount REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, period, trade_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS market_bars_weekly (
                code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume INTEGER,
                amount REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, trade_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS market_bars_monthly (
                code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adj_close REAL,
                volume INTEGER,
                amount REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, trade_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS daily_lhb (
                code TEXT,
                trade_date TEXT,
                reason TEXT,
                net_buy REAL,
                buy_amount REAL,
                sell_amount REAL,
                PRIMARY KEY (code, trade_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stock_fund_flows (
                code TEXT,
                trade_date TEXT,
                period TEXT,
                name TEXT,
                latest_price REAL,
                change_pct REAL,
                main_net_inflow REAL,
                main_net_inflow_pct REAL,
                super_large_net_inflow REAL,
                large_net_inflow REAL,
                medium_net_inflow REAL,
                small_net_inflow REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, trade_date, period, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS baidu_stock_fund_flows (
                code TEXT,
                trade_date TEXT,
                close REAL,
                change_pct REAL,
                super_net_inflow REAL,
                large_net_inflow REAL,
                medium_net_inflow REAL,
                small_net_inflow REAL,
                main_net_inflow REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, trade_date, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS board_fund_flows (
                board_name TEXT,
                kind TEXT,
                trade_date TEXT,
                period TEXT,
                change_pct REAL,
                main_net_inflow REAL,
                main_net_inflow_pct REAL,
                leader_name TEXT,
                leader_change_pct REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (board_name, kind, trade_date, period, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lhb_details (
                code TEXT,
                trade_date TEXT,
                name TEXT,
                reason TEXT,
                close REAL,
                change_pct REAL,
                net_buy REAL,
                buy_amount REAL,
                sell_amount REAL,
                turnover REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, trade_date, reason, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS restricted_releases (
                code TEXT,
                release_date TEXT,
                name TEXT,
                release_shares REAL,
                release_market_value REAL,
                release_ratio REAL,
                holder_type TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, release_date, holder_type, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS concept_boards (
                concept_name TEXT,
                concept_code TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (concept_name, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stock_concepts (
                code TEXT,
                concept_name TEXT,
                concept_code TEXT,
                name TEXT,
                latest_price REAL,
                change_pct REAL,
                turnover_rate REAL,
                pe_ttm REAL,
                pb REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, concept_name, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stock_profiles (
                code TEXT,
                name TEXT,
                industry TEXT,
                region TEXT,
                industry_blocks TEXT,
                region_blocks TEXT,
                concepts TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS research_reports (
                code TEXT,
                info_code TEXT,
                title TEXT,
                publish_date TEXT,
                org_name TEXT,
                author TEXT,
                rating TEXT,
                rating_change TEXT,
                industry TEXT,
                eps_this_year REAL,
                eps_next_year REAL,
                eps_next_two_year REAL,
                pdf_url TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, info_code, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS eps_forecasts (
                code TEXT,
                forecast_year TEXT,
                institution_count REAL,
                eps_min REAL,
                eps_avg REAL,
                eps_max REAL,
                industry_avg REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, forecast_year, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stock_news (
                code TEXT,
                datetime TEXT,
                title TEXT,
                content TEXT,
                url TEXT,
                provider TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, title, datetime, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS market_news (
                datetime TEXT,
                title TEXT,
                content TEXT,
                url TEXT,
                provider TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (title, datetime, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS company_announcements (
                code TEXT,
                announcement_id TEXT,
                title TEXT,
                date TEXT,
                categories TEXT,
                url TEXT,
                pdf_url TEXT,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, announcement_id, source)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS financial_metrics (
                code TEXT,
                report_date TEXT,
                report_name TEXT,
                notice_date TEXT,
                report_type TEXT,
                revenue REAL,
                revenue_yoy REAL,
                parent_netprofit REAL,
                parent_netprofit_yoy REAL,
                deduct_parent_netprofit REAL,
                deduct_parent_netprofit_yoy REAL,
                gross_margin REAL,
                net_margin REAL,
                roe REAL,
                roic REAL,
                debt_to_assets REAL,
                current_ratio REAL,
                quick_ratio REAL,
                cash_ratio REAL,
                operating_cash_flow REAL,
                operating_cash_flow_yoy REAL,
                cash_to_profit REAL,
                total_assets REAL,
                total_liabilities REAL,
                source TEXT,
                fetched_at TEXT,
                PRIMARY KEY (code, report_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                name TEXT,
                recommend_date TEXT,
                recommend_price REAL,
                recommend_reason TEXT,
                current_price REAL,
                return_pct REAL,
                status TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS watchlist_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                name TEXT,
                tier TEXT,
                watch_status TEXT DEFAULT 'ACTIVE',
                source TEXT,
                added_at TEXT,
                updated_at TEXT,
                entry_price REAL,
                current_price REAL,
                return_pct REAL,
                elapsed_days INTEGER,
                elapsed_trade_days INTEGER,
                latest_trade_date TEXT,
                return_5d_pct REAL,
                return_10d_pct REAL,
                return_20d_pct REAL,
                return_30d_pct REAL,
                return_60d_pct REAL,
                expected_return_pct REAL,
                expected_holding_days INTEGER,
                max_holding_days INTEGER,
                exit_hint TEXT,
                recommend_reason TEXT,
                fundamental_analysis TEXT,
                technical_analysis TEXT,
                news_risk_analysis TEXT,
                decision_snapshot TEXT,
                macro_context TEXT,
                remove_reason TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trading_account (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                initial_cash REAL NOT NULL,
                cash REAL NOT NULL,
                total_market_value REAL DEFAULT 0,
                total_equity REAL DEFAULT 0,
                realized_pnl REAL DEFAULT 0,
                unrealized_pnl REAL DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trading_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                quantity INTEGER NOT NULL,
                avg_cost REAL NOT NULL,
                current_price REAL DEFAULT 0,
                market_value REAL DEFAULT 0,
                unrealized_pnl REAL DEFAULT 0,
                unrealized_return_pct REAL DEFAULT 0,
                opened_at TEXT,
                last_buy_at TEXT,
                last_sell_at TEXT,
                status TEXT DEFAULT 'OPEN',
                linked_watchlist_id INTEGER,
                buy_reason TEXT,
                risk_note TEXT,
                realized_pnl REAL DEFAULT 0,
                realized_return_pct REAL DEFAULT 0,
                sold_quantity INTEGER DEFAULT 0,
                closed_at TEXT,
                last_reflected_at TEXT,
                reflection_count INTEGER DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                code TEXT,
                name TEXT,
                action TEXT NOT NULL,
                quantity INTEGER DEFAULT 0,
                price REAL DEFAULT 0,
                amount REAL DEFAULT 0,
                cash_before REAL DEFAULT 0,
                cash_after REAL DEFAULT 0,
                position_before INTEGER DEFAULT 0,
                position_after INTEGER DEFAULT 0,
                reason TEXT,
                decision_snapshot TEXT,
                linked_watchlist_id INTEGER,
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS backtest_signal_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                source TEXT,
                primary_strategy TEXT,
                strategy_tags TEXT,
                strategy_confidence REAL,
                technical_score REAL,
                theme_score REAL,
                key_metrics TEXT,
                market_regime TEXT,
                created_at TEXT,
                UNIQUE(signal_date, code, source)
            )
            """,
        ]

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for query in queries:
                    cursor.execute(query)
                self._ensure_schema_columns(cursor)
                self._ensure_unique_constraints(cursor)
                conn.commit()
            return True
        except Exception as exc:
            logger.error("Failed to initialize database schema: {}", exc)
            return False

    def _ensure_schema_columns(self, cursor) -> None:
        """为旧版 SQLite 表补齐新增字段。"""
        expected_columns = {
            "stock_basic": {
                "industry": "TEXT",
                "update_date": "TEXT",
            },
            "watchlist_items": {
                "decision_snapshot": "TEXT",
                "expected_holding_days": "INTEGER",
                "max_holding_days": "INTEGER",
                "exit_hint": "TEXT",
                "elapsed_days": "INTEGER",
                "elapsed_trade_days": "INTEGER",
                "latest_trade_date": "TEXT",
                "return_5d_pct": "REAL",
                "return_10d_pct": "REAL",
                "return_20d_pct": "REAL",
                "return_30d_pct": "REAL",
                "return_60d_pct": "REAL",
            },
            "trading_positions": {
                "realized_pnl": "REAL DEFAULT 0",
                "realized_return_pct": "REAL DEFAULT 0",
                "sold_quantity": "INTEGER DEFAULT 0",
                "closed_at": "TEXT",
                "last_reflected_at": "TEXT",
                "reflection_count": "INTEGER DEFAULT 0",
            },
        }

        for table_name, columns in expected_columns.items():
            existing_columns = self._get_existing_columns(cursor, table_name)
            if not existing_columns:
                continue
            for column_name, column_type in columns.items():
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                    logger.info("Added missing column {}.{} {}", table_name, column_name, column_type)

    @staticmethod
    def _get_existing_columns(cursor, table_name: str) -> set[str]:
        rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row[1] for row in rows}

    def _ensure_unique_constraints(self, cursor) -> None:
        unique_indexes = [
            ("stock_basic", "idx_stock_basic_code", ["code"]),
            ("daily_quotes", "idx_daily_quotes_code_trade_date", ["code", "trade_date"]),
            ("market_bars", "idx_market_bars_code_period_trade_date", ["code", "period", "trade_date"]),
            ("market_bars_weekly", "idx_market_bars_weekly_code_trade_date", ["code", "trade_date"]),
            ("market_bars_monthly", "idx_market_bars_monthly_code_trade_date", ["code", "trade_date"]),
            ("daily_lhb", "idx_daily_lhb_code_trade_date", ["code", "trade_date"]),
            ("stock_fund_flows", "idx_stock_fund_flows_code_date_period_source", ["code", "trade_date", "period", "source"]),
            ("baidu_stock_fund_flows", "idx_baidu_stock_fund_flows_code_date_source", ["code", "trade_date", "source"]),
            ("board_fund_flows", "idx_board_fund_flows_name_kind_date_period_source", ["board_name", "kind", "trade_date", "period", "source"]),
            ("lhb_details", "idx_lhb_details_code_date_reason_source", ["code", "trade_date", "reason", "source"]),
            ("restricted_releases", "idx_restricted_releases_code_date_type_source", ["code", "release_date", "holder_type", "source"]),
            ("concept_boards", "idx_concept_boards_name_source", ["concept_name", "source"]),
            ("stock_concepts", "idx_stock_concepts_code_name_source", ["code", "concept_name", "source"]),
            ("stock_profiles", "idx_stock_profiles_code_source", ["code", "source"]),
            ("research_reports", "idx_research_reports_code_info_source", ["code", "info_code", "source"]),
            ("eps_forecasts", "idx_eps_forecasts_code_year_source", ["code", "forecast_year", "source"]),
            ("stock_news", "idx_stock_news_code_title_datetime_source", ["code", "title", "datetime", "source"]),
            ("market_news", "idx_market_news_title_datetime_source", ["title", "datetime", "source"]),
            ("company_announcements", "idx_company_announcements_code_id_source", ["code", "announcement_id", "source"]),
            ("financial_metrics", "idx_financial_metrics_code_report_date", ["code", "report_date"]),
            ("watchlist_items", "idx_watchlist_items_code", ["code"]),
            ("trading_account", "idx_trading_account_name", ["account_name"]),
        ]

        for table_name, index_name, columns in unique_indexes:
            column_sql = ", ".join(columns)
            cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_sql})")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_bars_period_trade_date "
            "ON market_bars (period, trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_bars_weekly_trade_date "
            "ON market_bars_weekly (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_bars_monthly_trade_date "
            "ON market_bars_monthly (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_quotes_trade_date "
            "ON daily_quotes (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_fund_flows_trade_date "
            "ON stock_fund_flows (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_baidu_stock_fund_flows_trade_date "
            "ON baidu_stock_fund_flows (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_board_fund_flows_trade_date "
            "ON board_fund_flows (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_lhb_details_trade_date "
            "ON lhb_details (trade_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_restricted_releases_release_date "
            "ON restricted_releases (release_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_concepts_concept_name "
            "ON stock_concepts (concept_name)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_profiles_industry_region "
            "ON stock_profiles (industry, region)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_research_reports_publish_date "
            "ON research_reports (publish_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_news_datetime "
            "ON stock_news (datetime)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_news_datetime "
            "ON market_news (datetime)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_company_announcements_date "
            "ON company_announcements (date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_financial_metrics_report_date "
            "ON financial_metrics (report_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchlist_items_status "
            "ON watchlist_items (watch_status, updated_at)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_trading_positions_account_status "
            "ON trading_positions (account_id, status)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_trading_positions_code_status "
            "ON trading_positions (code, status)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_orders_account_created "
            "ON trade_orders (account_id, created_at)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_signal_date "
            "ON backtest_signal_snapshots (signal_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_signal_strategy "
            "ON backtest_signal_snapshots (primary_strategy, signal_date)"
        )

    def insert_dataframe(self, table_name: str, df: pd.DataFrame, if_exists: str = "replace") -> bool:
        if df is None or df.empty:
            logger.warning("No rows to insert into {}", table_name)
            return False

        try:
            with self.get_connection() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            logger.info("Inserted {} rows into {} ({})", len(df), table_name, if_exists)
            return True
        except Exception as exc:
            logger.error("Insert into {} failed: {}", table_name, exc)
            return False

    def upsert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        key_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> bool:
        if df is None or df.empty:
            logger.warning("No rows to upsert into {}", table_name)
            return False

        if not key_columns:
            logger.error("UPSERT {} failed: key_columns is empty", table_name)
            return False

        missing_key_columns = [col for col in key_columns if col not in df.columns]
        if missing_key_columns:
            logger.error("UPSERT {} failed: missing key columns {}", table_name, missing_key_columns)
            return False

        all_columns = list(df.columns)
        if update_columns is None:
            update_columns = [col for col in all_columns if col not in key_columns]

        placeholders = ", ".join(["?"] * len(all_columns))
        column_sql = ", ".join(all_columns)
        conflict_sql = ", ".join(key_columns)

        if update_columns:
            update_sql = ", ".join([f"{col}=excluded.{col}" for col in update_columns])
            sql = (
                f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders}) "
                f"ON CONFLICT({conflict_sql}) DO UPDATE SET {update_sql}"
            )
        else:
            sql = f"INSERT OR IGNORE INTO {table_name} ({column_sql}) VALUES ({placeholders})"

        rows = [
            tuple(None if pd.isna(value) else value for value in row)
            for row in df[all_columns].itertuples(index=False, name=None)
        ]

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, rows)
                conn.commit()
            logger.info("Successfully upserted {} rows into {}", len(df), table_name)
            return True
        except Exception as exc:
            logger.error("UPSERT into {} failed: {}", table_name, exc)
            return False

    def query_to_dataframe(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        try:
            with self.get_connection() as conn:
                return pd.read_sql_query(sql, conn, params=params)
        except Exception as exc:
            logger.error("SQL query failed: {}\nSQL: {}", exc, sql)
            return pd.DataFrame()

    def execute_non_query(self, sql: str, params: tuple = ()) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                conn.commit()
            return True
        except Exception as exc:
            logger.error("SQL execution failed: {}\nSQL: {}", exc, sql)
            return False
