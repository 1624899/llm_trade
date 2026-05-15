"""SQLite storage helpers for the local stock data lake."""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from src.database_schema import COLUMN_MIGRATIONS, NON_UNIQUE_INDEXES, TABLE_DEFINITIONS, UNIQUE_INDEXES


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
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                for query in TABLE_DEFINITIONS:
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
        for table_name, columns in COLUMN_MIGRATIONS.items():
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
        for table_name, index_name, columns in UNIQUE_INDEXES:
            column_sql = ", ".join(columns)
            cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_sql})")

        for table_name, index_name, columns in NON_UNIQUE_INDEXES:
            column_sql = ", ".join(columns)
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_sql})")

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
