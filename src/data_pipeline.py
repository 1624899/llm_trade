"""
离线数据同步管道 (Data Pipeline)
设计目标：在交易时间之外（如盘后）运行，将网络上容易限流的庞大数据，
全量一次性拉取并清洗入库至本地 SQLite 数据湖中。
"""

import akshare as ak
import glob
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
from loguru import logger
import yaml

from src.database import StockDatabase
from src.financial_data import FinancialDataProvider
from src.quote_sources import fetch_sina_quotes, fetch_tencent_quotes


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

DEFAULT_INDEX_BARS = {
    "sh000001": "上证指数",
    "sh000300": "沪深300",
    "sz399006": "创业板指",
}


class DataPipeline:
    def __init__(self):
        self.db = StockDatabase()
        self.yahoo_batch_size = 120
        self.enable_cleanup = True
        self.market_data_retention_days = 30
        self.macro_events_retention_days = 30
        self.output_retention_days = 30
        self.trade_execution_retention_days = 365
        self.daily_quotes_retention_days = 45
        self.market_bars_daily_retention_days = 540
        self.market_bars_weekly_retention_days = 1825
        self.market_bars_monthly_retention_days = 3650
        self.daily_lhb_retention_days = 180
        self.paper_trades_retention_days = 365
        self.alpha_vantage_base_url = "https://www.alphavantage.co/query"
        self.alpha_vantage_api_key = None
        self.alpha_vantage_daily_limit = 25
        self.alpha_vantage_request_interval = 12.0
        self.financial_data_provider = FinancialDataProvider()
        self._load_pipeline_settings()
        logger.info("离线数据同步管道初始化完毕")

    def _load_pipeline_settings(self):
        """读取数据管道配置，环境变量优先，其次 config.yaml。"""
        self.alpha_vantage_api_key = os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHA_VANTAGE_API_KEY")

        env_daily_limit = os.getenv("ALPHA_VANTAGE_DAILY_LIMIT")
        env_request_interval = os.getenv("ALPHA_VANTAGE_REQUEST_INTERVAL")

        if env_daily_limit:
            self.alpha_vantage_daily_limit = int(env_daily_limit)
        if env_request_interval:
            self.alpha_vantage_request_interval = float(env_request_interval)

        config_path = os.path.join("config", "config.yaml")
        if not os.path.exists(config_path):
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"读取数据管道配置失败: {e}")
            return

        normalized_config = {
            str(key).strip().lower().replace(" ", "_"): value
            for key, value in config.items()
        }
        alpha_cfg = normalized_config.get("alpha_vantage") or {}
        data_cfg = config.get("data", {}) or {}

        if not self.alpha_vantage_api_key:
            self.alpha_vantage_api_key = self._resolve_env_value(alpha_cfg.get("api_key") or alpha_cfg.get("apikey"))

        if env_daily_limit is None and alpha_cfg.get("daily_limit") is not None:
            self.alpha_vantage_daily_limit = int(alpha_cfg["daily_limit"])

        if env_request_interval is None and alpha_cfg.get("request_interval") is not None:
            self.alpha_vantage_request_interval = float(alpha_cfg["request_interval"])

        self.enable_cleanup = bool(data_cfg.get("enable_cleanup", self.enable_cleanup))
        self.market_data_retention_days = int(
            data_cfg.get("market_data_retention_days", self.market_data_retention_days)
        )
        self.macro_events_retention_days = int(
            data_cfg.get("macro_events_retention_days", self.macro_events_retention_days)
        )
        self.output_retention_days = int(data_cfg.get("output_retention_days", self.output_retention_days))
        self.trade_execution_retention_days = int(
            data_cfg.get("trade_execution_retention_days", self.trade_execution_retention_days)
        )
        self.daily_quotes_retention_days = int(
            data_cfg.get("daily_quotes_retention_days", self.daily_quotes_retention_days)
        )
        self.market_bars_daily_retention_days = int(
            data_cfg.get("market_bars_daily_retention_days", self.market_bars_daily_retention_days)
        )
        self.market_bars_weekly_retention_days = int(
            data_cfg.get("market_bars_weekly_retention_days", self.market_bars_weekly_retention_days)
        )
        self.market_bars_monthly_retention_days = int(
            data_cfg.get("market_bars_monthly_retention_days", self.market_bars_monthly_retention_days)
        )
        self.daily_lhb_retention_days = int(
            data_cfg.get("daily_lhb_retention_days", self.daily_lhb_retention_days)
        )
        self.paper_trades_retention_days = int(
            data_cfg.get("paper_trades_retention_days", self.paper_trades_retention_days)
        )

    @staticmethod
    def _resolve_env_value(value):
        if isinstance(value, str) and value.startswith("env:"):
            return os.getenv(value.split(":", 1)[1], "")
        return value

    def _safe_fetch(self, func, *args, retries=3, delay=5, **kwargs):
        """安全的数据获取，带重试机制"""
        for i in range(retries):
            try:
                res = func(*args, **kwargs)
                if res is not None and not res.empty:
                    return res
                logger.warning(f"获取数据为空，准备重试 ({i+1}/{retries})")
            except Exception as e:
                logger.error(f"调用 {func.__name__} 失败: {e}")
            if i < retries - 1:
                time.sleep(delay * (i + 1))
        return None

    def sync_daily_quotes(self):
        """Sync market snapshot with tested sources first."""
        if self._daily_quotes_is_fresh():
            logger.info("daily_quotes already covers the latest trade date; skip duplicate fetch")
            return True

        logger.info("Start syncing market snapshot...")
        today_str = datetime.now().strftime("%Y%m%d")

        codes = self._load_stock_codes_from_lake()
        df = self._fetch_primary_daily_quotes(codes)
        if df is None or df.empty:
            logger.warning("Primary snapshot sources unavailable, trying AKShare as backup")
            df = self._safe_fetch(ak.stock_zh_a_spot_em)
            if df is not None and not df.empty:
                return self._upsert_daily_quotes(self._normalize_ak_spot_quotes(df, today_str))

            logger.warning("AKShare snapshot backup unavailable, trying Yahoo daily fallback")
            df = self._build_daily_quotes_from_yahoo()
            if df is None or df.empty:
                logger.error("Failed to fetch market snapshot from all sources")
                return False
            return self._upsert_daily_quotes(df)

        return self._upsert_daily_quotes(df)

    def _fetch_primary_daily_quotes(self, codes):
        """Use tested quote sources first: Tencent -> Sina."""
        if not codes:
            return None

        for source_name, fetcher in (("Tencent", fetch_tencent_quotes), ("Sina", fetch_sina_quotes)):
            try:
                df = fetcher(codes)
            except Exception as exc:
                logger.warning(f"{source_name} snapshot source unavailable: {exc}")
                continue
            if df is not None and not df.empty:
                logger.info(f"{source_name} snapshot source succeeded: {len(df)} rows")
                clean_df = self._clean_daily_quotes(df)
                return self._enrich_daily_quote_valuations(clean_df, codes)
        return None

    def _enrich_daily_quote_valuations(self, clean_df: pd.DataFrame, codes) -> pd.DataFrame:
        """用 AKShare/东方财富快照补齐 PE、PB、总市值，避免行情源估值字段缺失。"""
        if clean_df is None or clean_df.empty:
            return clean_df

        if self._valuation_fields_are_usable(clean_df):
            return clean_df

        logger.info("daily_quotes 估值字段覆盖不足，尝试用 AKShare/东方财富快照补齐")
        ak_df = self._safe_fetch(ak.stock_zh_a_spot_em, retries=1, delay=1)
        if ak_df is None or ak_df.empty:
            logger.warning("AKShare/东方财富估值补齐失败，保留原始行情快照")
            return clean_df

        trade_date = str(clean_df["trade_date"].dropna().iloc[0]) if "trade_date" in clean_df.columns and clean_df["trade_date"].notna().any() else datetime.now().strftime("%Y%m%d")
        valuation_df = self._normalize_ak_spot_quotes(ak_df, trade_date)
        code_set = {str(code).zfill(6) for code in codes}
        valuation_df = valuation_df[valuation_df["code"].astype(str).str.zfill(6).isin(code_set)]
        if valuation_df.empty:
            logger.warning("AKShare/东方财富估值补齐没有匹配到当前股票池")
            return clean_df

        enrich_cols = ["code", "pe_ttm", "pb", "total_market_cap"]
        merged = clean_df.merge(
            valuation_df[enrich_cols],
            on="code",
            how="left",
            suffixes=("", "_ak"),
        )
        for col in ["pe_ttm", "pb", "total_market_cap"]:
            primary = pd.to_numeric(merged[col], errors="coerce")
            fallback = pd.to_numeric(merged[f"{col}_ak"], errors="coerce")
            merged[col] = primary.where(primary.notna(), fallback)
            merged = merged.drop(columns=[f"{col}_ak"])

        logger.info("AKShare/东方财富估值补齐完成：{} 行", len(merged))
        return self._clean_daily_quotes(merged)

    def _valuation_fields_are_usable(self, df: pd.DataFrame, min_coverage: float = 0.8) -> bool:
        """检查估值字段覆盖率，至少要求总市值大面积可用。"""
        if df is None or df.empty or "total_market_cap" not in df.columns:
            return False
        cap_coverage = pd.to_numeric(df["total_market_cap"], errors="coerce").notna().mean()
        pb_coverage = pd.to_numeric(df.get("pb"), errors="coerce").notna().mean() if "pb" in df.columns else 0
        pe_coverage = pd.to_numeric(df.get("pe_ttm"), errors="coerce").notna().mean() if "pe_ttm" in df.columns else 0
        return cap_coverage >= min_coverage and (pb_coverage >= min_coverage or pe_coverage >= min_coverage)

    def _normalize_ak_spot_quotes(self, df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """将 AKShare 东方财富全市场快照标准化为 daily_quotes 结构。"""
        column_mapping = {
            '代码': 'code',
            '最新价': 'price',
            '涨跌幅': 'change_pct',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe_ttm',
            '市净率': 'pb',
            '总市值': 'total_market_cap',
        }
        available_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        clean_df = df[list(available_cols.keys())].rename(columns=available_cols)
        clean_df['trade_date'] = trade_date
        return self._clean_daily_quotes(clean_df)

    def _clean_daily_quotes(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗 daily_quotes 的公共字段。"""
        clean_df = df.copy()
        numeric_cols = [
            'price', 'change_pct', 'volume', 'amount', 'turnover_rate',
            'pe_ttm', 'pb', 'total_market_cap'
        ]
        for col in numeric_cols:
            if col in clean_df.columns:
                clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')

        for col in numeric_cols:
            if col not in clean_df.columns:
                clean_df[col] = None

        clean_df = clean_df.dropna(subset=['code'])
        clean_df['code'] = clean_df['code'].astype(str).str.zfill(6)
        return clean_df[
            [
                'code', 'trade_date', 'price', 'change_pct', 'volume', 'amount',
                'turnover_rate', 'pe_ttm', 'pb', 'total_market_cap'
            ]
        ]

    def _upsert_daily_quotes(self, clean_df: pd.DataFrame) -> bool:
        """写入 daily_quotes。"""
        try:
            self.db.upsert_dataframe(
                "daily_quotes",
                clean_df,
                key_columns=["code", "trade_date"]
            )
            logger.info("全市场基本行情同步通过")
            return True
            
        except Exception as e:
            logger.error(f"行情数据清洗入库失败: {e}")
            return False

    def sync_market_bars(self, codes=None, periods=("daily", "weekly", "monthly")) -> bool:
        """同步日/周/月 K 线到 market_bars。

        数据源顺序：Yahoo Finance -> Alpha Vantage -> AKShare 单股历史。
        Yahoo Finance 适合非交易时段批量补历史 K 线；Alpha Vantage 受免费额度限制，
        更适合作为候选股级别补数；AKShare 作为最后兜底。
        """
        codes = codes or self._load_stock_codes_from_lake()
        if not codes:
            logger.error("没有可同步 K 线的股票代码，请先同步 stock_basic")
            return False

        all_success = True
        for period in periods:
            period = self._normalize_period(period)
            pending_codes = self._filter_codes_needing_bars(codes, period)
            if not pending_codes:
                logger.info(f"{period} K 线已是最新，无需重复拉取")
                continue

            skipped_count = len(codes) - len(pending_codes)
            logger.info(
                f"开始增量同步 {period} K 线，需要更新 {len(pending_codes)} 只，"
                f"跳过已更新 {skipped_count} 只"
            )

            bars_df = self._fetch_yahoo_bars(pending_codes, period)
            if bars_df is None or bars_df.empty:
                bars_df = self._fetch_alpha_vantage_bars(pending_codes, period)
            if bars_df is None or bars_df.empty:
                bars_df = self._fetch_ak_bars(pending_codes, period)

            if bars_df is None or bars_df.empty:
                logger.error(f"{period} K 线同步失败")
                all_success = False
                continue

            bars_df = self._clean_market_bars(bars_df, period)
            ok = self.db.upsert_dataframe(
                "market_bars",
                bars_df,
                key_columns=["code", "period", "trade_date"],
            )
            all_success = all_success and ok

        return all_success

    def sync_index_bars(self) -> bool:
        """Sync broad index daily bars used by MarketRegimeDetector."""
        rows = []
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for code, name in DEFAULT_INDEX_BARS.items():
            try:
                raw = ak.stock_zh_index_daily(symbol=code)
            except Exception as exc:
                logger.warning(f"指数 K 线同步失败 {name}({code}): {exc}")
                continue
            if raw is None or raw.empty:
                logger.warning(f"指数 K 线为空 {name}({code})")
                continue
            rows.extend(self._normalize_index_bars(code, raw, fetched_at))

        if not rows:
            logger.warning("指数 K 线同步未获取到有效数据")
            return False

        bars_df = self._clean_market_bars(pd.DataFrame(rows), "daily")
        ok = self.db.upsert_dataframe(
            "market_bars",
            bars_df,
            key_columns=["code", "period", "trade_date"],
        )
        if ok:
            logger.info(f"指数 K 线同步完成: {len(bars_df)} 条")
        return ok

    def sync_financial_metrics(self, codes=None, periods: int = 8) -> bool:
        """Sync compact financial metrics from Eastmoney via AKShare."""
        codes = codes or self._load_stock_codes_from_lake()
        if not codes:
            logger.warning("No stock codes available for financial metric sync")
            return False

        all_success = True
        for code in codes:
            try:
                metrics = self.financial_data_provider.fetch_financial_metrics(code, periods=periods)
            except Exception as exc:
                logger.warning(f"Financial metric fetch failed for {code}: {exc}")
                all_success = False
                continue

            if metrics is None or metrics.empty:
                logger.warning(f"Financial metric fetch returned empty data for {code}")
                all_success = False
                continue

            ok = self.db.upsert_dataframe(
                "financial_metrics",
                metrics,
                key_columns=["code", "report_date"],
            )
            all_success = all_success and ok
            time.sleep(0.2)

        return all_success

    def _normalize_index_bars(self, code: str, raw: pd.DataFrame, fetched_at: str) -> list:
        column_mapping = {
            "date": "trade_date",
            "日期": "trade_date",
            "open": "open",
            "开盘": "open",
            "high": "high",
            "最高": "high",
            "low": "low",
            "最低": "low",
            "close": "close",
            "收盘": "close",
            "volume": "volume",
            "成交量": "volume",
            "amount": "amount",
            "成交额": "amount",
        }
        available = {src: dst for src, dst in column_mapping.items() if src in raw.columns}
        if "trade_date" not in available.values() or "close" not in available.values():
            logger.warning(f"指数 K 线字段不完整 {code}: {list(raw.columns)}")
            return []

        df = raw[list(available.keys())].rename(columns=available).copy()
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = None
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
        df = df.dropna(subset=["trade_date", "close"])
        df["code"] = str(code)
        df["period"] = "daily"
        df["adj_close"] = df["close"]
        df["source"] = "akshare_index"
        df["fetched_at"] = fetched_at
        return df[
            [
                "code",
                "period",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "amount",
                "source",
                "fetched_at",
            ]
        ].to_dict("records")

    def _load_stock_codes_from_lake(self):
        """优先从本地 stock_basic 取代码，空库时再调用轻量接口。"""
        df = self.db.query_to_dataframe("SELECT code FROM stock_basic ORDER BY code")
        if df is not None and not df.empty:
            return df["code"].astype(str).str.zfill(6).tolist()

        code_df = self._safe_fetch(ak.stock_info_a_code_name, retries=2, delay=2)
        if code_df is None or code_df.empty:
            return []
        if "code" not in code_df.columns and "代码" in code_df.columns:
            code_df = code_df.rename(columns={"代码": "code"})
        if "code" not in code_df.columns:
            return []
        return code_df["code"].astype(str).str.zfill(6).tolist()

    def _normalize_period(self, period: str) -> str:
        aliases = {"1d": "daily", "day": "daily", "1w": "weekly", "week": "weekly", "1mo": "monthly", "month": "monthly"}
        normalized = aliases.get(str(period).lower(), str(period).lower())
        if normalized not in {"daily", "weekly", "monthly"}:
            raise ValueError(f"不支持的 K 线周期: {period}")
        return normalized

    def _filter_codes_needing_bars(self, codes, period: str):
        """只返回本地 K 线明显过期或缺失的代码。"""
        target_date = self._expected_latest_trade_date(period)
        codes = [str(code).zfill(6) for code in codes]

        sql = (
            "SELECT code, MAX(trade_date) AS latest_trade_date "
            "FROM market_bars "
            "WHERE period = ? "
            "GROUP BY code"
        )
        latest_df = self.db.query_to_dataframe(sql, (period,))
        if latest_df is None or latest_df.empty:
            logger.info(f"{period} K 线本地无记录，将执行首次同步")
            return codes

        latest_map = dict(zip(latest_df["code"].astype(str).str.zfill(6), latest_df["latest_trade_date"].astype(str)))
        pending_codes = [
            code
            for code in codes
            if self._bar_is_stale(latest_map.get(code), target_date)
        ]
        logger.info(
            f"{period} K 线增量检查: 目标日期 {target_date}, "
            f"已有记录 {len(latest_map)} 只, 待更新 {len(pending_codes)} 只"
        )
        return pending_codes

    def _expected_latest_trade_date(self, period: str) -> str:
        """估算当前周期应至少覆盖到的日期，避免每天重复拉完整历史。"""
        today = datetime.now().date()
        if period == "daily":
            expected = today
            while expected.weekday() >= 5:
                expected -= timedelta(days=1)
            return expected.strftime("%Y%m%d")

        if period == "weekly":
            return (today - timedelta(days=7)).strftime("%Y%m%d")

        first_day_this_month = today.replace(day=1)
        first_day_prev_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
        return first_day_prev_month.strftime("%Y%m%d")

    def _bar_is_stale(self, latest_trade_date, target_date: str) -> bool:
        if not latest_trade_date or latest_trade_date == "nan":
            return True
        latest = str(latest_trade_date).replace("-", "")
        return latest < target_date

    def _fetch_yahoo_bars(self, codes, period: str):
        """从 Yahoo Finance 批量拉取 A 股日/周/月 K 线。"""
        try:
            import yfinance as yf
        except ImportError:
            logger.warning("未安装 yfinance，无法使用 Yahoo Finance 降级数据源")
            return None

        interval_map = {"daily": "1d", "weekly": "1wk", "monthly": "1mo"}
        lookback_map = {"daily": "2y", "weekly": "5y", "monthly": "10y"}
        interval = interval_map[period]
        lookback = lookback_map[period]
        rows = []

        symbol_to_code = {
            self._to_yahoo_symbol(code): str(code).zfill(6)
            for code in codes
            if self._to_yahoo_symbol(code)
        }
        symbols = list(symbol_to_code.keys())
        total_batches = (len(symbols) + self.yahoo_batch_size - 1) // self.yahoo_batch_size
        if not symbols:
            logger.warning(f"Yahoo Finance 无可用 ticker，跳过 {period} K 线")
            return None

        logger.info(
            f"Yahoo Finance 开始拉取 {period} K 线: {len(symbols)} 个 ticker, "
            f"{total_batches} 批, lookback={lookback}"
        )

        for batch_no, start in enumerate(range(0, len(symbols), self.yahoo_batch_size), start=1):
            batch = symbols[start:start + self.yahoo_batch_size]
            logger.info(f"Yahoo Finance {period} K 线进度: {batch_no}/{total_batches}, 本批 {len(batch)} 个")
            try:
                raw = yf.download(
                    tickers=" ".join(batch),
                    period=lookback,
                    interval=interval,
                    group_by="ticker",
                    auto_adjust=False,
                    progress=False,
                    threads=True,
                )
            except Exception as e:
                logger.warning(f"Yahoo Finance 批量 K 线拉取失败: {e}")
                continue

            rows.extend(self._parse_yahoo_download(raw, batch, symbol_to_code, period))
            time.sleep(0.3)

        if not rows:
            return None
        logger.info(f"Yahoo Finance {period} K 线拉取完成: {len(rows)} 条")
        return pd.DataFrame(rows)

    def cleanup_old_data(self) -> bool:
        """清理临时行情缓存、旧输出和数据库重复行。"""
        logger.info("开始清理旧数据与临时缓存...")
        ok = True
        ok = self._deduplicate_database_tables() and ok
        ok = self._prune_database_history() and ok
        ok = self._cleanup_files("data/market_data/*", self.market_data_retention_days) and ok
        ok = self._cleanup_files("data/Macro events/**/*.csv", self.macro_events_retention_days, recursive=True) and ok
        ok = self._cleanup_files("outputs/trading_prompt_*.md", self.output_retention_days) and ok
        ok = self._cleanup_files("data/trade_executions/execution_*.json", self.trade_execution_retention_days) and ok
        return ok

    def _deduplicate_database_tables(self) -> bool:
        statements = [
            """
            DELETE FROM stock_basic
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM stock_basic GROUP BY code
            )
            """,
            """
            DELETE FROM daily_quotes
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM daily_quotes GROUP BY code, trade_date
            )
            """,
            """
            DELETE FROM market_bars
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM market_bars GROUP BY code, period, trade_date
            )
            """,
        ]
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                for sql in statements:
                    cursor.execute(sql)
                conn.commit()
                conn.execute("VACUUM")
            logger.info("数据库重复行清理完成")
            return True
        except Exception as e:
            logger.warning(f"数据库清理失败: {e}")
            return False

    def _prune_database_history(self) -> bool:
        """Keep the local lake compact while preserving enough history for analysis."""
        try:
            retention_specs = [
                ("daily_quotes", None, self.daily_quotes_retention_days),
                ("market_bars", "daily", self._market_bars_retention_days("daily")),
                ("market_bars", "weekly", self._market_bars_retention_days("weekly")),
                ("market_bars", "monthly", self._market_bars_retention_days("monthly")),
                ("daily_lhb", None, self.daily_lhb_retention_days),
            ]

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                total_deleted = 0

                for table_name, period, retention_days in retention_specs:
                    if retention_days < 0:
                        continue

                    cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y%m%d")
                    if period is None:
                        cursor.execute(
                            f"DELETE FROM {table_name} WHERE REPLACE(trade_date, '-', '') < ?",
                            (cutoff,),
                        )
                    else:
                        cursor.execute(
                            "DELETE FROM market_bars "
                            "WHERE period = ? AND REPLACE(trade_date, '-', '') < ?",
                            (period, cutoff),
                        )
                    total_deleted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

                if self.paper_trades_retention_days >= 0:
                    cutoff = (datetime.now() - timedelta(days=self.paper_trades_retention_days)).strftime("%Y-%m-%d")
                    cursor.execute(
                        "DELETE FROM paper_trades "
                        "WHERE COALESCE(status, '') != 'HOLD' "
                        "AND COALESCE(recommend_date, '') < ?",
                        (cutoff,),
                    )
                    total_deleted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

                conn.commit()
                if total_deleted:
                    conn.execute("VACUUM")
                    logger.info(f"数据库历史裁剪完成，删除 {total_deleted} 条过期记录")
                else:
                    logger.info("数据库历史裁剪完成，无过期记录")
            return True
        except Exception as e:
            logger.warning(f"数据库历史裁剪失败: {e}")
            return False

    def _market_bars_retention_days(self, period: str) -> int:
        retention_map = {
            "daily": self.market_bars_daily_retention_days,
            "weekly": self.market_bars_weekly_retention_days,
            "monthly": self.market_bars_monthly_retention_days,
        }
        return retention_map.get(period, self.market_bars_daily_retention_days)

    def _cleanup_files(self, pattern: str, retention_days: int, recursive: bool = False) -> bool:
        if retention_days < 0:
            return True

        cutoff = time.time() - retention_days * 86400
        deleted = 0
        for file_path in glob.glob(pattern, recursive=recursive):
            if not os.path.isfile(file_path):
                continue
            if os.path.basename(file_path) in {"latest_report.md", "execution_history.json"}:
                continue
            try:
                if os.path.getmtime(file_path) < cutoff:
                    os.remove(file_path)
                    deleted += 1
            except Exception as e:
                logger.warning(f"删除旧文件失败 {file_path}: {e}")
                return False

        if deleted:
            logger.info(f"清理旧文件 {pattern}: 删除 {deleted} 个")
        return True

    def _to_yahoo_symbol(self, code: str) -> str:
        """A 股代码转 Yahoo Finance ticker。"""
        code = str(code).zfill(6)
        if code.startswith(("600", "601", "603", "605", "688", "689")):
            return f"{code}.SS"
        if code.startswith(("000", "001", "002", "003", "300", "301")):
            return f"{code}.SZ"
        return ""

    def _to_alpha_vantage_symbol(self, code: str) -> str:
        """A 股代码转 Alpha Vantage ticker。"""
        code = str(code).zfill(6)
        if code.startswith(("600", "601", "603", "605", "688", "689")):
            return f"{code}.SHH"
        if code.startswith(("000", "001", "002", "003", "300", "301")):
            return f"{code}.SHZ"
        return ""

    def _fetch_alpha_vantage_bars(self, codes, period: str):
        """从 Alpha Vantage 拉取 A 股历史 K 线。

        由于免费额度严格，这里只适合小批量补数，不适合全市场同步。
        """
        if not self.alpha_vantage_api_key:
            logger.info("未配置 Alpha Vantage API Key，跳过该数据源")
            return None

        if len(codes) > self.alpha_vantage_daily_limit:
            logger.warning(
                f"Alpha Vantage 免费额度仅适合小批量补数，当前请求 {len(codes)} 只，"
                f"超过限制 {self.alpha_vantage_daily_limit}，跳过"
            )
            return None

        function_map = {
            "daily": ("TIME_SERIES_DAILY_ADJUSTED", "Time Series (Daily)"),
            "weekly": ("TIME_SERIES_WEEKLY_ADJUSTED", "Weekly Adjusted Time Series"),
            "monthly": ("TIME_SERIES_MONTHLY_ADJUSTED", "Monthly Adjusted Time Series"),
        }
        function_name, series_key = function_map[period]
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []

        for idx, code in enumerate(codes):
            symbol = self._to_alpha_vantage_symbol(code)
            if not symbol:
                continue

            data = self._request_alpha_vantage_series(symbol, function_name)
            if not data:
                continue

            series = data.get(series_key)
            if not isinstance(series, dict) or not series:
                logger.warning(f"Alpha Vantage 返回缺少时间序列: {symbol}")
                continue

            rows.extend(self._parse_alpha_vantage_series(code, period, series, fetched_at))

            if idx < len(codes) - 1:
                time.sleep(self.alpha_vantage_request_interval)

        if not rows:
            return None
        return pd.DataFrame(rows)

    def _request_alpha_vantage_series(self, symbol: str, function_name: str):
        """请求 Alpha Vantage 时间序列接口。"""
        params = {
            "function": function_name,
            "symbol": symbol,
            "apikey": self.alpha_vantage_api_key,
            "outputsize": "full",
            "datatype": "json",
        }

        # 周线/月线接口不接受 outputsize，但额外参数通常会被忽略；这里保持简单实现。
        if function_name != "TIME_SERIES_DAILY_ADJUSTED":
            params.pop("outputsize", None)

        try:
            response = requests.get(self.alpha_vantage_base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.warning(f"Alpha Vantage 请求失败 {symbol}: {e}")
            return None

        if not isinstance(data, dict):
            logger.warning(f"Alpha Vantage 返回格式异常: {symbol}")
            return None

        if data.get("Note"):
            logger.warning(f"Alpha Vantage 触发限频: {data['Note']}")
            return None

        if data.get("Error Message"):
            logger.warning(f"Alpha Vantage 返回错误 {symbol}: {data['Error Message']}")
            return None

        if "Information" in data and not any("Time Series" in key for key in data.keys()):
            logger.warning(f"Alpha Vantage 提示信息 {symbol}: {data['Information']}")
            return None

        return data

    def _parse_alpha_vantage_series(self, code: str, period: str, series: dict, fetched_at: str):
        """解析 Alpha Vantage 时间序列。"""
        rows = []
        for trade_date, values in series.items():
            rows.append(
                {
                    "code": str(code).zfill(6),
                    "period": period,
                    "trade_date": str(trade_date).replace("-", ""),
                    "open": values.get("1. open"),
                    "high": values.get("2. high"),
                    "low": values.get("3. low"),
                    "close": values.get("4. close"),
                    "adj_close": values.get("5. adjusted close", values.get("4. close")),
                    "volume": values.get("6. volume", values.get("5. volume")),
                    "amount": None,
                    "source": "alpha_vantage",
                    "fetched_at": fetched_at,
                }
            )
        return rows

    def _parse_yahoo_download(self, raw: pd.DataFrame, batch, symbol_to_code, period: str):
        """解析 yfinance.download 返回的宽表。"""
        if raw is None or raw.empty:
            return []

        rows = []
        is_multi = isinstance(raw.columns, pd.MultiIndex)
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for symbol in batch:
            if is_multi:
                if symbol not in raw.columns.get_level_values(0):
                    continue
                symbol_df = raw[symbol].copy()
            else:
                symbol_df = raw.copy()

            symbol_df = symbol_df.dropna(how="all")
            if symbol_df.empty:
                continue

            code = symbol_to_code[symbol]
            for date, row in symbol_df.iterrows():
                close = row.get("Close")
                if pd.isna(close):
                    continue
                rows.append(
                    {
                        "code": code,
                        "period": period,
                        "trade_date": pd.to_datetime(date).strftime("%Y%m%d"),
                        "open": row.get("Open"),
                        "high": row.get("High"),
                        "low": row.get("Low"),
                        "close": close,
                        "adj_close": row.get("Adj Close"),
                        "volume": row.get("Volume"),
                        "amount": None,
                        "source": "yahoo_finance",
                        "fetched_at": fetched_at,
                    }
                )
        return rows

    def _fetch_ak_bars(self, codes, period: str):
        """用 AKShare 单股历史接口兜底，适合候选股级别的小批量补数。"""
        if len(codes) > 200:
            logger.warning(f"AKShare 单股 K 线兜底仅适合小批量，当前 {len(codes)} 只，跳过")
            return None

        rows = []
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = "20180101" if period != "daily" else "20220101"
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for code in codes:
            try:
                df = ak.stock_zh_a_hist(
                    symbol=str(code).zfill(6),
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
            except Exception as e:
                logger.warning(f"AKShare K 线拉取失败 {code}: {e}")
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                rows.append(
                    {
                        "code": str(code).zfill(6),
                        "period": period,
                        "trade_date": str(row.get("日期", "")).replace("-", ""),
                        "open": row.get("开盘"),
                        "high": row.get("最高"),
                        "low": row.get("最低"),
                        "close": row.get("收盘"),
                        "adj_close": row.get("收盘"),
                        "volume": row.get("成交量"),
                        "amount": row.get("成交额"),
                        "source": "akshare",
                        "fetched_at": fetched_at,
                    }
                )
            time.sleep(0.2)

        if not rows:
            return None
        return pd.DataFrame(rows)

    def _clean_market_bars(self, df: pd.DataFrame, period: str) -> pd.DataFrame:
        """标准化 market_bars 字段与类型。"""
        clean_df = df.copy()
        clean_df["code"] = clean_df["code"].astype(str).str.zfill(6)
        clean_df["period"] = period
        clean_df["trade_date"] = clean_df["trade_date"].astype(str).str.replace("-", "", regex=False)

        numeric_cols = ["open", "high", "low", "close", "adj_close", "volume", "amount"]
        for col in numeric_cols:
            clean_df[col] = pd.to_numeric(clean_df.get(col), errors="coerce")

        clean_df = clean_df.dropna(subset=["code", "trade_date", "close"])
        retention_days = self._market_bars_retention_days(period)
        if retention_days >= 0:
            cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y%m%d")
            clean_df = clean_df[clean_df["trade_date"] >= cutoff]
        return clean_df[
            [
                "code", "period", "trade_date", "open", "high", "low", "close",
                "adj_close", "volume", "amount", "source", "fetched_at"
            ]
        ]

    def _build_daily_quotes_from_yahoo(self):
        """用 Yahoo Finance 最新两根日线生成 daily_quotes 降级快照。"""
        codes = self._load_stock_codes_from_lake()
        bars = self._fetch_yahoo_bars(codes, "daily") if codes else None
        if bars is None or bars.empty:
            return None

        rows = []
        for code, group in bars.sort_values("trade_date").groupby("code"):
            tail = group.tail(2)
            if tail.empty:
                continue
            latest = tail.iloc[-1]
            prev_close = tail.iloc[-2]["close"] if len(tail) >= 2 else None
            change_pct = None
            if prev_close and pd.notna(prev_close) and prev_close != 0:
                change_pct = (latest["close"] / prev_close - 1) * 100

            rows.append(
                {
                    "code": code,
                    "trade_date": latest["trade_date"],
                    "price": latest["close"],
                    "change_pct": change_pct,
                    "volume": latest.get("volume"),
                    "amount": latest.get("amount"),
                    "turnover_rate": None,
                    "pe_ttm": None,
                    "pb": None,
                    "total_market_cap": None,
                }
            )

        return self._clean_daily_quotes(pd.DataFrame(rows)) if rows else None

    def sync_stock_basic(self):
        """同步股票基础列表和行业信息"""
        if self._stock_basic_is_fresh():
            logger.info("stock_basic 今日已更新，跳过重复拉取")
            return True

        logger.info("开始同步股票基础列表...")
        
        # 获取基本代码名字
        code_df = self._safe_fetch(ak.stock_info_a_code_name)
        if code_df is None or code_df.empty:
            return False

        try:
            code_df = code_df.rename(
                columns={
                    "\u4ee3\u7801": "code",
                    "\u540d\u79f0": "name",
                    "symbol": "code",
                    "stock_code": "code",
                    "stock_name": "name",
                }
            )
            if "code" not in code_df.columns:
                logger.error("stock_basic sync failed: missing code column, columns={}", list(code_df.columns))
                return False

            clean_df = pd.DataFrame()
            clean_df["code"] = code_df["code"].astype(str).str.zfill(6)
            clean_df["name"] = code_df["name"].astype(str) if "name" in code_df.columns else clean_df["code"]
            clean_df["industry"] = code_df["industry"].astype(str) if "industry" in code_df.columns else None
            clean_df["update_date"] = datetime.now().strftime("%Y-%m-%d")
            clean_df = clean_df.drop_duplicates(subset=["code"], keep="last")

            ok = self.db.upsert_dataframe(
                "stock_basic",
                clean_df,
                key_columns=["code"]
            )
            if ok:
                logger.info("stock_basic sync completed: {} rows", len(clean_df))
            return ok
        except Exception as e:
            logger.error(f"stock_basic sync failed: {e}")
            return False

    def _stock_basic_is_fresh(self) -> bool:
        df = self.db.query_to_dataframe("SELECT MAX(update_date) AS latest_update_date FROM stock_basic")
        if df is None or df.empty:
            return False
        latest = df.iloc[0].get("latest_update_date")
        return str(latest) == datetime.now().strftime("%Y-%m-%d")

    def _daily_quotes_is_fresh(self) -> bool:
        df = self.db.query_to_dataframe("SELECT MAX(trade_date) AS latest_trade_date FROM daily_quotes")
        if df is None or df.empty:
            return False
        latest = df.iloc[0].get("latest_trade_date")
        if latest is None or pd.isna(latest):
            return False
        if str(latest).replace("-", "") < self._expected_latest_trade_date("daily"):
            return False

        coverage_df = self.db.query_to_dataframe(
            """
            SELECT pe_ttm, pb, total_market_cap
            FROM daily_quotes
            WHERE trade_date = ?
            """,
            (latest,),
        )
        if coverage_df is None or coverage_df.empty:
            return False
        if not self._valuation_fields_are_usable(coverage_df):
            logger.info("daily_quotes 日期已最新，但估值字段覆盖不足，需要重新同步补齐")
            return False
        return True

    def run_all(self):
        """执行所有同步任务"""
        logger.info("=== 离线数据管道作业开始 ===")
        cleanup_ok = self.cleanup_old_data() if self.enable_cleanup else True
        success_basic = self.sync_stock_basic()
        success_quotes = self.sync_daily_quotes()
        success_index_bars = self.sync_index_bars()
        success_bars = self.sync_market_bars(periods=("daily", "weekly", "monthly")) if success_basic else False
        
        if cleanup_ok and success_basic and success_quotes and success_index_bars and success_bars:
            logger.info("=== 离线数据管道作业成功完成 ===")
            return True
        else:
            logger.warning("=== 离线数据管道作业完成，但存在部分失败 ===")
            return False


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_all()
