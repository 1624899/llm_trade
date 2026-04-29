"""
K 线周期派生逻辑。

该模块只负责基于本地日线生成周线/月线，避免 DataPipeline 主文件继续膨胀。
"""

from datetime import datetime
import sys
import time

from loguru import logger
import pandas as pd


class PeriodBarDerivationMixin:
    def derive_period_bars(self, codes=None, periods=("weekly", "monthly")) -> bool:
        """基于本地日线派生周线/月线，并写入独立表。"""
        codes = [str(code).zfill(6) for code in (codes or self._load_stock_codes_from_lake())]
        if not codes:
            logger.error("No stock codes available for derived market bars")
            return False

        normalized_periods = []
        for period in periods or ():
            period = self._normalize_period(period)
            if period != "daily":
                normalized_periods.append(period)
        if not normalized_periods:
            logger.info("No weekly/monthly periods requested for derivation")
            return True

        logger.info(f"读取本地日线 K 线用于派生：{len(codes)} 支股票，周期={normalized_periods}")
        daily_df = self._load_daily_bars_for_derivation(codes)
        if daily_df is None or daily_df.empty:
            logger.error("No local daily bars available for derived market bars")
            return False

        all_success = True
        for period in normalized_periods:
            logger.info(f"开始派生 {period} K 线：{len(codes)} 支股票")
            bars_df = self._aggregate_daily_bars(daily_df, period)
            if bars_df is None or bars_df.empty:
                logger.warning(f"No {period} bars derived from local daily bars")
                all_success = False
                continue

            logger.info(f"{period} K 线聚合完成：{len(bars_df)} 行，准备清洗入库")
            clean_df = self._clean_market_bars(bars_df, period)
            ok = self._upsert_derived_period_bars(clean_df, period)
            all_success = ok and all_success
        return all_success

    def _render_progress(self, label: str, current: int, total: int, start_time: float) -> None:
        """在终端渲染轻量级进度条，避免长任务看起来像卡住。"""
        total = max(1, int(total))
        current = max(0, min(int(current), total))
        ratio = current / total
        bar_width = 28
        filled = int(bar_width * ratio)
        bar = "#" * filled + "-" * (bar_width - filled)
        elapsed = max(0.0, time.monotonic() - start_time)
        rate = current / elapsed if elapsed > 0 else 0.0
        remaining = (total - current) / rate if rate > 0 else 0.0
        line = (
            f"\r{label} [{bar}] {current}/{total} "
            f"{ratio * 100:5.1f}% elapsed={elapsed:5.1f}s eta={remaining:5.1f}s"
        )
        sys.stdout.write(line)
        if current >= total:
            sys.stdout.write("\n")
        sys.stdout.flush()

    def _period_bar_table(self, period: str) -> str:
        table_map = {
            "weekly": "market_bars_weekly",
            "monthly": "market_bars_monthly",
        }
        if period not in table_map:
            raise ValueError(f"No dedicated table for {period} bars")
        return table_map[period]

    def _upsert_derived_period_bars(self, clean_df: pd.DataFrame, period: str) -> bool:
        if clean_df is None or clean_df.empty:
            return False

        table_name = self._period_bar_table(period)
        storage_df = clean_df.drop(columns=["period"], errors="ignore")
        ok = self.db.upsert_dataframe(
            table_name,
            storage_df,
            key_columns=["code", "trade_date"],
        )
        if ok:
            logger.info(f"Successfully upserted {len(storage_df)} rows into {table_name}")
        return ok

    def _build_period_bars_from_daily(self, codes, period: str) -> pd.DataFrame:
        """基于本地日线聚合生成指定周期 K 线。"""
        if period not in {"weekly", "monthly"}:
            raise ValueError(f"Cannot derive {period} bars from daily bars")

        codes = [str(code).zfill(6) for code in codes]
        if not codes:
            return pd.DataFrame()

        daily_df = self._load_daily_bars_for_derivation(codes)
        if daily_df is None or daily_df.empty:
            return pd.DataFrame()

        return self._aggregate_daily_bars(daily_df, period)

    def _load_daily_bars_for_derivation(self, codes) -> pd.DataFrame:
        """一次性读取本地日线，周线/月线派生可复用，避免重复扫库。"""
        codes = [str(code).zfill(6) for code in codes]
        if not codes:
            return pd.DataFrame()

        placeholders = ",".join("?" for _ in codes)
        sql = (
            "SELECT code, trade_date, open, high, low, close, adj_close, volume, amount, fetched_at "
            "FROM market_bars "
            f"WHERE period = 'daily' AND code IN ({placeholders}) "
            "ORDER BY code, trade_date"
        )
        return self.db.query_to_dataframe(sql, tuple(codes))

    def _aggregate_daily_bars(self, daily_df: pd.DataFrame, period: str) -> pd.DataFrame:
        """将日线 OHLCV 数据向量化聚合为周线或月线。"""
        if daily_df is None or daily_df.empty:
            return pd.DataFrame()

        df = daily_df.copy()
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d", errors="coerce")
        for col in ["open", "high", "low", "close", "adj_close", "volume", "amount"]:
            df[col] = pd.to_numeric(df.get(col), errors="coerce")
        df = df.dropna(subset=["code", "trade_date", "open", "high", "low", "close"])
        if df.empty:
            return pd.DataFrame()

        if period == "weekly":
            df["_bucket"] = df["trade_date"].dt.to_period("W-FRI")
        elif period == "monthly":
            df["_bucket"] = df["trade_date"].dt.to_period("M")
        else:
            raise ValueError(f"Cannot derive {period} bars from daily bars")

        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        progress_total = 4
        progress_started_at = time.monotonic()
        progress_label = f"派生 {period} K线"
        self._render_progress(progress_label, 0, progress_total, progress_started_at)

        df = df.sort_values(["code", "_bucket", "trade_date"])
        self._render_progress(progress_label, 1, progress_total, progress_started_at)

        grouped = df.groupby(["code", "_bucket"], sort=False, observed=True)
        self._render_progress(progress_label, 2, progress_total, progress_started_at)

        bars = grouped.agg(
            trade_date=("trade_date", "last"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            adj_close=("adj_close", "last"),
        ).reset_index()
        sums = grouped[["volume", "amount"]].sum(min_count=1).reset_index()
        bars = bars.merge(sums, on=["code", "_bucket"], how="left")
        self._render_progress(progress_label, 3, progress_total, progress_started_at)

        bars["period"] = period
        bars["trade_date"] = bars["trade_date"].dt.strftime("%Y%m%d")
        bars["source"] = "derived_daily_qfq"
        bars["fetched_at"] = fetched_at
        bars = bars[
            [
                "code", "period", "trade_date", "open", "high", "low", "close",
                "adj_close", "volume", "amount", "source", "fetched_at"
            ]
        ]
        self._render_progress(progress_label, progress_total, progress_total, progress_started_at)
        return bars
