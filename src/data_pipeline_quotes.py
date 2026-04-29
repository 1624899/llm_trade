"""
日行情快照同步与清洗逻辑。
"""

from datetime import datetime

import akshare as ak
from loguru import logger
import pandas as pd

from src.quote_sources import fetch_sina_quotes, fetch_tencent_quotes


class DailyQuotesMixin:
    def sync_daily_quotes(self):
        """同步每日行情快照。"""
        if self._daily_quotes_is_fresh():
            logger.info("daily_quotes 已经覆盖最新交易日且估值字段可用，跳过重复获取")
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
        """使用主要免费快照源：腾讯 -> 新浪。"""
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
        """在快照估值字段覆盖率不足时，用 AKShare 快照补齐 PE/PB/总市值。"""
        if clean_df is None or clean_df.empty:
            return clean_df

        if self._valuation_fields_are_usable(clean_df):
            return clean_df

        logger.info("daily_quotes valuation coverage is low; enrich with AKShare snapshot")
        ak_df = self._safe_fetch(ak.stock_zh_a_spot_em, retries=1, delay=1)
        if ak_df is None or ak_df.empty:
            logger.warning("AKShare valuation enrichment returned no usable data")
            return clean_df

        trade_date = str(clean_df["trade_date"].dropna().iloc[0]) if "trade_date" in clean_df.columns and clean_df["trade_date"].notna().any() else datetime.now().strftime("%Y%m%d")
        valuation_df = self._normalize_ak_spot_quotes(ak_df, trade_date)
        code_set = {str(code).zfill(6) for code in codes}
        valuation_df = valuation_df[valuation_df["code"].astype(str).str.zfill(6).isin(code_set)]
        if valuation_df.empty:
            logger.warning("AKShare valuation enrichment has no overlapping codes")
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

        logger.info("daily_quotes valuation enrichment completed")
        return self._clean_daily_quotes(merged)

    def _valuation_fields_are_usable(self, df: pd.DataFrame, min_coverage: float = 0.8) -> bool:
        """检查市值、PE、PB 字段覆盖率是否达到阈值。"""
        if df is None or df.empty or "total_market_cap" not in df.columns:
            return False
        cap_coverage = pd.to_numeric(df["total_market_cap"], errors="coerce").notna().mean()
        pb_coverage = pd.to_numeric(df.get("pb"), errors="coerce").notna().mean() if "pb" in df.columns else 0
        pe_coverage = pd.to_numeric(df.get("pe_ttm"), errors="coerce").notna().mean() if "pe_ttm" in df.columns else 0
        return cap_coverage >= min_coverage and (pb_coverage >= min_coverage or pe_coverage >= min_coverage)

    def _normalize_ak_spot_quotes(self, df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """将 AKShare 行情快照字段标准化为 daily_quotes 表字段。"""
        column_mapping = {
            "代码": "code",
            "最新价": "price",
            "涨跌幅": "change_pct",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turnover_rate",
            "市盈率-动态": "pe_ttm",
            "市净率": "pb",
            "总市值": "total_market_cap",
            "code": "code",
            "price": "price",
            "change_pct": "change_pct",
            "volume": "volume",
            "amount": "amount",
            "turnover_rate": "turnover_rate",
            "pe_ttm": "pe_ttm",
            "pb": "pb",
            "total_market_cap": "total_market_cap",
        }
        available_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        clean_df = df[list(available_cols.keys())].rename(columns=available_cols)
        clean_df["trade_date"] = trade_date
        return self._clean_daily_quotes(clean_df)

    def _clean_daily_quotes(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗行情快照，完成数值转换、代码补零和字段筛选。"""
        clean_df = df.copy()
        numeric_cols = [
            "price", "change_pct", "volume", "amount", "turnover_rate",
            "pe_ttm", "pb", "total_market_cap"
        ]
        for col in numeric_cols:
            if col in clean_df.columns:
                clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

        for col in numeric_cols:
            if col not in clean_df.columns:
                clean_df[col] = None

        clean_df = clean_df.dropna(subset=["code"])
        clean_df["code"] = clean_df["code"].astype(str).str.zfill(6)
        return clean_df[
            [
                "code", "trade_date", "price", "change_pct", "volume", "amount",
                "turnover_rate", "pe_ttm", "pb", "total_market_cap"
            ]
        ]

    def _upsert_daily_quotes(self, clean_df: pd.DataFrame) -> bool:
        """将清洗后的行情快照写入 daily_quotes。"""
        try:
            self.db.upsert_dataframe(
                "daily_quotes",
                clean_df,
                key_columns=["code", "trade_date"]
            )
            logger.info("daily_quotes upsert completed")
            return True
        except Exception as e:
            logger.error(f"行情快照清洗入库失败 {e}")
            return False

    def _build_daily_quotes_from_yahoo(self):
        """从 Yahoo 历史日线构造最近交易日行情快照。"""
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

    def _daily_quotes_is_fresh(self) -> bool:
        """检查本地行情快照是否已覆盖最新交易日，且估值字段覆盖率达标。"""
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
            logger.info("daily_quotes latest date exists but valuation coverage is low")
            return False
        return True
