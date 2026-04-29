"""
指数 K 线同步逻辑。
"""

from datetime import datetime

import akshare as ak
from loguru import logger
import pandas as pd


DEFAULT_INDEX_BARS = {
    "sh000001": "上证指数",
    "sh000300": "沪深300",
    "sz399006": "创业板指",
}


class IndexBarsMixin:
    def sync_index_bars(self) -> bool:
        """同步主要指数日线数据，用于市场行情研判。"""
        rows = []
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for code, name in DEFAULT_INDEX_BARS.items():
            try:
                raw = ak.stock_zh_index_daily(symbol=code)
            except Exception as exc:
                logger.warning(f"指数 K 线同步失败{name}({code}): {exc}")
                continue
            if raw is None or raw.empty:
                logger.warning(f"指数 K 线数据为空{name}({code})")
                continue
            rows.extend(self._normalize_index_bars(code, raw, fetched_at))

        if not rows:
            logger.warning("未获取到可用指数 K 线")
            return False

        bars_df = self._clean_market_bars(pd.DataFrame(rows), "daily")
        ok = self.db.upsert_dataframe(
            "market_bars",
            bars_df,
            key_columns=["code", "period", "trade_date"],
        )
        if ok:
            logger.info(f"指数 K 线同步完成：{len(bars_df)} 行")
        return ok

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
            logger.warning(f"指数 K 线字段不完整{code}: {list(raw.columns)}")
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
