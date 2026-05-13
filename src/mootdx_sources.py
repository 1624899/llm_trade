"""mootdx 行情数据源封装。"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd
from loguru import logger


FREQUENCY_MAP = {
    "daily": 9,
    "weekly": 5,
    "monthly": 6,
}


def fetch_mootdx_bars(
    codes: Iterable[str],
    period: str = "daily",
    start_date: str | None = None,
    end_date: str | None = None,
    offset: int = 800,
    timeout: int = 15,
) -> pd.DataFrame:
    """从 mootdx 读取 K 线并统一成 market_bars 表字段。"""
    frequency = FREQUENCY_MAP.get(period)
    if frequency is None:
        raise ValueError(f"Unsupported mootdx period: {period}")

    try:
        from mootdx.quotes import Quotes
    except ImportError as exc:
        raise RuntimeError("mootdx 未安装，无法读取通达信行情") from exc

    client = Quotes.factory(market="std", multithread=True, heartbeat=True, bestip=False, timeout=timeout)
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_date = _normalize_trade_date(start_date)
    end_date = _normalize_trade_date(end_date)

    rows = []
    for code in [str(item).zfill(6) for item in codes]:
        try:
            raw = client.bars(symbol=code, frequency=frequency, offset=max(1, int(offset or 1)), adjust="qfq")
        except TypeError:
            # 兼容旧版 mootdx 参数名，避免版本差异导致主链路中断。
            raw = client.bars(symbol=code, category=frequency, offset=max(1, int(offset or 1)), adjust="qfq")
        except Exception as exc:
            logger.warning("mootdx K 线获取失败 {} {}: {}", code, period, exc)
            continue

        parsed = _parse_mootdx_bars(raw, code, period, fetched_at)
        if parsed.empty:
            continue
        if start_date:
            parsed = parsed[parsed["trade_date"] >= start_date]
        if end_date:
            parsed = parsed[parsed["trade_date"] <= end_date]
        rows.append(parsed)

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def _parse_mootdx_bars(raw: pd.DataFrame | None, code: str, period: str, fetched_at: str) -> pd.DataFrame:
    """兼容 mootdx 不同版本返回的列名。"""
    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy().reset_index()
    rows = []
    for _, row in df.iterrows():
        trade_date = _extract_trade_date(row)
        close = _first_value(row, "close", "收盘")
        if not trade_date or pd.isna(close):
            continue

        rows.append(
            {
                "code": str(code).zfill(6),
                "period": period,
                "trade_date": trade_date,
                "open": _first_value(row, "open", "开盘"),
                "high": _first_value(row, "high", "最高"),
                "low": _first_value(row, "low", "最低"),
                "close": close,
                "adj_close": close,
                "volume": _first_value(row, "vol", "volume", "成交量"),
                "amount": _first_value(row, "amount", "成交额"),
                "source": "mootdx",
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _first_value(row: pd.Series, *names: str):
    for name in names:
        if name in row and not pd.isna(row.get(name)):
            return row.get(name)
    return None


def _extract_trade_date(row: pd.Series) -> str:
    for name in ("date", "datetime", "trade_date", "日期", "index"):
        if name not in row or pd.isna(row.get(name)):
            continue
        normalized = _normalize_trade_date(row.get(name))
        if normalized:
            return normalized
    return ""


def _normalize_trade_date(value) -> str:
    if value is None or value == "":
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if not pd.isna(parsed):
        return pd.Timestamp(parsed).strftime("%Y%m%d")
    text = str(value).replace("-", "").replace("/", "").strip()
    return text[:8] if len(text) >= 8 and text[:8].isdigit() else ""
