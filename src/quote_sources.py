import math
from datetime import datetime
from typing import Dict, Iterable, List

import pandas as pd
import requests
from loguru import logger


def to_market_symbol(code: str, style: str = "tencent") -> str:
    """
    将股票代码转换为特定数据源所需的市场标识符（如 sh600000, sz000001）。
    """
    code = str(code).zfill(6)
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return f"sh{code}"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return f"sz{code}"
    if style == "tencent" and code.startswith(("4", "8", "9")):
        # 腾讯接口对北交所的支持标识
        return f"bj{code}"
    return ""


def fetch_tencent_quotes(codes: Iterable[str], batch_size: int = 700) -> pd.DataFrame:
    """
    通过腾讯行情接口 (qt.gtimg.cn) 批量拉取全市场快照。
    字段包含：最新价、涨跌幅、成交量、成交额、换手率、PE、总市值等。
    """
    rows = []
    code_list = [str(code).zfill(6) for code in codes]
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for start in range(0, len(code_list), batch_size):
        batch = code_list[start:start + batch_size]
        symbols = [to_market_symbol(code) for code in batch]
        symbols = [symbol for symbol in symbols if symbol]
        if not symbols:
            continue

        response = requests.get(
            "https://qt.gtimg.cn/q=" + ",".join(symbols),
            timeout=15,
        )
        response.encoding = "gbk"
        response.raise_for_status()

        for line in response.text.splitlines():
            if '="' not in line:
                continue
            payload = line.split('="', 1)[1].rstrip('";')
            parts = payload.split("~")
            if len(parts) < 46:
                continue

            code = parts[2].strip()
            price = _to_float(parts[3])
            if not code or not _is_valid_number(price) or price <= 0:
                continue

            # 腾讯接口返回的单位处理：成交量(手)、成交额(万元)、总市值(亿元)
            rows.append(
                {
                    "code": code.zfill(6),
                    "trade_date": _parse_tencent_trade_date(parts[30] if len(parts) > 30 else ""),
                    "price": price,
                    "change_pct": _to_float(parts[32]),
                    "volume": _to_float(parts[36]) * 100 if _is_valid_number(_to_float(parts[36])) else None,
                    "amount": _to_float(parts[37]) * 10000 if _is_valid_number(_to_float(parts[37])) else None,
                    "turnover_rate": _to_float(parts[38]),
                    "pe_ttm": _to_float(parts[39]),
                    "pb": None,
                    "total_market_cap": _to_float(parts[45]) * 100000000 if _is_valid_number(_to_float(parts[45])) else None,
                    "source": "tencent",
                    "fetched_at": fetched_at,
                }
            )

    return pd.DataFrame(rows)


def fetch_sina_quotes(codes: Iterable[str], batch_size: int = 800) -> pd.DataFrame:
    """
    通过新浪财经行情接口 (hq.sinajs.cn) 批量拉取快照作为备用源。
    主要用于补充基本行情数据（价格、成交量、成交额）。
    """
    rows = []
    code_list = [str(code).zfill(6) for code in codes]
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for start in range(0, len(code_list), batch_size):
        batch = code_list[start:start + batch_size]
        symbols = [to_market_symbol(code, style="sina") for code in batch]
        symbols = [symbol for symbol in symbols if symbol]
        if not symbols:
            continue

        response = requests.get(
            "https://hq.sinajs.cn/list=" + ",".join(symbols),
            headers={"Referer": "https://finance.sina.com.cn"},
            timeout=15,
        )
        response.encoding = "gb18030"
        response.raise_for_status()

        for line in response.text.splitlines():
            if '="' not in line:
                continue
            symbol = line.split("var hq_str_", 1)[1].split("=", 1)[0]
            payload = line.split('="', 1)[1].rstrip('";')
            parts = payload.split(",")
            if len(parts) < 32 or not parts[0]:
                continue

            code = symbol[-6:]
            price = _to_float(parts[3])
            prev_close = _to_float(parts[2])
            if not _is_valid_number(price) or price <= 0:
                continue

            # 计算涨跌幅
            change_pct = None
            if _is_valid_number(prev_close) and prev_close:
                change_pct = (price / prev_close - 1) * 100

            trade_date = parts[30].replace("-", "") if len(parts) > 30 else datetime.now().strftime("%Y%m%d")
            rows.append(
                {
                    "code": code.zfill(6),
                    "trade_date": trade_date,
                    "price": price,
                    "change_pct": change_pct,
                    "volume": _to_float(parts[8]),
                    "amount": _to_float(parts[9]),
                    "turnover_rate": None,
                    "pe_ttm": None,
                    "pb": None,
                    "total_market_cap": None,
                    "source": "sina",
                    "fetched_at": fetched_at,
                }
            )

    return pd.DataFrame(rows)


def fetch_latest_prices(codes: Iterable[str]) -> Dict[str, float]:
    """
    获取指定股票代码列表的最新价格字典 {code: price}。
    会自动在多个源之间进行尝试。
    """
    for fetcher in (fetch_tencent_quotes, fetch_sina_quotes):
        try:
            df = fetcher(codes)
        except Exception as exc:
            logger.warning(f"实时价格源不可用 {fetcher.__name__}: {exc}")
            continue
        if df is not None and not df.empty:
            return dict(zip(df["code"].astype(str).str.zfill(6), df["price"].astype(float)))
    return {}


def _parse_tencent_trade_date(value: str) -> str:
    """从腾讯接口返回的字符串中提取 YYYYMMDD 格式的日期。"""
    value = str(value).strip()
    if len(value) >= 8 and value[:8].isdigit():
        return value[:8]
    return datetime.now().strftime("%Y%m%d")


def _to_float(value):
    """通用的数值转换工具，处理空值和非法字符。"""
    try:
        if value in ("", None, "-"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_valid_number(value) -> bool:
    """检查是否为有效的数字（非 None 且非 NaN）。"""
    return value is not None and not math.isnan(value)
