"""A 股信号层数据源封装。"""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any

import akshare as ak
import pandas as pd
import requests
from loguru import logger


def fetch_ths_stock_fund_flow(symbol: str = "即时") -> pd.DataFrame:
    """读取同花顺个股资金流并标准化字段。"""
    try:
        raw = ak.stock_fund_flow_individual(symbol=symbol)
    except Exception as exc:
        logger.warning("同花顺个股资金流获取失败 {}: {}", symbol, exc)
        return pd.DataFrame()
    return _normalize_stock_fund_flow(raw, period=symbol, source="ths_fund_flow_individual")


def fetch_ths_board_fund_flow(kind: str = "concept", symbol: str = "即时") -> pd.DataFrame:
    """读取同花顺行业/概念资金流并标准化字段。"""
    try:
        if kind == "industry":
            raw = ak.stock_fund_flow_industry(symbol=symbol)
            source = "ths_fund_flow_industry"
        else:
            raw = ak.stock_fund_flow_concept(symbol=symbol)
            source = "ths_fund_flow_concept"
    except Exception as exc:
        logger.warning("同花顺{}资金流获取失败 {}: {}", kind, symbol, exc)
        return pd.DataFrame()
    return _normalize_board_fund_flow(raw, kind=kind, period=symbol, source=source)


def fetch_eastmoney_lhb(start_date: str, end_date: str | None = None) -> pd.DataFrame:
    """读取东方财富全市场龙虎榜详情。"""
    end_date = end_date or start_date
    try:
        raw = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
    except Exception as exc:
        logger.warning("东方财富龙虎榜获取失败 {}..{}: {}", start_date, end_date, exc)
        return pd.DataFrame()
    return _normalize_lhb(raw, source="eastmoney_lhb")


def fetch_eastmoney_restricted_release(start_date: str, end_date: str) -> pd.DataFrame:
    """读取东方财富限售股解禁明细。"""
    try:
        raw = ak.stock_restricted_release_detail_em(start_date=start_date, end_date=end_date)
    except Exception as exc:
        logger.warning("东方财富限售解禁获取失败 {}..{}: {}", start_date, end_date, exc)
        return pd.DataFrame()
    return _normalize_restricted_release(raw, source="eastmoney_restricted_release")


def fetch_ths_concept_boards() -> pd.DataFrame:
    """读取同花顺概念板块目录。"""
    try:
        raw = ak.stock_board_concept_name_ths()
    except Exception as exc:
        logger.warning("同花顺概念板块目录获取失败: {}", exc)
        return pd.DataFrame()
    return _normalize_concept_boards(raw, source="ths_concept_boards")


def fetch_eastmoney_concept_constituents(symbol: str, concept_code: str = "") -> pd.DataFrame:
    """读取东方财富概念成分股。"""
    try:
        raw = ak.stock_board_concept_cons_em(symbol=symbol)
    except Exception as exc:
        logger.warning("东方财富概念成分获取失败 {}: {}", symbol, exc)
        return pd.DataFrame()
    return _normalize_concept_constituents(raw, concept_name=symbol, concept_code=concept_code, source="eastmoney_concept_constituents")


def fetch_baidu_stock_profile(code: str, timeout: int = 10) -> pd.DataFrame:
    """读取百度股市通个股画像，提取行业、地域和概念标签。"""
    code = _code(code)
    if not code:
        return pd.DataFrame()

    params = {
        "code": code,
        "market": "ab",
        "typeCode": "all",
        "finClientType": "pc",
    }
    headers = {
        "Host": "finance.pae.baidu.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0",
        "Accept": "application/vnd.finance-web.v1+json",
        "Origin": "https://gushitong.baidu.com",
        "Referer": "https://gushitong.baidu.com/",
    }
    try:
        response = requests.get(
            "https://finance.pae.baidu.com/api/getrelatedblock",
            params=params,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        logger.warning("百度股市通个股画像获取失败 {}: {}", code, exc)
        return pd.DataFrame()

    row = _parse_baidu_profile_payload(payload, code=code, source="baidu_gushitong")
    return pd.DataFrame([row]) if row else pd.DataFrame()


def fetch_baidu_stock_fund_flow_history(code: str, days: int = 20, timeout: int = 10) -> pd.DataFrame:
    """读取百度股市通个股日级资金流。"""
    code = _code(code)
    if not code:
        return pd.DataFrame()

    params = {
        "code": code,
        "market": "ab",
        "pn": 0,
        "rn": max(1, int(days or 1)),
        "finClientType": "pc",
    }
    headers = {
        "Host": "finance.pae.baidu.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0",
        "Accept": "application/vnd.finance-web.v1+json",
        "Origin": "https://gushitong.baidu.com",
        "Referer": "https://gushitong.baidu.com/",
    }
    try:
        response = requests.get(
            "https://finance.pae.baidu.com/vapi/v1/fundsortlist",
            params=params,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        logger.warning("百度股市通资金流获取失败 {}: {}", code, exc)
        return pd.DataFrame()

    return _normalize_baidu_fund_flow_history(payload, code=code, source="baidu_fundsortlist")


def _normalize_stock_fund_flow(raw: pd.DataFrame | None, period: str, source: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for _, row in raw.iterrows():
        code = _code(_first(row, "股票代码", "代码", "code"))
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "trade_date": _date(_first(row, "日期", "trade_date")) or datetime.now().strftime("%Y%m%d"),
                "period": period,
                "name": _first(row, "股票简称", "名称", "name"),
                "latest_price": _num(_first(row, "最新价", "收盘价", "price")),
                "change_pct": _num(_first(row, "涨跌幅", "涨跌幅%", "change_pct")),
                "main_net_inflow": _num(_first(row, "主力净流入-净额", "主力净流入", "主力净额", "净额")),
                "main_net_inflow_pct": _num(_first(row, "主力净流入-净占比", "主力净占比", "净占比")),
                "super_large_net_inflow": _num(_first(row, "超大单净流入-净额", "超大单净额")),
                "large_net_inflow": _num(_first(row, "大单净流入-净额", "大单净额")),
                "medium_net_inflow": _num(_first(row, "中单净流入-净额", "中单净额")),
                "small_net_inflow": _num(_first(row, "小单净流入-净额", "小单净额")),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _normalize_board_fund_flow(raw: pd.DataFrame | None, kind: str, period: str, source: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for _, row in raw.iterrows():
        board_name = _first(row, "行业", "概念", "板块名称", "名称", "name")
        if not board_name:
            continue
        rows.append(
            {
                "board_name": str(board_name),
                "kind": kind,
                "trade_date": _date(_first(row, "日期", "trade_date")) or datetime.now().strftime("%Y%m%d"),
                "period": period,
                "change_pct": _num(_first(row, "涨跌幅", "涨跌幅%", "change_pct")),
                "main_net_inflow": _num(_first(row, "主力净流入-净额", "主力净流入", "净额")),
                "main_net_inflow_pct": _num(_first(row, "主力净流入-净占比", "主力净占比", "净占比")),
                "leader_name": _first(row, "领涨股", "龙头股", "leader_name"),
                "leader_change_pct": _num(_first(row, "领涨股涨跌幅", "领涨涨跌幅")),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _normalize_baidu_fund_flow_history(payload: dict, code: str, source: str) -> pd.DataFrame:
    if str(payload.get("ResultCode", -1)) != "0":
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for item in payload.get("Result", {}).get("list", []) or []:
        trade_date = _date(item.get("showtime"))
        if not trade_date:
            continue
        rows.append(
            {
                "code": _code(code),
                "trade_date": trade_date,
                "close": _num(item.get("closepx")),
                "change_pct": _num(item.get("ratio")),
                "super_net_inflow": _num(item.get("superNetIn")),
                "large_net_inflow": _num(item.get("largeNetIn")),
                "medium_net_inflow": _num(item.get("mediumNetIn")),
                "small_net_inflow": _num(item.get("littleNetIn")),
                "main_net_inflow": _num(item.get("extMainIn")),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _normalize_lhb(raw: pd.DataFrame | None, source: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for _, row in raw.iterrows():
        code = _code(_first(row, "代码", "股票代码", "SECURITY_CODE", "code"))
        trade_date = _date(_first(row, "上榜日", "交易日期", "TRADE_DATE", "date"))
        if not code or not trade_date:
            continue
        rows.append(
            {
                "code": code,
                "trade_date": trade_date,
                "name": _first(row, "名称", "股票简称", "SECURITY_NAME_ABBR", "name"),
                "reason": _first(row, "上榜原因", "解读", "EXPLAIN", "reason"),
                "close": _num(_first(row, "收盘价", "最新价", "CLOSE_PRICE")),
                "change_pct": _num(_first(row, "涨跌幅", "CHANGE_RATE")),
                "net_buy": _num(_first(row, "龙虎榜净买额", "净买额", "NET_BUY_AMT")),
                "buy_amount": _num(_first(row, "买入额", "BUY_AMT")),
                "sell_amount": _num(_first(row, "卖出额", "SELL_AMT")),
                "turnover": _num(_first(row, "成交额", "DEAL_AMT")),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _normalize_restricted_release(raw: pd.DataFrame | None, source: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for _, row in raw.iterrows():
        code = _code(_first(row, "股票代码", "代码", "SECURITY_CODE", "code"))
        release_date = _date(_first(row, "解禁时间", "解禁日期", "上市日期", "FREE_DATE"))
        if not code or not release_date:
            continue
        rows.append(
            {
                "code": code,
                "release_date": release_date,
                "name": _first(row, "股票简称", "名称", "SECURITY_NAME_ABBR", "name"),
                "release_shares": _num(_first(row, "解禁数量", "解禁股份数", "FREE_SHARES")),
                "release_market_value": _num(_first(row, "实际解禁市值", "解禁市值", "FREE_MARKET_CAP")),
                "release_ratio": _num(_first(row, "占总股本比例", "解禁比例", "FREE_RATIO")),
                "holder_type": _first(row, "股东类型", "限售股类型", "HOLDER_TYPE"),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _normalize_concept_boards(raw: pd.DataFrame | None, source: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for _, row in raw.iterrows():
        board_name = _first(row, "name", "概念名称", "板块名称", "名称")
        if not board_name:
            continue
        rows.append(
            {
                "concept_name": str(board_name),
                "concept_code": str(_first(row, "code", "概念代码", "板块代码") or ""),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _normalize_concept_constituents(
    raw: pd.DataFrame | None,
    concept_name: str,
    concept_code: str,
    source: str,
) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    fetched_at = _now()
    rows = []
    for _, row in raw.iterrows():
        code = _code(_first(row, "代码", "股票代码", "code"))
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "concept_name": str(concept_name),
                "concept_code": str(concept_code or ""),
                "name": _first(row, "名称", "股票简称", "name"),
                "latest_price": _num(_first(row, "最新价", "price")),
                "change_pct": _num(_first(row, "涨跌幅", "change_pct")),
                "turnover_rate": _num(_first(row, "换手率", "turnover_rate")),
                "pe_ttm": _num(_first(row, "市盈率-动态", "pe_ttm")),
                "pb": _num(_first(row, "市净率", "pb")),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _parse_baidu_profile_payload(payload: dict, code: str, source: str) -> dict:
    if str(payload.get("ResultCode", -1)) != "0":
        return {}

    industry_blocks = []
    region_blocks = []
    concepts = []
    for block in payload.get("Result", []) or []:
        block_type = str(block.get("type", ""))
        for item in block.get("list", []) or []:
            name = _clean_text(item.get("name"))
            if not name:
                continue
            entry = {
                "name": name,
                "change_pct": _num(item.get("increase")),
                "desc": _clean_text(item.get("desc")),
            }
            if "行业" in block_type:
                industry_blocks.append(entry)
            elif "概念" in block_type:
                concepts.append(entry)
            elif "地域" in block_type:
                region_blocks.append(entry)

    if not industry_blocks and not region_blocks and not concepts:
        return {}
    return {
        "code": _code(code),
        "name": "",
        "industry": industry_blocks[0]["name"] if industry_blocks else "",
        "region": region_blocks[0]["name"] if region_blocks else "",
        "industry_blocks": json.dumps(industry_blocks, ensure_ascii=False),
        "region_blocks": json.dumps(region_blocks, ensure_ascii=False),
        "concepts": json.dumps(concepts, ensure_ascii=False),
        "source": source,
        "fetched_at": _now(),
    }


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        return ""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text in {"-", "--", "None", "nan"} else text


def _first(row: pd.Series, *names: str) -> Any:
    for name in names:
        if name in row and not pd.isna(row.get(name)):
            return row.get(name)
    return None


def _code(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _date(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if not pd.isna(parsed):
        return pd.Timestamp(parsed).strftime("%Y%m%d")
    text = str(value).replace("-", "").replace("/", "").strip()
    return text[:8] if len(text) >= 8 and text[:8].isdigit() else ""


def _num(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(str(value).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
