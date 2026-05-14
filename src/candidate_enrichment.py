"""候选股小批量增强数据服务。"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from loguru import logger

from src.database import StockDatabase
from src.market_extras import fetch_eastmoney_research_reports, fetch_ths_eps_forecast
from src.signal_sources import (
    fetch_baidu_stock_fund_flow_history,
    fetch_baidu_stock_profile,
    fetch_eastmoney_lhb,
    fetch_eastmoney_restricted_release,
)


class CandidateEnrichmentService:
    """只面向候选股/持仓的小批量外部增强，避免拖累离线主同步。"""

    def __init__(self, db: Optional[StockDatabase] = None, config: Optional[Dict[str, Any]] = None):
        self.db = db or StockDatabase()
        cfg = config or {}
        self.enabled = bool(cfg.get("enabled", True))
        self.lhb_lookback_days = max(1, int(cfg.get("lhb_lookback_days", 10)))
        self.release_forward_days = max(1, int(cfg.get("release_forward_days", 60)))
        self.baidu_fund_flow_days = max(1, int(cfg.get("baidu_fund_flow_days", 5)))
        self.research_report_pages = max(1, int(cfg.get("research_report_pages", 1)))
        self.max_summary_items = max(1, int(cfg.get("max_summary_items", 3)))
        self._lhb_cache: Dict[tuple[str, str], pd.DataFrame] = {}
        self._release_cache: Dict[tuple[str, str], pd.DataFrame] = {}
        self._cache_lock = threading.Lock()

    def enrich_candidate(self, code: str, name: str = "") -> Dict[str, Any]:
        """增强单只候选股；任一来源失败都只降级为空摘要。"""
        code = str(code or "").zfill(6)
        if not self.enabled or not code.isdigit():
            return {"code": code, "name": name, "summary": ""}

        result: Dict[str, Any] = {
            "code": code,
            "name": name or code,
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        fetchers = (
            ("profile", self._load_baidu_profile),
            ("fund_flow", self._load_baidu_fund_flow),
            ("lhb", self._load_lhb),
            ("restricted_release", self._load_restricted_release),
            ("research_reports", self._load_research_reports),
            ("eps_forecasts", self._load_eps_forecasts),
            ("local_concepts", self._load_local_concepts),
        )
        for key, fetcher in fetchers:
            try:
                result[key] = fetcher(code)
            except Exception as exc:
                logger.warning("[Enrichment] {} 获取失败 {}: {}", key, code, exc)
                result[key] = [] if key != "profile" else {}

        result["summary"] = format_enrichment_for_prompt(result)
        return result

    def _load_baidu_profile(self, code: str) -> Dict[str, Any]:
        df = fetch_baidu_stock_profile(code)
        if df is None or df.empty:
            return {}
        row = df.iloc[0].to_dict()
        return {
            "industry": row.get("industry") or "",
            "region": row.get("region") or "",
            "concepts": _decode_json_list(row.get("concepts")),
            "industry_blocks": _decode_json_list(row.get("industry_blocks")),
            "region_blocks": _decode_json_list(row.get("region_blocks")),
        }

    def _load_baidu_fund_flow(self, code: str) -> list[Dict[str, Any]]:
        df = fetch_baidu_stock_fund_flow_history(code, days=self.baidu_fund_flow_days)
        return _records(df, limit=self.max_summary_items)

    def _load_lhb(self, code: str) -> list[Dict[str, Any]]:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=self.lhb_lookback_days)).strftime("%Y%m%d")
        cache_key = (start_date, end_date)
        with self._cache_lock:
            if cache_key not in self._lhb_cache:
                self._lhb_cache[cache_key] = fetch_eastmoney_lhb(start_date=start_date, end_date=end_date)
        df = self._lhb_cache[cache_key]
        if df is None or df.empty:
            return []
        matched = df[df["code"].astype(str).str.zfill(6) == code]
        return _records(matched.sort_values("trade_date", ascending=False), limit=self.max_summary_items)

    def _load_restricted_release(self, code: str) -> list[Dict[str, Any]]:
        start_date = datetime.now().strftime("%Y%m%d")
        end_date = (datetime.now() + timedelta(days=self.release_forward_days)).strftime("%Y%m%d")
        cache_key = (start_date, end_date)
        with self._cache_lock:
            if cache_key not in self._release_cache:
                self._release_cache[cache_key] = fetch_eastmoney_restricted_release(start_date=start_date, end_date=end_date)
        df = self._release_cache[cache_key]
        if df is None or df.empty:
            return []
        matched = df[df["code"].astype(str).str.zfill(6) == code]
        return _records(matched.sort_values("release_date"), limit=self.max_summary_items)

    def _load_research_reports(self, code: str) -> list[Dict[str, Any]]:
        df = fetch_eastmoney_research_reports(code, max_pages=self.research_report_pages)
        if df is None or df.empty:
            return []
        return _records(df.sort_values("publish_date", ascending=False), limit=self.max_summary_items)

    def _load_eps_forecasts(self, code: str) -> list[Dict[str, Any]]:
        df = fetch_ths_eps_forecast(code)
        if df is None or df.empty:
            return []
        return _records(df, limit=self.max_summary_items)

    def _load_local_concepts(self, code: str) -> list[Dict[str, Any]]:
        try:
            df = self.db.query_to_dataframe(
                """
                SELECT concept_name, concept_code, source
                FROM stock_concepts
                WHERE code = ?
                ORDER BY concept_name
                LIMIT ?
                """,
                (code, self.max_summary_items * 2),
            )
        except Exception as exc:
            logger.debug("[Enrichment] 本地概念读取失败 {}: {}", code, exc)
            return []
        return _records(df, limit=self.max_summary_items * 2)


def format_enrichment_for_prompt(enrichment: Dict[str, Any]) -> str:
    """把增强数据压缩成 Agent 可直接读取的摘要文本。"""
    if not isinstance(enrichment, dict):
        return ""
    lines = []
    profile = enrichment.get("profile") or {}
    concept_names = _concept_names(profile.get("concepts")) or [
        item.get("concept_name") for item in enrichment.get("local_concepts") or [] if item.get("concept_name")
    ]
    if profile or concept_names:
        parts = []
        if profile.get("industry"):
            parts.append(f"行业={profile.get('industry')}")
        if profile.get("region"):
            parts.append(f"地域={profile.get('region')}")
        if concept_names:
            parts.append("概念=" + "、".join(concept_names[:6]))
        if parts:
            lines.append("画像/概念：" + "；".join(parts))

    fund_flow = enrichment.get("fund_flow") or []
    if fund_flow:
        latest = fund_flow[0]
        lines.append(
            "百度资金流："
            f"{latest.get('trade_date', '')} 主力净流入={_fmt_num(latest.get('main_net_inflow'))}，"
            f"涨跌幅={_fmt_pct(latest.get('change_pct'))}"
        )

    lhb = enrichment.get("lhb") or []
    if lhb:
        lines.append(
            "龙虎榜："
            + "；".join(
                f"{item.get('trade_date', '')} {item.get('reason') or '上榜'} 净买={_fmt_num(item.get('net_buy'))}"
                for item in lhb[:3]
            )
        )

    releases = enrichment.get("restricted_release") or []
    if releases:
        lines.append(
            "解禁："
            + "；".join(
                f"{item.get('release_date', '')} 比例={_fmt_pct(item.get('release_ratio'))} 类型={item.get('holder_type') or ''}"
                for item in releases[:3]
            )
        )

    reports = enrichment.get("research_reports") or []
    if reports:
        lines.append(
            "研报："
            + "；".join(
                f"{item.get('publish_date', '')} {item.get('rating') or '未评级'} {item.get('title') or ''}"[:80]
                for item in reports[:3]
            )
        )

    eps = enrichment.get("eps_forecasts") or []
    if eps:
        lines.append(
            "EPS一致预期："
            + "；".join(
                f"{item.get('forecast_year', '')} 均值={_fmt_num(item.get('eps_avg'))} 机构={_fmt_num(item.get('institution_count'))}"
                for item in eps[:3]
            )
        )
    return "\n".join(line for line in lines if line.strip())


def _records(df: pd.DataFrame | None, limit: int = 3) -> list[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    clean = df.head(max(1, int(limit))).where(pd.notna(df.head(max(1, int(limit)))), None)
    return clean.to_dict(orient="records")


def _decode_json_list(value: Any) -> list[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    return [item for item in parsed if isinstance(item, dict)] if isinstance(parsed, list) else []


def _concept_names(items: Iterable[Dict[str, Any]] | None) -> list[str]:
    names = []
    for item in items or []:
        name = str(item.get("name") or item.get("concept_name") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def _fmt_num(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "未知"
    if abs(number) >= 1e8:
        return f"{number / 1e8:.2f}亿"
    if abs(number) >= 1e4:
        return f"{number / 1e4:.2f}万"
    return f"{number:.2f}"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "未知"
