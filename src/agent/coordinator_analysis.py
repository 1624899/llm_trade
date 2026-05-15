"""候选股并发分析 Mixin。

从 coordinator.py 拆分：候选股并发分析、单股分析、快照刷新、实时技术指标。
"""

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from loguru import logger


class CoordinatorAnalysisMixin:
    """候选股并发分析与实时快照刷新。"""

    def _analyze_candidates_concurrently(
        self,
        candidates: List[Dict[str, Any]],
        macro_context: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
        max_workers = max(1, min(self.candidate_analysis_max_workers, len(candidates)))
        logger.info(f"[Step 3] 使用 ThreadPoolExecutor 并发分析候选股，max_workers={max_workers}")

        detailed_reports: List[Dict[str, Any]] = []
        analysis_errors: List[Dict[str, str]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stock = {
                executor.submit(self._analyze_single_candidate, stock, macro_context): stock
                for stock in candidates
            }
            for future in as_completed(future_to_stock):
                stock = future_to_stock[future]
                try:
                    detailed_reports.append(future.result())
                except Exception as exc:
                    code = str(stock.get("code", ""))
                    name = str(stock.get("name", ""))
                    logger.warning(f"[Step 3] 候选股分析失败: {name}({code}) - {exc}")
                    detailed_reports.append(self._build_failed_candidate_profile(stock, exc))
                    analysis_errors.append({"code": code, "name": name, "error": str(exc)})

        order_by_code = {stock.get("code"): index for index, stock in enumerate(candidates)}
        detailed_reports.sort(key=lambda item: order_by_code.get(item["asset_info"].get("code"), 9999))
        return detailed_reports, analysis_errors

    def _analyze_single_candidate(
        self,
        stock: Dict[str, Any],
        macro_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        code = stock["code"]
        name = stock["name"]
        logger.info(f"  -> 正在分析: {name}({code})")

        enrichment_service = getattr(self, "candidate_enrichment", None)
        enrichment = (
            enrichment_service.enrich_candidate(code, name)
            if enrichment_service is not None
            else {"code": code, "name": name, "summary": ""}
        )
        enriched_stock = dict(stock)
        enriched_stock["external_enrichment"] = enrichment
        if self._is_missing(enriched_stock.get("industry")):
            industry = self._infer_industry_from_enrichment(enrichment)
            if industry:
                enriched_stock["industry"] = industry
        enriched_macro_context = dict(macro_context or {})
        enriched_macro_context["candidate_enrichment"] = enrichment
        enriched_macro_context["asset_snapshot"] = enriched_stock

        news_risk_report = self.news_risk_agent.analyze(code, name, macro_context=enriched_macro_context)
        logger.info(
            f"  -> NewsRisk done: {name}({code}), "
            f"level={news_risk_report.get('risk_level') if isinstance(news_risk_report, dict) else 'unknown'}, "
            f"action={news_risk_report.get('action') if isinstance(news_risk_report, dict) else 'unknown'}, "
            f"hard_exclude={news_risk_report.get('hard_exclude') if isinstance(news_risk_report, dict) else 'unknown'}"
        )
        if isinstance(news_risk_report, dict) and bool(news_risk_report.get("hard_exclude")):
            skip_reason = (
                "资讯硬风控已拦截，跳过基本面和技术面深度复核，"
                "避免继续消耗分析资源。"
            )
            logger.warning(f"  -> NewsRisk hard exclude: {name}({code}); skip deep review.")
            return {
                "asset_info": enriched_stock,
                "fundamental_analysis": skip_reason,
                "technical_analysis": skip_reason,
                "news_risk_analysis": news_risk_report,
                "external_enrichment": enrichment,
            }

        fund_report = self.fundamental_agent.analyze(code, name, macro_context=enriched_macro_context)
        logger.info(f"  -> Fundamental done: {name}({code}), chars={len(str(fund_report or ''))}")
        tech_report = self.technical_agent.analyze(code, name, macro_context=enriched_macro_context)
        logger.info(f"  -> Technical done: {name}({code}), chars={len(str(tech_report or ''))}")

        return {
            "asset_info": enriched_stock,
            "fundamental_analysis": fund_report,
            "technical_analysis": tech_report,
            "news_risk_analysis": news_risk_report,
            "external_enrichment": enrichment,
        }

    def _build_failed_candidate_profile(self, stock: Dict[str, Any], exc: Exception) -> Dict[str, Any]:
        code = stock.get("code", "")
        name = stock.get("name", "")
        error_text = f"深度分析失败：{exc}"
        return {
            "asset_info": stock,
            "fundamental_analysis": error_text,
            "technical_analysis": error_text,
            "news_risk_analysis": {
                "risk_level": "medium",
                "action": "watch",
                "hard_exclude": False,
                "summary": f"{name}({code}) 单股深度分析失败，最终决策需降低置信度。",
            },
        }

    def _refresh_candidates_realtime_snapshots(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """用实时快照覆盖候选行情，并重算进入 Agent 的关键技术指标。"""
        if not candidates:
            return []

        from src.agent.coordinator import fetch_latest_quotes  # late import for patch compatibility

        codes = [str(item.get("code") or "").zfill(6) for item in candidates if item.get("code")]
        realtime_quotes = fetch_latest_quotes(codes)
        if realtime_quotes:
            logger.info(
                "[RealtimeSnapshot] 实时快照覆盖 {} / {} 只候选",
                len(realtime_quotes),
                len(codes),
            )

        provider = self._get_realtime_signal_provider()
        refreshed: List[Dict[str, Any]] = []
        for item in candidates:
            code = str(item.get("code") or "").zfill(6)
            realtime_quote = realtime_quotes.get(code, {})
            merged = self._merge_candidate_realtime_snapshot(item, realtime_quote)
            if provider is not None:
                self._attach_realtime_technical_metrics(merged, provider, realtime_quote)
            refreshed.append(merged)
        return refreshed

    def _get_realtime_signal_provider(self):
        from src.agent.coordinator import TechnicalSignalProvider  # late import for patch compatibility

        technical_agent = getattr(self, "technical_agent", None)
        provider = getattr(technical_agent, "signal_provider", None)
        if isinstance(provider, TechnicalSignalProvider):
            return provider
        try:
            return TechnicalSignalProvider(db=self.db)
        except Exception as exc:
            logger.warning("[RealtimeSnapshot] 技术指标实时重算初始化失败: {}", exc)
            return None

    def _merge_candidate_realtime_snapshot(
        self,
        candidate: Dict[str, Any],
        realtime_quote: Dict[str, Any],
    ) -> Dict[str, Any]:
        merged = dict(candidate)
        if not realtime_quote:
            return merged

        for key in (
            "trade_date",
            "price",
            "open",
            "high",
            "low",
            "prev_close",
            "change_amount",
            "change_pct",
            "volume",
            "amount",
            "turnover_rate",
            "pe_ttm",
            "pb",
            "total_market_cap",
            "source",
            "fetched_at",
        ):
            value = realtime_quote.get(key)
            if not self._is_missing(value):
                merged[key] = value
        merged["snapshot_source"] = "realtime"
        return merged

    def _attach_realtime_technical_metrics(
        self,
        candidate: Dict[str, Any],
        provider,
        realtime_quote: Dict[str, Any],
    ) -> None:
        if not realtime_quote:
            return
        code = str(candidate.get("code") or "").zfill(6)
        try:
            report = provider.build_report(code, lookback=120, realtime_quote=realtime_quote)
        except Exception as exc:
            logger.warning("[RealtimeSnapshot] 技术指标实时重算失败 {}: {}", code, exc)
            return
        metrics = report.metrics or {}
        if not metrics.get("available"):
            return

        key_metrics = dict(candidate.get("key_metrics") or {})
        metric_mapping = {
            "ret5": metrics.get("ret5"),
            "ret10": metrics.get("ret10"),
            "ret20": metrics.get("ret20"),
            "volume_ratio": metrics.get("volume_ratio"),
            "bias20_ratio": metrics.get("bias20_ratio"),
            "ma10": metrics.get("ma10"),
            "ma20": metrics.get("ma20"),
            "close": metrics.get("close"),
            "trade_date": metrics.get("trade_date"),
            "turnover_rate": candidate.get("turnover_rate"),
        }
        for key, value in metric_mapping.items():
            if not self._is_missing(value):
                key_metrics[key] = value
                if key in {"ret5", "ret10", "ret20", "volume_ratio", "bias20_ratio"}:
                    candidate[key] = value
        candidate["key_metrics"] = key_metrics

    @staticmethod
    def _infer_industry_from_enrichment(enrichment: Dict[str, Any]) -> str:
        """从候选增强数据里提取行业，兜底修复本地基础表行业缺失。"""
        if not isinstance(enrichment, dict):
            return ""
        profile = enrichment.get("profile") if isinstance(enrichment.get("profile"), dict) else {}
        industry = str(profile.get("industry") or "").strip()
        if industry:
            return industry

        for item in enrichment.get("research_reports") or []:
            if not isinstance(item, dict):
                continue
            industry = str(item.get("industry") or "").strip()
            if industry:
                return industry

        for item in enrichment.get("eps_forecasts") or []:
            if not isinstance(item, dict):
                continue
            industry = str(item.get("industry") or "").strip()
            if industry:
                return industry
        return ""
