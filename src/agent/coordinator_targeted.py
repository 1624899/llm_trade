"""指定分析 Mixin。

从 coordinator.py 拆分：指定分析全流程、候选构建、兜底补齐、强推荐写回。
"""

import re
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import akshare as ak
from loguru import logger

from src.agent.trace_recorder import trace_recorder
from src.evaluation.watchlist import Watchlist


class CoordinatorTargetedMixin:
    """指定股票单独分析全流程。"""

    def run_targeted_analysis(
        self,
        codes: List[str],
        *,
        update_watchlist: bool = False,
        watchlist_source: str = "targeted_analysis",
    ) -> str:
        """对用户指定的股票做单独深度分析；强推荐结果会自动补入观察仓。"""
        normalized_codes = self._normalize_target_codes(codes)
        if not normalized_codes:
            return "没有识别到有效的 6 位股票代码。"

        logger.info(f"=== 开始指定股票单独分析：{', '.join(normalized_codes)} ===")
        start_time = time.time()
        trace_path = trace_recorder.start()

        candidates = self._build_target_candidates(normalized_codes)
        macro_context = self.macro_agent.analyze_macro_environment()
        detailed_reports, analysis_errors = self._analyze_candidates_concurrently(candidates, macro_context)
        report = self._format_targeted_analysis_report(detailed_reports, macro_context, analysis_errors)
        if update_watchlist and detailed_reports:
            stock_profiles_map = {item["asset_info"]["code"]: item for item in detailed_reports}
            updated = self.watchlist.upsert_recommendations(
                normalized_codes,
                stock_profiles_map,
                final_report=report,
                macro_context=macro_context,
                source=watchlist_source,
            )
            logger.info("[Watchlist] 指定分析写回观察仓 {} 只标的", updated)
        elif detailed_reports:
            updated = self._upsert_targeted_strong_recommendations(
                detailed_reports,
                final_report=report,
                macro_context=macro_context,
                source=watchlist_source,
            )
            if updated:
                logger.info("[Watchlist] 指定分析强推荐自动加入观察仓 {} 只标的", updated)

        elapsed = time.time() - start_time
        self._save_targeted_analysis_audit(
            requested_codes=normalized_codes,
            candidates=candidates,
            macro_context=macro_context,
            detailed_reports=detailed_reports,
            analysis_errors=analysis_errors,
            final_report=report,
            elapsed_seconds=elapsed,
            trace_path=str(trace_path),
        )
        trace_recorder.finish()
        logger.info(f"=== 指定股票单独分析结束，耗时 {elapsed:.1f} 秒 ===")
        return report

    def _upsert_targeted_strong_recommendations(
        self,
        detailed_reports: List[Dict[str, Any]],
        final_report: str,
        macro_context: Optional[Dict[str, Any]] = None,
        source: str = "targeted_analysis",
    ) -> int:
        """把指定分析中明确为强推荐、且不在 ACTIVE 观察仓的股票补入观察仓。"""
        if not detailed_reports:
            return 0
        active_codes = {
            str(item.get("code") or "").zfill(6)
            for item in self.watchlist.list_active()
            if item.get("code")
        }
        strong_codes: List[str] = []
        profiles: Dict[str, Dict[str, Any]] = {}
        for item in detailed_reports:
            asset = item.get("asset_info") or {}
            code = str(asset.get("code") or "").zfill(6)
            if not re.fullmatch(r"\d{6}", code) or code in active_codes:
                continue
            if self._infer_targeted_report_tier(code, final_report) != "强推荐":
                continue
            strong_codes.append(code)
            profiles[code] = item

        if not strong_codes:
            return 0
        return self.watchlist.upsert_recommendations(
            strong_codes,
            profiles,
            final_report=final_report,
            macro_context=macro_context,
            source=source,
        )

    @staticmethod
    def _infer_targeted_report_tier(code: str, final_report: str) -> str:
        """指定分析按代码所在表格行判定档位，避免相邻股票串行误判。"""
        normalized_code = str(code).zfill(6)
        for raw_line in (final_report or "").splitlines():
            line = raw_line.strip()
            if normalized_code not in line:
                continue
            search_text = line if line.startswith("|") and line.endswith("|") else line[:260]
            for tier in ("强推荐", "配置/轻仓验证", "观察", "不推荐"):
                if tier in search_text:
                    return tier
        return Watchlist._infer_tier(normalized_code, final_report)

    def _normalize_target_codes(self, codes: List[str]) -> List[str]:
        """清洗命令行输入的股票代码，保留原始顺序并去重。"""
        normalized: List[str] = []
        seen = set()
        for raw_code in codes or []:
            for part in str(raw_code).replace("，", ",").split(","):
                code = part.strip()
                if code.startswith(("sh", "sz", "SH", "SZ")):
                    code = code[2:]
                if len(code) == 6 and code.isdigit() and code not in seen:
                    normalized.append(code)
                    seen.add(code)
        return normalized

    def _build_target_candidates(self, codes: List[str]) -> List[Dict[str, Any]]:
        """从本地数据湖补齐股票名称和行情；指定分析优先用实时价格覆盖本地快照。"""
        placeholders = ",".join(["?"] * len(codes))
        basic_df = self.db.query_to_dataframe(
            f"SELECT code, name, industry FROM stock_basic WHERE code IN ({placeholders})",
            tuple(codes),
        )
        quote_df = self.db.query_to_dataframe(
            f"""
            SELECT q.code, q.price, q.change_pct, q.volume, q.amount, q.turnover_rate,
                   q.pe_ttm, q.pb, q.total_market_cap, q.trade_date
            FROM daily_quotes q
            JOIN (
                SELECT code, MAX(trade_date) AS trade_date
                FROM daily_quotes
                WHERE code IN ({placeholders})
                GROUP BY code
            ) latest
            ON q.code = latest.code AND q.trade_date = latest.trade_date
            """,
            tuple(codes),
        )

        basic_map = {row["code"]: row for _, row in basic_df.iterrows()} if not basic_df.empty else {}
        quote_map = {row["code"]: row for _, row in quote_df.iterrows()} if not quote_df.empty else {}

        from src.agent.coordinator import fetch_latest_quotes  # late import for patch compatibility
        realtime_quotes = fetch_latest_quotes(codes)
        if realtime_quotes:
            logger.info(
                "[TargetedAnalysis] 实时行情覆盖 {} / {} 只指定标的快照",
                len(realtime_quotes),
                len(codes),
            )

        candidates = []
        for code in codes:
            basic = basic_map.get(code, {})
            quote = quote_map.get(code, {})
            realtime_quote = realtime_quotes.get(code, {})
            merged_quote = {
                "trade_date": self._first_present(realtime_quote.get("trade_date"), quote.get("trade_date")),
                "price": self._first_present(realtime_quote.get("price"), quote.get("price")),
                "change_pct": self._first_present(realtime_quote.get("change_pct"), quote.get("change_pct")),
                "volume": self._first_present(realtime_quote.get("volume"), quote.get("volume")),
                "amount": self._first_present(realtime_quote.get("amount"), quote.get("amount")),
                "turnover_rate": self._first_present(realtime_quote.get("turnover_rate"), quote.get("turnover_rate")),
                "pe_ttm": self._first_present(realtime_quote.get("pe_ttm"), quote.get("pe_ttm")),
                "pb": self._first_present(realtime_quote.get("pb"), quote.get("pb")),
                "total_market_cap": self._first_present(
                    realtime_quote.get("total_market_cap"),
                    quote.get("total_market_cap"),
                ),
            }
            profile_fallback = self._fetch_target_profile_fallback(code, merged_quote)
            industry = self._first_present(
                basic.get("industry"),
                profile_fallback.get("industry"),
            )
            pb = self._first_present(
                merged_quote.get("pb"),
                profile_fallback.get("pb"),
            )
            total_market_cap = self._first_present(
                merged_quote.get("total_market_cap"),
                profile_fallback.get("total_market_cap"),
            )
            candidates.append(
                {
                    "code": code,
                    "name": basic.get("name") or profile_fallback.get("name") or code,
                    "industry": industry or "",
                    "price": merged_quote.get("price"),
                    "change_pct": merged_quote.get("change_pct"),
                    "volume": merged_quote.get("volume"),
                    "amount": merged_quote.get("amount"),
                    "turnover_rate": merged_quote.get("turnover_rate"),
                    "pe_ttm": self._first_present(merged_quote.get("pe_ttm"), profile_fallback.get("pe_ttm")),
                    "pb": pb,
                    "total_market_cap": total_market_cap,
                    "trade_date": merged_quote.get("trade_date"),
                    "analysis_mode": "targeted",
                    "screen_reason": "用户指定股票，跳过规则海选。",
                }
            )
        return candidates

    def _fetch_target_profile_fallback(self, code: str, quote: Dict[str, Any]) -> Dict[str, Any]:
        """指定分析兜底补齐行业和 PB；只在本地字段缺失时按需触发。"""
        fallback: Dict[str, Any] = {}
        if self._is_missing(quote.get("pb")):
            fallback.update(self._fetch_target_valuation_fallback(code, quote.get("trade_date")))
        if not fallback.get("total_market_cap") and not self._is_missing(quote.get("total_market_cap")):
            fallback["total_market_cap"] = quote.get("total_market_cap")

        if not fallback.get("industry"):
            fallback.update(self._fetch_target_basic_fallback(code))
        return fallback

    def _fetch_target_basic_fallback(self, code: str) -> Dict[str, Any]:
        try:
            df = ak.stock_individual_info_em(symbol=str(code).zfill(6))
        except Exception as exc:
            logger.warning("[TargetedAnalysis] 个股基础信息兜底失败 {}: {}", code, exc)
            return {}
        if df is None or df.empty or "item" not in df.columns or "value" not in df.columns:
            return {}

        info = {str(row.get("item", "")).strip(): row.get("value") for _, row in df.iterrows()}
        return {
            "name": info.get("股票简称"),
            "industry": info.get("行业"),
            "total_market_cap": self._to_float(info.get("总市值")),
        }

    def _fetch_target_valuation_fallback(self, code: str, trade_date: Any = None) -> Dict[str, Any]:
        try:
            df = ak.stock_value_em(symbol=str(code).zfill(6))
        except Exception as exc:
            logger.warning("[TargetedAnalysis] 个股估值兜底失败 {}: {}", code, exc)
            return {}
        if df is None or df.empty:
            return {}

        date_col = "数据日期"
        if date_col in df.columns:
            df = df.copy()
            df[date_col] = df[date_col].astype(str).str.replace("-", "", regex=False)
            target_date = str(trade_date or "").replace("-", "")
            if target_date:
                matched = df[df[date_col] <= target_date].tail(1)
                if not matched.empty:
                    df = matched
                else:
                    df = df.tail(1)
            else:
                df = df.tail(1)
        row = df.iloc[-1]
        return {
            "pb": self._to_float(row.get("市净率")),
            "total_market_cap": self._to_float(row.get("总市值")),
            "pe_ttm": self._to_float(row.get("PE(TTM)")),
        }

    def _save_targeted_analysis_audit(
        self,
        requested_codes: List[str],
        candidates: List[Dict[str, Any]],
        macro_context: Dict[str, Any],
        detailed_reports: List[Dict[str, Any]],
        analysis_errors: List[Dict[str, str]],
        final_report: str,
        elapsed_seconds: float,
        trace_path: str,
    ) -> None:
        output_dir = Path("outputs")
        output_dir.mkdir(parents=True, exist_ok=True)
        latest_report_path = output_dir / "latest_targeted_analysis.md"
        latest_audit_path = output_dir / "latest_targeted_analysis_audit.json"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_report_path = output_dir / f"targeted_analysis_{timestamp}.md"
        timestamped_audit_path = output_dir / f"targeted_analysis_audit_{timestamp}.json"
        audit = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "trace_path": trace_path,
            "requested_codes": requested_codes,
            "candidates": candidates,
            "macro_context": macro_context,
            "detailed_reports": detailed_reports,
            "analysis_errors": analysis_errors,
            "final_report": final_report,
        }
        try:
            latest_report_path.write_text(final_report, encoding="utf-8")
            timestamped_report_path.write_text(final_report, encoding="utf-8")
            text = json.dumps(audit, ensure_ascii=False, indent=2, default=str)
            latest_audit_path.write_text(text, encoding="utf-8")
            timestamped_audit_path.write_text(text, encoding="utf-8")
            logger.info(f"[TargetedAnalysis] report saved to {latest_report_path}")
        except Exception as exc:
            logger.warning(f"[TargetedAnalysis] failed to save report: {exc}")
