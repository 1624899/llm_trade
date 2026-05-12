"""
大模型多智能体选股协调器 (Coordinator)

职责：
1. 先执行确定性技术面预筛选（不依赖 LLM）。
2. 编排并调度多个专属 Agent（宏观、基本面、技术面、资讯风控、决策）。
3. 汇集所有分析结果，输出最终的优质股票推荐报告。
"""

import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
import akshare as ak
from loguru import logger

from src.agent.decision_agent import DecisionAgent
from src.agent.fundamental_agent import FundamentalAgent
from src.agent.macro_agent import MacroAgent
from src.agent.news_agent import NewsRiskAgent
from src.agent.quick_filter_agent import QuickFilterAgent
from src.agent.reflection_agent import ReflectionAgent
from src.agent.exit_agent import ExitAgent
from src.agent.trading_agent import TradingAgent
from src.stock_screener import StockScreener
from src.agent.technical_agent import TechnicalAgent
from src.agent.trace_recorder import trace_recorder
from src.database import StockDatabase
from src.evaluation.paper_trading import PaperTrading
from src.evaluation.trading_account import TradingAccount
from src.evaluation.watchlist import Watchlist
from src.evaluation.backtest import BacktestEngine
from src.quote_sources import fetch_latest_prices


DEFAULT_CANDIDATE_ANALYSIS_MAX_WORKERS = 3


class AgentCoordinator:
    """选股智能体中枢统筹。"""

    def __init__(self, config_path: Optional[str] = None):
        logger.info("初始化 Agent Coordinator ...")
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "config",
            "config.yaml",
        )
        self.config = self._load_config(self.config_path)
        self.candidate_analysis_max_workers = self._get_candidate_analysis_max_workers()

        self.screener = StockScreener()
        self.macro_agent = MacroAgent()
        self.fundamental_agent = FundamentalAgent()
        self.technical_agent = TechnicalAgent()
        self.news_risk_agent = NewsRiskAgent()
        self.quick_filter_agent = QuickFilterAgent()
        self.decision_agent = DecisionAgent()
        self.reflection_agent = ReflectionAgent()
        self.db = StockDatabase()
        self.paper_trading = PaperTrading(db=self.db)
        self.watchlist = Watchlist(db=self.db, max_items=10)
        self.trading_account = TradingAccount(db=self.db, **self._get_trading_account_config())
        self.exit_agent = ExitAgent(db=self.db)
        self.trading_agent = TradingAgent()
        self.backtest_engine = BacktestEngine(db=self.db)

    def run_picking_workflow(self, max_candidates: Optional[int] = None) -> str:
        """
        端到端全链路自动化选股流程。

        流程：多策略规则海选 → 宏观分析 → AI 轻量精筛 → 资讯硬风控 → 个股深度分析 → 决策输出。
        """
        logger.info("=== 开始选股任务 ===")
        start_time = time.time()
        trace_path = trace_recorder.start()

        prefilter_n, deep_review_n = self._get_picking_candidate_limits(max_candidates)
        logger.info(f"[Step 1] 多策略规则海选，目标 Top {prefilter_n} ...")
        prefilter_candidates = self.screener.run_technical_screening(top_n=prefilter_n)

        if not prefilter_candidates:
            logger.error("技术预筛选未找到任何符合条件的股票，流程终止。")
            return "当前市场未找到符合技术面筛选条件的股票，请稍后重试。"

        logger.info(f"规则海选完成，{len(prefilter_candidates)} 只候选进入宏观适配精筛。")

        logger.info("[Step 2] 宏观环境分析...")
        backtest_context: Dict[str, Any] = {"enabled": True, "source": "stock_screener"}
        try:
            saved_count = self.backtest_engine.record_signal_snapshot(
                prefilter_candidates,
                market_regime=getattr(self.screener, "market_regime", {}),
            )
            logger.info(
                "[Backtest] signal snapshots saved={}; walk_forward weights are applied inside StockScreener.",
                saved_count,
            )
        except Exception as exc:
            logger.warning("[Backtest] 回测信号快照记录失败: {}", exc)
            backtest_context = {"enabled": False, "source": "stock_screener", "summary": f"回测信号快照记录失败: {exc}"}

        candidate_brief = ", ".join(
            f"{item.get('name')}({item.get('code')}) score={item.get('technical_score', '-')}"
            for item in prefilter_candidates
        )
        logger.info(f"[Step 1] Candidate brief: {candidate_brief}")

        macro_context = self.macro_agent.analyze_macro_environment()
        logger.debug(f"宏观分析结果: {macro_context}")

        logger.info(f"[Step 3] AI 轻量精筛，目标 {deep_review_n} 只进入深度复核...")
        quick_filter_result = self.quick_filter_agent.filter_candidates(
            prefilter_candidates,
            macro_context=macro_context,
            target_n=deep_review_n,
        )
        candidates = quick_filter_result.get("selected_candidates") or []
        if not candidates:
            logger.error("AI 轻量精筛后没有候选股进入深度复核，流程终止。")
            return "AI 轻量精筛后没有候选股进入深度复核，本轮不推荐。\n[CODE_LIST] [/CODE_LIST]"
        logger.info(
            "[Step 3] 精筛完成，{} -> {}，mode={}，codes={}",
            len(prefilter_candidates),
            len(candidates),
            quick_filter_result.get("mode"),
            quick_filter_result.get("selected_codes"),
        )

        logger.info("[Step 4] 进入资讯硬风控与深度个案分析阶段...")
        detailed_reports, analysis_errors = self._analyze_candidates_concurrently(candidates, macro_context)

        if analysis_errors:
            logger.warning(f"[Step 4] {len(analysis_errors)} 只候选分析出现异常: {analysis_errors}")
        if not detailed_reports:
            logger.error("所有候选股深度分析均失败，流程终止。")
            return "所有候选股深度分析均失败，本轮不推荐。\n[CODE_LIST] [/CODE_LIST]"

        logger.info("[Step 5] 首席决策 Agent 综合排序并输出最终推荐...")
        final_markdown_report, selected_codes = self.decision_agent.synthesize_and_elect_winners(
            detailed_reports,
            "基于技术面预筛和深度分析，选出最优标的",
            pick_n=3,
            macro_context=macro_context,
        )

        if analysis_errors:
            final_markdown_report = self._append_analysis_error_summary(final_markdown_report, analysis_errors)

        logger.info(f"[Watchlist] 尝试将推荐股票更新到观察仓: {selected_codes}")
        stock_profiles_map = {item["asset_info"]["code"]: item for item in detailed_reports}
        watch_count = self.watchlist.upsert_recommendations(
            selected_codes,
            stock_profiles_map,
            final_report=final_markdown_report,
            macro_context=macro_context,
        )
        logger.info("[Watchlist] 本轮更新 {} 只观察标的；交易仓不会在 --pick 中自动买入", watch_count)

        elapsed = time.time() - start_time
        self._save_workflow_audit(
            prefilter_candidates=prefilter_candidates,
            candidates=candidates,
            macro_context=macro_context,
            quick_filter_result=quick_filter_result,
            backtest_context=backtest_context,
            detailed_reports=detailed_reports,
            selected_codes=selected_codes,
            analysis_errors=analysis_errors,
            final_report=final_markdown_report,
            elapsed_seconds=elapsed,
            trace_path=str(trace_path),
        )
        trace_recorder.finish()
        logger.info(f"=== 选股任务结束，耗时 {elapsed:.1f} 秒 ===")
        return final_markdown_report

    def _save_workflow_audit(
        self,
        prefilter_candidates: List[Dict[str, Any]],
        candidates: List[Dict[str, Any]],
        macro_context: Dict[str, Any],
        quick_filter_result: Dict[str, Any],
        backtest_context: Dict[str, Any],
        detailed_reports: List[Dict[str, Any]],
        selected_codes: List[str],
        analysis_errors: List[Dict[str, str]],
        final_report: str,
        elapsed_seconds: float,
        trace_path: str,
    ) -> None:
        output_dir = Path("outputs")
        output_dir.mkdir(parents=True, exist_ok=True)
        audit = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "trace_path": trace_path,
            "prefilter_candidates": prefilter_candidates,
            "candidates": candidates,
            "macro_context": macro_context,
            "quick_filter_result": quick_filter_result,
            "backtest_context": backtest_context,
            "detailed_reports": detailed_reports,
            "selected_codes": selected_codes,
            "analysis_errors": analysis_errors,
            "final_report": final_report,
        }
        latest_path = output_dir / "latest_workflow_audit.json"
        timestamped_path = output_dir / f"workflow_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            text = json.dumps(audit, ensure_ascii=False, indent=2, default=str)
            latest_path.write_text(text, encoding="utf-8")
            timestamped_path.write_text(text, encoding="utf-8")
            logger.info(f"[Audit] workflow audit saved to {latest_path}")
        except Exception as exc:
            logger.warning(f"[Audit] failed to save workflow audit: {exc}")

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

        news_risk_report = self.news_risk_agent.analyze(code, name, macro_context=macro_context)
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
                "asset_info": stock,
                "fundamental_analysis": skip_reason,
                "technical_analysis": skip_reason,
                "news_risk_analysis": news_risk_report,
            }

        fund_report = self.fundamental_agent.analyze(code, name, macro_context=macro_context)
        logger.info(f"  -> Fundamental done: {name}({code}), chars={len(str(fund_report or ''))}")
        tech_report = self.technical_agent.analyze(code, name, macro_context=macro_context)
        logger.info(f"  -> Technical done: {name}({code}), chars={len(str(tech_report or ''))}")

        return {
            "asset_info": stock,
            "fundamental_analysis": fund_report,
            "technical_analysis": tech_report,
            "news_risk_analysis": news_risk_report,
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

    def _append_analysis_error_summary(self, report: str, errors: List[Dict[str, str]]) -> str:
        lines = ["", "## 深度分析异常摘要"]
        for item in errors:
            lines.append(f"- {item.get('name')}({item.get('code')}): {item.get('error')}")
        return report.rstrip() + "\n" + "\n".join(lines)

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
        realtime_prices = fetch_latest_prices(codes)
        if realtime_prices:
            logger.info(
                "[TargetedAnalysis] 实时行情覆盖 {} / {} 只指定标的价格",
                len(realtime_prices),
                len(codes),
            )

        candidates = []
        for code in codes:
            basic = basic_map.get(code, {})
            quote = quote_map.get(code, {})
            realtime_price = realtime_prices.get(code)
            profile_fallback = self._fetch_target_profile_fallback(code, quote)
            industry = self._first_present(
                basic.get("industry"),
                profile_fallback.get("industry"),
            )
            pb = self._first_present(
                quote.get("pb"),
                profile_fallback.get("pb"),
            )
            total_market_cap = self._first_present(
                quote.get("total_market_cap"),
                profile_fallback.get("total_market_cap"),
            )
            candidates.append(
                {
                    "code": code,
                    "name": basic.get("name") or profile_fallback.get("name") or code,
                    "industry": industry or "",
                    "price": realtime_price if realtime_price is not None else quote.get("price"),
                    "change_pct": quote.get("change_pct"),
                    "volume": quote.get("volume"),
                    "amount": quote.get("amount"),
                    "turnover_rate": quote.get("turnover_rate"),
                    "pe_ttm": quote.get("pe_ttm"),
                    "pb": pb,
                    "total_market_cap": total_market_cap,
                    "trade_date": quote.get("trade_date"),
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

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if not AgentCoordinator._is_missing(value):
                return value
        return None

    @staticmethod
    def _is_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip() or value.strip().lower() in {"nan", "none", "null", "未知", "n/a"}
        if isinstance(value, float):
            return math.isnan(value)
        return False

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if AgentCoordinator._is_missing(value):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _format_targeted_analysis_report(
        self,
        detailed_reports: List[Dict[str, Any]],
        macro_context: Dict[str, Any],
        analysis_errors: List[Dict[str, str]],
    ) -> str:
        """生成指定股票单独分析 Markdown 报告。"""
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            "# 指定股票单独分析报告",
            "",
            f"- 生成时间：{now_text}",
            f"- 分析股票数：{len(detailed_reports)}",
            "- 流程说明：用户指定股票 -> 宏观环境 -> 基本面 Agent -> 技术面 Agent -> 资讯风控 Agent",
            "",
            "## 宏观环境摘要",
            "",
            f"- 市场情绪：{macro_context.get('market_sentiment', 'unknown')}",
            f"- 风险偏好：{macro_context.get('risk_appetite', 'unknown')}",
            f"- 流动性判断：{macro_context.get('liquidity_view', 'unknown')}",
            f"- 分析重点：{macro_context.get('analysis_focus', 'unknown')}",
            "",
        ]

        lines.extend(self._format_targeted_summary(detailed_reports))

        for item in detailed_reports:
            asset = item.get("asset_info", {})
            code = asset.get("code", "")
            name = asset.get("name", "")
            lines.extend(
                [
                    f"## {name}({code})",
                    "",
                    "### 快照",
                    "",
                    f"- 行业：{asset.get('industry') or '未知'}",
                    f"- 最新交易日：{asset.get('trade_date') or '本地行情缺失'}",
                    f"- 最新价：{self._format_snapshot_number(asset.get('price'))}",
                    f"- 涨跌幅：{self._format_snapshot_number(asset.get('change_pct'), suffix='%')}",
                    f"- 换手率：{self._format_snapshot_number(asset.get('turnover_rate'), suffix='%')}",
                    f"- PE(TTM)：{self._format_snapshot_number(asset.get('pe_ttm'))}",
                    f"- PB：{self._format_snapshot_number(asset.get('pb'))}",
                    "",
                    "### 基本面 Agent",
                    "",
                    str(item.get("fundamental_analysis", "")),
                    "",
                    "### 技术面 Agent",
                    "",
                    str(item.get("technical_analysis", "")),
                    "",
                    "### 资讯风控 Agent",
                    "",
                    self._format_news_risk_section(item.get("news_risk_analysis")),
                    "",
                ]
            )

        if analysis_errors:
            lines.append("## 分析异常")
            lines.append("")
            for error in analysis_errors:
                lines.append(f"- {error.get('name')}({error.get('code')}): {error.get('error')}")
            lines.append("")

        lines.append("> 本报告仅用于研究、复盘和工程实验，不构成任何投资建议。")
        return "\n".join(lines)

    def _format_snapshot_number(self, value: Any, *, suffix: str = "", digits: int = 2) -> str:
        number = self._to_float(value)
        if number is None:
            return "未知"
        text = f"{number:.{digits}f}".rstrip("0").rstrip(".")
        return f"{text}{suffix}"

    def _format_targeted_summary(self, detailed_reports: List[Dict[str, Any]]) -> List[str]:
        """生成指定股票分析的总评表，避免报告只有明细没有结论。"""
        if not detailed_reports:
            return []
        lines = [
            "## 综合结论",
            "",
            "| 股票 | 综合倾向 | 中期动作 | 核心矛盾 | 资讯风控 |",
            "| --- | --- | --- | --- | --- |",
        ]
        for item in detailed_reports:
            asset = item.get("asset_info", {})
            risk = item.get("news_risk_analysis") if isinstance(item.get("news_risk_analysis"), dict) else {}
            view = self._infer_targeted_view(item)
            lines.append(
                "| {name}({code}) | {view} | {mid_action} | {reason} | {risk_level}/{action} |".format(
                    name=asset.get("name", ""),
                    code=asset.get("code", ""),
                    view=view,
                    mid_action=self._targeted_midterm_action(view),
                    reason=self._build_targeted_reason(item),
                    risk_level=risk.get("risk_level", "unknown"),
                    action=risk.get("action", "unknown"),
                )
            )
        lines.append("")
        return lines

    def _infer_targeted_view(self, report: Dict[str, Any]) -> str:
        """按基本面、技术面和资讯风控给出中期视角的单股总评。"""
        fund_text = self._extract_targeted_signal_text(str(report.get("fundamental_analysis", "")))
        tech_text = self._extract_targeted_signal_text(str(report.get("technical_analysis", "")))
        risk = report.get("news_risk_analysis") if isinstance(report.get("news_risk_analysis"), dict) else {}
        if risk.get("hard_exclude"):
            return "回避"
        bad_fund_words = ["弱", "恶化", "下滑", "背离", "减值", "亏损", "负债压力"]
        bad_tech_words = ["跌破", "破位", "趋势失效", "放量滞涨", "上影派发", "弱势"]
        good_fund = any(word in fund_text for word in ["强", "增长", "改善", "高景气", "现金流良好", "盈利预期"])
        good_tech = not any(word in tech_text for word in bad_tech_words)
        if any(word in fund_text for word in bad_fund_words) and any(word in tech_text for word in bad_tech_words):
            return "减仓/回避"
        if good_fund and good_tech and risk.get("risk_level") == "low":
            return "中期积极"
        if "中性" in fund_text and any(word in tech_text for word in ["观望", "轻仓", "低吸"]):
            return "观察/轻仓验证"
        if risk.get("risk_level") == "low":
            return "中期观察"
        return "谨慎观望"

    @staticmethod
    def _extract_targeted_signal_text(text: str) -> str:
        """只保留当前判断段，避免把条件性止损/减仓条款误判成当下结论。"""
        if not text:
            return ""
        risk_headings = ("止损/失效", "减仓/清仓条件", "量能与风险", "质量风险", "风险评估")
        signal_headings = (
            "基本面结论",
            "最终评级",
            "技术结论",
            "买点/触发",
            "操作建议",
            "宏观适配",
            "财务趋势",
            "盈利预期",
        )
        kept: List[str] = []
        skipping_risk_block = False
        for raw_line in text.splitlines():
            line = re.sub(r"[*#`>\s]+", "", raw_line or "")
            if not line:
                continue
            if any(heading in line for heading in risk_headings):
                skipping_risk_block = True
                continue
            if any(heading in line for heading in signal_headings):
                skipping_risk_block = False
            if not skipping_risk_block:
                kept.append(raw_line)
        return "\n".join(kept)

    @staticmethod
    def _targeted_midterm_action(view: str) -> str:
        if view == "中期积极":
            return "可配置，回踩不破关键支撑可持有"
        if view == "观察/轻仓验证":
            return "轻仓或等待触发，不追高"
        if view == "减仓/回避":
            return "减仓或清仓，等待基本面/技术面修复"
        if view == "回避":
            return "不参与"
        return "等待更清晰信号"

    def _build_targeted_reason(self, report: Dict[str, Any]) -> str:
        """抽取基本面和技术面的第一层结论，压缩成总评理由。"""
        fund = self._first_meaningful_sentence(str(report.get("fundamental_analysis", "")))
        tech = self._first_meaningful_sentence(str(report.get("technical_analysis", "")))
        parts = [part for part in [fund, tech] if part]
        return "；".join(parts)[:180] if parts else "暂无足够结论"

    def _first_meaningful_sentence(self, text: str) -> str:
        text = re.sub(r"[*#`>\r\n]+", " ", text or "")
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return ""
        parts = re.split(r"[。；;]", text)
        return parts[0].strip()[:90] if parts else text[:90]

    def _format_news_risk_section(self, risk: Any) -> str:
        """把资讯风控结构化结果转成可读报告，不直接暴露 raw_news。"""
        if not isinstance(risk, dict):
            return str(risk or "")
        news_quality = risk.get("news_quality") or {}
        quality_text = ""
        if isinstance(news_quality, dict) and news_quality:
            quality_text = (
                f"{news_quality.get('quality', 'unknown')} "
                f"(raw={news_quality.get('raw_count', 0)}, "
                f"direct={news_quality.get('direct_count', 0)}, "
                f"mention={news_quality.get('mention_count', 0)})"
            )
        lines = [
            f"- 风险等级：{risk.get('risk_level', 'unknown')}",
            f"- 处理动作：{risk.get('action', 'unknown')}",
            f"- 硬排除：{risk.get('hard_exclude', False)}",
            f"- 规则摘要：{risk.get('summary', '')}",
            "",
            "#### LLM 风控结论",
            "",
            str(risk.get("llm_report") or "无"),
        ]
        if quality_text:
            lines.insert(4, f"- 新闻质量：{quality_text}")
        evidence = risk.get("evidence") or []
        if evidence:
            lines.extend(["", "#### 命中证据", ""])
            lines.extend(f"- {item}" for item in evidence)
        relevant_news = str(risk.get("relevant_news") or "").strip()
        if relevant_news:
            lines.extend(["", "#### 直接相关新闻", "", relevant_news])
        elif risk.get("raw_news"):
            lines.extend([
                "",
                "#### 直接相关新闻",
                "",
                "原始新闻源有返回结果，但未匹配到直接包含该股代码或名称的有效标题，未纳入核心风控证据。",
            ])
        news_detail_info = str(risk.get("news_detail_info") or "").strip()
        if news_detail_info:
            lines.extend(["", "#### 直接新闻正文摘要", "", news_detail_info])
        mention_news = str(risk.get("mention_news") or "").strip()
        if mention_news:
            lines.extend(["", "#### 弱相关提及（仅作背景）", "", mention_news])
        announcement_info = str(risk.get("announcement_info") or "").strip()
        if announcement_info:
            lines.extend(["", "#### 近期公告", "", announcement_info])
        announcement_detail_info = str(risk.get("announcement_detail_info") or "").strip()
        if announcement_detail_info:
            lines.extend(["", "#### 重点公告正文摘要", "", announcement_detail_info])
        return "\n".join(lines)
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

    def _get_candidate_analysis_max_workers(self) -> int:
        workflow_cfg = self.config.get("agent_workflow", {}) if isinstance(self.config, dict) else {}
        value = workflow_cfg.get("candidate_analysis_max_workers", DEFAULT_CANDIDATE_ANALYSIS_MAX_WORKERS)
        try:
            workers = int(value)
        except (TypeError, ValueError):
            workers = DEFAULT_CANDIDATE_ANALYSIS_MAX_WORKERS
        return max(1, min(workers, 5))

    def _get_trading_account_config(self) -> Dict[str, Any]:
        """读取交易仓配置，让用户可以通过 config/config.yaml 设置现金和交易约束。"""
        raw_cfg = self.config.get("trading_account", {}) if isinstance(self.config, dict) else {}
        if not isinstance(raw_cfg, dict):
            return {}
        allowed = {
            "account_name": str,
            "initial_cash": float,
            "max_positions": int,
            "lot_size": int,
            "min_holding_days": int,
            "rebuy_cooldown_days": int,
            "max_buys_per_run": int,
            "max_sells_per_run": int,
        }
        config: Dict[str, Any] = {}
        for key, caster in allowed.items():
            if key not in raw_cfg:
                continue
            try:
                value = caster(raw_cfg[key])
            except (TypeError, ValueError):
                logger.warning("[TradingAccount] 忽略非法配置 {}={}", key, raw_cfg[key])
                continue
            if key != "account_name" and value <= 0:
                logger.warning("[TradingAccount] 忽略非正数配置 {}={}", key, raw_cfg[key])
                continue
            config[key] = value
        return config

    def _get_picking_candidate_limits(self, max_candidates: Optional[int] = None) -> Tuple[int, int]:
        """读取选股数量配置：规则海选放宽，深度复核保持成本可控。"""
        output_cfg = getattr(self.screener, "stock_picking_config", {}).get("output", {}) or {}
        try:
            configured_prefilter = int(output_cfg.get("top_n") or 40)
        except (TypeError, ValueError):
            configured_prefilter = 40
        try:
            configured_max_prefilter = int(output_cfg.get("max_top_n") or configured_prefilter)
        except (TypeError, ValueError):
            configured_max_prefilter = configured_prefilter
        try:
            configured_deep_review = int(output_cfg.get("quick_filter_top_n") or 10)
        except (TypeError, ValueError):
            configured_deep_review = 10

        prefilter_n = max(10, min(configured_prefilter, max(configured_max_prefilter, 10)))
        deep_review_n = max(1, min(configured_deep_review, prefilter_n, 20))
        if max_candidates is not None:
            try:
                requested = int(max_candidates)
                deep_review_n = max(1, min(requested, prefilter_n, 20))
            except (TypeError, ValueError):
                pass
        return prefilter_n, deep_review_n

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        try:
            if not os.path.exists(config_path):
                return {}
            with open(config_path, "r", encoding="utf-8") as file:
                data = yaml.safe_load(file) or {}
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning(f"读取 Coordinator 配置失败，使用默认值: {exc}")
            return {}

    def run_trading_workflow(self) -> str:
        """
        独立模拟交易流程：
        1. 刷新观察仓和交易仓价格。
        2. 生成持仓退出信号。
        3. TradingAgent 给出买卖/持有决策。
        4. TradingAccount 校验硬约束并写入交易流水。
        """
        logger.info("=== 开始交易 Agent 模拟调仓 ===")
        self.watchlist.refresh_prices()
        account = self.trading_account.refresh_positions()
        positions = self.trading_account.list_open_positions()
        watch_items = self.watchlist.list_active()
        if self._watchlist_needs_trade_refresh(watch_items, positions):
            watch_items = self._ensure_watchlist_for_trade(watch_items, positions)
            self.watchlist.refresh_prices()
            watch_items = self.watchlist.list_active()
        macro_context = self.macro_agent.analyze_macro_environment()
        exit_signals = self._build_exit_signals(positions, macro_context)

        decisions = self.trading_agent.decide(
            watchlist=watch_items,
            positions=positions,
            account=account,
            exit_signals=exit_signals,
            macro_context=macro_context,
        )
        self.watchlist.apply_trading_decisions(decisions)
        results = self.trading_account.execute_decisions(decisions)
        report = self.trading_account.format_report(results)
        print(report)
        logger.info("=== 交易 Agent 模拟调仓结束 ===")
        return report

    def _watchlist_needs_trade_refresh(
        self,
        watch_items: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
    ) -> bool:
        """观察仓为空，或缺少当前持仓分析时，都需要先维护候选池。"""
        if not watch_items:
            return True
        watch_codes = {str(item.get("code") or "").zfill(6) for item in watch_items}
        position_codes = {str(item.get("code") or "").zfill(6) for item in positions if item.get("code")}
        return bool(position_codes - watch_codes)

    def _ensure_watchlist_for_trade(
        self,
        watch_items: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """交易前维护观察仓：空仓或缺少持仓分析时先补候选。"""
        watch_codes = {str(item.get("code") or "").zfill(6) for item in watch_items}
        position_codes = [str(item.get("code") or "").zfill(6) for item in positions if item.get("code")]
        position_codes = [code for code in position_codes if len(code) == 6 and code.isdigit()]
        missing_position_codes = [code for code in position_codes if code not in watch_codes]
        if missing_position_codes:
            logger.warning(
                "[Watchlist] 观察仓缺少当前交易持仓分析，先执行指定分析并写回观察仓: {}",
                missing_position_codes,
            )
            self.run_targeted_analysis(
                missing_position_codes,
                update_watchlist=True,
                watchlist_source="trade_position_refresh",
            )
        elif not watch_items:
            logger.warning("[Watchlist] 观察仓和交易仓均为空，先运行一轮选股流程生成交易候选池。")
            self.run_picking_workflow()
        return self.watchlist.list_active()

    def _build_exit_signals(
        self,
        positions: List[Dict[str, Any]],
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        signals: Dict[str, Dict[str, Any]] = {}
        for position in positions:
            code = str(position.get("code") or "").zfill(6)
            if not code:
                continue
            exit_position = dict(position)
            exit_position["recommend_price"] = position.get("avg_cost")
            exit_position["return_pct"] = position.get("unrealized_return_pct")
            try:
                signals[code] = self.exit_agent.evaluate_position(
                    exit_position,
                    macro_context=macro_context,
                ).to_dict()
            except Exception as exc:
                logger.warning("[TradingWorkflow] exit signal failed for {}: {}", code, exc)
        return signals

    def run_post_market_routine(self):
        """
        每日盘后例行维护：
        1. 结算虚拟观察仓当天浮盈浮亏。
        2. 输出持仓动作诊断。
        3. 仅对已清仓交易仓亏损样本触发 AI 反思。
        """
        logger.info("=== 开始盘后观察仓结算与已清仓交易复盘 ===")
        self.watchlist.refresh_prices()
        self.trading_account.refresh_positions()
        self.reflection_agent.generate_trading_reflections(threshold_pct=-3.0)

        self.paper_trading.update_portfolio()

        macro_context = self.macro_agent.analyze_macro_environment()
        diagnostics = self.paper_trading.diagnose_portfolio(macro_context=macro_context)
        review_report = self.paper_trading.format_diagnostics_report(diagnostics)
        trading_diagnostics = self.trading_account.format_post_market_diagnostics(macro_context=macro_context)
        trading_report = self.trading_account.format_report([])
        print(review_report)
        print(self.paper_trading.show_portfolio())
        print(trading_diagnostics)
        print(trading_report)

        logger.info("=== 盘后例行任务结束 ===")
        return review_report + "\n\n" + trading_diagnostics + "\n\n" + trading_report


if __name__ == "__main__":
    coordinator = AgentCoordinator()
    # print(coordinator.run_picking_workflow())
    # coordinator.run_post_market_routine()
