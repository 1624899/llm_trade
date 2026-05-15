"""
大模型多智能体选股协调器 (Coordinator)

职责：
1. 先执行确定性技术面预筛选（不依赖 LLM）。
2. 编排并调度多个专属 Agent（宏观、基本面、技术面、资讯风控、决策）。
3. 汇集所有分析结果，输出最终的优质股票推荐报告。
"""

import math
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml
from loguru import logger

from src.agent.coordinator_analysis import CoordinatorAnalysisMixin
from src.agent.coordinator_targeted import CoordinatorTargetedMixin
from src.agent.coordinator_report import CoordinatorReportMixin
from src.agent.coordinator_trading import CoordinatorTradingMixin
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
from src.candidate_enrichment import CandidateEnrichmentService
from src.database import StockDatabase
from src.evaluation.paper_trading import PaperTrading
from src.evaluation.trading_account import TradingAccount
from src.evaluation.watchlist import Watchlist
from src.evaluation.backtest import BacktestEngine
from src.quote_sources import fetch_latest_quotes  # re-export for backward-compatible patch paths
from src.technical_indicators import TechnicalSignalProvider  # re-export for backward-compatible patch paths


DEFAULT_CANDIDATE_ANALYSIS_MAX_WORKERS = 3


class AgentCoordinator(
    CoordinatorAnalysisMixin,
    CoordinatorTargetedMixin,
    CoordinatorReportMixin,
    CoordinatorTradingMixin,
):
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
        self.candidate_enrichment = CandidateEnrichmentService(
            db=self.db,
            config=(self.config.get("candidate_enrichment") or {}),
        )
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
        prefilter_candidates = self._refresh_candidates_realtime_snapshots(prefilter_candidates)
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
        candidates = self._refresh_candidates_realtime_snapshots(candidates)
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
        self.watchlist.refresh_recommendation_performance(include_removed=True)
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

    def run_trading_workflow(self) -> str:
        """
        独立模拟交易流程：
        1. 刷新观察仓和交易仓价格。
        2. 生成持仓退出信号。
        3. TradingAgent 给出买卖/持有决策。
        4. TradingAccount 校验硬约束并写入交易流水。
        """
        logger.info("=== 开始交易 Agent 模拟调仓 ===")
        self.watchlist.refresh_recommendation_performance(include_removed=True)
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
        history_samples = self.watchlist.list_history_for_trading(limit=30)

        decisions = self.trading_agent.decide(
            watchlist=watch_items,
            positions=positions,
            account=account,
            exit_signals=exit_signals,
            macro_context=macro_context,
            historical_recommendations=history_samples,
        )
        self.watchlist.apply_trading_decisions(decisions)
        results = self.trading_account.execute_decisions(decisions)
        report = self.trading_account.format_report(results)
        print(report)
        logger.info("=== 交易 Agent 模拟调仓结束 ===")
        return report

    def run_post_market_routine(self):
        """
        每日盘后例行维护：
        1. 结算虚拟观察仓当天浮盈浮亏。
        2. 输出持仓动作诊断。
        3. 仅对已清仓交易仓亏损样本触发 AI 反思。
        """
        logger.info("=== 开始盘后观察仓结算与已清仓交易复盘 ===")
        self.watchlist.refresh_recommendation_performance(include_removed=True)
        self.watchlist.refresh_prices()
        self.trading_account.refresh_positions()

        self.paper_trading.update_portfolio()

        macro_context = self.macro_agent.analyze_macro_environment()
        diagnostics = self.paper_trading.diagnose_portfolio(macro_context=macro_context)
        review_report = self.paper_trading.format_diagnostics_report(diagnostics)
        trading_diagnostics = self.trading_account.format_post_market_diagnostics(macro_context=macro_context)
        trading_report = self.trading_account.format_report([])

        self.reflection_agent.generate_trading_reflections(threshold_pct=-3.0)

        print(review_report)
        print(self.paper_trading.show_portfolio())
        print(trading_diagnostics)
        print(trading_report)

        logger.info("=== 盘后例行任务结束 ===")
        return review_report + "\n\n" + trading_diagnostics + "\n\n" + trading_report

    # ── 共享工具方法 ──────────────────────────────────────────

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


if __name__ == "__main__":
    coordinator = AgentCoordinator()
    # print(coordinator.run_picking_workflow())
    # coordinator.run_post_market_routine()
