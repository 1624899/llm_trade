"""
大模型多智能体选股协调器 (Coordinator)

职责：
1. 先执行确定性技术面预筛选（不依赖 LLM）。
2. 编排并调度多个专属 Agent（宏观、基本面、技术面、资讯风控、决策）。
3. 汇集所有分析结果，输出最终的优质股票推荐报告。
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from loguru import logger

from src.agent.decision_agent import DecisionAgent
from src.agent.fundamental_agent import FundamentalAgent
from src.agent.macro_agent import MacroAgent
from src.agent.news_agent import NewsRiskAgent
from src.agent.quick_filter_agent import QuickFilterAgent
from src.agent.reflection_agent import ReflectionAgent
from src.stock_screener import StockScreener
from src.agent.technical_agent import TechnicalAgent
from src.agent.trace_recorder import trace_recorder
from src.evaluation.paper_trading import PaperTrading


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
        self.paper_trading = PaperTrading()

    def run_picking_workflow(self, max_candidates: int = 10) -> str:
        """
        端到端全链路自动化选股流程。

        流程：多策略规则海选 → 宏观分析 → AI 轻量精筛 → 个股深度分析 → 决策输出。
        """
        logger.info("=== 开始选股任务 ===")
        start_time = time.time()
        trace_path = trace_recorder.start()

        deep_review_n = max(1, min(int(max_candidates or 8), 8))
        prefilter_n = max(20, deep_review_n)
        logger.info(f"[Step 1] 多策略规则海选，目标 Top {prefilter_n} ...")
        prefilter_candidates = self.screener.run_technical_screening(top_n=prefilter_n)

        if not prefilter_candidates:
            logger.error("技术预筛选未找到任何符合条件的股票，流程终止。")
            return "当前市场未找到符合技术面筛选条件的股票，请稍后重试。"

        logger.info(f"规则海选完成，{len(prefilter_candidates)} 只候选进入宏观适配精筛。")

        logger.info("[Step 2] 宏观环境分析...")
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

        logger.info("[Step 4] 进入深度个案分析阶段...")
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

        logger.info(f"[Paper Trading] 尝试将推荐股票加入观察仓: {selected_codes}")
        stock_profiles_map = {item["asset_info"]["code"]: item for item in detailed_reports}
        for code in selected_codes:
            if code in stock_profiles_map:
                profile = stock_profiles_map[code]
                name = profile["asset_info"]["name"]
                
                reason_parts = []
                if "fundamental_analysis" in profile:
                    reason_parts.append("【基本面分析】\n" + str(profile["fundamental_analysis"]))
                if "technical_analysis" in profile:
                    reason_parts.append("【技术面分析】\n" + str(profile["technical_analysis"]))
                if "news_risk_analysis" in profile:
                    reason_parts.append("【风控与资讯】\n" + str(profile["news_risk_analysis"]))
                
                reason = "\n\n".join(reason_parts)
                self.paper_trading.add_trade(code, name, reason)
            else:
                logger.warning(f"模型选取了不在原始池中的代码 {code}，已跳过建仓。")

        elapsed = time.time() - start_time
        self._save_workflow_audit(
            prefilter_candidates=prefilter_candidates,
            candidates=candidates,
            macro_context=macro_context,
            quick_filter_result=quick_filter_result,
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

        fund_report = self.fundamental_agent.analyze(code, name, macro_context=macro_context)
        logger.info(f"  -> Fundamental done: {name}({code}), chars={len(str(fund_report or ''))}")
        tech_report = self.technical_agent.analyze(code, name, macro_context=macro_context)
        logger.info(f"  -> Technical done: {name}({code}), chars={len(str(tech_report or ''))}")
        news_risk_report = self.news_risk_agent.analyze(code, name, macro_context=macro_context)
        logger.info(
            f"  -> NewsRisk done: {name}({code}), "
            f"level={news_risk_report.get('risk_level') if isinstance(news_risk_report, dict) else 'unknown'}, "
            f"action={news_risk_report.get('action') if isinstance(news_risk_report, dict) else 'unknown'}, "
            f"hard_exclude={news_risk_report.get('hard_exclude') if isinstance(news_risk_report, dict) else 'unknown'}"
        )

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

    def _get_candidate_analysis_max_workers(self) -> int:
        workflow_cfg = self.config.get("agent_workflow", {}) if isinstance(self.config, dict) else {}
        value = workflow_cfg.get("candidate_analysis_max_workers", DEFAULT_CANDIDATE_ANALYSIS_MAX_WORKERS)
        try:
            workers = int(value)
        except (TypeError, ValueError):
            workers = DEFAULT_CANDIDATE_ANALYSIS_MAX_WORKERS
        return max(1, min(workers, 5))

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

    def run_post_market_routine(self):
        """
        每日盘后例行维护：
        1. 结算虚拟观察仓当天浮盈浮亏。
        2. 输出持仓动作诊断。
        3. 触发 AI 自我反思机制，吸取失败教训。
        """
        logger.info("=== 开始盘后虚拟观察仓结算与反思 ===")
        self.paper_trading.update_portfolio()
        self.reflection_agent.generate_reflection_for_failures(threshold_pct=-3.0)

        macro_context = self.macro_agent.analyze_macro_environment()
        diagnostics = self.paper_trading.diagnose_portfolio(macro_context=macro_context)
        review_report = self.paper_trading.format_diagnostics_report(diagnostics)
        print(review_report)
        print(self.paper_trading.show_portfolio())

        logger.info("=== 盘后例行任务结束 ===")
        return review_report


if __name__ == "__main__":
    coordinator = AgentCoordinator()
    # print(coordinator.run_picking_workflow())
    # coordinator.run_post_market_routine()
