"""报告格式化 Mixin。

从 coordinator.py 拆分：所有报告格式化（指定分析报告、资讯风控段、总评表、审计保存）。
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger


class CoordinatorReportMixin:
    """报告格式化与审计保存。"""

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

    def _append_analysis_error_summary(self, report: str, errors: List[Dict[str, str]]) -> str:
        lines = ["", "## 深度分析异常摘要"]
        for item in errors:
            lines.append(f"- {item.get('name')}({item.get('code')}): {item.get('error')}")
        return report.rstrip() + "\n" + "\n".join(lines)
