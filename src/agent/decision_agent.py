"""
决策智能体 (Decision Agent)

职责：
1. 综合个股的深度分析报告（基本面、技术面、资讯风控）。
2. 应用确定性的资讯风控硬门槛（News-Risk Gate）。
3. 调用 LLM 进行最终排序和决策建议。
4. 如果 LLM 不可用，自动降级为基于预筛分数的规则排序，确保系统在离线或 API 故障时仍能输出结果。
"""

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from src.agent.tools import tools


class DecisionAgent:
    """最终决策智能体，负责汇聚分析结果并产出投资报告。"""

    def synthesize_and_elect_winners(
        self,
        detailed_reports: List[Dict[str, Any]],
        user_query: str = "请对筛选出的 A 股候选标的进行排序，并选出最优目标。",
        pick_n: int = 3,
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, List[str]]:
        """
        综合多维报告并选出最终胜出者。
        返回：Markdown 格式的决策报告，以及选中的股票代码列表。
        """
        logger.info(
            f"[Decision Agent] 正在综合 {len(detailed_reports)} 只候选股，目标 Top {pick_n}。"
        )

        # 1. 资讯硬风控过滤：在进入 LLM 之前，先根据 NewsRiskAgent 的结果剔除高危股
        eligible_reports, blocked_reports = self._split_news_risk_candidates(detailed_reports)
        if blocked_reports:
            logger.warning(
                f"[Decision Agent] 资讯硬风控剔除以下候选: "
                f"{[item['asset_info'].get('code') for item in blocked_reports]}"
            )
        
        # 如果所有候选股都被剔除了
        if not eligible_reports:
            return self._build_all_blocked_report(blocked_reports), []

        macro_context = macro_context or {}
        decision_date = datetime.now().strftime("%Y-%m-%d")
        macro_text = self._format_macro_context_for_prompt(macro_context)
        context_str = self._format_candidate_context(eligible_reports)
        if blocked_reports:
            context_str += self._format_blocked_context(blocked_reports)

        # 加载历史反思记忆，避免重复犯错
        reflection_memory = self._load_reflection_memory()
        
        system_prompt = f"""
你是 A 股最终决策决策智能体。

请结合宏观环境、技术预筛分、基本面分析、技术面分析、资讯风险检查以及历史反思规则，从候选池中选出最多 {pick_n} 只股票。
报告中的决策日期必须使用：{decision_date}。不要自行改写为其他年份或交易日。
直接输出报告正文，不要输出“好的、遵命、作为某某智能体”等寒暄句。
必须尊重资讯风险的硬性剔除结论。

用户目标：{user_query}

历史反思记忆：
{reflection_memory or "暂无"}

请输出一份详细且专业的中文 Markdown 决策报告。
必须使用统一推荐分层，不能把所有入选标的都笼统写成“推荐”：
- 强推荐：基本面强、技术形态明确、资讯风控低，适合作为本轮核心候选。
- 配置/轻仓验证：防御属性、回踩支撑、低风险配置或赔率一般但胜率较稳，只能轻仓或等触发条件。
- 观察：单独看有亮点，但买点、趋势、成长性或风控仍不够清晰。
- 不推荐：基本面、技术面或资讯风控存在明显短板。
如果某只股票只是弱市防御、缩量回踩、基本面稳但成长一般，请标为“配置/轻仓验证”，不要写成“强推荐”。
最终决策表必须包含“推荐分层”列，并在个股标题中使用对应分层，例如“长江电力(600900) —— 配置/轻仓验证”。
每只股票的资讯风控不能只写“风险等级：低/中/高”，必须至少说明：风险等级、处理动作、是否硬排除、新闻质量或关键证据；如果没有命中负面，也要写明“未发现明确负面关键词/公告硬风险”等依据。

报告结尾必须包含一个机器可解析的代码块，格式如下：
[CODE_LIST] 000001, 600000 [/CODE_LIST]
CODE_LIST 只放“强推荐”和“配置/轻仓验证”的代码，不放“观察”和“不推荐”的代码。
如果没有强推荐或配置/轻仓验证标的，请输出：
[CODE_LIST] [/CODE_LIST]
"""
        user_prompt = f"""
宏观上下文：
{macro_text}

个股深度分析报告：
{context_str}
"""

        # 调用 LLM 进行最终综合决策
        report = tools.call_llm(system_prompt, user_prompt, temperature=0.35)
        
        # 异常处理：如果 LLM 失败，使用预定义的规则逻辑降级输出，保证流程不中断
        if not report:
            logger.error("[Decision Agent] LLM 决策失败，尝试降级为规则排序方案。")
            return self._build_rule_based_fallback_report(eligible_reports, pick_n, macro_context)

        # 确保报告中包含固定决策日期和宏观适配度说明小节
        report = self._enforce_decision_date(report, decision_date)
        report = self._ensure_macro_adaptation_section(report, macro_context, eligible_reports)
        report = self._normalize_code_list_tags(report)
        selected_codes = self._extract_selected_codes(report, eligible_reports)
        logger.info(f"[Decision Agent] selected_codes={selected_codes}")
        return report, selected_codes

    def _format_candidate_context(self, reports: List[Dict[str, Any]]) -> str:
        """将个股分析报告格式化为 Prompt 友好的文本块。"""
        chunks = []
        for idx, profile in enumerate(reports, start=1):
            asset = profile.get("asset_info", {})
            market_cap = asset.get("total_market_cap")
            market_cap_text = "未知"
            if market_cap:
                try:
                    market_cap_text = f"{round(float(market_cap) / 1e8, 2)} 亿元"
                except (TypeError, ValueError):
                    market_cap_text = str(market_cap)

            chunks.append(
                "\n".join(
                    [
                        f"--- 候选股 {idx}: {asset.get('name')} ({asset.get('code')}) ---",
                        f"- 行业: {asset.get('industry', '未知')}",
                        f"- 价格: {asset.get('price', 0)}, 涨跌幅: {asset.get('change_pct', 0)}%",
                        f"- 估值/市值: PE_TTM {asset.get('pe_ttm', 'N/A')}, 总市值 {market_cap_text}",
                        (
                            f"- 技术预筛分: {asset.get('technical_score', 'N/A')}, "
                            f"入选理由: {asset.get('screen_reason', 'N/A')}"
                        ),
                        f"- 基本面分析: {profile.get('fundamental_analysis', '无')}",
                        f"- 技术面分析: {profile.get('technical_analysis', '无')}",
                        f"- 资讯风险: {self._format_news_risk_for_prompt(profile.get('news_risk_analysis'))}",
                    ]
                )
            )
        return "\n\n".join(chunks)

    def _format_blocked_context(self, reports: List[Dict[str, Any]]) -> str:
        """格式化被风控拦截的股票信息，告知 LLM 这些标的已被禁止。"""
        lines = ["", "--- 以下标的因资讯风险硬风控已被拦截，禁止推荐 ---"]
        for profile in reports:
            asset = profile.get("asset_info", {})
            lines.append(
                f"- {asset.get('name')}({asset.get('code')}): "
                f"{self._format_news_risk_for_prompt(profile.get('news_risk_analysis'))}"
            )
        return "\n".join(lines)

    def _load_reflection_memory(self) -> str:
        """加载本地反思规则手册。"""
        rules_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "data",
            "rules_book.txt",
        )
        if not os.path.exists(rules_path):
            return ""
        try:
            with open(rules_path, "r", encoding="utf-8") as file:
                return file.read().strip()
        except Exception as exc:
            logger.warning(f"[Decision Agent] 读取反思规则失败: {exc}")
            return ""

    def _extract_selected_codes(self, report: str, eligible_reports: List[Dict[str, Any]]) -> List[str]:
        """从 LLM 输出的 Markdown 报告中提取 [CODE_LIST] 标签内的代码。"""
        allowed_codes = {str(item.get("asset_info", {}).get("code", "")) for item in eligible_reports}
        match = re.search(
            r"(?:\[|【)\s*CODE_LIST\s*(?:\]|】)(.*?)(?:\[|【)\s*[/／]\s*CODE_LIST\s*(?:\]|】)",
            report or "",
            re.IGNORECASE | re.DOTALL,
        )

        if match:
            raw_codes = re.split(r"[,，、\s]+", match.group(1).strip())
        else:
            logger.warning("[Decision Agent] CODE_LIST tag missing or malformed; falling back to allowed code scan.")
            raw_codes = re.findall(r"(?<!\d)(\d{6})(?!\d)", report or "")
        
        selected = []
        for code in raw_codes:
            code = code.strip()
            # 只有在有效候选池中的代码才会被接受，防止 LLM 幻觉产生不存在的代码
            if code and code in allowed_codes and code not in selected:
                selected.append(code)
        return selected

    def _normalize_code_list_tags(self, report: str) -> str:
        """Normalize model-written full-width CODE_LIST tags to machine-readable ASCII tags."""
        if not report:
            return report
        report = re.sub(r"【\s*CODE_LIST\s*】", "[CODE_LIST]", report, flags=re.IGNORECASE)
        report = re.sub(r"【\s*[/／]\s*CODE_LIST\s*】", "[/CODE_LIST]", report, flags=re.IGNORECASE)
        return report

    def _enforce_decision_date(self, report: str, decision_date: str) -> str:
        """把最终报告中的决策日期固定为本次运行日期，防止模型自行漂移年份。"""
        if not report:
            return report

        date_line = f"**决策日期**: {decision_date}"
        pattern = r"(?im)^\s*\*{0,2}决策日期\*{0,2}\s*[:：]\s*.*$"
        if re.search(pattern, report):
            return re.sub(pattern, date_line, report, count=1)

        lines = report.splitlines()
        if lines and lines[0].lstrip().startswith("#"):
            return "\n".join([lines[0], date_line, *lines[1:]])
        return f"{date_line}\n\n{report}"

    def _split_news_risk_candidates(
        self,
        detailed_reports: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """根据 NewsRiskAgent 的 hard_exclude 结果，将候选股分为“合规”和“拦截”两部分。"""
        eligible = []
        blocked = []
        for profile in detailed_reports:
            risk = profile.get("news_risk_analysis")
            if isinstance(risk, dict) and bool(risk.get("hard_exclude")):
                blocked.append(profile)
            else:
                eligible.append(profile)
        return eligible, blocked

    def _format_news_risk_for_prompt(self, risk: Any) -> str:
        """统一资讯风险报告的显示格式。"""
        if isinstance(risk, dict):
            parts = [
                f"风险等级={risk.get('risk_level', 'unknown')}",
                f"处理动作={risk.get('action', 'unknown')}",
                f"硬排除={risk.get('hard_exclude', False)}",
            ]
            summary = str(risk.get("summary") or "").strip()
            if summary:
                parts.append(f"规则摘要={summary}")

            news_quality = risk.get("news_quality") or {}
            if isinstance(news_quality, dict) and news_quality:
                parts.append(
                    "新闻质量={quality}, raw={raw}, direct={direct}, mention={mention}".format(
                        quality=news_quality.get("quality", "unknown"),
                        raw=news_quality.get("raw_count", 0),
                        direct=news_quality.get("direct_count", 0),
                        mention=news_quality.get("mention_count", 0),
                    )
                )

            evidence = risk.get("evidence") or []
            if evidence:
                parts.append("命中证据=" + "；".join(str(item) for item in evidence[:3]))

            llm_report = str(risk.get("llm_report") or "").strip()
            if llm_report:
                compact_report = re.sub(r"\s+", " ", llm_report)
                parts.append(f"LLM风控结论={compact_report[:500]}")

            return "；".join(parts)
        return str(risk or "暂无")

    def _format_macro_context_for_prompt(self, macro_context: Dict[str, Any]) -> str:
        """将宏观分析结果格式化为列表。"""
        fields = [
            "market_sentiment",
            "risk_appetite",
            "liquidity_view",
            "favorable_sectors",
            "avoid_sectors",
            "key_risks",
            "analysis_focus",
        ]
        lines = []
        for field in fields:
            value = macro_context.get(field, "未知")
            if isinstance(value, (list, tuple, set)):
                value = "、".join(str(item) for item in value) or "无"
            lines.append(f"- {field}: {value}")
        return "\n".join(lines)

    def _ensure_macro_adaptation_section(
        self,
        report: str,
        macro_context: Dict[str, Any],
        eligible_reports: List[Dict[str, Any]],
    ) -> str:
        """如果 LLM 忘记输出宏观相关内容，则在报告开头补充当前的宏观背景，避免重复拼接单股属性。"""
        if "宏观" in report or "Macro" in report:
            return report

        risk_appetite = macro_context.get("risk_appetite", "未知")
        liquidity_view = macro_context.get("liquidity_view", "未知")
        key_risks = macro_context.get("key_risks", [])
        if isinstance(key_risks, (list, tuple, set)):
            key_risks_text = "、".join(str(item) for item in key_risks) or "未知"
        else:
            key_risks_text = str(key_risks or "未知")

        lines = [
            "## 宏观适配度说明",
            f"- **风险偏好**: {risk_appetite}",
            f"- **流动性视角**: {liquidity_view}",
            f"- **关注风险**: {key_risks_text}",
        ]
        return "\n".join(lines) + "\n\n" + report

    def _build_rule_based_fallback_report(
        self,
        eligible_reports: List[Dict[str, Any]],
        pick_n: int,
        macro_context: Dict[str, Any],
    ) -> Tuple[str, List[str]]:
        """LLM 不可用时的规则降级决策方案。"""
        # 按技术预筛分数进行降序排列
        ranked = sorted(
            eligible_reports,
            key=lambda item: float(item.get("asset_info", {}).get("technical_score") or 0),
            reverse=True,
        )
        selected = ranked[: max(0, int(pick_n or 0))]
        selected_codes = [str(item.get("asset_info", {}).get("code", "")) for item in selected]
        selected_codes = [code for code in selected_codes if code]

        risk_appetite = macro_context.get("risk_appetite", "未知")
        decision_date = datetime.now().strftime("%Y-%m-%d")
        lines = [
            "# 最终决策报告 (规则降级方案)",
            f"**决策日期**: {decision_date}",
            "",
            "注意：由于 LLM API 暂时不可用，本报告基于技术预筛分数和资讯风控硬门槛自动生成。",
            f"当前市场风险偏好：{risk_appetite}",
            "",
            "## 入选标的分层",
        ]
        for profile in selected:
            asset = profile.get("asset_info", {})
            tier = self._infer_fallback_tier(profile)
            lines.append(
                "- {name}({code}) —— {tier}；技术得分={score}, 入选理由={reason}".format(
                    name=asset.get("name", ""),
                    code=asset.get("code", ""),
                    tier=tier,
                    score=asset.get("technical_score", "N/A"),
                    reason=asset.get("screen_reason", "N/A"),
                )
            )
        if not selected:
            lines.append("- 本轮无符合规则的推荐标的。")
        
        lines.extend(["", f"[CODE_LIST] {', '.join(selected_codes)} [/CODE_LIST]"])
        return "\n".join(lines), selected_codes

    def _infer_fallback_tier(self, profile: Dict[str, Any]) -> str:
        """LLM 不可用时，用保守规则给入选标的补充分层，避免报告口径过强。"""
        asset = profile.get("asset_info", {})
        fund_text = str(profile.get("fundamental_analysis", ""))
        tech_text = str(profile.get("technical_analysis", ""))
        risk = profile.get("news_risk_analysis") if isinstance(profile.get("news_risk_analysis"), dict) else {}
        if risk.get("hard_exclude"):
            return "不推荐"
        if risk.get("risk_level") not in (None, "", "low") and risk.get("action") == "watch":
            return "观察"
        if "强" in fund_text and not any(word in tech_text for word in ["不宜追高", "观望", "高位"]):
            return "强推荐"
        primary_tags = asset.get("strategy_tags") or []
        if any(tag in primary_tags for tag in ["support_pullback", "value_bottom"]) or any(word in tech_text for word in ["轻仓", "低吸", "支撑", "缩量"]):
            return "配置/轻仓验证"
        return "观察"

    def _build_all_blocked_report(self, blocked_reports: List[Dict[str, Any]]) -> str:
        """当所有候选股都被风控拦截时的特殊报告。"""
        lines = [
            "# 最终决策报告",
            f"**决策日期**: {datetime.now().strftime('%Y-%m-%d')}",
            "",
            "**本轮不推荐**：所有技术入选标的均触及资讯风控硬门槛（如立案调查、高比例减持等），已全部拦截。",
            "",
            "## 风控拦截详情",
        ]
        for profile in blocked_reports:
            asset = profile.get("asset_info", {})
            lines.append(
                f"- {asset.get('name')}({asset.get('code')}): "
                f"{self._format_news_risk_for_prompt(profile.get('news_risk_analysis'))}"
            )
        lines.append("")
        lines.append("[CODE_LIST] [/CODE_LIST]")
        return "\n".join(lines)
