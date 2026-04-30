"""宏观与市场环境分析智能体 (Macro Agent).

职责：
1. 在技术预筛之后，分析当前 A 股市场情绪、政策/流动性背景和盘面主线。
2. 为后续基本面、技术面、资讯风控和决策 Agent 提供稳定的结构化市场上下文。
"""

from __future__ import annotations

import json
import re
from datetime import datetime, time
from typing import Any, Dict, Optional

from loguru import logger

from src.agent.tools import tools


MACRO_CONTEXT_SCHEMA_FIELDS = (
    "market_sentiment",
    "risk_appetite",
    "liquidity_view",
    "favorable_sectors",
    "avoid_sectors",
    "key_risks",
    "analysis_focus",
)

MACRO_CONTEXT_LIST_FIELDS = {"favorable_sectors", "avoid_sectors", "key_risks"}

DEFAULT_MACRO_CONTEXT: Dict[str, Any] = {
    "market_sentiment": "中性震荡",
    "risk_appetite": "中性偏谨慎",
    "liquidity_view": "成交与流动性按常规条件评估，需结合最新盘面确认",
    "favorable_sectors": [],
    "avoid_sectors": [],
    "key_risks": ["避免追高", "关注跌停、停牌、流动性异常和数据时效风险"],
    "analysis_focus": "优先复核候选股的趋势持续性、成交额、行业位置、资金承接和基本面风险。",
}


class MacroAgent:
    def analyze_macro_environment(self) -> Dict[str, Any]:
        """分析当前宏观与市场环境，输出固定 schema 的结构化上下文。"""
        now = datetime.now()
        trade_phase = self._trade_phase(now)
        logger.info(
            "[Macro Agent] 正在分析宏观环境与 A 股市场情绪，phase={}, as_of={}",
            trade_phase,
            now.strftime("%Y-%m-%d %H:%M:%S"),
        )

        fallback_context = self._fallback_context()
        market_sentiment = tools.fetch_market_sentiment()
        web_context = self._fetch_web_context(now, trade_phase)

        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(
            now=now,
            trade_phase=trade_phase,
            market_sentiment=market_sentiment,
            web_context=web_context,
        )

        res_text = tools.call_llm(system_prompt, user_prompt, temperature=0.05)
        if not res_text:
            logger.warning("[Macro Agent] 宏观分析失败，使用默认宏观上下文。")
            return fallback_context

        parsed_dict = self._parse_json_response(res_text)
        if not parsed_dict:
            logger.error("[Macro Agent] 宏观分析 JSON 解析失败，原文: {}", res_text)
            return fallback_context

        normalized = self._normalize_macro_context(parsed_dict, fallback_context)
        normalized = self._apply_market_snapshot_guardrails(normalized, market_sentiment, trade_phase)
        logger.info(
            "[Macro Agent] 宏观分析完成: 情绪={}, 风险偏好={}, 有利方向={}",
            normalized.get("market_sentiment", "未知"),
            normalized.get("risk_appetite", "未知"),
            normalized.get("favorable_sectors", []),
        )
        return normalized

    def analyze_macro_and_parse_query(self, user_query: str = "") -> Dict[str, Any]:
        """向后兼容旧调用；新流程不再解析用户选股意图。"""
        return self.analyze_macro_environment()

    def _fetch_web_context(self, now: datetime, trade_phase: str) -> str:
        today = now.strftime("%Y-%m-%d")
        stamp = now.strftime("%H:%M")
        if trade_phase in {"pre_market", "opening_auction"}:
            queries = [
                f"{today} A股 盘前策略 早盘 政策消息 流动性 题材主线 {stamp}",
                f"{today} 财联社 证券时报 A股 早盘 盘前 资金面 政策情绪 {stamp}",
            ]
            purpose = "MacroAgent 获取当天 A 股盘前策略、政策消息、流动性和潜在主线"
        elif trade_phase in {"intraday", "lunch_break"}:
            queries = [
                f"{today} A股 盘中 午评 异动 板块 涨停 资金流向 主线 {stamp}",
                f"{today} A股 实时行情 板块涨幅 主力资金 行业概念 午后 策略 {stamp}",
            ]
            purpose = "MacroAgent 获取当天 A 股盘中/午评主线、板块异动和资金流向"
        else:
            queries = [
                f"{today} A股 收评 盘后复盘 涨停复盘 主线板块 行业板块 主力资金流向 {stamp}",
                f"{today} A股 盘后 复盘 财联社 东方财富 证券时报 政策情绪 资金流向 {stamp}",
            ]
            purpose = "MacroAgent 获取当天 A 股盘后复盘、市场主线、资金流向和政策情绪变化"

        domains = [
            "finance.sina.com.cn",
            "eastmoney.com",
            "cls.cn",
            "stcn.com",
            "cnstock.com",
            "fupanwang.com",
        ]
        sections = []
        for index, query in enumerate(queries, start=1):
            text = tools.web_search(
                query,
                purpose=f"{purpose} #{index}",
                max_results=4,
                domains=domains,
            )
            if text and str(text).strip():
                sections.append(f"查询{index}: {query}\n{text.strip()}")

        if sections:
            return "\n\n".join(sections)

        logger.warning("[Macro Agent] 宏观网页搜索为空，切换到宽查询重试。")
        return tools.web_search(
            f"{today} A股 市场情绪 板块 主线 资金流向 政策 消息 {stamp}",
            purpose="MacroAgent 宏观搜索为空后的宽查询补充",
            max_results=6,
        )

    def _build_system_prompt(self) -> str:
        return """
你是一名 A 股宏观策略分析师。技术预筛已经在上游完成，你不需要生成 SQL，也不需要筛选股票。

请基于本地市场情绪快照、当前交易时段、政策/流动性背景，以及可用网页搜索摘要，输出用于后续个股分析的结构化 JSON。

重要规则：
1. 本地市场情绪快照优先级最高，尤其是涨跌家数、涨跌停、涨跌幅中位数和热门行业/概念。
2. 网页搜索摘要可能滞后、日期不匹配或混入旧文章；如果摘要日期不是当前日期，或内容与本地快照明显冲突，必须降低其权重。
3. 盘中运行时不要假装已有完整收评；只能描述“盘中暂态”，并提示后续需用成交量、午后资金和收盘确认。
4. 有利方向必须来自本地热门行业/概念、当天搜索摘要或明确政策线索；不要凭空加入泛 AI、新能源、地产等主题。
5. 如果市场宽度偏弱、跌停多于涨停或中位数为负，结论应偏谨慎，不要因为旧网页里的强势描述而给出偏强判断。
6. 输出必须是合法 JSON，字段值使用中文，列表字段必须是数组。

输出 JSON schema：
{
  "market_sentiment": "当前市场情绪判断，如偏强、震荡、中性、防守、风险偏高",
  "risk_appetite": "风险偏好判断",
  "liquidity_view": "流动性和成交活跃度判断",
  "favorable_sectors": ["当前更值得关注的行业或主题，可为空"],
  "avoid_sectors": ["当前需要谨慎的行业或主题，可为空"],
  "key_risks": ["后续分析个股时需要重点检查的风险点"],
  "analysis_focus": "给 FundamentalAgent、TechnicalAgent 和 DecisionAgent 的分析重点"
}

请严格只输出 JSON，不要包含 markdown 代码块或额外解释。
"""

    def _build_user_prompt(
        self,
        *,
        now: datetime,
        trade_phase: str,
        market_sentiment: str,
        web_context: str,
    ) -> str:
        phase_note = {
            "pre_market": "盘前，尚无当天完整成交确认。",
            "opening_auction": "集合竞价/开盘附近，波动噪音较高。",
            "intraday": "盘中交易，结论需要午后和收盘确认。",
            "lunch_break": "午间休市，可参考上午盘面但仍需下午确认。",
            "post_close": "收盘后，可使用当天收评和复盘信息。",
            "off_hours": "非交易时段，注意网页和本地行情日期是否匹配。",
        }.get(trade_phase, "未知交易时段，注意数据时效。")

        return f"""
运行时间：{now.strftime("%Y-%m-%d %H:%M:%S")}
交易时段：{trade_phase}（{phase_note}）

本地市场情绪快照：
{market_sentiment}

网页搜索摘要：
{web_context or "未启用或未获取到网页搜索结果。请仅基于本地市场情绪快照分析，并在风险中说明外部资讯缺失。"}
"""

    def _parse_json_response(self, text: str) -> Dict[str, Any]:
        if not text:
            return {}
        cleaned = text.replace("```json", "").replace("```", "").strip()
        try:
            parsed = json.loads(cleaned)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", cleaned, flags=re.S)
            if not match:
                return {}
            try:
                parsed = json.loads(match.group(0))
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}

    def _apply_market_snapshot_guardrails(
        self,
        context: Dict[str, Any],
        market_sentiment: str,
        trade_phase: str,
    ) -> Dict[str, Any]:
        text = market_sentiment or ""
        weak_snapshot = (
            "weak" in text.lower()
            or "risk-off" in text.lower()
            or self._extract_score(text) <= 45
            or self._extract_limit_balance(text) < 0
        )
        if weak_snapshot:
            sentiment = context.get("market_sentiment", "")
            if any(word in sentiment for word in ("偏强", "强势", "积极", "risk-on")):
                context["market_sentiment"] = "偏弱震荡"
            appetite = context.get("risk_appetite", "")
            if any(word in appetite for word in ("高", "进攻", "积极")):
                context["risk_appetite"] = "谨慎偏低"
            context["favorable_sectors"] = self._constrain_weak_market_sectors(
                context.get("favorable_sectors"),
                market_sentiment,
            )
            context["analysis_focus"] = self._build_weak_market_focus(context.get("favorable_sectors"))

        if trade_phase in {"intraday", "lunch_break", "opening_auction"}:
            risks = list(context.get("key_risks") or [])
            intraday_risk = "盘中结论尚未经过收盘确认，需复核午后资金承接和尾盘回落风险"
            if intraday_risk not in risks:
                risks.append(intraday_risk)
            context["key_risks"] = risks
        elif trade_phase == "post_close":
            liquidity = str(context.get("liquidity_view") or "")
            if "午后" in liquidity or "收盘确认" in liquidity:
                context["liquidity_view"] = "成交活跃度偏低，市场宽度较弱，资金偏防守，需等待后续交易日验证修复力度"
        if weak_snapshot:
            risks = list(context.get("key_risks") or [])
            weak_hot_risk = "弱市热门板块仅代表相对活跃，不等同于可追高，需要结合个股位置、成交额和封板质量确认"
            if weak_hot_risk not in risks:
                risks.append(weak_hot_risk)
            context["key_risks"] = risks
        return context

    def _constrain_weak_market_sectors(self, sectors: Any, market_sentiment: str) -> list:
        evidence = self._extract_snapshot_hot_items(market_sentiment)
        raw_sectors = self._normalize_list_field(sectors, [])
        if evidence:
            raw_sectors = [item for item in raw_sectors if item in evidence]

        noisy_or_too_broad = {
            "开发区",
            "次新股",
            "房地产",
            "传统消费",
            "高估值题材股",
            "前期涨幅过高的题材股",
        }
        preferred_keywords = (
            "华为",
            "海思",
            "油气",
            "卫星",
            "导航",
            "海工",
            "船舶",
            "飞机",
            "专精特新",
            "电子",
            "机械",
            "地热",
            "钙钛矿",
        )
        filtered = []
        for item in raw_sectors:
            if item in noisy_or_too_broad:
                continue
            if preferred_keywords and not any(keyword in item for keyword in preferred_keywords):
                continue
            if item not in filtered:
                filtered.append(item)

        if filtered:
            return filtered[:5]

        fallback = [
            item
            for item in evidence
            if item not in noisy_or_too_broad and any(keyword in item for keyword in preferred_keywords)
        ]
        return fallback[:5]

    def _extract_snapshot_hot_items(self, market_sentiment: str) -> list:
        items = []
        for label in ("热门行业", "热门概念"):
            match = re.search(rf"{label}:\s*([^\n\r]+)", market_sentiment or "")
            if not match:
                continue
            for item in re.split(r"[,，、]\s*", match.group(1).strip()):
                item = item.strip()
                if item and item not in items:
                    items.append(item)
        return items

    def _build_weak_market_focus(self, favorable_sectors: Any) -> str:
        sectors = self._normalize_list_field(favorable_sectors, [])
        sector_text = "、".join(sectors) if sectors else "相对强势且有资金承接的少数方向"
        return (
            f"弱市环境下以后续风控和胜率优先，不把热门板块直接等同于买入信号。"
            f"后续个股分析可重点观察{sector_text}中的龙头或低位承接品种，"
            "但必须同时确认业绩/公告无硬伤、成交额足够、技术形态未破位；"
            "对高位题材、缩量反抽、放量滞涨和跌停扩散行业降低评分，仓位预期保持克制。"
        )

    def _extract_score(self, text: str) -> float:
        match = re.search(r"\((\d+(?:\.\d+)?)\s*/\s*100\)", text or "")
        if not match:
            return 50.0
        try:
            return float(match.group(1))
        except ValueError:
            return 50.0

    def _extract_limit_balance(self, text: str) -> int:
        match = re.search(r"涨停\(估\):\s*(\d+).*?跌停\(估\):\s*(\d+)", text or "")
        if not match:
            return 0
        return int(match.group(1)) - int(match.group(2))

    def _trade_phase(self, now: datetime) -> str:
        current = now.time()
        if current < time(9, 15):
            return "pre_market"
        if time(9, 15) <= current < time(9, 30):
            return "opening_auction"
        if time(9, 30) <= current <= time(11, 30):
            return "intraday"
        if time(11, 30) < current < time(13, 0):
            return "lunch_break"
        if time(13, 0) <= current <= time(15, 0):
            return "intraday"
        if time(15, 0) < current <= time(18, 30):
            return "post_close"
        return "off_hours"

    def _fallback_context(self) -> Dict[str, Any]:
        return self._normalize_macro_context(DEFAULT_MACRO_CONTEXT)

    def _normalize_macro_context(
        self,
        raw_context: Optional[Dict[str, Any]],
        fallback_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        fallback = fallback_context or DEFAULT_MACRO_CONTEXT
        raw = raw_context if isinstance(raw_context, dict) else {}
        normalized: Dict[str, Any] = {}

        for field in MACRO_CONTEXT_SCHEMA_FIELDS:
            default_value = fallback.get(field, DEFAULT_MACRO_CONTEXT[field])
            value = raw.get(field, default_value)
            if field in MACRO_CONTEXT_LIST_FIELDS:
                normalized[field] = self._normalize_list_field(value, default_value)
            else:
                normalized[field] = self._normalize_text_field(value, default_value)

        return normalized

    def _normalize_text_field(self, value: Any, default_value: Any) -> str:
        if value is None:
            value = default_value
        if isinstance(value, (list, tuple, set)):
            value = " ".join(str(item).strip() for item in value if str(item).strip())
        text = str(value).strip()
        if not text:
            text = str(default_value or "").strip()
        return text

    def _normalize_list_field(self, value: Any, default_value: Any) -> list:
        if value is None or value == "":
            value = default_value
        if isinstance(value, str):
            items = [item.strip() for item in value.replace(";", ",").replace("；", ",").split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value]
        else:
            items = [str(value).strip()]
        return [item for item in items if item]
