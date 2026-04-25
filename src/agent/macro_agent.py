"""
宏观与策略分析智能体 (Macro Agent)

职责：
1. 在技术预筛之后分析当前宏观经济、政策环境、流动性和 A 股市场情绪。
2. 为后续基本面、技术面和决策 Agent 提供稳定的结构化市场背景。
"""

import json
from datetime import datetime
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
    "market_sentiment": "中性",
    "risk_appetite": "中性",
    "liquidity_view": "成交与流动性按常规条件评估",
    "favorable_sectors": [],
    "avoid_sectors": [],
    "key_risks": ["避免追高", "关注近期跌停、停牌和流动性异常"],
    "analysis_focus": "优先复核候选股的趋势持续性、成交额、行业位置和基本面风险。",
}


class MacroAgent:
    def analyze_macro_environment(self) -> Dict[str, Any]:
        """分析当前宏观与市场环境，输出固定 schema 的结构化上下文。"""
        logger.info("[Macro Agent] 正在分析宏观环境与 A 股市场情绪...")

        fallback_context = self._fallback_context()
        market_sentiment = tools.fetch_market_sentiment()
        today = datetime.now().strftime("%Y-%m-%d")
        web_context = tools.web_search(
            (
                f"{today} A股 收评 盘后复盘 涨停复盘 主线板块 行业板块 "
                "主力资金流向 政策情绪 东方财富 新浪 财联社 证券时报"
            ),
            purpose="MacroAgent 获取当天 A 股盘后复盘、市场主线、资金流向和政策情绪变化",
            max_results=6,
            domains=[
                "finance.sina.com.cn",
                "eastmoney.com",
                "cls.cn",
                "stcn.com",
                "cnstock.com",
                "fupanwang.com",
            ],
        )
        if not str(web_context or "").strip():
            logger.warning("[Macro Agent] 首轮宏观网页搜索为空，切换到宽查询重试。")
            web_context = tools.web_search(
                (
                    f"{today} A股 收评 盘后复盘 主线板块 涨停复盘 "
                    "市场情绪 资金流向 政策消息"
                ),
                purpose="MacroAgent 宏观搜索首轮为空后的宽查询补充",
                max_results=8,
            )

        system_prompt = """
你是一名 A 股宏观策略分析师。技术预筛已经在上游完成，你不需要生成 SQL，也不需要筛选股票。

请基于当前宏观环境、A 股市场情绪、政策/流动性背景，以及可用的网页搜索摘要，输出用于后续个股分析的结构化 JSON。

输出必须是合法 JSON，且必须包含以下字段：
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

        user_prompt = f"""
本地市场情绪快照：
{market_sentiment}

网页搜索摘要：
{web_context or "未启用或未获取到网页搜索结果，请仅基于本地市场情绪快照分析。"}
"""

        res_text = tools.call_llm(system_prompt, user_prompt, temperature=0.1)
        if not res_text:
            logger.warning("[Macro Agent] 宏观分析失败，使用默认宏观上下文。")
            return fallback_context

        try:
            res_text = res_text.replace("```json", "").replace("```", "").strip()
            parsed_dict = json.loads(res_text)
            normalized = self._normalize_macro_context(parsed_dict, fallback_context)
            logger.info(
                f"[Macro Agent] 宏观分析完成: 市场情绪为 {normalized.get('market_sentiment', '未知')}"
            )
            return normalized
        except json.JSONDecodeError as exc:
            logger.error(f"[Macro Agent] 宏观分析 JSON 解析失败: {exc}\n原文: {res_text}")
            return fallback_context

    def analyze_macro_and_parse_query(self, user_query: str = "") -> Dict[str, Any]:
        """向后兼容旧调用；新流程不再解析用户选股意图。"""
        return self.analyze_macro_environment()

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
            items = [item.strip() for item in value.replace(";", ",").split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value]
        else:
            items = [str(value).strip()]
        return [item for item in items if item]
