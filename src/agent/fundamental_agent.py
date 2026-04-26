"""Fundamental analysis agent.

The agent combines structured financial statement metrics, recent
announcements, and macro context before asking the LLM for a concise
investment-quality assessment.
"""

import json
from typing import Any, Dict, Optional

from loguru import logger

from src.agent.tools import tools
from src.financial_data import FinancialDataProvider


class FundamentalAgent:
    def __init__(self, financial_provider: FinancialDataProvider | None = None):
        self.financial_provider = financial_provider or FinancialDataProvider()

    def analyze(self, code: str, name: str, macro_context: Optional[Dict[str, Any]] = None) -> str:
        logger.info("[Fundamental Agent] analyzing {}({})", name, code)

        macro_text = json.dumps(macro_context or {}, ensure_ascii=False, indent=2)
        announcement_info = tools.fetch_stock_announcements(code, limit=10)
        financial_info = self.financial_provider.format_financial_summary(code, periods=8)
        announcement_detail_info = tools.fetch_stock_announcement_details(
            code,
            limit=10,
            max_announcements=3,
            financial_reports_covered=bool(financial_info),
        )
        announcement_prompt_info = announcement_info
        if announcement_detail_info:
            announcement_prompt_info = (
                f"{announcement_info}\n\n"
                f"重点公告正文摘要：\n{announcement_detail_info}"
            )

        system_prompt = """
你是一名A股基本面分析师。请基于结构化财务数据、近期公告和宏观环境，判断公司质量、成长性、估值支撑和主要风险。

分析要求：
1. 优先使用财务报表趋势，而不是只复述新闻。
2. 重点检查近几期营收、归母净利、扣非净利、毛利率、ROE、ROIC、经营现金流/利润和资产负债率。
3. 如果利润增长和现金流背离，或扣非净利明显弱于归母净利，要明确提示财务质量风险。
4. 结合宏观 risk_appetite、favorable_sectors、avoid_sectors 判断该股当前是否适合进入候选。
5. 输出 200-350 字，结构包含：基本面结论、财务趋势、质量风险、宏观适配、最终评级。
6. 最终评级只能是：强 / 中性 / 弱，并给出一句理由。
"""

        user_prompt = f"""
股票：{name}({code})

宏观上下文：
{macro_text}

结构化财务数据：
{financial_info or "未获取到东方财富财务报表数据，请降低确定性并明确数据缺口。"}

近期公告：
{announcement_prompt_info or "东方财富未获取到该股近期公告。"}
"""

        report = tools.call_llm(system_prompt, user_prompt, temperature=0.2)
        if not report:
            return "未能生成基本面分析。"
        return report
