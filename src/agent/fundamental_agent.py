"""基本面分析智能体 (Fundamental analysis agent)。

该智能体结合结构化的财务指标、近期公告和宏观背景，
调用大语言模型（LLM）对股票进行简洁的投资质量评估。
"""

import json
from typing import Any, Dict, Optional

from loguru import logger

from src.agent.tools import tools
from src.financial_data import FinancialDataProvider


class FundamentalAgent:
    """基本面分析类，负责收集财务数据和公告并生成分析报告。"""

    def __init__(self, financial_provider: FinancialDataProvider | None = None):
        """
        初始化基本面分析智能体。

        Args:
            financial_provider: 财务数据提供者实例，如果不提供则默认创建。
        """
        self.financial_provider = financial_provider or FinancialDataProvider()

    def analyze(self, code: str, name: str, macro_context: Optional[Dict[str, Any]] = None) -> str:
        """
        对指定股票进行基本面分析。

        Args:
            code: 股票代码。
            name: 股票名称。
            macro_context: 宏观背景信息（可选）。

        Returns:
            生成的分析报告。
        """
        logger.info("[基本面智能体] 正在分析 {}({})", name, code)

        # 处理宏观背景文本
        macro_text = json.dumps(macro_context or {}, ensure_ascii=False, indent=2)
        
        # 获取近期公告列表（前10条）
        announcement_info = tools.fetch_stock_announcements(code, limit=10)
        
        # 获取结构化的财务摘要（过往8个季度/年度周期）
        financial_info = self.financial_provider.format_financial_summary(code, periods=8)
        
        # 获取重要公告的详细摘要（通常覆盖财报相关的公告）
        announcement_detail_info = tools.fetch_stock_announcement_details(
            code,
            limit=10,
            max_announcements=3,
            financial_reports_covered=bool(financial_info),
        )
        
        # 组合公告信息
        announcement_prompt_info = announcement_info
        if announcement_detail_info:
            announcement_prompt_info = (
                f"{announcement_info}\n\n"
                f"重点公告正文摘要：\n{announcement_detail_info}"
            )

        # 设置系统提示词，定义分析师角色和分析标准
        system_prompt = """
你是一名A股基本面分析师。请基于结构化财务数据、近期公告和宏观环境，判断公司质量、成长性、估值支撑和主要风险。

分析要求：
1. 优先使用财务报表趋势，而不是只复述新闻。
2. 重点检查近几期营收、归母净利、扣非净利、毛利率、ROE、ROIC、经营现金流/利润和资产负债率。
3. 如果利润增长和现金流背离，或扣非净利明显弱于归母净利，要明确提示财务质量风险。
4. 结合宏观 risk_appetite、favorable_sectors、avoid_sectors 判断该股当前是否适合进入候选。
5. 用中期视角判断未来 1-3 个月或下一财报窗口的盈利预期和估值修复空间；如果预期收益空间不足 15%，不要给“强”。
6. 如果营收、利润、现金流、负债或公告风险已经恶化，要明确写“基本面恶化信号”，并说明是否应减仓/回避。
7. 输出 200-350 字，结构包含：基本面结论、财务趋势、盈利预期、质量风险、宏观适配、最终评级。
8. 最终评级只能是：强 / 中性 / 弱，并给出一句理由。
"""

        # 构造用户请求，提供具体的股票数据
        user_prompt = f"""
股票：{name}({code})

宏观上下文：
{macro_text}

结构化财务数据：
{financial_info or "未获取到东方财富财务报表数据，请降低确定性并明确数据缺口。"}

近期公告：
{announcement_prompt_info or "东方财富未获取到该股近期公告。"}
"""

        # 调用 LLM 生成报告
        report = tools.call_llm(system_prompt, user_prompt, temperature=0.2)
        if not report:
            return "未能生成基本面分析。"
        return report
