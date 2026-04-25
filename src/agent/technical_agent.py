"""技术面分析 Agent。

先组合确定性的 OHLCV 技术信号和多周期 K 线摘要，再交给 LLM 生成简洁交易计划。
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from loguru import logger

from src.agent.tools import tools
from src.technical_indicators import TechnicalSignalProvider


class TechnicalAgent:
    def __init__(self, signal_provider: TechnicalSignalProvider | None = None):
        self.signal_provider = signal_provider or TechnicalSignalProvider()

    def analyze(self, code: str, name: str, macro_context: Optional[Dict[str, Any]] = None) -> str:
        logger.info("[Technical Agent] analyzing {}({})", name, code)

        kline_data = tools.fetch_multi_period_kline(code)
        signal_summary = self.signal_provider.format_summary(code, lookback=120)
        macro_text = json.dumps(macro_context or {}, ensure_ascii=False, indent=2)

        system_prompt = """
你是 A 股技术分析员。你必须优先使用“量化技术信号摘要”里的确定性指标，再结合多周期 K 线文本和宏观环境判断。

分析要求：
1. 先判断当前形态：趋势突破、强动量延续、缩量回踩、支撑低吸、放量滞涨、平台跌破或中性震荡。
2. 买点必须具体：给出回踩区、突破确认价、是否适合追高。
3. 风险必须具体：给出初始止损位，并说明这个止损来自 ATR、平台支撑还是形态失效。
4. 如果出现“放量滞涨/上影派发”“跌破20日平台”等风险标签，必须降低评分，避免把高位接盘解释成强势。
5. 结合宏观环境：风险偏好低时，减少追高建议；风险偏好高且行业风口明确时，才允许顺势突破交易。

输出 200-350 字，结构固定：
- 技术结论：
- 买点/触发：
- 止损/失效：
- 量能与风险：
- 操作建议：
"""

        user_prompt = f"""
股票：{name}({code})

宏观环境：
{macro_text}

量化技术信号摘要：
{signal_summary}

多周期 K 线摘要：
{kline_data}
"""

        report = tools.call_llm(system_prompt, user_prompt, temperature=0.1)
        if not report:
            return "技术分析暂不可用：LLM 未返回有效结果。"
        return report
