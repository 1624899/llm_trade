"""持仓退出评估 Agent。

该模块不依赖 LLM，先用规则把“是否该卖”这件事闭环起来。它复用技术信号层，
结合收益率、技术破位、放量滞涨和宏观风险偏好，输出清晰的持仓动作。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from loguru import logger

from src.database import StockDatabase
from src.technical_indicators import TechnicalSignalProvider


ACTION_HOLD = "继续持有"
ACTION_WAIT = "等待确认"
ACTION_REDUCE = "减仓观察"
ACTION_CLEAR = "清仓退出"

ACTION_LEVELS = {
    ACTION_HOLD: 0,
    ACTION_WAIT: 1,
    ACTION_REDUCE: 2,
    ACTION_CLEAR: 3,
}


@dataclass
class ExitDecision:
    code: str
    action: str
    action_level: int
    reason: str
    stop_price: float | None = None
    technical_tags: list[str] | None = None
    risk_flags: list[str] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "action": self.action,
            "action_level": self.action_level,
            "reason": self.reason,
            "stop_price": self.stop_price,
            "technical_tags": self.technical_tags or [],
            "risk_flags": self.risk_flags or [],
        }


class ExitAgent:
    """根据持仓状态、技术信号和宏观环境生成退出动作。"""

    def __init__(
        self,
        db: Optional[StockDatabase] = None,
        signal_provider: Optional[TechnicalSignalProvider] = None,
    ):
        self.db = db or StockDatabase()
        self.signal_provider = signal_provider or TechnicalSignalProvider(db=self.db)

    def evaluate_position(
        self,
        position: Dict[str, Any],
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> ExitDecision:
        code = str(position.get("code", "")).zfill(6)
        current_price = self._to_float(position.get("current_price"))
        recommend_price = self._to_float(position.get("recommend_price"))
        return_pct = self._resolve_return_pct(position, current_price, recommend_price)

        try:
            report = self.signal_provider.build_report(code, lookback=120)
        except Exception as exc:
            logger.warning("[ExitAgent] 技术信号计算失败 {}: {}", code, exc)
            return self._decision(code, ACTION_HOLD, "技术信号暂不可用，沿用收益率规则。")

        metrics = report.metrics or {}
        if not metrics.get("available"):
            return self._decision(code, ACTION_HOLD, "K线样本不足，暂不触发技术退出。")

        levels = report.levels or {}
        risk_flags = report.risk_flags or []
        tags = report.tags or []
        latest_close = self._to_float(metrics.get("close")) or current_price
        price = current_price or latest_close

        initial_stop = self._to_float(levels.get("atr_stop_1_5x")) or self._to_float(levels.get("atr_stop_1x"))
        breakdown_trigger = self._to_float(levels.get("breakdown_trigger"))
        ma20 = self._to_float(metrics.get("ma20"))

        if price and initial_stop and price <= initial_stop:
            return self._decision(
                code,
                ACTION_CLEAR,
                f"跌破 ATR 动态止损位 {initial_stop}，趋势保护失效。",
                stop_price=initial_stop,
                tags=tags,
                risk_flags=risk_flags,
            )

        if price and breakdown_trigger and price <= breakdown_trigger:
            return self._decision(
                code,
                ACTION_CLEAR,
                f"跌破20日平台硬退出价 {breakdown_trigger}，优先控制回撤。",
                stop_price=breakdown_trigger,
                tags=tags,
                risk_flags=risk_flags,
            )

        if "跌破20日平台" in risk_flags:
            return self._decision(
                code,
                ACTION_CLEAR,
                "技术标签显示已跌破20日平台，持仓逻辑失效。",
                stop_price=breakdown_trigger,
                tags=tags,
                risk_flags=risk_flags,
            )

        if return_pct is not None and return_pct >= 15 and ma20 and price and price < ma20:
            return self._decision(
                code,
                ACTION_REDUCE,
                f"已有 {return_pct}% 浮盈但跌回 MA20 下方，触发移动止盈保护。",
                stop_price=ma20,
                tags=tags,
                risk_flags=risk_flags,
            )

        if "放量滞涨/上影派发" in risk_flags:
            return self._decision(
                code,
                ACTION_REDUCE,
                "出现放量滞涨/上影派发，疑似高位兑现，先降低仓位观察。",
                tags=tags,
                risk_flags=risk_flags,
            )

        if risk_flags:
            return self._decision(
                code,
                ACTION_WAIT,
                f"出现风险标签：{'、'.join(risk_flags)}，下一交易日确认是否修复。",
                tags=tags,
                risk_flags=risk_flags,
            )

        return self._decision(
            code,
            ACTION_HOLD,
            f"未触发技术退出，形态标签：{'、'.join(tags) if tags else '无明显异常'}。",
            stop_price=initial_stop,
            tags=tags,
            risk_flags=risk_flags,
        )

    def _decision(
        self,
        code: str,
        action: str,
        reason: str,
        *,
        stop_price: float | None = None,
        tags: list[str] | None = None,
        risk_flags: list[str] | None = None,
    ) -> ExitDecision:
        return ExitDecision(
            code=code,
            action=action,
            action_level=ACTION_LEVELS.get(action, 0),
            reason=reason,
            stop_price=stop_price,
            technical_tags=tags or [],
            risk_flags=risk_flags or [],
        )

    @staticmethod
    def _resolve_return_pct(
        position: Dict[str, Any],
        current_price: float | None,
        recommend_price: float | None,
    ) -> float | None:
        explicit = ExitAgent._to_float(position.get("return_pct"))
        if explicit is not None:
            return explicit
        if current_price is None or recommend_price in (None, 0):
            return None
        return round((current_price / recommend_price - 1) * 100, 2)

    @staticmethod
    def _macro_is_defensive(macro_context: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(macro_context, dict):
            return False
        values = [
            str(macro_context.get("risk_appetite", "")).lower(),
            str(macro_context.get("market_sentiment", "")).lower(),
            str(macro_context.get("regime", "")).lower(),
        ]
        defensive_words = ("low", "defensive", "risk_off", "extreme_risk", "conservative", "防守", "低")
        return any(any(word in value for word in defensive_words) for value in values)

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
