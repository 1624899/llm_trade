"""评分与辅助计算 Mixin。

从 stock_screener.py 拆分：RSI、MACD、K 线形态判定、综合评分、筛选理由格式化。
"""

from typing import Any, Dict, List

import numpy as np
import pandas as pd
from loguru import logger

from src.screener_profiles import BACKTEST_STRATEGY_PRIORS, STRATEGY_TRADE_PROFILES


class ScoringMixin:
    """技术指标计算、策略评分、筛选理由格式化。"""

    @staticmethod
    def _calculate_rsi(closes: pd.Series, period: int = 14) -> float | None:
        if len(closes) <= period:
            return None
        delta = closes.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        latest_loss = loss.iloc[-1]
        latest_gain = gain.iloc[-1]
        if pd.isna(latest_gain) or pd.isna(latest_loss):
            return None
        if latest_loss == 0:
            return 100.0
        rs = latest_gain / latest_loss
        return float(100 - (100 / (1 + rs)))

    @staticmethod
    def _calculate_macd_histogram(closes: pd.Series) -> pd.Series:
        ema12 = closes.ewm(span=12, adjust=False).mean()
        ema26 = closes.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        return (dif - dea) * 2

    @staticmethod
    def _has_macd_bullish_divergence(closes: pd.Series, macd_hist: pd.Series) -> bool:
        if len(closes) < 12 or len(macd_hist) < 12:
            return False
        recent = closes.tail(6)
        prior = closes.iloc[:-6].tail(6)
        recent_hist = macd_hist.tail(6)
        prior_hist = macd_hist.iloc[:-6].tail(6)
        if recent.empty or prior.empty or recent_hist.empty or prior_hist.empty:
            return False
        return bool(
            recent.min() < prior.min()
            and recent_hist.min() > prior_hist.min()
        )

    @staticmethod
    def _is_bullish_pullback_candle(
        *,
        latest_open: float,
        latest_high: float,
        latest_low: float,
        close: float,
        intraday_position: float,
    ) -> bool:
        candle_range = latest_high - latest_low
        if candle_range <= 0:
            return close >= latest_open
        lower_shadow = min(latest_open, close) - latest_low
        real_body = abs(close - latest_open)
        hammer_like = lower_shadow >= max(real_body * 1.5, candle_range * 0.25)
        doji_like = real_body <= candle_range * 0.2 and intraday_position >= 0.45
        return bool(close >= latest_open or intraday_position >= 0.5 or hammer_like or doji_like)

    def _score_strategy_candidate(
        self,
        *,
        strategy_matches: List[Dict[str, Any]],
        ret1: float,
        ret5: float,
        ret10: float,
        ret20: float,
        change_pct: float,
        high60_distance: float,
        low60_distance: float,
        avg_amount20: float,
        vol_ratio: float,
        turnover_rate: float | None,
        annual_vol20: float | None,
        recent_limit_up_count: int,
        intraday_position: float,
        bias20_ratio: float | None,
        pe_ttm: float | None,
        pb: float | None,
        close: float,
        ma5: float,
        ma10: float,
        ma20: float,
        ma60: float,
    ) -> float:
        """根据策略匹配度和技术指标对候选股票进行综合打分。"""
        primary = strategy_matches[0]["tag"]
        score = max(match["confidence"] for match in strategy_matches) * 40

        liquidity_bonus = min(12, max(0, np.log10(avg_amount20 / 50_000_000 + 1) * 8))
        volume_confirmation_bonus = min(5, max(0, (float(vol_ratio) - 1) * 2.5)) if pd.notna(vol_ratio) else 0
        turnover_bonus = min(4, max(0, (float(turnover_rate or 0) - 1.0) * 0.8))
        score += liquidity_bonus + volume_confirmation_bonus + turnover_bonus
        score += 3 * max(0, len(strategy_matches) - 1)

        if primary == "trend_breakout":
            score += max(-10, min(18, ret20)) * 0.7
            score += max(-5, min(10, ret5)) * 0.6
            score += 6 if close >= ma5 >= ma10 >= ma20 else 0
            score += 5 if ma20 >= ma60 else 0
            score += max(-10, min(8, high60_distance + 8))
        elif primary == "momentum_leader":
            score += min(24, max(0, ret20)) * 0.65
            score += min(8, max(0, ret5)) * 0.45
            score += 6 if close > ma5 > ma10 > ma20 > ma60 else 0
            score += max(0, 4 - abs(close / ma5 - 1) * 100)
        elif primary == "value_bottom":
            score += max(0, 12 - low60_distance) * 1.4
            score += 8 if close < ma20 else 4
            score += 8 if pe_ttm is not None and 0 < pe_ttm <= 12 else 4
            score += 6 if pb is not None and 0 < pb <= 1.2 else 2
        elif primary == "panic_reversal":
            score += min(18, abs(min(ret5, ret20 / 2))) * 0.9
            score += 8 if pe_ttm is not None and 0 < pe_ttm <= 15 else 4
            score += 5 if pb is not None and 0 < pb <= 2 else 0
        elif primary == "dragon_pullback":
            score += max(0, 8 - abs(close / ma5 - 1) * 100)
            score += min(12, abs(min(ret5, 0))) * 0.55
            score += 8 if close >= ma10 else 3
        elif primary == "first_limit_up_breakout":
            score += min(20, max(0, ret20)) * 0.9
            score += 10 if close >= ma5 >= ma10 >= ma20 else 4
            score += 8 if ma20 >= ma60 * 0.98 else 0
            score += 6 if 1.2 <= float(vol_ratio or 0) <= 2.8 else 0
        else:
            score += 10 if ma20 >= ma60 else 0
            score += max(0, 8 - abs(close / ma20 - 1) * 100)

        risk_penalty = 0.0
        vol = float(annual_vol20) if annual_vol20 is not None and pd.notna(annual_vol20) else 0.0
        if vol > 0.45:
            risk_penalty += min(12.0, (vol - 0.45) * 28.0)

        bias20 = float(bias20_ratio) if bias20_ratio is not None and pd.notna(bias20_ratio) else 1.0
        if bias20 > 1.20:
            risk_penalty += min(12.0, (bias20 - 1.20) * 55.0)

        if ret5 > 18:
            risk_penalty += min(10.0, (ret5 - 18) * 0.8)
        if ret10 > 32:
            risk_penalty += min(10.0, (ret10 - 32) * 0.5)
        if ret20 > 45:
            risk_penalty += min(14.0, (ret20 - 45) * 0.45)
        if recent_limit_up_count > 0:
            risk_penalty += min(10.0, recent_limit_up_count * 3.0)

        high_volume_stall = (
            pd.notna(vol_ratio)
            and float(vol_ratio) >= 2.5
            and (change_pct <= 0 or intraday_position < 0.45)
        )
        if high_volume_stall:
            risk_penalty += 10.0

        turnover = float(turnover_rate or 0.0)
        if turnover > 35.0:
            risk_penalty += min(12.0, (turnover - 35.0) * 0.35)

        if primary in {"momentum_leader", "first_limit_up_breakout"}:
            risk_penalty += 4.0
            if primary == "momentum_leader":
                risk_penalty += 4.0
                if intraday_position < 0.65:
                    risk_penalty += 3.0
            if ret5 > 12:
                risk_penalty += min(10.0, (ret5 - 12) * 0.9)
            if ret20 > 35:
                risk_penalty += min(14.0, (ret20 - 35) * 0.7)
            if pd.notna(vol_ratio) and float(vol_ratio) > 2.8:
                risk_penalty += min(8.0, (float(vol_ratio) - 2.8) * 4.0)
            if ret1 > 7:
                risk_penalty += min(6.0, (ret1 - 7) * 1.0)
        elif primary == "support_pullback":
            risk_penalty += 3.0
            if intraday_position < 0.6:
                risk_penalty += 3.0
        elif primary == "dragon_pullback":
            risk_penalty += 2.0

        score += float(BACKTEST_STRATEGY_PRIORS.get(primary, {}).get("score_bonus", 0.0))
        return max(0.0, score - risk_penalty)

    def _format_screen_reason(
        self,
        strategy_matches: List[Dict[str, Any]],
        ret5: float,
        ret20: float,
        pe_ttm: float | None,
        pb: float | None,
        vol_ratio: float,
        turnover_rate: float | None,
        low60_distance: float,
        trade_profile: Dict[str, Any] | None = None,
    ) -> str:
        """格式化筛选理由，用于展示命中策略和关键指标。"""
        primary = strategy_matches[0]
        tags = "、".join(match["label"] for match in strategy_matches)
        pe_text = "N/A" if pe_ttm is None else f"{pe_ttm:.1f}"
        pb_text = "N/A" if pb is None else f"{pb:.2f}"
        vol_text = "N/A" if pd.isna(vol_ratio) else f"{float(vol_ratio):.2f}"
        turnover_text = "N/A" if turnover_rate is None else f"{turnover_rate:.2f}%"
        profile = trade_profile or self._strategy_trade_profile(strategy_matches)
        return (
            f"【{primary['label']}】命中策略: {tags}; "
            f"ret5 {ret5:.2f}%, ret20 {ret20:.2f}%, PE {pe_text}, PB {pb_text}, "
            f"量比 {vol_text}, 换手 {turnover_text}, 距60日低点 {low60_distance:.2f}%；"
            f"建议持有 {profile['preferred_holding_days']} 日，{profile['exit_hint']}"
        )

    def _strategy_trade_profile(self, strategy_matches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """根据主策略返回建议持有周期和退出提示。"""
        primary = strategy_matches[0]["tag"] if strategy_matches else "unknown"
        default = {
            "preferred_holding_days": 10,
            "max_holding_days": 10,
            "exit_hint": "按10日内验证处理，走势不及预期时降低仓位。",
        }
        profile = dict(default)
        profile.update(STRATEGY_TRADE_PROFILES.get(str(primary), {}))
        return profile
