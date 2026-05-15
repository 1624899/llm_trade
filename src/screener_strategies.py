"""策略检测器 Mixin。

从 stock_screener.py 拆分：7 个策略检测器 + 归一化辅助函数。
"""

from typing import Any, Dict, List

import numpy as np
import pandas as pd


class StrategyDetectorMixin:
    """_detect_strategy_matches 及其依赖的归一化函数。"""

    def _detect_strategy_matches(
        self,
        *,
        profile: Dict[str, Any],
        close: float,
        ma5: float,
        ma10: float,
        ma20: float,
        ma60: float,
        ret3: float,
        ret5: float,
        ret10: float,
        ret20: float,
        change_pct: float,
        high60_distance: float,
        low60_distance: float,
        vol_ratio: float,
        vol5_ratio: float,
        turnover_rate: float | None,
        pe_ttm: float | None,
        pb: float | None,
        latest_open: float,
        intraday_position: float,
        ret1: float = 0.0,
        bias20_ratio: float | None = None,
        rsi14: float | None = None,
        macd_hist_rising: bool = False,
        macd_bullish_divergence: bool = False,
        bullish_pullback_candle: bool = False,
        recent_limit_up_count: int = 0,
        first_limit_up_breakout: bool = False,
        potential_dragon_pullback: bool = False,
        is_new_stock_channel: bool = False,
    ) -> List[Dict[str, Any]]:
        """检测股票是否符合预设的多样化交易策略。"""
        matches: List[Dict[str, Any]] = []
        turnover = turnover_rate or 0.0
        volume_ratio = float(vol_ratio) if pd.notna(vol_ratio) else 1.0
        volume5_ratio = float(vol5_ratio) if pd.notna(vol5_ratio) else 1.0
        bias20 = float(bias20_ratio) if bias20_ratio is not None and pd.notna(bias20_ratio) else 1.0
        reversal_indicators_available = rsi14 is not None
        reversal_confirmed = (
            not reversal_indicators_available
            or (rsi14 is not None and rsi14 <= 20)
            or macd_hist_rising
            or macd_bullish_divergence
        )
        tuned_trend_bias20 = min(
            float(profile.get("max_trend_bias20", 1.22)),
            float(profile.get("bt_max_trend_bias20", profile.get("max_trend_bias20", 1.22))),
        )
        tuned_momentum_bias20 = min(
            float(profile.get("max_momentum_bias20", 1.30)),
            float(profile.get("bt_max_momentum_bias20", profile.get("max_momentum_bias20", 1.30))),
        )
        tuned_first_limit_bias20 = min(
            float(profile.get("max_momentum_bias20", 1.30)),
            float(profile.get("bt_first_limit_max_bias20", profile.get("max_momentum_bias20", 1.30))),
        )

        # 1. 经典趋势突破策略 (Trend Breakout)
        if (
            close >= ma20
            and ma20 >= ma60 * float(profile.get("bt_trend_ma20_ma60_min", 0.98))
            and ma20 <= ma60 * float(profile.get("bt_trend_ma20_ma60_max", 99.0))
            and float(profile["ret20_min"]) <= ret20 <= float(profile["ret20_max"])
            and ret20 >= float(profile.get("bt_trend_ret20_min", profile["ret20_min"]))
            and ret5 >= float(profile["ret5_min"])
            and ret5 >= float(profile.get("bt_trend_ret5_min", profile["ret5_min"]))
            and ret5 <= float(profile.get("bt_trend_ret5_max", 12.0))
            and change_pct < float(profile["limit_up_change_pct"])
            and bias20 <= tuned_trend_bias20
            and volume_ratio >= float(profile.get("bt_trend_min_volume_ratio", 0.0))
            and volume_ratio <= float(profile.get("bt_trend_max_volume_ratio", profile.get("max_volume_ratio", 99.0)))
            and intraday_position >= float(profile.get("bt_trend_min_intraday_position", 0.0))
            and (high60_distance < -3 or volume_ratio >= float(profile["min_volume_ratio_near_high60"]))
        ):
            confidence = 0.62
            confidence += 0.10 if close >= ma5 >= ma10 >= ma20 else 0.0
            confidence += 0.08 if 5 <= ret20 <= 25 else 0.0
            confidence += 0.06 if 1.1 <= volume_ratio <= 3.0 else 0.0
            matches.append(
                {
                    "tag": "trend_breakout",
                    "label": "经典趋势突破",
                    "confidence": min(0.95, confidence),
                }
            )

        # 2. 强势主升浪策略 (Momentum Leader)
        high_volume_stall = (
            volume_ratio >= 2.5
            and (close <= latest_open or intraday_position < 0.35)
            and ret5 < 12
        )
        if (
            close > ma5 > ma10 > ma20 > ma60
            and abs(close / ma5 - 1) <= 0.06
            and bias20 <= tuned_momentum_bias20
            and ret20 >= float(profile.get("bt_momentum_ret20_min", 24.0))
            and ret10 >= float(profile.get("bt_momentum_ret10_min", 10.0))
            and ret5 >= float(profile.get("bt_momentum_ret5_min", 4.0))
            and ret5 <= float(profile.get("bt_momentum_ret5_max", 12.0))
            and change_pct < float(profile["limit_up_change_pct"])
            and volume_ratio >= 1.1
            and volume_ratio <= float(profile.get("bt_momentum_max_volume_ratio", 2.8))
            and volume5_ratio >= 1.05
            and intraday_position >= float(profile.get("bt_momentum_min_intraday_position", 0.52))
            and (turnover >= 5.0 or volume_ratio >= 1.6)
            and not high_volume_stall
        ):
            confidence = 0.66
            confidence += 0.10 if ret20 >= 35 else 0.0
            confidence += 0.08 if turnover >= 8.0 else 0.0
            confidence += 0.06 if 1.2 <= volume_ratio <= 3.5 else 0.0
            matches.append(
                {
                    "tag": "momentum_leader",
                    "label": "强势主升浪",
                    "confidence": min(0.96, confidence),
                }
            )

        # 3. 优质股底部低吸策略 (Value Bottom)
        if (
            pe_ttm is not None
            and pb is not None
            and 0 < pe_ttm <= 15
            and 0 < pb <= 1.5
            and low60_distance <= 12
            and ret3 > -5
            and (volume_ratio >= 1.5 or turnover >= max(1.5, float(profile["turnover_rate_min"])))
            and reversal_confirmed
        ):
            confidence = 0.58
            confidence += 0.10 if close < ma20 else 0.04
            confidence += 0.08 if low60_distance <= 8 else 0.0
            confidence += 0.07 if volume_ratio >= 1.8 else 0.0
            matches.append(
                {
                    "tag": "value_bottom",
                    "label": "优质股底部低吸",
                    "confidence": min(0.92, confidence),
                }
            )

        # 4. 错杀/洗盘反转策略 (Panic Reversal)
        if (
            (ret5 <= -8 or ret10 <= -12)
            and pe_ttm is not None
            and 0 < pe_ttm <= 20
            and (volume_ratio >= 1.8 or turnover >= 3.0)
            and intraday_position >= 0.45
            and reversal_confirmed
        ):
            confidence = 0.60
            confidence += 0.10 if intraday_position >= 0.6 else 0.0
            confidence += 0.08 if volume_ratio >= 2.2 else 0.0
            confidence += 0.05 if pb is not None and 0 < pb <= 2.0 else 0.0
            matches.append(
                {
                    "tag": "panic_reversal",
                    "label": "财报错杀/洗盘反转",
                    "confidence": min(0.94, confidence),
                }
            )

        # 5. 缩量回踩支撑策略 (Support Pullback)
        if (
            ma20 >= ma60 * 0.98
            and ret10 > -8
            and (abs(close / ma20 - 1) <= 0.03 or abs(close / ma60 - 1) <= 0.04)
            and volume_ratio <= float(profile.get("bt_support_max_volume_ratio", 0.78))
            and ret1 <= float(profile.get("bt_support_ret1_max", 3.0))
            and intraday_position >= float(profile.get("bt_support_min_intraday_position", 0.52))
            and change_pct > float(profile["limit_down_change_pct"]) / 2
            and bullish_pullback_candle
        ):
            matches.append(
                {
                    "tag": "support_pullback",
                    "label": "缩量回踩支撑",
                    "confidence": 0.68,
                }
            )

        # 6. Dragon Pullback
        if (
            potential_dragon_pullback
            and recent_limit_up_count >= 2
            and ret1 <= -5
            and volume_ratio >= 0.7
            and volume5_ratio >= 0.9
            and intraday_position >= float(profile.get("bt_dragon_min_intraday_position", 0.38))
        ):
            confidence = 0.64
            confidence += 0.08 if abs(close / ma5 - 1) <= 0.025 else 0.0
            confidence += 0.06 if intraday_position >= 0.35 else 0.0
            matches.append(
                {
                    "tag": "dragon_pullback",
                    "label": "Dragon Pullback",
                    "confidence": min(0.90, confidence),
                }
            )

        # 7. First Limit-up Breakout
        if (
            first_limit_up_breakout
            and close >= ma20
            and ret5 <= float(profile.get("bt_first_limit_ret5_max", 18.0))
            and ret20 <= min(float(profile["ret20_max"]), float(profile.get("bt_first_limit_ret20_max", 28.0)))
            and bias20 <= tuned_first_limit_bias20
            and volume_ratio >= 1.2
            and volume_ratio <= float(profile.get("bt_first_limit_max_volume_ratio", 4.5))
            and ma20 <= ma60 * float(profile.get("bt_first_limit_ma20_ma60_max", 99.0))
        ):
            confidence = 0.63
            confidence += 0.08 if ret20 <= 20 else 0.0
            confidence += 0.06 if ma20 >= ma60 * 0.98 else 0.0
            matches.append(
                {
                    "tag": "first_limit_up_breakout",
                    "label": "First Limit-up Breakout",
                    "confidence": min(0.91, confidence),
                }
            )

        if is_new_stock_channel and matches:
            for match in matches:
                if match["tag"] in {"momentum_leader", "first_limit_up_breakout", "dragon_pullback"}:
                    match["confidence"] = min(0.96, match["confidence"] + 0.04)

        return sorted(matches, key=lambda item: item["confidence"], reverse=True)

    @staticmethod
    def _normalize_amount_series_to_yuan(bars: pd.DataFrame) -> pd.Series:
        """把不同数据源的成交额统一到元口径。"""
        bar_amount = pd.to_numeric(bars.get("bar_amount"), errors="coerce")
        quote_amount = pd.to_numeric(bars.get("quote_amount"), errors="coerce")
        amount = bar_amount.fillna(quote_amount)

        closes = pd.to_numeric(bars.get("close"), errors="coerce")
        volumes = pd.to_numeric(bars.get("volume"), errors="coerce")
        hand_based_amount = closes * volumes * 100

        valid = amount.notna() & (amount > 0) & hand_based_amount.notna() & (hand_based_amount > 0)
        if valid.any():
            median_ratio = (hand_based_amount[valid] / amount[valid]).replace([np.inf, -np.inf], np.nan).dropna().median()
            if pd.notna(median_ratio) and 100 <= float(median_ratio) <= 2_000:
                amount = amount * 1_000

        return amount.fillna(closes * volumes)

    @staticmethod
    def _normalize_turnover_rate(value: Any) -> float | None:
        if value in (None, "") or pd.isna(value):
            return None
        turnover = float(value)
        if 0 < turnover <= 1.0:
            return turnover * 100.0
        return turnover
