"""
市场环境（Regime）检测器，用于自动选择选股配置（Profile）。

检测器是确定性的且优先使用本地数据。它从 market_bars 表中读取宽基指数数据，
只有在指数历史数据足够时才返回高置信度的市场环境判断。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from loguru import logger

from src.database import StockDatabase


# 默认的市场环境配置
DEFAULT_MARKET_REGIME_CONFIG: Dict[str, Any] = {
    "enabled": False,                # 是否启用自动切换
    "allow_empty_on_extreme_risk": True,  # 极端风险下是否允许空仓（不选股）
    "min_history_days": 60,          # 计算信号所需的最小历史天数
    "lookback_days": 120,            # 从数据库读取的回溯天数
    "indices": {                     # 监控的指数列表
        "sh000001": "上证指数",
        "sh000300": "沪深300",
        "sz399006": "创业板指",
    },
    "profile_map": {                 # 市场环境到选股配置的映射
        "defensive": "conservative",   # 防御状态 -> 保守配置
        "range_bound": "balanced",     # 震荡状态 -> 平衡配置
        "offensive": "aggressive",     # 进攻状态 -> 激进配置
        "extreme_risk": "conservative", # 极端风险 -> 保守配置
        "unknown": "balanced",         # 未知状态 -> 平衡配置
    },
}


@dataclass
class IndexSignal:
    """单个指数的分析信号"""
    code: str
    name: str
    ret5: float          # 5日收益率
    ret20: float         # 20日收益率
    ret60: float         # 60日收益率
    annual_vol20: float  # 20日年化波动率
    close: float         # 当前收盘价
    ma20: float          # 20日均线
    ma60: float          # 60日均线
    drawdown60: float    # 60日最大回撤
    trend_score: float   # 综合趋势评分


class MarketRegimeDetector:
    """对大盘状态进行分类，并将其映射到相应的选股筛选配置。"""

    def __init__(self, db: StockDatabase | None = None, config: Dict[str, Any] | None = None):
        self.db = db or StockDatabase()
        self.config = self._merge_config(config or {})

    def detect(self) -> Dict[str, Any]:
        """
        检测当前市场环境状态。
        
        返回包含 regime, profile, allow_empty, reason 和指标数据的字典。
        """
        if not bool(self.config.get("enabled", True)):
            return self._unknown("市场环境自动切换已禁用")

        # 加载指数数据
        bars = self._load_index_bars()
        if bars.empty:
            return self._unknown("本地数据库无指数数据")

        # 构建分析信号
        signals = self._build_signals(bars)
        if len(signals) < 2:
            return self._unknown("可用指数历史数据不足", signals)

        # 计算各指数信号的平均值
        avg_ret5 = float(np.mean([signal.ret5 for signal in signals]))
        avg_ret20 = float(np.mean([signal.ret20 for signal in signals]))
        avg_ret60 = float(np.mean([signal.ret60 for signal in signals]))
        avg_vol20 = float(np.mean([signal.annual_vol20 for signal in signals]))
        avg_drawdown60 = float(np.mean([signal.drawdown60 for signal in signals]))
        
        # 计算市场宽度指标
        above_ma60_ratio = sum(signal.close >= signal.ma60 for signal in signals) / len(signals)
        weak_count = sum(signal.ret20 <= -6 or signal.drawdown60 <= -10 for signal in signals)
        strong_count = sum(signal.ret20 >= 4 and signal.close >= signal.ma20 >= signal.ma60 for signal in signals)

        # 核心分类逻辑
        if weak_count >= 2 and (avg_ret20 <= -7 or avg_drawdown60 <= -12 or avg_vol20 >= 0.35):
            # 极端风险：多个指数剧烈回撤或高波动
            regime = "extreme_risk"
            reason = "多个指数显示剧烈回撤或异常高波动"
        elif avg_ret20 <= -3 or above_ma60_ratio <= 0.34 or weak_count >= 2:
            # 防御：指数普遍走弱或处于中期趋势线下方
            regime = "defensive"
            reason = "宽基指数走势疲软或处于中期趋势线下"
        elif avg_ret20 >= 3 and avg_ret60 >= 2 and above_ma60_ratio >= 0.67 and strong_count >= 1:
            # 进攻：指数呈现明显的正向趋势且得到中期确认
            regime = "offensive"
            reason = "宽基指数呈现正向趋势且中期趋势确认"
        else:
            # 震荡：指数表现不一或处于横盘区间
            regime = "range_bound"
            reason = "宽基指数表现分化或处于震荡区间"

        # 映射到筛选 Profile
        profile = self._profile_for_regime(regime)
        # 判断是否允许空仓（仅在极端风险下根据配置决定）
        allow_empty = bool(self.config.get("allow_empty_on_extreme_risk", True)) and regime == "extreme_risk"
        
        result = {
            "regime": regime,
            "profile": profile,
            "allow_empty": allow_empty,
            "reason": reason,
            "metrics": {
                "avg_ret5": round(avg_ret5, 2),
                "avg_ret20": round(avg_ret20, 2),
                "avg_ret60": round(avg_ret60, 2),
                "avg_annual_vol20": round(avg_vol20, 4),
                "avg_drawdown60": round(avg_drawdown60, 2),
                "above_ma60_ratio": round(above_ma60_ratio, 2),
                "weak_index_count": weak_count,
                "strong_index_count": strong_count,
            },
            "signals": [signal.__dict__ for signal in signals],
        }
        logger.info(f"[MarketRegime] regime={regime}, profile={profile}, reason={reason}")
        return result

    def _load_index_bars(self) -> pd.DataFrame:
        """从数据库加载指数的日线数据。"""
        indices = self.config.get("indices") or {}
        codes = [str(code).strip() for code in indices.keys() if str(code).strip()]
        if not codes:
            return pd.DataFrame()

        placeholders = ", ".join(["?"] * len(codes))
        lookback_days = max(int(self.config.get("lookback_days", 120) or 120), 60)
        
        # 获取最近的回溯日期
        latest_dates = self.db.query_to_dataframe(
            """
                SELECT DISTINCT trade_date
                FROM market_bars
                WHERE period = 'daily'
                  AND code IN ({})
                ORDER BY trade_date DESC
                LIMIT ?
            """.format(placeholders),
            tuple(codes + [lookback_days]),
        )
        if latest_dates is None or latest_dates.empty:
            return pd.DataFrame()

        min_trade_date = str(latest_dates["trade_date"].min())
        
        # 拉取日期范围内的所有 K 线
        return self.db.query_to_dataframe(
            """
                SELECT code, trade_date, open, high, low, close, volume, amount
                FROM market_bars
                WHERE period = 'daily'
                  AND code IN ({})
                  AND trade_date >= ?
                ORDER BY code, trade_date
            """.format(placeholders),
            tuple(codes + [min_trade_date]),
        )

    def _build_signals(self, bars: pd.DataFrame) -> List[IndexSignal]:
        """根据 K 线数据计算各维度的技术信号指标。"""
        min_history_days = max(int(self.config.get("min_history_days", 60) or 60), 30)
        index_names = self.config.get("indices") or {}
        signals: List[IndexSignal] = []
        
        for code, group in bars.groupby("code", sort=False):
            group = group.sort_values("trade_date").copy()
            group["close"] = pd.to_numeric(group["close"], errors="coerce")
            closes = group["close"].dropna()
            
            if len(closes) < min_history_days:
                continue

            close = float(closes.iloc[-1])
            ma20 = float(closes.tail(20).mean())
            ma60 = float(closes.tail(60).mean())
            ret5 = self._return_pct(closes, 5)
            ret20 = self._return_pct(closes, 20)
            ret60 = self._return_pct(closes, 60)
            
            # 计算波动率
            annual_vol20 = float(closes.pct_change().tail(20).std() * np.sqrt(252))
            
            # 计算近期最大值及相对于它的回撤
            high60 = float(closes.tail(60).max())
            drawdown60 = (close / high60 - 1) * 100 if high60 else 0.0
            
            # 趋势评分公式（启发式）
            trend_score = ret20 + ret60 * 0.5 + (5 if close >= ma20 >= ma60 else 0) + drawdown60 * 0.3
            
            signals.append(
                IndexSignal(
                    code=str(code),
                    name=str(index_names.get(code, code)),
                    ret5=round(float(ret5), 2),
                    ret20=round(float(ret20), 2),
                    ret60=round(float(ret60), 2),
                    annual_vol20=round(annual_vol20, 4) if not np.isnan(annual_vol20) else 0.0,
                    close=round(close, 4),
                    ma20=round(ma20, 4),
                    ma60=round(ma60, 4),
                    drawdown60=round(float(drawdown60), 2),
                    trend_score=round(float(trend_score), 2),
                )
            )
        return signals

    def _profile_for_regime(self, regime: str) -> str:
        """根据配置映射表获取对应的筛选 Profile 名称。"""
        profile_map = self.config.get("profile_map") or {}
        profile = str(profile_map.get(regime) or "balanced").lower()
        if profile not in {"conservative", "balanced", "aggressive"}:
            return "balanced"
        return profile

    def _unknown(self, reason: str, signals: List[IndexSignal] | None = None) -> Dict[str, Any]:
        """返回状态未知时的结果。"""
        return {
            "regime": "unknown",
            "profile": self._profile_for_regime("unknown"),
            "allow_empty": False,
            "reason": reason,
            "metrics": {},
            "signals": [signal.__dict__ for signal in signals or []],
        }

    def _merge_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """合并传入配置与默认配置。"""
        merged = dict(DEFAULT_MARKET_REGIME_CONFIG)
        for key, value in config.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                nested = dict(merged[key])
                nested.update(value)
                merged[key] = nested
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _return_pct(closes: pd.Series, days: int) -> float:
        """计算指定天数的收益率（百分比）。"""
        if len(closes) <= days or not closes.iloc[-days - 1]:
            return 0.0
        return (float(closes.iloc[-1]) / float(closes.iloc[-days - 1]) - 1) * 100
