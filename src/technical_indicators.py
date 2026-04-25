"""基于本地 OHLCV K 线计算确定性的技术面信号。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from loguru import logger

from src.database import StockDatabase


@dataclass
class TechnicalSignalReport:
    code: str
    metrics: Dict[str, Any]
    levels: Dict[str, Any]
    tags: List[str]
    risk_flags: List[str]
    trade_plan: Dict[str, Any]
    generated_at: str


class TechnicalSignalProvider:
    """在 LLM 解读前，先计算可复核的技术指标和交易位置。"""

    def __init__(self, db: StockDatabase | None = None):
        self.db = db or StockDatabase()

    def build_report(self, code: str, lookback: int = 120) -> TechnicalSignalReport:
        bars = self.load_daily_bars(code, lookback=lookback)
        return self.calculate_report(code, bars)

    def format_summary(self, code: str, lookback: int = 120) -> str:
        try:
            report = self.build_report(code, lookback=lookback)
        except Exception as exc:
            logger.warning("[TechnicalSignals] failed to build report for {}: {}", code, exc)
            return "量化技术信号暂不可用，请降低技术面结论的确定性。"
        return self.format_report(report)

    def load_daily_bars(self, code: str, lookback: int = 120) -> pd.DataFrame:
        query = """
            SELECT trade_date, open, high, low, close, volume, amount
            FROM market_bars
            WHERE code = ? AND period = 'daily'
            ORDER BY trade_date DESC
            LIMIT ?
        """
        df = self.db.query_to_dataframe(query, (str(code).zfill(6), int(lookback)))
        if df is None or df.empty:
            return pd.DataFrame()
        return df.sort_values("trade_date").reset_index(drop=True)

    def calculate_report(self, code: str, bars: pd.DataFrame) -> TechnicalSignalReport:
        if bars is None or bars.empty or len(bars) < 20:
            return TechnicalSignalReport(
                code=str(code).zfill(6),
                metrics={"available": False, "reason": "daily bars less than 20"},
                levels={},
                tags=["数据不足"],
                risk_flags=["K线样本不足"],
                trade_plan={},
                generated_at=datetime.now().isoformat(timespec="seconds"),
            )

        df = bars.copy()
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            df[col] = pd.to_numeric(df.get(col), errors="coerce")
        df = df.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
        if len(df) < 20:
            return self.calculate_report(code, pd.DataFrame())

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"].fillna(0)
        latest = df.iloc[-1]
        prev_close = close.shift(1)

        true_range = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)

        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        atr14 = true_range.rolling(14).mean()
        avg_volume20 = volume.shift(1).rolling(20).mean()

        latest_close = float(latest["close"])
        latest_open = float(latest.get("open") or latest_close)
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        latest_volume = float(latest.get("volume") or 0)

        prev20 = df.iloc[:-1].tail(20)
        prev60 = df.iloc[:-1].tail(60)
        support20 = float(prev20["low"].min()) if not prev20.empty else float(low.tail(20).min())
        resistance20 = float(prev20["high"].max()) if not prev20.empty else float(high.tail(20).max())
        support60 = float(prev60["low"].min()) if not prev60.empty else support20
        resistance60 = float(prev60["high"].max()) if not prev60.empty else resistance20

        atr_value = self._safe_float(atr14.iloc[-1])
        atr_pct = atr_value / latest_close * 100 if latest_close and atr_value is not None else None
        volume_base = self._safe_float(avg_volume20.iloc[-1])
        volume_ratio = latest_volume / volume_base if volume_base and volume_base > 0 else None
        close_position = (
            (latest_close - latest_low) / (latest_high - latest_low)
            if latest_high > latest_low
            else 0.5
        )

        metrics = {
            "available": True,
            "trade_date": str(latest.get("trade_date", "")),
            "close": round(latest_close, 3),
            "ret5": self._return_pct(close, 5),
            "ret20": self._return_pct(close, 20),
            "ma5": self._round(ma5.iloc[-1]),
            "ma10": self._round(ma10.iloc[-1]),
            "ma20": self._round(ma20.iloc[-1]),
            "ma60": self._round(ma60.iloc[-1]),
            "bias20": self._pct(latest_close, ma20.iloc[-1]),
            "atr14": self._round(atr_value),
            "atr14_pct": self._round(atr_pct),
            "volume_ratio": self._round(volume_ratio),
            "volume_percentile60": self._volume_percentile(volume.tail(60), latest_volume),
            "close_position": self._round(close_position),
            "drawdown_from_20d_high": self._pct(latest_close, high.tail(20).max()),
        }

        levels = {
            "support20": round(support20, 3),
            "support60": round(support60, 3),
            "resistance20": round(resistance20, 3),
            "resistance60": round(resistance60, 3),
            "atr_stop_1x": self._round(latest_close - atr_value if atr_value is not None else None),
            "atr_stop_1_5x": self._round(latest_close - 1.5 * atr_value if atr_value is not None else None),
            "breakout_trigger": round(resistance20 * 1.01, 3) if resistance20 else None,
            "breakdown_trigger": round(support20 * 0.99, 3) if support20 else None,
        }

        tags, risk_flags = self._detect_tags(
            latest_close=latest_close,
            latest_open=latest_open,
            resistance20=resistance20,
            support20=support20,
            metrics=metrics,
        )

        trade_plan = self._build_trade_plan(latest_close, levels, tags, risk_flags)

        return TechnicalSignalReport(
            code=str(code).zfill(6),
            metrics=metrics,
            levels=levels,
            tags=tags,
            risk_flags=risk_flags,
            trade_plan=trade_plan,
            generated_at=datetime.now().isoformat(timespec="seconds"),
        )

    def format_report(self, report: TechnicalSignalReport) -> str:
        metrics = report.metrics
        if not metrics.get("available"):
            return f"量化技术信号：{metrics.get('reason', '数据不足')}"

        levels = report.levels
        plan = report.trade_plan
        tags = "、".join(report.tags) if report.tags else "无明显形态标签"
        risks = "、".join(report.risk_flags) if report.risk_flags else "暂无高优先级风险标签"
        return "\n".join(
            [
                "量化技术信号摘要：",
                (
                    f"- 最新交易日 {metrics.get('trade_date')}: 收盘 {metrics.get('close')}, "
                    f"5日涨跌 {metrics.get('ret5')}%, 20日涨跌 {metrics.get('ret20')}%, "
                    f"MA5/10/20/60={metrics.get('ma5')}/{metrics.get('ma10')}/"
                    f"{metrics.get('ma20')}/{metrics.get('ma60')}。"
                ),
                (
                    f"- 波动与量能: ATR14 {metrics.get('atr14')} "
                    f"({metrics.get('atr14_pct')}%), 量比 {metrics.get('volume_ratio')}, "
                    f"60日量能分位 {metrics.get('volume_percentile60')}%。"
                ),
                (
                    f"- 位置: 20日支撑 {levels.get('support20')}, 20日压力 {levels.get('resistance20')}, "
                    f"60日支撑 {levels.get('support60')}, 60日压力 {levels.get('resistance60')}, "
                    f"偏离MA20 {metrics.get('bias20')}%。"
                ),
                f"- 形态标签: {tags}。",
                f"- 风险标签: {risks}。",
                (
                    f"- 交易计划参考: 理想回踩区 {plan.get('pullback_zone')}, "
                    f"突破确认价 {plan.get('breakout_price')}, "
                    f"初始止损 {plan.get('initial_stop')}, "
                    f"风险收益观察位 {plan.get('first_target')}。"
                ),
            ]
        )

    def _detect_tags(
        self,
        *,
        latest_close: float,
        latest_open: float,
        resistance20: float,
        support20: float,
        metrics: Dict[str, Any],
    ) -> tuple[List[str], List[str]]:
        tags: List[str] = []
        risk_flags: List[str] = []

        ma5 = metrics.get("ma5")
        ma10 = metrics.get("ma10")
        ma20 = metrics.get("ma20")
        ma60 = metrics.get("ma60")
        ret5 = metrics.get("ret5") or 0
        ret20 = metrics.get("ret20") or 0
        volume_ratio = metrics.get("volume_ratio") or 0
        close_position = metrics.get("close_position") or 0.5
        bias20 = metrics.get("bias20") or 0

        if ma5 and ma10 and ma20 and ma60 and latest_close > ma5 > ma10 > ma20 > ma60:
            tags.append("均线多头发散")

        if resistance20 and latest_close > resistance20 * 1.01 and volume_ratio >= 1.2:
            tags.append("箱体放量突破")

        if support20 and latest_close < support20 * 0.99:
            risk_flags.append("跌破20日平台")

        if ma20 and latest_close > ma20 and abs(bias20) <= 3 and volume_ratio <= 0.85:
            tags.append("缩量回踩MA20")

        if support20 and 0 <= (latest_close / support20 - 1) * 100 <= 3:
            tags.append("临近20日支撑")

        if resistance20 and 0 <= (resistance20 / latest_close - 1) * 100 <= 3:
            tags.append("临近20日压力")

        if ret5 <= -8 and volume_ratio >= 1.5:
            tags.append("恐慌放量")

        if volume_ratio >= 2 and ret5 <= 3 and close_position <= 0.45:
            risk_flags.append("放量滞涨/上影派发")

        if ret20 >= 20 and volume_ratio >= 1.2 and latest_close >= latest_open:
            tags.append("强动量延续")

        if not tags:
            tags.append("形态中性")

        return tags, risk_flags

    def _build_trade_plan(
        self,
        latest_close: float,
        levels: Dict[str, Any],
        tags: List[str],
        risk_flags: List[str],
    ) -> Dict[str, Any]:
        support20 = levels.get("support20")
        resistance20 = levels.get("resistance20")
        atr_stop = levels.get("atr_stop_1_5x") or levels.get("atr_stop_1x")

        pullback_low = support20 if support20 else latest_close * 0.97
        pullback_high = latest_close if "缩量回踩MA20" in tags else latest_close * 0.99
        initial_stop = min(atr_stop, support20 * 0.98) if atr_stop and support20 else atr_stop or support20

        return {
            "pullback_zone": self._format_range(pullback_low, pullback_high),
            "breakout_price": levels.get("breakout_trigger"),
            "initial_stop": self._round(initial_stop),
            "first_target": self._round(resistance20 if resistance20 and resistance20 > latest_close else latest_close * 1.08),
            "action_bias": "avoid_chasing" if risk_flags else "wait_for_confirm_or_pullback",
        }

    @staticmethod
    def _return_pct(series: pd.Series, periods: int) -> float | None:
        if len(series) <= periods:
            return None
        base = series.iloc[-periods - 1]
        latest = series.iloc[-1]
        if pd.isna(base) or base == 0:
            return None
        return round((latest / base - 1) * 100, 2)

    @staticmethod
    def _pct(value: float, base: Any) -> float | None:
        if pd.isna(base) or base == 0:
            return None
        return round((float(value) / float(base) - 1) * 100, 2)

    @staticmethod
    def _round(value: Any, digits: int = 2) -> float | None:
        if value is None or pd.isna(value):
            return None
        return round(float(value), digits)

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None
        return float(value)

    @staticmethod
    def _volume_percentile(series: pd.Series, latest_volume: float) -> float | None:
        clean = pd.to_numeric(series, errors="coerce").dropna()
        if clean.empty:
            return None
        return round(float((clean <= latest_volume).mean() * 100), 2)

    @staticmethod
    def _format_range(low: Any, high: Any) -> str:
        if low is None or high is None:
            return "N/A"
        low_value = round(float(min(low, high)), 3)
        high_value = round(float(max(low, high)), 3)
        return f"{low_value}-{high_value}"
