"""基于规则的股票筛选器。

该筛选器设计为确定性的逻辑。它首先进行广泛的风险和流动性过滤，
然后让多个策略检测器并行运行。只有在所有检测器都未命中的情况下，
股票才会被最终的策略网格剔除。
"""

import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import yaml
from loguru import logger

from src.database import StockDatabase
from src.market_regime import MarketRegimeDetector
from src.theme_scorer import ThemeScorer


# 默认筛选配置：分为保守、平衡和激进三种模式
DEFAULT_SCREENING_PROFILES: Dict[str, Dict[str, Any]] = {
    "conservative": {  # 保守型：高市值、低波动、严格流动性要求
        "min_price": 3.0,
        "min_history_days": 60,
        "max_annual_vol20": 0.60,
        "max_volume_ratio": 3.5,
        "min_volume_ratio_near_high60": 0.9,
        "min_total_market_cap": 8_000_000_000,
        "min_avg_amount20": 80_000_000,
        "ret20_min": -3.0,
        "ret20_max": 22.0,
        "ret5_min": -5.0,
        "max_ma5_ma20_ratio": 1.10,
        "max_momentum_bias20": 1.24,
        "max_trend_bias20": 1.18,
        "min_new_stock_history_days": 30,
        "allow_new_stock_channel": False,
        "turnover_rate_min": 0.8,
        "turnover_rate_max": 12.0,
        "max_per_industry": 2,
        "limit_up_change_pct": 9.5,
        "limit_down_change_pct": -9.5,
    },
    "balanced": {  # 平衡型：默认配置
        "min_price": 2.0,
        "min_history_days": 60,
        "max_annual_vol20": 0.80,
        "max_volume_ratio": 5.0,
        "min_volume_ratio_near_high60": 0.8,
        "min_total_market_cap": 3_000_000_000,
        "min_avg_amount20": 50_000_000,
        "ret20_min": -5.0,
        "ret20_max": 35.0,
        "ret5_min": -8.0,
        "max_ma5_ma20_ratio": 1.15,
        "max_momentum_bias20": 1.30,
        "max_trend_bias20": 1.22,
        "min_new_stock_history_days": 30,
        "allow_new_stock_channel": False,
        "turnover_rate_min": 0.5,
        "turnover_rate_max": 20.0,
        "max_per_industry": 3,
        "limit_up_change_pct": 9.5,
        "limit_down_change_pct": -9.5,
    },
    "aggressive": {  # 激进型：允许高波动、小市值、高换手
        "min_price": 2.0,
        "min_history_days": 60,
        "max_annual_vol20": 1.10,
        "max_volume_ratio": 8.0,
        "min_volume_ratio_near_high60": 0.6,
        "min_total_market_cap": 1_500_000_000,
        "min_avg_amount20": 30_000_000,
        "ret20_min": -10.0,
        "ret20_max": 55.0,
        "ret5_min": -12.0,
        "max_ma5_ma20_ratio": 1.25,
        "max_momentum_bias20": 1.38,
        "max_trend_bias20": 1.28,
        "min_new_stock_history_days": 30,
        "allow_new_stock_channel": True,
        "turnover_rate_min": 0.3,
        "turnover_rate_max": 35.0,
        "max_per_industry": 4,
        "limit_up_change_pct": 9.8,
        "limit_down_change_pct": -9.8,
    },
}


class StockScreener:
    """在 LLM 分析之前使用的确定性预筛选器。"""

    def __init__(
        self,
        db: StockDatabase | None = None,
        profile: str | None = None,
        config_path: str | Path | None = None,
    ):
        self.db = db or StockDatabase()
        self.config_path = Path(config_path) if config_path else Path(__file__).resolve().parents[1] / "config" / "stock_picking.yaml"
        self.stock_picking_config = self._load_stock_picking_config()
        self.market_regime = self._detect_market_regime(profile)
        auto_profile = None
        if not profile and self.market_regime.get("regime") in {"defensive", "range_bound", "offensive", "extreme_risk"}:
            auto_profile = self.market_regime.get("profile")
        self.profile_name, self.screening_profile = self._load_screening_profile(profile or auto_profile, self.stock_picking_config)
        self.last_audit: Dict[str, Any] = {}
        self.theme_scorer = self._build_theme_scorer()

    def run_technical_screening(self, top_n: int = 10, lookback_days: int = 90) -> List[Dict[str, Any]]:
        """
        执行技术面筛选主流程。
        1. 检查市场环境（风控）。
        2. 从数据库查询候选股票池。
        3. 应用基础过滤（价格、波动率、市值等）。
        4. 执行多策略匹配（突破、主升、抄底等）。
        5. 进行综合打分（技术面 + 题材热度）。
        6. 按分值排序并应用行业上限。
        """
        profile = self.screening_profile
        top_n = max(5, min(int(top_n or profile.get("top_n", 10)), int(profile.get("max_top_n", 10))))
        lookback_days = max(60, int(lookback_days or 90))

        if bool(self.market_regime.get("allow_empty")):
            # 如果检测到极端市场风险，返回空列表
            self.last_audit = self._build_empty_market_regime_audit(top_n)
            self._write_screener_audit(self.last_audit)
            logger.warning("[Screener] 检测到极端市场风险；返回空候选列表。")
            return []

        latest_dates = self.db.query_to_dataframe(
            """
                SELECT DISTINCT trade_date
                FROM market_bars
                WHERE period = 'daily'
                ORDER BY trade_date DESC
                LIMIT ?
            """,
            (lookback_days,),
        )
        if latest_dates is None or latest_dates.empty:
            logger.warning("[Screener] 未找到日线行情数据。")
            return []
        min_trade_date = str(latest_dates["trade_date"].min())

        stock_basic_columns = self._get_table_columns("stock_basic")
        name_expr = "COALESCE(b.name, mb.code)" if "name" in stock_basic_columns else "mb.code"
        industry_expr = "b.industry" if "industry" in stock_basic_columns else "NULL"
        query = f"""
            WITH latest_quotes AS (
                SELECT q.*
                FROM daily_quotes q
                JOIN (
                    SELECT MAX(trade_date) AS trade_date
                    FROM daily_quotes
                ) latest ON q.trade_date = latest.trade_date
            ),
            valuation_quotes AS (
                SELECT code, pe_ttm, pb, total_market_cap
                FROM (
                    SELECT
                        q.code,
                        q.pe_ttm,
                        q.pb,
                        q.total_market_cap,
                        ROW_NUMBER() OVER (
                            PARTITION BY q.code
                            ORDER BY q.trade_date DESC
                        ) AS rn
                    FROM daily_quotes q
                    WHERE q.pe_ttm IS NOT NULL
                       OR q.pb IS NOT NULL
                       OR q.total_market_cap IS NOT NULL
                )
                WHERE rn = 1
            )
            SELECT
                mb.code,
                {name_expr} AS name,
                {industry_expr} AS industry,
                mb.trade_date,
                mb.open,
                CASE
                    WHEN mb.trade_date = q.trade_date
                         AND q.price IS NOT NULL
                         AND (mb.high IS NULL OR q.price > mb.high)
                    THEN q.price
                    ELSE mb.high
                END AS high,
                CASE
                    WHEN mb.trade_date = q.trade_date
                         AND q.price IS NOT NULL
                         AND (mb.low IS NULL OR q.price < mb.low)
                    THEN q.price
                    ELSE mb.low
                END AS low,
                CASE
                    WHEN mb.trade_date = q.trade_date AND q.price IS NOT NULL
                    THEN q.price
                    ELSE mb.close
                END AS close,
                mb.volume,
                mb.amount AS bar_amount,
                q.price,
                q.change_pct,
                q.amount AS quote_amount,
                q.turnover_rate,
                COALESCE(q.pe_ttm, v.pe_ttm) AS pe_ttm,
                COALESCE(q.pb, v.pb) AS pb,
                COALESCE(q.total_market_cap, v.total_market_cap) AS total_market_cap
            FROM market_bars mb
            JOIN latest_quotes q ON mb.code = q.code
            LEFT JOIN valuation_quotes v ON mb.code = v.code
            LEFT JOIN stock_basic b ON mb.code = b.code
            WHERE mb.period = 'daily'
              AND mb.trade_date >= ?
              AND mb.code NOT LIKE '300%'
              AND mb.code NOT LIKE '301%'
              AND mb.code NOT LIKE '688%'
              AND mb.code NOT LIKE '689%'
              AND mb.code NOT LIKE '8%'
              AND mb.code NOT LIKE '4%'
              AND q.price > 0
            ORDER BY mb.code, mb.trade_date
        """
        df = self.db.query_to_dataframe(query, (min_trade_date,))
        if df is None or df.empty:
            logger.warning("[Screener] 股票池查询后无可用数据。")
            return []

        picks: List[Dict[str, Any]] = []
        rejected_counts: Counter[str] = Counter()
        rejected_stocks: Dict[str, Dict[str, Any]] = {}

        def reject(reason: str, code_value: Any, name_value: Any = "", metrics: Dict[str, Any] | None = None) -> None:
            code_text = str(code_value).zfill(6)
            rejected_counts[reason] += 1
            rejected_stocks[code_text] = {
                "code": code_text,
                "name": "" if name_value is None else str(name_value),
                "reason": reason,
                "metrics": metrics or {},
            }

        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "bar_amount",
            "price",
            "change_pct",
            "quote_amount",
            "turnover_rate",
            "pe_ttm",
            "pb",
            "total_market_cap",
        ]

        for code, bars in df.groupby("code", sort=False):
            bars = bars.sort_values("trade_date").copy()
            # 1. 历史长度检查
            min_history_days = int(profile["min_history_days"])
            min_new_stock_history_days = int(profile.get("min_new_stock_history_days", 30) or 30)
            is_new_stock_channel = bool(profile.get("allow_new_stock_channel")) and len(bars) >= min_new_stock_history_days
            if len(bars) < min_history_days and not is_new_stock_channel:
                reject("history_too_short", code, metrics={"bar_count": len(bars), "min_history_days": min_history_days})
                continue

            for col in numeric_columns:
                if col in bars.columns:
                    bars[col] = pd.to_numeric(bars[col], errors="coerce")

            # 2. 排除 ST 和退市股
            latest = bars.iloc[-1]
            name = str(latest.get("name") or "")
            if "ST" in name.upper() or "退" in name:
                reject("st_or_delisting", code, name)
                continue

            # 3. 价格过滤
            close = float(latest.get("close") or latest.get("price") or 0)
            if close <= float(profile["min_price"]):
                reject("price_too_low", code, name, {"close": close, "min_price": profile["min_price"]})
                continue

            closes = bars["close"].dropna()
            is_new_stock_channel = bool(profile.get("allow_new_stock_channel")) and len(closes) >= min_new_stock_history_days
            if len(closes) < min_history_days and not is_new_stock_channel:
                reject("valid_close_history_too_short", code, name, {"close_count": len(closes), "min_history_days": min_history_days})
                continue

            ma5 = closes.tail(5).mean()
            ma10 = closes.tail(10).mean()
            ma20 = closes.tail(20).mean()
            ma60 = closes.tail(60).mean()
            ma5_ma20_ratio = ma5 / ma20 if ma20 else np.nan
            bias20_ratio = close / ma20 if ma20 else np.nan

            ret1 = (close / closes.iloc[-2] - 1) * 100 if len(closes) >= 2 and closes.iloc[-2] else 0
            ret3 = (close / closes.iloc[-4] - 1) * 100 if len(closes) >= 4 and closes.iloc[-4] else 0
            ret5 = (close / closes.iloc[-6] - 1) * 100 if len(closes) >= 6 and closes.iloc[-6] else 0
            ret10 = (close / closes.iloc[-11] - 1) * 100 if len(closes) >= 11 and closes.iloc[-11] else 0
            ret20 = (close / closes.iloc[-21] - 1) * 100 if len(closes) >= 21 and closes.iloc[-21] else 0
            rsi14 = self._calculate_rsi(closes, 14)
            macd_hist = self._calculate_macd_histogram(closes)
            macd_hist_latest = float(macd_hist.iloc[-1]) if len(macd_hist) and pd.notna(macd_hist.iloc[-1]) else None
            macd_hist_prev = float(macd_hist.iloc[-2]) if len(macd_hist) >= 2 and pd.notna(macd_hist.iloc[-2]) else None
            macd_hist_rising = macd_hist_latest is not None and macd_hist_prev is not None and macd_hist_latest > macd_hist_prev
            macd_bullish_divergence = self._has_macd_bullish_divergence(closes, macd_hist)

            # 4. 波动率检查
            daily_returns = closes.pct_change()
            annual_vol20 = daily_returns.tail(20).std() * np.sqrt(252)
            if pd.notna(annual_vol20) and annual_vol20 > float(profile["max_annual_vol20"]):
                reject("annual_vol20_too_high", code, name, {"annual_vol20": round(float(annual_vol20), 4), "max": profile["max_annual_vol20"]})
                continue

            high60 = bars["high"].tail(60).max()
            low60 = bars["low"].tail(60).min()
            high60_distance = (close / high60 - 1) * 100 if high60 else 0
            low60_distance = (close / low60 - 1) * 100 if low60 else 0

            # 5. 流动性检查（平均成交额）
            amount_series = self._normalize_amount_series_to_yuan(bars)
            avg_amount20 = amount_series.tail(20).mean()
            if pd.isna(avg_amount20) or avg_amount20 < float(profile["min_avg_amount20"]):
                reject(
                    "avg_amount20_too_low",
                    code,
                    name,
                    {"avg_amount20": None if pd.isna(avg_amount20) else round(float(avg_amount20), 2), "min": profile["min_avg_amount20"]},
                )
                continue

            volume = bars["volume"].replace(0, np.nan)
            vol_ratio = volume.iloc[-1] / volume.tail(20).mean() if len(volume.dropna()) >= 20 else 1.0
            vol5_ratio = volume.tail(5).mean() / volume.tail(20).mean() if len(volume.dropna()) >= 20 else 1.0
            if pd.notna(vol_ratio) and vol_ratio > float(profile["max_volume_ratio"]):
                reject("volume_ratio_too_high", code, name, {"volume_ratio": round(float(vol_ratio), 4), "max": profile["max_volume_ratio"]})
                continue

            # 6. 异常波动检查（跌停、零成交）
            raw_volume = bars["volume"].fillna(0)
            if (raw_volume.tail(5) <= 0).any():
                reject("recent_zero_volume", code, name)
                continue
            recent_pct = closes.pct_change().tail(5) * 100
            limit_up_threshold = float(profile["limit_up_change_pct"])
            limit_down_threshold = float(profile["limit_down_change_pct"])
            past_10_pct = closes.pct_change().tail(11).iloc[:-1] * 100
            recent_limit_up_count = int((past_10_pct >= limit_up_threshold).sum()) if not past_10_pct.empty else 0
            potential_dragon_pullback = (
                recent_limit_up_count >= 2
                and ret1 <= -5
                and (abs(close / ma5 - 1) <= 0.04 or abs(close / ma10 - 1) <= 0.05)
            )
            if (recent_pct <= limit_down_threshold).any() and not potential_dragon_pullback:
                reject("recent_limit_down", code, name, {"limit_down_change_pct": profile["limit_down_change_pct"]})
                continue

            change_pct = latest.get("change_pct")
            change_pct = float(change_pct) if pd.notna(change_pct) else 0.0
            turnover_rate = latest.get("turnover_rate")
            turnover_rate = float(turnover_rate) if pd.notna(turnover_rate) else None
            if turnover_rate is not None and (
                turnover_rate < float(profile["turnover_rate_min"])
                or turnover_rate > float(profile["turnover_rate_max"])
            ):
                reject(
                    "turnover_rate_out_of_range",
                    code,
                    name,
                    {
                        "turnover_rate": turnover_rate,
                        "min": profile["turnover_rate_min"],
                        "max": profile["turnover_rate_max"],
                    },
                )
                continue

            # 7. 市值检查
            total_market_cap = latest.get("total_market_cap")
            total_market_cap = float(total_market_cap) if pd.notna(total_market_cap) else None
            if total_market_cap is not None and total_market_cap < float(profile["min_total_market_cap"]):
                reject("market_cap_too_low", code, name, {"total_market_cap": total_market_cap, "min": profile["min_total_market_cap"]})
                continue

            pe_ttm = latest.get("pe_ttm")
            pe_ttm = float(pe_ttm) if pd.notna(pe_ttm) else None
            pb = None if pd.isna(latest.get("pb")) else float(latest.get("pb"))
            latest_open = float(latest.get("open") or close)
            latest_high = float(latest.get("high") or close)
            latest_low = float(latest.get("low") or close)
            intraday_position = (close - latest_low) / (latest_high - latest_low) if latest_high > latest_low else 0.5
            first_limit_up_breakout = change_pct >= limit_up_threshold and recent_limit_up_count == 0
            bullish_pullback_candle = self._is_bullish_pullback_candle(
                latest_open=latest_open,
                latest_high=latest_high,
                latest_low=latest_low,
                close=close,
                intraday_position=intraday_position,
            )

            strategy_matches = self._detect_strategy_matches(
                profile=profile,
                close=close,
                ma5=ma5,
                ma10=ma10,
                ma20=ma20,
                ma60=ma60,
                ret3=ret3,
                ret1=ret1,
                ret5=ret5,
                ret10=ret10,
                ret20=ret20,
                change_pct=change_pct,
                high60_distance=high60_distance,
                low60_distance=low60_distance,
                vol_ratio=vol_ratio,
                vol5_ratio=vol5_ratio,
                turnover_rate=turnover_rate,
                pe_ttm=pe_ttm,
                pb=pb,
                latest_open=latest_open,
                intraday_position=intraday_position,
                bias20_ratio=bias20_ratio,
                rsi14=rsi14,
                macd_hist_rising=macd_hist_rising,
                macd_bullish_divergence=macd_bullish_divergence,
                bullish_pullback_candle=bullish_pullback_candle,
                recent_limit_up_count=recent_limit_up_count,
                first_limit_up_breakout=first_limit_up_breakout,
                potential_dragon_pullback=potential_dragon_pullback,
                is_new_stock_channel=is_new_stock_channel,
            )
            if not strategy_matches:
                reject(
                    "no_strategy_detector_matched",
                    code,
                    name,
                    {
                        "close": close,
                        "ma20": round(float(ma20), 3),
                        "ma60": round(float(ma60), 3),
                        "ret1": round(float(ret1), 2),
                        "ret5": round(float(ret5), 2),
                        "ret20": round(float(ret20), 2),
                        "rsi14": round(float(rsi14), 2) if rsi14 is not None else None,
                        "pe_ttm": pe_ttm,
                        "pb": pb,
                        "volume_ratio": round(float(vol_ratio), 2) if pd.notna(vol_ratio) else None,
                        "vol5_ratio": round(float(vol5_ratio), 2) if pd.notna(vol5_ratio) else None,
                        "turnover_rate": turnover_rate,
                    },
                )
                continue

            technical_score = self._score_strategy_candidate(
                strategy_matches=strategy_matches,
                ret1=ret1,
                ret5=ret5,
                ret10=ret10,
                ret20=ret20,
                change_pct=change_pct,
                high60_distance=high60_distance,
                low60_distance=low60_distance,
                avg_amount20=avg_amount20,
                vol_ratio=vol_ratio,
                turnover_rate=turnover_rate,
                annual_vol20=annual_vol20,
                recent_limit_up_count=recent_limit_up_count,
                intraday_position=intraday_position,
                bias20_ratio=bias20_ratio,
                pe_ttm=pe_ttm,
                pb=pb,
                close=close,
                ma5=ma5,
                ma10=ma10,
                ma20=ma20,
                ma60=ma60,
            )

            key_metrics = {
                "ret3": round(float(ret3), 2),
                "ret1": round(float(ret1), 2),
                "ret5": round(float(ret5), 2),
                "ret10": round(float(ret10), 2),
                "ret20": round(float(ret20), 2),
                "annual_vol20": round(float(annual_vol20), 4) if pd.notna(annual_vol20) else None,
                "volume_ratio": round(float(vol_ratio), 2) if pd.notna(vol_ratio) else None,
                "vol5_ratio": round(float(vol5_ratio), 2) if pd.notna(vol5_ratio) else None,
                "avg_amount20": round(float(avg_amount20), 2),
                "turnover_rate": turnover_rate,
                "total_market_cap": total_market_cap,
                "ma5_ma20_ratio": round(float(ma5_ma20_ratio), 4) if pd.notna(ma5_ma20_ratio) else None,
                "bias20_ratio": round(float(bias20_ratio), 4) if pd.notna(bias20_ratio) else None,
                "rsi14": round(float(rsi14), 2) if rsi14 is not None else None,
                "macd_hist_rising": macd_hist_rising,
                "macd_bullish_divergence": macd_bullish_divergence,
                "recent_limit_up_count": recent_limit_up_count,
                "is_new_stock_channel": is_new_stock_channel,
                "low60_distance": round(float(low60_distance), 2),
                "strategy_tags": [match["tag"] for match in strategy_matches],
                "strategy_confidence": round(float(max(match["confidence"] for match in strategy_matches)), 2),
            }
            picks.append(
                {
                    "code": str(code).zfill(6),
                    "name": latest.get("name"),
                    "industry": latest.get("industry"),
                    "price": float(latest.get("price") or close),
                    "change_pct": change_pct,
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "total_market_cap": total_market_cap,
                    "technical_score": round(float(technical_score), 2),
                    "ma20": round(float(ma20), 3),
                    "ma60": round(float(ma60), 3),
                    "ret5": round(float(ret5), 2),
                    "ret20": round(float(ret20), 2),
                    "rsi14": round(float(rsi14), 2) if rsi14 is not None else None,
                    "annual_vol20": round(float(annual_vol20), 4) if pd.notna(annual_vol20) else None,
                    "volume_ratio": round(float(vol_ratio), 2) if pd.notna(vol_ratio) else None,
                    "avg_amount20": round(float(avg_amount20), 2),
                    "strategy_tags": [match["tag"] for match in strategy_matches],
                    "strategy_confidence": round(float(max(match["confidence"] for match in strategy_matches)), 2),
                    "key_metrics": key_metrics,
                    "screen_reason": self._format_screen_reason(strategy_matches, ret5, ret20, pe_ttm, pb, vol_ratio, turnover_rate, low60_distance),
                }
            )

        if not picks:
            self.last_audit = self._build_audit_record(df, [], rejected_counts, rejected_stocks, top_n)
            self._write_screener_audit(self.last_audit)
            logger.warning("[Screener] 无股票通过筛选。")
            return []

        if self.theme_scorer is not None:
            picks = self.theme_scorer.score_candidates(picks)
        else:
            picks = self._attach_disabled_theme_score(picks)

        ranked = sorted(picks, key=lambda item: item["technical_score"], reverse=True)
        candidates = self._apply_strategy_quota(
            ranked,
            top_n=top_n,
            max_per_industry=int(profile["max_per_industry"]),
            reject=reject,
        )

        self.last_audit = self._build_audit_record(df, candidates, rejected_counts, rejected_stocks, top_n)
        self._write_screener_audit(self.last_audit)
        logger.info("[Screener] 已选择 {} 个候选股，使用配置={}，剔除原因分布={}", len(candidates), self.profile_name, dict(rejected_counts.most_common(5)))
        return candidates

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

        # 1. 经典趋势突破策略 (Trend Breakout)
        # 条件：站上20日线，20日线向上，20日跌幅在可控范围内，且非缩量接近新高
        if (
            close >= ma20
            and ma20 >= ma60 * 0.98
            and float(profile["ret20_min"]) <= ret20 <= float(profile["ret20_max"])
            and ret5 >= float(profile["ret5_min"])
            and change_pct < float(profile["limit_up_change_pct"])
            and bias20 <= float(profile.get("max_trend_bias20", 1.22))
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
        # 条件：均线多头排列，强力上涨，放量且无滞涨迹象
        high_volume_stall = (
            volume_ratio >= 2.5
            and (close <= latest_open or intraday_position < 0.35)
            and ret5 < 12
        )
        if (
            close > ma5 > ma10 > ma20 > ma60
            and abs(close / ma5 - 1) <= 0.06
            and bias20 <= float(profile.get("max_momentum_bias20", 1.30))
            and ret20 >= 20
            and ret5 >= 3
            and change_pct < float(profile["limit_up_change_pct"])
            and volume_ratio >= 1.1
            and volume5_ratio >= 1.05
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
        # 条件：低 PE/PB，处于60日低点附近，有启动迹象
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
        # 条件：短期超跌，PE 合理，日内出现反转信号（放量且收盘在日内高点附近）
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
        # 条件：趋势向上，近期回撤，在20/60日线附近显著缩量企稳
        if (
            ma20 >= ma60 * 0.98
            and ret10 > -8
            and (abs(close / ma20 - 1) <= 0.03 or abs(close / ma60 - 1) <= 0.04)
            and volume_ratio <= 0.85
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

        # 6. Dragon Pullback: a recent limit-up leader's first sharp pullback near MA5/MA10.
        if (
            potential_dragon_pullback
            and recent_limit_up_count >= 2
            and ret1 <= -5
            and volume_ratio >= 0.7
            and volume5_ratio >= 0.9
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

        # 7. First Limit-up Breakout: first board from a controlled low/mid position.
        if (
            first_limit_up_breakout
            and close >= ma20
            and ret20 <= float(profile["ret20_max"])
            and bias20 <= float(profile.get("max_momentum_bias20", 1.30))
            and volume_ratio >= 1.2
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

        # Tushare 日线 amount 常见单位是千元，volume 是手；本地阈值统一按元比较。
        valid = amount.notna() & (amount > 0) & hand_based_amount.notna() & (hand_based_amount > 0)
        if valid.any():
            median_ratio = (hand_based_amount[valid] / amount[valid]).replace([np.inf, -np.inf], np.nan).dropna().median()
            if pd.notna(median_ratio) and 100 <= float(median_ratio) <= 2_000:
                amount = amount * 1_000

        return amount.fillna(closes * volumes)

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
            score += max(-10, min(20, ret20)) * 1.4
            score += max(-5, min(12, ret5)) * 1.2
            score += 10 if close >= ma5 >= ma10 >= ma20 else 0
            score += 8 if ma20 >= ma60 else 0
            score += max(-10, min(8, high60_distance + 8))
        elif primary == "momentum_leader":
            score += min(32, max(0, ret20)) * 1.1
            score += min(14, max(0, ret5)) * 1.0
            score += 14 if close > ma5 > ma10 > ma20 > ma60 else 0
            score += max(0, 8 - abs(close / ma5 - 1) * 100)
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
            score += max(0, 10 - abs(close / ma5 - 1) * 100)
            score += min(14, abs(min(ret5, 0))) * 0.7
            score += 8 if close >= ma10 else 3
        elif primary == "first_limit_up_breakout":
            score += min(24, max(0, ret20)) * 1.0
            score += 10 if close >= ma5 >= ma10 >= ma20 else 4
            score += 6 if ma20 >= ma60 * 0.98 else 0
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

        if primary in {"momentum_leader", "first_limit_up_breakout"}:
            if ret5 > 12:
                risk_penalty += min(10.0, (ret5 - 12) * 0.9)
            if ret20 > 35:
                risk_penalty += min(14.0, (ret20 - 35) * 0.7)
            if pd.notna(vol_ratio) and float(vol_ratio) > 2.8:
                risk_penalty += min(8.0, (float(vol_ratio) - 2.8) * 4.0)
            if ret1 > 7:
                risk_penalty += min(6.0, (ret1 - 7) * 1.0)

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
    ) -> str:
        """格式化筛选理由，用于展示命中策略和关键指标。"""
        primary = strategy_matches[0]
        tags = "、".join(match["label"] for match in strategy_matches)
        pe_text = "N/A" if pe_ttm is None else f"{pe_ttm:.1f}"
        pb_text = "N/A" if pb is None else f"{pb:.2f}"
        vol_text = "N/A" if pd.isna(vol_ratio) else f"{float(vol_ratio):.2f}"
        turnover_text = "N/A" if turnover_rate is None else f"{turnover_rate:.2f}%"
        return (
            f"【{primary['label']}】命中策略: {tags}; "
            f"ret5 {ret5:.2f}%, ret20 {ret20:.2f}%, PE {pe_text}, PB {pb_text}, "
            f"量比 {vol_text}, 换手 {turnover_text}, 距60日低点 {low60_distance:.2f}%。"
        )

    def _load_screening_profile(
        self,
        requested_profile: str | None = None,
        config: Dict[str, Any] | None = None,
    ) -> tuple[str, Dict[str, Any]]:
        """从配置中加载筛选配置，并与默认配置合并。"""
        config = config or {}
        default_profile = str(config.get("default_profile") or "balanced").lower()
        profile_name = str(requested_profile or default_profile or "balanced").lower()
        if profile_name not in DEFAULT_SCREENING_PROFILES:
            logger.warning("[Screener] unknown profile {}; falling back to balanced.", profile_name)
            profile_name = "balanced"

        profiles_config = config.get("profiles") or {}
        merged = dict(DEFAULT_SCREENING_PROFILES[profile_name])
        if isinstance(profiles_config.get(profile_name), dict):
            merged.update({k: v for k, v in profiles_config[profile_name].items() if v is not None})

        output_config = config.get("output") or {}
        merged["top_n"] = int(output_config.get("top_n", 10) or 10)
        merged["max_top_n"] = int(output_config.get("max_top_n", 10) or 10)
        return profile_name, self._sanitize_screening_profile(merged)

    def _load_stock_picking_config(self) -> Dict[str, Any]:
        """加载 stock_picking.yaml 配置文件。"""
        if not self.config_path.exists():
            logger.warning("[Screener] config not found: {}; using defaults.", self.config_path)
            return {}
        try:
            with self.config_path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning("[Screener] failed to load config {}: {}", self.config_path, exc)
            return {}

    def _sanitize_screening_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """对筛选配置参数进行清洗和合理性检查。"""
        safe = dict(DEFAULT_SCREENING_PROFILES["balanced"])
        safe.update(profile)

        positive_keys = [
            "min_price",
            "min_history_days",
            "max_annual_vol20",
            "max_volume_ratio",
            "min_total_market_cap",
            "min_avg_amount20",
            "max_ma5_ma20_ratio",
            "max_momentum_bias20",
            "max_trend_bias20",
            "min_new_stock_history_days",
            "turnover_rate_max",
            "max_per_industry",
            "top_n",
            "max_top_n",
        ]
        for key in positive_keys:
            try:
                if float(safe[key]) <= 0:
                    safe[key] = DEFAULT_SCREENING_PROFILES["balanced"].get(key, 10)
            except (TypeError, ValueError):
                safe[key] = DEFAULT_SCREENING_PROFILES["balanced"].get(key, 10)

        if float(safe["turnover_rate_min"]) < 0:
            safe["turnover_rate_min"] = DEFAULT_SCREENING_PROFILES["balanced"]["turnover_rate_min"]
        if float(safe["ret20_min"]) >= float(safe["ret20_max"]):
            safe["ret20_min"] = DEFAULT_SCREENING_PROFILES["balanced"]["ret20_min"]
            safe["ret20_max"] = DEFAULT_SCREENING_PROFILES["balanced"]["ret20_max"]
        if float(safe["turnover_rate_min"]) >= float(safe["turnover_rate_max"]):
            safe["turnover_rate_min"] = DEFAULT_SCREENING_PROFILES["balanced"]["turnover_rate_min"]
            safe["turnover_rate_max"] = DEFAULT_SCREENING_PROFILES["balanced"]["turnover_rate_max"]
        return safe

    def _build_audit_record(
        self,
        df: pd.DataFrame,
        candidates: List[Dict[str, Any]],
        rejected_counts: Counter[str],
        rejected_stocks: Dict[str, Dict[str, Any]],
        top_n: int,
    ) -> Dict[str, Any]:
        """构建筛选审计记录，包含通过和被剔除的股票信息。"""
        input_stock_count = int(df["code"].nunique()) if df is not None and not df.empty else 0
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "profile": self.profile_name,
            "market_regime": self.market_regime,
            "top_n": top_n,
            "input_stock_count": input_stock_count,
            "candidate_count": len(candidates),
            "rejected_count": len(rejected_stocks),
            "rule_reject_counts": dict(rejected_counts.most_common()),
            "rejected_stocks": list(rejected_stocks.values()),
            "candidates": [
                {
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "industry": item.get("industry"),
                    "technical_score": item.get("technical_score"),
                    "theme_score": item.get("theme_score", 0.0),
                    "theme_reason": item.get("theme_reason", ""),
                    "matched_themes": item.get("matched_themes", []),
                    "strategy_tags": item.get("strategy_tags", []),
                    "strategy_confidence": item.get("strategy_confidence"),
                    "screen_reason": item.get("screen_reason"),
                    "key_metrics": item.get("key_metrics", {}),
                }
                for item in candidates
            ],
        }

    def _build_empty_market_regime_audit(self, top_n: int) -> Dict[str, Any]:
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "profile": self.profile_name,
            "market_regime": self.market_regime,
            "top_n": top_n,
            "input_stock_count": 0,
            "candidate_count": 0,
            "rejected_count": 0,
            "rule_reject_counts": {"market_extreme_risk": 1},
            "rejected_stocks": [],
            "candidates": [],
        }

    def _detect_market_regime(self, explicit_profile: str | None) -> Dict[str, Any]:
        """检测当前市场环境（Regime），用于自动切换筛选配置。"""
        if explicit_profile:
            return {
                "regime": "manual",
                "profile": str(explicit_profile).lower(),
                "allow_empty": False,
                "reason": "manual profile override",
                "metrics": {},
                "signals": [],
            }
        try:
            detector = MarketRegimeDetector(
                db=self.db,
                config=(self.stock_picking_config or {}).get("market_regime") or {},
            )
            return detector.detect()
        except Exception as exc:
            logger.warning("[Screener] market regime detection failed; using balanced defaults: {}", exc)
            return {
                "regime": "unknown",
                "profile": "balanced",
                "allow_empty": False,
                "reason": f"market regime detection failed: {exc}",
                "metrics": {},
                "signals": [],
            }

    def _write_screener_audit(self, audit: Dict[str, Any]) -> None:
        """将筛选审计结果写入 JSON 文件。"""
        output_config = (self.stock_picking_config or {}).get("output") or {}
        if not bool(output_config.get("save_audit", False)):
            return

        output_path = Path(output_config.get("audit_path") or "outputs/screener_audit.json")
        if not output_path.is_absolute():
            output_path = Path(__file__).resolve().parents[1] / output_path

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("[Screener] failed to write audit file: {}", exc)

    def _build_theme_scorer(self) -> ThemeScorer | None:
        """根据配置构建题材打分器（ThemeScorer）。"""
        theme_config = self.stock_picking_config.get("theme_scoring") or {}
        if not bool(theme_config.get("enabled", False)):
            return None
        return ThemeScorer(
            top_boards_per_kind=int(theme_config.get("top_boards_per_kind", 8) or 8),
            max_bonus=float(theme_config.get("max_bonus", 12.0) or 12.0),
        )

    def _attach_disabled_theme_score(self, picks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for item in picks:
            item["theme_score"] = 0.0
            item["theme_reason"] = "theme scoring disabled"
            item["matched_themes"] = []
            item.setdefault("key_metrics", {})["theme_score"] = 0.0
        return picks

    def _apply_industry_cap(self, ranked: List[Dict[str, Any]], top_n: int, max_per_industry: int = 3) -> List[Dict[str, Any]]:
        """应用行业分布限制，确保候选股票在不同行业间有一定分散度。"""
        candidates = []
        industry_counts = {}
        for item in ranked:
            industry = item.get("industry")
            if industry:
                if industry_counts.get(industry, 0) >= max_per_industry:
                    continue
                industry_counts[industry] = industry_counts.get(industry, 0) + 1
            candidates.append(item)
            if len(candidates) >= top_n:
                break
        return candidates

    def _apply_strategy_quota(
        self,
        ranked: List[Dict[str, Any]],
        top_n: int,
        max_per_industry: int = 3,
        reject=None,
    ) -> List[Dict[str, Any]]:
        """按主策略分配候选名额，避免单一策略挤占整个候选池。"""
        if not ranked or top_n <= 0:
            return []

        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in ranked:
            primary_tag = self._get_primary_strategy_tag(item)
            groups[primary_tag].append(item)

        group_count = max(1, min(len(groups), top_n))
        per_strategy_quota = max(1, int(math.ceil(top_n / group_count)))
        selected: List[Dict[str, Any]] = []
        selected_codes = set()
        industry_counts: Dict[str, int] = {}

        def try_add(item: Dict[str, Any], count_reject: bool = True) -> bool:
            if len(selected) >= top_n:
                return False
            code = item.get("code")
            if code in selected_codes:
                return False
            industry = item.get("industry")
            if industry and industry_counts.get(industry, 0) >= max_per_industry:
                if count_reject and reject:
                    reject(
                        "industry_cap_exceeded",
                        item.get("code"),
                        item.get("name"),
                        {"industry": industry, "max_per_industry": max_per_industry},
                    )
                return False
            selected.append(item)
            selected_codes.add(code)
            if industry:
                industry_counts[industry] = industry_counts.get(industry, 0) + 1
            return True

        for tag in sorted(groups):
            group = sorted(
                groups[tag],
                key=lambda item: (
                    float(item.get("strategy_confidence") or 0),
                    float(item.get("technical_score") or 0),
                ),
                reverse=True,
            )
            picked_for_group = 0
            for item in group:
                if picked_for_group >= per_strategy_quota:
                    break
                if try_add(item):
                    picked_for_group += 1

        # 极端行情下某些策略不足额时，用全局高分候选补位，但仍遵守行业上限。
        for item in ranked:
            if len(selected) >= top_n:
                break
            try_add(item, count_reject=False)

        return sorted(selected, key=lambda item: float(item.get("technical_score") or 0), reverse=True)

    def _get_primary_strategy_tag(self, item: Dict[str, Any]) -> str:
        """读取候选股主策略标签。"""
        tags = item.get("strategy_tags") or []
        if isinstance(tags, list) and tags:
            return str(tags[0] or "unknown")
        return "unknown"

    def _get_table_columns(self, table_name: str) -> set:
        """获取数据库表的列名，用于动态构建查询语句。"""
        try:
            with self.db.get_connection() as conn:
                rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            return {row[1] for row in rows}
        except Exception as exc:
            logger.warning("[Screener] failed to inspect table {}: {}", table_name, exc)
            return set()


ScreenerAgent = StockScreener


if __name__ == "__main__":
    screener = StockScreener()
    result = screener.run_technical_screening(top_n=10)
    for row in result:
        print(row)
