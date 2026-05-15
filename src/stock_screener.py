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
from src.screener_profiles import DEFAULT_SCREENING_PROFILES
from src.screener_strategies import StrategyDetectorMixin
from src.screener_scoring import ScoringMixin
from src.theme_scorer import ThemeScorer


class StockScreener(StrategyDetectorMixin, ScoringMixin):
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

    def run_technical_screening(
        self,
        top_n: int = 10,
        lookback_days: int = 90,
        as_of_date: str | None = None,
        apply_backtest_weights: bool = True,
    ) -> List[Dict[str, Any]]:
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
        as_of_date = self._normalize_as_of_date(as_of_date)

        if bool(self.market_regime.get("allow_empty")):
            self.last_audit = self._build_empty_market_regime_audit(top_n)
            self._write_screener_audit(self.last_audit)
            logger.warning("[Screener] 检测到极端市场风险；返回空候选列表。")
            return []

        latest_dates_sql = """
                SELECT DISTINCT trade_date
                FROM market_bars
                WHERE period = 'daily'
            """
        latest_dates_params: tuple[Any, ...] = ()
        if as_of_date:
            latest_dates_sql += " AND trade_date <= ?"
            latest_dates_params = (as_of_date,)
        latest_dates_sql += """
                ORDER BY trade_date DESC
                LIMIT ?
            """
        latest_dates = self.db.query_to_dataframe(
            latest_dates_sql,
            latest_dates_params + (lookback_days,),
        )
        if latest_dates is None or latest_dates.empty:
            logger.warning("[Screener] 未找到日线行情数据。")
            return []
        min_trade_date = str(latest_dates["trade_date"].min())
        latest_bar_date = str(latest_dates["trade_date"].max())
        latest_quote_date_df = self.db.query_to_dataframe(
            """
            SELECT MAX(trade_date) AS trade_date
            FROM daily_quotes
            WHERE trade_date <= ?
              AND (? IS NULL OR trade_date <= ?)
            """,
            (latest_bar_date, as_of_date, as_of_date),
        )
        latest_quote_date = None
        if latest_quote_date_df is not None and not latest_quote_date_df.empty:
            latest_quote_date = latest_quote_date_df.iloc[0].get("trade_date")

        stock_basic_columns = self._get_table_columns("stock_basic")
        name_expr = "COALESCE(b.name, mb.code)" if "name" in stock_basic_columns else "mb.code"
        industry_expr = "b.industry" if "industry" in stock_basic_columns else "NULL"
        query = f"""
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
                CASE
                    WHEN mb.trade_date = q.trade_date AND q.price IS NOT NULL
                    THEN q.price
                    ELSE mb.close
                END AS price,
                CASE
                    WHEN mb.trade_date = q.trade_date
                    THEN q.change_pct
                    ELSE NULL
                END AS change_pct,
                CASE
                    WHEN mb.trade_date = q.trade_date AND q.amount IS NOT NULL
                    THEN q.amount
                    ELSE mb.amount
                END AS quote_amount,
                CASE
                    WHEN mb.trade_date = q.trade_date
                    THEN q.turnover_rate
                    ELSE NULL
                END AS turnover_rate,
                q.pe_ttm,
                q.pb,
                q.total_market_cap
            FROM market_bars mb
            LEFT JOIN daily_quotes q ON mb.code = q.code AND q.trade_date = ?
            LEFT JOIN stock_basic b ON mb.code = b.code
            WHERE mb.period = 'daily'
              AND mb.trade_date >= ?
              AND (? IS NULL OR mb.trade_date <= ?)
              AND mb.code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
              AND mb.code NOT LIKE '300%'
              AND mb.code NOT LIKE '301%'
              AND mb.code NOT LIKE '688%'
              AND mb.code NOT LIKE '689%'
              AND mb.code NOT LIKE '8%'
              AND mb.code NOT LIKE '4%'
              AND COALESCE(
                    CASE
                        WHEN mb.trade_date = q.trade_date AND q.price IS NOT NULL
                        THEN q.price
                        ELSE mb.close
                    END,
                    0
                  ) > 0
            ORDER BY mb.code, mb.trade_date
        """
        df = self.db.query_to_dataframe(
            query,
            (latest_quote_date, min_trade_date, as_of_date, as_of_date),
        )
        if df is None or df.empty:
            logger.warning("[Screener] 股票池查询后无可用数据。")
            return []

        valuation_df = self.db.query_to_dataframe(
            """
            SELECT code, pe_ttm, pb, total_market_cap
            FROM daily_quotes
            WHERE (? IS NULL OR trade_date <= ?)
              AND (pe_ttm IS NOT NULL OR pb IS NOT NULL OR total_market_cap IS NOT NULL)
            ORDER BY code, trade_date
            """,
            (as_of_date, as_of_date),
        )
        if valuation_df is not None and not valuation_df.empty:
            valuation_df = valuation_df.groupby("code", sort=False).tail(1)
            valuation_df = valuation_df.rename(
                columns={
                    "pe_ttm": "valuation_pe_ttm",
                    "pb": "valuation_pb",
                    "total_market_cap": "valuation_total_market_cap",
                }
            )
            df = df.merge(valuation_df, on="code", how="left")
            for col in ("pe_ttm", "pb", "total_market_cap"):
                valuation_col = f"valuation_{col}"
                if valuation_col in df.columns:
                    df[col] = df[col].combine_first(df[valuation_col])
                    df = df.drop(columns=[valuation_col])

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
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for code, bars in df.groupby("code", sort=False):
            min_history_days = int(profile["min_history_days"])
            min_new_stock_history_days = int(profile.get("min_new_stock_history_days", 30) or 30)
            is_new_stock_channel = bool(profile.get("allow_new_stock_channel")) and len(bars) >= min_new_stock_history_days
            if len(bars) < min_history_days and not is_new_stock_channel:
                reject("history_too_short", code, metrics={"bar_count": len(bars), "min_history_days": min_history_days})
                continue

            latest = bars.iloc[-1]
            name = str(latest.get("name") or "")
            if "ST" in name.upper() or "退" in name:
                reject("st_or_delisting", code, name)
                continue

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

            daily_returns = closes.pct_change()
            annual_vol20 = daily_returns.tail(20).std() * np.sqrt(252)
            if pd.notna(annual_vol20) and annual_vol20 > float(profile["max_annual_vol20"]):
                reject("annual_vol20_too_high", code, name, {"annual_vol20": round(float(annual_vol20), 4), "max": profile["max_annual_vol20"]})
                continue

            high60 = bars["high"].tail(60).max()
            low60 = bars["low"].tail(60).min()
            high60_distance = (close / high60 - 1) * 100 if high60 else 0
            low60_distance = (close / low60 - 1) * 100 if low60 else 0

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
            change_pct = float(change_pct) if pd.notna(change_pct) else float(ret1)
            turnover_rate = latest.get("turnover_rate")
            turnover_rate = self._normalize_turnover_rate(turnover_rate)
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
            rsi14 = self._calculate_rsi(closes, 14)
            macd_hist = self._calculate_macd_histogram(closes)
            macd_hist_latest = float(macd_hist.iloc[-1]) if len(macd_hist) and pd.notna(macd_hist.iloc[-1]) else None
            macd_hist_prev = float(macd_hist.iloc[-2]) if len(macd_hist) >= 2 and pd.notna(macd_hist.iloc[-2]) else None
            macd_hist_rising = macd_hist_latest is not None and macd_hist_prev is not None and macd_hist_latest > macd_hist_prev
            macd_bullish_divergence = self._has_macd_bullish_divergence(closes, macd_hist)

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
            trade_profile = self._strategy_trade_profile(strategy_matches)

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
                "preferred_holding_days": trade_profile["preferred_holding_days"],
                "max_holding_days": trade_profile["max_holding_days"],
                "exit_hint": trade_profile["exit_hint"],
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
                    "preferred_holding_days": trade_profile["preferred_holding_days"],
                    "max_holding_days": trade_profile["max_holding_days"],
                    "exit_hint": trade_profile["exit_hint"],
                    "key_metrics": key_metrics,
                    "screen_reason": self._format_screen_reason(
                        strategy_matches,
                        ret5,
                        ret20,
                        pe_ttm,
                        pb,
                        vol_ratio,
                        turnover_rate,
                        low60_distance,
                        trade_profile,
                    ),
                }
            )

        if not picks:
            self.last_audit = self._build_audit_record(df, [], rejected_counts, rejected_stocks, top_n)
            self._write_screener_audit(self.last_audit)
            logger.warning("[Screener] 无股票通过筛选。")
            return []

        ranked = sorted(picks, key=lambda item: item["technical_score"], reverse=True)
        candidates = self._apply_strategy_quota(
            ranked,
            top_n=top_n,
            max_per_industry=int(profile["max_per_industry"]),
            reject=reject,
        )
        if self.theme_scorer is not None:
            candidates = self.theme_scorer.score_candidates(candidates)
            candidates = sorted(candidates, key=lambda item: float(item.get("technical_score") or 0), reverse=True)
        else:
            candidates = self._attach_disabled_theme_score(candidates)

        self.last_audit = self._build_audit_record(df, candidates, rejected_counts, rejected_stocks, top_n)
        self._write_screener_audit(self.last_audit)
        logger.info("[Screener] 已选择 {} 个候选股，使用配置={}，剔除原因分布={}", len(candidates), self.profile_name, dict(rejected_counts.most_common(5)))
        return candidates

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
        strategy_counts: Dict[str, int] = defaultdict(int)
        enforce_caps = len(groups) > 1
        strategy_caps = self._strategy_caps(top_n)

        def try_add(item: Dict[str, Any], count_reject: bool = True, enforce_strategy_cap: bool = True) -> bool:
            if len(selected) >= top_n:
                return False
            code = item.get("code")
            if code in selected_codes:
                return False
            primary_tag = self._get_primary_strategy_tag(item)
            if enforce_caps and enforce_strategy_cap and strategy_counts.get(primary_tag, 0) >= strategy_caps.get(primary_tag, top_n):
                if count_reject and reject:
                    reject(
                        "strategy_cap_exceeded",
                        item.get("code"),
                        item.get("name"),
                        {"strategy": primary_tag, "max_count": strategy_caps.get(primary_tag, top_n)},
                    )
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
            strategy_counts[primary_tag] += 1
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

        for item in ranked:
            if len(selected) >= top_n:
                break
            try_add(item, count_reject=False)

        for item in ranked:
            if len(selected) >= top_n:
                break
            try_add(item, count_reject=False, enforce_strategy_cap=False)

        return sorted(selected, key=lambda item: float(item.get("technical_score") or 0), reverse=True)

    def _strategy_caps(self, top_n: int) -> Dict[str, int]:
        """根据走步回测反馈设置单策略最大占比，避免弱势策略挤占候选池。"""
        from src.screener_profiles import BACKTEST_STRATEGY_PRIORS

        caps: Dict[str, int] = {}
        for strategy, prior in BACKTEST_STRATEGY_PRIORS.items():
            max_share = float(prior.get("max_share", 1.0) or 1.0)
            caps[strategy] = max(1, int(math.ceil(top_n * max_share)))
        return caps

    def _get_primary_strategy_tag(self, item: Dict[str, Any]) -> str:
        """读取候选股主策略标签。"""
        tags = item.get("strategy_tags") or []
        if isinstance(tags, list) and tags:
            return str(tags[0] or "unknown")
        return "unknown"

    @staticmethod
    def _normalize_as_of_date(value: Any) -> str | None:
        """规范化回测截面日期，确保筛选器只读取该日期及以前的数据。"""
        if value in (None, ""):
            return None
        text = str(value).strip().replace("-", "")
        return text if len(text) == 8 and text.isdigit() else None

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
