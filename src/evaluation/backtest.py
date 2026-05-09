"""多策略选股雷达的轻量回测评估模块。"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from loguru import logger

from src.database import StockDatabase


DEFAULT_HOLDING_DAYS = (3, 5, 10, 20)
DEFAULT_STRATEGY_HORIZONS = {
    "dragon_pullback": 10,
    "first_limit_up_breakout": 3,
    "support_pullback": 5,
    "trend_breakout": 20,
    "momentum_leader": 20,
}


class BacktestEngine:
    """记录选股信号快照，并用后续 K 线表现校准策略权重。"""

    def __init__(
        self,
        db: StockDatabase | None = None,
        output_path: str | Path = "outputs/latest_backtest_report.json",
    ):
        self.db = db or StockDatabase()
        self.output_path = Path(output_path)

    def record_signal_snapshot(
        self,
        candidates: List[Dict[str, Any]],
        *,
        signal_date: str | None = None,
        market_regime: Dict[str, Any] | None = None,
        source: str = "picking_prefilter",
    ) -> int:
        """保存本轮规则海选快照，避免后续只拿当前数据库状态倒推历史。"""
        if not candidates:
            return 0
        signal_date = self._normalize_trade_date(signal_date) or self._latest_trade_date()
        if not signal_date:
            logger.warning("[Backtest] 未找到可用交易日，跳过候选快照记录。")
            return 0

        rows = []
        created_at = datetime.now().isoformat(timespec="seconds")
        for item in candidates:
            code = str(item.get("code") or "").zfill(6)
            if len(code) != 6 or not code.isdigit():
                continue
            strategy_tags = item.get("strategy_tags") if isinstance(item.get("strategy_tags"), list) else []
            rows.append(
                {
                    "signal_date": signal_date,
                    "code": code,
                    "name": item.get("name"),
                    "source": source,
                    "primary_strategy": self._primary_strategy(item),
                    "strategy_tags": json.dumps(strategy_tags, ensure_ascii=False),
                    "strategy_confidence": self._safe_float(item.get("strategy_confidence")),
                    "technical_score": self._safe_float(item.get("technical_score")),
                    "theme_score": self._safe_float(item.get("theme_score")),
                    "key_metrics": json.dumps(item.get("key_metrics") or {}, ensure_ascii=False, default=str),
                    "market_regime": json.dumps(market_regime or {}, ensure_ascii=False, default=str),
                    "created_at": created_at,
                }
            )

        if not rows:
            return 0
        df = pd.DataFrame(rows)
        ok = self.db.upsert_dataframe(
            "backtest_signal_snapshots",
            df,
            key_columns=["signal_date", "code", "source"],
        )
        return len(rows) if ok else 0

    def build_weight_reference(
        self,
        candidates: List[Dict[str, Any]] | None = None,
        *,
        market_regime: Dict[str, Any] | None = None,
        holding_days: Sequence[int] = DEFAULT_HOLDING_DAYS,
        min_samples: int = 3,
        source: str = "picking_prefilter",
    ) -> Dict[str, Any]:
        """生成给精筛 Agent 使用的回测权重参考。"""
        report = self.evaluate_signal_snapshots(
            holding_days=holding_days,
            source=source,
            min_completed_holding_days=min(holding_days) if holding_days else 3,
        )
        strategy_stats = report.get("strategy_stats", {})
        adjustments = self._build_strategy_adjustments(strategy_stats, min_samples=min_samples)
        candidate_adjustments = self._build_candidate_adjustments(candidates or [], adjustments)
        reference = {
            "enabled": True,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source": source,
            "sample_count": report.get("evaluated_count", 0),
            "latest_signal_date": report.get("latest_signal_date"),
            "holding_days": list(holding_days),
            "market_regime": market_regime or {},
            "strategy_stats": strategy_stats,
            "strategy_weight_adjustments": adjustments,
            "candidate_adjustments": candidate_adjustments,
            "summary": self._build_summary(report, adjustments),
        }
        self._save_report(reference)
        return reference

    def run_walk_forward_backtest(
        self,
        *,
        as_of_dates: Sequence[str] | None = None,
        top_n: int = 20,
        lookback_days: int = 90,
        holding_days: Sequence[int] = DEFAULT_HOLDING_DAYS,
        step: int = 5,
        max_windows: int = 20,
        profile: str = "balanced",
        source: str = "walk_forward_masked",
        screener_config_path: str | Path | None = None,
    ) -> Dict[str, Any]:
        """遮盖未来数据做走步回测：只用截面日及以前信息选股，再揭开后续行情评估。"""
        holding_days = tuple(sorted({int(day) for day in holding_days if int(day) > 0})) or DEFAULT_HOLDING_DAYS
        dates = [self._normalize_trade_date(date) for date in (as_of_dates or [])]
        dates = [date for date in dates if date]
        if not dates:
            dates = self._select_walk_forward_dates(
                lookback_days=lookback_days,
                max_holding_days=max(holding_days),
                step=step,
                limit=max_windows,
            )
        if not dates:
            return self._empty_report("no_eligible_walk_forward_dates")

        # 延迟导入，避免评估模块被数据库初始化时产生不必要依赖。
        from src.stock_screener import StockScreener

        total_saved = 0
        windows: List[Dict[str, Any]] = []
        self._clear_walk_forward_snapshots(source=source, signal_dates=dates)
        for as_of_date in dates:
            screener = StockScreener(db=self.db, profile=profile, config_path=screener_config_path)
            screener.theme_scorer = None
            candidates = screener.run_technical_screening(
                top_n=top_n,
                lookback_days=lookback_days,
                as_of_date=as_of_date,
                apply_backtest_weights=False,
            )
            saved = self.record_signal_snapshot(
                candidates,
                signal_date=as_of_date,
                market_regime={"regime": "walk_forward", "profile": profile, "as_of_date": as_of_date},
                source=source,
            )
            total_saved += saved
            windows.append(
                {
                    "as_of_date": as_of_date,
                    "candidate_count": len(candidates),
                    "saved_count": saved,
                    "codes": [str(item.get("code") or "").zfill(6) for item in candidates],
                }
            )

        report = self.evaluate_signal_snapshots(
            holding_days=holding_days,
            source=source,
            min_completed_holding_days=min(holding_days),
        )
        report.update(
            {
                "mode": "walk_forward_masked",
                "source": source,
                "window_count": len(windows),
                "saved_signal_count": total_saved,
                "windows": windows,
            }
        )
        report["strategy_weight_adjustments"] = self._build_strategy_adjustments(
            report.get("strategy_stats", {}),
            min_samples=3,
        )
        report["summary"] = self._build_summary(report, report["strategy_weight_adjustments"])
        self._save_report(report)
        return report

    def evaluate_signal_snapshots(
        self,
        *,
        holding_days: Sequence[int] = DEFAULT_HOLDING_DAYS,
        source: str = "picking_prefilter",
        min_completed_holding_days: int = 3,
    ) -> Dict[str, Any]:
        """评估已保存信号在后续若干交易日的表现。"""
        holding_days = tuple(sorted({int(day) for day in holding_days if int(day) > 0}))
        if not holding_days:
            holding_days = DEFAULT_HOLDING_DAYS
        snapshots = self._load_snapshots(source=source)
        if snapshots.empty:
            return self._empty_report("no_signal_snapshots")

        codes = sorted(set(snapshots["code"].astype(str).str.zfill(6)))
        min_signal_date = str(snapshots["signal_date"].min())
        bars = self._load_future_bars(codes, min_signal_date)
        if bars.empty:
            return self._empty_report("no_future_bars")

        evaluations: List[Dict[str, Any]] = []
        latest_signal_date = str(snapshots["signal_date"].max())
        for _, signal in snapshots.iterrows():
            row = self._evaluate_single_signal(signal, bars, holding_days)
            if not row:
                continue
            if int(row.get("completed_holding_days") or 0) >= min_completed_holding_days:
                evaluations.append(row)

        if not evaluations:
            return self._empty_report("no_completed_signals", latest_signal_date=latest_signal_date)

        strategy_stats = self._summarize_by_strategy(evaluations, holding_days)
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source": source,
            "snapshot_count": int(len(snapshots)),
            "evaluated_count": int(len(evaluations)),
            "latest_signal_date": latest_signal_date,
            "holding_days": list(holding_days),
            "strategy_stats": strategy_stats,
            "evaluations": evaluations,
        }

    def _evaluate_single_signal(
        self,
        signal: pd.Series,
        bars: pd.DataFrame,
        holding_days: Sequence[int],
    ) -> Dict[str, Any]:
        code = str(signal.get("code") or "").zfill(6)
        signal_date = str(signal.get("signal_date") or "")
        stock_bars = bars[(bars["code"] == code) & (bars["trade_date"] >= signal_date)].sort_values("trade_date")
        if len(stock_bars) < 2:
            return {}
        entry = stock_bars.iloc[0]
        entry_close = self._safe_float(entry.get("close"))
        if not entry_close or entry_close <= 0:
            return {}

        returns: Dict[str, float] = {}
        completed = len(stock_bars) - 1
        for day in holding_days:
            if completed < day:
                continue
            future_close = self._safe_float(stock_bars.iloc[day].get("close"))
            if future_close is None:
                continue
            returns[f"return_{day}d"] = round((future_close / entry_close - 1) * 100, 4)
        if not returns:
            return {}

        return {
            "signal_date": signal_date,
            "code": code,
            "name": signal.get("name"),
            "primary_strategy": signal.get("primary_strategy") or "unknown",
            "strategy_confidence": self._safe_float(signal.get("strategy_confidence")),
            "technical_score": self._safe_float(signal.get("technical_score")),
            "theme_score": self._safe_float(signal.get("theme_score")),
            "entry_close": entry_close,
            "completed_holding_days": completed,
            **returns,
        }

    def _summarize_by_strategy(
        self,
        evaluations: List[Dict[str, Any]],
        holding_days: Sequence[int],
    ) -> Dict[str, Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in evaluations:
            grouped[str(item.get("primary_strategy") or "unknown")].append(item)

        stats: Dict[str, Dict[str, Any]] = {}
        for strategy, rows in grouped.items():
            strategy_stat: Dict[str, Any] = {"sample_count": len(rows)}
            for day in holding_days:
                key = f"return_{day}d"
                values = [float(row[key]) for row in rows if row.get(key) is not None]
                if not values:
                    continue
                strategy_stat[key] = {
                    "sample_count": len(values),
                    "avg_return_pct": round(sum(values) / len(values), 4),
                    "win_rate": round(sum(1 for value in values if value > 0) / len(values), 4),
                    "max_drawdown_proxy": round(min(values), 4),
                    "best_return_pct": round(max(values), 4),
                }
            preferred_day = self._preferred_horizon_for_strategy(strategy, holding_days, strategy_stat)
            preferred_key = f"return_{preferred_day}d"
            preferred = strategy_stat.get(preferred_key) or next(
                (strategy_stat.get(f"return_{day}d") for day in reversed(holding_days) if strategy_stat.get(f"return_{day}d")),
                {},
            )
            strategy_stat["preferred_horizon"] = preferred_day
            strategy_stat["effect_score"] = self._effect_score(preferred)
            stats[strategy] = strategy_stat
        return stats

    def _preferred_horizon_for_strategy(
        self,
        strategy: str,
        holding_days: Sequence[int],
        strategy_stat: Dict[str, Any],
    ) -> int:
        """根据策略类型选择主评价周期，避免所有信号都被 10 日收益牵着走。"""
        available_days = [int(day) for day in holding_days if strategy_stat.get(f"return_{int(day)}d")]
        if not available_days:
            return 10 if 10 in holding_days else max(holding_days)

        preferred = DEFAULT_STRATEGY_HORIZONS.get(strategy)
        if preferred in available_days:
            return preferred
        if preferred is not None:
            return min(available_days, key=lambda day: abs(day - preferred))
        return 10 if 10 in available_days else max(available_days)

    def _build_strategy_adjustments(
        self,
        strategy_stats: Dict[str, Dict[str, Any]],
        *,
        min_samples: int,
    ) -> Dict[str, Dict[str, Any]]:
        adjustments: Dict[str, Dict[str, Any]] = {}
        for strategy, stats in strategy_stats.items():
            sample_count = int(stats.get("sample_count") or 0)
            effect_score = self._safe_float(stats.get("effect_score")) or 0.0
            if sample_count < min_samples:
                multiplier = 1.0
                confidence = "low"
                reason = f"样本不足(n={sample_count} < {min_samples})，保持中性权重。"
            else:
                multiplier = max(0.75, min(1.25, 1.0 + effect_score / 40.0))
                confidence = "medium" if sample_count < min_samples * 3 else "high"
                reason = "历史表现较优，适度加权。" if multiplier > 1.03 else "历史表现偏弱，适度降权。" if multiplier < 0.97 else "历史表现接近中性。"
            adjustments[strategy] = {
                "multiplier": round(multiplier, 4),
                "effect_score": round(effect_score, 4),
                "sample_count": sample_count,
                "confidence": confidence,
                "reason": reason,
            }
        return adjustments

    def _build_candidate_adjustments(
        self,
        candidates: List[Dict[str, Any]],
        strategy_adjustments: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for item in candidates:
            code = str(item.get("code") or "").zfill(6)
            if len(code) != 6:
                continue
            primary = self._primary_strategy(item)
            adjustment = strategy_adjustments.get(primary, {})
            multiplier = self._safe_float(adjustment.get("multiplier")) or 1.0
            result[code] = {
                "primary_strategy": primary,
                "multiplier": round(multiplier, 4),
                "score_bonus": round((multiplier - 1.0) * 20.0, 4),
                "reason": adjustment.get("reason") or "暂无历史样本，保持中性。",
            }
        return result

    def _build_summary(self, report: Dict[str, Any], adjustments: Dict[str, Dict[str, Any]]) -> str:
        sample_count = int(report.get("evaluated_count") or 0)
        if sample_count <= 0:
            return "暂无完成持有周期的历史信号，回测权重保持中性。"
        reliable = {
            strategy: adjustment
            for strategy, adjustment in adjustments.items()
            if adjustment.get("confidence") != "low"
        }
        ranked = sorted(
            reliable.items(),
            key=lambda pair: float(pair[1].get("effect_score") or 0),
            reverse=True,
        )
        low_sample_items = [
            f"{strategy}(n={adjustment.get('sample_count')})"
            for strategy, adjustment in adjustments.items()
            if adjustment.get("confidence") == "low"
        ]
        low_sample_count = len(low_sample_items)
        low_sample_note = (
            f"；{low_sample_count} 个策略样本不足未参与优劣排名：{', '.join(low_sample_items)}"
            if low_sample_count > 0
            else ""
        )
        if not ranked:
            return f"已评估 {sample_count} 条历史信号；所有策略样本仍不足，暂不判断相对优劣。"
        best = ranked[0] if ranked else ("unknown", {"effect_score": 0})
        worst = ranked[-1] if ranked else ("unknown", {"effect_score": 0})
        return (
            f"已评估 {sample_count} 条历史信号；"
            f"相对较优策略 {best[0]}(effect={best[1].get('effect_score')})，"
            f"相对偏弱策略 {worst[0]}(effect={worst[1].get('effect_score')})"
            f"{low_sample_note}。"
        )

    def _load_snapshots(self, *, source: str) -> pd.DataFrame:
        return self.db.query_to_dataframe(
            """
            SELECT *
            FROM backtest_signal_snapshots
            WHERE source = ?
            ORDER BY signal_date, code
            """,
            (source,),
        )

    def _load_future_bars(self, codes: Iterable[str], min_signal_date: str) -> pd.DataFrame:
        code_list = [str(code).zfill(6) for code in codes]
        if not code_list:
            return pd.DataFrame()
        placeholders = ",".join(["?"] * len(code_list))
        return self.db.query_to_dataframe(
            f"""
            SELECT code, trade_date, close
            FROM market_bars
            WHERE period = 'daily'
              AND trade_date >= ?
              AND code IN ({placeholders})
            ORDER BY code, trade_date
            """,
            tuple([min_signal_date] + code_list),
        )

    def _clear_walk_forward_snapshots(self, *, source: str, signal_dates: Sequence[str]) -> None:
        """清理本次走步窗口的旧快照，避免多轮调参后旧候选残留污染新报告。"""
        dates = [self._normalize_trade_date(date) for date in signal_dates]
        dates = [date for date in dates if date]
        if not dates:
            return
        placeholders = ",".join(["?"] * len(dates))
        ok = self.db.execute_non_query(
            f"""
            DELETE FROM backtest_signal_snapshots
            WHERE source = ?
              AND signal_date IN ({placeholders})
            """,
            tuple([source] + dates),
        )
        if not ok:
            logger.warning("[Backtest] 清理旧走步快照失败，source={}", source)

    def _latest_trade_date(self) -> str | None:
        df = self.db.query_to_dataframe(
            """
            SELECT MAX(trade_date) AS trade_date
            FROM market_bars
            WHERE period = 'daily'
            """
        )
        if df.empty:
            return None
        return self._normalize_trade_date(df.iloc[0].get("trade_date"))

    def _select_walk_forward_dates(
        self,
        *,
        lookback_days: int,
        max_holding_days: int,
        step: int,
        limit: int,
    ) -> List[str]:
        df = self.db.query_to_dataframe(
            """
            SELECT DISTINCT trade_date
            FROM market_bars
            WHERE period = 'daily'
            ORDER BY trade_date
            """
        )
        if df.empty:
            return []
        dates = [str(value) for value in df["trade_date"].tolist()]
        start = max(0, int(lookback_days) - 1)
        end = len(dates) - max(1, int(max_holding_days)) - 1
        if end < start:
            return []
        selected = dates[start : end + 1 : max(1, int(step))]
        if limit > 0:
            selected = selected[-int(limit):]
        return selected

    def _save_report(self, report: Dict[str, Any]) -> None:
        try:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        except Exception as exc:
            logger.warning("[Backtest] 保存回测报告失败: {}", exc)

    def _empty_report(self, reason: str, latest_signal_date: str | None = None) -> Dict[str, Any]:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
            "snapshot_count": 0,
            "evaluated_count": 0,
            "latest_signal_date": latest_signal_date,
            "strategy_stats": {},
            "evaluations": [],
        }

    def _primary_strategy(self, item: Dict[str, Any]) -> str:
        tags = item.get("strategy_tags") or []
        if isinstance(tags, list) and tags:
            return str(tags[0] or "unknown")
        return str(item.get("primary_strategy") or "unknown")

    def _effect_score(self, stat: Dict[str, Any]) -> float:
        if not isinstance(stat, dict) or not stat:
            return 0.0
        avg_return = self._safe_float(stat.get("avg_return_pct")) or 0.0
        win_rate = self._safe_float(stat.get("win_rate")) or 0.0
        drawdown = self._safe_float(stat.get("max_drawdown_proxy")) or 0.0
        return avg_return * 2.0 + (win_rate - 0.5) * 20.0 + min(0.0, drawdown) * 0.25

    @staticmethod
    def _normalize_trade_date(value: Any) -> str | None:
        if value in (None, ""):
            return None
        text = str(value).strip().replace("-", "")
        return text if len(text) == 8 and text.isdigit() else None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
