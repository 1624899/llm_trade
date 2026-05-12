"""分析 QuickFilter 回测中 trend_breakout 的致败特征。

默认读取 outputs/latest_backtest_report.json，按 10 日路径胜率口径
拆分 trend_breakout 的成功/失败样本，并输出诊断字段均值与阈值命中率。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List


DEFAULT_REPORT_PATH = Path("outputs/latest_backtest_report.json")
DIAGNOSTIC_FIELDS = [
    "technical_score",
    "strategy_confidence",
    "ret5",
    "ret10",
    "ret20",
    "volume_ratio",
    "turnover_rate",
    "bias20_ratio",
    "outcome_return_5d",
    "outcome_return_10d",
    "outcome_return_20d",
]


def safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def numeric_values(rows: Iterable[Dict[str, Any]], field: str) -> List[float]:
    values: List[float] = []
    for row in rows:
        value = safe_float(row.get(field))
        if value is not None:
            values.append(value)
    return values


def summarize_field(rows: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
    values = numeric_values(rows, field)
    if not values:
        return {"n": 0, "avg": None, "median": None, "min": None, "max": None}
    return {
        "n": len(values),
        "avg": round(mean(values), 4),
        "median": round(median(values), 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
    }


def hit_rate(rows: List[Dict[str, Any]], field: str, op: str, threshold: float) -> float | None:
    values = numeric_values(rows, field)
    if not values:
        return None
    if op == ">=":
        hits = [value for value in values if value >= threshold]
    elif op == "<=":
        hits = [value for value in values if value <= threshold]
    else:
        hits = [value for value in values if value > threshold]
    return round(len(hits) / len(values), 4)


def analyze(report: Dict[str, Any], horizon: int, threshold: float) -> Dict[str, Any]:
    rows = [
        row
        for row in report.get("quick_filter_evaluations", [])
        if row.get("primary_strategy") == "trend_breakout"
        and safe_float(row.get(f"outcome_return_{horizon}d")) is not None
    ]
    wins = [row for row in rows if safe_float(row.get(f"outcome_return_{horizon}d")) >= threshold]
    losses = [row for row in rows if safe_float(row.get(f"outcome_return_{horizon}d")) < threshold]

    field_summary = {
        field: {
            "win": summarize_field(wins, field),
            "loss": summarize_field(losses, field),
        }
        for field in DIAGNOSTIC_FIELDS
    }
    threshold_summary = {
        "bias20_ratio>=1.12": {
            "win": hit_rate(wins, "bias20_ratio", ">=", 1.12),
            "loss": hit_rate(losses, "bias20_ratio", ">=", 1.12),
        },
        "ret5>=8": {
            "win": hit_rate(wins, "ret5", ">=", 8.0),
            "loss": hit_rate(losses, "ret5", ">=", 8.0),
        },
        "ret10>=15": {
            "win": hit_rate(wins, "ret10", ">=", 15.0),
            "loss": hit_rate(losses, "ret10", ">=", 15.0),
        },
        "volume_ratio>=2.2": {
            "win": hit_rate(wins, "volume_ratio", ">=", 2.2),
            "loss": hit_rate(losses, "volume_ratio", ">=", 2.2),
        },
        "volume_ratio<=0.9": {
            "win": hit_rate(wins, "volume_ratio", "<=", 0.9),
            "loss": hit_rate(losses, "volume_ratio", "<=", 0.9),
        },
    }
    threshold_scan = scan_thresholds(rows, f"outcome_return_{horizon}d", threshold)
    return {
        "horizon": horizon,
        "win_threshold_pct": threshold,
        "sample_count": len(rows),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(len(wins) / len(rows), 4) if rows else None,
        "field_summary": field_summary,
        "threshold_summary": threshold_summary,
        "threshold_scan": threshold_scan,
    }


def scan_thresholds(rows: List[Dict[str, Any]], outcome_key: str, win_threshold: float) -> List[Dict[str, Any]]:
    """扫描常用诊断字段阈值，找出低胜率特征簇。"""
    specs = [
        ("bias20_ratio", ">=", [1.08, 1.10, 1.12]),
        ("ret5", ">=", [6.0, 8.0, 10.0]),
        ("ret10", ">=", [12.0, 15.0, 18.0]),
        ("ret20", ">=", [13.0, 15.0, 18.0]),
        ("volume_ratio", ">=", [1.45, 1.55, 1.70, 2.00]),
        ("volume_ratio", "<=", [0.90, 1.00, 1.10]),
    ]
    total_wins = sum(1 for row in rows if safe_float(row.get(outcome_key)) is not None and safe_float(row.get(outcome_key)) >= win_threshold)
    result: List[Dict[str, Any]] = []
    for field, op, thresholds in specs:
        for threshold in thresholds:
            if op == ">=":
                subset = [row for row in rows if (safe_float(row.get(field)) is not None and safe_float(row.get(field)) >= threshold)]
            else:
                subset = [row for row in rows if (safe_float(row.get(field)) is not None and safe_float(row.get(field)) <= threshold)]
            if len(subset) < 3 or len(subset) >= len(rows):
                continue
            wins = sum(1 for row in subset if safe_float(row.get(outcome_key)) >= win_threshold)
            rest_count = len(rows) - len(subset)
            rest_wins = total_wins - wins
            result.append(
                {
                    "rule": f"{field}{op}{threshold}",
                    "sample_count": len(subset),
                    "win_rate": round(wins / len(subset), 4),
                    "rest_win_rate": round(rest_wins / rest_count, 4) if rest_count else None,
                    "win_rate_gap": round((wins / len(subset)) - (rest_wins / rest_count), 4) if rest_count else None,
                }
            )
    return sorted(result, key=lambda item: (item["win_rate"], -item["sample_count"]))


def main() -> None:
    parser = argparse.ArgumentParser(description="分析 trend_breakout 精筛失败特征")
    parser.add_argument("--report", default=str(DEFAULT_REPORT_PATH), help="回测报告 JSON 路径")
    parser.add_argument("--horizon", type=int, default=10, help="评价周期，默认 10 日")
    parser.add_argument("--threshold", type=float, default=0.5, help="胜率阈值，默认收益 >= 0.5%")
    args = parser.parse_args()

    report_path = Path(args.report)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    result = analyze(report, args.horizon, args.threshold)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
