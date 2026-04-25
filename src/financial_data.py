"""财务报表数据接入与摘要生成。

本模块通过 AKShare 的东方财富接口抓取 A 股财务报表，并将原始宽表整理成适合
Prompt 使用的紧凑摘要。它刻意独立于 LLM Agent，便于测试，也方便后续被筛选器
或持仓检查逻辑复用。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List

import akshare as ak
import pandas as pd
from loguru import logger


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_value(row: pd.Series | Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and not pd.isna(value):
            return value
    return None


def _format_pct(value: float | None) -> str:
    return "N/A" if value is None else f"{value:.2f}%"


def _format_money(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value / 1e8:.2f}亿"


@dataclass
class FinancialStatementBundle:
    indicators: pd.DataFrame
    profit: pd.DataFrame
    balance: pd.DataFrame
    cash_flow: pd.DataFrame


class FinancialDataProvider:
    """抓取并归一化 A 股财务报表指标。"""

    def fetch_financial_bundle(self, code: str) -> FinancialStatementBundle:
        statement_symbol = self.to_statement_symbol(code)
        indicator_symbol = self.to_indicator_symbol(code)
        logger.info("[FinancialData] fetching Eastmoney financial statements for {}", code)
        return FinancialStatementBundle(
            indicators=self._safe_fetch(
                "financial indicators",
                ak.stock_financial_analysis_indicator_em,
                symbol=indicator_symbol,
                indicator="按报告期",
            ),
            profit=self._safe_fetch(
                "profit sheet",
                ak.stock_profit_sheet_by_report_em,
                symbol=statement_symbol,
            ),
            balance=self._safe_fetch(
                "balance sheet",
                ak.stock_balance_sheet_by_report_em,
                symbol=statement_symbol,
            ),
            cash_flow=self._safe_fetch(
                "cash flow sheet",
                ak.stock_cash_flow_sheet_by_report_em,
                symbol=statement_symbol,
            ),
        )

    def fetch_financial_metrics(self, code: str, periods: int = 8) -> pd.DataFrame:
        bundle = self.fetch_financial_bundle(code)
        return self.normalize_financial_metrics(code, bundle, periods=periods)

    def format_financial_summary(self, code: str, periods: int = 8) -> str:
        try:
            metrics = self.fetch_financial_metrics(code, periods=periods)
        except Exception as exc:
            logger.warning("[FinancialData] failed to fetch financial summary for {}: {}", code, exc)
            return ""
        return self.format_metrics_for_prompt(metrics)

    def normalize_financial_metrics(
        self,
        code: str,
        bundle: FinancialStatementBundle,
        periods: int = 8,
    ) -> pd.DataFrame:
        frames = {
            "indicators": bundle.indicators,
            "profit": bundle.profit,
            "balance": bundle.balance,
            "cash_flow": bundle.cash_flow,
        }
        for name, frame in frames.items():
            if frame is None or frame.empty:
                frames[name] = pd.DataFrame()
                continue
            frame = frame.copy()
            frame["REPORT_DATE"] = pd.to_datetime(frame.get("REPORT_DATE"), errors="coerce")
            frame = frame.dropna(subset=["REPORT_DATE"]).sort_values("REPORT_DATE", ascending=False)
            frames[name] = frame

        report_dates = sorted(
            {
                date
                for frame in frames.values()
                if not frame.empty
                for date in frame["REPORT_DATE"].dropna().tolist()
            },
            reverse=True,
        )[:periods]

        rows: List[Dict[str, Any]] = []
        for report_date in report_dates:
            indicator = self._row_for_report_date(frames["indicators"], report_date)
            profit = self._row_for_report_date(frames["profit"], report_date)
            balance = self._row_for_report_date(frames["balance"], report_date)
            cash = self._row_for_report_date(frames["cash_flow"], report_date)

            revenue = _to_float(_first_value(indicator, ["TOTALOPERATEREVE", "TOTAL_OPERATE_INCOME"]))
            if revenue is None:
                revenue = _to_float(_first_value(profit, ["TOTAL_OPERATE_INCOME", "OPERATE_INCOME"]))
            parent_netprofit = _to_float(_first_value(indicator, ["PARENTNETPROFIT"]))
            if parent_netprofit is None:
                parent_netprofit = _to_float(_first_value(profit, ["PARENT_NETPROFIT", "NETPROFIT"]))
            operating_cash_flow = _to_float(_first_value(cash, ["NETCASH_OPERATE", "NETCASH_OPERATENOTE"]))
            cash_to_profit = None
            if parent_netprofit not in (None, 0) and operating_cash_flow is not None:
                cash_to_profit = operating_cash_flow / parent_netprofit

            rows.append(
                {
                    "code": str(code).zfill(6),
                    "report_date": pd.Timestamp(report_date).strftime("%Y%m%d"),
                    "report_name": _first_value(indicator, ["REPORT_DATE_NAME"])
                    or _first_value(profit, ["REPORT_DATE_NAME"])
                    or pd.Timestamp(report_date).strftime("%Y-%m-%d"),
                    "notice_date": self._format_optional_date(
                        _first_value(indicator, ["NOTICE_DATE"]) or _first_value(profit, ["NOTICE_DATE"])
                    ),
                    "report_type": _first_value(indicator, ["REPORT_TYPE"]) or _first_value(profit, ["REPORT_TYPE"]),
                    "revenue": revenue,
                    "revenue_yoy": _to_float(_first_value(indicator, ["TOTALOPERATEREVETZ", "DJD_TOI_YOY"]))
                    or _to_float(_first_value(profit, ["TOTAL_OPERATE_INCOME_YOY", "OPERATE_INCOME_YOY"])),
                    "parent_netprofit": parent_netprofit,
                    "parent_netprofit_yoy": _to_float(_first_value(indicator, ["PARENTNETPROFITTZ", "DJD_DPNP_YOY"]))
                    or _to_float(_first_value(profit, ["PARENT_NETPROFIT_YOY", "NETPROFIT_YOY"])),
                    "deduct_parent_netprofit": _to_float(_first_value(indicator, ["KCFJCXSYJLR"]))
                    or _to_float(_first_value(profit, ["DEDUCT_PARENT_NETPROFIT"])),
                    "deduct_parent_netprofit_yoy": _to_float(_first_value(indicator, ["KCFJCXSYJLRTZ", "DJD_DEDUCTDPNP_YOY"]))
                    or _to_float(_first_value(profit, ["DEDUCT_PARENT_NETPROFIT_YOY"])),
                    "gross_margin": _to_float(_first_value(indicator, ["XSMLL", "MLR"])),
                    "net_margin": _to_float(_first_value(indicator, ["XSJLL"])),
                    "roe": _to_float(_first_value(indicator, ["ROEJQ"])),
                    "roic": _to_float(_first_value(indicator, ["ROIC"])),
                    "debt_to_assets": _to_float(_first_value(indicator, ["ZCFZL"]))
                    or _to_float(_first_value(balance, ["TOTAL_LIABILITIES_YOY"])),
                    "current_ratio": _to_float(_first_value(indicator, ["LD"])),
                    "quick_ratio": _to_float(_first_value(indicator, ["SD"])),
                    "cash_ratio": _to_float(_first_value(indicator, ["XJLLB", "CASH_RATIO"])),
                    "operating_cash_flow": operating_cash_flow,
                    "operating_cash_flow_yoy": _to_float(_first_value(cash, ["NETCASH_OPERATE_YOY", "NETCASH_OPERATENOTE_YOY"])),
                    "cash_to_profit": cash_to_profit,
                    "total_assets": _to_float(_first_value(balance, ["TOTAL_ASSETS"])),
                    "total_liabilities": _to_float(_first_value(balance, ["TOTAL_LIABILITIES"])),
                    "source": "eastmoney_akshare",
                    "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        return pd.DataFrame(rows)

    def format_metrics_for_prompt(self, metrics: pd.DataFrame) -> str:
        if metrics is None or metrics.empty:
            return ""

        latest = metrics.iloc[0].to_dict()
        lines = [
            "东方财富财务数据摘要：",
            (
                f"- 最新报告期 {latest.get('report_name') or latest.get('report_date')}: "
                f"营收 {_format_money(_to_float(latest.get('revenue')))}, "
                f"营收同比 {_format_pct(_to_float(latest.get('revenue_yoy')))}, "
                f"归母净利 {_format_money(_to_float(latest.get('parent_netprofit')))}, "
                f"归母净利同比 {_format_pct(_to_float(latest.get('parent_netprofit_yoy')))}。"
            ),
            (
                f"- 盈利质量: 毛利率 {_format_pct(_to_float(latest.get('gross_margin')))}, "
                f"净利率 {_format_pct(_to_float(latest.get('net_margin')))}, "
                f"ROE {_format_pct(_to_float(latest.get('roe')))}, "
                f"ROIC {_format_pct(_to_float(latest.get('roic')))}。"
            ),
            (
                f"- 现金流/负债: 经营现金流 {_format_money(_to_float(latest.get('operating_cash_flow')))}, "
                f"经营现金流/归母净利 {self._format_ratio(_to_float(latest.get('cash_to_profit')))}, "
                f"资产负债率 {_format_pct(_to_float(latest.get('debt_to_assets')))}, "
                f"流动比率 {self._format_ratio(_to_float(latest.get('current_ratio')))}。"
            ),
        ]

        lines.append("- 近几期趋势：")
        for _, row in metrics.head(6).iterrows():
            lines.append(
                "  "
                f"{row.get('report_name') or row.get('report_date')}: "
                f"营收同比 {_format_pct(_to_float(row.get('revenue_yoy')))}, "
                f"归母净利同比 {_format_pct(_to_float(row.get('parent_netprofit_yoy')))}, "
                f"扣非净利同比 {_format_pct(_to_float(row.get('deduct_parent_netprofit_yoy')))}, "
                f"ROE {_format_pct(_to_float(row.get('roe')))}, "
                f"现金流/利润 {self._format_ratio(_to_float(row.get('cash_to_profit')))}"
            )
        return "\n".join(lines)

    @staticmethod
    def to_statement_symbol(code: str) -> str:
        code = str(code).strip().upper().replace(".SH", "").replace(".SZ", "").zfill(6)
        if code.startswith(("600", "601", "603", "605", "688", "689")):
            return f"SH{code}"
        return f"SZ{code}"

    @staticmethod
    def to_indicator_symbol(code: str) -> str:
        code = str(code).strip().upper().replace(".SH", "").replace(".SZ", "").zfill(6)
        if code.startswith(("600", "601", "603", "605", "688", "689")):
            return f"{code}.SH"
        return f"{code}.SZ"

    @staticmethod
    def _safe_fetch(label: str, func: Any, **kwargs: Any) -> pd.DataFrame:
        try:
            df = func(**kwargs)
        except Exception as exc:
            logger.warning("[FinancialData] {} fetch failed: {}", label, exc)
            return pd.DataFrame()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    @staticmethod
    def _row_for_report_date(frame: pd.DataFrame, report_date: pd.Timestamp) -> pd.Series:
        if frame is None or frame.empty:
            return pd.Series(dtype=object)
        matched = frame[frame["REPORT_DATE"] == report_date]
        if matched.empty:
            return pd.Series(dtype=object)
        return matched.iloc[0]

    @staticmethod
    def _format_optional_date(value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return pd.Timestamp(parsed).strftime("%Y%m%d")

    @staticmethod
    def _format_ratio(value: float | None) -> str:
        return "N/A" if value is None else f"{value:.2f}"
