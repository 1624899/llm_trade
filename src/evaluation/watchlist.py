"""Watchlist management for recommended stocks.

The watchlist is a candidate pool, not a simulated position book. It stores the
latest recommendation context that the trading agent can use later.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

from src.database import StockDatabase
from src.quote_sources import fetch_latest_prices


class Watchlist:
    def __init__(self, db: Optional[StockDatabase] = None, max_items: int = 10):
        self.db = db or StockDatabase()
        self.max_items = max(1, int(max_items or 10))

    def upsert_recommendations(
        self,
        selected_codes: Iterable[str],
        stock_profiles: Dict[str, Dict[str, Any]],
        final_report: str = "",
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Add or update selected recommendations in the watchlist."""
        count = 0
        for raw_code in selected_codes or []:
            code = str(raw_code).zfill(6)
            profile = stock_profiles.get(code)
            if not profile:
                logger.warning("[Watchlist] selected code {} not found in stock profiles", code)
                continue
            if self.upsert_item(profile, final_report=final_report, macro_context=macro_context):
                count += 1
        self.prune_to_limit()
        return count

    def upsert_item(
        self,
        profile: Dict[str, Any],
        final_report: str = "",
        macro_context: Optional[Dict[str, Any]] = None,
        source: str = "pick",
    ) -> bool:
        asset = profile.get("asset_info") or {}
        code = str(asset.get("code") or profile.get("code") or "").zfill(6)
        if not re.fullmatch(r"\d{6}", code):
            logger.warning("[Watchlist] invalid code skipped: {}", code)
            return False

        name = str(asset.get("name") or profile.get("name") or code)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_price = self._get_current_price(code)
        existing = self.get_item(code)
        entry_price = self._to_float(existing.get("entry_price")) if existing else current_price
        return_pct = self._return_pct(current_price, entry_price)
        tier = self._infer_tier(code, final_report)
        reason = self._compose_reason(profile)

        sql = """
            INSERT INTO watchlist_items
            (code, name, tier, watch_status, source, added_at, updated_at, entry_price,
             current_price, return_pct, expected_return_pct, recommend_reason,
             fundamental_analysis, technical_analysis, news_risk_analysis, macro_context, remove_reason)
            VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                tier=excluded.tier,
                watch_status='ACTIVE',
                source=excluded.source,
                updated_at=excluded.updated_at,
                current_price=excluded.current_price,
                return_pct=excluded.return_pct,
                expected_return_pct=excluded.expected_return_pct,
                recommend_reason=excluded.recommend_reason,
                fundamental_analysis=excluded.fundamental_analysis,
                technical_analysis=excluded.technical_analysis,
                news_risk_analysis=excluded.news_risk_analysis,
                macro_context=excluded.macro_context,
                remove_reason=NULL
        """
        return self.db.execute_non_query(
            sql,
            (
                code,
                name,
                tier,
                source,
                now,
                now,
                entry_price,
                current_price,
                return_pct,
                self._infer_expected_return(final_report, code),
                reason,
                str(profile.get("fundamental_analysis") or ""),
                str(profile.get("technical_analysis") or ""),
                str(profile.get("news_risk_analysis") or ""),
                json.dumps(macro_context or {}, ensure_ascii=False, default=str),
            ),
        )

    def refresh_prices(self) -> None:
        items = self.list_active()
        if not items:
            return
        codes = [item["code"] for item in items]
        prices = self._get_latest_prices_batch(codes)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in items:
            code = str(item["code"]).zfill(6)
            price = self._to_float(prices.get(code))
            entry_price = self._to_float(item.get("entry_price"))
            if price is None or price <= 0:
                continue
            self.db.execute_non_query(
                "UPDATE watchlist_items SET current_price = ?, return_pct = ?, updated_at = ? WHERE id = ?",
                (price, self._return_pct(price, entry_price), now, int(item["id"])),
            )

    def list_active(self) -> List[Dict[str, Any]]:
        df = self.db.query_to_dataframe(
            """
            SELECT *
            FROM watchlist_items
            WHERE watch_status = 'ACTIVE'
            ORDER BY
                CASE tier
                    WHEN '强推荐' THEN 0
                    WHEN '配置/轻仓验证' THEN 1
                    WHEN '观察' THEN 2
                    ELSE 3
                END,
                updated_at DESC,
                id DESC
            """
        )
        if df.empty:
            return []
        return df.to_dict("records")

    def get_item(self, code: str) -> Dict[str, Any]:
        df = self.db.query_to_dataframe(
            "SELECT * FROM watchlist_items WHERE code = ? LIMIT 1",
            (str(code).zfill(6),),
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()

    def prune_to_limit(self) -> None:
        active = self.list_active()
        overflow = active[self.max_items :]
        if not overflow:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in overflow:
            self.db.execute_non_query(
                """
                UPDATE watchlist_items
                SET watch_status = 'REMOVED', remove_reason = ?, updated_at = ?
                WHERE id = ?
                """,
                ("观察仓超过上限，移出优先级较低标的", now, int(item["id"])),
            )

    def _get_current_price(self, code: str) -> float:
        prices = self._get_latest_prices_batch([code])
        return float(prices.get(str(code).zfill(6), 0) or 0)

    def _get_latest_prices_batch(self, codes: List[str]) -> Dict[str, float]:
        normalized = [str(code).zfill(6) for code in codes]
        prices = fetch_latest_prices(normalized)
        missing = [code for code in normalized if code not in prices]
        if missing:
            placeholders = ",".join(["?"] * len(missing))
            df = self.db.query_to_dataframe(
                f"""
                SELECT q.code, q.price
                FROM daily_quotes q
                JOIN (
                    SELECT code, MAX(trade_date) AS trade_date
                    FROM daily_quotes
                    WHERE code IN ({placeholders})
                    GROUP BY code
                ) latest ON q.code = latest.code AND q.trade_date = latest.trade_date
                """,
                tuple(missing),
            )
            if not df.empty:
                prices.update(dict(zip(df["code"].astype(str).str.zfill(6), df["price"].astype(float))))
        return {str(code).zfill(6): float(price) for code, price in prices.items() if price}

    @staticmethod
    def _compose_reason(profile: Dict[str, Any]) -> str:
        parts = []
        for key, label in [
            ("fundamental_analysis", "基本面分析"),
            ("technical_analysis", "技术面分析"),
            ("news_risk_analysis", "风控与资讯"),
        ]:
            value = profile.get(key)
            if value:
                parts.append(f"【{label}】\n{value}")
        return "\n\n".join(parts)

    @staticmethod
    def _infer_tier(code: str, final_report: str) -> str:
        if not final_report:
            return "配置/轻仓验证"
        idx = final_report.find(code)
        if idx < 0:
            return "配置/轻仓验证"
        window = final_report[max(0, idx - 120) : idx + 220]
        for tier in ("强推荐", "配置/轻仓验证", "观察", "不推荐"):
            if tier in window:
                return tier
        return "配置/轻仓验证"

    @staticmethod
    def _infer_expected_return(final_report: str, code: str) -> float | None:
        idx = final_report.find(code) if final_report else -1
        if idx < 0:
            return None
        window = final_report[max(0, idx - 120) : idx + 260]
        matches = re.findall(r"(\d+(?:\.\d+)?)\s*%", window)
        if not matches:
            return None
        values = [float(item) for item in matches]
        return max(values) if values else None

    @staticmethod
    def _return_pct(current_price: float | None, entry_price: float | None) -> float | None:
        if not current_price or not entry_price:
            return None
        return round((float(current_price) / float(entry_price) - 1) * 100, 2)

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
