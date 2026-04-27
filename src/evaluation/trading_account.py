"""Persistent simulated trading account.

This module owns cash, positions, trade orders and hard trading constraints.
It never sends real broker orders.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

from src.database import StockDatabase
from src.quote_sources import fetch_latest_prices


class TradingAccount:
    def __init__(
        self,
        db: Optional[StockDatabase] = None,
        *,
        account_name: str = "default",
        initial_cash: float = 10000.0,
        max_positions: int = 5,
        lot_size: int = 100,
        min_holding_days: int = 5,
        rebuy_cooldown_days: int = 5,
        max_buys_per_run: int = 2,
        max_sells_per_run: int = 2,
    ):
        self.db = db or StockDatabase()
        self.account_name = account_name
        self.initial_cash = float(initial_cash)
        self.max_positions = int(max_positions)
        self.lot_size = int(lot_size)
        self.min_holding_days = int(min_holding_days)
        self.rebuy_cooldown_days = int(rebuy_cooldown_days)
        self.max_buys_per_run = int(max_buys_per_run)
        self.max_sells_per_run = int(max_sells_per_run)
        self._run_buys = 0
        self._run_sells = 0

    def ensure_account(self) -> Dict[str, Any]:
        account = self.get_account()
        if account:
            return account

        now = self._now()
        ok = self.db.execute_non_query(
            """
            INSERT INTO trading_account
            (account_name, initial_cash, cash, total_market_value, total_equity,
             realized_pnl, unrealized_pnl, created_at, updated_at)
            VALUES (?, ?, ?, 0, ?, 0, 0, ?, ?)
            """,
            (self.account_name, self.initial_cash, self.initial_cash, self.initial_cash, now, now),
        )
        if not ok:
            raise RuntimeError("failed to initialize trading account")
        logger.info("[TradingAccount] initialized account={} cash={}", self.account_name, self.initial_cash)
        return self.get_account()

    def get_account(self) -> Dict[str, Any]:
        df = self.db.query_to_dataframe(
            "SELECT * FROM trading_account WHERE account_name = ? LIMIT 1",
            (self.account_name,),
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()

    def list_open_positions(self) -> List[Dict[str, Any]]:
        account = self.ensure_account()
        df = self.db.query_to_dataframe(
            """
            SELECT *
            FROM trading_positions
            WHERE account_id = ? AND status = 'OPEN'
            ORDER BY opened_at, id
            """,
            (int(account["id"]),),
        )
        if df.empty:
            return []
        return df.to_dict("records")

    def refresh_positions(self) -> Dict[str, Any]:
        account = self.ensure_account()
        positions = self.list_open_positions()
        prices = self.get_latest_prices([item["code"] for item in positions])

        total_market_value = 0.0
        total_unrealized = 0.0
        for item in positions:
            code = str(item["code"]).zfill(6)
            quantity = int(item.get("quantity") or 0)
            avg_cost = float(item.get("avg_cost") or 0)
            price = float(prices.get(code) or item.get("current_price") or 0)
            market_value = round(quantity * price, 2)
            unrealized = round((price - avg_cost) * quantity, 2)
            return_pct = round((price / avg_cost - 1) * 100, 2) if avg_cost else 0.0
            total_market_value += market_value
            total_unrealized += unrealized
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET current_price = ?, market_value = ?, unrealized_pnl = ?,
                    unrealized_return_pct = ?
                WHERE id = ?
                """,
                (price, market_value, unrealized, return_pct, int(item["id"])),
            )

        cash = float(account.get("cash") or 0)
        total_equity = round(cash + total_market_value, 2)
        now = self._now()
        self.db.execute_non_query(
            """
            UPDATE trading_account
            SET total_market_value = ?, total_equity = ?, unrealized_pnl = ?, updated_at = ?
            WHERE id = ?
            """,
            (round(total_market_value, 2), total_equity, round(total_unrealized, 2), now, int(account["id"])),
        )
        return self.get_account()

    def execute_decisions(self, decisions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        reports = []
        for decision in decisions or []:
            action = str(decision.get("action") or "HOLD").upper()
            if action == "BUY":
                reports.append(self.buy(decision))
            elif action == "SELL":
                reports.append(self.sell(decision))
            elif action in {"HOLD", "WATCH", "REMOVE"}:
                reports.append(self.record_non_trade(decision))
        self.refresh_positions()
        return reports

    def buy(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        account = self.ensure_account()
        code = str(decision.get("code") or "").zfill(6)
        if not self._valid_code(code):
            return self._rejected(decision, "invalid code")
        if self._run_buys >= self.max_buys_per_run:
            return self._rejected(decision, "本次运行买入次数已达上限")
        if self._find_open_position(code):
            return self._rejected(decision, "交易仓已持有该股票")
        if len(self.list_open_positions()) >= self.max_positions:
            return self._rejected(decision, "交易仓持仓数量已达上限")
        if self._in_rebuy_cooldown(code):
            return self._rejected(decision, "卖出冷却期内禁止回买")

        price = self._resolve_price(decision)
        if price <= 0:
            return self._rejected(decision, "无法确定买入价格")

        cash_before = float(account.get("cash") or 0)
        requested_qty = int(decision.get("quantity") or 0)
        if requested_qty <= 0:
            target_cash = min(cash_before, float(decision.get("target_cash") or cash_before / 3))
            requested_qty = int(target_cash // (price * self.lot_size)) * self.lot_size
        quantity = max(0, requested_qty - requested_qty % self.lot_size)
        amount = round(quantity * price, 2)
        if quantity <= 0 or amount > cash_before:
            return self._rejected(decision, "现金不足一手或买入金额超过可用现金")

        now = self._now()
        cash_after = round(cash_before - amount, 2)
        name = str(decision.get("name") or code)
        self.db.execute_non_query(
            """
            INSERT INTO trading_positions
            (account_id, code, name, quantity, avg_cost, current_price, market_value,
             unrealized_pnl, unrealized_return_pct, opened_at, last_buy_at, status,
             linked_watchlist_id, buy_reason, risk_note)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, 'OPEN', ?, ?, ?)
            """,
            (
                int(account["id"]),
                code,
                name,
                quantity,
                price,
                price,
                amount,
                now,
                now,
                self._to_int(decision.get("linked_watchlist_id")),
                str(decision.get("reason") or ""),
                str(decision.get("risk_note") or ""),
            ),
        )
        self.db.execute_non_query(
            "UPDATE trading_account SET cash = ?, updated_at = ? WHERE id = ?",
            (cash_after, now, int(account["id"])),
        )
        self._record_order(account, decision, "BUY", quantity, price, amount, cash_before, cash_after, 0, quantity)
        self._run_buys += 1
        return {"code": code, "action": "BUY", "quantity": quantity, "price": price, "amount": amount, "status": "FILLED"}

    def sell(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        account = self.ensure_account()
        code = str(decision.get("code") or "").zfill(6)
        position = self._find_open_position(code)
        if not position:
            return self._rejected(decision, "交易仓未持有该股票")
        if self._run_sells >= self.max_sells_per_run:
            return self._rejected(decision, "本次运行卖出次数已达上限")
        risk_override = bool(decision.get("risk_override"))
        if not risk_override and not self._holding_period_satisfied(position):
            return self._rejected(decision, "未满足最短持有期")

        price = self._resolve_price(decision, fallback=position.get("current_price"))
        if price <= 0:
            return self._rejected(decision, "无法确定卖出价格")

        quantity_before = int(position.get("quantity") or 0)
        requested_qty = int(decision.get("quantity") or quantity_before)
        quantity = min(quantity_before, requested_qty)
        quantity = max(0, quantity - quantity % self.lot_size) if quantity < quantity_before else quantity_before
        if quantity <= 0:
            return self._rejected(decision, "卖出数量无效")

        cash_before = float(account.get("cash") or 0)
        amount = round(quantity * price, 2)
        cash_after = round(cash_before + amount, 2)
        quantity_after = quantity_before - quantity
        avg_cost = float(position.get("avg_cost") or 0)
        realized_pnl_delta = round((price - avg_cost) * quantity, 2)
        now = self._now()

        if quantity_after <= 0:
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET quantity = 0, current_price = ?, market_value = 0, unrealized_pnl = 0,
                    unrealized_return_pct = 0, last_sell_at = ?, status = 'CLOSED'
                WHERE id = ?
                """,
                (price, now, int(position["id"])),
            )
        else:
            market_value = round(quantity_after * price, 2)
            unrealized = round((price - avg_cost) * quantity_after, 2)
            return_pct = round((price / avg_cost - 1) * 100, 2) if avg_cost else 0.0
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET quantity = ?, current_price = ?, market_value = ?, unrealized_pnl = ?,
                    unrealized_return_pct = ?, last_sell_at = ?
                WHERE id = ?
                """,
                (quantity_after, price, market_value, unrealized, return_pct, now, int(position["id"])),
            )

        realized_pnl = float(account.get("realized_pnl") or 0) + realized_pnl_delta
        self.db.execute_non_query(
            """
            UPDATE trading_account
            SET cash = ?, realized_pnl = ?, updated_at = ?
            WHERE id = ?
            """,
            (cash_after, round(realized_pnl, 2), now, int(account["id"])),
        )
        self._record_order(
            account,
            decision,
            "SELL",
            quantity,
            price,
            amount,
            cash_before,
            cash_after,
            quantity_before,
            quantity_after,
        )
        self._run_sells += 1
        return {"code": code, "action": "SELL", "quantity": quantity, "price": price, "amount": amount, "status": "FILLED"}

    def record_non_trade(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        account = self.ensure_account()
        action = str(decision.get("action") or "HOLD").upper()
        code = str(decision.get("code") or "").zfill(6) if decision.get("code") else ""
        position = self._find_open_position(code) if code else {}
        cash = float(account.get("cash") or 0)
        qty = int(position.get("quantity") or 0)
        self._record_order(account, decision, action, 0, self._resolve_price(decision), 0, cash, cash, qty, qty)
        return {"code": code, "action": action, "status": "RECORDED", "reason": str(decision.get("reason") or "")}

    def get_latest_prices(self, codes: Iterable[str]) -> Dict[str, float]:
        normalized = [str(code).zfill(6) for code in codes if code]
        if not normalized:
            return {}
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

    def format_report(self, execution_results: List[Dict[str, Any]]) -> str:
        account = self.refresh_positions()
        positions = self.list_open_positions()
        lines = ["### AI 交易仓执行报告", ""]
        lines.append(
            f"- 现金: {round(float(account.get('cash') or 0), 2)} | "
            f"总市值: {round(float(account.get('total_market_value') or 0), 2)} | "
            f"总权益: {round(float(account.get('total_equity') or 0), 2)} | "
            f"已实现盈亏: {round(float(account.get('realized_pnl') or 0), 2)}"
        )
        lines.append("")
        lines.append("| 动作 | 代码 | 数量 | 价格 | 金额 | 状态/理由 |")
        lines.append("| :---: | :---: | ---: | ---: | ---: | :--- |")
        for item in execution_results:
            lines.append(
                f"| {item.get('action')} | {item.get('code', '')} | {item.get('quantity', 0)} | "
                f"{item.get('price', '')} | {item.get('amount', '')} | {item.get('status', '')} {item.get('reason', '')} |"
            )
        lines.append("")
        lines.append("| 持仓 | 代码 | 数量 | 成本 | 现价 | 浮盈亏 |")
        lines.append("| :--- | :---: | ---: | ---: | ---: | ---: |")
        if not positions:
            lines.append("| 当前无持仓 | - | 0 | - | - | - |")
        for pos in positions:
            lines.append(
                f"| {pos.get('name')} | {pos.get('code')} | {pos.get('quantity')} | "
                f"{pos.get('avg_cost')} | {pos.get('current_price')} | {pos.get('unrealized_return_pct')}% |"
            )
        return "\n".join(lines)

    def _record_order(
        self,
        account: Dict[str, Any],
        decision: Dict[str, Any],
        action: str,
        quantity: int,
        price: float,
        amount: float,
        cash_before: float,
        cash_after: float,
        position_before: int,
        position_after: int,
    ) -> None:
        self.db.execute_non_query(
            """
            INSERT INTO trade_orders
            (account_id, code, name, action, quantity, price, amount, cash_before,
             cash_after, position_before, position_after, reason, decision_snapshot,
             linked_watchlist_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(account["id"]),
                str(decision.get("code") or "").zfill(6) if decision.get("code") else "",
                str(decision.get("name") or ""),
                action,
                int(quantity),
                float(price or 0),
                float(amount or 0),
                float(cash_before or 0),
                float(cash_after or 0),
                int(position_before or 0),
                int(position_after or 0),
                str(decision.get("reason") or ""),
                json.dumps(decision, ensure_ascii=False, default=str),
                self._to_int(decision.get("linked_watchlist_id")),
                self._now(),
            ),
        )

    def _find_open_position(self, code: str) -> Dict[str, Any]:
        account = self.ensure_account()
        df = self.db.query_to_dataframe(
            """
            SELECT *
            FROM trading_positions
            WHERE account_id = ? AND code = ? AND status = 'OPEN'
            LIMIT 1
            """,
            (int(account["id"]), str(code).zfill(6)),
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()

    def _in_rebuy_cooldown(self, code: str) -> bool:
        account = self.ensure_account()
        df = self.db.query_to_dataframe(
            """
            SELECT created_at
            FROM trade_orders
            WHERE account_id = ? AND code = ? AND action = 'SELL'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (int(account["id"]), str(code).zfill(6)),
        )
        if df.empty:
            return False
        return self._days_since(str(df.iloc[0]["created_at"])) < self.rebuy_cooldown_days

    def _holding_period_satisfied(self, position: Dict[str, Any]) -> bool:
        opened_at = str(position.get("opened_at") or "")
        return self._days_since(opened_at) >= self.min_holding_days

    def _days_since(self, timestamp: str) -> int:
        try:
            dt = datetime.strptime(timestamp[:19], "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return 9999
        return (datetime.now() - dt).days

    def _resolve_price(self, decision: Dict[str, Any], fallback: Any = None) -> float:
        price = self._to_float(decision.get("price"))
        if price and price > 0:
            return price
        code = decision.get("code")
        if code:
            prices = self.get_latest_prices([str(code).zfill(6)])
            price = self._to_float(prices.get(str(code).zfill(6)))
            if price and price > 0:
                return price
        fallback_price = self._to_float(fallback)
        return fallback_price or 0.0

    def _rejected(self, decision: Dict[str, Any], reason: str) -> Dict[str, Any]:
        item = dict(decision)
        item["action"] = str(item.get("action") or "HOLD").upper()
        item["status"] = "REJECTED"
        item["reason"] = f"{reason}; {item.get('reason', '')}".strip()
        self.record_non_trade({**item, "action": "HOLD"})
        return {
            "code": str(decision.get("code") or "").zfill(6) if decision.get("code") else "",
            "action": item["action"],
            "status": "REJECTED",
            "reason": item["reason"],
            "quantity": 0,
            "price": self._resolve_price(decision),
            "amount": 0,
        }

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _valid_code(code: str) -> bool:
        return len(code) == 6 and code.isdigit()

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None
