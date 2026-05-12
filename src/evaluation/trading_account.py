"""持久化模拟交易账户 (Persistent simulated trading account)。

本模块管理现金、持仓、交易订单以及硬性交易约束。
注意，该模块仅用于模拟交易记录，绝不会向真实券商发送订单。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

from src.database import StockDatabase
from src.quote_sources import fetch_latest_prices


class TradingAccount:
    """
    模拟交易账户类。
    管理账户余额、持仓计算、买卖订单执行以及诸如冷却期、持仓上限等业务约束。
    """

    def __init__(
        self,
        db: Optional[StockDatabase] = None,
        *,
        account_name: str = "default",
        initial_cash: float = 16000.0,
        max_positions: int = 5,
        lot_size: int = 100,
        min_holding_days: int = 5,
        rebuy_cooldown_days: int = 5,
        max_buys_per_run: int = 2,
        max_sells_per_run: int = 2,
    ):
        """
        初始化模拟账户。

        Args:
            db: 股票数据库实例，若未提供则默认创建。
            account_name: 账户标识符，默认为 "default"。
            initial_cash: 初始模拟现金余额（如：16000.0）。
            max_positions: 该账户允许的最大同时持仓数（如：5只股票）。
            lot_size: 最小交易单位（如：A股为 100 股一手）。
            min_holding_days: 建仓后必须持有的最少天数（防范频繁交易）。
            rebuy_cooldown_days: 卖出标的后，不允许重新买入的冷却期天数。
            max_buys_per_run: 单次运行最大允许的买单数量。
            max_sells_per_run: 单次运行最大允许的卖单数量。
        """
        self.db = db or StockDatabase()
        self.account_name = account_name
        self.initial_cash = float(initial_cash)
        self.max_positions = int(max_positions)
        self.lot_size = int(lot_size)
        self.min_holding_days = int(min_holding_days)
        self.rebuy_cooldown_days = int(rebuy_cooldown_days)
        self.max_buys_per_run = int(max_buys_per_run)
        self.max_sells_per_run = int(max_sells_per_run)
        self._run_buys = 0   # 内部计数器：记录当前运行周期的买入次数
        self._run_sells = 0  # 内部计数器：记录当前运行周期的卖出次数

    def ensure_account(self) -> Dict[str, Any]:
        """确保当前账户在数据库中存在，若不存在则初始化记录。"""
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
            raise RuntimeError("初始化模拟账户失败")
        logger.info("[交易账户] 账户已初始化 account={} cash={}", self.account_name, self.initial_cash)
        return self.get_account()

    def get_account(self) -> Dict[str, Any]:
        """获取账户当前状态和指标。"""
        df = self.db.query_to_dataframe(
            "SELECT * FROM trading_account WHERE account_name = ? LIMIT 1",
            (self.account_name,),
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()

    def set_cash(self, cash: float, *, reset_baseline: bool = False) -> Dict[str, Any]:
        """手动设置交易仓可用现金，可选同步重置初始资金基准。"""
        account = self.ensure_account()
        cash_value = round(max(0.0, float(cash)), 2)
        now = self._now()
        if reset_baseline:
            self.db.execute_non_query(
                """
                UPDATE trading_account
                SET initial_cash = ?, cash = ?, updated_at = ?
                WHERE id = ?
                """,
                (cash_value, cash_value, now, int(account["id"])),
            )
        else:
            self.db.execute_non_query(
                "UPDATE trading_account SET cash = ?, updated_at = ? WHERE id = ?",
                (cash_value, now, int(account["id"])),
            )
        logger.info("[交易账户] 已手动设置 account={} cash={}", self.account_name, cash_value)
        return self.refresh_positions()

    def list_open_positions(self) -> List[Dict[str, Any]]:
        """列出当前处于 'OPEN' 状态的所有持仓。"""
        return self.list_positions(status="OPEN")

    def list_positions(self, status: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """列出交易仓持仓记录；不传 status 时包含 OPEN 和 CLOSED 历史记录。"""
        account = self.ensure_account()
        params: list[Any] = [int(account["id"])]
        where = "WHERE account_id = ?"
        if status:
            where += " AND status = ?"
            params.append(str(status).upper())
        limit_sql = ""
        if limit:
            limit_sql = "LIMIT ?"
            params.append(int(limit))
        df = self.db.query_to_dataframe(
            f"""
            SELECT *
            FROM trading_positions
            {where}
            ORDER BY
                CASE status WHEN 'OPEN' THEN 0 ELSE 1 END,
                COALESCE(closed_at, last_sell_at, opened_at) DESC,
                id DESC
            {limit_sql}
            """,
            tuple(params),
        )
        if df.empty:
            return []
        return df.to_dict("records")

    def refresh_positions(self) -> Dict[str, Any]:
        """调用最新行情，刷新所有持仓当前的市值、浮盈亏以及更新总权益。"""
        account = self.ensure_account()
        self._backfill_legacy_realized_pnl(int(account["id"]))
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
            
            # 更新单支股票持仓记录
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
        realized_pnl = self._sum_realized_pnl(int(account["id"]))
        now = self._now()
        
        # 更新总账户看板记录
        self.db.execute_non_query(
            """
            UPDATE trading_account
            SET total_market_value = ?, total_equity = ?, realized_pnl = ?,
                unrealized_pnl = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                round(total_market_value, 2),
                total_equity,
                realized_pnl,
                round(total_unrealized, 2),
                now,
                int(account["id"]),
            ),
        )
        return self.get_account()

    def execute_decisions(self, decisions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """执行传入决策列表（将 BUY / SELL 等意图落地）。最后刷新市值。"""
        reports = []
        for decision in decisions or []:
            action = str(decision.get("action") or "HOLD").upper()
            if action == "BUY":
                reports.append(self.buy(decision))
            elif action == "SELL":
                reports.append(self.sell(decision))
            elif action in {"HOLD", "WATCH", "REMOVE"}:
                reports.append(self.record_non_trade(decision))
        
        # 执行完毕后刷新当前持仓状态
        self.refresh_positions()
        return reports

    def buy(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """执行买单操作。检查编码合法性、最大持仓、买入次数限制等。"""
        account = self.ensure_account()
        code = str(decision.get("code") or "").zfill(6)
        
        # 1. 业务逻辑层拦截
        if not self._valid_code(code):
            return self._rejected(decision, "invalid code")
        if self._run_buys >= self.max_buys_per_run:
            return self._rejected(decision, "本次运行买入次数已达上限")
        existing_position = self._find_open_position(code)
        if not existing_position and len(self.list_open_positions()) >= self.max_positions:
            return self._rejected(decision, "交易仓持仓数量已达上限")
        if not existing_position and self._in_rebuy_cooldown(code):
            return self._rejected(decision, "卖出冷却期内禁止回买")

        # 2. 定价与数量换算
        price = self._resolve_price(decision)
        if price <= 0:
            return self._rejected(decision, "无法确定买入价格")

        cash_before = float(account.get("cash") or 0)
        requested_qty = int(decision.get("quantity") or 0)
        
        # 如果未指定数量，则按照目标金额（或默认三分之一现金）换算成匹配整手 lot_size 的股数
        if requested_qty <= 0:
            target_cash = min(cash_before, float(decision.get("target_cash") or cash_before / 3))
            requested_qty = int(target_cash // (price * self.lot_size)) * self.lot_size
            
        quantity = max(0, requested_qty - requested_qty % self.lot_size)
        amount = round(quantity * price, 2)
        
        if quantity <= 0 or amount > cash_before:
            return self._rejected(decision, "现金不足一手或买入金额超过可用现金")

        # 3. 数据层入库 (包含订单记录及扣款)。已有持仓时按加权平均成本加仓。
        now = self._now()
        cash_after = round(cash_before - amount, 2)
        name = str(decision.get("name") or code)
        position_before = int(existing_position.get("quantity") or 0)
        position_after = position_before + quantity

        if existing_position:
            old_cost = float(existing_position.get("avg_cost") or 0)
            new_avg_cost = round(((old_cost * position_before) + amount) / position_after, 4)
            market_value = round(position_after * price, 2)
            unrealized = round((price - new_avg_cost) * position_after, 2)
            return_pct = round((price / new_avg_cost - 1) * 100, 2) if new_avg_cost else 0.0
            merged_reason = self._merge_text(existing_position.get("buy_reason"), decision.get("reason"))
            merged_risk = self._merge_text(existing_position.get("risk_note"), decision.get("risk_note"))
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET name = ?, quantity = ?, avg_cost = ?, current_price = ?, market_value = ?,
                    unrealized_pnl = ?, unrealized_return_pct = ?, last_buy_at = ?,
                    buy_reason = ?, risk_note = ?
                WHERE id = ?
                """,
                (
                    name,
                    position_after,
                    new_avg_cost,
                    price,
                    market_value,
                    unrealized,
                    return_pct,
                    now,
                    merged_reason,
                    merged_risk,
                    int(existing_position["id"]),
                ),
            )
        else:
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
        self._record_order(account, decision, "BUY", quantity, price, amount, cash_before, cash_after, position_before, position_after)
        
        self._run_buys += 1
        return {"code": code, "action": "BUY", "quantity": quantity, "price": price, "amount": amount, "status": "FILLED"}

    def sell(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        """执行卖出单操作。校验股票是否持有及最小持有期限等，更新累计账户的已实现盈亏。"""
        account = self.ensure_account()
        code = str(decision.get("code") or "").zfill(6)
        position = self._find_open_position(code)
        
        # 1. 业务逻辑层拦截
        if not position:
            return self._rejected(decision, "交易仓未持有该股票")
        if self._run_sells >= self.max_sells_per_run:
            return self._rejected(decision, "本次运行卖出次数已达上限")
            
        risk_override = bool(decision.get("risk_override"))
        if not risk_override and not self._holding_period_satisfied(position):
            return self._rejected(decision, "未满足最短持有期")

        # 2. 定价与可卖出数量核对
        price = self._resolve_price(decision, fallback=position.get("current_price"))
        if price <= 0:
            return self._rejected(decision, "无法确定卖出价格")

        quantity_before = int(position.get("quantity") or 0)
        requested_qty = int(decision.get("quantity") or quantity_before)
        quantity = min(quantity_before, requested_qty)
        # 向下取整数手。如果不满一手而又是全部卖出，则原封不动抛出。
        quantity = max(0, quantity - quantity % self.lot_size) if quantity < quantity_before else quantity_before
        if quantity <= 0:
            return self._rejected(decision, "卖出数量无效")

        # 3. 盈亏、现金、结余清算 (包含利润核算)
        cash_before = float(account.get("cash") or 0)
        amount = round(quantity * price, 2)
        cash_after = round(cash_before + amount, 2)
        quantity_after = quantity_before - quantity
        
        avg_cost = float(position.get("avg_cost") or 0)
        realized_pnl_delta = round((price - avg_cost) * quantity, 2)
        realized_pnl_position = round(float(position.get("realized_pnl") or 0) + realized_pnl_delta, 2)
        sold_quantity = int(position.get("sold_quantity") or 0) + quantity
        realized_return_pct = (
            round(realized_pnl_position / (avg_cost * sold_quantity) * 100, 2)
            if avg_cost > 0 and sold_quantity > 0
            else 0.0
        )
        now = self._now()

        # 全部清仓逻辑
        if quantity_after <= 0:
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET quantity = 0, current_price = ?, market_value = 0, unrealized_pnl = 0,
                    unrealized_return_pct = 0, realized_pnl = ?, realized_return_pct = ?,
                    sold_quantity = ?, last_sell_at = ?, closed_at = ?, status = 'CLOSED'
                WHERE id = ?
                """,
                (
                    price,
                    realized_pnl_position,
                    realized_return_pct,
                    sold_quantity,
                    now,
                    now,
                    int(position["id"]),
                ),
            )
        # 部分减仓逻辑
        else:
            market_value = round(quantity_after * price, 2)
            unrealized = round((price - avg_cost) * quantity_after, 2)
            return_pct = round((price / avg_cost - 1) * 100, 2) if avg_cost else 0.0
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET quantity = ?, current_price = ?, market_value = ?, unrealized_pnl = ?,
                    unrealized_return_pct = ?, realized_pnl = ?, realized_return_pct = ?,
                    sold_quantity = ?, last_sell_at = ?
                WHERE id = ?
                """,
                (
                    quantity_after,
                    price,
                    market_value,
                    unrealized,
                    return_pct,
                    realized_pnl_position,
                    realized_return_pct,
                    sold_quantity,
                    now,
                    int(position["id"]),
                ),
            )

        realized_pnl = self._sum_realized_pnl(int(account["id"]))
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
        """将 HOLD (持有)、WATCH (观察) 等非交易类动作仅记入订单流水历史，不做真实扣款和挂单修改。"""
        account = self.ensure_account()
        action = str(decision.get("action") or "HOLD").upper()
        code = str(decision.get("code") or "").zfill(6) if decision.get("code") else ""
        position = self._find_open_position(code) if code else {}
        cash = float(account.get("cash") or 0)
        qty = int(position.get("quantity") or 0)
        
        self._record_order(account, decision, action, 0, self._resolve_price(decision), 0, cash, cash, qty, qty)
        return {"code": code, "action": action, "status": "RECORDED", "reason": str(decision.get("reason") or "")}

    def get_latest_prices(self, codes: Iterable[str]) -> Dict[str, float]:
        """批量获取股票的最新价格（优先线上抓取，兜底日线库）。"""
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
        """生成 Markdown 格式的 AI 交易仓执行面板报告，用于直观展示当前资金与持仓变化。"""
        account = self.refresh_positions()
        positions = self.list_open_positions()
        history_positions = self.list_positions(status="CLOSED", limit=5)
        realized = round(float(account.get("realized_pnl") or 0), 2)
        unrealized = round(float(account.get("unrealized_pnl") or 0), 2)
        lines = ["### AI 交易仓执行报告", ""]
        
        # 头部账户汇总
        lines.append(
            f"- 现金: {round(float(account.get('cash') or 0), 2)} | "
            f"总市值: {round(float(account.get('total_market_value') or 0), 2)} | "
            f"总权益: {round(float(account.get('total_equity') or 0), 2)} | "
            f"已实现盈亏: {realized} | "
            f"浮动盈亏: {unrealized} | "
            f"总盈亏: {round(realized + unrealized, 2)}"
        )
        lines.append("")
        
        # 执行结果明细行
        lines.append("| 动作 | 代码 | 数量 | 价格 | 金额 | 状态/理由 |")
        lines.append("| :---: | :---: | ---: | ---: | ---: | :--- |")
        for item in execution_results:
            lines.append(
                f"| {item.get('action')} | {item.get('code', '')} | {item.get('quantity', 0)} | "
                f"{item.get('price', '')} | {item.get('amount', '')} | {item.get('status', '')} {item.get('reason', '')} |"
            )
        lines.append("")
        
        # 当前持仓明细展示；减仓后保留在当前行，按券商常见口径展示未实现 + 已实现。
        lines.append("| 持仓 | 代码 | 持有数量 | 已卖数量 | 成本 | 现价 | 未实现 | 已实现 | 持仓总盈亏 |")
        lines.append("| :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        if not positions:
            lines.append("| 当前无持仓 | - | 0 | 0 | - | - | - | - | - |")
        for pos in positions:
            unrealized_pnl = round(float(pos.get("unrealized_pnl") or 0), 2)
            realized_pnl = round(float(pos.get("realized_pnl") or 0), 2)
            total_pnl = round(unrealized_pnl + realized_pnl, 2)
            total_return_pct = self._position_total_return_pct(pos, total_pnl)
            lines.append(
                f"| {pos.get('name')} | {pos.get('code')} | {pos.get('quantity')} | "
                f"{pos.get('sold_quantity') or 0} | {pos.get('avg_cost')} | {pos.get('current_price')} | "
                f"{unrealized_pnl} | {realized_pnl} | {total_pnl} ({total_return_pct}%) |"
            )
        lines.append("")
        lines.append("| 历史持仓 | 代码 | 卖出数量 | 成本 | 卖出价 | 已实现盈亏 |")
        lines.append("| :--- | :---: | ---: | ---: | ---: | ---: |")
        if not history_positions:
            lines.append("| 暂无历史持仓 | - | 0 | - | - | - |")
        for pos in history_positions:
            realized_pnl = round(float(pos.get("realized_pnl") or 0), 2)
            return_pct = round(float(pos.get("realized_return_pct") or 0), 2)
            lines.append(
                f"| {pos.get('name')} | {pos.get('code')} | {pos.get('sold_quantity') or 0} | "
                f"{pos.get('avg_cost')} | {pos.get('current_price')} | {realized_pnl} ({return_pct}%) |"
            )
        return "\n".join(lines)

    def format_post_market_diagnostics(self, macro_context: Optional[Dict[str, Any]] = None) -> str:
        """生成交易仓盘后诊断；交易仓只复盘已结算历史持仓，当前浮亏不做反思。"""
        self.refresh_positions()
        positions = self.list_positions(status="CLOSED", limit=20)
        if not positions:
            return "### AI 交易仓盘后诊断\n\n当前没有已结算的交易仓历史持仓。"

        lines = ["### AI 交易仓盘后诊断", ""]
        lines.append("当前持仓只刷新浮动盈亏，不触发交易仓亏损反思；下表仅复盘已清仓样本。")
        lines.append("")
        lines.append("| 类型 | 股票 | 代码 | 成本 | 卖出价 | 已实现盈亏 | 盘后动作 | 诊断原因 |")
        lines.append("| :---: | :--- | :---: | ---: | ---: | ---: | :---: | :--- |")
        for pos in positions:
            item = self._diagnose_trading_position(pos, macro_context=macro_context)
            lines.append(
                f"| {item['kind']} | {item['name']} | {item['code']} | {item['avg_cost']} | "
                f"{item['price']} | {item['pnl']} ({item['return_pct']}%) | "
                f"{item['action']} | {item['reason']} |"
            )
        return "\n".join(lines)

    def _diagnose_trading_position(
        self,
        position: Dict[str, Any],
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """生成已结算历史持仓的诊断行。"""
        status = str(position.get("status") or "OPEN").upper()
        code = str(position.get("code") or "").zfill(6)
        avg_cost = round(float(position.get("avg_cost") or 0), 2)
        price = round(float(position.get("current_price") or 0), 2)
        if status == "OPEN":
            total_pnl = round(float(position.get("unrealized_pnl") or 0) + float(position.get("realized_pnl") or 0), 2)
            return_pct = self._position_total_return_pct(position, total_pnl)
            pnl = total_pnl
            action = "仅刷新"
            reason = "当前持仓属于未结算样本，只展示持仓总盈亏，不写入亏损反思。"
            kind = "当前"
        else:
            return_pct = round(float(position.get("realized_return_pct") or 0), 2)
            pnl = round(float(position.get("realized_pnl") or 0), 2)
            action = "历史复盘"
            if pnl < 0:
                reason = "已清仓亏损样本，继续纳入总盈亏和风控反思。"
            elif pnl > 0:
                reason = "已清仓盈利样本，纳入历史胜负统计，检查卖点质量。"
            else:
                reason = "已清仓持平样本，保留用于后续交易质量统计。"
            kind = "历史"
        return {
            "kind": kind,
            "name": position.get("name") or code,
            "code": code,
            "avg_cost": avg_cost,
            "price": price,
            "pnl": pnl,
            "return_pct": return_pct,
            "action": action,
            "reason": reason,
        }

    def _position_total_return_pct(self, position: Dict[str, Any], total_pnl: float) -> float:
        """按建仓总股数估算持仓总收益率，减仓后避免收益率被剩余股数放大。"""
        avg_cost = float(position.get("avg_cost") or 0)
        held_quantity = int(position.get("quantity") or 0)
        sold_quantity = int(position.get("sold_quantity") or 0)
        base_quantity = max(held_quantity + sold_quantity, sold_quantity, held_quantity)
        if avg_cost <= 0 or base_quantity <= 0:
            return 0.0
        return round(float(total_pnl or 0) / (avg_cost * base_quantity) * 100, 2)

    def _diagnose_open_position(
        self,
        position: Dict[str, Any],
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, str]:
        """当前持仓使用 ExitAgent 做盘后动作建议，失败时回退到收益率规则。"""
        try:
            from src.agent.exit_agent import ExitAgent

            exit_position = dict(position)
            exit_position["recommend_price"] = position.get("avg_cost")
            exit_position["return_pct"] = position.get("unrealized_return_pct")
            signal = ExitAgent(db=self.db).evaluate_position(exit_position, macro_context=macro_context).to_dict()
            return str(signal.get("action") or "继续持有"), str(signal.get("reason") or "")
        except Exception as exc:
            logger.warning("[交易账户] 当前持仓诊断降级: {}", exc)
            return_pct = float(position.get("unrealized_return_pct") or 0)
            if return_pct <= -5:
                return "清仓退出", f"浮亏 {return_pct}% 已触发交易仓止损观察线。"
            if return_pct >= 10:
                return "减仓观察", f"浮盈 {return_pct}% 已达到锁定利润观察线。"
            return "继续持有", f"浮动收益 {return_pct}% 仍在交易仓容忍区间。"

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
        """底层方法：登记交易履历/流水单到 trade_orders 表中。"""
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
        """获取某只股票是否处于 OPEN 状态及对应的单子细节。"""
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

    def _sum_realized_pnl(self, account_id: int) -> float:
        """按持仓历史汇总已实现盈亏，保证清仓记录持续参与账户盈亏。"""
        self._backfill_legacy_realized_pnl(account_id)
        df = self.db.query_to_dataframe(
            """
            SELECT COALESCE(SUM(realized_pnl), 0) AS realized_pnl
            FROM trading_positions
            WHERE account_id = ?
            """,
            (int(account_id),),
        )
        if df.empty:
            return 0.0
        return round(float(df.iloc[0]["realized_pnl"] or 0), 2)

    def _backfill_legacy_realized_pnl(self, account_id: int) -> None:
        """为升级前缺少历史盈亏字段的清仓记录，从卖出流水反推一次。"""
        df = self.db.query_to_dataframe(
            """
            SELECT id, code, avg_cost, opened_at, last_sell_at, closed_at
            FROM trading_positions
            WHERE account_id = ?
              AND status = 'CLOSED'
              AND COALESCE(sold_quantity, 0) = 0
              AND COALESCE(realized_pnl, 0) = 0
            """,
            (int(account_id),),
        )
        if df.empty:
            return

        for _, row in df.iterrows():
            opened_at = str(row.get("opened_at") or "")
            ended_at = str(row.get("closed_at") or row.get("last_sell_at") or "")
            code = str(row.get("code") or "").zfill(6)
            avg_cost = float(row.get("avg_cost") or 0)
            if not code or avg_cost <= 0:
                continue
            orders = self.db.query_to_dataframe(
                """
                SELECT quantity, price
                FROM trade_orders
                WHERE account_id = ?
                  AND code = ?
                  AND action = 'SELL'
                  AND created_at >= ?
                  AND (? = '' OR created_at <= ?)
                """,
                (int(account_id), code, opened_at, ended_at, ended_at),
            )
            if orders.empty:
                continue
            sold_quantity = int(orders["quantity"].fillna(0).sum())
            if sold_quantity <= 0:
                continue
            realized_pnl = round(
                sum((float(item["price"] or 0) - avg_cost) * int(item["quantity"] or 0) for _, item in orders.iterrows()),
                2,
            )
            realized_return_pct = round(realized_pnl / (avg_cost * sold_quantity) * 100, 2)
            self.db.execute_non_query(
                """
                UPDATE trading_positions
                SET sold_quantity = ?, realized_pnl = ?, realized_return_pct = ?
                WHERE id = ?
                """,
                (sold_quantity, realized_pnl, realized_return_pct, int(row["id"])),
            )

    def _in_rebuy_cooldown(self, code: str) -> bool:
        """检查特定代码由于之前进行过 SELL（卖出），目前是否仍在重新买入的冷却期内。"""
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
        """判断是否满足了最低持有天数 (控制换手率防频繁交易)。"""
        opened_at = str(position.get("opened_at") or "")
        return self._days_since(opened_at) >= self.min_holding_days

    def _days_since(self, timestamp: str) -> int:
        """帮助方法：统计指定时间到目前相差的天数。"""
        try:
            dt = datetime.strptime(timestamp[:19], "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return 9999
        return (datetime.now() - dt).days

    def _resolve_price(self, decision: Dict[str, Any], fallback: Any = None) -> float:
        """决定最终交易委托买卖价格；依次使用指令单自带价格 -> 实时查询价 -> 退回兜底价。"""
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
        """拒绝记录：当订单触发硬性拦截器（比如风控、或者无可用资金余额上限）时录入为 REJECTED。"""
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
        """返回当前日期时间的格式化字符串。"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _valid_code(code: str) -> bool:
        """简单校验是否为有效 A 股标识符编码（6 位数字）。"""
        return len(code) == 6 and code.isdigit()

    @staticmethod
    def _to_float(value: Any) -> float | None:
        """安全转换为浮动数据类型。"""
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        """安全数字转化支持。"""
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _merge_text(old_value: Any, new_value: Any) -> str:
        """合并多次建仓理由，避免加仓覆盖首笔买入依据。"""
        old_text = str(old_value or "").strip()
        new_text = str(new_value or "").strip()
        if not old_text:
            return new_text
        if not new_text or new_text in old_text:
            return old_text
        return f"{old_text}\n加仓：{new_text}"
