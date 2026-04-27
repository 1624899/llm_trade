"""虚拟观察仓系统。

核心职责：
1. 记录 Agent 推荐标的和推荐时点价格。
2. 盘后更新观察仓最新价格和浮动收益。
3. 结合收益率规则、技术破位和宏观风险输出持仓动作诊断。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger

from src.agent.exit_agent import ACTION_CLEAR, ExitAgent
from src.database import StockDatabase
from src.quote_sources import fetch_latest_prices


DEFAULT_POST_MARKET_RULES = {
    "stop_loss_pct": -5.0,
    "take_profit_pct": 15.0,
    "reduce_watch_pct": 10.0,
    "wait_confirm_loss_pct": -2.0,
}


class PaperTrading:
    def __init__(
        self,
        db: Optional[StockDatabase] = None,
        rules: Optional[Dict[str, Any]] = None,
        exit_agent: Optional[ExitAgent] = None,
    ):
        self.db = db or StockDatabase()
        self.rules = {**DEFAULT_POST_MARKET_RULES, **(rules or {})}
        self.exit_agent = exit_agent or ExitAgent(db=self.db)
        logger.info("初始化虚拟观察仓 (Paper Trading) ...")

    def _get_current_price(self, symbol: str) -> float:
        """优先从实时行情源取价，失败后回退到本地最新快照。"""
        code = str(symbol).zfill(6)
        prices = fetch_latest_prices([code])
        if code in prices:
            return float(prices[code])

        df = self.db.query_to_dataframe(
            """
            SELECT price
            FROM daily_quotes
            WHERE code = ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (code,),
        )
        if df is not None and not df.empty:
            return float(df.iloc[0]["price"])

        logger.warning("无法获取 {} 的当前价格", code)
        return 0.0

    def add_trade(self, code: str, name: str, reason: str) -> None:
        """把 Agent 推荐的股票加入观察仓。"""
        code = str(code).zfill(6)
        existing = self.db.query_to_dataframe(
            """
            SELECT id
            FROM paper_trades
            WHERE code = ? AND status = 'HOLD'
            LIMIT 1
            """,
            (code,),
        )
        if existing is not None and not existing.empty:
            logger.info("观察仓已存在 {}({}) 的 HOLD 记录，跳过重复录入", name, code)
            return

        current_price = self._get_current_price(code)
        if current_price <= 0:
            logger.error("无法确定 {}({}) 的买入价格，取消录入虚拟仓", name, code)
            return

        recommend_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = """
            INSERT INTO paper_trades
            (code, name, recommend_date, recommend_price, recommend_reason, current_price, return_pct, status)
            VALUES (?, ?, ?, ?, ?, ?, 0.0, 'HOLD')
        """
        success = self.db.execute_non_query(sql, (code, name, recommend_date, current_price, reason, current_price))
        if success:
            logger.info("已加入虚拟观察仓: {}({}) @ {}元", name, code, current_price)
        else:
            logger.error("虚拟仓录入失败: {}({})", name, code)

    def _get_latest_prices_batch(self, codes: list) -> dict:
        """批量获取最新价，实时源不可用时回退到本地快照。"""
        normalized_codes = [str(code).zfill(6) for code in codes]
        prices = fetch_latest_prices(normalized_codes)
        missing_codes = [code for code in normalized_codes if code not in prices]

        if missing_codes:
            placeholders = ",".join(["?"] * len(missing_codes))
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
                tuple(missing_codes),
            )
            if df is not None and not df.empty:
                prices.update(dict(zip(df["code"].astype(str).str.zfill(6), df["price"].astype(float))))

        return prices

    def update_portfolio(self) -> None:
        """盘后刷新持仓最新价和浮动收益。"""
        logger.info("[Paper Trading] 开始更新观察仓最新价格...")
        df = self.db.query_to_dataframe("SELECT * FROM paper_trades WHERE status = 'HOLD'")

        if df.empty:
            logger.info("虚拟仓当前为空，无需更新。")
            return

        latest_prices = self._get_latest_prices_batch(df["code"].tolist())
        for _, row in df.iterrows():
            code = str(row["code"]).zfill(6)
            buy_price = float(row["recommend_price"])
            if code not in latest_prices or buy_price <= 0:
                continue

            new_price = float(latest_prices[code])
            return_pct = round((new_price - buy_price) / buy_price * 100, 2)
            update_sql = "UPDATE paper_trades SET current_price = ?, return_pct = ? WHERE id = ?"
            self.db.execute_non_query(update_sql, (new_price, return_pct, int(row["id"])))
            logger.info(
                "更新仓位: {}({}), 成本: {}, 最新: {}, 收益: {}%",
                row["name"],
                code,
                buy_price,
                new_price,
                return_pct,
            )

    def diagnose_portfolio(self, macro_context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """为每只持仓生成盘后动作：继续持有、减仓观察、清仓退出、等待确认。"""
        df = self.db.query_to_dataframe(
            """
            SELECT id, name, code, recommend_price, current_price, return_pct, recommend_date, recommend_reason
            FROM paper_trades
            WHERE status = 'HOLD'
            ORDER BY recommend_date, id
            """
        )
        if df.empty:
            return []

        diagnostics: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            diagnostic = self._diagnose_position(row, macro_context=macro_context)
            diagnostics.append(diagnostic)
            if diagnostic["action"] == ACTION_CLEAR:
                self._close_trade(int(row["id"]), diagnostic["reason"])

        return diagnostics

    def _diagnose_position(self, row, macro_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return_pct = float(row["return_pct"])
        action, reason, action_level = self._diagnose_by_return_pct(return_pct)

        position = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        exit_signal = self.exit_agent.evaluate_position(position, macro_context=macro_context).to_dict()
        if int(exit_signal.get("action_level", 0)) > action_level:
            action = exit_signal["action"]
            reason = exit_signal["reason"]
            action_level = int(exit_signal.get("action_level", action_level))

        return {
            "id": int(row["id"]),
            "name": row["name"],
            "code": str(row["code"]).zfill(6),
            "recommend_price": float(row["recommend_price"]),
            "current_price": float(row["current_price"]),
            "return_pct": return_pct,
            "action": action,
            "reason": reason,
            "action_level": action_level,
            "exit_signal": exit_signal,
        }

    def _diagnose_by_return_pct(self, return_pct: float) -> tuple[str, str, int]:
        if return_pct <= float(self.rules["stop_loss_pct"]):
            return (
                ACTION_CLEAR,
                f"触发止损规则：收益 {return_pct}% <= {self.rules['stop_loss_pct']}%。",
                3,
            )
        if return_pct >= float(self.rules["take_profit_pct"]):
            return (
                "减仓观察",
                f"触发止盈规则：收益 {return_pct}% >= {self.rules['take_profit_pct']}%，锁定部分利润。",
                2,
            )
        if return_pct >= float(self.rules["reduce_watch_pct"]):
            return (
                "减仓观察",
                f"收益达到 {return_pct}%，建议降低仓位后观察趋势延续。",
                2,
            )
        if return_pct <= float(self.rules["wait_confirm_loss_pct"]):
            return (
                "等待确认",
                f"轻度回撤 {return_pct}%，未触发止损，下一交易日确认是否修复。",
                1,
            )
        return "继续持有", f"收益 {return_pct}% 仍在规则容忍区间内。", 0

    def _close_trade(self, trade_id: int, reason: str) -> None:
        self.db.execute_non_query("UPDATE paper_trades SET status = 'CLOSED' WHERE id = ?", (trade_id,))
        logger.warning("[Paper Trading] 观察仓触发清仓退出: trade_id={}, reason={}", trade_id, reason)

    def run_post_market_review(self, macro_context: Optional[Dict[str, Any]] = None) -> str:
        """执行完整盘后观察仓流程：刷新价格、诊断、输出报告。"""
        self.update_portfolio()
        diagnostics = self.diagnose_portfolio(macro_context=macro_context)
        return self.format_diagnostics_report(diagnostics)

    def format_diagnostics_report(self, diagnostics: List[Dict[str, Any]]) -> str:
        if not diagnostics:
            return "### AI 虚拟观察仓盘后诊断\n\n当前没有 HOLD 状态持仓。"

        report = "### AI 虚拟观察仓盘后诊断\n\n"
        report += "| 股票名称 | 股票代码 | 成本价 | 最新价 | 浮动盈亏 | 盘后动作 | 规则原因 |\n"
        report += "| :--- | :---: | :---: | :---: | :---: | :---: | :--- |\n"
        for item in diagnostics:
            report += (
                f"| {item['name']} | {item['code']} | {item['recommend_price']} | "
                f"{item['current_price']} | {item['return_pct']}% | {item['action']} | {item['reason']} |\n"
            )
        return report

    def show_portfolio(self) -> str:
        """展示当前仍在 HOLD 状态的观察仓。"""
        df = self.db.query_to_dataframe(
            """
            SELECT name, code, recommend_price, current_price, return_pct, recommend_date
            FROM paper_trades
            WHERE status = 'HOLD'
            """
        )
        if df.empty:
            return "### AI 虚拟观察仓\n\n当前没有 HOLD 状态持仓。"

        report = "### AI 虚拟观察仓\n\n"
        report += "| 股票名称 | 股票代码 | 推荐日 | 成本价 | 最新价 | 浮动盈亏 |\n"
        report += "| :--- | :---: | :---: | :---: | :---: | :---: |\n"

        total_pct = 0.0
        for _, row in df.iterrows():
            report += (
                f"| {row['name']} | {row['code']} | {row['recommend_date'][:10]} | "
                f"{row['recommend_price']} | {row['current_price']} | {row['return_pct']}% |\n"
            )
            total_pct += float(row["return_pct"])

        avg_pct = round(total_pct / len(df), 2)
        report += f"\n**当前持仓平均收益率:** {avg_pct}%\n"
        return report


if __name__ == "__main__":
    pt = PaperTrading()
    pt.add_trade("000001", "平安银行", "模拟 Agent 自动推荐测试")
    print(pt.run_post_market_review())
