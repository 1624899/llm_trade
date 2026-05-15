"""交易工作流 Mixin。

从 coordinator.py 拆分：交易工作流辅助方法、观察仓维护、退出信号。
"""

from typing import Any, Dict, List, Optional

from loguru import logger


class CoordinatorTradingMixin:
    """交易工作流辅助方法。"""

    def _watchlist_needs_trade_refresh(
        self,
        watch_items: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
    ) -> bool:
        """观察仓为空，或缺少当前持仓分析时，都需要先维护候选池。"""
        if not watch_items:
            return True
        watch_codes = {str(item.get("code") or "").zfill(6) for item in watch_items}
        position_codes = {str(item.get("code") or "").zfill(6) for item in positions if item.get("code")}
        return bool(position_codes - watch_codes)

    def _ensure_watchlist_for_trade(
        self,
        watch_items: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """交易前维护观察仓：空仓或缺少持仓分析时先补候选。"""
        watch_codes = {str(item.get("code") or "").zfill(6) for item in watch_items}
        position_codes = [str(item.get("code") or "").zfill(6) for item in positions if item.get("code")]
        position_codes = [code for code in position_codes if len(code) == 6 and code.isdigit()]
        missing_position_codes = [code for code in position_codes if code not in watch_codes]
        if missing_position_codes:
            logger.warning(
                "[Watchlist] 观察仓缺少当前交易持仓分析，先执行指定分析并写回观察仓: {}",
                missing_position_codes,
            )
            self.run_targeted_analysis(
                missing_position_codes,
                update_watchlist=True,
                watchlist_source="trade_position_refresh",
            )
        elif not watch_items:
            logger.warning("[Watchlist] 观察仓和交易仓均为空，先运行一轮选股流程生成交易候选池。")
            self.run_picking_workflow()
        return self.watchlist.list_active()

    def _build_exit_signals(
        self,
        positions: List[Dict[str, Any]],
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        signals: Dict[str, Dict[str, Any]] = {}
        for position in positions:
            code = str(position.get("code") or "").zfill(6)
            if not code:
                continue
            exit_position = dict(position)
            exit_position["recommend_price"] = position.get("avg_cost")
            exit_position["return_pct"] = position.get("unrealized_return_pct")
            try:
                signals[code] = self.exit_agent.evaluate_position(
                    exit_position,
                    macro_context=macro_context,
                ).to_dict()
            except Exception as exc:
                logger.warning("[TradingWorkflow] exit signal failed for {}: {}", code, exc)
        return signals
