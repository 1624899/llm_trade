"""交易智能体 (Trading agent)，用于管理模拟账户的交易决策。"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

from src.agent.exit_agent import ACTION_CLEAR, ACTION_REDUCE
from src.agent.tools import tools


class TradingAgent:
    """交易智能体类，根据观察仓（watchlist）和当前持仓（positions）生成可执行的模拟交易决策。"""

    def __init__(
        self,
        *,
        max_positions: int = 5,
        max_buys_per_run: int = 2,
        max_sells_per_run: int = 2,
        rules_path: str = "data/rules_book.txt",
    ):
        """
        初始化交易智能体。

        Args:
            max_positions: 最大持仓位数。
            max_buys_per_run: 每次运行最大的买入操作数。
            max_sells_per_run: 每次运行最大的卖出操作数。
            rules_path: 交易规则/反思记录的文件路径。
        """
        self.max_positions = int(max_positions)
        self.max_buys_per_run = int(max_buys_per_run)
        self.max_sells_per_run = int(max_sells_per_run)
        self.rules_path = rules_path

    def decide(
        self,
        *,
        watchlist: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        account: Dict[str, Any],
        exit_signals: Optional[Dict[str, Dict[str, Any]]] = None,
        macro_context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        生成最终的交易决策。如果是 LLM 决策失败，将使用兜底逻辑。

        Args:
            watchlist: 观察仓列表。
            positions: 当前持仓列表。
            account: 账户信息（余额等）。
            exit_signals: 退出信号字典。
            macro_context: 宏观环境信息（可选）。

        Returns:
            归一化后的决策列表。
        """
        exit_signals = exit_signals or {}
        
        # 1. 尝试获取 LLM 生成的决策
        llm_decisions = self._llm_decisions(
            watchlist=watchlist,
            positions=positions,
            account=account,
            exit_signals=exit_signals,
            macro_context=macro_context,
        )
        
        # 2. 如果存在 LLM 决策，则进行规范化处理（校验仓位、约束等）
        if llm_decisions:
            return self._normalize_decisions(llm_decisions, watchlist, positions, exit_signals, account)
        
        # 3. 兜底逻辑：如果 LLM 出错或未输出，执行基于规则的退出和观察仓买入
        return self._fallback_decisions(watchlist, positions, account, exit_signals)

    def _llm_decisions(
        self,
        *,
        watchlist: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        account: Dict[str, Any],
        exit_signals: Dict[str, Dict[str, Any]],
        macro_context: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """调用 LLM 生成初始交易决策 JSON。"""
        system_prompt = """
你是 LLM-TRADE 的交易 Agent，只管理一个模拟账户，不做真实交易。
你的任务是基于观察仓推荐、当前交易仓、宏观环境、退出信号和历史反思规则，输出克制、清晰、可执行的模拟交易决策。

硬约束：
1. 交易仓最多同时持有 5 只股票。
2. A 股按 100 股一手买卖，BUY/SELL 数量必须是 100 的整数倍；不要输出 50 股这类不可执行数量。
3. 禁止频繁交易，默认按 1-3 个月中期波段/配置视角持有，除非风险恶化，不要因为短期波动反复买卖。
4. 低风险、基本面和技术面未恶化、中期预期收益大于 15% 的股票，应偏向 HOLD/BUY，不要因为宏观环境短期偏弱或微小浮盈就 SELL。
5. SELL 只允许在黑天鹅、基本面反转/恶化、资讯硬风险、技术硬破位/趋势失效、或已接近/达到中期止盈目标时使用。
6. 只输出 JSON，不要输出 Markdown。

JSON 格式：
{
  "decisions": [
    {
      "code": "000001",
      "name": "股票名",
      "action": "BUY|SELL|HOLD|WATCH|REMOVE",
      "quantity": 0,
      "target_cash": 3000,
      "risk_override": false,
      "reason": "交易理由"
    }
  ]
}
"""
        user_prompt = json.dumps(
            {
                "account": account,
                "positions": positions,
                "watchlist": watchlist,
                "exit_signals": exit_signals,
                "macro_context": macro_context or {},
                "reflection_rules": self._load_reflection_rules(),
            },
            ensure_ascii=False,
            default=str,
        )
        try:
            raw = tools.call_llm(system_prompt, user_prompt, temperature=0.15)
        except Exception as exc:
            logger.warning("[交易智能体] LLM 决策失败: {}", exc)
            return []
            
        parsed = self._parse_json(raw)
        decisions = parsed.get("decisions") if isinstance(parsed, dict) else None
        return decisions if isinstance(decisions, list) else []

    def _normalize_decisions(
        self,
        decisions: Iterable[Dict[str, Any]],
        watchlist: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        exit_signals: Dict[str, Dict[str, Any]],
        account: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        对 LLM 输出的决策进行格式归一化和硬性业务约束校验。
        例如：代码补全 6 位、动作合法化、买入卖出数量限制、强制风险清仓等。
        """
        watch_map = {str(item.get("code")).zfill(6): item for item in watchlist}
        position_map = {str(item.get("code")).zfill(6): item for item in positions}
        normalized: List[Dict[str, Any]] = []
        buys = 0
        sells = 0

        for item in decisions:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").zfill(6)
            if not re.fullmatch(r"\d{6}", code):
                continue
                
            action = str(item.get("action") or "HOLD").upper()
            if action not in {"BUY", "SELL", "HOLD", "WATCH", "REMOVE"}:
                action = "HOLD"
            
            watch = watch_map.get(code, {})
            pos = position_map.get(code, {})
            signal = exit_signals.get(code, {})
            reason = str(item.get("reason") or signal.get("reason") or "交易 Agent 决策")

            # 执行硬性业务约束
            if action == "BUY":
                # 如果已持仓、不在观察仓、或买入次数超限，降级为观察（WATCH）
                if code in position_map or code not in watch_map or buys >= self.max_buys_per_run:
                    action = "WATCH"
                else:
                    buys += 1
            if action == "SELL":
                # 未持仓、卖出次数超限、非硬退出，或不满足整手交易时，降级为持有（HOLD）。
                quantity = self._normalized_sell_quantity(item.get("quantity"), pos)
                if (
                    code not in position_map
                    or sells >= self.max_sells_per_run
                    or not self._sell_allowed(item, pos, signal, reason)
                    or quantity <= 0
                ):
                    action = "HOLD"
                else:
                    sells += 1
                    item["quantity"] = quantity
            
            normalized.append(
                {
                    "code": code,
                    "name": item.get("name") or watch.get("name") or pos.get("name") or code,
                    "action": action,
                    "quantity": self._to_int(item.get("quantity")) or 0,
                    "target_cash": self._to_float(item.get("target_cash")) or self._default_target_cash(account, positions),
                    "price": self._to_float(item.get("price")) or watch.get("current_price") or pos.get("current_price"),
                    "risk_override": bool(item.get("risk_override")) or int(signal.get("action_level", 0) or 0) >= 3,
                    "reason": reason,
                    "linked_watchlist_id": watch.get("id"),
                    "exit_signal": signal,
                }
            )

        # 最后通过强力退出检查，确保特大风险标的必须被处理
        return self._force_exit_risk(normalized, positions, exit_signals)

    def _sell_allowed(
        self,
        decision: Dict[str, Any],
        position: Dict[str, Any],
        signal: Dict[str, Any],
        reason: str,
    ) -> bool:
        """Only allow exits for hard risk, hard technical failure, or real medium-term profit taking."""
        level = int(signal.get("action_level", 0) or 0)
        if level >= 3:
            return True

        text = " ".join(
            str(value or "")
            for value in (
                reason,
                signal.get("reason"),
                decision.get("risk_note"),
                " ".join(signal.get("risk_flags") or []),
                " ".join(signal.get("technical_tags") or []),
            )
        )
        hard_words = (
            "黑天鹅",
            "基本面恶化",
            "基本面反转",
            "业绩暴雷",
            "财务造假",
            "监管立案",
            "退市",
            "硬风险",
            "清仓",
            "趋势失效",
            "技术破位",
            "跌破",
            "破位",
            "ATR",
            "止损",
        )
        if any(word in text for word in hard_words):
            return True

        return_pct = self._to_float(position.get("unrealized_return_pct"))
        if return_pct is None:
            return_pct = self._to_float(position.get("return_pct"))
        return bool(level >= 2 and return_pct is not None and return_pct >= 15)

    def _normalized_sell_quantity(self, requested: Any, position: Dict[str, Any]) -> int:
        """Round SELL quantity to A-share board lots; selling the full position is allowed as-is."""
        quantity_before = int(position.get("quantity") or 0)
        quantity = self._to_int(requested) or quantity_before
        quantity = min(quantity_before, quantity)
        if quantity <= 0:
            return 0
        if quantity == quantity_before:
            return quantity
        return quantity - quantity % 100

    def _fallback_decisions(
        self,
        watchlist: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        account: Dict[str, Any],
        exit_signals: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """兜底决策逻辑：基于硬性规则判断买卖。"""
        decisions: List[Dict[str, Any]] = []
        held_codes = {str(item.get("code")).zfill(6) for item in positions}
        sells = 0
        
        # 1. 遍历持仓逻辑
        for pos in positions:
            code = str(pos.get("code")).zfill(6)
            signal = exit_signals.get(code, {})
            level = int(signal.get("action_level", 0) or 0)
            
            if level >= 3 and sells < self.max_sells_per_run:
                # 级别 3 以上强制清仓
                decisions.append(
                    {
                        "code": code,
                        "name": pos.get("name") or code,
                        "action": "SELL",
                        "quantity": int(pos.get("quantity") or 0),
                        "price": pos.get("current_price"),
                        "risk_override": True,
                        "reason": signal.get("reason") or "退出信号触发清仓",
                        "exit_signal": signal,
                    }
                )
                sells += 1
            elif level >= 2 and sells < self.max_sells_per_run:
                # 级别 2 只提示观察，不因宏观偏弱或微盈自动减仓；A 股一手 100 股，半仓可能不可执行。
                decisions.append(
                    {
                        "code": code,
                        "name": pos.get("name") or code,
                        "action": "HOLD",
                        "quantity": 0,
                        "price": pos.get("current_price"),
                        "risk_override": False,
                        "reason": (signal.get("reason") or "退出信号提示观察") + "；未出现硬退出条件，按中期持有纪律继续持有。",
                        "exit_signal": signal,
                    }
                )
            else:
                # 默认持有
                decisions.append(
                    {
                        "code": code,
                        "name": pos.get("name") or code,
                        "action": "HOLD",
                        "price": pos.get("current_price"),
                        "reason": "未触发清仓风险，遵守中期持有纪律",
                        "exit_signal": signal,
                    }
                )

        # 2. 遍历观察仓逻辑（补位买入）
        slots = max(0, self.max_positions - len(positions))
        buys = 0
        for item in watchlist:
            if buys >= self.max_buys_per_run or buys >= slots:
                break
                
            code = str(item.get("code")).zfill(6)
            if code in held_codes:
                continue
                
            if not self._is_actionable_watch(item):
                # 不符合买入条件的观察标的标记为 WATCH
                decisions.append(
                    {
                        "code": code,
                        "name": item.get("name") or code,
                        "action": "WATCH",
                        "price": item.get("current_price"),
                        "linked_watchlist_id": item.get("id"),
                        "reason": "观察仓标的尚未达到交易仓配置条件",
                    }
                )
                continue
                
            # 符合条件的强标的，触发买入 BUY
            decisions.append(
                {
                    "code": code,
                    "name": item.get("name") or code,
                    "action": "BUY",
                    "target_cash": self._default_target_cash(account, positions),
                    "price": item.get("current_price"),
                    "linked_watchlist_id": item.get("id"),
                    "reason": "观察仓为强推荐/配置标的，风险未恶化，进入交易仓试配",
                }
            )
            buys += 1
            
        return decisions

    def _force_exit_risk(
        self,
        decisions: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        exit_signals: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        强力风控：即使 LLM 没有给出卖出指令，由于观察到 action_level >= 3 的重大隐患，
        也必须在决策列表顶部插入强制卖出记录。
        """
        existing_codes = {str(item.get("code")).zfill(6) for item in decisions}
        sell_count = sum(1 for item in decisions if item.get("action") == "SELL")
        for pos in positions:
            code = str(pos.get("code")).zfill(6)
            signal = exit_signals.get(code, {})
            level = int(signal.get("action_level", 0) or 0)
            
            # 如果已有决策、信号级别不足、或卖出名额已满，则跳过
            if level < 3 or code in existing_codes or sell_count >= self.max_sells_per_run:
                continue
                
            decisions.insert(
                0,
                {
                    "code": code,
                    "name": pos.get("name") or code,
                    "action": "SELL",
                    "quantity": int(pos.get("quantity") or 0),
                    "price": pos.get("current_price"),
                    "risk_override": True,
                    "reason": signal.get("reason") or "硬退出信号触发清仓",
                    "exit_signal": signal,
                },
            )
            sell_count += 1
        return decisions

    def _is_actionable_watch(self, item: Dict[str, Any]) -> bool:
        """检查观察仓标的是否可以作为“买入点”。(由兜底逻辑调用)"""
        tier = str(item.get("tier") or "")
        # 仅限强推荐或验证级
        if tier not in {"强推荐", "配置/轻仓验证"}:
            return False
            
        # 检查理由和风险分析中是否有负面关键词
        text = " ".join(
            str(item.get(key) or "")
            for key in ("recommend_reason", "fundamental_analysis", "technical_analysis", "news_risk_analysis")
        )
        bad_words = ("立案", "减持", "破位", "恶化", "背离", "放量滞涨", "趋势失效", "禁止")
        if any(word in text for word in bad_words):
            return False
            
        expected = self._to_float(item.get("expected_return_pct"))
        if tier == "强推荐":
            return True
        # 配置档位需要预期收益 >= 15%
        return expected is None or expected >= 15

    def _default_target_cash(self, account: Dict[str, Any], positions: List[Dict[str, Any]]) -> float:
        """计算单一标的的最大可用购买金额（平铺到剩余仓位槽）。"""
        cash = self._to_float(account.get("cash")) or 0.0
        remaining_slots = max(1, self.max_positions - len(positions))
        return round(min(cash, cash / remaining_slots), 2)

    def _load_reflection_rules(self) -> str:
        """从文件加载历史反思规则，以便 LLM 学习。"""
        try:
            if not os.path.exists(self.rules_path):
                return ""
            with open(self.rules_path, "r", encoding="utf-8") as file:
                # 仅获取最后 4000 字符（防止上下文溢出）
                return file.read()[-4000:]
        except Exception as exc:
            logger.warning("[交易智能体] 加载反思规则失败: {}", exc)
            return ""

    @staticmethod
    def _parse_json(text: str) -> Dict[str, Any]:
        """解析 LLM 返回的 JSON 文本。"""
        if not text:
            return {}
        cleaned = text.strip()
        # 匹配 Markdown 代码块中的内容
        fenced = re.search(r"```(?:json)?\s*(.*?)```", cleaned, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            cleaned = fenced.group(1).strip()
        # 如果不是以 { 开头，尝试正则定位
        if not cleaned.startswith("{"):
            match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
            cleaned = match.group(0) if match else cleaned
        try:
            parsed = json.loads(cleaned)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _to_float(value: Any) -> float | None:
        """安全转换为浮点数。"""
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        """安全转换为整数。"""
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None
