"""
错题反思智能体 (Reflection Agent)
职责：
1. 只扫描已清仓且亏损的交易仓样本。
2. 将推荐理由、实际交易行为、卖出结果与近期走势喂给 LLM 复盘。
3. 将反思教训固化为系统风控规则库（rules_book.txt），供下一周期的 DecisionAgent 加载。
"""

import os
import re
from datetime import datetime
from loguru import logger

from src.database import StockDatabase
from src.agent.tools import tools

class ReflectionAgent:
    def __init__(self):
        self.db = StockDatabase()
        logger.info("初始化错题反思系统 (Reflection Agent) ...")
        
        self.rules_path = os.path.join(os.path.dirname(self.db.db_path), "rules_book.txt")

    def generate_reflection_for_failures(self, threshold_pct: float = -3.0):
        """
        兼容旧入口：观察仓和未清仓持仓不做反思，避免把未完成样本写成规则。
        """
        logger.info("[Reflection] 跳过观察仓/未清仓样本反思；仅已清仓交易仓亏损样本会沉淀规则。")
        return

    def generate_trading_reflections(self, threshold_pct: float = -3.0):
        """基于已结算交易仓样本生成亏损反思；同一笔清仓记录只反思一次。"""
        logger.info(f"[Reflection] 开始扫描已结算交易仓亏损案例，阈值：{threshold_pct}%")
        cases = self._load_trading_failure_cases(threshold_pct)
        if not cases:
            logger.info("[Reflection] 交易仓本期未发现需要反思的亏损案例。")
            return

        new_rules = []
        reflected_position_ids = []
        for case in cases:
            position_id = case.get("position_id")
            if position_id is not None:
                reflected_position_ids.append(int(position_id))
            code = str(case.get("code") or "").zfill(6)
            name = case.get("name") or code
            recent_kline = tools.fetch_recent_kline(code, days=5)
            system_prompt = self._build_reflection_prompt()
            user_prompt = f"""
股票：{name}({code})
亏损幅度：{case.get('return_pct')}%
推荐分层：{case.get('tier')}
推荐理由：{case.get('recommend_reason')}
基本面分析：{case.get('fundamental_analysis')}
技术面分析：{case.get('technical_analysis')}
资讯风控：{case.get('news_risk_analysis')}
买入理由：{case.get('buy_reason')}
卖出/持有动作理由：{case.get('trade_reason')}
买入成本：{case.get('avg_cost')}
当前/卖出价格：{case.get('price')}
持有天数：{case.get('holding_days')}
近期走势：
{recent_kline}
"""
            rule = tools.call_llm(system_prompt, user_prompt, temperature=0.25)
            clean_rule = self._clean_reflection_rule(rule)
            if clean_rule:
                new_rules.append(f"[{datetime.now().strftime('%Y-%m-%d')}] 交易亏损复盘({name}): {clean_rule}")

        if new_rules:
            self._save_rules(new_rules)
        if reflected_position_ids:
            self._mark_trading_cases_reflected(reflected_position_ids)

    def _load_trading_failure_cases(self, threshold_pct: float) -> list:
        sell_cases = self.db.query_to_dataframe(
            """
            SELECT
                p.id AS position_id,
                p.code,
                COALESCE(p.name, w.name, p.code) AS name,
                p.current_price AS price,
                p.sold_quantity AS quantity,
                latest_sell.reason AS trade_reason,
                COALESCE(p.closed_at, p.last_sell_at) AS created_at,
                p.avg_cost,
                p.opened_at,
                p.buy_reason,
                w.tier,
                w.recommend_reason,
                w.fundamental_analysis,
                w.technical_analysis,
                w.news_risk_analysis,
                p.realized_return_pct AS return_pct
            FROM trading_positions p
            LEFT JOIN (
                SELECT account_id, code, reason, MAX(created_at) AS created_at
                FROM trade_orders
                WHERE action = 'SELL'
                GROUP BY account_id, code
            ) latest_sell ON latest_sell.account_id = p.account_id AND latest_sell.code = p.code
            LEFT JOIN watchlist_items w ON w.code = p.code
            WHERE p.status = 'CLOSED'
              AND p.avg_cost > 0
              AND p.realized_return_pct <= ?
              AND COALESCE(p.reflection_count, 0) = 0
              AND p.last_reflected_at IS NULL
            ORDER BY COALESCE(p.closed_at, p.last_sell_at, p.opened_at) DESC
            LIMIT 20
            """,
            (float(threshold_pct),),
        )
        rows = []
        if sell_cases is None or sell_cases.empty:
            return rows
        for _, row in sell_cases.iterrows():
            item = row.to_dict()
            item["holding_days"] = self._holding_days(item.get("opened_at"), item.get("created_at"))
            rows.append(item)
        return rows

    def _mark_trading_cases_reflected(self, position_ids: list[int]) -> None:
        """标记已处理的清仓记录，避免每日盘后重复反思同一笔亏损。"""
        if not position_ids:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        unique_ids = sorted(set(int(item) for item in position_ids if item))
        placeholders = ",".join(["?"] * len(unique_ids))
        self.db.execute_non_query(
            f"""
            UPDATE trading_positions
            SET last_reflected_at = ?,
                reflection_count = COALESCE(reflection_count, 0) + 1
            WHERE id IN ({placeholders})
            """,
            (now, *unique_ids),
        )

    @staticmethod
    def _holding_days(opened_at, ended_at) -> int | None:
        try:
            start = datetime.strptime(str(opened_at)[:19], "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(str(ended_at)[:19], "%Y-%m-%d %H:%M:%S")
            return max(0, (end - start).days)
        except Exception:
            return None

    @staticmethod
    def _build_reflection_prompt() -> str:
        """生成约束更强的复盘提示词，避免把单个亏损样本泛化成口号。"""
        return """
你是一位成熟、严格但不武断的交易复盘导师。
请只基于用户给出的推荐理由、实际交易行为、持有结果和近期走势，提炼一条可执行的复盘规则。

输出要求：
1. 只输出一条中文规则，40-90 字。
2. 必须写清【触发条件】和【动作】，例如“当...且...时，...；若...则...”。
3. 只能总结本案例证据支持的情境，不要扩展到所有突破、所有回踩或所有基本面股票。
4. 避免口号化和绝对化措辞，不要使用“一律、永远、绝不、任何、必然、铁律、惨痛”等词。
5. 如果输入证据不足以归因，请只输出“证据不足：不沉淀规则”。
"""

    @staticmethod
    def _clean_reflection_rule(rule: str | None) -> str:
        """清洗并拒收明显空泛的复盘结果。"""
        if not rule:
            return ""

        clean_rule = str(rule).strip().strip('"“”')
        clean_rule = re.sub(r"^(风控铁律|复盘规则|规则|教训)\s*[:：]\s*", "", clean_rule)
        clean_rule = clean_rule.replace("\n", " ").strip()
        clean_rule = re.sub(r"\s+", " ", clean_rule)

        if not clean_rule or clean_rule.startswith("证据不足"):
            return ""

        # 这类词会把单个亏损样本硬套成普适禁令，写入规则本前先降噪。
        replacements = {
            "一律": "应先",
            "永远": "持续",
            "绝不": "避免",
            "任何": "未确认的",
            "必然": "可能",
            "铁律": "规则",
            "惨痛": "",
        }
        for old, new in replacements.items():
            clean_rule = clean_rule.replace(old, new)

        # 仍然只有态度、没有条件或动作的句子，不进入规则本。
        has_condition = any(key in clean_rule for key in ("当", "若", "如果", "出现", "跌破", "破位", "未", "且", "时", "后"))
        has_action = any(key in clean_rule for key in ("等待", "降低", "减仓", "清仓", "止损", "回避", "避免", "不追", "不买", "不要", "卖出"))
        if len(clean_rule) < 10 or not (has_condition and has_action):
            return ""

        return clean_rule
            
    def _save_rules(self, new_rules: list):
        """持久化教训库"""
        os.makedirs(os.path.dirname(self.rules_path), exist_ok=True)
        try:
            with open(self.rules_path, "a", encoding="utf-8") as f:
                for rule in new_rules:
                    f.write(rule + "\n")
            logger.info(f"[Reflection] 成功将 {len(new_rules)} 条新教训刻入系统风控法则库！")
        except Exception as e:
            logger.error(f"[Reflection] 保存反思库失败: {e}")

if __name__ == "__main__":
    agent = ReflectionAgent()
    agent.generate_trading_reflections()
