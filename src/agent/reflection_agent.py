"""
错题反思智能体 (Reflection Agent)
职责：
1. 定期扫描虚拟仓中处于亏损状态（如跌幅过大）的标的。
2. 将入选原因与当前走势喂给 LLM 进行自省，提取教训（例如“以后不能在缩量阴跌时盲目判定企稳”）。
3. 将反思教训固化为系统风控规则库（rules_book.txt），供下一周期的 DecisionAgent 加载。
"""

import os
from datetime import datetime
from loguru import logger

from src.database import StockDatabase
from src.agent.tools import tools
from src.evaluation.paper_trading import PaperTrading

class ReflectionAgent:
    def __init__(self):
        self.db = StockDatabase()
        logger.info("初始化错题反思系统 (Reflection Agent) ...")
        
        self.rules_path = os.path.join(os.path.dirname(self.db.db_path), "rules_book.txt")

    def generate_reflection_for_failures(self, threshold_pct: float = -3.0):
        """
        寻找浮亏超过 threshold_pct 的标的，生成失败复盘记录
        """
        logger.info(f"[Reflection] 开始巡视虚拟仓，阈值：浮亏超 {threshold_pct}% 的标的将进入错题本。")
        
        sql = f"SELECT * FROM paper_trades WHERE status = 'HOLD' AND return_pct <= {threshold_pct}"
        df = self.db.query_to_dataframe(sql)
        
        if df.empty:
            logger.info("[Reflection] 本期未发现需要触发深度反思的亏损标的，体系运转良好。")
            return
            
        new_rules = []
        for _, row in df.iterrows():
            code = row['code']
            name = row['name']
            buy_p = row['recommend_price']
            cur_p = row['current_price']
            pct = row['return_pct']
            reason = row['recommend_reason']
            
            logger.warning(f"-> 发现亏损案例: {name} (成本: {buy_p}, 现价: {cur_p}, 多仓收益: {pct}%)")
            
            # 再去拉一下最新的这几天的K线，看看这几天是不是真暴跌了
            recent_kline = tools.fetch_recent_kline(code, days=5)
            
            system_prompt = """
            你是一位具有深刻反思意识的资深量化交易员导师。
            你的徒弟最近推荐了一只股票导致亏损，下面是当初的买入理由和目前的走势。
            任务：
            请用一句话提取一个【风控铁律】，防止以后再犯类似的错误。
            （语气要严厉深刻，不废话，只输出一条几十个字的核心原则）
            """
            
            user_prompt = f"股票：{name}({code})\n建仓理由：{reason}\n近期走势(惨淡)：\n{recent_kline}\n亏损率：{pct}%"
            
            rule = tools.call_llm(system_prompt, user_prompt, temperature=0.3)
            if rule:
                clean_rule = rule.replace('"', '').replace('风控铁律：', '').replace('风控铁律:', '').strip()
                new_rules.append(f"[{datetime.now().strftime('%Y-%m-%d')}] 惨痛教训({name}): {clean_rule}")

        # 如果提取出了新的反思，固化到磁盘
        if new_rules:
            self._save_rules(new_rules)

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
            system_prompt = """
你是一位成熟、严格的交易复盘导师。
请基于推荐内容、实际交易行为、持有结果和近期走势，提炼一条可复用的风控铁律。
只输出一条几十字的规则，不要寒暄，不要写长文。
"""
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
            if rule:
                clean_rule = (
                    rule.replace('"', "")
                    .replace("风控铁律：", "")
                    .replace("风控铁律:", "")
                    .strip()
                )
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
    agent.generate_reflection_for_failures()
