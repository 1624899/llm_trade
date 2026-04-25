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
