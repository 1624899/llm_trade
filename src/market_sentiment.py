from datetime import datetime
from typing import Dict

import pandas as pd
from loguru import logger

from src.database import StockDatabase
from src.market_extras import fetch_sina_board_list


class MarketSentiment:
    """基于本地行情快照和新浪板块数据构建 A 股市场情绪快照。"""

    def __init__(self, db: StockDatabase = None):
        self.db = db or StockDatabase()

    def build_snapshot(self, top_n: int = 8) -> Dict:
        """
        构建完整的市场情绪快照。
        包含：市场涨跌分布汇总、热门行业/概念板块、情绪评分及标签。
        """
        quotes = self._load_latest_quotes()
        board_strength = self._load_board_strength(top_n=top_n)

        snapshot = {
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "local_quotes_or_bars+sina_free_boards",
            "market": self._summarize_quotes(quotes),
            "hot_industries": board_strength.get("industry", []),
            "hot_concepts": board_strength.get("concept", []),
        }
        # 计算综合评分和标签
        snapshot["score"] = self._score(snapshot)
        snapshot["label"] = self._label(snapshot["score"])
        return snapshot

    def format_for_prompt(self, top_n: int = 8) -> str:
        """
        将快照格式化为便于 LLM 理解的字符串。
        """
        snapshot = self.build_snapshot(top_n=top_n)
        market = snapshot["market"]
        lines = [
            f"市场情绪: {snapshot['label']} ({snapshot['score']:.1f}/100)",
            f"涨/跌家数: {market.get('up_count', 0)}/{market.get('down_count', 0)}, "
            f"涨停(估): {market.get('limit_up_count', 0)}, "
            f"跌停(估): {market.get('limit_down_count', 0)}, "
            f"涨跌幅中位数: {market.get('median_change_pct', 0):.2f}%",
        ]
        if snapshot["hot_industries"]:
            lines.append("热门行业: " + ", ".join(item["name"] for item in snapshot["hot_industries"][:top_n]))
        if snapshot["hot_concepts"]:
            lines.append("热门概念: " + ", ".join(item["name"] for item in snapshot["hot_concepts"][:top_n]))
        return "\n".join(lines)

    def _load_latest_quotes(self) -> pd.DataFrame:
        """从本地数据库加载最新的行情快照数据。"""
        """从本地数据湖加载最新可用行情，优先使用日期更新的 K 线数据。"""
        quote_date = self._latest_trade_date("daily_quotes")
        bar_date = self._latest_trade_date("market_bars", period="daily")
        if bar_date and (not quote_date or bar_date > quote_date):
            return self._load_latest_market_bars_snapshot()
        return self._load_latest_daily_quotes()

    def _latest_trade_date(self, table_name: str, period: str | None = None) -> str:
        where_clause = " WHERE period = 'daily'" if period else ""
        df = self.db.query_to_dataframe(f"SELECT MAX(trade_date) AS latest FROM {table_name}{where_clause}")
        if df is None or df.empty or "latest" not in df.columns:
            return ""
        value = df.iloc[0].get("latest")
        return "" if pd.isna(value) else str(value)

    def _load_latest_daily_quotes(self) -> pd.DataFrame:
        """从 daily_quotes 加载最新行情快照。"""
        query = """
            SELECT *
            FROM daily_quotes
            WHERE trade_date = (SELECT MAX(trade_date) FROM daily_quotes)
        """
        return self.db.query_to_dataframe(query)

    def _load_latest_market_bars_snapshot(self) -> pd.DataFrame:
        """从 market_bars 计算最新交易日相对前一交易日的涨跌幅。"""
        query = """
            WITH latest_bars AS (
                SELECT code, trade_date, close
                FROM market_bars
                WHERE period = 'daily'
                  AND trade_date = (
                      SELECT MAX(trade_date)
                      FROM market_bars
                      WHERE period = 'daily'
                        AND code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
                  )
                  AND code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
            ),
            previous_bars AS (
                SELECT p.code, p.close
                FROM market_bars p
                JOIN latest_bars lb ON lb.code = p.code
                WHERE p.period = 'daily'
                  AND p.trade_date = (
                      SELECT MAX(p2.trade_date)
                      FROM market_bars p2
                      WHERE p2.period = 'daily'
                        AND p2.code = lb.code
                        AND p2.trade_date < lb.trade_date
                  )
            )
            SELECT
                lb.code,
                lb.trade_date,
                lb.close AS price,
                (lb.close / pb.close - 1) * 100 AS change_pct
            FROM latest_bars lb
            JOIN previous_bars pb ON pb.code = lb.code
            WHERE pb.close > 0
              AND lb.close IS NOT NULL
        """
        return self.db.query_to_dataframe(query)

    def _summarize_quotes(self, quotes: pd.DataFrame) -> Dict:
        """
        对行情快照进行统计汇总，计算涨跌分布。
        """
        if quotes is None or quotes.empty or "change_pct" not in quotes.columns:
            return {}

        change = pd.to_numeric(quotes["change_pct"], errors="coerce").dropna()
        if change.empty:
            return {}

        return {
            "stock_count": int(len(change)),
            "up_count": int((change > 0).sum()),
            "down_count": int((change < 0).sum()),
            "flat_count": int((change == 0).sum()),
            "limit_up_count": int((change >= 9.8).sum()),
            "limit_down_count": int((change <= -9.8).sum()),
            "strong_count": int((change >= 5).sum()),      # 大涨家数
            "weak_count": int((change <= -5).sum()),        # 大跌家数
            "avg_change_pct": float(change.mean()),
            "median_change_pct": float(change.median()),
        }

    def _load_board_strength(self, top_n: int) -> Dict[str, list]:
        """
        从新浪财经加载热门行业和概念板块的强度。
        """
        result = {"industry": [], "concept": []}
        for kind in ("industry", "concept"):
            try:
                boards = fetch_sina_board_list(kind)
            except Exception as exc:
                logger.warning(f"Failed to fetch Sina {kind} boards for sentiment: {exc}")
                continue
            if boards is None or boards.empty or "change_pct" not in boards.columns:
                continue
            # 按涨跌幅排序并取前 top_n
            boards = boards.sort_values("change_pct", ascending=False).head(top_n)
            result[kind] = [
                {
                    "name": row.get("name"),
                    "change_pct": row.get("change_pct"),
                    "leader_name": row.get("leader_name"),
                    "leader_change_pct": row.get("leader_change_pct"),
                }
                for _, row in boards.iterrows()
            ]
        return result

    def _score(self, snapshot: Dict) -> float:
        """
        计算市场情绪评分 (0-100)。
        评分基于涨跌比、大涨/大跌比、涨跌停差值以及涨跌幅中位数。
        """
        market = snapshot.get("market") or {}
        total = market.get("stock_count") or 0
        if total <= 0:
            return 50.0

        up_ratio = market.get("up_count", 0) / total
        strong_ratio = market.get("strong_count", 0) / total
        weak_ratio = market.get("weak_count", 0) / total
        limit_balance = (market.get("limit_up_count", 0) - market.get("limit_down_count", 0)) / max(total, 1)
        median = market.get("median_change_pct", 0) or 0

        # 评分公式：以50为基准，根据各项指标加减分
        score = 50 + (up_ratio - 0.5) * 45 + (strong_ratio - weak_ratio) * 35 + limit_balance * 80 + median * 2
        return max(0.0, min(100.0, float(score)))

    def _label(self, score: float) -> str:
        """根据评分划分情绪标签。"""
        if score >= 70:
            return "risk-on"     # 极度乐观/风险偏好开启
        if score >= 55:
            return "positive"    # 偏向乐观
        if score <= 30:
            return "risk-off"    # 极度悲观/避险开启
        if score <= 45:
            return "weak"        # 偏向悲观
        return "neutral"         # 中性
