"""AgentTools K 线与市场情绪能力（Mixin）。

从 tools.py 拆分：K 线获取、多周期行情、市场情绪等方法。
"""

from datetime import datetime, timedelta

import akshare as ak
from loguru import logger

from src.database import StockDatabase
from src.market_sentiment import MarketSentiment
from src.technical_indicators import TechnicalSignalProvider


class MarketDataMixin:
    """K 线获取、市场情绪、多周期行情相关方法。"""

    def fetch_market_sentiment(self) -> str:
        """Build a free market sentiment snapshot from local quotes and Sina boards."""
        try:
            return MarketSentiment().format_for_prompt()
        except Exception as e:
            logger.warning(f"Build market sentiment failed: {e}")
            return "Market sentiment snapshot is unavailable."

    def fetch_recent_kline(self, symbol: str, days: int = 10) -> str:
        """获取最近若干天的日K线数据，归纳为文本表格。"""
        local_text = self._fetch_local_kline_text(symbol, "daily", days)
        if local_text:
            return local_text

        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")

            if df is not None and not df.empty:
                df = df.tail(days)
                kline_text = "日期      收盘价   涨跌幅%   成交额\n"
                for _, row in df.iterrows():
                    d = row.get("日期", "")
                    c = row.get("收盘", 0)
                    pct = row.get("涨跌幅", 0)
                    amt = row.get("成交额", 0) / 100000000
                    kline_text += f"{d}  {c}    {pct}%    {amt:.2f}亿\n"
                return kline_text
            return "无法获取K线。"
        except Exception as e:
            logger.warning(f"获取K线数据失败 {symbol}: {e}")
            return "K线数据拉取异常。"

    def fetch_multi_period_kline(self, symbol: str, realtime_quote: dict | None = None) -> str:
        """获取日/周/月多周期K线摘要，优先使用本地数据湖。"""
        sections = []
        period_specs = [("daily", 12, "日线"), ("weekly", 12, "周线"), ("monthly", 8, "月线")]

        for period, limit, title in period_specs:
            text = self._fetch_local_kline_text(
                symbol,
                period,
                limit,
                title=title,
                realtime_quote=realtime_quote if period == "daily" else None,
            )
            if text:
                sections.append(text)

        if sections:
            return "\n\n".join(sections)

        return self.fetch_recent_kline(symbol, days=12)

    def _fetch_local_kline_text(
        self,
        symbol: str,
        period: str,
        limit: int,
        title: str = "日线",
        realtime_quote: dict | None = None,
    ) -> str:
        """从本地 market_bars 表读取 K 线摘要。"""
        try:
            db = StockDatabase()
            normalized_symbol = str(symbol).zfill(6)
            if period == "daily":
                query = """
                    WITH recent_bars AS (
                        SELECT trade_date, close, volume, amount
                        FROM market_bars
                        WHERE code = ? AND period = 'daily'
                        ORDER BY trade_date DESC
                        LIMIT ?
                    )
                    SELECT
                        b.trade_date,
                        COALESCE(q.price, b.close) AS close,
                        b.volume,
                        b.amount
                    FROM recent_bars b
                    LEFT JOIN daily_quotes q
                      ON q.code = ? AND q.trade_date = b.trade_date AND q.price > 0
                    ORDER BY b.trade_date DESC
                """
                df = db.query_to_dataframe(query, (normalized_symbol, limit, normalized_symbol))
                df = TechnicalSignalProvider.apply_realtime_quote_to_daily_bars(df, realtime_quote)
            else:
                table_map = {
                    "weekly": "market_bars_weekly",
                    "monthly": "market_bars_monthly",
                }
                table_name = table_map.get(period)
                if table_name is None:
                    return ""
                query = f"""
                    SELECT trade_date, close, volume, amount
                    FROM {table_name}
                    WHERE code = ?
                    ORDER BY trade_date DESC
                    LIMIT ?
                """
                df = db.query_to_dataframe(query, (normalized_symbol, limit))
            if df is None or df.empty:
                return ""

            df = df.sort_values("trade_date")
            kline_text = f"{title}K线  日期      收盘价   涨跌幅%   成交额\n"
            prev_close = None
            for _, row in df.iterrows():
                close = row.get("close", 0)
                pct = ""
                if prev_close and prev_close != 0:
                    pct = f"{(close / prev_close - 1) * 100:.2f}%"
                prev_close = close

                amount = row.get("amount", 0)
                amount_text = "-"
                if amount and amount == amount:
                    amount_text = f"{amount / 100000000:.2f}亿"

                kline_text += f"{row.get('trade_date', '')}  {close:.2f}    {pct or '-'}    {amount_text}\n"
            return kline_text
        except Exception as e:
            logger.warning(f"读取本地K线失败 {symbol}/{period}: {e}")
            return ""
