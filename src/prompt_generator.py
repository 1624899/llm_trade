"""
标准化语料生成模块
将市场数据、技术指标和账户信息转换为LLM可理解的标准化格式，包含新增的实时数据和增强指标
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from loguru import logger
try:
    from .utils import get_trading_minutes, get_current_time_str, format_number, load_etf_list
except ImportError:
    from utils import get_trading_minutes, get_current_time_str, format_number, load_etf_list


class PromptGenerator:
    """语料生成器"""
    
    def __init__(self, config: Dict):
        """
        初始化语料生成器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.account_data = None
        self.trading_minutes = 0
        self.current_time = get_current_time_str()
        self.etf_list = load_etf_list()
        
        logger.info("语料生成器初始化完成")
    
    def generate_trading_prompt(self, market_data: Dict[str, Any],
                              account_data: Dict[str, Any]) -> str:
        """
        生成交易分析提示词
        
        Args:
            market_data: 市场数据字典
            account_data: 账户数据字典
            
        Returns:
            标准化交易提示词
        """
        try:
            # 更新账户数据和交易分钟数
            self.account_data = account_data
            account_info = account_data.get('account_info', {})
            start_time_str = account_info.get('start_time')
            self.trading_minutes = get_trading_minutes(start_time_str)
            
            # 生成头部信息
            header = self._generate_header(account_data)
            
            # 生成历史分析数据部分
            history_section = self._generate_history_section()
            
            # 生成ETF数据部分
            etf_sections = self._generate_etf_sections(market_data)
            
            # 生成市场情绪数据部分
            market_sentiment_section = self._generate_market_sentiment_section(market_data)
            
            # 生成账户信息部分
            account_section = self._generate_account_section(account_data)
            
            # 生成绩效指标部分
            performance_section = self._generate_performance_section(account_data)
            
            # 生成分析请求
            analysis_request = self.generate_analysis_request()
            
            # 组合完整的提示词
            prompt = f"{header}\n\n{history_section}\n\n{etf_sections}\n\n{market_sentiment_section}\n\n{account_section}\n\n{performance_section}\n\n{analysis_request}"
            
            logger.info("交易提示词生成成功")
            return prompt
            
        except Exception as e:
            logger.error(f"生成交易提示词失败: {e}")
            return ""
    
    def _generate_header(self, account_data: Dict[str, Any]) -> str:
        """
        生成提示词头部
        
        Args:
            account_data: 账户数据
            
        Returns:
            头部字符串
        """
        account_info = account_data.get('account_info', {})
        call_count = account_info.get('call_count', 0)
        
        header = f"""自您开始交易以来已经过去了 {self.trading_minutes} 分钟。当前时间是 {self.current_time}，您已被调用 {call_count} 次。接下来，我会为您提供各种状态数据、价格数据和预测信号。下面是您的当前账户信息、价值、表现、头寸等。

以下所有价格或信号数据均按顺序排列：最旧→最新"""
        
        return header
    
    def _generate_etf_sections(self, market_data: Dict[str, Any]) -> str:
        """
        生成ETF数据部分
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            ETF数据字符串
        """
        etf_sections = []
        
        for etf_code, etf_data in market_data.items():
            # 跳过市场情绪数据
            if etf_code.startswith('_'):
                continue
                
            try:
                section = self._generate_single_etf_section(etf_code, etf_data)
                if section:
                    etf_sections.append(section)
            except Exception as e:
                logger.error(f"生成ETF {etf_code} 数据部分失败: {e}")
                continue
        
        return "\n\n---\n\n".join(etf_sections)
    
    def _generate_market_sentiment_section(self, market_data: Dict[str, Any]) -> str:
        """
        生成市场情绪数据部分
        
        注意：行业资金流向数据为当日数据，但非实时更新
        
        Args:
            market_data: 市场数据字典
            
        Returns:
            市场情绪数据字符串
        """
        try:
            # 检查是否有市场情绪数据
            market_sentiment_data = market_data.get('_market_sentiment', [])
            
            if not market_sentiment_data or not isinstance(market_sentiment_data, list):
                return "### 市场情绪指标\n\n暂无市场情绪数据。"
            
            # 生成市场情绪数据部分
            section = "### 市场情绪指标（行业资金流向）\n\n"
            section += "⚠️ **注意**：以下行业资金流向数据为当日数据，但非实时更新\n\n"
            
            # 只显示前10个行业
            top_industries = market_sentiment_data[:10]
            
            for industry in top_industries:
                rank = industry.get('rank', 0)
                industry_name = industry.get('industry', '')
                index_value = industry.get('index', 0)
                change_pct = industry.get('change_pct', 0)
                
                section += f"**{rank}. {industry_name}**\n"
                section += f"- 行业指数: {index_value:.2f}，涨跌幅: {change_pct:+.2f}%\n"
                
                # 添加资金流入流出信息
                inflow = industry.get('inflow', 0)
                outflow = industry.get('outflow', 0)
                net_amount = industry.get('net_amount', 0)
                # 统一使用亿元作为单位显示
                unit = "亿元"
                
                section += f"- 资金流入: {inflow:.2f}{unit}，资金流出: {outflow:.2f}{unit}，净流入: {net_amount:+.2f}{unit}\n\n"
            
            return section
            
        except Exception as e:
            logger.error(f"生成市场情绪数据部分失败: {e}")
            return "### 市场情绪指标\n\n市场情绪数据获取失败。"
            
    def _generate_single_etf_section(self, etf_code: str, etf_data: Dict[str, Any]) -> str:
        """
        生成单个ETF数据部分
        
        Args:
            etf_code: ETF代码
            etf_data: ETF数据
            
        Returns:
            单个ETF数据字符串
        """
        try:
            # 获取基本信息，优先从配置文件获取标准名称
            etf_name = self._get_etf_name_from_config(etf_code) or etf_data.get('name', f'ETF{etf_code}')
            category = self._get_etf_category_from_config(etf_code) or etf_data.get('category', '其他')
            
            # 获取当前价格和指标
            current_data = etf_data.get('current_data', {})
            current_price = current_data.get('current_price', 0)
            current_ema20 = current_data.get('current_ema_long', 0)
            current_macd = current_data.get('current_macd', 0)
            current_rsi7 = current_data.get('current_rsi_7', 0)
            
            # 获取新增的增强指标
            trend_strength = current_data.get('trend_strength', 0)
            support = current_data.get('support', 0)
            resistance = current_data.get('resistance', 0)
            volatility = current_data.get('volatility', 0)
            
            # 获取日内数据
            intraday_data = etf_data.get('intraday_data', {})
            intraday_section = self._generate_intraday_section(intraday_data)
            
            # 获取长期数据
            long_term_data = etf_data.get('long_term_data', {})
            long_term_section = self._generate_long_term_section(long_term_data)
            
            # 获取买卖盘口数据
            order_book = etf_data.get('order_book', {})
            order_book_section = self._generate_order_book_section(order_book)
            
            # 获取资金流向数据
            fund_flow = etf_data.get('fund_flow', {})
            fund_flow_section = self._generate_fund_flow_section(fund_flow)
            
            section = f"""### 所有{category}ETF（{etf_name}）数据
            **current_price = {current_price:.2f}**，**current_ema20 = {current_ema20:.2f}**，**current_macd = {current_macd:.2f}**，**current_rsi（7 个周期）= {current_rsi7:.2f}**
            
            #### 增强技术指标：
            - **趋势强度**：{trend_strength:.2f}（-1到1，越接近1表示上升趋势越强）
            - **支撑位**：{support:.2f}，**阻力位**：{resistance:.2f}
            - **波动率**：{volatility:.4f}
            
            #### 实时买卖盘口数据（五档行情）：
            
            {order_book_section}
            
            #### 资金流向分析：
            
            {fund_flow_section}
            
            #### 日内系列（3 分钟间隔，最旧→最新）：
            
            {intraday_section}
            
            #### 长期背景（4 小时时间范围）：
            
            {long_term_section}"""
            
            return section
            
        except Exception as e:
            logger.error(f"生成单个ETF {etf_code} 数据部分失败: {e}")
            return ""
    
    def _generate_intraday_section(self, intraday_data: Dict[str, Any]) -> str:
        """
        生成日内数据部分
        
        Args:
            intraday_data: 日内数据字典
            
        Returns:
            日内数据字符串
        """
        try:
            # 获取价格序列
            mid_prices = intraday_data.get('mid_prices', [])
            ema_series = intraday_data.get('ema_series', [])
            macd_series = intraday_data.get('macd_series', [])
            rsi7_series = intraday_data.get('rsi7_series', [])
            rsi14_series = intraday_data.get('rsi14_series', [])
            
            # 如果没有数据，返回提示信息
            if not mid_prices and not ema_series and not macd_series and not rsi7_series and not rsi14_series:
                return "日内数据暂无"
            
            # 格式化数据
            mid_prices_str = ', '.join([f"{p:.2f}" for p in mid_prices]) if mid_prices else "暂无数据"
            ema_series_str = ', '.join([f"{e:.2f}" for e in ema_series]) if ema_series else "暂无数据"
            macd_series_str = ', '.join([f"{m:.2f}" for m in macd_series]) if macd_series else "暂无数据"
            rsi7_series_str = ', '.join([f"{r:.1f}" for r in rsi7_series]) if rsi7_series else "暂无数据"
            rsi14_series_str = ', '.join([f"{r:.1f}" for r in rsi14_series]) if rsi14_series else "暂无数据"
            
            section = f"""**中间价**：[{mid_prices_str}]

**EMA指标（20期）**：[{ema_series_str}]

**MACD指标**：[{macd_series_str}]

**RSI指标（7期）**：[{rsi7_series_str}]

**RSI指标（14期）**：[{rsi14_series_str}]"""
            
            return section
            
        except Exception as e:
            logger.error(f"生成日内数据部分失败: {e}")
            return "日内数据获取失败"
    
    def _generate_long_term_section(self, long_term_data: Dict[str, Any]) -> str:
        """
        生成长期数据部分
        
        Args:
            long_term_data: 长期数据字典
            
        Returns:
            长期数据字符串
        """
        try:
            # 获取长期指标
            ema20 = long_term_data.get('ema20', 0)
            ema50 = long_term_data.get('ema50', 0)
            atr3 = long_term_data.get('atr3', 0)
            atr14 = long_term_data.get('atr14', 0)
            current_volume = long_term_data.get('current_volume', 0)
            avg_volume = long_term_data.get('avg_volume', 0)
            
            # 获取长期序列
            macd_series = long_term_data.get('macd_series', [])
            rsi_series = long_term_data.get('rsi_series', [])
            
            # 如果没有数据，返回提示信息
            if ema20 == 0 and ema50 == 0 and atr3 == 0 and atr14 == 0 and current_volume == 0 and avg_volume == 0 and not macd_series and not rsi_series:
                return "长期数据暂无"
            
            # 格式化数据
            macd_series_str = ', '.join([f"{m:.2f}" for m in macd_series]) if macd_series else "暂无数据"
            rsi_series_str = ', '.join([f"{r:.1f}" for r in rsi_series]) if rsi_series else "暂无数据"
            
            # 格式化各个指标值
            ema20_str = f"{ema20:.2f}" if ema20 > 0 else "暂无数据"
            ema50_str = f"{ema50:.2f}" if ema50 > 0 else "暂无数据"
            atr3_str = f"{atr3:.2f}" if atr3 > 0 else "暂无数据"
            atr14_str = f"{atr14:.2f}" if atr14 > 0 else "暂无数据"
            current_volume_str = f"{current_volume:,}" if current_volume > 0 else "暂无数据"
            avg_volume_str = f"{avg_volume:,}" if avg_volume > 0 else "暂无数据"
            
            section = f"""- **20 周期 EMA**：{ema20_str} vs. **50 周期 EMA**：{ema50_str}
- **3 期 ATR**：{atr3_str} vs. **14 期 ATR**：{atr14_str}
- **当前交易量**：{current_volume_str} 股 vs. **平均交易量**：{avg_volume_str} 股
- **MACD指标**：[{macd_series_str}]
- **RSI指标（14期）**：[{rsi_series_str}]"""
            
            return section
            
        except Exception as e:
            logger.error(f"生成长期数据部分失败: {e}")
            return "长期数据获取失败"
    
    def _generate_order_book_section(self, order_book: Dict[str, Any]) -> str:
        """
        生成买卖盘口数据部分
        
        Args:
            order_book: 买卖盘口数据字典
            
        Returns:
            买卖盘口数据字符串
        """
        try:
            if not order_book:
                return "暂无买卖盘口数据"
            
            # 获取买卖盘数据 - 尝试多种可能的字段名
            bid_prices = order_book.get('bid_prices', [])
            bid_volumes = order_book.get('bid_volumes', [])
            ask_prices = order_book.get('ask_prices', [])
            ask_volumes = order_book.get('ask_volumes', [])
            
            # 如果没有标准格式数据，尝试使用原始格式
            if not bid_prices and not ask_prices:
                bid_levels = order_book.get('bid_levels', [])
                ask_levels = order_book.get('ask_levels', [])
                
                if bid_levels and ask_levels:
                    # 从原始格式提取数据
                    bid_prices = [level.get('price', 0) for level in bid_levels]
                    bid_volumes = [level.get('vol', 0) for level in bid_levels]
                    ask_prices = [level.get('price', 0) for level in ask_levels]
                    ask_volumes = [level.get('vol', 0) for level in ask_levels]
            
            # 如果仍然没有数据，尝试从data_fetcher.py的格式中获取
            if not bid_prices and not ask_prices:
                # 检查是否有bid和ask字段（来自sina_crawler.py）
                bid_data = order_book.get('bid', [])
                ask_data = order_book.get('ask', [])
                
                if bid_data:
                    # 从bid字段提取数据
                    bid_prices = [item.get('price', 0) for item in bid_data if isinstance(item, dict)]
                    bid_volumes = [item.get('vol', 0) for item in bid_data if isinstance(item, dict)]
                
                if ask_data:
                    # 从ask字段提取数据
                    ask_prices = [item.get('price', 0) for item in ask_data if isinstance(item, dict)]
                    ask_volumes = [item.get('vol', 0) for item in ask_data if isinstance(item, dict)]
            
            # 添加调试信息
            logger.debug(f"买卖盘口数据调试信息:")
            logger.debug(f"  - bid_prices: {bid_prices}")
            logger.debug(f"  - bid_volumes: {bid_volumes}")
            logger.debug(f"  - ask_prices: {ask_prices}")
            logger.debug(f"  - ask_volumes: {ask_volumes}")
            logger.debug(f"  - order_book keys: {list(order_book.keys())}")
            
            # 如果仍然没有数据，返回提示信息
            if not bid_prices and not ask_prices:
                logger.warning("买卖盘口数据格式不匹配或为空")
                return "买卖盘口数据格式不匹配"
            
            # 格式化买卖盘数据
            section = ""
            
            # 过滤异常数据 - 获取合理的价格范围
            def filter_valid_data(prices, volumes):
                """过滤掉异常价格数据"""
                valid_prices = []
                valid_volumes = []
                for price, volume in zip(prices, volumes):
                    # 价格合理性检查：价格大于0，小于1000，且与第一个价格的差异不超过50%
                    if (price > 0 and price < 1000 and volume > 0 and
                        len(valid_prices) == 0 or  # 第一个有效数据
                        (len(prices) > 0 and abs(price - prices[0]) / prices[0] < 0.5)):  # 与第一个价格差异不超过50%
                        valid_prices.append(price)
                        valid_volumes.append(volume)
                return valid_prices, valid_volumes
            
            # 过滤买盘数据
            if bid_prices and bid_volumes:
                bid_prices, bid_volumes = filter_valid_data(bid_prices, bid_volumes)
            
            # 过滤卖盘数据
            if ask_prices and ask_volumes:
                ask_prices, ask_volumes = filter_valid_data(ask_prices, ask_volumes)
            
            # 买盘数据（从高到低）
            if bid_prices and bid_volumes:
                section += "**买盘**：\n"
                for i in range(min(5, len(bid_prices), len(bid_volumes))):
                    price = bid_prices[i]
                    volume = bid_volumes[i]
                    section += f"- 买{i+1}: {price:.3f}元，{volume:,}手\n"
            
            # 卖盘数据（从低到高）
            if ask_prices and ask_volumes:
                if section:
                    section += "\n"
                section += "**卖盘**：\n"
                for i in range(min(5, len(ask_prices), len(ask_volumes))):
                    price = ask_prices[i]
                    volume = ask_volumes[i]
                    section += f"- 卖{i+1}: {price:.3f}元，{volume:,}手\n"
            
            # 添加买卖价差信息
            if bid_prices and ask_prices:
                best_bid = bid_prices[0] if bid_prices else 0
                best_ask = ask_prices[0] if ask_prices else 0
                if best_bid > 0 and best_ask > 0:
                    spread = best_ask - best_bid
                    spread_pct = (spread / best_bid) * 100 if best_bid > 0 else 0
                    section += f"\n**买卖价差**: {spread:.3f} ({spread_pct:.2f}%)"
            
            return section if section else "买卖盘口数据不完整"
            
        except Exception as e:
            logger.error(f"生成买卖盘口数据部分失败: {e}")
            return "买卖盘口数据获取失败"
    
    def _generate_fund_flow_section(self, fund_flow: Dict[str, Any]) -> str:
        """
        生成资金流向数据部分
        
        注意：资金流向数据为前一交易日数据，非实时数据
        
        Args:
            fund_flow: 资金流向数据字典
            
        Returns:
            资金流向数据字符串
        """
        try:
            if not fund_flow:
                return "暂无资金流向数据"
            
            # 检查数据格式，适配不同的接口返回格式
            if 'recent_3_days' in fund_flow:
                # 最近3天的数据格式
                recent_data = fund_flow.get('recent_3_days', [])
                if not recent_data:
                    return "暂无资金流向数据"
                
                section = "### 资金流向分析（最近3天）\n\n"
                section += "**注意**：以下资金流向数据为前3个交易日数据，不一定包括当日，非实时数据\n\n"
                
                # 按日期倒序排列（最新的在前）
                for i, data in enumerate(recent_data):
                    date = data.get('date', '')
                    close_price = data.get('close_price', 0)
                    change_pct = data.get('change_pct', 0)
                    main_net_inflow = data.get('main_net_inflow', 0)
                    main_net_inflow_ratio = data.get('main_net_inflow_ratio', 0)
                    super_large_net_inflow = data.get('super_large_net_inflow', 0)
                    large_net_inflow = data.get('large_net_inflow', 0)
                    medium_net_inflow = data.get('medium_net_inflow', 0)
                    small_net_inflow = data.get('small_net_inflow', 0)
                    
                    section += f"#### 第{i+1}天（{date}）\n"
                    section += f"- **收盘价**：{close_price:.2f}，**涨跌幅**：{change_pct:.2f}%\n"
                    section += f"- **主力净流入**：{main_net_inflow:,.0f}元 ({main_net_inflow_ratio:.2f}%)\n"
                    section += f"- **超大单净流入**：{super_large_net_inflow:,.0f}元\n"
                    section += f"- **大单净流入**：{large_net_inflow:,.0f}元\n"
                    section += f"- **中单净流入**：{medium_net_inflow:,.0f}元\n"
                    section += f"- **小单净流入**：{small_net_inflow:,.0f}元\n\n"
            elif 'close_price' in fund_flow:
                # 原始格式（单天数据）
                date = fund_flow.get('date', '')
                close_price = fund_flow.get('close_price', 0)
                change_pct = fund_flow.get('change_pct', 0)
                main_net_inflow = fund_flow.get('main_net_inflow', 0)
                main_net_inflow_ratio = fund_flow.get('main_net_inflow_ratio', 0)
                super_large_net_inflow = fund_flow.get('super_large_net_inflow', 0)
                large_net_inflow = fund_flow.get('large_net_inflow', 0)
                medium_net_inflow = fund_flow.get('medium_net_inflow', 0)
                small_net_inflow = fund_flow.get('small_net_inflow', 0)
                
                # 添加数据时效性说明
                section = f"""### 资金流向分析（{date}）
                
**注意**：以下资金流向数据为前一交易日数据，非实时数据
                
- **日期**：{date}
- **收盘价**：{close_price:.2f}，**涨跌幅**：{change_pct:.2f}%
- **主力净流入**：{main_net_inflow:,.0f}元 ({main_net_inflow_ratio:.2f}%)
- **超大单净流入**：{super_large_net_inflow:,.0f}元
- **大单净流入**：{large_net_inflow:,.0f}元
- **中单净流入**：{medium_net_inflow:,.0f}元
- **小单净流入**：{small_net_inflow:,.0f}元"""
            else:
                # 简化格式（从stock_individual_fund_flow_rank接口）
                section = f"""### 资金流向分析
                
**注意**：资金流向数据为前一交易日数据，非实时数据
                
- **资金流向数据**：已获取简化版本
- **详细信息**：{str(fund_flow)[:200]}..."""
            
            return section
            
        except Exception as e:
            logger.error(f"生成资金流向数据部分失败: {e}")
            return "资金流向数据获取失败"
    
    def _generate_account_section(self, account_data: Dict[str, Any]) -> str:
        """
        生成ETF账户信息部分
        
        Args:
            account_data: 账户数据
            
        Returns:
            账户信息字符串
        """
        try:
            account_info = account_data.get('account_info', {})
            positions = account_data.get('positions', [])
            
            total_assets = account_info.get('total_assets', 0)
            total_pnl = account_info.get('total_pnl', 0)
            daily_pnl = account_info.get('daily_pnl', 0)
            available_cash = account_info.get('available_cash', 0)
            
            section = f"""### 您的ETF账户信息和表现

- **总资产**：{total_assets:,.2f}
- **总盈亏**：{total_pnl:,.2f}
- **当日盈亏**：{daily_pnl:,.2f}
- **可用现金**：{available_cash:,.2f}

#### 当前ETF持仓和表现："""
            
            # 添加持仓信息
            for position in positions:
                # 格式化ETF持仓信息
                symbol = position.get('symbol', 'N/A')
                # 优先从配置文件获取标准名称
                name = self._get_etf_name_from_config(symbol) or position.get('name', 'N/A')
                quantity = position.get('quantity', 0)
                available_quantity = position.get('available_quantity', 0)
                position_ratio = position.get('position_ratio', 0)
                avg_price = position.get('avg_price', 0)
                daily_pnl = position.get('daily_pnl', 0)
                total_pnl = position.get('total_pnl', 0)
                market_value = position.get('market_value', 0)
                
                position_info = f"""
**{symbol} - {name}**
- 持仓数量: {quantity}
- 可用持仓: {available_quantity}
- 仓位占比: {position_ratio:.2f}%
- 买入均价: {avg_price:.2f}
- 当日盈亏: {daily_pnl:.2f}
- 总盈亏: {total_pnl:.2f}
- 持仓市值: {market_value:.2f}"""
                
                section += position_info
            
            # 添加风险说明
            section += f"\n\n> 注：ETF投资为全额买入，无杠杆交易，风险以实际亏损金额计算。"
            
            return section
            
        except Exception as e:
            logger.error(f"生成ETF账户信息部分失败: {e}")
            return "ETF账户信息获取失败"
    
    def _generate_performance_section(self, account_data: Dict[str, Any]) -> str:
        """
        生成ETF绩效指标部分
        
        Args:
            account_data: 账户数据
            
        Returns:
            绩效指标字符串
        """
        try:
            positions = account_data.get('positions', [])
            
            if not positions:
                return """### ETF投资绩效指标

当前无持仓"""
            
            # 计算绩效指标
            total_positions = len(positions)
            winning_positions = sum(1 for pos in positions if pos.get('total_pnl', 0) > 0)
            losing_positions = sum(1 for pos in positions if pos.get('total_pnl', 0) < 0)
            win_rate = (winning_positions / total_positions * 100) if total_positions > 0 else 0
            
            total_pnl = sum(pos.get('total_pnl', 0) for pos in positions)
            daily_pnl = sum(pos.get('daily_pnl', 0) for pos in positions)
            
            # 计算平均仓位
            avg_position_ratio = sum(pos.get('position_ratio', 0) for pos in positions) / total_positions if total_positions > 0 else 0
            
            section = f"""### ETF投资绩效指标

- **持仓总数**：{total_positions}
- **盈利持仓**：{winning_positions}
- **亏损持仓**：{losing_positions}
- **胜率**：{win_rate:.1f}%
- **总盈亏**：{total_pnl:.2f}
- **当日盈亏**：{daily_pnl:.2f}
- **平均仓位**：{avg_position_ratio:.2f}%"""
            
            return section
            
        except Exception as e:
            logger.error(f"生成ETF绩效指标部分失败: {e}")
            return "ETF绩效指标获取失败"
    
    def generate_analysis_request(self) -> str:
        """
        生成分析请求
        
        Returns:
            分析请求字符串
        """
        request = """

---

### 交易分析请求

基于以上提供的数据，请完整提供以下兼具短期操作性与中长期战略视角的分析：

1. **市场状况分析**：  
   - 当前各ETF的短期技术信号（3–30分钟级别）与中期趋势状态（日线/4小时级别）分别如何？是否存在背离或共振？  
   - 哪些ETF处于趋势初期、中期或尾声阶段？

2. **持仓风险评估**：  
   - 当前持仓是否与中期市场方向一致？是否存在“逆势持仓”或“弱势资产拖累组合”？  
   - 各持仓的风险回报比（基于支撑/阻力/波动率）是否合理？

3. **交易建议**：  
   请明确区分两类建议：  
   - **短期操作**（未来1–2小时内可执行）  
   - **中长期调仓方向**（未来1–5个交易日的目标仓位调整）  

4. **风险管理**：建议的止损位和目标价位是否基于有效技术位或波动率？  
5. **资金管理**：当前现金占比是否合理？是否应向高趋势强度、强资金流入的资产倾斜？  
6. **市场情绪分析**：结合行业资金流向，判断当前主线是科技（芯片/AI/军工）还是避险（黄金）？  
7. **增强技术分析**：综合趋势强度、EMA排列、MACD动量、支撑阻力与波动率，评估各ETF所处阶段。

---

**重要：请严格按照以下JSON格式提供交易建议，便于系统自动提取：**

```json
{
  "analysis_summary": "简要分析总结（需包含短期信号与中长期趋势判断）",
  "recommendations": [
    {
      "symbol": "ETF代码",
      "name": "ETF名称",
      "action": "买入/卖出/持有/观望",
      "quantity": "目标持仓数量（整数）",
      "buy_quantity": "本次建议买入数量（整数）",
      "sell_quantity": "本次建议卖出数量（整数）",
      "buy_price": "建议买入价格（保留两位小数）",
      "sell_price": "建议卖出价格（保留两位小数）",
      "stop_loss": "止损价格（保留两位小数）",
      "take_profit": "止盈价格（保留两位小数）",
      "time_horizon": "短期/中期",
      "reason": "操作理由（需说明：技术状态、资金面、是否符合中长期配置逻辑）"
    }
  ]
}
"""
        
        return request
    
    def save_prompt_to_file(self, prompt: str, filename: str = None) -> str:
        """
        保存提示词到文件
        
        Args:
            prompt: 提示词内容
            filename: 文件名
            
        Returns:
            保存的文件路径
        """
        try:
            import os
            
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"trading_prompt_{timestamp}.md"
            
            # 确保输出目录存在
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(prompt)
            
            logger.info(f"提示词已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存提示词失败: {e}")
            return ""
    
    def _generate_history_section(self) -> str:
        """
        生成历史分析数据部分
        
        Returns:
            历史分析数据字符串
        """
        try:
            import os
            
            # 读取历史记录文件
            history_file = "data/trading_analysis_history.json"
            
            if not os.path.exists(history_file):
                return "### 历史分析记录\n\n暂无历史分析记录。"
            
            try:
                with open(history_file, 'r', encoding='utf-8') as f:
                    history_data = json.load(f)
                
                if not isinstance(history_data, list) or not history_data:
                    return "### 历史分析记录\n\n暂无历史分析记录。"
                
                history_section = "### 历史分析记录（最近1条）\n\n"
                
                for record in history_data:
                    # 提取关键信息
                    timestamp = record.get('timestamp', '')
                    analysis_summary = record.get('analysis_summary', '')
                    recommendations = record.get('recommendations', [])
                    
                    history_section += f"**分析时间**: {timestamp}\n"
                    history_section += f"**分析总结**: {analysis_summary}\n"
                    
                    if recommendations:
                        history_section += "**建议操作**:\n"
                        for rec in recommendations:
                            symbol = rec.get('symbol', '')
                            action = rec.get('action', '')
                            quantity = rec.get('quantity', '')
                            stop_loss = rec.get('stop_loss', '')
                            take_profit = rec.get('take_profit', '')
                            reason = rec.get('reason', '')
                            
                            # 在历史记录中也使用标准ETF名称
                            etf_name = self._get_etf_name_from_config(symbol)
                            display_name = f"{etf_name}" if etf_name else symbol
                            
                            history_section += f"- {symbol}: {action} {quantity}股, 止损{stop_loss}, 止盈{take_profit}, 理由: {reason}\n"
                    
                    history_section += "\n---\n\n"
                
                return history_section
                
            except Exception as e:
                logger.error(f"读取历史分析文件失败: {e}")
                return "### 历史分析记录\n\n历史分析数据读取失败。"
            
        except Exception as e:
            logger.error(f"生成历史分析数据部分失败: {e}")
            return "### 历史分析记录\n\n历史分析数据获取失败。"
    
    def _get_etf_name_from_config(self, etf_code: str) -> str:
        """
        从配置文件获取ETF标准名称
        
        Args:
            etf_code: ETF代码
            
        Returns:
            ETF标准名称，如果找不到返回None
        """
        try:
            monitored_etfs = self.etf_list.get('monitored_etfs', [])
            for etf_info in monitored_etfs:
                if etf_info.get('code') == etf_code:
                    return etf_info.get('name')
            return None
        except Exception as e:
            logger.error(f"从配置文件获取ETF名称失败: {e}")
            return None
    
    def _get_etf_category_from_config(self, etf_code: str) -> str:
        """
        从配置文件获取ETF类别
        
        Args:
            etf_code: ETF代码
            
        Returns:
            ETF类别，如果找不到返回None
        """
        try:
            monitored_etfs = self.etf_list.get('monitored_etfs', [])
            for etf_info in monitored_etfs:
                if etf_info.get('code') == etf_code:
                    return etf_info.get('category')
            return None
        except Exception as e:
            logger.error(f"从配置文件获取ETF类别失败: {e}")
            return None