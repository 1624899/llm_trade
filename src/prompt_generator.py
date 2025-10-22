"""
标准化语料生成模块
将市场数据、技术指标和账户信息转换为LLM可理解的标准化格式
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
            
            # 生成账户信息部分
            account_section = self._generate_account_section(account_data)
            
            # 生成绩效指标部分
            performance_section = self._generate_performance_section(account_data)
            
            # 生成分析请求
            analysis_request = self.generate_analysis_request()
            
            # 组合完整的提示词
            prompt = f"{header}\n\n{history_section}\n\n{etf_sections}\n\n{account_section}\n\n{performance_section}\n\n{analysis_request}"
            
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
            try:
                section = self._generate_single_etf_section(etf_code, etf_data)
                if section:
                    etf_sections.append(section)
            except Exception as e:
                logger.error(f"生成ETF {etf_code} 数据部分失败: {e}")
                continue
        
        return "\n\n---\n\n".join(etf_sections)
    
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
            
            # 获取日内数据
            intraday_data = etf_data.get('intraday_data', {})
            intraday_section = self._generate_intraday_section(intraday_data)
            
            # 获取长期数据
            long_term_data = etf_data.get('long_term_data', {})
            long_term_section = self._generate_long_term_section(long_term_data)
            
            section = f"""### 所有{category}ETF（{etf_name}）数据
**current_price = {current_price:.2f}**，**current_ema20 = {current_ema20:.2f}**，**current_macd = {current_macd:.2f}**，**current_rsi（7 个周期）= {current_rsi7:.2f}**

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

基于以上提供的数据，请提供以下分析：

1. **市场状况分析**：当前各ETF的技术指标显示什么信号？
2. **持仓风险评估**：当前持仓的风险水平如何？
3. **交易建议**：基于当前市场数据和持仓情况，提供具体的交易建议

**重要：请严格按照以下JSON格式提供交易建议，便于系统自动提取：**

```json
{
  "analysis_summary": "简要分析总结",
  "recommendations": [
    {
      "symbol": "ETF代码",
      "name": "ETF名称",
      "action": "买入/卖出/持有/观望",
      "quantity": "目标持仓数量",
      "buy_quantity": "建议买入数量",
      "sell_quantity": "建议卖出数量",
      "buy_price": "建议买入价格",
      "sell_price": "建议卖出价格",
      "stop_loss": "止损价格",
      "take_profit": "止盈价格",
      "reason": "操作理由"
    }
  ]
}
```

**重要提示：**
- action字段必须是"买入"、"卖出"、"持有"或"观望"之一
- 如果是观望操作，quantity、buy_quantity、sell_quantity应设为"0"
- 如果是买入操作，sell_quantity应设为"0"
- 如果是卖出操作，buy_quantity应设为"0"
- 如果是持有操作，buy_quantity和sell_quantity应设为"0"
- 价格字段请保留两位小数
- 请为每个ETF提供具体的理由，包括技术指标分析

4. **风险管理**：建议的止损位和目标价位
5. **资金管理**：是否需要调整仓位配置

请提供详细的分析，并务必按照上述JSON格式提供具体的操作建议。"""
        
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
                
                history_section = "### 历史分析记录（最近3条）\n\n"
                
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