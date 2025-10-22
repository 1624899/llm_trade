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
        
        header = f"""自您开始交易以来已经过去了 {self.trading_minutes} 分钟。当前时间是 {self.current_time}，您已被调用 {call_count} 次。下面，我们为您提供各种状态数据、价格数据和预测信号，以便您发现阿尔法。下面是您的当前账户信息、价值、表现、头寸等。

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
            
            # 获取模拟数据
            simulated_data = self._generate_simulated_data()
            
            # 获取日内数据
            intraday_data = etf_data.get('intraday_data', {})
            intraday_section = self._generate_intraday_section(intraday_data)
            
            # 获取长期数据
            long_term_data = etf_data.get('long_term_data', {})
            long_term_section = self._generate_long_term_section(long_term_data)
            
            section = f"""### 所有{category}ETF（{etf_name}）数据  
**current_price = {current_price:.2f}**，**current_ema20 = {current_ema20:.2f}**，**current_macd = {current_macd:.2f}**，**current_rsi（7 个周期）= {current_rsi7:.2f}**

此外，以下是{category}ETF的最新未平仓合约（模拟为ETF持仓热度）和隐含资金成本（年化股息调整后）：

- **持仓热度（模拟）**：最新：{simulated_data['holdings_heat']}，平均：{simulated_data['avg_holdings']}  
- **隐含资金成本**：{simulated_data['funding_rate']}（年化，反映股息与融资成本）

#### 日内系列（3 分钟间隔，最旧→最新）：

{intraday_section}

#### 长期背景（4 小时时间范围）：

{long_term_section}"""
            
            return section
            
        except Exception as e:
            logger.error(f"生成单个ETF {etf_code} 数据部分失败: {e}")
            return ""
    
    def _generate_simulated_data(self) -> Dict[str, str]:
        """
        生成模拟数据
        
        Returns:
            模拟数据字典
        """
        import random
        
        # 生成随机但合理的模拟数据
        holdings_heat = f"{random.uniform(8, 15):.2f} 万手"
        avg_holdings = f"{random.uniform(8, 15):.2f} 万手"
        funding_rate = f"{random.uniform(-2e-5, -1e-5):.1E}"
        
        return {
            'holdings_heat': holdings_heat,
            'avg_holdings': avg_holdings,
            'funding_rate': funding_rate
        }
    
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
            mid_prices = intraday_data.get('mid_prices', [228.82, 228.75, 228.68, 228.61, 228.63, 228.49, 228.46, 228.47, 228.40, 228.45])
            ema_series = intraday_data.get('ema_series', [228.71, 228.70, 228.68, 228.66, 228.65, 228.62, 228.60, 228.58, 228.55, 228.62])
            macd_series = intraday_data.get('macd_series', [0.12, 0.10, 0.06, 0.03, 0.01, -0.02, -0.04, -0.06, -0.07, -0.08])
            rsi7_series = intraday_data.get('rsi7_series', [55.8, 54.9, 41.2, 43.5, 46.1, 36.8, 38.2, 37.9, 33.1, 43.21])
            rsi14_series = intraday_data.get('rsi14_series', [57.9, 57.5, 50.1, 51.3, 52.6, 47.4, 47.9, 47.8, 45.3, 48.5])
            
            # 格式化数据
            mid_prices_str = ', '.join([f"{p:.2f}" for p in mid_prices])
            ema_series_str = ', '.join([f"{e:.2f}" for e in ema_series])
            macd_series_str = ', '.join([f"{m:.2f}" for m in macd_series])
            rsi7_series_str = ', '.join([f"{r:.1f}" for r in rsi7_series])
            rsi14_series_str = ', '.join([f"{r:.1f}" for r in rsi14_series])
            
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
            ema20 = long_term_data.get('ema20', 223.15)
            ema50 = long_term_data.get('ema50', 226.80)
            atr3 = long_term_data.get('atr3', 1.82)
            atr14 = long_term_data.get('atr14', 2.11)
            current_volume = long_term_data.get('current_volume', 85200)
            avg_volume = long_term_data.get('avg_volume', 980000)
            
            # 获取长期序列
            macd_series = long_term_data.get('macd_series', [-3.02, -2.81, -2.60, -2.48, -2.20, -1.85, -1.50, -1.25, -0.81, -0.28])
            rsi_series = long_term_data.get('rsi_series', [40.1, 40.9, 41.2, 38.7, 45.6, 50.0, 51.8, 50.1, 59.0, 62.8])
            
            # 格式化数据
            macd_series_str = ', '.join([f"{m:.2f}" for m in macd_series])
            rsi_series_str = ', '.join([f"{r:.1f}" for r in rsi_series])
            
            section = f"""- **20 周期 EMA**：{ema20:.2f} vs. **50 周期 EMA**：{ema50:.2f}  
- **3 期 ATR**：{atr3:.2f} vs. **14 期 ATR**：{atr14:.2f}  
- **当前交易量**：{current_volume:,} 股 vs. **平均交易量**：{avg_volume:,} 股  
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
                position_ratio = position.get('position_ratio', 0)
                avg_price = position.get('avg_price', 0)
                daily_pnl = position.get('daily_pnl', 0)
                total_pnl = position.get('total_pnl', 0)
                market_value = position.get('market_value', 0)
                
                position_info = f"""
**{symbol} - {name}**
- 持仓数量: {quantity}
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
      "action": "买入/卖出/持有",
      "quantity": "目标持仓数量",
      "buy_quantity": "建议买入数量",
      "sell_quantity": "建议卖出数量",
      "stop_loss": "止损价格",
      "take_profit": "止盈价格",
      "reason": "操作理由"
    }
  ]
}
```

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