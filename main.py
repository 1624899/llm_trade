"""
A股ETF交易分析系统主程序
整合所有模块，提供完整的ETF交易分析功能
"""

import sys
import os
import argparse
from datetime import datetime
from typing import Dict, List, Any

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.utils import (
    load_config, load_etf_list, setup_logging, 
    create_directories, is_trading_time
)
from src.data_fetcher import ETFDataFetcher
from src.indicators import TechnicalIndicators
from src.account import AccountManager
from src.prompt_generator import PromptGenerator
from src.llm_client import LLMClient


class ETFTradingSystem:
    """ETF交易系统主类"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        初始化交易系统
        
        Args:
            config_path: 配置文件路径
        """
        # 加载配置
        self.config = load_config(config_path)
        self.etf_list = load_etf_list()
        
        # 设置日志
        setup_logging(self.config)
        
        # 创建必要目录
        create_directories()
        
        # 初始化各个模块
        self.data_fetcher = ETFDataFetcher(self.config)
        self.indicators = TechnicalIndicators(self.config)
        self.account_manager = AccountManager()
        self.prompt_generator = PromptGenerator(self.config)
        self.llm_client = LLMClient(self.config)
        
        # 获取系统配置
        self.system_config = self.config.get('system', {})
        self.test_mode = self.system_config.get('test_mode', False)
        
        # 监控的ETF列表
        self.monitored_etfs = self.etf_list.get('monitored_etfs', [])
        
        # 系统状态
        self.is_running = False
        
        print("🚀 A股ETF交易分析系统初始化完成")
        print(f"📊 监控ETF数量: {len(self.monitored_etfs)}")
        print(f"💰 当前持仓数量: {len(self.account_manager.get_positions())}")
        if self.test_mode:
            print("🧪 系统处于测试模式，将跳过LLM交互")
        else:
            print(f"🤖 LLM模型: {self.llm_client.get_model_info()['provider']} - {self.llm_client.get_model_info()['model']}")
    
    def run_single_analysis(self) -> bool:
        """
        运行单次分析
        
        Returns:
            分析是否成功
        """
        try:
            print("\n" + "="*60)
            print("📈 开始ETF交易分析...")
            print(f"⏰ 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
            
            # 1. 获取市场数据
            print("📡 正在获取市场数据...")
            market_data = self._collect_market_data()
            
            if not market_data:
                print("❌ 市场数据获取失败，分析终止")
                return False
            
            print(f"✅ 成功获取 {len(market_data)} 个ETF的市场数据")
            

            
            # 3. 生成标准化语料
            print("📝 正在生成分析语料...")
            account_data = self.account_manager.account_data
            prompt = self.prompt_generator.generate_trading_prompt(market_data, account_data)
            
            if not prompt:
                print("❌ 语料生成失败，分析终止")
                return False
            
            print("✅ 分析语料生成成功")
            
            # 4. 调用LLM获取交易建议（测试模式下跳过）
            if self.test_mode:
                print("🧪 测试模式：跳过LLM交互，使用模拟响应")
                trading_advice = self._generate_test_response()
            else:
                print("🤖 正在调用AI模型生成交易建议...")
                trading_advice = self.llm_client.generate_trading_advice(prompt)
                
                if not trading_advice:
                    print("❌ 交易建议生成失败")
                    return False
                
                print("✅ 交易建议生成成功")
            
            # 5. 保存结果
            self._save_analysis_results(prompt, trading_advice, market_data)
            
            # 6. 显示结果
            self._display_results(trading_advice)
            
            print("\n🎉 单次分析完成！")
            return True
            
        except Exception as e:
            print(f"❌ 分析过程中发生错误: {e}")
            return False
    
    def run_continuous_analysis(self, interval_minutes: int = 30) -> None:
        """
        运行连续分析
        
        Args:
            interval_minutes: 分析间隔（分钟）
        """
        import time
        
        print(f"\n🔄 开始连续分析模式，间隔: {interval_minutes} 分钟")
        print("按 Ctrl+C 停止分析")
        
        self.is_running = True
        
        try:
            while self.is_running:
                # 检查是否为交易时间
                if is_trading_time():
                    print(f"\n🕐 {datetime.now().strftime('%H:%M:%S')} - 开始新一轮分析")
                    
                    success = self.run_single_analysis()
                    
                    if success:
                        print(f"✅ 分析完成，等待 {interval_minutes} 分钟后进行下次分析")
                    else:
                        print("❌ 分析失败，5分钟后重试")
                        interval_minutes = min(5, interval_minutes)
                else:
                    print(f"⏸️  当前非交易时间，等待 {interval_minutes} 分钟后检查")
                
                # 等待下次分析
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\n⏹️  用户停止分析")
            self.is_running = False
        except Exception as e:
            print(f"\n❌ 连续分析发生错误: {e}")
            self.is_running = False
    
    def _collect_market_data(self) -> Dict[str, Any]:
        """
        收集市场数据（使用并发优化版本）
        
        Returns:
            市场数据字典
        """
        print("📡 正在并发获取市场数据...")
        
        # 获取市场情绪数据（行业资金流向）
        market_sentiment_data = self.data_fetcher.get_market_sentiment_data()
        
        # 使用并发方式获取所有ETF的基础数据
        etf_codes = [etf_info['code'] for etf_info in self.monitored_etfs]
        basic_data = self.data_fetcher.get_multiple_etf_data(etf_codes)
        
        # 使用并发方式获取所有ETF的增强数据
        enhanced_data = self.data_fetcher.get_multiple_etf_advanced_data(self.monitored_etfs)
        
        market_data = {}
        
        for etf_info in self.monitored_etfs:
            etf_code = etf_info['code']
            etf_name = etf_info['name']
            etf_category = etf_info['category']
            
            try:
                # 检查基础数据
                if etf_code not in basic_data:
                    print(f"⚠️  ETF {etf_code} 基础数据获取失败")
                    continue
                
                # 检查增强数据
                if etf_code not in enhanced_data:
                    print(f"⚠️  ETF {etf_code} 增强数据获取失败")
                    continue
                
                real_time_data = enhanced_data[etf_code]['real_time_data']
                historical_data = enhanced_data[etf_code]['historical_data']
                
                if not real_time_data or (historical_data is None or historical_data.empty):
                    print(f"⚠️  ETF {etf_code} 关键数据缺失")
                    continue
                
                # 计算技术指标
                indicators = self.indicators.calculate_all_indicators(historical_data)
                
                # 获取最新指标值
                current_indicators = self.indicators.get_latest_indicator_values(historical_data, indicators)
                
                # 获取指标序列
                indicator_series = self.indicators.get_indicator_series(indicators, count=10)
                
                # 获取日内数据
                intraday_data = self.data_fetcher.get_intraday_data(etf_code, interval_minutes=3)
                
                # 从增强数据中获取其他信息
                order_book_data = enhanced_data[etf_code]['order_book_data']
                fund_flow_data = enhanced_data[etf_code]['fund_flow_data']
                minute_tick_data = enhanced_data[etf_code]['minute_tick_data']
                
                # 计算增强技术指标
                if not historical_data.empty:
                    close_prices = historical_data['close']
                    # 趋势强度
                    trend_strength = self.indicators.calculate_trend_strength(close_prices)
                    # 支撑阻力位
                    support_resistance = self.indicators.calculate_support_resistance(close_prices)
                    # 波动率
                    volatility = self.indicators.calculate_volatility(close_prices)
                    
                    # 添加到当前指标中
                    current_indicators['trend_strength'] = trend_strength
                    current_indicators['support'] = support_resistance.get('support', 0)
                    current_indicators['resistance'] = support_resistance.get('resistance', 0)
                    current_indicators['volatility'] = volatility
                
                # 计算EMA50（改进之前的简化处理）
                if not historical_data.empty and len(historical_data) >= 50:
                    ema50 = self.indicators.calculate_ema(historical_data['close'], 50)
                    current_indicators['current_ema50'] = float(ema50.iloc[-1]) if not ema50.empty else 0
                
                # 构建ETF数据
                etf_data = {
                    'name': etf_name,
                    'category': etf_category,
                    'current_data': current_indicators,
                    'intraday_data': {
                        'mid_prices': [data.get('mid_price', 0) for data in (intraday_data or [])[-10:]],
                        'ema_series': indicator_series.get('ema_long', [0] * 10),
                        'macd_series': indicator_series.get('macd', [0] * 10),
                        'rsi7_series': indicator_series.get('rsi_7', [0] * 10),
                        'rsi14_series': indicator_series.get('rsi_14', [0] * 10)
                    },
                    'long_term_data': {
                        'ema20': current_indicators.get('current_ema_long', 0),
                        'ema50': current_indicators.get('current_ema50', 0),  # 使用计算得到的EMA50
                        'atr3': current_indicators.get('current_atr_3', 0),
                        'atr14': current_indicators.get('current_atr_14', 0),
                        'current_volume': current_indicators.get('current_volume', 0),
                        'avg_volume': historical_data['volume'].mean() if not historical_data.empty else 0,
                        'macd_series': indicator_series.get('macd', [0] * 10),
                        'rsi_series': indicator_series.get('rsi_14', [0] * 10)
                    },
                    'order_book': order_book_data or {},
                    'fund_flow': fund_flow_data or {},
                    'minute_tick_data': minute_tick_data if minute_tick_data is not None and not minute_tick_data.empty else None
                }
                
                market_data[etf_code] = etf_data
                print(f"✅ ETF {etf_code} 数据处理完成")
                
            except Exception as e:
                print(f"⚠️  处理ETF {etf_code} 数据失败: {e}")
                continue
        
        # 添加市场情绪数据到市场数据中
        if market_sentiment_data:
            market_data['_market_sentiment'] = market_sentiment_data
        
        print(f"✅ 市场数据收集完成，成功处理 {len(market_data)} 个ETF")
        return market_data
    
    
    def _save_analysis_results(self, prompt: str, advice: str, market_data: Dict[str, Any]) -> None:
        """
        保存分析结果
        
        Args:
            prompt: 分析语料
            advice: 交易建议
            market_data: 市场数据
        """
        try:
            # 保存语料
            prompt_file = self.prompt_generator.save_prompt_to_file(prompt)
            if prompt_file:
                print(f"📄 语料已保存: {prompt_file}")
            
            # 保存建议
            advice_file = self.llm_client.save_advice_to_file(advice)
            if advice_file:
                print(f"📄 建议已保存: {advice_file}")
            
            # 提取交易信号并保存JSON数据
            print("🔍 正在提取交易信号...")
            # 在测试模式下，直接解析JSON格式的建议
            if self.test_mode:
                try:
                    import json
                    trading_signals = json.loads(advice)
                except Exception as e:
                    print(f"⚠️  测试模式下解析交易信号失败: {e}")
                    trading_signals = None
            else:
                trading_signals = self.llm_client.extract_trading_signals(advice)
            
            if trading_signals:
                account_data = self.account_manager.account_data
                json_file = self.llm_client.save_trading_analysis(trading_signals, market_data, account_data)
                if json_file:
                    print(f"📊 交易分析JSON已保存: {json_file}")
                    
                    # 显示提取的交易信号
                    recommendations = trading_signals.get('recommendations', [])
                    if recommendations:
                        print("💡 提取的交易信号:")
                        for i, rec in enumerate(recommendations, 1):
                            symbol = rec.get('symbol', '')
                            action = rec.get('action', '')
                            quantity = rec.get('quantity', '')
                            stop_loss = rec.get('stop_loss', '')
                            take_profit = rec.get('take_profit', '')
                            print(f"  {i}. {symbol}: {action} {quantity}股, 止损{stop_loss}, 止盈{take_profit}")
                else:
                    print("⚠️  交易分析JSON保存失败")
            else:
                print("⚠️  交易信号提取失败")
            
            # 保存市场数据缓存
            self.data_fetcher.save_cache_to_file()
            print("💾 市场数据已保存到 data/market_data 目录")
                
        except Exception as e:
            print(f"⚠️  保存分析结果失败: {e}")
    
    def _display_results(self, advice: str) -> None:
        """
        显示分析结果
        
        Args:
            advice: 交易建议
        """
        print("\n" + "="*60)
        print("🤖 AI交易建议")
        print("="*60)
        print(advice)
        print("="*60)
        
        # 显示账户摘要
        account_summary = self.account_manager.export_account_summary()
        print("\n📊 账户摘要")
        print("="*60)
        print(account_summary)
    
    def test_system(self) -> None:
        """测试系统功能"""
        print("\n🧪 开始系统测试...")
        
        # 测试配置加载
        print("1. 测试配置加载...")
        if self.config and self.etf_list:
            print("✅ 配置加载成功")
        else:
            print("❌ 配置加载失败")
            return
        
        # 测试数据获取
        print("2. 测试数据获取...")
        test_etf = self.monitored_etfs[0] if self.monitored_etfs else None
        if test_etf:
            data = self.data_fetcher.get_real_time_data(test_etf['code'])
            if data:
                print(f"✅ 数据获取成功: {test_etf['name']}")
            else:
                print("❌ 数据获取失败")
                return
        
        # 测试LLM连接（测试模式下跳过）
        print("3. 测试LLM连接...")
        if self.test_mode:
            print("🧪 测试模式：跳过LLM连接测试")
        else:
            if self.llm_client.test_connection():
                print("✅ LLM连接成功")
            else:
                print("❌ LLM连接失败")
                return
        
        print("🎉 系统测试完成，所有功能正常！")
    
    def show_status(self) -> None:
        """显示系统状态"""
        print("\n📊 系统状态")
        print("="*50)
        print(f"配置文件: {'✅ 已加载' if self.config else '❌ 未加载'}")
        print(f"ETF列表: {'✅ 已加载' if self.etf_list else '❌ 未加载'}")
        print(f"监控ETF数量: {len(self.monitored_etfs)}")
        print(f"当前持仓数量: {len(self.account_manager.get_positions())}")
        account_info = self.account_manager.get_account_info()
        print(f"账户总价值: {account_info.get('total_assets', 0):.2f}")
        print(f"总收益率: {account_info.get('total_return_pct', 0):.2f}%")
        if self.test_mode:
            print("🧪 系统处于测试模式，跳过LLM交互")
        else:
            print(f"LLM模型: {self.llm_client.get_model_info()['provider']} - {self.llm_client.get_model_info()['model']}")
        print(f"交易时间: {'✅ 是' if is_trading_time() else '❌ 否'}")
        print("="*50)
    
    def _generate_test_response(self) -> str:
        """
        生成测试模式下的模拟LLM响应
        
        Returns:
            模拟的交易建议文本
        """
        test_response = """{
  "analysis_summary": "测试模式下的市场分析总结：当前市场处于震荡状态，各ETF技术指标显示不同的信号。",
  "recommendations": [
    {
      "symbol": "512710",
      "name": "军工龙头",
      "action": "持有",
      "quantity": "9200",
      "buy_quantity": "0",
      "sell_quantity": "0",
      "buy_price": "",
      "sell_price": "",
      "stop_loss": "0.65",
      "take_profit": "0.72",
      "reason": "技术指标显示短期有支撑，建议继续持有观察"
    },
    {
      "symbol": "518880",
      "name": "黄金ETF",
      "action": "观望",
      "quantity": "0",
      "buy_quantity": "0",
      "sell_quantity": "0",
      "buy_price": "",
      "sell_price": "",
      "stop_loss": "",
      "take_profit": "",
      "reason": "当前无持仓，市场方向不明朗，建议观望"
    },
    {
      "symbol": "512010",
      "name": "医药ETF",
      "action": "买入",
      "quantity": "5000",
      "buy_quantity": "1000",
      "sell_quantity": "0",
      "buy_price": "0.415",
      "sell_price": "",
      "stop_loss": "0.39",
      "take_profit": "0.45",
      "reason": "技术指标显示超卖反弹迹象，建议适量买入"
    }
  ]
}"""
        return test_response
        print(f"配置文件: {'✅ 已加载' if self.config else '❌ 未加载'}")
        print(f"ETF列表: {'✅ 已加载' if self.etf_list else '❌ 未加载'}")
        print(f"监控ETF数量: {len(self.monitored_etfs)}")
        print(f"当前持仓数量: {len(self.account_manager.get_positions())}")
        print(f"账户总价值: {self.account_manager.get_account_info().get('portfolio_value', 0):.2f}")
        print(f"总收益率: {self.account_manager.get_account_info().get('total_return_pct', 0):.2f}%")
        print(f"LLM模型: {self.llm_client.get_model_info()['provider']} - {self.llm_client.get_model_info()['model']}")
        print(f"交易时间: {'✅ 是' if is_trading_time() else '❌ 否'}")
        print("="*50)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股ETF交易分析系统')
    parser.add_argument('--mode', choices=['single', 'continuous', 'test', 'status'], 
                       default='single', help='运行模式')
    parser.add_argument('--interval', type=int, default=30, 
                       help='连续分析间隔（分钟）')
    parser.add_argument('--config', type=str, default='config/config.yaml', 
                       help='配置文件路径')
    
    args = parser.parse_args()
    
    try:
        # 初始化系统
        system = ETFTradingSystem(args.config)
        
        if args.mode == 'single':
            # 单次分析
            system.run_single_analysis()
            
        elif args.mode == 'continuous':
            # 连续分析
            system.run_continuous_analysis(args.interval)
            
        elif args.mode == 'test':
            # 系统测试
            system.test_system()
            
        elif args.mode == 'status':
            # 显示状态
            system.show_status()
            
    except KeyboardInterrupt:
        print("\n👋 程序被用户中断")
    except Exception as e:
        print(f"\n❌ 程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()