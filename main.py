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
        
        # 监控的ETF列表
        self.monitored_etfs = self.etf_list.get('monitored_etfs', [])
        
        # 系统状态
        self.is_running = False
        
        print("🚀 A股ETF交易分析系统初始化完成")
        print(f"📊 监控ETF数量: {len(self.monitored_etfs)}")
        print(f"💰 当前持仓数量: {len(self.account_manager.get_positions())}")
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
            
            # 2. 更新账户持仓价格
            print("💼 正在更新账户持仓价格...")
            self._update_account_prices(market_data)
            
            # 3. 生成标准化语料
            print("📝 正在生成分析语料...")
            account_data = self.account_manager.account_data
            prompt = self.prompt_generator.generate_trading_prompt(market_data, account_data)
            
            if not prompt:
                print("❌ 语料生成失败，分析终止")
                return False
            
            print("✅ 分析语料生成成功")
            
            # 4. 调用LLM获取交易建议
            print("🤖 正在调用AI模型生成交易建议...")
            trading_advice = self.llm_client.generate_trading_advice(prompt)
            
            if not trading_advice:
                print("❌ 交易建议生成失败")
                return False
            
            print("✅ 交易建议生成成功")
            
            # 5. 保存结果
            self._save_analysis_results(prompt, trading_advice, market_data)
            
            # 6. 更新调用次数
            self.account_manager.update_call_count()
            
            # 7. 显示结果
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
        收集市场数据
        
        Returns:
            市场数据字典
        """
        market_data = {}
        
        for etf_info in self.monitored_etfs:
            etf_code = etf_info['code']
            etf_name = etf_info['name']
            etf_category = etf_info['category']
            
            try:
                # 获取实时数据
                real_time_data = self.data_fetcher.get_real_time_data(etf_code)
                if not real_time_data:
                    continue
                
                # 获取历史数据
                historical_data = self.data_fetcher.get_historical_data(etf_code, period="1d", count=100)
                if historical_data is None or historical_data.empty:
                    continue
                
                # 计算技术指标
                indicators = self.indicators.calculate_all_indicators(historical_data)
                
                # 获取最新指标值
                current_indicators = self.indicators.get_latest_indicator_values(historical_data, indicators)
                
                # 获取指标序列
                indicator_series = self.indicators.get_indicator_series(indicators, count=10)
                
                # 获取日内数据
                intraday_data = self.data_fetcher.get_intraday_data(etf_code, interval_minutes=3)
                
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
                        'ema50': current_indicators.get('current_ema_long', 0),  # 简化处理
                        'atr3': current_indicators.get('current_atr_3', 0),
                        'atr14': current_indicators.get('current_atr_14', 0),
                        'current_volume': current_indicators.get('current_volume', 0),
                        'avg_volume': 980000,  # 模拟平均成交量
                        'macd_series': indicator_series.get('macd', [0] * 10),
                        'rsi_series': indicator_series.get('rsi_14', [0] * 10)
                    }
                }
                
                market_data[etf_code] = etf_data
                
            except Exception as e:
                print(f"⚠️  获取ETF {etf_code} 数据失败: {e}")
                continue
        
        return market_data
    
    def _update_account_prices(self, market_data: Dict[str, Any]) -> None:
        """
        更新账户持仓价格
        
        Args:
            market_data: 市场数据字典
        """
        positions = self.account_manager.get_positions()
        
        for position in positions:
            symbol = position['symbol']
            
            if symbol in market_data:
                current_price = market_data[symbol]['current_data'].get('current_price', 0)
                if current_price > 0:
                    self.account_manager.update_position_price(symbol, current_price)
                    print(f"💰 更新持仓 {symbol} 价格: {current_price:.2f}")
    
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
        
        # 测试LLM连接
        print("3. 测试LLM连接...")
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