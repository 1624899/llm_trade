"""
A股ETF交易分析系统主程序 - AI自主交易模式
整合所有模块，提供完整的AI自主ETF交易功能
"""

import sys
import os
import argparse
import threading
import time
import schedule
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from loguru import logger

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
    """ETF交易系统主类 - AI自主交易模式"""
    
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
        
        # 分层任务配置
        self.layered_config = self.config.get('layered_tasks', {})
        self.data_api_config = self.config.get('data_api', {})
        
        # 分层任务线程和事件
        self.task_threads = {}
        self.stop_events = {}
        
        # 数据缓存
        self.price_cache = {}
        self.decision_cache = {}
        self.position_cache = {}
        self.cache_lock = threading.Lock()
        
        # 任务状态监控
        self.task_status = defaultdict(dict)
        self.task_failures = defaultdict(int)
        
        # 数据接口缓存
        self.realtime_prices_cache = []
        self.position_history_cache = []
        self.trade_records_cache = []
        
        # 交易冷却时间控制
        self.last_trade_time = {}  # 记录每个ETF的最后交易时间
        self.trade_cooldown_seconds = 300  # 5分钟冷却时间
        
        print("🚀 A股ETF交易分析系统 - AI自主交易模式初始化完成")
        print(f"📊 监控ETF数量: {len(self.monitored_etfs)}")
        print(f"💰 当前持仓数量: {len(self.account_manager.get_positions())}")
        if self.test_mode:
            print("🧪 系统处于测试模式，将跳过LLM交互")
        else:
            print(f"🤖 LLM模型: {self.llm_client.get_model_info()['active_model']} - {self.llm_client.get_model_info()['model']}")
    
    def run_trading_decision(self) -> bool:
        """
        运行AI自主交易决策模式
        
        Returns:
            决策是否成功
        """
        try:
            print("\n" + "="*60)
            print("🎯 开始AI自主交易决策...")
            print(f"⏰ 决策时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
            
            # 1. 获取市场数据
            print("📡 正在获取市场数据...")
            try:
                market_data = self._collect_market_data()
                
                if not market_data:
                    print("❌ 市场数据获取失败，决策终止")
                    logger.error("市场数据获取失败，决策终止")
                    return False
                
                print(f"✅ 成功获取 {len(market_data)} 个ETF的市场数据")
                logger.info(f"成功获取 {len(market_data)} 个ETF的市场数据")
            except Exception as e:
                print(f"❌ 市场数据获取异常: {e}")
                logger.error(f"市场数据获取异常: {e}")
                return False
            
            # 2. 生成AI自主交易决策语料
            print("📝 正在生成AI自主交易决策语料...")
            try:
                account_data = self.account_manager.account_data
                prompt = self.prompt_generator.generate_trading_decision_prompt(market_data, account_data)
                
                if not prompt:
                    print("❌ AI自主交易决策语料生成失败，决策终止")
                    logger.error("AI自主交易决策语料生成失败，决策终止")
                    return False
                
                print("✅ AI自主交易决策语料生成成功")
                logger.info("AI自主交易决策语料生成成功")
            except Exception as e:
                print(f"❌ AI自主交易决策语料生成异常: {e}")
                logger.error(f"AI自主交易决策语料生成异常: {e}")
                return False
            
            # 3. 调用LLM获取交易决策（测试模式下跳过）
            if self.test_mode:
                print("🧪 测试模式：跳过LLM交互，使用模拟决策")
                trading_decision = self._generate_test_decision_response()
                logger.info("测试模式：使用模拟决策")
            else:
                print("🤖 正在调用AI模型生成AI自主交易决策...")
                try:
                    trading_decision = self.llm_client.generate_trading_decision(prompt, account_data=account_data)
                    
                    if not trading_decision:
                        print("❌ AI自主交易决策生成失败")
                        logger.error("AI自主交易决策生成失败")
                        return False
                    
                    print("✅ AI自主交易决策生成成功")
                    logger.info(f"AI自主交易决策生成成功: {trading_decision.get('decision', 'UNKNOWN')} {trading_decision.get('symbol', '')}")
                except Exception as e:
                    print(f"❌ AI自主交易决策生成异常: {e}")
                    logger.error(f"AI自主交易决策生成异常: {e}")
                    return False
            
            # 4. 保存决策结果
            self._save_decision_results(prompt, trading_decision, market_data, account_data)
            
            # 5. 执行AI决策
            print(f"🎯 开始执行AI决策: {trading_decision.get('decision', 'UNKNOWN')} {trading_decision.get('symbol', '')}")
            try:
                execution_success = self.execute_ai_decision_task(trading_decision)
                
                # 6. 显示决策结果
                self._display_decision_results(trading_decision)
                
                execution_status = "执行成功" if execution_success else "执行失败"
                print(f"\n🎉 AI自主交易决策完成！{execution_status}")
                logger.info(f"AI自主交易决策完成，状态: {execution_status}")
                return execution_success
            except Exception as e:
                print(f"❌ AI决策执行异常: {e}")
                logger.error(f"AI决策执行异常: {e}")
                return False
            
        except Exception as e:
            print(f"❌ AI自主交易决策过程中发生错误: {e}")
            logger.error(f"AI自主交易决策过程中发生错误: {e}")
            return False
    
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
    
    def _save_decision_results(self, prompt: str, decision: Dict[str, Any], 
                             market_data: Dict[str, Any], account_data: Dict[str, Any]) -> None:
        """
        保存AI自主交易决策结果
        
        Args:
            prompt: 决策语料
            decision: 交易决策
            market_data: 市场数据
            account_data: 账户数据
        """
        try:
            import os
            import json
            from datetime import datetime
            
            # 保存语料
            prompt_file = self.prompt_generator.save_prompt_to_file(prompt, f"ai_trading_decision_prompt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
            if prompt_file:
                print(f"📄 AI自主交易决策语料已保存: {prompt_file}")
            
            # 保存决策JSON
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            decision_file = f"ai_trading_decision_{timestamp}.json"
            
            # 确保输出目录存在
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)
            
            decision_path = os.path.join(output_dir, decision_file)
            
            # 创建完整的决策记录
            decision_record = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "model_used": f"{self.llm_client.get_model_info()['active_model']} - {self.llm_client.get_model_info()['model']}",
                "decision": decision,
                "market_snapshot": {},
                "account_snapshot": account_data.get("account_info", {}),
                "current_positions": account_data.get("positions", [])
            }
            
            # 添加市场快照
            for symbol, data in market_data.items():
                # 跳过市场情绪数据（以_开头）
                if symbol.startswith('_'):
                    continue
                    
                # 确保data是字典类型
                if not isinstance(data, dict):
                    print(f"⚠️  跳过非字典类型的市场数据: {symbol}")
                    continue
                    
                current_data = data.get("current_data", {})
                decision_record["market_snapshot"][symbol] = {
                    "name": data.get("name", ""),
                    "current_price": current_data.get("current_price", 0),
                    "current_ema_long": current_data.get("current_ema_long", 0),
                    "current_macd": current_data.get("current_macd", 0),
                    "current_rsi_7": current_data.get("current_rsi_7", 0)
                }
            
            # 保存决策文件
            with open(decision_path, 'w', encoding='utf-8') as f:
                json.dump(decision_record, f, ensure_ascii=False, indent=2)
            
            print(f"📊 AI自主交易决策JSON已保存: {decision_path}")
            
            # 保存市场数据缓存
            self.data_fetcher.save_cache_to_file()
            print("💾 市场数据已保存到 data/market_data 目录")
                
        except Exception as e:
            print(f"⚠️  保存AI自主交易决策结果失败: {e}")
    
    def _display_decision_results(self, decision: Dict[str, Any]) -> None:
        """
        显示AI自主交易决策结果（支持多股票格式）
        
        Args:
            decision: 交易决策字典（单股票或多股票格式）
        """
        print("\n" + "="*60)
        print("🎯 AI自主交易决策")
        print("="*60)
        
        # 检查是否为多股票格式
        if "trading_decisions" in decision:
            print("🔄 多股票交易决策:")
            print("="*60)
            
            trading_decisions = decision.get("trading_decisions", [])
            
            if not trading_decisions:
                print("📋 无交易决策，建议观望")
            else:
                for i, single_decision in enumerate(trading_decisions):
                    print(f"\n📊 决策 {i+1}/{len(trading_decisions)}:")
                    self._display_single_decision_info(single_decision)
                    
                    # 添加分隔线
                    if i < len(trading_decisions) - 1:
                        print("-" * 40)
            
            print("\n" + "="*60)
            print(f"🎯 总计: {len(trading_decisions)} 个交易决策")
        else:
            # 单股票格式
            print("🔄 单股票交易决策:")
            print("="*60)
            self._display_single_decision_info(decision)
        
        # 显示账户摘要
        account_summary = self.account_manager.export_account_summary()
        print("\n📊 账户摘要")
        print("="*60)
        print(account_summary)
    
    def _display_single_decision_info(self, decision: Dict[str, Any]) -> None:
        """
        显示单个决策的详细信息
        
        Args:
            decision: 单个交易决策字典
        """
        decision_type = decision.get('decision', 'UNKNOWN')
        symbol = decision.get('symbol', 'N/A')
        amount = decision.get('amount', 0)
        quantity = decision.get('quantity', 0)
        confidence = decision.get('confidence', 0)
        reason = decision.get('reason', '')
        
        print(f"📋 决策类型: {decision_type}")
        print(f"📈 ETF代码: {symbol}")
        print(f"💰 交易金额: {amount:.2f}")
        print(f"📊 交易数量: {quantity}")
        print(f"🎯 置信度: {confidence:.2f}")
        print(f"💡 决策理由: {reason}")
        
        # 显示风险提示
        if decision_type == "BUY":
            print("\n⚠️  风险提示: 买入决策已通过风险控制验证，单次交易金额不超过总资产的10%")
        elif decision_type == "SELL":
            print("\n⚠️  风险提示: 卖出决策已验证持仓充足性")
        elif decision_type == "HOLD":
            print("\nℹ️  信息提示: 当前市场状况建议持有观望")
    
    def _generate_test_decision_response(self) -> Dict[str, Any]:
        """
        生成测试模式下的模拟AI自主交易决策响应
        
        Returns:
            模拟的交易决策字典
        """
        test_decision = {
            "decision": "BUY",
            "symbol": "512010",
            "amount": 1000.0,
            "quantity": 2400,
            "confidence": 0.75,
            "reason": "测试模式：技术指标显示超卖反弹迹象，RSI低于30，MACD即将金叉，建议适量买入"
        }
        return test_decision
    
    def test_system(self) -> None:
        """测试系统功能"""
        print("\n🧪 开始AI自主交易系统测试...")
        
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
        
        print("🎉 AI自主交易系统测试完成，所有功能正常！")
    
    def run_automatic_trading(self) -> None:
        """
        运行自动交易模式（分层定时任务系统）
        """
        print("\n🚀 启动AI自主交易自动模式...")
        print("分层定时任务系统启动中...")
        
        try:
            # 启动所有分层任务
            self.start_price_monitoring()
            self.start_ai_decision_maker()
            self.start_position_updater()
            
            print("✅ 所有分层任务已启动")
            print("按 Ctrl+C 停止所有任务")
            
            # 获取休眠配置
            trading_config = self.layered_config.get('trading_hours', {})
            sleep_interval = trading_config.get('sleep_interval', 60)  # 默认60秒休眠
            
            # 主循环，等待停止信号
            self.is_running = True
            while self.is_running:
                # 检查是否为交易时间
                if self.is_trading_time():
                    # 交易时间内正常运行
                    print(f"📈 当前为交易时间，系统正常运行...")
                else:
                    # 非交易时间显示休眠状态
                    print(f"💤 当前非交易时间，系统休眠中...")
                
                # 休眠一段时间再检查
                time.sleep(sleep_interval)
                
        except KeyboardInterrupt:
            print("\n⏹️  用户停止AI自主交易自动模式")
            self.stop_all_tasks()
        except Exception as e:
            print(f"\n❌ AI自主交易自动模式发生错误: {e}")
            self.stop_all_tasks()
    
    def start_price_monitoring(self) -> None:
        """
        启动价格监控任务（1分钟间隔）
        """
        if not self.layered_config.get('price_monitoring', {}).get('enabled', False):
            print("⚠️  价格监控任务已禁用")
            return
        
        def price_monitoring_task():
            """价格监控任务函数"""
            config = self.layered_config.get('price_monitoring', {})
            interval = config.get('interval_seconds', 60)
            max_retries = config.get('max_retries', 3)
            retry_delay = config.get('retry_delay', 5)
            
            stop_event = self.stop_events.get('price_monitoring')
            
            while not stop_event.is_set():
                try:
                    # 检查是否为交易时间
                    if not self.is_trading_time():
                        time.sleep(interval)
                        continue
                    
                    start_time = time.time()
                    print(f"📡 [{datetime.now().strftime('%H:%M:%S')}] 开始价格监控任务...")
                    
                    # 获取所有ETF的实时价格
                    etf_codes = [etf['code'] for etf in self.monitored_etfs]
                    price_data = self.data_fetcher.get_multiple_etf_data(etf_codes)
                    
                    if price_data:
                        # 更新价格缓存（保留时间戳）
                        with self.cache_lock:
                            current_time = datetime.now()
                            for code, data in price_data.items():
                                # 保留原有时间戳，更新价格数据
                                if code in self.price_cache:
                                    old_timestamp = self.price_cache[code].get('timestamp')
                                    self.price_cache[code] = data
                                    if old_timestamp:
                                        self.price_cache[code]['timestamp'] = old_timestamp
                                    else:
                                        self.price_cache[code]['timestamp'] = current_time
                                else:
                                    self.price_cache[code] = data
                                    self.price_cache[code]['timestamp'] = current_time
                            
                            # 更新实时价格数据接口缓存
                            for code, data in price_data.items():
                                price_record = {
                                    'timestamp': current_time,
                                    'code': code,
                                    'name': data.get('name', ''),
                                    'price': data.get('current_price', 0),
                                    'change_pct': data.get('change_pct', 0)
                                }
                                self.realtime_prices_cache.append(price_record)
                            
                            # 限制缓存大小
                            max_points = self.data_api_config.get('realtime_prices', {}).get('max_history_points', 100)
                            if len(self.realtime_prices_cache) > max_points:
                                self.realtime_prices_cache = self.realtime_prices_cache[-max_points:]
                        
                        # 更新任务状态
                        self.task_status['price_monitoring'] = {
                            'last_run': datetime.now(),
                            'status': 'success',
                            'duration': time.time() - start_time,
                            'etf_count': len(price_data)
                        }
                        self.task_failures['price_monitoring'] = 0
                        
                        print(f"✅ 价格监控完成，获取 {len(price_data)} 个ETF数据，耗时 {time.time() - start_time:.2f}s")
                    else:
                        raise Exception("获取价格数据失败")
                    
                    # 等待下次执行
                    stop_event.wait(interval)
                    
                except Exception as e:
                    print(f"❌ 价格监控任务失败: {e}")
                    self.task_failures['price_monitoring'] += 1
                    self.task_status['price_monitoring'] = {
                        'last_run': datetime.now(),
                        'status': 'failed',
                        'error': str(e),
                        'failures': self.task_failures['price_monitoring']
                    }
                    
                    # 检查失败次数
                    max_failures = self.layered_config.get('monitoring', {}).get('max_task_failures', 5)
                    if self.task_failures['price_monitoring'] >= max_failures:
                        print(f"⚠️  价格监控任务失败次数过多，停止执行")
                        break
                    
                    # 重试延迟
                    time.sleep(retry_delay)
        
        # 创建停止事件和线程
        self.stop_events['price_monitoring'] = threading.Event()
        thread = threading.Thread(target=price_monitoring_task, name="PriceMonitoring")
        thread.daemon = True
        thread.start()
        self.task_threads['price_monitoring'] = thread
        
        print("📡 价格监控任务已启动（1分钟间隔）")
    
    def start_ai_decision_maker(self) -> None:
        """
        启动AI决策任务（10分钟间隔）
        """
        if not self.layered_config.get('ai_decision', {}).get('enabled', False):
            print("⚠️  AI决策任务已禁用")
            return
        
        def ai_decision_task():
            """AI决策任务函数"""
            config = self.layered_config.get('ai_decision', {})
            interval = config.get('interval_seconds', 600)
            max_retries = config.get('max_retries', 2)
            retry_delay = config.get('retry_delay', 10)
            cache_duration = config.get('decision_cache_duration', 300)
            
            stop_event = self.stop_events.get('ai_decision')
            
            while not stop_event.is_set():
                try:
                    # 检查是否为交易时间
                    if not self.is_trading_time():
                        time.sleep(interval)
                        continue
                    
                    start_time = time.time()
                    print(f"🤖 [{datetime.now().strftime('%H:%M:%S')}] 开始AI自主交易决策任务...")
                    
                    # 检查决策缓存
                    with self.cache_lock:
                        last_decision_time = self.decision_cache.get('timestamp', 0)
                        if time.time() - last_decision_time < cache_duration:
                            print("📋 使用缓存的AI自主交易决策结果")
                            stop_event.wait(interval)
                            continue
                    
                    # 获取市场数据
                    market_data = self._collect_market_data()
                    if not market_data:
                        raise Exception("获取市场数据失败")
                    
                    # 确保价格缓存是最新的
                    self._update_price_cache()
                    
                    # 生成交易决策
                    account_data = self.account_manager.account_data
                    prompt = self.prompt_generator.generate_trading_decision_prompt(market_data, account_data)
                    
                    if not prompt:
                        raise Exception("生成AI自主交易决策语料失败")
                    
                    # 调用LLM获取决策
                    if self.test_mode:
                        trading_decision = self._generate_test_decision_response()
                    else:
                        trading_decision = self.llm_client.generate_trading_decision(prompt, account_data=account_data)
                    
                    if not trading_decision:
                        raise Exception("生成AI自主交易决策失败")
                    
                    # 缓存决策结果
                    with self.cache_lock:
                        self.decision_cache = {
                            'timestamp': time.time(),
                            'decision': trading_decision,
                            'market_data': market_data
                        }
                    
                    # 保存决策结果
                    self._save_decision_results(prompt, trading_decision, market_data, account_data)
                    
                    # 执行AI决策
                    print(f"🎯 开始执行AI决策: {trading_decision.get('decision', 'UNKNOWN')} {trading_decision.get('symbol', '')}")
                    execution_success = self.execute_ai_decision_task(trading_decision)
                    
                    # 更新任务状态
                    self.task_status['ai_decision'] = {
                        'last_run': datetime.now(),
                        'status': 'success' if execution_success else 'execution_failed',
                        'duration': time.time() - start_time,
                        'decision_type': trading_decision.get('decision', 'UNKNOWN'),
                        'execution_success': execution_success
                    }
                    self.task_failures['ai_decision'] = 0
                    
                    execution_status = "执行成功" if execution_success else "执行失败"
                    print(f"✅ AI自主交易决策完成，类型: {trading_decision.get('decision', 'UNKNOWN')}，{execution_status}，耗时 {time.time() - start_time:.2f}s")
                    
                    # 等待下次执行
                    stop_event.wait(interval)
                    
                except Exception as e:
                    print(f"❌ AI自主交易决策任务失败: {e}")
                    self.task_failures['ai_decision'] += 1
                    self.task_status['ai_decision'] = {
                        'last_run': datetime.now(),
                        'status': 'failed',
                        'error': str(e),
                        'failures': self.task_failures['ai_decision']
                    }
                    
                    # 检查失败次数
                    max_failures = self.layered_config.get('monitoring', {}).get('max_task_failures', 5)
                    if self.task_failures['ai_decision'] >= max_failures:
                        print(f"⚠️  AI自主交易决策任务失败次数过多，停止执行")
                        break
                    
                    # 重试延迟
                    time.sleep(retry_delay)
        
        # 创建停止事件和线程
        self.stop_events['ai_decision'] = threading.Event()
        thread = threading.Thread(target=ai_decision_task, name="AIDecision")
        thread.daemon = True
        thread.start()
        self.task_threads['ai_decision'] = thread
        
        print("🤖 AI自主交易决策任务已启动（10分钟间隔）")
    
    def execute_ai_decision_task(self, trading_decision: Dict[str, Any]) -> bool:
        """
        执行AI决策任务（支持多股票格式）
        
        Args:
            trading_decision: AI交易决策字典
            
        Returns:
            是否执行成功
        """
        try:
            print(f"🎯 [{datetime.now().strftime('%H:%M:%S')}] 开始执行AI决策...")
            
            # 检查是否为多股票格式
            if "trading_decisions" in trading_decision:
                print("🔄 检测到多股票交易决策格式")
                return self._execute_multi_stock_trading_task(trading_decision)
            else:
                print("🔄 检测到单股票交易决策格式")
                return self._execute_single_stock_trading_task(trading_decision)
            
        except Exception as e:
            print(f"❌ AI决策执行过程中发生错误: {e}")
            return False
    
    def _execute_single_stock_trading_task(self, trading_decision: Dict[str, Any]) -> bool:
        """
        执行单股票交易任务
        
        Args:
            trading_decision: 单股票交易决策字典
            
        Returns:
            是否执行成功
        """
        try:
            # 1. 验证交易决策
            validated_decision = self.validate_trading_decision(trading_decision)
            if not validated_decision:
                print("❌ 交易决策验证失败")
                return False
            
            # 2. 执行交易
            execution_result = self.execute_trading_decision(validated_decision)
            if not execution_result:
                print("❌ 交易执行失败")
                return False
            
            # 3. 处理交易结果
            self.handle_trading_result(validated_decision, execution_result)
            
            print("✅ 单股票AI决策执行完成")
            return True
            
        except Exception as e:
            print(f"❌ 单股票AI决策执行失败: {e}")
            return False
    
    def _execute_multi_stock_trading_task(self, trading_decision: Dict[str, Any]) -> bool:
        """
        执行多股票交易任务
        
        Args:
            trading_decision: 多股票交易决策字典
            
        Returns:
            是否执行成功
        """
        try:
            trading_decisions = trading_decision.get("trading_decisions", [])
            
            if not trading_decisions:
                print("📋 多股票决策数组为空，无需执行")
                return True
            
            successful_executions = 0
            total_executions = len(trading_decisions)
            
            print(f"🔄 开始执行 {total_executions} 个交易决策...")
            
            for i, single_decision in enumerate(trading_decisions):
                print(f"\n🔄 执行第 {i+1}/{total_executions} 个交易决策")
                print(f"   决策: {single_decision.get('decision', 'N/A')} {single_decision.get('symbol', 'N/A')}")
                
                if self._execute_single_stock_trading_task(single_decision):
                    successful_executions += 1
                    print(f"   ✅ 第 {i+1} 个交易决策执行成功")
                else:
                    print(f"   ⚠️  第 {i+1} 个交易决策执行失败，继续下一个")
            
            print(f"\n🎯 多股票交易执行完成: {successful_executions}/{total_executions} 个决策成功")
            
            # 如果至少有一个决策成功，就认为整体成功
            return successful_executions > 0
            
        except Exception as e:
            print(f"❌ 多股票AI决策执行失败: {e}")
            return False
    
    def validate_trading_decision(self, trading_decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        验证交易决策的可行性（支持多股票格式）
        
        Args:
            trading_decision: AI交易决策字典（单股票或多股票格式）
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            # 检查是否为新的多股票格式
            if "trading_decisions" in trading_decision:
                print("🔄 检测到多股票交易决策格式")
                return self._validate_multi_stock_trading_decision(trading_decision)
            else:
                print("🔄 检测到单股票交易决策格式")
                return self._validate_single_stock_trading_decision(trading_decision)
            
        except Exception as e:
            print(f"❌ 验证交易决策失败: {e}")
            return None
    
    def _validate_single_stock_trading_decision(self, trading_decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        验证单股票交易决策
        
        Args:
            trading_decision: 单股票交易决策字典
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            # 检查必要字段
            required_fields = ["decision", "symbol", "confidence", "reason"]
            for field in required_fields:
                if field not in trading_decision:
                    print(f"❌ 交易决策缺少必要字段: {field}")
                    return None
            
            decision_type = trading_decision.get('decision', '').upper()
            symbol = trading_decision.get('symbol', '')
            confidence = trading_decision.get('confidence', 0)
            
            # 验证决策类型
            if decision_type not in ["BUY", "SELL", "HOLD"]:
                print(f"❌ 无效的决策类型: {decision_type}")
                return None
            
            # 验证置信度
            if confidence < 0.5:  # 置信度阈值
                print(f"❌ 决策置信度过低: {confidence}")
                return None
            
            # 验证ETF代码
            if not symbol or len(symbol) != 6:
                print(f"❌ 无效的ETF代码: {symbol}, 类型: {type(symbol)}")
                return None
            
            # 检查是否为交易时间
            if not self.is_trading_time():
                print("⚠️  当前非交易时间，跳过交易执行")
                return None
            
            # 检查交易冷却时间
            if decision_type in ["BUY", "SELL"]:
                current_time = time.time()
                last_trade_time = self.last_trade_time.get(symbol, 0)
                
                if current_time - last_trade_time < self.trade_cooldown_seconds:
                    remaining_time = self.trade_cooldown_seconds - (current_time - last_trade_time)
                    print(f"⚠️  ETF {symbol} 仍在冷却时间内，剩余 {remaining_time:.0f} 秒")
                    return None
            
            # 获取账户信息
            account_info = self.account_manager.get_account_info()
            total_assets = account_info.get('total_assets', 0)
            available_cash = account_info.get('available_cash', 0)
            
            # 风险控制验证
            if decision_type == "BUY":
                # 检查买入金额
                amount = trading_decision.get('amount', 0)
                max_single_trade = total_assets * 0.1  # 单次交易不超过总资产10%
                
                if amount > max_single_trade:
                    print(f"⚠️  买入金额{amount}超过限制{max_single_trade}，调整为最大限制")
                    trading_decision["amount"] = max_single_trade
                
                if amount > available_cash:
                    print(f"⚠️  买入金额{amount}超过可用现金{available_cash}，调整为可用现金金额")
                    trading_decision["amount"] = available_cash
                
                if trading_decision.get('amount', 0) <= 0:
                    print("❌ 买入金额无效")
                    return None
            
            elif decision_type == "SELL":
                # 检查持仓
                position = self.account_manager.get_position_by_symbol(symbol)
                if not position:
                    print(f"❌ 没有ETF {symbol} 的持仓，无法卖出")
                    return None
                
                available_quantity = position.get('available_quantity', 0)
                sell_quantity = trading_decision.get('quantity', 0)
                
                if sell_quantity > available_quantity:
                    print(f"⚠️  卖出数量{sell_quantity}超过可用持仓{available_quantity}，调整为可用持仓数量")
                    trading_decision["quantity"] = available_quantity
                
                if trading_decision.get('quantity', 0) <= 0:
                    print("❌ 卖出数量无效")
                    return None
            
            print(f"✅ 单股票交易决策验证通过: {decision_type} {symbol}")
            return trading_decision
            
        except Exception as e:
            print(f"❌ 验证单股票交易决策失败: {e}")
            return None
    
    def _validate_multi_stock_trading_decision(self, trading_decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        验证多股票交易决策
        
        Args:
            trading_decision: 多股票交易决策字典
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            trading_decisions = trading_decision.get("trading_decisions", [])
            
            if not isinstance(trading_decisions, list):
                print("❌ trading_decisions 必须是数组格式")
                return None
            
            if not trading_decisions:
                print("📋 多股票决策数组为空，返回HOLD决策")
                return {"trading_decisions": []}
            
            validated_decisions = []
            
            for i, single_decision in enumerate(trading_decisions):
                print(f"🔍 验证第 {i+1} 个交易决策")
                
                validated_single = self._validate_single_stock_trading_decision(single_decision)
                if validated_single:
                    validated_decisions.append(validated_single)
                    print(f"✅ 第 {i+1} 个交易决策验证通过")
                else:
                    print(f"⚠️  第 {i+1} 个交易决策验证失败，跳过")
            
            if not validated_decisions:
                print("❌ 所有交易决策验证失败")
                return None
            
            print(f"✅ 多股票交易决策验证通过，有效决策数: {len(validated_decisions)}")
            return {"trading_decisions": validated_decisions}
            
        except Exception as e:
            print(f"❌ 验证多股票交易决策失败: {e}")
            return None
    
    def execute_trading_decision(self, trading_decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        执行具体的交易操作
        
        Args:
            trading_decision: 验证后的交易决策字典
            
        Returns:
            交易执行结果字典，如果执行失败返回None
        """
        try:
            decision_type = trading_decision.get('decision', '').upper()
            symbol = trading_decision.get('symbol', '')
            
            # 获取ETF名称
            etf_name = ""
            for etf_info in self.monitored_etfs:
                if etf_info.get('code') == symbol:
                    etf_name = etf_info.get('name', '')
                    break
            
            if not etf_name:
                print(f"⚠️  未找到ETF {symbol} 的名称信息")
                etf_name = f"ETF_{symbol}"
            
            # 获取当前价格（带重试机制）
            current_price = self._get_current_price_with_retry(symbol, etf_name)
            
            if current_price <= 0:
                print(f"❌ 无法获取ETF {symbol} 的当前价格")
                return None
            
            execution_result = {
                'decision_type': decision_type,
                'symbol': symbol,
                'name': etf_name,
                'price': current_price,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'success': False,
                'message': ''
            }
            
            if decision_type == "BUY":
                # 计算买入数量
                amount = trading_decision.get('amount', 0)
                quantity = trading_decision.get('quantity', 0)
                
                if amount > 0 and quantity <= 0:
                    # 根据金额计算数量
                    quantity = int(amount / current_price / 100) * 100  # 按手数计算，1手=100股
                    if quantity <= 0:
                        quantity = 100  # 至少买1手
                
                elif quantity > 0 and amount <= 0:
                    # 根据数量计算金额
                    amount = quantity * current_price
                
                # 执行买入
                success = self.account_manager.buy_etf(symbol, etf_name, quantity, current_price)
                
                if success:
                    execution_result['success'] = True
                    execution_result['quantity'] = quantity
                    execution_result['amount'] = amount
                    execution_result['message'] = f"成功买入 {symbol} {quantity}股，价格 {current_price:.3f}"
                    print(f"✅ {execution_result['message']}")
                    
                    # 如果交易决策中包含盈利/止损信息，更新持仓
                    if 'profit_target_price' in trading_decision or 'profit_target' in trading_decision:
                        position = self.account_manager.get_position_by_symbol(symbol)
                        if position:
                            # 支持两种字段名：profit_target_price 和 profit_target
                            profit_target_price = trading_decision.get('profit_target_price') or trading_decision.get('profit_target', 0)
                            # 支持两种字段名：stop_loss_price 和 stop_loss
                            stop_loss_price = trading_decision.get('stop_loss_price') or trading_decision.get('stop_loss', 0)
                            profit_target_pct = trading_decision.get('profit_target_pct', 0)
                            stop_loss_pct = trading_decision.get('stop_loss_pct', 0)
                            
                            # 更新持仓的盈利/止损目标
                            if profit_target_price > 0:
                                position['profit_target_price'] = profit_target_price
                                if profit_target_pct > 0:
                                    position['profit_target_pct'] = profit_target_pct
                            
                            if stop_loss_price > 0:
                                position['stop_loss_price'] = stop_loss_price
                                if stop_loss_pct > 0:
                                    position['stop_loss_pct'] = stop_loss_pct
                            
                            # 保存账户数据
                            self.account_manager._save_account_data()
                            print(f"✅ 已更新 {symbol} 的盈利/止损目标")
                else:
                    execution_result['message'] = f"买入 {symbol} 失败"
                    print(f"❌ {execution_result['message']}")
                    return None
            
            elif decision_type == "SELL":
                # 卖出数量
                quantity = trading_decision.get('quantity', 0)
                amount = quantity * current_price
                
                # 执行卖出
                success = self.account_manager.sell_etf(symbol, quantity, current_price)
                
                if success:
                    execution_result['success'] = True
                    execution_result['quantity'] = quantity
                    execution_result['amount'] = amount
                    execution_result['message'] = f"成功卖出 {symbol} {quantity}股，价格 {current_price:.3f}"
                    print(f"✅ {execution_result['message']}")
                else:
                    execution_result['message'] = f"卖出 {symbol} 失败"
                    print(f"❌ {execution_result['message']}")
                    return None
            
            elif decision_type == "HOLD":
                execution_result['success'] = True
                execution_result['message'] = f"持有 {symbol}，不执行交易"
                print(f"📋 {execution_result['message']}")
            
            return execution_result
            
        except Exception as e:
            print(f"❌ 执行交易决策失败: {e}")
            return None
    
    def _get_current_price_with_retry(self, symbol: str, etf_name: str = "", max_retries: int = 3) -> float:
        """
        带重试机制获取当前价格
        
        Args:
            symbol: ETF代码
            etf_name: ETF名称
            max_retries: 最大重试次数
            
        Returns:
            当前价格，获取失败返回0
        """
        for attempt in range(max_retries):
            try:
                # 1. 首先尝试从价格缓存获取
                with self.cache_lock:
                    if symbol in self.price_cache:
                        cached_price = self.price_cache[symbol].get('current_price', 0)
                        if cached_price > 0:
                            logger.info(f"从缓存获取ETF {symbol} 价格: {cached_price:.3f}")
                            return cached_price
                
                # 2. 如果缓存没有，尝试实时获取
                logger.info(f"缓存无ETF {symbol} 价格数据，尝试实时获取（尝试 {attempt + 1}/{max_retries}）")
                real_time_data = self.data_fetcher.get_real_time_data(symbol)
                
                if real_time_data:
                    current_price = real_time_data.get('current_price', 0)
                    if current_price > 0:
                        # 更新缓存
                        with self.cache_lock:
                            self.price_cache[symbol] = real_time_data
                        
                        logger.info(f"实时获取ETF {symbol} 价格成功: {current_price:.3f}")
                        return current_price
                
                # 3. 如果实时获取失败，尝试从增强数据获取
                logger.info(f"实时获取失败，尝试从增强数据获取ETF {symbol} 价格")
                enhanced_data = self.data_fetcher.get_multiple_etf_advanced_data([{'code': symbol, 'name': etf_name, 'category': ''}])
                
                if enhanced_data and symbol in enhanced_data:
                    real_time_data = enhanced_data[symbol].get('real_time_data')
                    if real_time_data:
                        current_price = real_time_data.get('current_price', 0)
                        if current_price > 0:
                            # 更新缓存
                            with self.cache_lock:
                                self.price_cache[symbol] = real_time_data
                            
                            logger.info(f"从增强数据获取ETF {symbol} 价格成功: {current_price:.3f}")
                            return current_price
                
                # 如果本次尝试失败，等待后重试
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    logger.warning(f"获取ETF {symbol} 价格失败，{wait_time}秒后重试")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"获取ETF {symbol} 价格异常（尝试 {attempt + 1}/{max_retries}）: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
        
        logger.error(f"获取ETF {symbol} 价格失败，已达到最大重试次数")
        return 0.0
    
    def _update_price_cache(self) -> None:
        """
        更新价格缓存
        """
        try:
            etf_codes = [etf['code'] for etf in self.monitored_etfs]
            price_data = self.data_fetcher.get_multiple_etf_data(etf_codes)
            
            if price_data:
                with self.cache_lock:
                    # 更新价格缓存，但保留时间戳信息
                    for code, data in price_data.items():
                        if code in self.price_cache:
                            # 保留原有的时间戳，更新价格数据
                            old_timestamp = self.price_cache[code].get('timestamp')
                            self.price_cache[code] = data
                            if old_timestamp:
                                self.price_cache[code]['timestamp'] = old_timestamp
                        else:
                            self.price_cache[code] = data
                
                logger.info(f"价格缓存更新完成，更新了 {len(price_data)} 个ETF数据")
            else:
                logger.warning("价格缓存更新失败，未获取到有效数据")
                
        except Exception as e:
            logger.error(f"更新价格缓存失败: {e}")
    
    def handle_trading_result(self, trading_decision: Dict[str, Any],
                            execution_result: Dict[str, Any]) -> None:
        """
        处理交易执行结果
        
        Args:
            trading_decision: 交易决策字典
            execution_result: 交易执行结果字典
        """
        try:
            # 创建交易记录
            trade_record = {
                'timestamp': execution_result.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                'decision': trading_decision.get('decision', ''),
                'symbol': execution_result.get('symbol', ''),
                'name': execution_result.get('name', ''),
                'action': execution_result.get('decision_type', ''),
                'quantity': execution_result.get('quantity', 0),
                'price': execution_result.get('price', 0),
                'amount': execution_result.get('amount', 0),
                'confidence': trading_decision.get('confidence', 0),
                'reason': trading_decision.get('reason', ''),
                'success': execution_result.get('success', False),
                'message': execution_result.get('message', ''),
                'ai_decision_id': f"{trading_decision.get('symbol', '')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
            
            # 保存交易执行记录
            self._save_trade_execution_record(trade_record)
            
            # 更新交易执行状态
            self._update_trading_status(trade_record)
            
            # 记录日志
            if execution_result.get('success', False):
                print(f"📊 交易执行成功: {trade_record['symbol']} {trade_record['action']} {trade_record['quantity']}股")
                
                # 更新交易冷却时间
                if trade_record.get('action') in ['BUY', 'SELL']:
                    self.last_trade_time[trade_record['symbol']] = time.time()
                    print(f"⏰ 更新 {trade_record['symbol']} 交易冷却时间")
            else:
                print(f"⚠️  交易执行未成功: {trade_record['message']}")
            
        except Exception as e:
            print(f"❌ 处理交易结果失败: {e}")
    
    def _save_trade_execution_record(self, trade_record: Dict[str, Any]) -> None:
        """
        保存交易执行记录
        
        Args:
            trade_record: 交易记录字典
        """
        try:
            import os
            import json
            
            # 确保目录存在
            records_dir = "data/trade_executions"
            os.makedirs(records_dir, exist_ok=True)
            
            # 保存到交易执行历史文件
            history_file = os.path.join(records_dir, "execution_history.json")
            
            # 读取现有记录
            history = []
            if os.path.exists(history_file):
                try:
                    with open(history_file, 'r', encoding='utf-8') as f:
                        history = json.load(f)
                    if not isinstance(history, list):
                        history = []
                except Exception as e:
                    print(f"⚠️  读取交易执行历史失败: {e}")
                    history = []
            
            # 添加新记录
            history.append(trade_record)
            
            # 只保留最近100条记录
            history = history[-100:]
            
            # 保存历史记录
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            
            # 保存到当日记录文件
            today = datetime.now().strftime('%Y%m%d')
            today_file = os.path.join(records_dir, f"execution_{today}.json")
            
            today_history = []
            if os.path.exists(today_file):
                try:
                    with open(today_file, 'r', encoding='utf-8') as f:
                        today_history = json.load(f)
                    if not isinstance(today_history, list):
                        today_history = []
                except Exception as e:
                    print(f"⚠️  读取当日交易执行记录失败: {e}")
                    today_history = []
            
            today_history.append(trade_record)
            
            with open(today_file, 'w', encoding='utf-8') as f:
                json.dump(today_history, f, ensure_ascii=False, indent=2)
            
            print(f"💾 交易执行记录已保存")
            
        except Exception as e:
            print(f"❌ 保存交易执行记录失败: {e}")
    
    def _update_trading_status(self, trade_record: Dict[str, Any]) -> None:
        """
        更新交易执行状态
        
        Args:
            trade_record: 交易记录字典
        """
        try:
            # 更新任务状态
            self.task_status['ai_decision_execution'] = {
                'last_run': datetime.now(),
                'status': 'success' if trade_record.get('success', False) else 'failed',
                'last_trade': {
                    'symbol': trade_record.get('symbol', ''),
                    'action': trade_record.get('action', ''),
                    'success': trade_record.get('success', False),
                    'timestamp': trade_record.get('timestamp', '')
                }
            }
            
            # 更新交易记录缓存
            with self.cache_lock:
                self.trade_records_cache.append(trade_record)
                
                # 限制缓存大小
                max_records = self.data_api_config.get('trade_records', {}).get('max_records', 1000)
                if len(self.trade_records_cache) > max_records:
                    self.trade_records_cache = self.trade_records_cache[-max_records:]
            
        except Exception as e:
            print(f"❌ 更新交易状态失败: {e}")
    
    def start_position_updater(self) -> None:
        """
        启动持仓更新任务（30秒间隔）
        """
        if not self.layered_config.get('position_update', {}).get('enabled', False):
            print("⚠️  持仓更新任务已禁用")
            return
        
        def position_update_task():
            """持仓更新任务函数"""
            config = self.layered_config.get('position_update', {})
            interval = config.get('interval_seconds', 30)
            max_retries = config.get('max_retries', 3)
            retry_delay = config.get('retry_delay', 3)
            
            stop_event = self.stop_events.get('position_update')
            
            while not stop_event.is_set():
                try:
                    start_time = time.time()
                    print(f"💰 [{datetime.now().strftime('%H:%M:%S')}] 开始持仓更新任务...")
                    
                    # 获取当前持仓
                    positions = self.account_manager.get_positions()
                    
                    if positions:
                        # 更新每个持仓的当前价格
                        updated_count = 0
                        for position in positions:
                            symbol = position['symbol']
                            
                            # 从价格缓存获取最新价格
                            with self.cache_lock:
                                current_price = None
                                if symbol in self.price_cache:
                                    current_price = self.price_cache[symbol].get('current_price')
                            
                            if current_price:
                                # 更新持仓价格
                                if self.account_manager.hold_position(symbol, current_price):
                                    updated_count += 1
                        
                        # 更新持仓缓存
                        with self.cache_lock:
                            self.position_cache = {
                                'timestamp': time.time(),
                                'positions': self.account_manager.get_positions(),
                                'account_info': self.account_manager.get_account_info()
                            }
                            
                            # 更新持仓历史数据接口缓存
                            position_record = {
                                'timestamp': datetime.now(),
                                'total_assets': self.account_manager.get_total_value(),
                                'total_pnl': self.account_manager.get_total_pnl(),
                                'daily_pnl': self.account_manager.get_daily_pnl(),
                                'positions_count': len(positions),
                                'positions': positions.copy()
                            }
                            self.position_history_cache.append(position_record)
                            
                            # 限制缓存大小
                            max_records = self.data_api_config.get('position_history', {}).get('max_records', 1000)
                            if len(self.position_history_cache) > max_records:
                                self.position_history_cache = self.position_history_cache[-max_records:]
                        
                        # 更新任务状态
                        self.task_status['position_update'] = {
                            'last_run': datetime.now(),
                            'status': 'success',
                            'duration': time.time() - start_time,
                            'updated_positions': updated_count,
                            'total_positions': len(positions)
                        }
                        self.task_failures['position_update'] = 0
                        
                        print(f"✅ 持仓更新完成，更新 {updated_count}/{len(positions)} 个持仓，耗时 {time.time() - start_time:.2f}s")
                    else:
                        print("📋 当前无持仓，跳过更新")
                    
                    # 等待下次执行
                    stop_event.wait(interval)
                    
                except Exception as e:
                    print(f"❌ 持仓更新任务失败: {e}")
                    self.task_failures['position_update'] += 1
                    self.task_status['position_update'] = {
                        'last_run': datetime.now(),
                        'status': 'failed',
                        'error': str(e),
                        'failures': self.task_failures['position_update']
                    }
                    
                    # 检查失败次数
                    max_failures = self.layered_config.get('monitoring', {}).get('max_task_failures', 5)
                    if self.task_failures['position_update'] >= max_failures:
                        print(f"⚠️  持仓更新任务失败次数过多，停止执行")
                        break
                    
                    # 重试延迟
                    time.sleep(retry_delay)
        
        # 创建停止事件和线程
        self.stop_events['position_update'] = threading.Event()
        thread = threading.Thread(target=position_update_task, name="PositionUpdate")
        thread.daemon = True
        thread.start()
        self.task_threads['position_update'] = thread
        
        print("💰 持仓更新任务已启动（30秒间隔）")
    
    def stop_all_tasks(self) -> None:
        """
        停止所有定时任务
        """
        print("\n⏹️  正在停止所有定时任务...")
        
        # 设置所有停止事件
        for task_name, stop_event in self.stop_events.items():
            stop_event.set()
        
        # 等待所有线程结束
        for task_name, thread in self.task_threads.items():
            if thread.is_alive():
                print(f"⏳ 等待 {task_name} 任务结束...")
                thread.join(timeout=5)
                if thread.is_alive():
                    print(f"⚠️  {task_name} 任务未能在超时时间内结束")
        
        # 清空任务字典
        self.task_threads.clear()
        self.stop_events.clear()
        
        self.is_running = False
        print("✅ 所有定时任务已停止")
    
    def is_trading_time(self) -> bool:
        """
        检查是否为交易时间
        
        Returns:
            是否为交易时间
        """
        try:
            trading_config = self.layered_config.get('trading_hours', {})
            
            # 检查是否仅在工作日交易
            if trading_config.get('weekdays_only', True):
                now = datetime.now()
                if now.weekday() >= 5:  # 周末不交易
                    return False
            
            # 获取交易时间配置
            morning_start = trading_config.get('morning_start', "09:30")
            morning_end = trading_config.get('morning_end', "11:30")
            afternoon_start = trading_config.get('afternoon_start', "13:00")
            afternoon_end = trading_config.get('afternoon_end', "15:00")
            
            # 解析时间
            now_time = datetime.now().time()
            morning_start_time = datetime.strptime(morning_start, "%H:%M").time()
            morning_end_time = datetime.strptime(morning_end, "%H:%M").time()
            afternoon_start_time = datetime.strptime(afternoon_start, "%H:%M").time()
            afternoon_end_time = datetime.strptime(afternoon_end, "%H:%M").time()
            
            # 检查是否在交易时间内
            return (morning_start_time <= now_time <= morning_end_time or
                    afternoon_start_time <= now_time <= afternoon_end_time)
                    
        except Exception as e:
            print(f"❌ 检查交易时间失败: {e}")
            return False
    
    def get_realtime_prices_data(self) -> List[Dict]:
        """
        获取实时价格数据（用于前端接口）
        
        Returns:
            实时价格数据列表
        """
        with self.cache_lock:
            return self.realtime_prices_cache.copy()
    
    def get_position_history_data(self) -> List[Dict]:
        """
        获取持仓历史数据（用于前端接口）
        
        Returns:
            持仓历史数据列表
        """
        with self.cache_lock:
            return self.position_history_cache.copy()
    
    def get_trade_records_data(self) -> List[Dict]:
        """
        获取交易记录数据（用于前端接口）
        
        Returns:
            交易记录数据列表
        """
        # 更新交易记录缓存
        trade_history = self.account_manager.get_trade_history()
        with self.cache_lock:
            self.trade_records_cache = trade_history.copy()
            return self.trade_records_cache.copy()
    
    def get_task_status(self) -> Dict[str, Dict]:
        """
        获取任务状态（用于监控）
        
        Returns:
            任务状态字典
        """
        return dict(self.task_status)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股ETF交易分析系统 - AI自主交易模式')
    parser.add_argument('--mode', choices=['test', 'auto'],
                       default='auto', help='运行模式：test-系统测试，auto-自动交易')
    parser.add_argument('--config', type=str, default='config/config.yaml',
                       help='配置文件路径')
    
    args = parser.parse_args()
    
    try:
        # 初始化系统
        system = ETFTradingSystem(args.config)
        
        if args.mode == 'test':
            # 系统测试
            system.test_system()
            
        elif args.mode == 'auto':
            # 自动交易模式（分层定时任务系统）
            system.run_automatic_trading()
            
    except KeyboardInterrupt:
        print("\n👋 程序被用户中断")
        if 'system' in locals():
            system.stop_all_tasks()
    except Exception as e:
        print(f"\n❌ 程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()