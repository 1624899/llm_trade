"""
A股ETF交易分析系统主程序 - AI自主交易模式
整合所有模块，提供完整的AI自主ETF交易功能
"""

import sys
import os
import argparse
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
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
from src.sharpe_ratio_api import SharpeRatioAPI

# 导入重构后的模块
from src.trading_executor import TradingExecutor
from src.market_data_collector import MarketDataCollector
from src.task_scheduler import TaskScheduler
from src.risk_manager import RiskManager
from src.data_cache_manager import DataCacheManager
from src.trade_recorder import TradeRecorder


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
        
        # 初始化夏普比率API
        self.sharpe_api = SharpeRatioAPI(self.account_manager, self.indicators)
        
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
        
        # 初始化数据缓存管理器
        self.cache_manager = DataCacheManager(self.data_api_config)
        
        # 初始化各个功能模块
        self.trading_executor = TradingExecutor(
            self.account_manager, 
            self.data_fetcher, 
            self.monitored_etfs,
            self.cache_manager.cache_lock,
            self.cache_manager.price_cache
        )
        
        self.market_data_collector = MarketDataCollector(
            self.data_fetcher,
            self.indicators,
            self.monitored_etfs,
            self.cache_manager.cache_lock,
            self.cache_manager.price_cache
        )
        
        self.task_scheduler = TaskScheduler(
            self.layered_config,
            self.is_trading_time
        )
        
        self.risk_manager = RiskManager(
            self.sharpe_api,
            self.account_manager
        )
        
        self.trade_recorder = TradeRecorder()
        
        print("🚀 A股ETF交易分析系统 - AI自主交易模式初始化完成")
        print(f"📊 监控ETF数量: {len(self.monitored_etfs)}")
        print(f"💰 当前持仓数量: {len(self.account_manager.get_positions())}")
        
        # 初始化夏普比率指标
        print("📈 初始化夏普比率指标...")
        self.update_sharpe_ratio_metrics()
        
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
                market_data = self.market_data_collector.collect_market_data()
                
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
                prompt = self.prompt_generator.generate_trading_decision_prompt(market_data, account_data, self.account_manager)
                
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
    
    def execute_ai_decision_task(self, trading_decision: Dict[str, Any]) -> bool:
        """
        执行AI决策任务（委托给交易执行器）
        
        Args:
            trading_decision: AI交易决策字典
            
        Returns:
            是否执行成功
        """
        try:
            # 基于夏普比率评估交易风险
            risk_assessment = self.risk_manager.evaluate_sharpe_ratio_risk(trading_decision)
            risk_level = risk_assessment.get('risk_level', 'unknown')
            recommendation = risk_assessment.get('recommendation', 'proceed')
            
            print(f"📊 夏普比率风险评估: {risk_level}, 建议: {recommendation}")
            if risk_assessment.get('factors'):
                for factor in risk_assessment['factors']:
                    print(f"   - {factor}")
            
            # 根据风险评估调整交易决策
            adjusted_decision = self.risk_manager.adjust_decision_by_risk(trading_decision, risk_assessment)
            
            # 执行交易决策
            execution_result = self.trading_executor.execute_ai_decision_task(adjusted_decision)
            
            # 处理交易结果
            if execution_result:
                trade_record = self.trading_executor.handle_trading_result(adjusted_decision, execution_result)
                if trade_record:
                    # 保存交易记录
                    self.trade_recorder.save_trade_execution_record(trade_record)
                    
                    # 更新交易记录缓存
                    trade_history = self.trade_recorder.get_trade_history()
                    self.cache_manager.update_trade_records_cache(trade_history)
                    
                    # 交易执行成功后更新夏普比率指标
                    print("📈 交易执行成功，更新夏普比率指标...")
                    self.update_sharpe_ratio_metrics()
                
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ AI决策执行过程中发生错误: {e}")
            logger.error(f"AI决策执行过程中发生错误: {e}")
            return False
    
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
            self.task_scheduler.start_price_monitoring(self._price_monitoring_task)
            self.task_scheduler.start_ai_decision_maker(self._ai_decision_task)
            self.task_scheduler.start_position_updater(self._position_update_task)
            self.task_scheduler.start_sharpe_ratio_updater(self._sharpe_ratio_update_task)
            
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
            self.task_scheduler.stop_all_tasks()
        except Exception as e:
            print(f"\n❌ AI自主交易自动模式发生错误: {e}")
            self.task_scheduler.stop_all_tasks()
    
    def _price_monitoring_task(self) -> bool:
        """价格监控任务"""
        try:
            # 获取所有ETF的实时价格
            etf_codes = [etf['code'] for etf in self.monitored_etfs]
            price_data = self.data_fetcher.get_multiple_etf_data(etf_codes)
            
            if price_data:
                # 更新价格缓存
                self.cache_manager.update_price_cache(price_data)
                return True
            else:
                raise Exception("获取价格数据失败")
                
        except Exception as e:
            print(f"❌ 价格监控任务失败: {e}")
            return False
    
    def _ai_decision_task(self) -> bool:
        """AI决策任务"""
        try:
            # 检查决策缓存
            if self.cache_manager.is_decision_cache_valid():
                print("📋 使用缓存的AI自主交易决策结果")
                return True
            
            # 获取市场数据
            market_data = self.market_data_collector.collect_market_data()
            if not market_data:
                raise Exception("获取市场数据失败")
            
            # 确保价格缓存是最新的
            self.market_data_collector.update_price_cache()
            
            # 生成交易决策
            account_data = self.account_manager.account_data
            prompt = self.prompt_generator.generate_trading_decision_prompt(market_data, account_data, self.account_manager)
            
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
            self.cache_manager.update_decision_cache(trading_decision, market_data)
            
            # 保存决策结果
            self._save_decision_results(prompt, trading_decision, market_data, account_data)
            
            # 执行AI决策
            print(f"🎯 开始执行AI决策: {trading_decision.get('decision', 'UNKNOWN')} {trading_decision.get('symbol', '')}")
            
            execution_success = self.execute_ai_decision_task(trading_decision)
            
            return execution_success
            
        except Exception as e:
            print(f"❌ AI自主交易决策任务失败: {e}")
            return False
    
    def _position_update_task(self) -> bool:
        """持仓更新任务"""
        try:
            # 获取当前持仓
            positions = self.account_manager.get_positions()
            
            if positions:
                # 更新每个持仓的当前价格
                updated_count = 0
                for position in positions:
                    symbol = position['symbol']
                    
                    # 从价格缓存获取最新价格
                    current_price = self.cache_manager.get_cached_price(symbol)
                    
                    if current_price > 0:
                        # 更新持仓价格
                        if self.account_manager.hold_position(symbol, current_price):
                            updated_count += 1
                
                # 更新持仓缓存
                account_info = self.account_manager.get_account_info()
                self.cache_manager.update_position_cache(positions, account_info)
                
                # 定期更新夏普比率指标（每5次持仓更新一次）
                if len(self.cache_manager.position_history_cache) % 5 == 0:
                    self.update_sharpe_ratio_metrics()
                
                print(f"✅ 持仓更新完成，更新 {updated_count}/{len(positions)} 个持仓")
                return True
            else:
                print("📋 当前无持仓，跳过更新")
                return True
                
        except Exception as e:
            print(f"❌ 持仓更新任务失败: {e}")
            return False
    
    def _sharpe_ratio_update_task(self) -> bool:
        """夏普比率更新任务"""
        try:
            return self.update_sharpe_ratio_metrics()
        except Exception as e:
            print(f"❌ 夏普比率更新任务失败: {e}")
            return False
    
    def update_sharpe_ratio_metrics(self, force_refresh: bool = False) -> bool:
        """
        更新夏普比率指标（委托给风险管理器）
        
        Args:
            force_refresh: 是否强制刷新缓存
            
        Returns:
            是否更新成功
        """
        return self.risk_manager.update_sharpe_ratio_metrics(force_refresh)
    
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
        return self.cache_manager.get_realtime_prices_data()
    
    def get_position_history_data(self) -> List[Dict]:
        """
        获取持仓历史数据（用于前端接口）
        
        Returns:
            持仓历史数据列表
        """
        return self.cache_manager.get_position_history_data()
    
    def get_trade_records_data(self) -> List[Dict]:
        """
        获取交易记录数据（用于前端接口）
        
        Returns:
            交易记录数据列表
        """
        return self.cache_manager.get_trade_records_data()
    
    def get_task_status(self) -> Dict[str, Dict]:
        """
        获取任务状态（用于监控）
        
        Returns:
            任务状态字典
        """
        return self.task_scheduler.get_task_status()


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
            system.task_scheduler.stop_all_tasks()
    except Exception as e:
        print(f"\n❌ 程序运行错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()