"""
任务调度模块
负责分层定时任务系统的管理
"""

import time
import threading
from datetime import datetime
from typing import Dict, Any, Callable
from collections import defaultdict
from loguru import logger


class TaskScheduler:
    """任务调度器类"""
    
    def __init__(self, layered_config: Dict[str, Any], is_trading_time_func: Callable):
        """
        初始化任务调度器
        
        Args:
            layered_config: 分层任务配置
            is_trading_time_func: 检查交易时间的函数
        """
        self.layered_config = layered_config
        self.is_trading_time = is_trading_time_func
        
        # 分层任务线程和事件
        self.task_threads = {}
        self.stop_events = {}
        
        # 任务状态监控
        self.task_status = defaultdict(dict)
        self.task_failures = defaultdict(int)
    
    def start_price_monitoring(self, price_monitoring_func: Callable) -> None:
        """
        启动价格监控任务（1分钟间隔）
        
        Args:
            price_monitoring_func: 价格监控任务函数
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
                    
                    # 执行价格监控任务
                    success = price_monitoring_func()
                    
                    if success:
                        # 更新任务状态
                        self.task_status['price_monitoring'] = {
                            'last_run': datetime.now(),
                            'status': 'success',
                            'duration': time.time() - start_time
                        }
                        self.task_failures['price_monitoring'] = 0
                        
                        print(f"✅ 价格监控完成，耗时 {time.time() - start_time:.2f}s")
                    else:
                        raise Exception("价格监控任务执行失败")
                    
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
    
    def start_ai_decision_maker(self, ai_decision_func: Callable) -> None:
        """
        启动AI决策任务（10分钟间隔）
        
        Args:
            ai_decision_func: AI决策任务函数
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
            decision_cache = {}
            
            while not stop_event.is_set():
                try:
                    # 检查是否为交易时间
                    if not self.is_trading_time():
                        time.sleep(interval)
                        continue
                    
                    start_time = time.time()
                    print(f"🤖 [{datetime.now().strftime('%H:%M:%S')}] 开始AI自主交易决策任务...")
                    
                    # 检查决策缓存
                    last_decision_time = decision_cache.get('timestamp', 0)
                    if time.time() - last_decision_time < cache_duration:
                        print("📋 使用缓存的AI自主交易决策结果")
                        stop_event.wait(interval)
                        continue
                    
                    # 执行AI决策任务
                    success = ai_decision_func()
                    
                    if success:
                        # 缓存决策结果
                        decision_cache = {
                            'timestamp': time.time(),
                            'success': True
                        }
                        
                        # 更新任务状态
                        self.task_status['ai_decision'] = {
                            'last_run': datetime.now(),
                            'status': 'success',
                            'duration': time.time() - start_time
                        }
                        self.task_failures['ai_decision'] = 0
                        
                        print(f"✅ AI自主交易决策完成，耗时 {time.time() - start_time:.2f}s")
                    else:
                        raise Exception("AI决策任务执行失败")
                    
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
    
    def start_position_updater(self, position_update_func: Callable) -> None:
        """
        启动持仓更新任务（30秒间隔）
        
        Args:
            position_update_func: 持仓更新任务函数
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
                    
                    # 执行持仓更新任务
                    success = position_update_func()
                    
                    if success:
                        # 更新任务状态
                        self.task_status['position_update'] = {
                            'last_run': datetime.now(),
                            'status': 'success',
                            'duration': time.time() - start_time
                        }
                        self.task_failures['position_update'] = 0
                        
                        print(f"✅ 持仓更新完成，耗时 {time.time() - start_time:.2f}s")
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
    
    def start_sharpe_ratio_updater(self, sharpe_ratio_update_func: Callable) -> None:
        """
        启动夏普比率更新任务（10分钟间隔）
        
        Args:
            sharpe_ratio_update_func: 夏普比率更新任务函数
        """
        if not self.layered_config.get('sharpe_ratio_update', {}).get('enabled', True):
            print("⚠️  夏普比率更新任务已禁用")
            return
        
        def sharpe_ratio_update_task():
            """夏普比率更新任务函数"""
            config = self.layered_config.get('sharpe_ratio_update', {})
            interval = config.get('interval_seconds', 600)  # 默认10分钟
            max_retries = config.get('max_retries', 3)
            retry_delay = config.get('retry_delay', 10)
            
            stop_event = self.stop_events.get('sharpe_ratio_update')
            
            while not stop_event.is_set():
                try:
                    # 检查是否为交易时间
                    if not self.is_trading_time():
                        time.sleep(interval)
                        continue
                    
                    start_time = time.time()
                    print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] 开始夏普比率更新任务...")
                    
                    # 执行夏普比率更新任务
                    success = sharpe_ratio_update_func()
                    
                    # 更新任务状态
                    self.task_status['sharpe_ratio_update'] = {
                        'last_run': datetime.now(),
                        'status': 'success' if success else 'failed',
                        'duration': time.time() - start_time
                    }
                    self.task_failures['sharpe_ratio_update'] = 0
                    
                    status_text = "成功" if success else "失败"
                    print(f"✅ 夏普比率更新完成，耗时 {time.time() - start_time:.2f}s，状态: {status_text}")
                    
                    # 等待下次执行
                    stop_event.wait(interval)
                    
                except Exception as e:
                    print(f"❌ 夏普比率更新任务失败: {e}")
                    self.task_failures['sharpe_ratio_update'] += 1
                    self.task_status['sharpe_ratio_update'] = {
                        'last_run': datetime.now(),
                        'status': 'failed',
                        'error': str(e),
                        'failures': self.task_failures['sharpe_ratio_update']
                    }
                    
                    # 检查失败次数
                    max_failures = self.layered_config.get('monitoring', {}).get('max_task_failures', 5)
                    if self.task_failures['sharpe_ratio_update'] >= max_failures:
                        print(f"⚠️  夏普比率更新任务失败次数过多，停止执行")
                        break
                    
                    # 重试延迟
                    time.sleep(retry_delay)
        
        # 创建停止事件和线程
        self.stop_events['sharpe_ratio_update'] = threading.Event()
        thread = threading.Thread(target=sharpe_ratio_update_task, name="SharpeRatioUpdate")
        thread.daemon = True
        thread.start()
        self.task_threads['sharpe_ratio_update'] = thread
        
        print("📊 夏普比率更新任务已启动（10分钟间隔）")
    
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
        
        print("✅ 所有定时任务已停止")
    
    def get_task_status(self) -> Dict[str, Dict]:
        """
        获取任务状态（用于监控）
        
        Returns:
            任务状态字典
        """
        return dict(self.task_status)