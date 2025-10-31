"""
数据缓存管理模块
负责价格、决策、持仓等数据缓存的管理
"""

import time
import threading
from datetime import datetime
from typing import Dict, Any, List
from loguru import logger


class DataCacheManager:
    """数据缓存管理器类"""
    
    def __init__(self, data_api_config: Dict[str, Any]):
        """
        初始化数据缓存管理器
        
        Args:
            data_api_config: 数据API配置
        """
        self.data_api_config = data_api_config
        
        # 缓存锁
        self.cache_lock = threading.Lock()
        
        # 数据缓存
        self.price_cache = {}
        self.decision_cache = {}
        self.position_cache = {}
        
        # 数据接口缓存
        self.realtime_prices_cache = []
        self.position_history_cache = []
        self.trade_records_cache = []
    
    def update_price_cache(self, price_data: Dict[str, Any]) -> None:
        """
        更新价格缓存
        
        Args:
            price_data: 价格数据字典
        """
        try:
            if price_data:
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
                
                logger.info(f"价格缓存更新完成，更新了 {len(price_data)} 个ETF数据")
            else:
                logger.warning("价格缓存更新失败，未获取到有效数据")
                
        except Exception as e:
            logger.error(f"更新价格缓存失败: {e}")
    
    def update_decision_cache(self, trading_decision: Dict[str, Any], market_data: Dict[str, Any]) -> None:
        """
        更新决策缓存
        
        Args:
            trading_decision: 交易决策
            market_data: 市场数据
        """
        try:
            with self.cache_lock:
                self.decision_cache = {
                    'timestamp': time.time(),
                    'decision': trading_decision,
                    'market_data': market_data
                }
            logger.info("决策缓存更新完成")
        except Exception as e:
            logger.error(f"更新决策缓存失败: {e}")
    
    def update_position_cache(self, positions: List[Dict], account_info: Dict[str, Any]) -> None:
        """
        更新持仓缓存
        
        Args:
            positions: 持仓列表
            account_info: 账户信息
        """
        try:
            with self.cache_lock:
                self.position_cache = {
                    'timestamp': time.time(),
                    'positions': positions,
                    'account_info': account_info
                }
                
                # 更新持仓历史数据接口缓存
                position_record = {
                    'timestamp': datetime.now(),
                    'total_assets': account_info.get('total_assets', 0),
                    'total_pnl': account_info.get('total_pnl', 0),
                    'daily_pnl': account_info.get('daily_pnl', 0),
                    'positions_count': len(positions),
                    'positions': positions.copy()
                }
                self.position_history_cache.append(position_record)
                
                # 限制缓存大小
                max_records = self.data_api_config.get('position_history', {}).get('max_records', 1000)
                if len(self.position_history_cache) > max_records:
                    self.position_history_cache = self.position_history_cache[-max_records:]
            
            logger.info("持仓缓存更新完成")
        except Exception as e:
            logger.error(f"更新持仓缓存失败: {e}")
    
    def update_trade_records_cache(self, trade_records: List[Dict]) -> None:
        """
        更新交易记录缓存
        
        Args:
            trade_records: 交易记录列表
        """
        try:
            with self.cache_lock:
                self.trade_records_cache = trade_records.copy()
                
                # 限制缓存大小
                max_records = self.data_api_config.get('trade_records', {}).get('max_records', 1000)
                if len(self.trade_records_cache) > max_records:
                    self.trade_records_cache = self.trade_records_cache[-max_records:]
            
            logger.info("交易记录缓存更新完成")
        except Exception as e:
            logger.error(f"更新交易记录缓存失败: {e}")
    
    def get_price_cache(self) -> Dict[str, Any]:
        """
        获取价格缓存
        
        Returns:
            价格缓存字典
        """
        with self.cache_lock:
            return self.price_cache.copy()
    
    def get_decision_cache(self) -> Dict[str, Any]:
        """
        获取决策缓存
        
        Returns:
            决策缓存字典
        """
        with self.cache_lock:
            return self.decision_cache.copy()
    
    def get_position_cache(self) -> Dict[str, Any]:
        """
        获取持仓缓存
        
        Returns:
            持仓缓存字典
        """
        with self.cache_lock:
            return self.position_cache.copy()
    
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
        with self.cache_lock:
            return self.trade_records_cache.copy()
    
    def get_cached_price(self, symbol: str) -> float:
        """
        获取缓存的ETF价格
        
        Args:
            symbol: ETF代码
            
        Returns:
            缓存的价格，如果不存在返回0
        """
        with self.cache_lock:
            if symbol in self.price_cache:
                return self.price_cache[symbol].get('current_price', 0)
            return 0.0
    
    def is_decision_cache_valid(self, cache_duration: int = 300) -> bool:
        """
        检查决策缓存是否有效
        
        Args:
            cache_duration: 缓存有效期（秒）
            
        Returns:
            缓存是否有效
        """
        with self.cache_lock:
            last_decision_time = self.decision_cache.get('timestamp', 0)
            return time.time() - last_decision_time < cache_duration
    
    def clear_cache(self, cache_type: str = 'all') -> None:
        """
        清理缓存
        
        Args:
            cache_type: 缓存类型 ('price', 'decision', 'position', 'realtime_prices', 'position_history', 'trade_records', 'all')
        """
        try:
            with self.cache_lock:
                if cache_type in ['price', 'all']:
                    self.price_cache.clear()
                    logger.info("价格缓存已清理")
                
                if cache_type in ['decision', 'all']:
                    self.decision_cache.clear()
                    logger.info("决策缓存已清理")
                
                if cache_type in ['position', 'all']:
                    self.position_cache.clear()
                    logger.info("持仓缓存已清理")
                
                if cache_type in ['realtime_prices', 'all']:
                    self.realtime_prices_cache.clear()
                    logger.info("实时价格数据接口缓存已清理")
                
                if cache_type in ['position_history', 'all']:
                    self.position_history_cache.clear()
                    logger.info("持仓历史数据接口缓存已清理")
                
                if cache_type in ['trade_records', 'all']:
                    self.trade_records_cache.clear()
                    logger.info("交易记录数据接口缓存已清理")
                    
        except Exception as e:
            logger.error(f"清理缓存失败: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            缓存统计信息字典
        """
        with self.cache_lock:
            return {
                'price_cache_size': len(self.price_cache),
                'decision_cache_valid': bool(self.decision_cache),
                'position_cache_valid': bool(self.position_cache),
                'realtime_prices_cache_size': len(self.realtime_prices_cache),
                'position_history_cache_size': len(self.position_history_cache),
                'trade_records_cache_size': len(self.trade_records_cache),
                'last_decision_time': self.decision_cache.get('timestamp', 0),
                'last_position_update': self.position_cache.get('timestamp', 0)
            }