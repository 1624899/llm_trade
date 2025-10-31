"""
交易记录模块
负责保存和管理交易执行记录
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from loguru import logger


class TradeRecorder:
    """交易记录器类"""
    
    def __init__(self, records_dir: str = "data/trade_executions"):
        """
        初始化交易记录器
        
        Args:
            records_dir: 交易记录目录
        """
        self.records_dir = records_dir
        self._ensure_directory_exists()
    
    def _ensure_directory_exists(self) -> None:
        """确保记录目录存在"""
        try:
            os.makedirs(self.records_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"创建交易记录目录失败: {e}")
    
    def save_trade_execution_record(self, trade_record: Dict[str, Any]) -> None:
        """
        保存交易执行记录
        
        Args:
            trade_record: 交易记录字典
        """
        try:
            # 保存到交易执行历史文件
            history_file = os.path.join(self.records_dir, "execution_history.json")
            
            # 读取现有记录
            history = self._load_json_file(history_file, [])
            
            # 添加新记录
            history.append(trade_record)
            
            # 只保留最近100条记录
            history = history[-100:]
            
            # 保存历史记录
            self._save_json_file(history_file, history)
            
            # 保存到当日记录文件
            today = datetime.now().strftime('%Y%m%d')
            today_file = os.path.join(self.records_dir, f"execution_{today}.json")
            
            today_history = self._load_json_file(today_file, [])
            today_history.append(trade_record)
            
            self._save_json_file(today_file, today_history)
            
            print(f"💾 交易执行记录已保存")
            logger.info(f"交易执行记录已保存: {trade_record.get('symbol', '')} {trade_record.get('action', '')}")
            
        except Exception as e:
            print(f"❌ 保存交易执行记录失败: {e}")
            logger.error(f"保存交易执行记录失败: {e}")
    
    def _load_json_file(self, file_path: str, default_value: Any) -> Any:
        """
        加载JSON文件
        
        Args:
            file_path: 文件路径
            default_value: 默认值
            
        Returns:
            加载的数据，失败时返回默认值
        """
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # 确保返回正确的数据类型
                if isinstance(default_value, list) and not isinstance(data, list):
                    return default_value
                return data
            return default_value
        except Exception as e:
            logger.warning(f"读取JSON文件失败 {file_path}: {e}")
            return default_value
    
    def _save_json_file(self, file_path: str, data: Any) -> None:
        """
        保存JSON文件
        
        Args:
            file_path: 文件路径
            data: 要保存的数据
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存JSON文件失败 {file_path}: {e}")
    
    def get_trade_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取交易历史记录
        
        Args:
            limit: 返回记录数量限制
            
        Returns:
            交易历史记录列表
        """
        try:
            history_file = os.path.join(self.records_dir, "execution_history.json")
            history = self._load_json_file(history_file, [])
            
            # 按时间倒序排列，返回最新的记录
            history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return history[:limit]
            
        except Exception as e:
            logger.error(f"获取交易历史记录失败: {e}")
            return []
    
    def get_today_trades(self) -> List[Dict[str, Any]]:
        """
        获取今日交易记录
        
        Returns:
            今日交易记录列表
        """
        try:
            today = datetime.now().strftime('%Y%m%d')
            today_file = os.path.join(self.records_dir, f"execution_{today}.json")
            return self._load_json_file(today_file, [])
            
        except Exception as e:
            logger.error(f"获取今日交易记录失败: {e}")
            return []
    
    def get_trades_by_symbol(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        获取指定ETF的交易记录
        
        Args:
            symbol: ETF代码
            days: 查询天数
            
        Returns:
            指定ETF的交易记录列表
        """
        try:
            trades = []
            current_date = datetime.now()
            
            # 遍历指定天数的记录文件
            for i in range(days):
                date = current_date - timedelta(days=i)
                date_str = date.strftime('%Y%m%d')
                file_path = os.path.join(self.records_dir, f"execution_{date_str}.json")
                
                if os.path.exists(file_path):
                    day_trades = self._load_json_file(file_path, [])
                    # 筛选指定ETF的记录
                    symbol_trades = [trade for trade in day_trades if trade.get('symbol') == symbol]
                    trades.extend(symbol_trades)
            
            # 按时间倒序排列
            trades.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return trades
            
        except Exception as e:
            logger.error(f"获取ETF {symbol} 交易记录失败: {e}")
            return []
    
    def get_trading_statistics(self, days: int = 30) -> Dict[str, Any]:
        """
        获取交易统计信息
        
        Args:
            days: 统计天数
            
        Returns:
            交易统计信息字典
        """
        try:
            stats = {
                'total_trades': 0,
                'successful_trades': 0,
                'failed_trades': 0,
                'buy_trades': 0,
                'sell_trades': 0,
                'hold_trades': 0,
                'total_amount': 0.0,
                'symbols_traded': set(),
                'daily_trades': {}
            }
            
            current_date = datetime.now()
            
            # 遍历指定天数的记录文件
            for i in range(days):
                date = current_date - timedelta(days=i)
                date_str = date.strftime('%Y%m%d')
                file_path = os.path.join(self.records_dir, f"execution_{date_str}.json")
                
                if os.path.exists(file_path):
                    day_trades = self._load_json_file(file_path, [])
                    daily_count = len(day_trades)
                    
                    stats['total_trades'] += daily_count
                    stats['daily_trades'][date_str] = daily_count
                    
                    for trade in day_trades:
                        # 统计成功/失败
                        if trade.get('success', False):
                            stats['successful_trades'] += 1
                        else:
                            stats['failed_trades'] += 1
                        
                        # 统计交易类型
                        action = trade.get('action', '').upper()
                        if action == 'BUY':
                            stats['buy_trades'] += 1
                        elif action == 'SELL':
                            stats['sell_trades'] += 1
                        elif action == 'HOLD':
                            stats['hold_trades'] += 1
                        
                        # 统计交易金额
                        amount = trade.get('amount', 0)
                        if amount > 0:
                            stats['total_amount'] += amount
                        
                        # 统计交易过的ETF
                        symbol = trade.get('symbol', '')
                        if symbol:
                            stats['symbols_traded'].add(symbol)
            
            # 转换set为list
            stats['symbols_traded'] = list(stats['symbols_traded'])
            
            # 计算成功率
            if stats['total_trades'] > 0:
                stats['success_rate'] = stats['successful_trades'] / stats['total_trades']
            else:
                stats['success_rate'] = 0.0
            
            # 计算平均每日交易次数
            stats['avg_daily_trades'] = stats['total_trades'] / days
            
            return stats
            
        except Exception as e:
            logger.error(f"获取交易统计信息失败: {e}")
            return {}
    
    def cleanup_old_records(self, days_to_keep: int = 90) -> None:
        """
        清理旧的交易记录
        
        Args:
            days_to_keep: 保留天数
        """
        try:
            if not os.path.exists(self.records_dir):
                return
            
            current_date = datetime.now()
            cutoff_date = current_date - timedelta(days=days_to_keep)
            
            for filename in os.listdir(self.records_dir):
                if filename.startswith('execution_') and filename.endswith('.json'):
                    # 提取日期
                    date_str = filename[10:18]  # 提取YYYYMMDD部分
                    try:
                        file_date = datetime.strptime(date_str, '%Y%m%d')
                        if file_date < cutoff_date:
                            file_path = os.path.join(self.records_dir, filename)
                            os.remove(file_path)
                            logger.info(f"已删除旧交易记录文件: {filename}")
                    except ValueError:
                        # 文件名格式不正确，跳过
                        continue
                        
        except Exception as e:
            logger.error(f"清理旧交易记录失败: {e}")