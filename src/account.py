"""
账户数据管理模块
管理账户信息、持仓数据和交易记录
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from loguru import logger
from .utils import load_account_data, save_account_data, calculate_return_pct


class AccountManager:
    """账户管理器"""
    
    def __init__(self, account_data_path: str = "data/account_data.json"):
        """
        初始化账户管理器
        
        Args:
            account_data_path: 账户数据文件路径
        """
        self.account_data_path = account_data_path
        self.account_data = self._load_account_data()
        
        logger.info("账户管理器初始化完成")
    
    def _load_account_data(self) -> Dict[str, Any]:
        """
        加载账户数据
        
        Returns:
            账户数据字典
        """
        try:
            return load_account_data(self.account_data_path)
        except Exception as e:
            logger.error(f"加载账户数据失败: {e}")
            # 返回默认账户数据
            return self._get_default_account_data()
    
    def _get_default_account_data(self) -> Dict[str, Any]:
        """
        获取默认账户数据
        
        Returns:
            默认账户数据字典
        """
        return {
            "account_info": {
                "total_assets": 10000.0,
                "total_pnl": 0.0,
                "daily_pnl": 0.0,
                "available_cash": 10000.0,
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "call_count": 0
            },
            "positions": []
        }
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        获取账户信息
        
        Returns:
            账户信息字典
        """
        return self.account_data.get('account_info', {})
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """
        获取持仓列表
        
        Returns:
            持仓列表
        """
        return self.account_data.get('positions', [])
    
    def add_position(self, position: Dict[str, Any]) -> bool:
        """
        添加ETF持仓
        
        Args:
            position: 持仓信息，包含: symbol(代码), name(名称), quantity(持仓),
                     position_ratio(仓位), avg_price(买入均价), daily_pnl(当日盈亏)
            
        Returns:
            是否添加成功
        """
        try:
            # 验证必要字段
            required_fields = ['symbol', 'name', 'quantity', 'position_ratio', 'avg_price']
            for field in required_fields:
                if field not in position:
                    logger.error(f"持仓信息缺少必要字段: {field}")
                    return False
            
            # 设置默认值
            position.setdefault('daily_pnl', 0.0)
            position.setdefault('total_pnl', 0.0)
            
            # 计算持仓市值
            position['market_value'] = position['quantity'] * position['avg_price']
            
            # 检查是否已存在相同持仓
            existing_positions = self.get_positions()
            for existing_pos in existing_positions:
                if existing_pos['symbol'] == position['symbol']:
                    logger.warning(f"持仓 {position['symbol']} 已存在")
                    return False
            
            # 添加持仓
            self.account_data['positions'].append(position)
            self._update_account_summary()
            self._save_account_data()
            
            logger.info(f"ETF持仓添加成功: {position['symbol']}")
            return True
            
        except Exception as e:
            logger.error(f"添加ETF持仓失败: {e}")
            return False
    
    def update_position_price(self, symbol: str, current_price: float) -> bool:
        """
        更新ETF持仓价格
        
        Args:
            symbol: ETF代码
            current_price: 当前价格
            
        Returns:
            是否更新成功
        """
        try:
            positions = self.get_positions()
            for position in positions:
                if position['symbol'] == symbol:
                    old_price = position.get('current_price', position.get('avg_price', 0))
                    position['current_price'] = current_price
                    
                    # 重新计算市值和总盈亏
                    position['market_value'] = current_price * position['quantity']
                    position['total_pnl'] = (current_price - position['avg_price']) * position['quantity']
                    
                    logger.info(f"ETF持仓 {symbol} 价格更新: {old_price} -> {current_price}")
                    break
            else:
                logger.warning(f"未找到ETF持仓: {symbol}")
                return False
            
            self._update_account_summary()
            self._save_account_data()
            return True
            
        except Exception as e:
            logger.error(f"更新ETF持仓价格失败: {e}")
            return False
    
    def remove_position(self, symbol: str) -> bool:
        """
        移除持仓
        
        Args:
            symbol: ETF代码
            
        Returns:
            是否移除成功
        """
        try:
            positions = self.get_positions()
            original_count = len(positions)
            
            # 过滤掉指定持仓
            self.account_data['positions'] = [
                pos for pos in positions if pos['symbol'] != symbol
            ]
            
            if len(self.account_data['positions']) < original_count:
                self._update_account_summary()
                self._save_account_data()
                logger.info(f"持仓移除成功: {symbol}")
                return True
            else:
                logger.warning(f"未找到持仓: {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"移除持仓失败: {e}")
            return False
    
    def update_account_info(self, updates: Dict[str, Any]) -> bool:
        """
        更新账户信息
        
        Args:
            updates: 更新的字段
            
        Returns:
            是否更新成功
        """
        try:
            account_info = self.account_data.get('account_info', {})
            account_info.update(updates)
            self.account_data['account_info'] = account_info
            
            self._save_account_data()
            logger.info("账户信息更新成功")
            return True
            
        except Exception as e:
            logger.error(f"更新账户信息失败: {e}")
            return False
    
    def _update_account_summary(self) -> None:
        """更新ETF账户摘要信息"""
        try:
            positions = self.get_positions()
            account_info = self.account_data.get('account_info', {})
            
            # 计算持仓总市值
            positions_value = sum(
                pos.get('market_value', pos.get('quantity', 0) * pos.get('avg_price', 0))
                for pos in positions
            )
            
            # 计算总盈亏和当日盈亏
            total_pnl = sum(pos.get('total_pnl', 0.0) for pos in positions)
            daily_pnl = sum(pos.get('daily_pnl', 0.0) for pos in positions)
            
            # 更新账户信息
            available_cash = account_info.get('available_cash', 0.0)
            total_assets = available_cash + positions_value
            
            account_info['total_assets'] = total_assets
            account_info['total_pnl'] = total_pnl
            account_info['daily_pnl'] = daily_pnl
            
            logger.info(f"ETF账户摘要更新: 总资产={total_assets:.2f}, 总盈亏={total_pnl:.2f}, 当日盈亏={daily_pnl:.2f}")
            
        except Exception as e:
            logger.error(f"更新ETF账户摘要失败: {e}")
    
    def _save_account_data(self) -> None:
        """保存账户数据"""
        try:
            save_account_data(self.account_data, self.account_data_path)
        except Exception as e:
            logger.error(f"保存账户数据失败: {e}")
    
    def get_position_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        根据代码获取持仓
        
        Args:
            symbol: ETF代码
            
        Returns:
            持仓信息字典
        """
        positions = self.get_positions()
        for position in positions:
            if position['symbol'] == symbol:
                return position
        return None
    
    def get_total_pnl(self) -> float:
        """
        获取总盈亏
        
        Returns:
            总盈亏
        """
        positions = self.get_positions()
        return sum(pos.get('total_pnl', 0.0) for pos in positions)
    
    def get_daily_pnl(self) -> float:
        """
        获取当日盈亏
        
        Returns:
            当日盈亏
        """
        positions = self.get_positions()
        return sum(pos.get('daily_pnl', 0.0) for pos in positions)
    
    def get_positions_by_category(self, category: str) -> List[Dict[str, Any]]:
        """
        根据类别获取持仓
        
        Args:
            category: 持仓类别
            
        Returns:
            指定类别的持仓列表
        """
        positions = self.get_positions()
        return [pos for pos in positions if pos.get('category') == category]
    
    def calculate_position_metrics(self) -> Dict[str, Any]:
        """
        计算ETF持仓指标
        
        Returns:
            持仓指标字典
        """
        try:
            positions = self.get_positions()
            
            if not positions:
                return {
                    'total_positions': 0,
                    'total_value': 0.0,
                    'total_pnl': 0.0,
                    'daily_pnl': 0.0,
                    'avg_position_ratio': 0.0,
                    'winning_positions': 0,
                    'losing_positions': 0
                }
            
            total_value = sum(
                pos.get('market_value', pos.get('quantity', 0) * pos.get('avg_price', 0))
                for pos in positions
            )
            total_pnl = sum(pos.get('total_pnl', 0.0) for pos in positions)
            daily_pnl = sum(pos.get('daily_pnl', 0.0) for pos in positions)
            
            # 计算平均仓位
            avg_position_ratio = sum(pos.get('position_ratio', 0.0) for pos in positions) / len(positions) if positions else 0.0
            
            # 计算盈利和亏损持仓数
            winning_count = sum(1 for pos in positions if pos.get('total_pnl', 0) > 0)
            losing_count = sum(1 for pos in positions if pos.get('total_pnl', 0) < 0)
            
            return {
                'total_positions': len(positions),
                'total_value': total_value,
                'total_pnl': total_pnl,
                'daily_pnl': daily_pnl,
                'avg_position_ratio': avg_position_ratio,
                'winning_positions': winning_count,
                'losing_positions': losing_count
            }
            
        except Exception as e:
            logger.error(f"计算ETF持仓指标失败: {e}")
            return {}
    
    def update_call_count(self) -> None:
        """更新调用次数"""
        try:
            account_info = self.account_data.get('account_info', {})
            current_count = account_info.get('call_count', 0)
            account_info['call_count'] = current_count + 1
            self._save_account_data()
            
        except Exception as e:
            logger.error(f"更新调用次数失败: {e}")
    
    def export_account_summary(self) -> str:
        """
        导出ETF账户摘要
        
        Returns:
            ETF账户摘要字符串
        """
        try:
            account_info = self.get_account_info()
            positions = self.get_positions()
            metrics = self.calculate_position_metrics()
            
            summary = f"""
ETF账户摘要
=======
总资产: {account_info.get('total_assets', 0):.2f}
总盈亏: {account_info.get('total_pnl', 0):.2f}
当日盈亏: {account_info.get('daily_pnl', 0):.2f}
可用现金: {account_info.get('available_cash', 0):.2f}
调用次数: {account_info.get('call_count', 0)}

ETF持仓概览
===========
总持仓数: {metrics.get('total_positions', 0)}
持仓总价值: {metrics.get('total_value', 0):.2f}
总盈亏: {metrics.get('total_pnl', 0):.2f}
当日盈亏: {metrics.get('daily_pnl', 0):.2f}
平均仓位: {metrics.get('avg_position_ratio', 0):.2f}%
盈利持仓: {metrics.get('winning_positions', 0)}
亏损持仓: {metrics.get('losing_positions', 0)}
"""
            
            return summary
            
        except Exception as e:
            logger.error(f"导出ETF账户摘要失败: {e}")
            return "ETF账户摘要导出失败"