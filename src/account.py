"""
账户数据管理模块
只负责从data/account_data.json获取信息
"""

import json
from typing import Dict, List, Optional, Any
from loguru import logger
from .utils import load_account_data


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
            # 返回空的默认账户数据
            return {
                "account_info": {},
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
==========
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
    
    def update_position_price(self, symbol: str, current_price: float) -> bool:
        """
        更新ETF持仓价格（已移除功能）
        根据用户要求，此功能已移除，用户需手动维护data/account_data.json文件
        
        Args:
            symbol: ETF代码
            current_price: 当前价格
            
        Returns:
            总是返回False，表示未执行更新操作
        """
        logger.warning(f"更新持仓价格功能已移除，请手动更新data/account_data.json文件中的{symbol}持仓价格")
        return False
    
    def update_call_count(self) -> None:
        """
        更新调用次数（已移除功能）
        根据用户要求，此功能已移除，用户需手动维护data/account_data.json文件
        """
        logger.warning("更新调用次数功能已移除，请手动更新data/account_data.json文件中的调用次数")