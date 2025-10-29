"""
账户数据管理模块
模拟交易账户管理
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from loguru import logger
from .utils import save_account_data
import yaml


class AccountManager:
    """账户管理器"""
    
    def __init__(self, account_data_path: str = "data/account_data.json"):
        """
        初始化账户管理器
        
        Args:
            account_data_path: 账户数据文件路径
        """
        self.account_data_path = account_data_path
        # 读取配置文件
        self.config = self._load_config()
        # 初始化模拟交易账户数据
        self.account_data = self._initialize_account_data()
        
        logger.info("模拟交易账户管理器初始化完成")
    
    def _initialize_account_data(self) -> Dict[str, Any]:
        """
        初始化账户数据
        
        Returns:
            账户数据字典
        """
        # 初始化账户信息，设置10000元初始资金
        account_info = {
            "total_assets": 10000.0,  # 总资产
            "total_pnl": 0.0,  # 总盈亏
            "daily_pnl": 0.0,  # 当日盈亏
            "available_cash": 10000.0,  # 可用现金
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 开始时间
            "initial_cash": 10000.0  # 初始资金
        }
        
        # 初始化空持仓列表
        positions = []
        
        # 初始化空交易历史
        trade_history = []
        
        return {
            "account_info": account_info,
            "positions": positions,
            "trade_history": trade_history
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
        """
        try:
            with open("config/config.yaml", "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            # 返回默认配置
            return {
                "trading": {
                    "commission": {
                        "enabled": True,
                        "fee": 5.0
                    },
                    "t1_rule": {
                        "enabled": True,
                        "gold_etf_t0": ["518880"]
                    }
                }
            }
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        获取账户信息
        
        Returns:
            账户信息字典
        """
        # 更新账户总资产
        self._update_total_assets()
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
    
    def buy_etf(self, symbol: str, name: str, quantity: int, price: float) -> bool:
        """
        买入ETF
        
        Args:
            symbol: ETF代码
            name: ETF名称
            quantity: 买入数量
            price: 买入价格
            
        Returns:
            是否买入成功
        """
        try:
            # 获取佣金配置
            commission_config = self.config.get("trading", {}).get("commission", {})
            commission_enabled = commission_config.get("enabled", True)
            commission_fee = commission_config.get("fee", 5.0)
            
            # 计算总金额和佣金
            total_amount = quantity * price
            total_cost = total_amount
            if commission_enabled:
                total_cost += commission_fee
            
            # 检查是否有足够现金
            available_cash = self.get_cash_balance()
            if available_cash < total_cost:
                logger.warning(f"现金不足，无法买入 {symbol}。需要 {total_cost:.2f}，可用 {available_cash:.2f}")
                return False
            
            # 更新现金余额
            self.account_data['account_info']['available_cash'] -= total_cost
            
            # 获取当前时间
            current_time = datetime.now()
            purchase_date = current_time.strftime("%Y-%m-%d")
            
            # 检查是否已有该ETF持仓
            position = self.get_position_by_symbol(symbol)
            if position:
                # 更新现有持仓
                old_quantity = position['quantity']
                old_avg_price = position['avg_price']
                new_quantity = old_quantity + quantity
                new_avg_price = (old_quantity * old_avg_price + quantity * price) / new_quantity
                
                position['quantity'] = new_quantity
                position['avg_price'] = new_avg_price
                # T+1规则：普通ETF当日买入的份额不可卖出，黄金ETF可以随时卖出
                t1_config = self.config.get("trading", {}).get("t1_rule", {})
                gold_etf_t0 = t1_config.get("gold_etf_t0", ["518880"])
                if symbol in gold_etf_t0:
                    # 黄金ETF，T+0交易
                    position['available_quantity'] = position.get('available_quantity', 0) + quantity
                else:
                    # 普通ETF，T+1交易
                    position['available_quantity'] = position.get('available_quantity', 0)
                position['purchase_date'] = purchase_date
            else:
                # 创建新持仓
                new_position = {
                    'symbol': symbol,
                    'name': name,
                    'quantity': quantity,
                    'position_ratio': 0.0,  # 将在更新总资产时计算
                    'avg_price': price,
                    'current_price': price,
                    'daily_pnl': 0.0,
                    'total_pnl': 0.0,
                    'market_value': total_amount,
                    'purchase_date': purchase_date
                }
                
                # T+1规则：普通ETF当日买入的份额不可卖出，黄金ETF可以随时卖出
                t1_config = self.config.get("trading", {}).get("t1_rule", {})
                gold_etf_t0 = t1_config.get("gold_etf_t0", ["518880"])
                if symbol in gold_etf_t0:
                    # 黄金ETF，T+0交易
                    new_position['available_quantity'] = quantity
                else:
                    # 普通ETF，T+1交易
                    new_position['available_quantity'] = 0
                
                self.account_data['positions'].append(new_position)
            
            # 记录交易历史
            self._add_trade_record('buy', symbol, name, quantity, price, total_amount, commission_fee if commission_enabled else 0.0)
            
            # 更新账户总资产
            self._update_total_assets()
            
            # 保存账户数据
            self._save_account_data()
            
            logger.info(f"成功买入 {symbol} {name} {quantity}股，价格 {price:.2f}，总金额 {total_amount:.2f}，佣金 {commission_fee if commission_enabled else 0.0:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"买入ETF失败: {e}")
            return False
    
    def sell_etf(self, symbol: str, quantity: int, price: float) -> bool:
        """
        卖出ETF
        
        Args:
            symbol: ETF代码
            quantity: 卖出数量
            price: 卖出价格
            
        Returns:
            是否卖出成功
        """
        try:
            # 检查是否有足够持仓
            position = self.get_position_by_symbol(symbol)
            if not position:
                logger.warning(f"没有持仓 {symbol}，无法卖出")
                return False
            
            available_quantity = position.get('available_quantity', 0)
            if available_quantity < quantity:
                logger.warning(f"持仓不足，无法卖出 {symbol}。需要 {quantity}，可用 {available_quantity}")
                return False
            
            # 获取佣金配置
            commission_config = self.config.get("trading", {}).get("commission", {})
            commission_enabled = commission_config.get("enabled", True)
            commission_fee = commission_config.get("fee", 5.0)
            
            # 计算总金额和佣金
            total_amount = quantity * price
            net_amount = total_amount
            if commission_enabled:
                net_amount -= commission_fee
            
            # 更新现金余额
            self.account_data['account_info']['available_cash'] += net_amount
            
            # 更新持仓
            position['quantity'] -= quantity
            position['available_quantity'] -= quantity
            
            # 计算盈亏（包含佣金成本）
            profit_loss = (price - position['avg_price']) * quantity
            if commission_enabled:
                profit_loss -= commission_fee
            position['total_pnl'] += profit_loss
            
            # 如果持仓为0，移除该持仓
            if position['quantity'] <= 0:
                self.account_data['positions'].remove(position)
            
            # 记录交易历史
            self._add_trade_record('sell', symbol, position.get('name', ''), quantity, price, total_amount, commission_fee if commission_enabled else 0.0)
            
            # 更新账户总资产和总盈亏
            self._update_total_assets()
            self._update_total_pnl()
            
            # 保存账户数据
            self._save_account_data()
            
            logger.info(f"成功卖出 {symbol} {quantity}股，价格 {price:.2f}，总金额 {total_amount:.2f}，佣金 {commission_fee if commission_enabled else 0.0:.2f}，盈亏 {profit_loss:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"卖出ETF失败: {e}")
            return False
    
    def hold_position(self, symbol: str, current_price: float) -> bool:
        """
        更新持仓当前价格
        
        Args:
            symbol: ETF代码
            current_price: 当前价格
            
        Returns:
            是否更新成功
        """
        try:
            position = self.get_position_by_symbol(symbol)
            if not position:
                logger.warning(f"没有持仓 {symbol}，无法更新价格")
                return False
            
            # 记录昨日收盘价（用于计算当日盈亏）
            yesterday_price = position.get('current_price', position['avg_price'])
            
            # 更新当前价格
            position['current_price'] = current_price
            
            # 计算市值
            position['market_value'] = position['quantity'] * current_price
            
            # 计算当日盈亏
            position['daily_pnl'] = (current_price - yesterday_price) * position['quantity']
            
            # 计算总盈亏
            position['total_pnl'] = (current_price - position['avg_price']) * position['quantity']
            
            # 更新账户总资产和总盈亏
            self._update_total_assets()
            self._update_total_pnl()
            
            # 保存账户数据
            self._save_account_data()
            
            logger.info(f"更新 {symbol} 价格为 {current_price:.2f}，市值 {position['market_value']:.2f}，当日盈亏 {position['daily_pnl']:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"更新持仓价格失败: {e}")
            return False
    
    def get_cash_balance(self) -> float:
        """
        获取现金余额
        
        Returns:
            现金余额
        """
        return self.account_data['account_info'].get('available_cash', 0.0)
    
    def get_total_value(self) -> float:
        """
        获取账户总价值
        
        Returns:
            账户总价值
        """
        self._update_total_assets()
        return self.account_data['account_info'].get('total_assets', 0.0)
    
    def get_trade_history(self) -> List[Dict[str, Any]]:
        """
        获取交易历史
        
        Returns:
            交易历史列表
        """
        return self.account_data.get('trade_history', [])
    
    def get_total_pnl(self) -> float:
        """
        获取总盈亏
        
        Returns:
            总盈亏
        """
        self._update_total_pnl()
        return self.account_data['account_info'].get('total_pnl', 0.0)
    
    def get_daily_pnl(self) -> float:
        """
        获取当日盈亏
        
        Returns:
            当日盈亏
        """
        self._update_daily_pnl()
        return self.account_data['account_info'].get('daily_pnl', 0.0)
    
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
模拟交易账户摘要
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
            logger.error(f"导出模拟交易账户摘要失败: {e}")
            return "模拟交易账户摘要导出失败"
    
    def _add_trade_record(self, trade_type: str, symbol: str, name: str,
                          quantity: int, price: float, amount: float, commission: float = 0.0) -> None:
        """
        添加交易记录
        
        Args:
            trade_type: 交易类型 (buy/sell)
            symbol: ETF代码
            name: ETF名称
            quantity: 交易数量
            price: 交易价格
            amount: 交易金额
            commission: 佣金费用
        """
        trade_record = {
            'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'type': trade_type,
            'symbol': symbol,
            'name': name,
            'quantity': quantity,
            'price': price,
            'amount': amount,
            'commission_fee': commission
        }
        
        self.account_data['trade_history'].append(trade_record)
    
    def _update_total_assets(self) -> None:
        """
        更新账户总资产
        """
        try:
            # 计算持仓总市值
            positions_value = sum(pos.get('market_value', 0) for pos in self.get_positions())
            
            # 计算总资产 = 现金 + 持仓市值
            total_assets = self.get_cash_balance() + positions_value
            
            # 更新账户信息
            self.account_data['account_info']['total_assets'] = total_assets
            
            # 更新持仓比例
            if total_assets > 0:
                for position in self.get_positions():
                    market_value = position.get('market_value', 0)
                    position['position_ratio'] = (market_value / total_assets) * 100
            
        except Exception as e:
            logger.error(f"更新账户总资产失败: {e}")
    
    def _update_total_pnl(self) -> None:
        """
        更新账户总盈亏
        """
        try:
            # 计算持仓总盈亏
            total_pnl = sum(pos.get('total_pnl', 0.0) for pos in self.get_positions())
            
            # 更新账户信息
            self.account_data['account_info']['total_pnl'] = total_pnl
            
        except Exception as e:
            logger.error(f"更新账户总盈亏失败: {e}")
    
    def _update_daily_pnl(self) -> None:
        """
        更新账户当日盈亏
        """
        try:
            # 计算持仓当日盈亏
            daily_pnl = sum(pos.get('daily_pnl', 0.0) for pos in self.get_positions())
            
            # 更新账户信息
            self.account_data['account_info']['daily_pnl'] = daily_pnl
            
        except Exception as e:
            logger.error(f"更新账户当日盈亏失败: {e}")
    
    def _save_account_data(self) -> None:
        """
        保存账户数据到文件
        """
        try:
            save_account_data(self.account_data, self.account_data_path)
        except Exception as e:
            logger.error(f"保存账户数据失败: {e}")
    
    def load_account_data_from_file(self) -> bool:
        """
        从文件加载账户数据（可选功能，不用于初始化）
        
        Returns:
            是否加载成功
        """
        try:
            if os.path.exists(self.account_data_path):
                with open(self.account_data_path, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                
                # 验证数据结构
                if 'account_info' in loaded_data and 'positions' in loaded_data:
                    self.account_data = loaded_data
                    logger.info(f"从文件加载账户数据成功: {self.account_data_path}")
                    return True
                else:
                    logger.warning(f"文件中的账户数据结构不正确: {self.account_data_path}")
                    return False
            else:
                logger.info(f"账户数据文件不存在，使用默认数据: {self.account_data_path}")
                return False
                
        except Exception as e:
            logger.error(f"从文件加载账户数据失败: {e}")
            return False
    