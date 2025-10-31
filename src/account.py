"""
账户数据管理模块
模拟交易账户管理
"""

import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from loguru import logger
from .utils import save_account_data
from .indicators import TechnicalIndicators
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
        # 加载或初始化账户数据
        self.account_data = self._load_or_initialize_account_data()
        # 初始化技术指标计算器
        self.indicators = TechnicalIndicators(self.config)
        
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
    
    def _load_or_initialize_account_data(self) -> Dict[str, Any]:
        """
        加载或初始化账户数据
        优先从文件加载历史数据，如果文件不存在或加载失败则初始化新数据
        
        Returns:
            账户数据字典
        """
        try:
            # 尝试从文件加载历史数据
            if os.path.exists(self.account_data_path):
                with open(self.account_data_path, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                
                # 验证数据结构
                if 'account_info' in loaded_data and 'positions' in loaded_data:
                    logger.info(f"成功加载历史账户数据，文件: {self.account_data_path}")
                    logger.info(f"加载的持仓数量: {len(loaded_data.get('positions', []))}")
                    logger.info(f"加载的交易记录数量: {len(loaded_data.get('trade_history', []))}")
                    return loaded_data
                else:
                    logger.warning(f"历史账户数据文件结构不正确，使用默认数据: {self.account_data_path}")
            else:
                logger.info(f"历史账户数据文件不存在，将创建新的账户数据: {self.account_data_path}")
                
        except Exception as e:
            logger.warning(f"加载历史账户数据失败，将创建新账户数据: {e}")
        
        # 如果加载失败，创建新的账户数据
        return self._initialize_account_data()

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
                
                # 更新盈利/止损目标（基于新的均价）
                if 'profit_target_price' not in position:
                    position['profit_target_price'] = new_avg_price * 1.05  # 默认盈利目标5%
                    position['stop_loss_price'] = new_avg_price * 0.97     # 默认止损3%
                    position['profit_target_pct'] = 5.0
                    position['stop_loss_pct'] = 3.0
                
                # T+1规则：普通ETF当日买入的份额不可卖出，黄金ETF可以随时卖出
                t1_config = self.config.get("trading", {}).get("t1_rule", {})
                gold_etf_t0 = t1_config.get("gold_etf_t0", ["518880"])
                if symbol in gold_etf_t0:
                    # 黄金ETF，T+0交易
                    position['available_quantity'] = position.get('available_quantity', 0) + quantity
                else:
                    # 普通ETF，T+1交易，新买入部分当日不可卖出
                    position['available_quantity'] = position.get('available_quantity', 0)
                position['purchase_date'] = purchase_date
                # 更新市值
                position['market_value'] = new_quantity * position['current_price']
                # 更新总盈亏（包含佣金）
                position['total_pnl'] = (position['current_price'] - new_avg_price) * new_quantity
            else:
                # 创建新持仓
                new_position = {
                    'symbol': symbol,
                    'name': name,
                    'quantity': quantity,
                    'position_ratio': 0.0,  # 将在更新总资产时计算
                    'avg_price': price,
                    'current_price': price,
                    'previous_close_price': price,  # 初始化前一日收盘价为当前价格
                    'daily_pnl': 0.0,
                    'total_pnl': 0.0,
                    'market_value': total_amount,
                    'purchase_date': purchase_date,
                    # 添加盈利/止损目标
                    'profit_target_price': price * 1.05,  # 默认盈利目标5%
                    'stop_loss_price': price * 0.97,     # 默认止损3%
                    'profit_target_pct': 5.0,             # 盈利目标百分比
                    'stop_loss_pct': 3.0                  # 止损百分比
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
            previous_price = position.get('previous_close_price', position['current_price'])
            
            # 更新当前价格
            position['current_price'] = current_price
            position['previous_close_price'] = current_price  # 更新作为下次计算的基准
            
            # 计算市值
            position['market_value'] = position['quantity'] * current_price
            
            # 计算当日盈亏
            position['daily_pnl'] = (current_price - previous_price) * position['quantity']
            
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
            metrics = self.get_performance_metrics()  # 使用包含夏普比率的方法
            
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

夏普比率指标
=========
投资组合夏普比率: {metrics.get('portfolio_sharpe_ratio', 0):.4f}
平均ETF夏普比率: {metrics.get('avg_etf_sharpe_ratio', 0):.4f}
夏普比率评级: {metrics.get('sharpe_rating', '未知')}
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
    
    def fix_position_consistency(self) -> None:
        """
        修复持仓数据一致性，确保总持仓数量与可用数量正确
        """
        try:
            logger.info("开始修复持仓数据一致性...")
            
            for position in self.get_positions():
                symbol = position['symbol']
                
                # 重新计算实际持仓数量（从交易历史）
                actual_quantity = 0
                actual_available_quantity = 0
                t1_config = self.config.get("trading", {}).get("t1_rule", {})
                gold_etf_t0 = t1_config.get("gold_etf_t0", ["518880"])
                is_gold_etf = symbol in gold_etf_t0
                
                # 分析交易历史
                today = datetime.now().strftime("%Y-%m-%d")
                
                for trade in self.get_trade_history():
                    if trade['symbol'] == symbol:
                        if trade['type'] == 'buy':
                            actual_quantity += trade['quantity']
                            # T+1规则：黄金ETFT+0，普通ETF T+1
                            if is_gold_etf or trade['time'] < f"{today} 00:00:00":
                                actual_available_quantity += trade['quantity']
                        elif trade['type'] == 'sell':
                            actual_quantity -= trade['quantity']
                            actual_available_quantity -= trade['quantity']
                
                # 更新持仓数据
                old_quantity = position['quantity']
                old_available = position.get('available_quantity', 0)
                
                position['quantity'] = actual_quantity
                position['available_quantity'] = max(0, actual_available_quantity)
                
                logger.info(f"修复 {symbol}: 总持仓 {old_quantity}->{actual_quantity}, "
                          f"可用 {old_available}->{position['available_quantity']}")
            
            # 更新账户数据
            self._update_total_assets()
            self._update_total_pnl()
            self._update_daily_pnl()
            self._save_account_data()
            
            logger.info("持仓数据一致性修复完成")
            
        except Exception as e:
            logger.error(f"修复持仓数据一致性失败: {e}")
    
    def validate_account_calculations(self) -> Dict[str, Any]:
        """
        验证账户计算是否正确
        
        Returns:
            验证结果字典
        """
        try:
            account_info = self.get_account_info()
            positions = self.get_positions()
            trade_history = self.get_trade_history()
            
            # 验证总资产计算
            cash_balance = account_info.get('available_cash', 0)
            total_positions_value = sum(pos.get('market_value', 0) for pos in positions)
            calculated_total_assets = cash_balance + total_positions_value
            displayed_total_assets = account_info.get('total_assets', 0)
            
            # 验证总盈亏计算
            calculated_total_pnl = sum(pos.get('total_pnl', 0) for pos in positions)
            displayed_total_pnl = account_info.get('total_pnl', 0)
            
            # 验证当日盈亏计算
            calculated_daily_pnl = sum(pos.get('daily_pnl', 0) for pos in positions)
            displayed_daily_pnl = account_info.get('daily_pnl', 0)
            
            # 检查初始资金
            initial_cash = account_info.get('initial_cash', 0)
            
            validation_result = {
                'total_assets': {
                    'calculated': calculated_total_assets,
                    'displayed': displayed_total_assets,
                    'difference': abs(calculated_total_assets - displayed_total_assets),
                    'match': abs(calculated_total_assets - displayed_total_assets) < 0.01
                },
                'total_pnl': {
                    'calculated': calculated_total_pnl,
                    'displayed': displayed_total_pnl,
                    'difference': abs(calculated_total_pnl - displayed_total_pnl),
                    'match': abs(calculated_total_pnl - displayed_total_pnl) < 0.01
                },
                'daily_pnl': {
                    'calculated': calculated_daily_pnl,
                    'displayed': displayed_daily_pnl,
                    'difference': abs(calculated_daily_pnl - displayed_daily_pnl),
                    'match': abs(calculated_daily_pnl - displayed_daily_pnl) < 0.01
                },
                'initial_cash': initial_cash,
                'available_cash': cash_balance,
                'positions_count': len(positions),
                'trade_history_count': len(trade_history)
            }
            
            logger.info("账户计算验证完成")
            return validation_result
            
        except Exception as e:
            logger.error(f"验证账户计算失败: {e}")
            return {}
    
    def calculate_portfolio_sharpe_ratio(self, risk_free_rate: float = 0.0) -> float:
        """
        计算整个投资组合的夏普比率
        
        Args:
            risk_free_rate: 无风险利率，默认为0.0
            
        Returns:
            投资组合的夏普比率值
        """
        try:
            # 获取账户信息
            account_info = self.get_account_info()
            initial_cash = account_info.get('initial_cash', 10000.0)
            current_assets = account_info.get('total_assets', 0.0)
            
            # 计算投资组合的总收益率
            total_return = (current_assets - initial_cash) / initial_cash
            
            # 获取交易历史，计算收益率序列
            trade_history = self.get_trade_history()
            if not trade_history:
                logger.warning("无交易历史，无法计算夏普比率")
                return 0.0
            
            # 按时间排序交易历史
            sorted_trades = sorted(trade_history, key=lambda x: datetime.strptime(x['time'], "%Y-%m-%d %H:%M:%S"))
            
            # 计算每日收益率序列
            daily_returns = []
            portfolio_value = initial_cash
            
            # 按日期分组交易
            daily_trades = {}
            for trade in sorted_trades:
                trade_date = trade['time'][:10]  # 提取日期部分
                if trade_date not in daily_trades:
                    daily_trades[trade_date] = []
                daily_trades[trade_date].append(trade)
            
            # 计算每日投资组合价值和收益率
            prev_value = portfolio_value
            for date, trades in sorted(daily_trades.items()):
                # 计算当日交易后的投资组合价值
                daily_change = 0.0
                for trade in trades:
                    if trade['type'] == 'buy':
                        daily_change -= trade['amount'] + trade.get('commission_fee', 0.0)
                    elif trade['type'] == 'sell':
                        daily_change += trade['amount'] - trade.get('commission_fee', 0.0)
                
                # 更新投资组合价值
                portfolio_value += daily_change
                
                # 计算当日收益率
                if prev_value > 0:
                    daily_return = (portfolio_value - prev_value) / prev_value
                    daily_returns.append(daily_return)
                
                prev_value = portfolio_value
            
            # 如果没有足够的收益率数据，使用总收益率和估算的波动率
            if len(daily_returns) < 2:
                # 使用简单的波动率估算
                estimated_volatility = abs(total_return) * 2  # 简单估算
                if estimated_volatility == 0:
                    return 0.0
                return total_return / estimated_volatility
            
            # 使用TechnicalIndicators计算夏普比率
            returns_series = pd.Series(daily_returns)
            sharpe_ratio = self.indicators.calculate_sharpe_ratio(returns_series, risk_free_rate, 'daily')
            
            # 如果计算失败，返回0
            if pd.isna(sharpe_ratio):
                logger.warning("投资组合夏普比率计算失败，返回0")
                return 0.0
                
            logger.info(f"投资组合夏普比率计算完成: {sharpe_ratio:.4f}")
            return float(sharpe_ratio)
            
        except Exception as e:
            logger.error(f"计算投资组合夏普比率失败: {e}")
            return 0.0
    
    def calculate_etf_sharpe_ratio(self, etf_code: str, risk_free_rate: float = 0.0) -> float:
        """
        计算单个ETF的夏普比率
        
        Args:
            etf_code: ETF代码
            risk_free_rate: 无风险利率，默认为0.0
            
        Returns:
            单个ETF的夏普比率值
        """
        try:
            # 获取持仓信息
            position = self.get_position_by_symbol(etf_code)
            if not position:
                logger.warning(f"未找到ETF {etf_code} 的持仓，无法计算夏普比率")
                return 0.0
            
            # 获取该ETF的交易历史
            trade_history = self.get_trade_history()
            etf_trades = [trade for trade in trade_history if trade['symbol'] == etf_code]
            
            if not etf_trades:
                logger.warning(f"ETF {etf_code} 无交易历史，无法计算夏普比率")
                return 0.0
            
            # 按时间排序交易历史
            sorted_trades = sorted(etf_trades, key=lambda x: datetime.strptime(x['time'], "%Y-%m-%d %H:%M:%S"))
            
            # 计算持仓期间的收益率序列
            returns = []
            shares_held = 0
            total_cost = 0.0
            
            for trade in sorted_trades:
                if trade['type'] == 'buy':
                    # 买入：增加持仓和成本
                    buy_shares = trade['quantity']
                    buy_price = trade['price']
                    buy_cost = trade['amount'] + trade.get('commission_fee', 0.0)
                    
                    shares_held += buy_shares
                    total_cost += buy_cost
                    
                elif trade['type'] == 'sell' and shares_held > 0:
                    # 卖出：计算收益率
                    sell_shares = min(trade['quantity'], shares_held)
                    sell_price = trade['price']
                    sell_revenue = trade['amount'] - trade.get('commission_fee', 0.0)
                    
                    # 计算这部分持仓的成本
                    cost_ratio = sell_shares / shares_held
                    sold_cost = total_cost * cost_ratio
                    
                    # 计算收益率
                    if sold_cost > 0:
                        trade_return = (sell_revenue - sold_cost) / sold_cost
                        returns.append(trade_return)
                    
                    # 更新持仓和成本
                    shares_held -= sell_shares
                    total_cost -= sold_cost
            
            # 如果仍有持仓，计算当前收益
            if shares_held > 0:
                current_price = position.get('current_price', position.get('avg_price', 0))
                current_value = shares_held * current_price
                if total_cost > 0:
                    current_return = (current_value - total_cost) / total_cost
                    returns.append(current_return)
            
            # 如果没有足够的收益率数据，使用简单的收益率计算
            if len(returns) < 2:
                # 使用当前持仓的总收益率
                total_pnl = position.get('total_pnl', 0.0)
                market_value = position.get('market_value', 0.0)
                cost_basis = market_value - total_pnl
                
                if cost_basis <= 0:
                    return 0.0
                
                total_return = total_pnl / cost_basis
                # 简单波动率估算
                estimated_volatility = abs(total_return) * 2
                if estimated_volatility == 0:
                    return 0.0 if total_return <= 0 else 10.0  # 正收益无波动，给予高夏普比率
                
                return total_return / estimated_volatility
            
            # 使用TechnicalIndicators计算夏普比率
            returns_series = pd.Series(returns)
            sharpe_ratio = self.indicators.calculate_sharpe_ratio(returns_series, risk_free_rate, 'daily')
            
            # 如果计算失败，返回0
            if pd.isna(sharpe_ratio):
                logger.warning(f"ETF {etf_code} 夏普比率计算失败，返回0")
                return 0.0
                
            logger.info(f"ETF {etf_code} 夏普比率计算完成: {sharpe_ratio:.4f}")
            return float(sharpe_ratio)
            
        except Exception as e:
            logger.error(f"计算ETF {etf_code} 夏普比率失败: {e}")
            return 0.0
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        获取包含夏普比率的绩效指标
        
        Returns:
            包含夏普比率的绩效指标字典
        """
        try:
            # 获取基本绩效指标
            metrics = self.calculate_position_metrics()
            
            # 计算投资组合夏普比率
            portfolio_sharpe = self.calculate_portfolio_sharpe_ratio()
            metrics['portfolio_sharpe_ratio'] = portfolio_sharpe
            
            # 计算各ETF的夏普比率
            etf_sharpe_ratios = {}
            positions = self.get_positions()
            for position in positions:
                etf_code = position['symbol']
                etf_sharpe = self.calculate_etf_sharpe_ratio(etf_code)
                etf_sharpe_ratios[etf_code] = etf_sharpe
            
            metrics['etf_sharpe_ratios'] = etf_sharpe_ratios
            
            # 计算平均ETF夏普比率
            if etf_sharpe_ratios:
                avg_etf_sharpe = sum(etf_sharpe_ratios.values()) / len(etf_sharpe_ratios)
                metrics['avg_etf_sharpe_ratio'] = avg_etf_sharpe
            else:
                metrics['avg_etf_sharpe_ratio'] = 0.0
            
            # 添加夏普比率评级
            if portfolio_sharpe > 1.0:
                metrics['sharpe_rating'] = '优秀'
            elif portfolio_sharpe > 0.5:
                metrics['sharpe_rating'] = '良好'
            elif portfolio_sharpe > 0.0:
                metrics['sharpe_rating'] = '一般'
            else:
                metrics['sharpe_rating'] = '较差'
            
            logger.info("绩效指标（含夏普比率）计算完成")
            return metrics
            
        except Exception as e:
            logger.error(f"获取绩效指标失败: {e}")
            return {}
    