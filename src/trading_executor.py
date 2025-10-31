"""
交易执行模块
负责执行AI交易决策的核心逻辑
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional
from loguru import logger


class TradingExecutor:
    """交易执行器类"""
    
    def __init__(self, account_manager, data_fetcher, monitored_etfs, cache_lock, price_cache):
        """
        初始化交易执行器
        
        Args:
            account_manager: 账户管理器
            data_fetcher: 数据获取器
            monitored_etfs: 监控的ETF列表
            cache_lock: 缓存锁
            price_cache: 价格缓存
        """
        self.account_manager = account_manager
        self.data_fetcher = data_fetcher
        self.monitored_etfs = monitored_etfs
        self.cache_lock = cache_lock
        self.price_cache = price_cache
        self.last_trade_time = {}  # 记录每个ETF的最后交易时间
        self.trade_cooldown_seconds = 300  # 5分钟冷却时间
    
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
            
            # 记录日志
            if execution_result.get('success', False):
                print(f"📊 交易执行成功: {trade_record['symbol']} {trade_record['action']} {trade_record['quantity']}股")
                
                # 更新交易冷却时间
                if trade_record.get('action') in ['BUY', 'SELL']:
                    self.last_trade_time[trade_record['symbol']] = time.time()
                    print(f"⏰ 更新 {trade_record['symbol']} 交易冷却时间")
            else:
                print(f"⚠️  交易执行未成功: {trade_record['message']}")
            
            # 返回交易记录，供调用方处理
            return trade_record
            
        except Exception as e:
            print(f"❌ 处理交易结果失败: {e}")
            return None