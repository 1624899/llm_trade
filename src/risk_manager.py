"""
风险管理模块
负责基于夏普比率的风险评估和决策调整
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional
from loguru import logger


class RiskManager:
    """风险管理器类"""
    
    def __init__(self, sharpe_api, account_manager):
        """
        初始化风险管理器
        
        Args:
            sharpe_api: 夏普比率API
            account_manager: 账户管理器
        """
        self.sharpe_api = sharpe_api
        self.account_manager = account_manager
    
    def evaluate_sharpe_ratio_risk(self, trading_decision: Dict[str, Any]) -> Dict[str, Any]:
        """
        基于夏普比率评估交易决策的风险
        
        Args:
            trading_decision: 交易决策字典
            
        Returns:
            风险评估结果字典
        """
        try:
            logger.info("开始基于夏普比率评估交易决策风险")
            
            # 验证输入参数
            if not trading_decision or not isinstance(trading_decision, dict):
                logger.warning("交易决策参数无效，返回默认风险评估")
                return {'risk_level': 'unknown', 'recommendation': 'proceed', 'error': '无效的交易决策参数'}
            
            # 检查夏普比率API是否可用
            if not self.sharpe_api:
                logger.error("夏普比率API不可用，无法进行夏普比率风险评估")
                return {'risk_level': 'unknown', 'recommendation': 'proceed', 'error': '夏普比率API不可用'}
            
            # 使用夏普比率API进行风险评估
            risk_assessment = self.sharpe_api.evaluate_risk(trading_decision)
            
            logger.info(f"夏普比率风险评估完成，风险等级: {risk_assessment.get('risk_level', 'unknown')}，建议: {risk_assessment.get('recommendation', 'proceed')}")
            
            return risk_assessment
            
        except Exception as e:
            error_msg = f"夏普比率风险评估失败: {e}"
            logger.error(error_msg)
            return {'risk_level': 'unknown', 'recommendation': 'proceed', 'error': str(e)}
    
    def adjust_decision_by_risk(self, trading_decision: Dict[str, Any], risk_assessment: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据风险评估调整交易决策
        
        Args:
            trading_decision: 原始交易决策
            risk_assessment: 风险评估结果
            
        Returns:
            调整后的交易决策
        """
        try:
            # 复制原始决策，避免修改原对象
            adjusted_decision = trading_decision.copy()
            
            recommendation = risk_assessment.get('recommendation', 'proceed')
            decision_type = trading_decision.get('decision', '').upper()
            
            # 根据建议调整决策
            if recommendation == 'avoid':
                # 避免交易：改为持有
                if decision_type in ['BUY', 'SELL']:
                    adjusted_decision['decision'] = 'HOLD'
                    adjusted_decision['amount'] = 0
                    adjusted_decision['quantity'] = 0
                    adjusted_decision['reason'] = f"基于夏普比率风险评估，避免交易: {risk_assessment.get('factors', [''])[0]}"
                    logger.info(f"基于夏普比率风险评估，将{decision_type}决策调整为HOLD")
                    
            elif recommendation == 'reduce_amount' and decision_type == 'BUY':
                # 减少买入金额
                account_info = self.account_manager.get_account_info()
                total_assets = account_info.get('total_assets', 0)
                original_amount = adjusted_decision.get('amount', 0)
                
                # 减少到总资产的5%或原金额的50%，取较小值
                reduced_amount = min(total_assets * 0.05, original_amount * 0.5)
                
                if reduced_amount < original_amount:
                    adjusted_decision['amount'] = reduced_amount
                    # 重新计算数量
                    current_price = self._get_current_price_with_retry(trading_decision.get('symbol', ''))
                    if current_price > 0:
                        adjusted_decision['quantity'] = int(reduced_amount / current_price / 100) * 100
                    
                    adjusted_decision['reason'] = f"基于夏普比率风险评估，减少买入金额: {risk_assessment.get('factors', [''])[0]}"
                    logger.info(f"基于夏普比率风险评估，将买入金额从{original_amount:.2f}减少到{reduced_amount:.2f}")
                    
            elif recommendation == 'sell' and decision_type in ['BUY', 'HOLD']:
                # 建议卖出：如果原决策是买入或持有，改为卖出
                position = self.account_manager.get_position_by_symbol(trading_decision.get('symbol', ''))
                if position:
                    available_quantity = position.get('available_quantity', 0)
                    if available_quantity > 0:
                        adjusted_decision['decision'] = 'SELL'
                        adjusted_decision['amount'] = 0
                        adjusted_decision['quantity'] = available_quantity
                        adjusted_decision['reason'] = f"基于夏普比率风险评估，建议卖出: {risk_assessment.get('factors', [''])[0]}"
                        logger.info(f"基于夏普比率风险评估，将{decision_type}决策调整为SELL")
            
            return adjusted_decision
            
        except Exception as e:
            logger.error(f"根据风险评估调整决策失败: {e}")
            return trading_decision  # 出错时返回原始决策
    
    def update_sharpe_ratio_metrics(self, force_refresh: bool = False) -> bool:
        """
        更新夏普比率指标
        
        Args:
            force_refresh: 是否强制刷新缓存
            
        Returns:
            是否更新成功
        """
        start_time = time.time()
        try:
            print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] 开始更新夏普比率指标...")
            logger.info("开始更新夏普比率指标")
            
            # 检查夏普比率API是否可用
            if not self.sharpe_api:
                logger.error("夏普比率API不可用，无法更新夏普比率")
                return False
            
            # 获取绩效摘要（包含夏普比率）
            performance_summary = self.sharpe_api.get_performance_summary(force_refresh=force_refresh)
            
            if not performance_summary:
                logger.error("获取绩效摘要失败，无法更新夏普比率")
                return False
            
            # 提取夏普比率信息
            portfolio_sharpe = performance_summary.get('portfolio_sharpe_ratio', 0.0)
            avg_etf_sharpe = performance_summary.get('avg_etf_sharpe_ratio', 0.0)
            sharpe_rating = performance_summary.get('sharpe_rating', '未知')
            etf_sharpe_ratios = performance_summary.get('etf_sharpe_ratios', {})
            
            # 验证夏普比率数据的有效性
            if not isinstance(portfolio_sharpe, (int, float)):
                logger.warning(f"投资组合夏普比率数据类型异常: {type(portfolio_sharpe)}")
                portfolio_sharpe = 0.0
            
            if not isinstance(avg_etf_sharpe, (int, float)):
                logger.warning(f"平均ETF夏普比率数据类型异常: {type(avg_etf_sharpe)}")
                avg_etf_sharpe = 0.0
            
            if not isinstance(etf_sharpe_ratios, dict):
                logger.warning(f"ETF夏普比率数据类型异常: {type(etf_sharpe_ratios)}")
                etf_sharpe_ratios = {}
            
            print(f"✅ 夏普比率指标更新完成，耗时 {time.time() - start_time:.2f}s")
            print(f"   投资组合夏普比率: {portfolio_sharpe:.4f}")
            print(f"   平均ETF夏普比率: {avg_etf_sharpe:.4f}")
            print(f"   夏普比率评级: {sharpe_rating}")
            print(f"   总ETF数量: {performance_summary.get('total_etfs', 0)}")
            print(f"   正夏普比率ETF: {performance_summary.get('positive_sharpe_etfs', 0)}")
            print(f"   负夏普比率ETF: {performance_summary.get('negative_sharpe_etfs', 0)}")
            
            # 显示各ETF的夏普比率
            if etf_sharpe_ratios:
                print("   各ETF夏普比率:")
                for etf_code, sharpe in etf_sharpe_ratios.items():
                    print(f"     {etf_code}: {sharpe:.4f}")
            
            logger.info(f"夏普比率指标更新完成，投资组合夏普比率: {portfolio_sharpe:.4f}，耗时: {time.time() - start_time:.2f}s")
            return True
            
        except Exception as e:
            error_msg = f"更新夏普比率指标失败: {e}"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return False
    
    def _get_current_price_with_retry(self, symbol: str, max_retries: int = 3) -> float:
        """
        带重试机制获取当前价格（简化版本，主要用于风险调整时的价格计算）
        
        Args:
            symbol: ETF代码
            max_retries: 最大重试次数
            
        Returns:
            当前价格，获取失败返回0
        """
        # 这里应该调用数据获取器，但为了避免循环依赖，简化处理
        # 在实际使用中，可以通过依赖注入的方式传入数据获取器
        try:
            # 简化实现，实际应该从数据获取器获取
            logger.warning(f"简化版本的价格获取，ETF {symbol} 价格返回0")
            return 0.0
        except Exception as e:
            logger.error(f"获取ETF {symbol} 价格失败: {e}")
            return 0.0