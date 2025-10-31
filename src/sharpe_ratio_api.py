"""
夏普比率API模块
提供统一的夏普比率计算和管理接口
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from loguru import logger
import pandas as pd


class SharpeRatioAPI:
    """夏普比率API类，提供统一的夏普比率计算和管理接口"""
    
    def __init__(self, account_manager, indicators=None):
        """
        初始化夏普比率API
        
        Args:
            account_manager: AccountManager实例
            indicators: TechnicalIndicators实例（可选）
        """
        self.account_manager = account_manager
        self.indicators = indicators
        
        # 缓存夏普比率计算结果
        self._sharpe_cache = {}
        self._cache_timestamp = {}
        self._cache_expiry = 300  # 缓存5分钟
        
        logger.info("夏普比率API初始化完成")
    
    def get_portfolio_sharpe_ratio(self, risk_free_rate: float = 0.0, 
                                force_refresh: bool = False) -> float:
        """
        获取投资组合夏普比率
        
        Args:
            risk_free_rate: 无风险利率，默认为0.0
            force_refresh: 是否强制刷新缓存
            
        Returns:
            投资组合的夏普比率值
        """
        try:
            cache_key = f"portfolio_{risk_free_rate}"
            
            # 检查缓存
            if not force_refresh and self._is_cache_valid(cache_key):
                logger.debug(f"从缓存获取投资组合夏普比率: {self._sharpe_cache[cache_key]:.4f}")
                return self._sharpe_cache[cache_key]
            
            logger.info("开始计算投资组合夏普比率")
            
            # 验证账户管理器
            if not self.account_manager:
                logger.error("账户管理器不可用，无法计算投资组合夏普比率")
                return 0.0
            
            # 计算夏普比率
            sharpe_ratio = self.account_manager.calculate_portfolio_sharpe_ratio(risk_free_rate)
            
            # 验证结果
            if not isinstance(sharpe_ratio, (int, float)):
                logger.warning(f"投资组合夏普比率结果类型异常: {type(sharpe_ratio)}")
                sharpe_ratio = 0.0
            
            # 更新缓存
            self._update_cache(cache_key, sharpe_ratio)
            
            logger.info(f"投资组合夏普比率计算完成: {sharpe_ratio:.4f}")
            return float(sharpe_ratio)
            
        except Exception as e:
            logger.error(f"获取投资组合夏普比率失败: {e}")
            return 0.0
    
    def get_etf_sharpe_ratio(self, etf_code: str, risk_free_rate: float = 0.0,
                           force_refresh: bool = False) -> float:
        """
        获取单个ETF的夏普比率
        
        Args:
            etf_code: ETF代码
            risk_free_rate: 无风险利率，默认为0.0
            force_refresh: 是否强制刷新缓存
            
        Returns:
            单个ETF的夏普比率值
        """
        try:
            cache_key = f"etf_{etf_code}_{risk_free_rate}"
            
            # 检查缓存
            if not force_refresh and self._is_cache_valid(cache_key):
                logger.debug(f"从缓存获取ETF {etf_code} 夏普比率: {self._sharpe_cache[cache_key]:.4f}")
                return self._sharpe_cache[cache_key]
            
            logger.info(f"开始计算ETF {etf_code} 夏普比率")
            
            # 验证参数
            if not etf_code or not isinstance(etf_code, str):
                logger.warning(f"ETF代码无效: {etf_code}")
                return 0.0
            
            # 验证账户管理器
            if not self.account_manager:
                logger.error("账户管理器不可用，无法计算ETF夏普比率")
                return 0.0
            
            # 计算夏普比率
            sharpe_ratio = self.account_manager.calculate_etf_sharpe_ratio(etf_code, risk_free_rate)
            
            # 验证结果
            if not isinstance(sharpe_ratio, (int, float)):
                logger.warning(f"ETF {etf_code} 夏普比率结果类型异常: {type(sharpe_ratio)}")
                sharpe_ratio = 0.0
            
            # 更新缓存
            self._update_cache(cache_key, sharpe_ratio)
            
            logger.info(f"ETF {etf_code} 夏普比率计算完成: {sharpe_ratio:.4f}")
            return float(sharpe_ratio)
            
        except Exception as e:
            logger.error(f"获取ETF {etf_code} 夏普比率失败: {e}")
            return 0.0
    
    def get_all_etf_sharpe_ratios(self, risk_free_rate: float = 0.0,
                                force_refresh: bool = False) -> Dict[str, float]:
        """
        获取所有ETF的夏普比率
        
        Args:
            risk_free_rate: 无风险利率，默认为0.0
            force_refresh: 是否强制刷新缓存
            
        Returns:
            ETF代码到夏普比率的映射字典
        """
        try:
            logger.info("开始获取所有ETF夏普比率")
            
            # 验证账户管理器
            if not self.account_manager:
                logger.error("账户管理器不可用，无法获取ETF夏普比率")
                return {}
            
            # 获取绩效指标
            performance_metrics = self.account_manager.get_performance_metrics()
            
            if not performance_metrics:
                logger.warning("无法获取绩效指标，返回空ETF夏普比率字典")
                return {}
            
            # 提取ETF夏普比率
            etf_sharpe_ratios = performance_metrics.get('etf_sharpe_ratios', {})
            
            # 验证数据类型
            if not isinstance(etf_sharpe_ratios, dict):
                logger.warning(f"ETF夏普比率数据类型异常: {type(etf_sharpe_ratios)}")
                return {}
            
            # 验证并清理数据
            cleaned_ratios = {}
            for etf_code, sharpe in etf_sharpe_ratios.items():
                if isinstance(sharpe, (int, float)):
                    cleaned_ratios[etf_code] = float(sharpe)
                else:
                    logger.warning(f"ETF {etf_code} 夏普比率数据类型异常: {type(sharpe)}")
            
            logger.info(f"获取到 {len(cleaned_ratios)} 个ETF的夏普比率")
            return cleaned_ratios
            
        except Exception as e:
            logger.error(f"获取所有ETF夏普比率失败: {e}")
            return {}
    
    def get_performance_summary(self, risk_free_rate: float = 0.0,
                            force_refresh: bool = False) -> Dict[str, Any]:
        """
        获取包含夏普比率的完整绩效摘要
        
        Args:
            risk_free_rate: 无风险利率，默认为0.0
            force_refresh: 是否强制刷新缓存
            
        Returns:
            完整的绩效摘要字典
        """
        try:
            logger.info("开始获取绩效摘要")
            
            # 验证账户管理器
            if not self.account_manager:
                logger.error("账户管理器不可用，无法获取绩效摘要")
                return {}
            
            # 获取绩效指标
            performance_metrics = self.account_manager.get_performance_metrics()
            
            if not performance_metrics:
                logger.warning("无法获取绩效指标，返回空绩效摘要")
                return {}
            
            # 获取投资组合夏普比率
            portfolio_sharpe = self.get_portfolio_sharpe_ratio(risk_free_rate, force_refresh)
            
            # 获取所有ETF夏普比率
            etf_sharpe_ratios = self.get_all_etf_sharpe_ratios(risk_free_rate, force_refresh)
            
            # 计算平均ETF夏普比率
            avg_etf_sharpe = 0.0
            if etf_sharpe_ratios:
                avg_etf_sharpe = sum(etf_sharpe_ratios.values()) / len(etf_sharpe_ratios)
            
            # 构建绩效摘要
            summary = {
                'portfolio_sharpe_ratio': portfolio_sharpe,
                'avg_etf_sharpe_ratio': avg_etf_sharpe,
                'etf_sharpe_ratios': etf_sharpe_ratios,
                'sharpe_rating': self._get_sharpe_rating(portfolio_sharpe),
                'total_etfs': len(etf_sharpe_ratios),
                'positive_sharpe_etfs': sum(1 for s in etf_sharpe_ratios.values() if s > 0),
                'negative_sharpe_etfs': sum(1 for s in etf_sharpe_ratios.values() if s < 0),
                'risk_free_rate': risk_free_rate,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 添加其他绩效指标
            for key, value in performance_metrics.items():
                if key not in summary:
                    summary[key] = value
            
            logger.info(f"绩效摘要获取完成，投资组合夏普比率: {portfolio_sharpe:.4f}")
            return summary
            
        except Exception as e:
            logger.error(f"获取绩效摘要失败: {e}")
            return {}
    
    def evaluate_risk(self, trading_decision: Dict[str, Any], 
                    risk_free_rate: float = 0.0) -> Dict[str, Any]:
        """
        基于夏普比率评估交易决策的风险
        
        Args:
            trading_decision: 交易决策字典
            risk_free_rate: 无风险利率，默认为0.0
            
        Returns:
            风险评估结果字典
        """
        try:
            logger.info("开始基于夏普比率评估交易决策风险")
            
            # 验证输入参数
            if not trading_decision or not isinstance(trading_decision, dict):
                logger.warning("交易决策参数无效，返回默认风险评估")
                return {
                    'risk_level': 'unknown',
                    'recommendation': 'proceed',
                    'error': '无效的交易决策参数'
                }
            
            # 获取绩效摘要
            performance_summary = self.get_performance_summary(risk_free_rate)
            
            if not performance_summary:
                logger.warning("无法获取绩效摘要，跳过夏普比率风险评估")
                return {
                    'risk_level': 'unknown',
                    'recommendation': 'proceed',
                    'error': '无法获取绩效摘要'
                }
            
            # 提取关键指标
            portfolio_sharpe = performance_summary.get('portfolio_sharpe_ratio', 0.0)
            etf_sharpe_ratios = performance_summary.get('etf_sharpe_ratios', {})
            
            # 获取决策信息
            decision_type = trading_decision.get('decision', '').upper()
            symbol = trading_decision.get('symbol', '')
            amount = trading_decision.get('amount', 0)
            
            # 验证决策类型
            if decision_type not in ['BUY', 'SELL', 'HOLD']:
                logger.warning(f"未知的决策类型: {decision_type}")
                decision_type = 'HOLD'  # 默认为持有
            
            # 初始化风险评估结果
            risk_assessment = {
                'risk_level': 'low',
                'recommendation': 'proceed',
                'factors': [],
                'portfolio_sharpe': portfolio_sharpe,
                'decision_type': decision_type,
                'symbol': symbol,
                'amount': amount,
                'risk_free_rate': risk_free_rate
            }
            
            logger.info(f"评估投资组合夏普比率: {portfolio_sharpe:.4f}")
            
            # 评估投资组合整体风险
            if portfolio_sharpe < 0:
                risk_assessment['risk_level'] = 'high'
                risk_assessment['recommendation'] = 'reduce'
                risk_assessment['factors'].append('投资组合夏普比率为负，整体表现不佳')
                logger.warning(f"投资组合夏普比率为负 ({portfolio_sharpe:.4f})，风险等级: 高")
            elif portfolio_sharpe < 0.5:
                risk_assessment['risk_level'] = 'medium'
                risk_assessment['factors'].append('投资组合夏普比率较低，风险调整后收益一般')
                logger.info(f"投资组合夏普比率较低 ({portfolio_sharpe:.4f})，风险等级: 中")
            else:
                logger.info(f"投资组合夏普比率良好 ({portfolio_sharpe:.4f})，风险等级: 低")
            
            # 评估特定ETF的风险
            if symbol and symbol in etf_sharpe_ratios:
                etf_sharpe = etf_sharpe_ratios[symbol]
                risk_assessment['etf_sharpe'] = etf_sharpe
                
                logger.info(f"评估ETF {symbol} 夏普比率: {etf_sharpe:.4f}")
                
                if etf_sharpe < -1.0:
                    risk_assessment['risk_level'] = 'high'
                    risk_assessment['recommendation'] = 'avoid'
                    risk_assessment['factors'].append(f'ETF {symbol} 夏普比率极低 ({etf_sharpe:.2f})，历史表现很差')
                    logger.warning(f"ETF {symbol} 夏普比率极低 ({etf_sharpe:.4f})，建议避免交易")
                elif etf_sharpe < 0:
                    risk_assessment['risk_level'] = 'medium'
                    risk_assessment['factors'].append(f'ETF {symbol} 夏普比率为负 ({etf_sharpe:.2f})，历史表现不佳')
                    logger.warning(f"ETF {symbol} 夏普比率为负 ({etf_sharpe:.4f})，历史表现不佳")
                else:
                    logger.info(f"ETF {symbol} 夏普比率良好 ({etf_sharpe:.4f})")
            elif symbol:
                logger.warning(f"未找到ETF {symbol} 的夏普比率数据")
            
            # 根据决策类型调整风险评估
            if decision_type == 'BUY':
                # 买入决策：更严格的风险评估
                logger.info(f"评估买入决策，金额: {amount:.2f}")
                
                if portfolio_sharpe < 0.3:
                    risk_assessment['recommendation'] = 'reduce_amount'
                    risk_assessment['factors'].append('投资组合夏普比率较低，建议减少买入金额')
                    logger.info("投资组合夏普比率较低，建议减少买入金额")
                
                # 检查买入金额是否过大
                try:
                    if self.account_manager:
                        account_info = self.account_manager.get_account_info()
                        total_assets = account_info.get('total_assets', 0)
                        
                        if total_assets > 0:
                            amount_ratio = amount / total_assets
                            logger.info(f"买入金额比例: {amount_ratio:.2%}")
                            
                            if amount_ratio > 0.15:  # 超过15%
                                risk_assessment['risk_level'] = 'high'
                                risk_assessment['recommendation'] = 'reduce_amount'
                                risk_assessment['factors'].append('单次买入金额过大，超过总资产的15%')
                                logger.warning(f"单次买入金额过大 ({amount_ratio:.2%})，建议减少")
                        else:
                            logger.warning("无法获取有效的总资产数据")
                except Exception as e:
                    logger.error(f"检查买入金额时发生错误: {e}")
                    
            elif decision_type == 'SELL':
                # 卖出决策：考虑止损
                logger.info(f"评估卖出决策")
                
                if symbol and symbol in etf_sharpe_ratios:
                    etf_sharpe = etf_sharpe_ratios[symbol]
                    if etf_sharpe < -0.5:
                        risk_assessment['recommendation'] = 'sell'
                        risk_assessment['factors'].append(f'ETF {symbol} 夏普比率很低，建议及时止损')
                        logger.info(f"ETF {symbol} 夏普比率很低，建议及时止损")
            
            # 记录最终风险评估结果
            logger.info(f"夏普比率风险评估完成，风险等级: {risk_assessment['risk_level']}，建议: {risk_assessment['recommendation']}")
            if risk_assessment['factors']:
                logger.info(f"风险因素: {'; '.join(risk_assessment['factors'])}")
            
            return risk_assessment
            
        except Exception as e:
            error_msg = f"夏普比率风险评估失败: {e}"
            logger.error(error_msg)
            return {
                'risk_level': 'unknown',
                'recommendation': 'proceed',
                'error': str(e)
            }
    
    def clear_cache(self) -> None:
        """清除夏普比率缓存"""
        try:
            self._sharpe_cache.clear()
            self._cache_timestamp.clear()
            logger.info("夏普比率缓存已清除")
        except Exception as e:
            logger.error(f"清除夏普比率缓存失败: {e}")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        Returns:
            缓存信息字典
        """
        try:
            cache_info = {
                'cache_size': len(self._sharpe_cache),
                'cache_keys': list(self._sharpe_cache.keys()),
                'cache_expiry_seconds': self._cache_expiry,
                'current_timestamp': datetime.now().timestamp()
            }
            
            # 添加每个缓存项的时间戳
            for key, timestamp in self._cache_timestamp.items():
                cache_info[f'{key}_timestamp'] = timestamp
                cache_info[f'{key}_age_seconds'] = datetime.now().timestamp() - timestamp
                cache_info[f'{key}_is_valid'] = self._is_cache_valid(key)
            
            return cache_info
            
        except Exception as e:
            logger.error(f"获取缓存信息失败: {e}")
            return {}
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """
        检查缓存是否有效
        
        Args:
            cache_key: 缓存键
            
        Returns:
            缓存是否有效
        """
        try:
            if cache_key not in self._sharpe_cache:
                return False
            
            if cache_key not in self._cache_timestamp:
                return False
            
            age = datetime.now().timestamp() - self._cache_timestamp[cache_key]
            return age < self._cache_expiry
            
        except Exception as e:
            logger.error(f"检查缓存有效性失败: {e}")
            return False
    
    def _update_cache(self, cache_key: str, value: float) -> None:
        """
        更新缓存
        
        Args:
            cache_key: 缓存键
            value: 缓存值
        """
        try:
            self._sharpe_cache[cache_key] = value
            self._cache_timestamp[cache_key] = datetime.now().timestamp()
            logger.debug(f"更新缓存 {cache_key}: {value:.4f}")
        except Exception as e:
            logger.error(f"更新缓存失败: {e}")
    
    def _get_sharpe_rating(self, sharpe_ratio: float) -> str:
        """
        根据夏普比率获取评级
        
        Args:
            sharpe_ratio: 夏普比率值
            
        Returns:
            夏普比率评级字符串
        """
        try:
            if sharpe_ratio > 1.0:
                return '优秀'
            elif sharpe_ratio > 0.5:
                return '良好'
            elif sharpe_ratio > 0.0:
                return '一般'
            else:
                return '较差'
        except Exception as e:
            logger.error(f"获取夏普比率评级失败: {e}")
            return '未知'