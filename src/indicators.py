"""
技术指标计算模块
计算EMA、MACD、RSI、KDJ、BOLL、WR等技术指标
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from loguru import logger


class TechnicalIndicators:
    """技术指标计算器"""
    
    def __init__(self, config: Dict):
        """
        初始化技术指标计算器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.indicator_config = config.get('indicators', {})
        
        logger.info("技术指标计算器初始化完成")
    
    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """
        计算指数移动平均线(EMA)
        
        Args:
            prices: 价格序列
            period: 周期
            
        Returns:
            EMA序列
        """
        try:
            ema = prices.ewm(span=period, adjust=False).mean()
            return ema
        except Exception as e:
            logger.error(f"EMA计算失败: {e}")
            return pd.Series()
    
    def calculate_macd(self, prices: pd.Series, fast: int = 12, 
                      slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
        """
        计算MACD指标
        
        Args:
            prices: 价格序列
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期
            
        Returns:
            MACD指标字典
        """
        try:
            # 计算快慢线EMA
            ema_fast = self.calculate_ema(prices, fast)
            ema_slow = self.calculate_ema(prices, slow)
            
            # 计算MACD线
            macd_line = ema_fast - ema_slow
            
            # 计算信号线
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            
            # 计算MACD柱状图
            histogram = macd_line - signal_line
            
            return {
                'macd': macd_line,
                'signal': signal_line,
                'histogram': histogram
            }
        except Exception as e:
            logger.error(f"MACD计算失败: {e}")
            return {}
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        计算相对强弱指数(RSI)
        
        Args:
            prices: 价格序列
            period: 周期
            
        Returns:
            RSI序列
        """
        try:
            # 计算价格变化
            delta = prices.diff()
            
            # 分离涨跌
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            # 计算平均涨跌幅
            avg_gain = gain.rolling(window=period).mean()
            avg_loss = loss.rolling(window=period).mean()
            
            # 计算RS和RSI
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
        except Exception as e:
            logger.error(f"RSI计算失败: {e}")
            return pd.Series()
    
    def calculate_kdj(self, high: pd.Series, low: pd.Series, 
                     close: pd.Series, period: int = 9) -> Dict[str, pd.Series]:
        """
        计算KDJ指标
        
        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            period: 周期
            
        Returns:
            KDJ指标字典
        """
        try:
            # 计算最高价和最低价的滚动值
            highest_high = high.rolling(window=period).max()
            lowest_low = low.rolling(window=period).min()
            
            # 计算RSV (Raw Stochastic Value)
            rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
            
            # 计算K值 (使用指数移动平均)
            k_period = self.indicator_config.get('kdj', {}).get('k_period', 3)
            k_values = rsv.ewm(alpha=1/k_period, adjust=False).mean()
            
            # 计算D值
            d_period = self.indicator_config.get('kdj', {}).get('d_period', 3)
            d_values = k_values.ewm(alpha=1/d_period, adjust=False).mean()
            
            # 计算J值
            j_values = 3 * k_values - 2 * d_values
            
            return {
                'k': k_values,
                'd': d_values,
                'j': j_values,
                'rsv': rsv
            }
        except Exception as e:
            logger.error(f"KDJ计算失败: {e}")
            return {}
    
    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, 
                                 std_dev: float = 2.0) -> Dict[str, pd.Series]:
        """
        计算布林带(BOLL)
        
        Args:
            prices: 价格序列
            period: 周期
            std_dev: 标准差倍数
            
        Returns:
            布林带指标字典
        """
        try:
            # 计算中轨（简单移动平均）
            middle_band = prices.rolling(window=period).mean()
            
            # 计算标准差
            rolling_std = prices.rolling(window=period).std()
            
            # 计算上下轨
            upper_band = middle_band + (rolling_std * std_dev)
            lower_band = middle_band - (rolling_std * std_dev)
            
            # 计算带宽
            bandwidth = (upper_band - lower_band) / middle_band * 100
            
            # 计算%B（价格在布林带中的位置）
            percent_b = (prices - lower_band) / (upper_band - lower_band)
            
            return {
                'upper': upper_band,
                'middle': middle_band,
                'lower': lower_band,
                'bandwidth': bandwidth,
                'percent_b': percent_b
            }
        except Exception as e:
            logger.error(f"布林带计算失败: {e}")
            return {}
    
    def calculate_williams_r(self, high: pd.Series, low: pd.Series, 
                           close: pd.Series, period: int = 14) -> pd.Series:
        """
        计算威廉指标(%R)
        
        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            period: 周期
            
        Returns:
            威廉指标序列
        """
        try:
            # 计算最高价和最低价的滚动值
            highest_high = high.rolling(window=period).max()
            lowest_low = low.rolling(window=period).min()
            
            # 计算威廉指标
            wr = (highest_high - close) / (highest_high - lowest_low) * -100
            
            return wr
        except Exception as e:
            logger.error(f"威廉指标计算失败: {e}")
            return pd.Series()
    
    def calculate_atr(self, high: pd.Series, low: pd.Series, 
                     close: pd.Series, period: int = 14) -> pd.Series:
        """
        计算平均真实波幅(ATR)
        
        Args:
            high: 最高价序列
            low: 最低价序列
            close: 收盘价序列
            period: 周期
            
        Returns:
            ATR序列
        """
        try:
            # 计算真实波幅
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            # 计算平均真实波幅
            atr = tr.rolling(window=period).mean()
            
            return atr
        except Exception as e:
            logger.error(f"ATR计算失败: {e}")
            return pd.Series()
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        计算所有技术指标
        
        Args:
            df: 包含OHLCV数据的DataFrame
            
        Returns:
            所有技术指标字典
        """
        try:
            indicators = {}
            
            # 获取配置参数
            ema_config = self.indicator_config.get('ema', {})
            macd_config = self.indicator_config.get('macd', {})
            rsi_config = self.indicator_config.get('rsi', {})
            kdj_config = self.indicator_config.get('kdj', {})
            boll_config = self.indicator_config.get('boll', {})
            wr_config = self.indicator_config.get('wr', {})
            
            # EMA指标
            if 'close' in df.columns:
                indicators['ema_short'] = self.calculate_ema(df['close'], ema_config.get('short', 12))
                indicators['ema_long'] = self.calculate_ema(df['close'], ema_config.get('long', 20))
                indicators['ema_signal'] = self.calculate_ema(df['close'], ema_config.get('signal', 9))
            
            # MACD指标
            if 'close' in df.columns:
                macd_data = self.calculate_macd(
                    df['close'], 
                    macd_config.get('fast', 12),
                    macd_config.get('slow', 26),
                    macd_config.get('signal', 9)
                )
                indicators.update(macd_data)
            
            # RSI指标
            if 'close' in df.columns:
                indicators['rsi_7'] = self.calculate_rsi(df['close'], rsi_config.get('period_7', 7))
                indicators['rsi_14'] = self.calculate_rsi(df['close'], rsi_config.get('period_14', 14))
            
            # KDJ指标
            if all(col in df.columns for col in ['high', 'low', 'close']):
                kdj_data = self.calculate_kdj(
                    df['high'], df['low'], df['close'],
                    kdj_config.get('period', 9)
                )
                indicators.update(kdj_data)
            
            # 布林带指标
            if 'close' in df.columns:
                boll_data = self.calculate_bollinger_bands(
                    df['close'],
                    boll_config.get('period', 20),
                    boll_config.get('std_dev', 2)
                )
                indicators.update(boll_data)
            
            # 威廉指标
            if all(col in df.columns for col in ['high', 'low', 'close']):
                indicators['williams_r'] = self.calculate_williams_r(
                    df['high'], df['low'], df['close'],
                    wr_config.get('period', 14)
                )
            
            # ATR指标
            if all(col in df.columns for col in ['high', 'low', 'close']):
                indicators['atr_3'] = self.calculate_atr(df['high'], df['low'], df['close'], 3)
                indicators['atr_14'] = self.calculate_atr(df['high'], df['low'], df['close'], 14)
            
            logger.info("所有技术指标计算完成")
            return indicators
            
        except Exception as e:
            logger.error(f"技术指标计算失败: {e}")
            return {}
    
    def get_latest_indicator_values(self, df: pd.DataFrame, 
                                  indicators: Dict[str, pd.Series]) -> Dict[str, float]:
        """
        获取最新的指标值
        
        Args:
            df: 原始数据DataFrame
            indicators: 指标字典
            
        Returns:
            最新指标值字典
        """
        try:
            latest_values = {}
            
            # 获取最新价格数据
            if not df.empty:
                latest_row = df.iloc[-1]
                latest_values['current_price'] = float(latest_row.get('close', 0))
                latest_values['current_volume'] = int(latest_row.get('volume', 0))
            
            # 获取最新指标值
            for name, series in indicators.items():
                if not series.empty:
                    latest_values[f'current_{name}'] = float(series.iloc[-1])
            
            return latest_values
            
        except Exception as e:
            logger.error(f"获取最新指标值失败: {e}")
            return {}
    
    def get_indicator_series(self, indicators: Dict[str, pd.Series], 
                           count: int = 10) -> Dict[str, List[float]]:
        """
        获取指标序列（用于生成语料）
        
        Args:
            indicators: 指标字典
            count: 获取的数据条数
            
        Returns:
            指标序列字典
        """
        try:
            series_data = {}
            
            for name, series in indicators.items():
                if not series.empty:
                    # 获取最新的count条数据
                    if len(series) >= count:
                        series_data[name] = series.tail(count).tolist()
                    else:
                        # 如果数据不足，用前面的值填充
                        padding = [series.iloc[0]] * (count - len(series))
                        series_data[name] = padding + series.tolist()
            
            return series_data
            
        except Exception as e:
            logger.error(f"获取指标序列失败: {e}")
            return {}