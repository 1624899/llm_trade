"""
技术指标计算模块
计算EMA、MACD、RSI、KDJ、BOLL、WR等技术指标，以及增强指标和风险指标
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
        计算RSI（Wilders平滑版本，使用EMA）
        """
        try:
            delta = prices.diff()
            gain = delta.where(delta > 0, 0.0)
            loss = -delta.where(delta < 0, 0.0)
            
            # 使用 Wilders 平滑（alpha = 1/period）
            avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
            avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            # 处理除零或无穷
            rsi = rsi.replace([np.inf, -np.inf], np.nan).fillna(50)
            return rsi
        except Exception as e:
            logger.error(f"RSI计算失败: {e}")
            return pd.Series()
    
    def calculate_kdj(self, high: pd.Series, low: pd.Series, 
                 close: pd.Series, period: int = 9) -> Dict[str, pd.Series]:
        """
        计算KDJ指标（标准SMA版本）
        """
        try:
            # 滚动最高/最低
            highest_high = high.rolling(window=period, min_periods=1).max()
            lowest_low = low.rolling(window=period, min_periods=1).min()
            
            # RSV
            rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
            rsv = rsv.fillna(50)  # 初始值设为50

            # K: 3日SMA of RSV
            k_period = self.indicator_config.get('kdj', {}).get('k_period', 3)
            k_values = rsv.rolling(window=k_period, min_periods=1).mean()
            
            # D: 3日SMA of K
            d_period = self.indicator_config.get('kdj', {}).get('d_period', 3)
            d_values = k_values.rolling(window=d_period, min_periods=1).mean()
            
            # J = 3K - 2D
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
            band_width = upper_band - lower_band
            bandwidth =  band_width / middle_band * 100
            
            # 计算%B（价格在布林带中的位置）
            percent_b = np.where(
                band_width > 1e-8, 
                (prices - lower_band) / band_width, 
                 0.5  # 当带宽为0时，视为中轨
                )
            percent_b = pd.Series(percent_b, index=prices.index)
            
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
        计算ATR（Wilders平滑版本）
        """
        try:
            # 真实波幅 TR
            tr1 = high - low
            tr2 = (high - close.shift(1)).abs()
            tr3 = (low - close.shift(1)).abs()
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
            # 初始化 ATR 序列
            atr = pd.Series(index=tr.index, dtype='float64')
            atr.iloc[:period] = np.nan
            
            if len(tr) >= period:
                # 初始值：前 period 个 TR 的 SMA
                initial_atr = tr.iloc[:period].mean()
                atr.iloc[period - 1] = initial_atr
                
                # 递推：ATR_t = (ATR_{t-1} * (n-1) + TR_t) / n
                for i in range(period, len(tr)):
                    atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr.iloc[i]) / period
            
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
    
    def calculate_trend_strength(self, prices: pd.Series, period: int = 20) -> float:
        """
        计算趋势强度指标
        
        Args:
            prices: 价格序列
            period: 计算周期
            
        Returns:
            趋势强度值 (-1到1之间，越接近1表示上升趋势越强，越接近-1表示下降趋势越强)
        """
        try:
            if len(prices) < period:
                return 0.0
            
            # 计算线性回归斜率
            x = np.arange(len(prices[-period:]))
            y = prices[-period:].values
            
            # 线性回归
            slope, intercept = np.polyfit(x, y, 1)
            
            # 计算趋势强度 (标准化斜率)
            price_range = prices[-period:].max() - prices[-period:].min()
            if price_range == 0:
                return 0.0
            
            trend_strength = slope / price_range * period
            
            # 限制在-1到1之间
            trend_strength = np.clip(trend_strength, -1, 1)
            
            return float(trend_strength)
            
        except Exception as e:
            logger.error(f"趋势强度计算失败: {e}")
            return 0.0
    
    def calculate_support_resistance(self, prices: pd.Series, period: int = 20) -> Dict[str, float]:
        """
        计算支撑位和阻力位
        
        Args:
            prices: 价格序列
            period: 计算周期
            
        Returns:
            支撑位和阻力位字典
        """
        try:
            if len(prices) < period:
                return {'support': 0.0, 'resistance': 0.0}
            
            recent_prices = prices[-period:]
            
            # 简单支撑位：近期最低价
            support = recent_prices.min()
            
            # 简单阻力位：近期最高价
            resistance = recent_prices.max()
            
            return {
                'support': float(support),
                'resistance': float(resistance)
            }
            
        except Exception as e:
            logger.error(f"支撑阻力位计算失败: {e}")
            return {'support': 0.0, 'resistance': 0.0}
    
    def calculate_volatility(self, prices: pd.Series, period: int = 14) -> float:
        """
        计算波动率指标
        
        Args:
            prices: 价格序列
            period: 计算周期
            
        Returns:
            波动率值
        """
        try:
            if len(prices) < period:
                return 0.0
            
            # 计算收益率
            returns = prices.pct_change().dropna()
            
            if len(returns) < period:
                return 0.0
            
            # 计算标准差作为波动率
            volatility = returns[-period:].std()
            
            return float(volatility)
            
        except Exception as e:
            logger.error(f"波动率计算失败: {e}")
            return 0.0
    
    def calculate_correlation(self, prices1: pd.Series, prices2: pd.Series, period: int = 20) -> float:
        """
        计算两个价格序列的相关性
        
        Args:
            prices1: 第一个价格序列
            prices2: 第二个价格序列
            period: 计算周期
            
        Returns:
            相关性值 (-1到1之间)
        """
        try:
            if len(prices1) < period or len(prices2) < period:
                return 0.0
            
            # 取最近period个数据点
            p1 = prices1[-period:]
            p2 = prices2[-period:]
            
            # 计算相关系数
            correlation = p1.corr(p2)
            
            return float(correlation) if not np.isnan(correlation) else 0.0
            
        except Exception as e:
            logger.error(f"相关性计算失败: {e}")
            return 0.0