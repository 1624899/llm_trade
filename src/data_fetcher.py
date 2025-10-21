"""
行情数据获取模块
使用AkShare获取A股ETF实时和历史数据
"""

import akshare as ak
import efinance as ef
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from loguru import logger
import time
import os
from .utils import validate_etf_code, get_current_time_str


class ETFDataFetcher:
    """ETF数据获取器"""
    
    def __init__(self, config: Dict):
        """
        初始化数据获取器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.data_config = config.get('data', {})
        self.cache_duration = self.data_config.get('cache_duration', 3600)
        self.retry_times = self.data_config.get('retry_times', 3)
        self.timeout = self.data_config.get('timeout', 30)
        
        # 数据缓存
        self.cache = {}
        
        logger.info("ETF数据获取器初始化完成")
    
    def get_real_time_data(self, etf_code: str) -> Optional[Dict]:
        """
        获取ETF实时数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            实时数据字典
        """
        if not validate_etf_code(etf_code):
            logger.error(f"无效的ETF代码: {etf_code}")
            return None
        
        # 检查缓存
        cache_key = f"realtime_{etf_code}"
        if self._is_cache_valid(cache_key):
            logger.info(f"使用缓存数据: {etf_code}")
            return self.cache[cache_key]['data']
        
        for attempt in range(self.retry_times):
            try:
                logger.info(f"获取ETF实时数据: {etf_code}, 尝试次数: {attempt + 1}")
                
                # 使用AkShare获取实时数据
                df = ak.fund_etf_fund_info_em(fund=etf_code)
                
                if df.empty:
                    logger.warning(f"ETF {etf_code} 实时数据为空")
                    return None
                
                # 提取关键信息
                latest_data = df.iloc[-1] if len(df) > 0 else None
                if latest_data is None:
                    logger.warning(f"ETF {etf_code} 无法获取最新数据")
                    return None
                
                # 构造返回数据
                result = {
                    'code': etf_code,
                    'name': latest_data.get('基金简称', ''),
                    'current_price': float(latest_data.get('最新价', 0)),
                    'open_price': float(latest_data.get('开盘价', 0)),
                    'high_price': float(latest_data.get('最高价', 0)),
                    'low_price': float(latest_data.get('最低价', 0)),
                    'prev_close': float(latest_data.get('昨收价', 0)),
                    'volume': int(latest_data.get('成交量', 0)),
                    'amount': float(latest_data.get('成交额', 0)),
                    'change_pct': float(latest_data.get('涨跌幅', 0)),
                    'update_time': get_current_time_str()
                }
                
                # 缓存数据
                self.cache[cache_key] = {
                    'data': result,
                    'timestamp': time.time()
                }
                
                logger.info(f"ETF {etf_code} 实时数据获取成功")
                return result
                
            except Exception as e:
                logger.error(f"获取ETF {etf_code} 实时数据失败 (尝试 {attempt + 1}): {e}")
                if attempt < self.retry_times - 1:
                    time.sleep(2 ** attempt)  # 指数退避
        
        return None
    
    def get_historical_data(self, etf_code: str, period: str = "1d", 
                           count: int = 100) -> Optional[pd.DataFrame]:
        """
        获取ETF历史数据
        
        Args:
            etf_code: ETF代码
            period: 时间周期 (1m, 5m, 15m, 30m, 1h, 1d)
            count: 数据条数
            
        Returns:
            历史数据DataFrame
        """
        if not validate_etf_code(etf_code):
            logger.error(f"无效的ETF代码: {etf_code}")
            return None
        
        # 检查缓存
        cache_key = f"hist_{etf_code}_{period}_{count}"
        if self._is_cache_valid(cache_key):
            logger.info(f"使用缓存历史数据: {etf_code}")
            return self.cache[cache_key]['data']
        
        for attempt in range(self.retry_times):
            try:
                logger.info(f"获取ETF历史数据: {etf_code}, 周期: {period}, 尝试次数: {attempt + 1}")
                
                # 优先使用efinance获取分钟级数据
                df = None
                if period in ['1m', '5m', '15m', '30m']:
                    try:
                        logger.info(f"尝试使用efinance获取ETF {etf_code} 分钟级数据...")
                        # 使用efinance获取分钟级数据
                        df = ef.stock.get_quote_history(etf_code, klt=1)  # 1分钟K线
                        # 转换列名为标准格式
                        if not df.empty:
                            df = df.rename(columns={
                                '日期': 'date',
                                '开盘': 'open',
                                '收盘': 'close',
                                '最高': 'high',
                                '最低': 'low',
                                '成交量': 'volume',
                                '成交额': 'amount'
                            })
                            logger.info(f"efinance获取ETF {etf_code} 分钟级数据成功，数据条数: {len(df)}")
                    except Exception as e:
                        logger.warning(f"efinance获取ETF {etf_code} 分钟级数据失败: {e}")
                        # 回退到AkShare
                        try:
                            logger.info(f"回退到AkShare获取ETF {etf_code} 分钟级数据...")
                            df = ak.fund_etf_hist_em(symbol=etf_code, period=period)
                        except Exception as ak_e:
                            logger.warning(f"AkShare获取ETF {etf_code} 分钟级数据也失败: {ak_e}")
                            # 如果分钟级数据获取失败，使用日级数据替代
                            logger.warning(f"分钟级数据获取失败，使用日级数据替代: {etf_code}")
                            df = ak.fund_etf_hist_em(symbol=etf_code)
                elif period == '1h':
                    # 小时级数据（使用60分钟）
                    try:
                        logger.info(f"尝试使用efinance获取ETF {etf_code} 小时级数据...")
                        df = ef.stock.get_quote_history(etf_code, klt=60)  # 60分钟K线
                        # 转换列名为标准格式
                        if not df.empty:
                            df = df.rename(columns={
                                '日期': 'date',
                                '开盘': 'open',
                                '收盘': 'close',
                                '最高': 'high',
                                '最低': 'low',
                                '成交量': 'volume',
                                '成交额': 'amount'
                            })
                            logger.info(f"efinance获取ETF {etf_code} 小时级数据成功，数据条数: {len(df)}")
                    except Exception as e:
                        logger.warning(f"efinance获取ETF {etf_code} 小时级数据失败: {e}")
                        # 回退到AkShare
                        try:
                            logger.info(f"回退到AkShare获取ETF {etf_code} 小时级数据...")
                            df = ak.fund_etf_hist_em(symbol=etf_code, period="60")
                        except Exception as ak_e:
                            logger.warning(f"AkShare获取ETF {etf_code} 小时级数据也失败: {ak_e}")
                            logger.warning(f"小时级数据获取失败，使用日级数据替代: {etf_code}")
                            df = ak.fund_etf_hist_em(symbol=etf_code)
                else:
                    # 日级数据
                    try:
                        logger.info(f"尝试使用efinance获取ETF {etf_code} 日级数据...")
                        df = ef.stock.get_quote_history(etf_code, klt=101)  # 日K线
                        # 转换列名为标准格式
                        if not df.empty:
                            df = df.rename(columns={
                                '日期': 'date',
                                '开盘': 'open',
                                '收盘': 'close',
                                '最高': 'high',
                                '最低': 'low',
                                '成交量': 'volume',
                                '成交额': 'amount'
                            })
                            logger.info(f"efinance获取ETF {etf_code} 日级数据成功，数据条数: {len(df)}")
                    except Exception as e:
                        logger.warning(f"efinance获取ETF {etf_code} 日级数据失败: {e}")
                        # 回退到AkShare
                        logger.info(f"回退到AkShare获取ETF {etf_code} 日级数据...")
                        df = ak.fund_etf_hist_em(symbol=etf_code)
                
                if df is None or df.empty:
                    logger.warning(f"ETF {etf_code} 历史数据为空")
                    return None
                
                # 数据预处理
                df = self._preprocess_historical_data(df)
                
                # 取指定数量的数据
                if len(df) > count:
                    df = df.tail(count)
                
                # 缓存数据
                self.cache[cache_key] = {
                    'data': df,
                    'timestamp': time.time()
                }
                
                logger.info(f"ETF {etf_code} 历史数据获取成功，数据条数: {len(df)}")
                return df
                
            except Exception as e:
                logger.error(f"获取ETF {etf_code} 历史数据失败 (尝试 {attempt + 1}): {e}")
                if attempt < self.retry_times - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def get_intraday_data(self, etf_code: str, interval_minutes: int = 3) -> Optional[List[Dict]]:
        """
        获取日内数据（指定间隔）
        
        Args:
            etf_code: ETF代码
            interval_minutes: 时间间隔（分钟）
            
        Returns:
            日内数据列表
        """
        # 获取分钟级数据
        df = self.get_historical_data(etf_code, period="1m", count=240)  # 4小时=240分钟
        
        if df is None or df.empty:
            return None
        
        # 按指定间隔重采样
        resampled_data = []
        
        for i in range(0, len(df), interval_minutes):
            if i + interval_minutes <= len(df):
                chunk = df.iloc[i:i + interval_minutes]
                
                # 计算该时间段的OHLCV
                ohlcv = {
                    'time': chunk.index[0].strftime("%H:%M:%S"),
                    'open': chunk['open'].iloc[0],
                    'high': chunk['high'].max(),
                    'low': chunk['low'].min(),
                    'close': chunk['close'].iloc[-1],
                    'volume': chunk['volume'].sum(),
                    'mid_price': (chunk['high'].max() + chunk['low'].min()) / 2
                }
                resampled_data.append(ohlcv)
        
        return resampled_data
    
    def get_multiple_etf_data(self, etf_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个ETF数据
        
        Args:
            etf_codes: ETF代码列表
            
        Returns:
            ETF数据字典
        """
        results = {}
        
        for code in etf_codes:
            try:
                data = self.get_real_time_data(code)
                if data:
                    results[code] = data
                else:
                    logger.warning(f"无法获取ETF {code} 的数据")
                
                # 避免请求过于频繁
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"获取ETF {code} 数据时发生错误: {e}")
        
        return results
    
    def _preprocess_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理历史数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            预处理后的DataFrame
        """
        try:
            # 检查是否已经标准化了列名
            if 'date' not in df.columns:
                # 标准化列名（针对AkShare数据）
                column_mapping = {
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount'
                }
                
                # 重命名列
                df = df.rename(columns=column_mapping)
            
            # 确保数据类型正确
            numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 设置日期索引
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            
            # 删除空值
            df = df.dropna()
            
            # 按日期排序
            df = df.sort_index()
            
            return df
            
        except Exception as e:
            logger.error(f"历史数据预处理失败: {e}")
            return df
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """
        检查缓存是否有效
        
        Args:
            cache_key: 缓存键
            
        Returns:
            缓存是否有效
        """
        if cache_key not in self.cache:
            return False
        
        cache_time = self.cache[cache_key]['timestamp']
        current_time = time.time()
        
        return (current_time - cache_time) < self.cache_duration
    
    def clear_cache(self) -> None:
        """清空缓存"""
        self.cache.clear()
        logger.info("数据缓存已清空")
    
    def save_cache_to_file(self, cache_dir: str = "data/market_data") -> None:
        """
        将缓存数据保存到文件
        
        Args:
            cache_dir: 缓存目录
        """
        try:
            os.makedirs(cache_dir, exist_ok=True)
            
            for cache_key, cache_data in self.cache.items():
                filename = f"{cache_key}_{int(cache_data['timestamp'])}.csv"
                filepath = os.path.join(cache_dir, filename)
                
                if isinstance(cache_data['data'], pd.DataFrame):
                    cache_data['data'].to_csv(filepath)
                elif isinstance(cache_data['data'], dict):
                    # 将字典转换为DataFrame保存
                    df = pd.DataFrame([cache_data['data']])
                    df.to_csv(filepath, index=False)
            
            logger.info(f"缓存数据已保存到: {cache_dir}")
            
        except Exception as e:
            logger.error(f"保存缓存数据失败: {e}")