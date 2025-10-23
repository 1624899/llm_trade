"""
行情数据获取模块
使用akshare、efinance和新浪财经爬取获取A股ETF实时和历史数据
"""

import efinance as ef
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from loguru import logger
import time
import os
import concurrent.futures
import threading
from .utils import validate_etf_code, get_current_time_str
from .sina_crawler import SinaETFCrawler


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
        self.realtime_cache_duration = self.data_config.get('realtime_cache_duration', 60)  # 实时数据缓存1分钟
        self.retry_times = self.data_config.get('retry_times', 3)
        self.timeout = self.data_config.get('timeout', 30)
        self.max_workers = self.data_config.get('max_workers', 5)  # 并发线程数
        
        # 数据缓存
        self.cache = {}
        self.cache_lock = threading.Lock()  # 线程安全锁
        
        # 数据有效性检查参数
        self.price_change_limit = 0.2  # 价格变动限制20%
        self.volume_change_limit = 10.0  # 成交量变动限制10倍
        
        # 初始化新浪财经爬取器
        self.sina_crawler = SinaETFCrawler()
        
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
        
        # 检查缓存（使用较短的实时数据缓存时间）
        cache_key = f"realtime_{etf_code}"
        if self._is_cache_valid(cache_key, realtime=True):
            logger.info(f"使用缓存数据: {etf_code}")
            return self.cache[cache_key]['data']
        
        for attempt in range(self.retry_times):
            try:
                logger.info(f"获取ETF实时数据: {etf_code}, 尝试次数: {attempt + 1}")
                
                # 使用efinance获取ETF的最新日线数据作为实时数据
                etf_data = ef.stock.get_quote_history(etf_code, klt=101)  # 日K线
                
                if etf_data.empty:
                    logger.warning(f"ETF {etf_code} 实时数据为空")
                    return None
                
                # 提取最新的数据（今天的数据）
                latest_data = etf_data.iloc[-1]
                
                # 构造返回数据
                result = {
                    'code': etf_code,
                    'name': latest_data.get('股票名称', ''),
                    'current_price': float(latest_data.get('收盘', 0)),  # 使用收盘价作为当前价格
                    'open_price': float(latest_data.get('开盘', 0)),
                    'high_price': float(latest_data.get('最高', 0)),
                    'low_price': float(latest_data.get('最低', 0)),
                    'prev_close': float(latest_data.get('收盘', 0) - latest_data.get('涨跌额', 0)),  # 计算昨收价
                    'volume': int(latest_data.get('成交量', 0)),
                    'amount': float(latest_data.get('成交额', 0)),
                    'change_pct': float(latest_data.get('涨跌幅', 0)),
                    'update_time': get_current_time_str()
                }
                
                # 数据有效性检查
                if not self._validate_realtime_data(result):
                    logger.warning(f"ETF {etf_code} 实时数据未通过有效性检查")
                    return None
                
                # 缓存数据（使用线程安全的方式）
                with self.cache_lock:
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
                if df is None or df.empty:
                    logger.warning(f"ETF {etf_code} 历史数据为空")
                    return None
                
                # 数据预处理
                df = self._preprocess_historical_data(df)
                
                # 取指定数量的数据
                if len(df) > count:
                    df = df.tail(count)
                
                # 数据有效性检查
                if not self._validate_historical_data(df):
                    logger.warning(f"ETF {etf_code} 历史数据未通过有效性检查")
                    return None
                
                # 缓存数据（使用线程安全的方式）
                with self.cache_lock:
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
    
    def get_minute_tick_data(self, etf_code: str, period: str = "1") -> Optional[pd.DataFrame]:
        """
        获取ETF分钟级TICK数据（使用新浪财经爬取）
        
        Args:
            etf_code: ETF代码
            period: 时间周期 (1=1分钟, 5=5分钟)
            
        Returns:
            分钟级TICK数据DataFrame
        """
        try:
            logger.info(f"获取ETF分钟级TICK数据: {etf_code}, 周期: {period}分钟")
            
            # 检查缓存
            cache_key = f"tick_{etf_code}_{period}"
            if self._is_cache_valid(cache_key, realtime=True):
                logger.info(f"使用缓存TICK数据: {etf_code}")
                return self.cache[cache_key]['data']
            
            # 使用新浪财经爬取器获取实时行情数据
            quote_data = self.sina_crawler.get_real_time_quote(etf_code)
            
            if quote_data is None:
                logger.warning(f"ETF {etf_code} 实时数据为空")
                return None
            
            # 将实时数据转换为DataFrame格式
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')  # 使用当前时间
            tick_data = pd.DataFrame([{
                'time': current_time,
                'price': quote_data['current_price'],
                'volume': quote_data['volume'],
                'amount': quote_data['amount'],
                'change_pct': quote_data['change_pct']
            }])
            
            # 设置时间索引
            if 'time' in tick_data.columns:
                tick_data['time'] = pd.to_datetime(tick_data['time'])
                tick_data.set_index('time', inplace=True)
            
            # 缓存数据
            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': tick_data,
                    'timestamp': time.time()
                }
            
            logger.info(f"ETF {etf_code} 分钟级TICK数据获取成功，数据条数: {len(tick_data)}")
            return tick_data
            
        except Exception as e:
            logger.error(f"获取ETF {etf_code} 分钟级TICK数据失败: {e}")
            return None
    
    def get_order_book_data(self, etf_code: str) -> Optional[Dict]:
        """
        获取ETF买卖盘口数据（五档行情，使用新浪财经爬取）
        
        Args:
            etf_code: ETF代码
            
        Returns:
            买卖盘口数据字典
        """
        try:
            logger.info(f"获取ETF买卖盘口数据: {etf_code}")
            
            # 检查缓存
            cache_key = f"orderbook_{etf_code}"
            if self._is_cache_valid(cache_key, realtime=True):
                logger.info(f"使用缓存买卖盘数据: {etf_code}")
                return self.cache[cache_key]['data']
            
            # 使用新浪财经爬取器获取五档买卖盘数据
            order_book_data = self.sina_crawler.get_order_book(etf_code)
            
            if order_book_data is None:
                logger.warning(f"ETF {etf_code} 买卖盘口数据为空")
                return None
            
            # 转换为标准格式
            result = {
                'code': order_book_data['code'],
                'name': order_book_data['name'],
                'current_price': order_book_data['price'],
                'change_pct': order_book_data['change_pct'],
                'bid_prices': [item['price'] for item in order_book_data['bid']],
                'bid_volumes': [item['vol'] for item in order_book_data['bid']],
                'ask_prices': [item['price'] for item in order_book_data['ask']],
                'ask_volumes': [item['vol'] for item in order_book_data['ask']],
                'bid_levels': order_book_data['bid'],  # 保留原始格式
                'ask_levels': order_book_data['ask'],  # 保留原始格式
                'update_time': order_book_data['update_time']
            }
            
            # 缓存数据
            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': result,
                    'timestamp': time.time()
                }
            
            logger.info(f"ETF {etf_code} 买卖盘口数据获取成功")
            return result
            
        except Exception as e:
            logger.error(f"获取ETF {etf_code} 买卖盘口数据失败: {e}")
            return None
    
    def get_fund_flow_data(self, etf_code: str) -> Optional[Dict]:
        """
        获取ETF资金流向数据（大单流向分析）
        
        Args:
            etf_code: ETF代码
            
        Returns:
            资金流向数据字典
        """
        try:
            logger.info(f"获取ETF资金流向数据: {etf_code}")
            
            # 使用akshare获取个股资金流向数据
            try:
                df = ak.stock_individual_fund_flow(stock=etf_code, market="sh" if etf_code.startswith(("5", "6")) else "sz")
            except Exception as e:
                logger.warning(f"资金流向数据获取失败: {e}")
                return None
            
            if df is None or df.empty:
                logger.warning(f"ETF {etf_code} 资金流向数据为空")
                return None
            
            # 获取最新一天的数据
            if len(df) > 0:
                latest_data = df.iloc[-1]
                fund_flow = {
                    'date': latest_data.get('日期', ''),
                    'close_price': float(latest_data.get('收盘价', 0)),
                    'change_pct': float(latest_data.get('涨跌幅', 0)),
                    'main_net_inflow': float(latest_data.get('主力净流入-净额', 0)),
                    'main_net_inflow_ratio': float(latest_data.get('主力净流入-净占比', 0)),
                    'super_large_net_inflow': float(latest_data.get('超大单净流入-净额', 0)),
                    'super_large_net_inflow_ratio': float(latest_data.get('超大单净流入-净占比', 0)),
                    'large_net_inflow': float(latest_data.get('大单净流入-净额', 0)),
                    'large_net_inflow_ratio': float(latest_data.get('大单净流入-净占比', 0)),
                    'medium_net_inflow': float(latest_data.get('中单净流入-净额', 0)),
                    'medium_net_inflow_ratio': float(latest_data.get('中单净流入-净占比', 0)),
                    'small_net_inflow': float(latest_data.get('小单净流入-净额', 0)),
                    'small_net_inflow_ratio': float(latest_data.get('小单净流入-净占比', 0))
                }
                
                logger.info(f"ETF {etf_code} 资金流向数据获取成功")
                return fund_flow
            
            logger.warning(f"ETF {etf_code} 资金流向数据为空")
            return None
            
        except Exception as e:
            logger.error(f"获取ETF {etf_code} 资金流向数据失败: {e}")
            return None
    
    def get_market_sentiment_data(self) -> Optional[List[Dict]]:
        """
        获取市场情绪指标（行业资金流向）
        
        Returns:
            行业资金流向数据列表
        """
        try:
            logger.info("获取市场情绪指标数据")
            
            # 使用akshare获取行业资金流向数据
            df = ak.stock_fund_flow_industry()
            
            if df is None or df.empty:
                logger.warning("行业资金流向数据为空")
                return None
            
            # 记录原始数据列名，用于调试
            logger.info(f"行业资金流向数据列名: {list(df.columns)}")
            
            # 转换为列表格式，并修正单位问题
            sentiment_data = []
            for _, row in df.iterrows():
                # 获取原始资金数据（单位可能是万元或亿元）
                inflow_raw = float(row.get('流入资金', 0))
                outflow_raw = float(row.get('流出资金', 0))
                net_amount_raw = float(row.get('净额', 0))
                
                # 记录原始数据用于调试
                if row.get('序号') == 1:  # 只记录第一条数据的调试信息
                    logger.info(f"行业资金流向原始数据示例 - 流入: {inflow_raw}, 流出: {outflow_raw}, 净额: {net_amount_raw}")
                
                # 根据数值大小判断单位并转换
                def format_amount(amount: float) -> Tuple[float, str]:
                    """格式化金额，返回数值和单位"""
                    if abs(amount) >= 100000000:  # 1亿以上
                        return amount / 100000000, "亿元"
                    elif abs(amount) >= 10000:  # 1万以上
                        return amount / 10000, "万元"
                    else:
                        return amount, "元"
                
                inflow_value, inflow_unit = format_amount(inflow_raw)
                outflow_value, outflow_unit = format_amount(outflow_raw)
                net_value, net_unit = format_amount(net_amount_raw)
                
                # 统一使用最大的单位
                units = [inflow_unit, outflow_unit, net_unit]
                if "亿元" in units:
                    final_unit = "亿元"
                    inflow_formatted = inflow_raw / 100000000
                    outflow_formatted = outflow_raw / 100000000
                    net_formatted = net_amount_raw / 100000000
                elif "万元" in units:
                    final_unit = "万元"
                    inflow_formatted = inflow_raw / 10000
                    outflow_formatted = outflow_raw / 10000
                    net_formatted = net_amount_raw / 10000
                else:
                    final_unit = "元"
                    inflow_formatted = inflow_raw
                    outflow_formatted = outflow_raw
                    net_formatted = net_amount_raw
                
                industry_data = {
                    'rank': int(row.get('序号', 0)),
                    'industry': row.get('行业', ''),
                    'index': float(row.get('行业指数', 0)),
                    'change_pct': float(row.get('行业-涨跌幅', 0)),
                    'inflow': inflow_formatted,
                    'outflow': outflow_formatted,
                    'net_amount': net_formatted,
                    'unit': final_unit,  # 添加单位字段
                    'inflow_raw': inflow_raw,  # 保留原始数据
                    'outflow_raw': outflow_raw,
                    'net_amount_raw': net_amount_raw,
                    'company_count': int(row.get('公司家数', 0)),
                    'leading_stock': row.get('领涨股', ''),
                    'leading_stock_change': float(row.get('领涨股-涨跌幅', 0)),
                    'current_price': float(row.get('当前价', 0))
                }
                sentiment_data.append(industry_data)
            
            logger.info(f"市场情绪指标数据获取成功，共{len(sentiment_data)}个行业")
            return sentiment_data
            
        except Exception as e:
            logger.error(f"获取市场情绪指标数据失败: {e}")
            return None
    
    def get_multiple_etf_data(self, etf_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个ETF数据（使用新浪财经爬取，并发版本）
        
        Args:
            etf_codes: ETF代码列表
            
        Returns:
            ETF数据字典
        """
        results = {}
        
        try:
            logger.info(f"批量获取ETF实时数据，共{len(etf_codes)}只")
            
            # 优先使用新浪财经爬取器批量获取
            sina_results = self.sina_crawler.get_multiple_quotes(etf_codes)
            
            # 将新浪财经数据转换为标准格式
            for code, data in sina_results.items():
                try:
                    standard_data = {
                        'code': data['code'],
                        'name': data['name'],
                        'current_price': data['current_price'],
                        'open_price': data['open_price'],
                        'high_price': data['high_price'],
                        'low_price': data['low_price'],
                        'prev_close': data['prev_close'],
                        'change_amount': data.get('change_amount', 0.0),
                        'change_pct': data['change_pct'],
                        'volume': data['volume'],
                        'amount': data['amount'],
                        'update_time': data['update_time'],
                        'market': data.get('market', 'UNKNOWN')
                    }
                    results[code] = standard_data
                    logger.info(f"成功获取ETF {code} 数据（新浪财经）")
                except Exception as e:
                    logger.error(f"转换新浪财经数据失败 {code}: {e}")
            
            # 对于获取失败的ETF，尝试使用原有方法
            failed_codes = [code for code in etf_codes if code not in results]
            if failed_codes:
                logger.info(f"尝试使用原有方法获取失败的ETF数据: {failed_codes}")
                
                def fetch_single_etf(code: str) -> Tuple[str, Optional[Dict]]:
                    """获取单个ETF数据的内部函数"""
                    try:
                        data = self.get_real_time_data(code)
                        return code, data
                    except Exception as e:
                        logger.error(f"获取ETF {code} 数据时发生错误: {e}")
                        return code, None
                
                # 使用线程池并发获取数据
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(failed_codes))) as executor:
                    # 提交所有任务
                    future_to_code = {executor.submit(fetch_single_etf, code): code for code in failed_codes}
                    
                    # 收集结果
                    for future in concurrent.futures.as_completed(future_to_code):
                        code, data = future.result()
                        if data:
                            results[code] = data
                            logger.info(f"成功获取ETF {code} 数据（原有方法）")
                        else:
                            logger.warning(f"无法获取ETF {code} 的数据")
            
        except Exception as e:
            logger.error(f"批量获取ETF数据失败: {e}")
        
        return results
    
    def get_multiple_etf_advanced_data(self, etf_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个ETF的增强数据（包括实时数据、盘口数据、资金流向等，使用新浪财经爬取）
        
        Args:
            etf_codes: ETF代码列表 或 ETF信息字典列表
            
        Returns:
            ETF增强数据字典
        """
        results = {}
        
        def fetch_etf_advanced_data(etf_input) -> Tuple[str, Dict]:
            """获取单个ETF增强数据的内部函数"""
            # 处理不同格式的输入
            if isinstance(etf_input, str):
                etf_code = etf_input
                etf_name = ""
                etf_category = ""
            else:
                etf_code = etf_input['code']
                etf_name = etf_input.get('name', '')
                etf_category = etf_input.get('category', '')
            
            try:
                logger.info(f"获取ETF {etf_code} 增强数据")
                
                # 使用新浪财经爬取器获取完整数据（包含实时数据和买卖盘）
                complete_data = self.sina_crawler.get_complete_data(etf_code)
                
                # 从完整数据中提取实时数据和买卖盘数据
                real_time_data = {
                    'code': complete_data['code'],
                    'name': complete_data['name'],
                    'current_price': complete_data['current_price'],
                    'open_price': complete_data['open_price'],
                    'high_price': complete_data['high_price'],
                    'low_price': complete_data['low_price'],
                    'prev_close': complete_data['prev_close'],
                    'change_amount': complete_data['change_amount'],
                    'change_pct': complete_data['change_pct'],
                    'volume': complete_data['volume'],
                    'amount': complete_data['amount'],
                    'update_time': complete_data['update_time'],
                    'market': complete_data['market']
                } if complete_data else None
                
                order_book_data = {
                    'code': complete_data['code'],
                    'name': complete_data['name'],
                    'price': complete_data['current_price'],
                    'open': complete_data['open_price'],
                    'prev_close': complete_data['prev_close'],
                    'change_pct': complete_data['change_pct'],
                    'bid': complete_data['bid'],
                    'ask': complete_data['ask'],
                    'update_time': complete_data['update_time']
                } if complete_data else None
                
                # 获取其他数据（使用原有方法）
                historical_data = self.get_historical_data(etf_code, period="1d", count=100)
                fund_flow_data = self.get_fund_flow_data(etf_code)
                minute_tick_data = self.get_minute_tick_data(etf_code, period="1")
                
                result = {
                    'code': etf_code,
                    'name': real_time_data.get('name', etf_name) if real_time_data else etf_name,
                    'category': etf_category,
                    'real_time_data': real_time_data,
                    'historical_data': historical_data,
                    'order_book_data': order_book_data,
                    'fund_flow_data': fund_flow_data,
                    'minute_tick_data': minute_tick_data,
                    'data_source': 'sina_finance' if real_time_data else 'fallback'
                }
                
                return etf_code, result
                
            except Exception as e:
                logger.error(f"获取ETF {etf_code} 增强数据时发生错误: {e}")
                return etf_code, {}
        
        # 使用线程池并发获取数据
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(etf_codes))) as executor:
            # 提交所有任务
            future_to_etf = {executor.submit(fetch_etf_advanced_data, etf_input): etf_input for etf_input in etf_codes}
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_etf):
                etf_code, data = future.result()
                if data:
                    results[etf_code] = data
                    logger.info(f"成功获取ETF {etf_code} 增强数据")
                else:
                    logger.warning(f"无法获取ETF {etf_code} 的增强数据")
        
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
    
    def _is_cache_valid(self, cache_key: str, realtime: bool = False) -> bool:
        """
        检查缓存是否有效
        
        Args:
            cache_key: 缓存键
            realtime: 是否为实时数据
            
        Returns:
            缓存是否有效
        """
        with self.cache_lock:
            if cache_key not in self.cache:
                return False
            
            cache_time = self.cache[cache_key]['timestamp']
            current_time = time.time()
            
            # 根据数据类型使用不同的缓存时间
            duration = self.realtime_cache_duration if realtime else self.cache_duration
            
            return (current_time - cache_time) < duration
    
    def _determine_market(self, etf_code: str) -> str:
        """
        改进的交易所判断逻辑
        
        Args:
            etf_code: ETF代码
            
        Returns:
            交易所代码 ('sh' 或 'sz')
        """
        # 常见的ETF代码规则
        if etf_code.startswith('5'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('15'):
            # 深圳交易所ETF
            return 'sz'
        elif etf_code.startswith('51'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('56'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('159'):
            # 深圳交易所ETF
            return 'sz'
        elif etf_code.startswith('512'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('513'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('515'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('516'):
            # 上海交易所ETF
            return 'sh'
        elif etf_code.startswith('518'):
            # 上海交易所ETF
            return 'sh'
        else:
            # 默认为深圳交易所
            logger.warning(f"未知的ETF代码格式: {etf_code}，默认使用深圳交易所")
            return 'sz'
    
    def _validate_realtime_data(self, data: Dict) -> bool:
        """
        验证实时数据的有效性
        
        Args:
            data: 实时数据字典
            
        Returns:
            数据是否有效
        """
        try:
            if not data:
                return False
            
            # 检查必需字段
            required_fields = ['current_price', 'open_price', 'high_price', 'low_price', 'volume']
            for field in required_fields:
                if field not in data or data[field] is None:
                    logger.warning(f"实时数据缺少必需字段: {field}")
                    return False
            
            # 检查价格合理性
            current_price = data['current_price']
            open_price = data['open_price']
            high_price = data['high_price']
            low_price = data['low_price']
            
            # 价格必须为正数
            if any(price <= 0 for price in [current_price, open_price, high_price, low_price]):
                logger.warning("实时数据中存在非正价格")
                return False
            
            # 高低价关系检查
            if not (low_price <= current_price <= high_price):
                logger.warning(f"价格关系异常: 低价{low_price}, 当前价{current_price}, 高价{high_price}")
                return False
            
            # 价格变动幅度检查
            if open_price > 0:
                price_change = abs(current_price - open_price) / open_price
                if price_change > self.price_change_limit:
                    logger.warning(f"价格变动过大: {price_change:.2%}")
                    return False
            
            # 成交量检查
            volume = data['volume']
            if volume < 0:
                logger.warning("成交量为负数")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"实时数据验证失败: {e}")
            return False
    
    def _validate_historical_data(self, df: pd.DataFrame) -> bool:
        """
        验证历史数据的有效性
        
        Args:
            df: 历史数据DataFrame
            
        Returns:
            数据是否有效
        """
        try:
            if df is None or df.empty:
                return False
            
            # 检查必需列
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                if col not in df.columns:
                    logger.warning(f"历史数据缺少必需列: {col}")
                    return False
            
            # 检查数据完整性
            if df.isnull().all().any():
                logger.warning("历史数据中存在全为空值的列")
                return False
            
            # 检查价格合理性
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if (df[col] <= 0).any():
                    logger.warning(f"历史数据{col}中存在非正价格")
                    return False
            
            # 检查价格关系
            invalid_prices = (df['low'] > df['high']) | (df['close'] > df['high']) | (df['close'] < df['low'])
            if invalid_prices.any():
                logger.warning("历史数据中存在价格关系异常")
                return False
            
            # 检查成交量
            if (df['volume'] < 0).any():
                logger.warning("历史数据中存在负成交量")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"历史数据验证失败: {e}")
            return False
    
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
            
            # 按缓存键的类型分组，以便找到最新的数据进行保存
            cache_groups = {}
            for cache_key, cache_data in self.cache.items():
                # 提取基础键（不包含时间戳的部分）
                if cache_key.startswith('realtime_'):
                    base_key = cache_key
                elif cache_key.startswith('hist_'):
                    # 对于历史数据，提取ETF代码、周期和数量作为基础键
                    parts = cache_key.split('_')
                    if len(parts) >= 4:  # hist_code_period_count
                        base_key = '_'.join(parts[:4])  # 只取前4部分作为基础键
                    else:
                        base_key = cache_key
                else:
                    base_key = cache_key
                
                if base_key not in cache_groups:
                    cache_groups[base_key] = []
                cache_groups[base_key].append((cache_key, cache_data))
            
            # 对每个组，只保存时间戳最新的数据
            for base_key, cache_list in cache_groups.items():
                # 按时间戳排序，获取最新的数据
                latest_cache = max(cache_list, key=lambda x: x[1]['timestamp'])
                latest_cache_key, latest_cache_data = latest_cache
                
                # 生成文件名，使用最新的时间戳
                filename = f"{latest_cache_key}_{int(latest_cache_data['timestamp'])}.csv"
                filepath = os.path.join(cache_dir, filename)
                
                # 删除同类型的历史文件（避免积累过多文件）
                for existing_file in os.listdir(cache_dir):
                    if existing_file.startswith(f"{base_key}_") and existing_file.endswith('.csv'):
                        existing_filepath = os.path.join(cache_dir, existing_file)
                        if existing_filepath != filepath:  # 不删除当前要保存的文件
                            try:
                                os.remove(existing_filepath)
                                logger.debug(f"删除旧的缓存文件: {existing_filepath}")
                            except Exception as e:
                                logger.warning(f"删除旧缓存文件失败 {existing_filepath}: {e}")
                
                # 保存最新的数据
                if isinstance(latest_cache_data['data'], pd.DataFrame):
                    latest_cache_data['data'].to_csv(filepath)
                elif isinstance(latest_cache_data['data'], dict):
                    # 将字典转换为DataFrame保存
                    df = pd.DataFrame([latest_cache_data['data']])
                    df.to_csv(filepath, index=False)
            
            logger.info(f"缓存数据已保存到: {cache_dir}")
            
        except Exception as e:
            logger.error(f"保存缓存数据失败: {e}")