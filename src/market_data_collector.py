"""
市场数据收集模块
负责收集和处理市场数据
"""

import time
from datetime import datetime
from typing import Dict, Any, List
from loguru import logger


class MarketDataCollector:
    """市场数据收集器类"""
    
    def __init__(self, data_fetcher, indicators, monitored_etfs, cache_lock, price_cache):
        """
        初始化市场数据收集器
        
        Args:
            data_fetcher: 数据获取器
            indicators: 技术指标计算器
            monitored_etfs: 监控的ETF列表
            cache_lock: 缓存锁
            price_cache: 价格缓存
        """
        self.data_fetcher = data_fetcher
        self.indicators = indicators
        self.monitored_etfs = monitored_etfs
        self.cache_lock = cache_lock
        self.price_cache = price_cache
    
    def collect_market_data(self) -> Dict[str, Any]:
        """
        收集市场数据（使用并发优化版本）
        
        Returns:
            市场数据字典
        """
        print("📡 正在并发获取市场数据...")
        
        # 获取市场情绪数据（行业资金流向）
        market_sentiment_data = self.data_fetcher.get_market_sentiment_data()
        
        # 使用并发方式获取所有ETF的基础数据
        etf_codes = [etf_info['code'] for etf_info in self.monitored_etfs]
        basic_data = self.data_fetcher.get_multiple_etf_data(etf_codes)
        
        # 使用并发方式获取所有ETF的增强数据
        enhanced_data = self.data_fetcher.get_multiple_etf_advanced_data(self.monitored_etfs)
        
        market_data = {}
        
        for etf_info in self.monitored_etfs:
            etf_code = etf_info['code']
            etf_name = etf_info['name']
            etf_category = etf_info['category']
            
            try:
                # 检查基础数据
                if etf_code not in basic_data:
                    print(f"⚠️  ETF {etf_code} 基础数据获取失败")
                    continue
                
                # 检查增强数据
                if etf_code not in enhanced_data:
                    print(f"⚠️  ETF {etf_code} 增强数据获取失败")
                    continue
                
                real_time_data = enhanced_data[etf_code]['real_time_data']
                historical_data = enhanced_data[etf_code]['historical_data']
                
                if not real_time_data or (historical_data is None or historical_data.empty):
                    print(f"⚠️  ETF {etf_code} 关键数据缺失")
                    continue
                
                # 计算技术指标
                indicators = self.indicators.calculate_all_indicators(historical_data)
                
                # 获取最新指标值
                current_indicators = self.indicators.get_latest_indicator_values(historical_data, indicators)
                
                # 获取指标序列
                indicator_series = self.indicators.get_indicator_series(indicators, count=10)
                
                # 获取日内数据
                intraday_data = self.data_fetcher.get_intraday_data(etf_code, interval_minutes=3)
                
                # 从增强数据中获取其他信息
                order_book_data = enhanced_data[etf_code]['order_book_data']
                fund_flow_data = enhanced_data[etf_code]['fund_flow_data']
                minute_tick_data = enhanced_data[etf_code]['minute_tick_data']
                
                # 计算增强技术指标
                if not historical_data.empty:
                    close_prices = historical_data['close']
                    # 趋势强度
                    trend_strength = self.indicators.calculate_trend_strength(close_prices)
                    # 支撑阻力位
                    support_resistance = self.indicators.calculate_support_resistance(close_prices)
                    # 波动率
                    volatility = self.indicators.calculate_volatility(close_prices)
                    
                    # 添加到当前指标中
                    current_indicators['trend_strength'] = trend_strength
                    current_indicators['support'] = support_resistance.get('support', 0)
                    current_indicators['resistance'] = support_resistance.get('resistance', 0)
                    current_indicators['volatility'] = volatility
                
                # 计算EMA50（改进之前的简化处理）
                if not historical_data.empty and len(historical_data) >= 50:
                    ema50 = self.indicators.calculate_ema(historical_data['close'], 50)
                    current_indicators['current_ema50'] = float(ema50.iloc[-1]) if not ema50.empty else 0
                
                # 构建ETF数据
                etf_data = {
                    'name': etf_name,
                    'category': etf_category,
                    'current_data': current_indicators,
                    'intraday_data': {
                        'mid_prices': [data.get('mid_price', 0) for data in (intraday_data or [])[-10:]],
                        'ema_series': indicator_series.get('ema_long', [0] * 10),
                        'macd_series': indicator_series.get('macd', [0] * 10),
                        'rsi7_series': indicator_series.get('rsi_7', [0] * 10),
                        'rsi14_series': indicator_series.get('rsi_14', [0] * 10)
                    },
                    'long_term_data': {
                        'ema20': current_indicators.get('current_ema_long', 0),
                        'ema50': current_indicators.get('current_ema50', 0),  # 使用计算得到的EMA50
                        'atr3': current_indicators.get('current_atr_3', 0),
                        'atr14': current_indicators.get('current_atr_14', 0),
                        'current_volume': current_indicators.get('current_volume', 0),
                        'avg_volume': historical_data['volume'].mean() if not historical_data.empty else 0,
                        'macd_series': indicator_series.get('macd', [0] * 10),
                        'rsi_series': indicator_series.get('rsi_14', [0] * 10)
                    },
                    'order_book': order_book_data or {},
                    'fund_flow': fund_flow_data or {},
                    'minute_tick_data': minute_tick_data if minute_tick_data is not None and not minute_tick_data.empty else None
                }
                
                market_data[etf_code] = etf_data
                print(f"✅ ETF {etf_code} 数据处理完成")
                
            except Exception as e:
                print(f"⚠️  处理ETF {etf_code} 数据失败: {e}")
                continue
        
        # 添加市场情绪数据到市场数据中
        if market_sentiment_data:
            market_data['_market_sentiment'] = market_sentiment_data
        
        print(f"✅ 市场数据收集完成，成功处理 {len(market_data)} 个ETF")
        return market_data
    
    def update_price_cache(self) -> None:
        """
        更新价格缓存
        """
        try:
            etf_codes = [etf['code'] for etf in self.monitored_etfs]
            price_data = self.data_fetcher.get_multiple_etf_data(etf_codes)
            
            if price_data:
                with self.cache_lock:
                    # 更新价格缓存，但保留时间戳信息
                    for code, data in price_data.items():
                        if code in self.price_cache:
                            # 保留原有的时间戳，更新价格数据
                            old_timestamp = self.price_cache[code].get('timestamp')
                            self.price_cache[code] = data
                            if old_timestamp:
                                self.price_cache[code]['timestamp'] = old_timestamp
                        else:
                            self.price_cache[code] = data
                
                logger.info(f"价格缓存更新完成，更新了 {len(price_data)} 个ETF数据")
            else:
                logger.warning("价格缓存更新失败，未获取到有效数据")
                
        except Exception as e:
            logger.error(f"更新价格缓存失败: {e}")