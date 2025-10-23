#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
新浪财经ETF数据爬取模块
提供实时行情、五档买卖盘数据获取功能
"""

import requests
import re
from typing import Dict, List, Optional
from loguru import logger
import time


def get_market_prefix(fund_code: str) -> str:
    """
    根据基金代码判断交易所前缀
    
    Args:
        fund_code: 基金代码
        
    Returns:
        交易所前缀 ('sz' 或 'sh')
    """
    if fund_code.startswith('15') or fund_code.startswith('16'):
        return 'sz'
    elif fund_code.startswith('51') or fund_code.startswith('56') or fund_code.startswith('58'):
        return 'sh'
    else:
        # 默认深交所
        return 'sz'


class SinaETFCrawler:
    """新浪财经ETF数据爬取器"""
    
    def __init__(self):
        """初始化爬取器"""
        self.session = requests.Session()
        # 设置请求头，避免被拒绝
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://finance.sina.com.cn/',
        })
        logger.info("新浪财经ETF爬取器初始化完成")
    
    def _fetch_raw_data(self, fund_code: str) -> Optional[str]:
        """
        获取ETF原始数据
        
        Args:
            fund_code: ETF代码
            
        Returns:
            原始数据字符串
        """
        try:
            market = get_market_prefix(fund_code)
            url = f"https://hq.sinajs.cn/list={market}{fund_code}"
            
            logger.info(f"请求 {fund_code} 原始数据: {url}")
            resp = self.session.get(url, timeout=8)
            resp.raise_for_status()
            text = resp.text.strip()

            # 检查是否返回有效数据
            if not text.startswith('var hq_str_') or '="",' in text or text.endswith('="";'):
                logger.warning(f"{fund_code} 返回空数据或无效数据")
                return None

            return text
        except Exception as e:
            logger.error(f"获取 {fund_code} 原始数据失败: {e}")
            return None
    
    def _parse_raw_data(self, fund_code: str, text: str) -> Optional[Dict]:
        """
        解析ETF原始数据
        
        Args:
            fund_code: ETF代码
            text: 原始数据文本
            
        Returns:
            解析后的数据字典
        """
        try:
            # 提取引号内的数据部分
            data_str = text.split('"')[1]
            if not data_str or data_str == '':
                logger.warning(f"{fund_code} 数据为空字符串")
                return None

            fields = data_str.split(',')
            if len(fields) < 33:
                logger.warning(f"{fund_code} 字段数量不足（{len(fields)} < 33）")
                return None

            # 解析基础数据
            name = fields[0]
            open_price = float(fields[1]) if fields[1] else 0.0
            prev_close = float(fields[2]) if fields[2] else 0.0
            current_price = float(fields[3]) if fields[3] else 0.0
            high_price = float(fields[4]) if fields[4] else 0.0
            low_price = float(fields[5]) if fields[5] else 0.0
            
            # 根据调试结果，修正成交量和成交额的索引
            # 成交量在索引8，成交额在索引9
            volume = int(float(fields[8])) if len(fields) > 8 and fields[8] else 0
            amount = float(fields[9]) if len(fields) > 9 and fields[9] else 0.0

            # 计算涨跌幅
            if prev_close > 0:
                change_pct = (current_price - prev_close) / prev_close * 100
                change_amount = current_price - prev_close
            else:
                change_pct = 0.0
                change_amount = 0.0

            # 解析时间 - 根据调试结果修正索引
            date_str = fields[30] if len(fields) > 30 else ''
            time_str = fields[31] if len(fields) > 31 else ''
            update_time = f"{date_str} {time_str}" if date_str and time_str else ''

            # 解析五档买卖盘数据
            # === 五档买盘：量在偶数索引(10,12,14,16,18)，价在奇数索引(11,13,15,17,19) ===
            bids = []
            for i in range(5):
                vol_idx = 10 + i * 2      # 10,12,14,16,18
                price_idx = vol_idx + 1   # 11,13,15,17,19
                try:
                    vol = int(float(fields[vol_idx]))
                    price = float(fields[price_idx])
                    if vol > 0 and price > 0 and price < 1000:
                        bids.append({'price': price, 'vol': vol})
                except (ValueError, IndexError):
                    continue

            # === 五档卖盘：量在偶数索引(20,22,24,26,28)，价在奇数索引(21,23,25,27,29) ===
            asks = []
            for i in range(5):
                vol_idx = 20 + i * 2      # 20,22,24,26,28
                price_idx = vol_idx + 1   # 21,23,25,27,29
                try:
                    vol = int(float(fields[vol_idx]))
                    price = float(fields[price_idx])
                    if vol > 0 and price > 0 and price < 1000:
                        asks.append({'price': price, 'vol': vol})
                except (ValueError, IndexError):
                    continue

            market = get_market_prefix(fund_code)

            return {
                'code': fund_code,
                'name': name,
                'current_price': current_price,
                'open_price': open_price,
                'high_price': high_price,
                'low_price': low_price,
                'prev_close': prev_close,
                'change_amount': change_amount,
                'change_pct': round(change_pct, 2),
                'volume': volume,
                'amount': amount,
                'update_time': update_time,
                'market': market.upper(),
                'bid': bids,
                'ask': asks
            }
        except Exception as e:
            logger.error(f"解析 {fund_code} 数据失败: {e}")
            return None

    def get_real_time_quote(self, fund_code: str) -> Optional[Dict]:
        """
        获取ETF实时行情数据
        
        Args:
            fund_code: ETF代码
            
        Returns:
            实时行情数据字典
        """
        try:
            text = self._fetch_raw_data(fund_code)
            if not text:
                return None
                
            data = self._parse_raw_data(fund_code, text)
            if not data:
                return None

            # 提取实时行情所需的数据
            result = {
                'code': data['code'],
                'name': data['name'],
                'current_price': data['current_price'],
                'open_price': data['open_price'],
                'high_price': data['high_price'],
                'low_price': data['low_price'],
                'prev_close': data['prev_close'],
                'change_amount': data['change_amount'],
                'change_pct': data['change_pct'],
                'volume': data['volume'],
                'amount': data['amount'],
                'update_time': data['update_time'],
                'market': data['market']
            }

            logger.info(f"✅ {fund_code} | {data['name']} | 当前价: {data['current_price']} | 涨跌幅: {data['change_pct']:.2f}%")
            return result

        except Exception as e:
            logger.error(f"获取 {fund_code} 实时行情失败: {e}")
            return None
    
    def get_complete_data(self, fund_code: str) -> Optional[Dict]:
        """
        获取ETF完整数据（包括实时行情和五档买卖盘）
        
        Args:
            fund_code: ETF代码
            
        Returns:
            完整数据字典
        """
        try:
            text = self._fetch_raw_data(fund_code)
            if not text:
                return None
                
            data = self._parse_raw_data(fund_code, text)
            if not data:
                return None

            # 返回完整数据
            result = {
                'code': data['code'],
                'name': data['name'],
                'current_price': data['current_price'],
                'open_price': data['open_price'],
                'high_price': data['high_price'],
                'low_price': data['low_price'],
                'prev_close': data['prev_close'],
                'change_amount': data['change_amount'],
                'change_pct': data['change_pct'],
                'volume': data['volume'],
                'amount': data['amount'],
                'update_time': data['update_time'],
                'market': data['market'],
                'bid': data['bid'],
                'ask': data['ask']
            }

            bid1 = data['bid'][0] if data['bid'] else 'N/A'
            ask1 = data['ask'][0] if data['ask'] else 'N/A'
            logger.info(f"✅ {fund_code} | {data['name']} | 当前价: {data['current_price']} | 涨跌幅: {data['change_pct']:.2f}% | 买一: {bid1} | 卖一: {ask1}")
            return result

        except Exception as e:
            logger.error(f"获取 {fund_code} 完整数据失败: {e}")
            return None
    
    def get_order_book(self, fund_code: str) -> Optional[Dict]:
        """
        获取ETF五档买卖盘数据（按标准33字段格式解析）
        
        Args:
            fund_code: ETF代码
            
        Returns:
            五档买卖盘数据字典
        """
        try:
            text = self._fetch_raw_data(fund_code)
            if not text:
                return None
                
            data = self._parse_raw_data(fund_code, text)
            if not data:
                return None

            # 提取买卖盘所需的数据
            result = {
                'code': data['code'],
                'name': data['name'],
                'price': data['current_price'],
                'open': data['open_price'],
                'prev_close': data['prev_close'],
                'change_pct': data['change_pct'],
                'bid': data['bid'],
                'ask': data['ask'],
                'update_time': data['update_time']
            }

            bid1 = data['bid'][0] if data['bid'] else 'N/A'
            ask1 = data['ask'][0] if data['ask'] else 'N/A'
            logger.info(f"✅ {fund_code} 买卖盘 | 买一: {bid1} | 卖一: {ask1}")
            return result

        except Exception as e:
            logger.error(f"获取 {fund_code} 买卖盘失败: {e}")
            return None
    
    def get_multiple_quotes(self, fund_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个ETF的实时行情
        
        Args:
            fund_codes: ETF代码列表
            
        Returns:
            实时行情数据字典
        """
        results = {}
        
        for code in fund_codes:
            try:
                data = self.get_real_time_quote(code)
                if data:
                    results[code] = data
                # 添加延时避免请求过快
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"批量获取 {code} 行情失败: {e}")
                
        return results
    
    def get_multiple_complete_data(self, fund_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个ETF的完整数据（包括实时行情和五档买卖盘）
        
        Args:
            fund_codes: ETF代码列表
            
        Returns:
            完整数据字典
        """
        results = {}
        
        for code in fund_codes:
            try:
                data = self.get_complete_data(code)
                if data:
                    results[code] = data
                # 添加延时避免请求过快
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"批量获取 {code} 完整数据失败: {e}")
                
        return results
    
    def get_multiple_order_books(self, fund_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取多个ETF的五档买卖盘
        
        Args:
            fund_codes: ETF代码列表
            
        Returns:
            买卖盘数据字典
        """
        results = {}
        
        for code in fund_codes:
            try:
                data = self.get_order_book(code)
                if data:
                    results[code] = data
                # 添加延时避免请求过快
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"批量获取 {code} 买卖盘失败: {e}")
                
        return results


# 测试代码
if __name__ == '__main__':
    # 测试ETF代码
    TEST_ETFS = ['159599', '159637', '159770', '512010', '512710', '515980', '518880']
    
    crawler = SinaETFCrawler()
    
    print("=== 测试实时行情 ===")
    quotes = crawler.get_multiple_quotes(TEST_ETFS)
    for code, data in quotes.items():
        print(f"{code}: {data['name']} - {data['current_price']} ({data['change_pct']:.2f}%)")
    
    print("\n=== 测试五档买卖盘 ===")
    order_books = crawler.get_multiple_order_books(TEST_ETFS)
    for code, data in order_books.items():
        bid1 = data['bid'][0] if data['bid'] else 'N/A'
        ask1 = data['ask'][0] if data['ask'] else 'N/A'
        print(f"{code}: 买一 {bid1} | 卖一 {ask1}")
    
    print("\n=== 测试完整数据 ===")
    complete_data = crawler.get_multiple_complete_data(TEST_ETFS[:3])  # 只测试前3个
    for code, data in complete_data.items():
        bid1 = data['bid'][0] if data['bid'] else 'N/A'
        ask1 = data['ask'][0] if data['ask'] else 'N/A'
        print(f"{code}: {data['name']} - {data['current_price']} ({data['change_pct']:.2f}%) | 买一 {bid1} | 卖一 {ask1}")