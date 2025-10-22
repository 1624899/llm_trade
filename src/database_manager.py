"""
数据库管理模块
负责MySQL数据库连接和数据操作
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger
import os


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, config: Dict):
        """
        初始化数据库管理器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.db_config = config.get('database', {})
        self.connection = None
        self.cursor = None
        
        # 确保market_data目录存在
        os.makedirs("data/market_data", exist_ok=True)
        
        logger.info("数据库管理器初始化完成")
    
    def connect(self) -> bool:
        """
        连接数据库
        
        Returns:
            是否连接成功
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 3306),
                user=self.db_config.get('user', 'root'),
                password=self.db_config.get('password', ''),
                database=self.db_config.get('database', 'market_data_db'),
                charset=self.db_config.get('charset', 'utf8mb4')
            )
            
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                logger.info("MySQL数据库连接成功")
                return True
                
        except Error as e:
            logger.error(f"MySQL数据库连接失败: {e}")
            return False
    
    def disconnect(self) -> None:
        """断开数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logger.info("MySQL数据库连接已断开")
        except Error as e:
            logger.error(f"断开数据库连接时发生错误: {e}")
    
    def create_database_and_tables(self) -> bool:
        """
        创建数据库和表结构
        
        Returns:
            是否创建成功
        """
        try:
            # 先连接到MySQL服务器（不指定数据库）
            temp_connection = mysql.connector.connect(
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 3306),
                user=self.db_config.get('user', 'root'),
                password=self.db_config.get('password', '')
            )
            
            temp_cursor = temp_connection.cursor()
            
            # 创建数据库
            db_name = self.db_config.get('database', 'market_data_db')
            temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"数据库 {db_name} 创建成功")
            
            # 关闭临时连接
            temp_cursor.close()
            temp_connection.close()
            
            # 重新连接到指定数据库
            if not self.connect():
                return False
            
            # 创建历史数据表
            create_hist_table_query = """
            CREATE TABLE IF NOT EXISTS historical_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                etf_code VARCHAR(10) NOT NULL,
                period VARCHAR(10) NOT NULL,
                date DATETIME NOT NULL,
                open_price DECIMAL(10, 4),
                close_price DECIMAL(10, 4),
                high_price DECIMAL(10, 4),
                low_price DECIMAL(10, 4),
                volume BIGINT,
                amount DECIMAL(15, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_data (etf_code, period, date)
            )
            """
            
            self.cursor.execute(create_hist_table_query)
            
            # 创建实时数据表
            create_realtime_table_query = """
            CREATE TABLE IF NOT EXISTS realtime_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                etf_code VARCHAR(10) NOT NULL,
                name VARCHAR(50),
                current_price DECIMAL(10, 4),
                open_price DECIMAL(10, 4),
                high_price DECIMAL(10, 4),
                low_price DECIMAL(10, 4),
                prev_close DECIMAL(10, 4),
                volume BIGINT,
                amount DECIMAL(15, 2),
                change_pct DECIMAL(8, 4),
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_etf (etf_code)
            )
            """
            
            self.cursor.execute(create_realtime_table_query)
            
            # 提交更改
            self.connection.commit()
            
            logger.info("数据库表结构创建成功")
            return True
            
        except Error as e:
            logger.error(f"创建数据库和表结构失败: {e}")
            return False
    
    def save_historical_data(self, etf_code: str, period: str, df: pd.DataFrame) -> bool:
        """
        保存历史数据到数据库
        
        Args:
            etf_code: ETF代码
            period: 时间周期
            df: 历史数据DataFrame
            
        Returns:
            是否保存成功
        """
        try:
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return False
            
            # 准备插入数据
            insert_query = """
            INSERT INTO historical_data 
            (etf_code, period, date, open_price, close_price, high_price, low_price, volume, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            open_price=VALUES(open_price), close_price=VALUES(close_price), 
            high_price=VALUES(high_price), low_price=VALUES(low_price),
            volume=VALUES(volume), amount=VALUES(amount)
            """
            
            # 批量插入数据
            data_to_insert = []
            for index, row in df.iterrows():
                data_to_insert.append((
                    etf_code,
                    period,
                    index,  # date
                    float(row['open']) if not pd.isna(row['open']) else None,
                    float(row['close']) if not pd.isna(row['close']) else None,
                    float(row['high']) if not pd.isna(row['high']) else None,
                    float(row['low']) if not pd.isna(row['low']) else None,
                    int(row['volume']) if not pd.isna(row['volume']) else None,
                    float(row['amount']) if not pd.isna(row['amount']) else None
                ))
            
            self.cursor.executemany(insert_query, data_to_insert)
            self.connection.commit()
            
            logger.info(f"ETF {etf_code} 历史数据保存成功，共 {len(data_to_insert)} 条记录")
            return True
            
        except Error as e:
            logger.error(f"保存ETF {etf_code} 历史数据失败: {e}")
            return False
    
    def save_realtime_data(self, data: Dict) -> bool:
        """
        保存实时数据到数据库
        
        Args:
            data: 实时数据字典
            
        Returns:
            是否保存成功
        """
        try:
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return False
            
            # 准备插入数据
            insert_query = """
            INSERT INTO realtime_data 
            (etf_code, name, current_price, open_price, high_price, low_price, 
             prev_close, volume, amount, change_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            name=VALUES(name), current_price=VALUES(current_price), open_price=VALUES(open_price),
            high_price=VALUES(high_price), low_price=VALUES(low_price), prev_close=VALUES(prev_close),
            volume=VALUES(volume), amount=VALUES(amount), change_pct=VALUES(change_pct),
            update_time=CURRENT_TIMESTAMP
            """
            
            data_to_insert = (
                data.get('code'),
                data.get('name'),
                float(data.get('current_price', 0)),
                float(data.get('open_price', 0)),
                float(data.get('high_price', 0)),
                float(data.get('low_price', 0)),
                float(data.get('prev_close', 0)),
                int(data.get('volume', 0)),
                float(data.get('amount', 0)),
                float(data.get('change_pct', 0))
            )
            
            self.cursor.execute(insert_query, data_to_insert)
            self.connection.commit()
            
            logger.info(f"ETF {data.get('code')} 实时数据保存成功")
            return True
            
        except Error as e:
            logger.error(f"保存ETF {data.get('code')} 实时数据失败: {e}")
            return False
    
    def get_historical_data(self, etf_code: str, period: str, count: int = 100) -> Optional[pd.DataFrame]:
        """
        从数据库获取历史数据
        
        Args:
            etf_code: ETF代码
            period: 时间周期
            count: 数据条数
            
        Returns:
            历史数据DataFrame
        """
        try:
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return None
            
            select_query = """
            SELECT date, open_price, close_price, high_price, low_price, volume, amount
            FROM historical_data
            WHERE etf_code = %s AND period = %s
            ORDER BY date DESC
            LIMIT %s
            """
            
            self.cursor.execute(select_query, (etf_code, period, count))
            results = self.cursor.fetchall()
            
            if not results:
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame(results, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            
            logger.info(f"从数据库获取ETF {etf_code} 历史数据成功，共 {len(df)} 条记录")
            return df
            
        except Error as e:
            logger.error(f"从数据库获取ETF {etf_code} 历史数据失败: {e}")
            return None
    
    def get_realtime_data(self, etf_code: str) -> Optional[Dict]:
        """
        从数据库获取实时数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            实时数据字典
        """
        try:
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return None
            
            select_query = """
            SELECT etf_code, name, current_price, open_price, high_price, low_price, 
                   prev_close, volume, amount, change_pct, update_time
            FROM realtime_data
            WHERE etf_code = %s
            """
            
            self.cursor.execute(select_query, (etf_code,))
            result = self.cursor.fetchone()
            
            if not result:
                return None
            
            # 转换为字典
            columns = ['code', 'name', 'current_price', 'open_price', 'high_price', 'low_price', 
                      'prev_close', 'volume', 'amount', 'change_pct', 'update_time']
            data = dict(zip(columns, result))
            
            logger.info(f"从数据库获取ETF {etf_code} 实时数据成功")
            return data
            
        except Error as e:
            logger.error(f"从数据库获取ETF {etf_code} 实时数据失败: {e}")
            return None