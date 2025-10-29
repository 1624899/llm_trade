"""
工具函数模块
"""

import os
import yaml
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置字典
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"配置文件加载成功: {config_path}")
        return config
    except Exception as e:
        logger.error(f"配置文件加载失败: {e}")
        raise


def load_etf_list(etf_list_path: str = "config/etf_list.yaml") -> Dict[str, List[Dict]]:
    """
    加载ETF列表
    
    Args:
        etf_list_path: ETF列表文件路径
        
    Returns:
        ETF列表字典
    """
    try:
        with open(etf_list_path, 'r', encoding='utf-8') as f:
            etf_list = yaml.safe_load(f)
        logger.info(f"ETF列表加载成功: {etf_list_path}")
        return etf_list
    except Exception as e:
        logger.error(f"ETF列表加载失败: {e}")
        raise


def load_account_data(account_data_path: str = "data/account_data.json") -> Dict[str, Any]:
    """
    加载账户数据
    
    Args:
        account_data_path: 账户数据文件路径
        
    Returns:
        账户数据字典
    """
    try:
        with open(account_data_path, 'r', encoding='utf-8') as f:
            account_data = json.load(f)
        logger.info(f"账户数据加载成功: {account_data_path}")
        return account_data
    except Exception as e:
        logger.error(f"账户数据加载失败: {e}")
        raise


def save_account_data(account_data: Dict[str, Any],
                     account_data_path: str = "data/account_data.json") -> None:
    """
    保存账户数据
    
    Args:
        account_data: 账户数据字典
        account_data_path: 账户数据文件路径
    """
    try:
        # 确保目录存在
        dir_path = os.path.dirname(account_data_path)
        if dir_path:  # 只有当目录路径不为空时才创建目录
            os.makedirs(dir_path, exist_ok=True)
        
        with open(account_data_path, 'w', encoding='utf-8') as f:
            json.dump(account_data, f, ensure_ascii=False, indent=2)
        logger.info(f"账户数据保存成功: {account_data_path}")
    except Exception as e:
        logger.error(f"账户数据保存失败: {e}")
        raise


def setup_logging(config: Dict[str, Any]) -> None:
    """
    设置日志配置
    
    Args:
        config: 配置字典
    """
    try:
        log_config = config.get('logging', {})
        log_level = log_config.get('level', 'INFO')
        log_file = log_config.get('file', 'logs/etf_trading.log')
        
        # 确保日志目录存在
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # 配置loguru
        logger.remove()  # 移除默认处理器
        logger.add(
            log_file,
            level=log_level,
            rotation=log_config.get('max_size', '10MB'),
            retention=log_config.get('backup_count', 5),
            encoding='utf-8',
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}"
        )
        logger.add(
            lambda msg: print(msg, end=''),  # 控制台输出
            level=log_level,
            format="{time:HH:mm:ss} | {level} | {message}"
        )
        
        logger.info("日志系统初始化成功")
    except Exception as e:
        print(f"日志系统初始化失败: {e}")
        raise


def get_trading_minutes(start_time_str: str = None) -> int:
    """
    获取交易分钟数
    
    Args:
        start_time_str: 开始时间字符串，如果为None则从account_data.json读取
        
    Returns:
        从开始交易到现在的分钟数
    """
    # 如果没有提供开始时间，则从账户数据中读取
    if start_time_str is None:
        try:
            account_data = load_account_data()
            account_info = account_data.get('account_info', {})
            start_time_str = account_info.get('start_time')
        except Exception as e:
            logger.warning(f"无法从账户数据加载开始时间，使用默认时间: {e}")
            start_time_str = "2024-01-01 09:30:00"
    
    # 解析开始时间
    try:
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.warning(f"开始时间格式错误，使用默认时间: {start_time_str}")
        start_time = datetime(2024, 1, 1, 9, 30, 0)
    
    current_time = datetime.now()
    
    # 计算交易日的分钟数（简化计算，实际需要排除非交易日）
    total_minutes = int((current_time - start_time).total_seconds() / 60)
    
    # 排除非交易时间（每天9:30-15:00为交易时间，共5.5小时=330分钟）
    # 简化处理：假设只有1/4的时间是交易时间
    trading_minutes = total_minutes // 4
    
    return trading_minutes


def format_number(num: float, decimal_places: int = 2) -> str:
    """
    格式化数字
    
    Args:
        num: 数字
        decimal_places: 小数位数
        
    Returns:
        格式化后的字符串
    """
    if num is None:
        return "N/A"
    
    if abs(num) >= 1e6:
        return f"{num/1e6:.{decimal_places}f}M"
    elif abs(num) >= 1e3:
        return f"{num/1e3:.{decimal_places}f}K"
    else:
        return f"{num:.{decimal_places}f}"


def calculate_return_pct(entry_price: float, current_price: float) -> float:
    """
    计算收益率百分比
    
    Args:
        entry_price: 入场价格
        current_price: 当前价格
        
    Returns:
        收益率百分比
    """
    if entry_price == 0:
        return 0.0
    return ((current_price - entry_price) / entry_price) * 100


def validate_etf_code(code: str) -> bool:
    """
    验证ETF代码格式
    
    Args:
        code: ETF代码
        
    Returns:
        是否有效
    """
    if not code or not isinstance(code, str):
        return False
    
    # A股ETF代码通常是6位数字
    return code.isdigit() and len(code) == 6


def create_directories() -> None:
    """
    创建必要的目录
    """
    directories = [
        "data/market_data",
        "logs",
        "outputs"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"目录创建成功: {directory}")


def get_current_time_str() -> str:
    """
    获取当前时间字符串
    
    Returns:
        格式化的当前时间字符串
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def is_trading_time() -> bool:
    """
    判断是否为交易时间
    
    Returns:
        是否为交易时间
    """
    now = datetime.now()
    
    # 周末不交易
    if now.weekday() >= 5:  # 5=周六, 6=周日
        return False
    
    # 交易时间：9:30-11:30, 13:00-15:00
    current_time = now.time()
    
    morning_start = datetime.strptime("09:30", "%H:%M").time()
    morning_end = datetime.strptime("11:30", "%H:%M").time()
    afternoon_start = datetime.strptime("13:00", "%H:%M").time()
    afternoon_end = datetime.strptime("15:00", "%H:%M").time()
    
    return (morning_start <= current_time <= morning_end or 
            afternoon_start <= current_time <= afternoon_end)