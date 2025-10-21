"""
演示标准化语料生成
展示系统如何将市场数据转换为LLM可理解的格式
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.utils import load_config
from src.account import AccountManager
from src.prompt_generator import PromptGenerator

def demonstrate_prompt_generation():
    """演示语料生成过程"""
    print("📊 演示标准化语料生成过程")
    print("=" * 50)
    
    # 加载配置和账户数据
    config = load_config('config/config.yaml')
    account_manager = AccountManager('data/account_data.json')
    prompt_generator = PromptGenerator(config)
    
    # 创建模拟市场数据（基于用户提供的示例）
    mock_market_data = {
        "518880": {
            "name": "黄金ETF",
            "category": "黄金",
            "current_data": {
                "current_price": 228.45,
                "current_ema_long": 228.62,
                "current_macd": -0.08,
                "current_rsi_7": 43.21
            },
            "intraday_data": {
                "mid_prices": [228.82, 228.75, 228.68, 228.61, 228.63, 228.49, 228.46, 228.47, 228.40, 228.45],
                "ema_series": [228.71, 228.70, 228.68, 228.66, 228.65, 228.62, 228.60, 228.58, 228.55, 228.62],
                "macd_series": [0.12, 0.10, 0.06, 0.03, 0.01, -0.02, -0.04, -0.06, -0.07, -0.08],
                "rsi7_series": [55.8, 54.9, 41.2, 43.5, 46.1, 36.8, 38.2, 37.9, 33.1, 43.21],
                "rsi14_series": [57.9, 57.5, 50.1, 51.3, 52.6, 47.4, 47.9, 47.8, 45.3, 48.5]
            },
            "long_term_data": {
                "ema20": 223.15,
                "ema50": 226.80,
                "atr3": 1.82,
                "atr14": 2.11,
                "current_volume": 85200,
                "avg_volume": 980000,
                "macd_series": [-3.02, -2.81, -2.60, -2.48, -2.20, -1.85, -1.50, -1.25, -0.81, -0.28],
                "rsi_series": [40.1, 40.9, 41.2, 38.7, 45.6, 50.0, 51.8, 50.1, 59.0, 62.8]
            }
        },
        "512760": {
            "name": "芯片ETF",
            "category": "芯片",
            "current_data": {
                "current_price": 215.30,
                "current_ema_long": 216.05,
                "current_macd": -0.15,
                "current_rsi_7": 24.6
            },
            "intraday_data": {
                "mid_prices": [216.50, 216.42, 216.38, 216.10, 215.95, 215.75, 215.78, 215.65, 215.48, 215.30],
                "ema_series": [216.68, 216.62, 216.55, 216.42, 216.30, 216.15, 216.05, 215.90, 215.78, 216.05],
                "macd_series": [0.05, 0.01, -0.03, -0.08, -0.12, -0.16, -0.18, -0.21, -0.23, -0.15],
                "rsi7_series": [40.2, 39.5, 33.1, 26.0, 27.5, 23.2, 34.0, 27.0, 26.8, 24.6],
                "rsi14_series": [46.5, 46.2, 43.2, 38.4, 38.9, 35.9, 40.1, 35.8, 35.8, 33.7]
            },
            "long_term_data": {
                "ema20": 210.20,
                "ema50": 214.50,
                "atr3": 2.85,
                "atr14": 4.92,
                "current_volume": 120500,
                "avg_volume": 1250000,
                "macd_series": [-0.91, -0.82, -0.72, -0.66, -0.55, -0.37, -0.22, -0.11, 0.07, 0.24],
                "rsi_series": [46.6, 46.4, 48.1, 46.3, 50.3, 55.7, 56.2, 55.4, 61.0, 62.3]
            }
        }
    }
    
    # 获取账户数据
    account_data = account_manager.account_data
    
    # 生成标准化语料
    prompt = prompt_generator.generate_trading_prompt(mock_market_data, account_data)
    
    print("✅ 标准化语料生成成功！")
    print(f"📝 语料总长度: {len(prompt)} 字符")
    print("\n📋 语料预览（前500字符）:")
    print("-" * 50)
    print(prompt[:500] + "...")
    
    # 保存语料到文件
    prompt_file = prompt_generator.save_prompt_to_file(prompt)
    print(f"\n💾 语料已保存到: {prompt_file}")
    
    print("\n" + "=" * 50)
    print("🎯 演示完成！系统可以:")
    print("  1. 自动获取A股ETF行情数据")
    print("  2. 计算多种技术指标")
    print("  3. 整合账户持仓信息")
    print("  4. 生成标准化语料")
    print("  5. 调用LLM获取交易建议")
    print("=" * 50)

if __name__ == "__main__":
    demonstrate_prompt_generation()