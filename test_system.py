import sys
import os
sys.path.append('src')

try:
    from main import ETFTradingSystem
    print("✅ 成功导入ETFTradingSystem")
    
    # 创建系统实例
    system = ETFTradingSystem()
    print("✅ 成功创建系统实例")
    
    # 临时减少ETF数量进行快速测试
    original_count = len(system.monitored_etfs)
    system.monitored_etfs = system.monitored_etfs[:2]
    print(f'🚀 开始快速测试，监控ETF数量: {len(system.monitored_etfs)} (原始: {original_count})')
    
    # 运行单次分析
    print("开始运行单次分析...")
    success = system.run_single_analysis()
    
    if success:
        print('🎉 快速测试成功！')
    else:
        print('❌ 快速测试失败')
        
except Exception as e:
    print(f'❌ 测试过程中发生错误: {e}')
    import traceback
    traceback.print_exc()