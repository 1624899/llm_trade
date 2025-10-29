#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试账户功能：T+1交易规则、佣金计算和当日盈亏计算
"""

import sys
import os
import json
from datetime import datetime

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.account import AccountManager


def test_t1_rule():
    """测试T+1交易规则"""
    print("=" * 60)
    print("🧪 测试T+1交易规则")
    print("=" * 60)
    
    # 创建账户管理器
    account_manager = AccountManager("data/account_data.json")
    
    # 重置账户数据以进行干净的测试
    account_manager.account_data = account_manager._initialize_account_data()
    
    print("初始账户信息:")
    account_info = account_manager.get_account_info()
    print(f"  可用现金: {account_info['available_cash']:.2f}")
    print(f"  总资产: {account_info['total_assets']:.2f}")
    print()
    
    # 测试普通ETF的T+1规则
    print("1️⃣ 测试普通ETF（159995）T+1规则:")
    print("   买入100股普通ETF，价格1.0元")
    success = account_manager.buy_etf("159995", "普通ETF", 100, 1.0)
    print(f"   买入结果: {'✅ 成功' if success else '❌ 失败'}")
    
    # 检查持仓
    positions = account_manager.get_positions()
    if positions:
        position = positions[0]
        print(f"   持仓数量: {position['quantity']}")
        print(f"   可卖数量: {position['available_quantity']}")
        print(f"   买入日期: {position.get('purchase_date', 'N/A')}")
        print(f"   T+1规则验证: 普通ETF当日买入后available_quantity应为0")
        print(f"   实际结果: available_quantity = {position['available_quantity']}")
        if position['available_quantity'] == 0:
            print("   ✅ T+1规则生效 - 普通ETF当日买入不可卖出")
        else:
            print("   ❌ T+1规则未生效")
    print()
    
    # 测试黄金ETF的T+0规则
    print("2️⃣ 测试黄金ETF（518880）T+0规则:")
    print("   买入100股黄金ETF，价格4.0元")
    success = account_manager.buy_etf("518880", "黄金ETF", 100, 4.0)
    print(f"   买入结果: {'✅ 成功' if success else '❌ 失败'}")
    
    # 检查黄金ETF持仓
    gold_position = account_manager.get_position_by_symbol("518880")
    if gold_position:
        print(f"   持仓数量: {gold_position['quantity']}")
        print(f"   可卖数量: {gold_position['available_quantity']}")
        print(f"   买入日期: {gold_position.get('purchase_date', 'N/A')}")
        print(f"   T+0规则验证: 黄金ETF当日买入后available_quantity应等于quantity")
        print(f"   实际结果: available_quantity = {gold_position['available_quantity']}")
        if gold_position['available_quantity'] == gold_position['quantity']:
            print("   ✅ T+0规则生效 - 黄金ETF可以随时卖出")
        else:
            print("   ❌ T+0规则未生效")
    print()
    
    # 尝试卖出当日买入的普通ETF（应该失败）
    print("3️⃣ 尝试卖出当日买入的普通ETF（应该失败）:")
    success = account_manager.sell_etf("159995", 50, 1.1)
    print(f"   卖出结果: {'✅ 成功' if success else '❌ 失败'}")
    if not success:
        print("   ✅ T+1规则生效 - 普通ETF当日买入不可卖出")
    else:
        print("   ❌ T+1规则未生效 - 普通ETF当日买入后仍可卖出")
    print()
    
    # 尝试卖出当日买入的黄金ETF（应该成功）
    print("4️⃣ 尝试卖出当日买入的黄金ETF（应该成功）:")
    success = account_manager.sell_etf("518880", 50, 4.1)
    print(f"   卖出结果: {'✅ 成功' if success else '❌ 失败'}")
    if success:
        print("   ✅ T+0规则生效 - 黄金ETF可以随时卖出")
    else:
        print("   ❌ T+0规则未生效 - 黄金ETF当日买入后不可卖出")
    print()


def test_commission_calculation():
    """测试佣金计算"""
    print("=" * 60)
    print("🧮 测试佣金计算")
    print("=" * 60)
    
    # 创建账户管理器
    account_manager = AccountManager("data/account_data.json")
    
    # 重置账户数据以进行干净的测试
    account_manager.account_data = account_manager._initialize_account_data()
    
    initial_cash = account_manager.get_cash_balance()
    print(f"初始现金: {initial_cash:.2f}")
    
    # 买入ETF，测试佣金扣除
    print("1️⃣ 买入ETF测试佣金扣除:")
    print("   买入100股ETF，价格2.0元，佣金5元")
    success = account_manager.buy_etf("159995", "佣金测试ETF", 100, 2.0)
    print(f"   买入结果: {'✅ 成功' if success else '❌ 失败'}")
    
    after_buy_cash = account_manager.get_cash_balance()
    expected_cost = 100 * 2.0 + 5.0  # 价格 + 佣金
    actual_cost = initial_cash - after_buy_cash
    print(f"   预期花费: {expected_cost:.2f}")
    print(f"   实际花费: {actual_cost:.2f}")
    print(f"   现金余额: {after_buy_cash:.2f}")
    
    if abs(actual_cost - expected_cost) < 0.01:
        print("   ✅ 买入佣金计算正确")
    else:
        print("   ❌ 买入佣金计算错误")
    print()
    
    # 检查交易记录中的佣金
    trade_history = account_manager.get_trade_history()
    if trade_history:
        last_trade = trade_history[-1]
        print(f"   交易记录中的佣金: {last_trade.get('commission_fee', 0):.2f}")
        if last_trade.get('commission_fee', 0) == 5.0:
            print("   ✅ 交易记录中佣金记录正确")
        else:
            print("   ❌ 交易记录中佣金记录错误")
    print()
    
    # 买入黄金ETF（T+0），用于测试卖出佣金
    print("2️⃣ 买入黄金ETF（T+0）用于测试卖出:")
    print("   买入50股黄金ETF，价格2.0元")
    success = account_manager.buy_etf("518880", "黄金ETF", 50, 2.0)
    print(f"   买入结果: {'✅ 成功' if success else '❌ 失败'}")
    
    after_buy_gold_cash = account_manager.get_cash_balance()
    print(f"   现金余额: {after_buy_gold_cash:.2f}")
    print()
    
    # 卖出黄金ETF，测试佣金扣除
    print("3️⃣ 卖出黄金ETF测试佣金扣除:")
    print("   卖出25股黄金ETF，价格2.2元，佣金5元")
    success = account_manager.sell_etf("518880", 25, 2.2)
    print(f"   卖出结果: {'✅ 成功' if success else '❌ 失败'}")
    
    after_sell_cash = account_manager.get_cash_balance()
    expected_income = 25 * 2.2 - 5.0  # 收入 - 佣金
    actual_income = after_sell_cash - after_buy_gold_cash
    print(f"   预期收入: {expected_income:.2f}")
    print(f"   实际收入: {actual_income:.2f}")
    print(f"   现金余额: {after_sell_cash:.2f}")
    
    if abs(actual_income - expected_income) < 0.01:
        print("   ✅ 卖出佣金计算正确")
    else:
        print("   ❌ 卖出佣金计算错误")
    
    # 检查交易记录中的佣金
    trade_history = account_manager.get_trade_history()
    if len(trade_history) >= 2:
        last_trade = trade_history[-1]  # 最后一笔是卖出交易
        print(f"   卖出交易记录中的佣金: {last_trade.get('commission_fee', 0):.2f}")
        if last_trade.get('commission_fee', 0) == 5.0:
            print("   ✅ 卖出交易记录中佣金记录正确")
        else:
            print("   ❌ 卖出交易记录中佣金记录错误")
    print()


def test_daily_pnl_calculation():
    """测试当日盈亏计算"""
    print("=" * 60)
    print("📊 测试当日盈亏计算")
    print("=" * 60)
    
    # 创建账户管理器
    account_manager = AccountManager("data/account_data.json")
    
    # 重置账户数据以进行干净的测试
    account_manager.account_data = account_manager._initialize_account_data()
    
    # 买入ETF
    print("1️⃣ 买入ETF:")
    print("   买入100股ETF，价格1.0元")
    account_manager.buy_etf("159995", "盈亏测试ETF", 100, 1.0)
    
    # 检查初始盈亏
    positions = account_manager.get_positions()
    if positions:
        position = positions[0]
        print(f"   初始总盈亏: {position['total_pnl']:.2f}")
        print(f"   初始当日盈亏: {position['daily_pnl']:.2f}")
    print()
    
    # 更新价格，模拟价格变动
    print("2️⃣ 更新价格，模拟价格变动:")
    print("   更新价格从1.0元到1.2元")
    account_manager.hold_position("159995", 1.2)
    
    # 检查盈亏变化
    updated_position = account_manager.get_position_by_symbol("159995")
    if updated_position:
        print(f"   更新后总盈亏: {updated_position['total_pnl']:.2f}")
        print(f"   更新后当日盈亏: {updated_position['daily_pnl']:.2f}")
        expected_pnl = (1.2 - 1.0) * 100  # (新价格 - 买入价格) * 数量
        if abs(updated_position['total_pnl'] - expected_pnl) < 0.01:
            print("   ✅ 总盈亏计算正确")
        else:
            print(f"   ❌ 总盈亏计算错误，预期: {expected_pnl:.2f}, 实际: {updated_position['total_pnl']:.2f}")
        
        if abs(updated_position['daily_pnl'] - expected_pnl) < 0.01:
            print("   ✅ 当日盈亏计算正确")
        else:
            print(f"   ❌ 当日盈亏计算错误，预期: {expected_pnl:.2f}, 实际: {updated_position['daily_pnl']:.2f}")
    print()
    
    # 再次更新价格，模拟价格继续变动
    print("3️⃣ 再次更新价格，模拟价格继续变动:")
    print("   更新价格从1.2元到1.1元")
    account_manager.hold_position("159995", 1.1)
    
    # 检查盈亏变化
    updated_position2 = account_manager.get_position_by_symbol("159995")
    if updated_position2:
        print(f"   再次更新后总盈亏: {updated_position2['total_pnl']:.2f}")
        print(f"   再次更新后当日盈亏: {updated_position2['daily_pnl']:.2f}")
        expected_total_pnl = (1.1 - 1.0) * 100  # (新价格 - 买入价格) * 数量
        expected_daily_pnl = (1.1 - 1.2) * 100  # (新价格 - 昨日价格) * 数量
        print(f"   预期总盈亏: {expected_total_pnl:.2f}")
        print(f"   预期当日盈亏: {expected_daily_pnl:.2f}")
        
        if abs(updated_position2['total_pnl'] - expected_total_pnl) < 0.01:
            print("   ✅ 总盈亏计算正确")
        else:
            print(f"   ❌ 总盈亏计算错误")
        
        if abs(updated_position2['daily_pnl'] - expected_daily_pnl) < 0.01:
            print("   ✅ 当日盈亏计算正确")
        else:
            print(f"   ❌ 当日盈亏计算错误")
    print()


def main():
    """主函数"""
    print("🚀 开始测试账户功能")
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 测试T+1交易规则
    test_t1_rule()
    
    # 测试佣金计算
    test_commission_calculation()
    
    # 测试当日盈亏计算
    test_daily_pnl_calculation()
    
    print("=" * 60)
    print("✅ 所有测试完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()