# A股ETF AI自主交易系统使用说明

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置系统

编辑 `config/config.yaml` 文件，配置LLM API密钥：

```yaml
llm:
  provider: "openai"  # 或其他支持的提供商
  api_key: "your_api_key_here"
  base_url: "https://api.openai.com/v1"
  model: "gpt-4"
```

### 3. 配置账户数据

编辑 `data/account_data.json` 文件，录入您的持仓信息。

### 4. 运行AI自主交易系统

```bash
# AI自主交易模式（默认）
python main.py --mode auto

# 系统测试（不执行实际交易）
python main.py --mode test
```

# 启动自动交易（分层定时任务）
python main.py --mode auto

# 系统功能测试
python main.py --mode test
```

## 功能说明

### 1. AI自主交易模式（默认）
系统完全基于AI决策进行自主交易：
- 获取实时市场数据
- 计算技术指标
- AI生成交易决策
- 自动执行交易操作
- 记录交易结果
- 更新账户持仓

### 2. 系统测试模式
测试各个模块是否正常工作，不执行实际交易操作。

## 配置说明

### ETF监控列表
在 `config/etf_list.yaml` 中配置要监控的ETF：

```yaml
monitored_etfs:
  - code: "518880"
    name: "黄金ETF"
    category: "黄金"
  - code: "512760"
    name: "芯片ETF"
    category: "芯片"
```

### 技术指标参数
在 `config/config.yaml` 中调整技术指标参数：

```yaml
indicators:
  ema:
    short: 12
    long: 20
  macd:
    fast: 12
    slow: 26
    signal: 9
  rsi:
    period_7: 7
    period_14: 14
```

### 账户数据格式
在 `data/account_data.json` 中管理账户信息：

```json
{
  "account_info": {
    "account_type": "普通账户",
    "leverage": 1,
    "available_cash": 8420.35,
    "total_assets": 12850.72,
    "start_time": "2024-01-01 09:30:00",
    "call_count": 0
  },
  "positions": [
    {
      "symbol": "518880",
      "name": "黄金ETF",
      "quantity": 42.0,
      "available_quantity": 42.0,
      "avg_price": 218.90,
      "current_price": 228.45,
      "position_ratio": 75.5,
      "daily_pnl": 401.10,
      "total_pnl": 401.10,
      "market_value": 9595.0
    }
  ]
}
```

### AI交易配置
在 `config/config.yaml` 中配置AI交易参数：

```yaml
trading:
  max_position_ratio: 10.0  # 单次交易最大仓位比例（%）
  risk_level: "medium"       # 风险等级：low/medium/high
  enable_auto_trade: true    # 启用自动交易
  test_mode: false          # 测试模式（不执行实际交易）
```

## 输出文件

系统会在以下目录生成输出文件：

- `outputs/` - AI交易决策提示词和执行结果
- `logs/` - 系统日志
- `data/market_data/` - 市场数据缓存
- `data/trade_executions/` - 交易执行记录
- `data/Macro events/` - 宏观事件数据

## AI自主交易流程详解

1. **数据获取阶段**
   - 获取多源市场数据（价格、成交量、盘口等）
   - 计算技术指标（EMA、MACD、RSI等）
   - 获取市场情绪和资金流向数据
   - 获取宏观事件和日历数据

2. **AI决策阶段**
   - 将市场数据转换为标准化语料
   - 调用LLM API生成交易决策
   - 验证决策格式和参数有效性
   - 应用风险控制规则

3. **交易执行阶段**
   - 验证账户资金和持仓状态
   - 执行买入/卖出/持有操作
   - 更新账户持仓信息
   - 记录交易执行结果

4. **结果记录阶段**
   - 保存交易决策和执行记录
   - 更新账户数据文件
   - 记录系统日志
   - 生成交易执行报告

## 注意事项

1. **API密钥安全**：请妥善保管LLM API密钥，不要提交到版本控制系统
2. **交易时间**：系统会自动判断交易时间，非交易时间不会执行交易
3. **网络连接**：确保网络连接正常以获取实时数据
4. **资金管理**：确保账户资金充足，系统会自动检查可用资金
5. **风险控制**：系统内置风险控制机制，单次交易金额不超过总资产的10%
6. **交易确认**：AI自主交易模式会直接执行交易，请确保理解系统功能
7. **数据备份**：建议定期备份账户数据和交易记录

## 故障排除

### 常见问题

1. **数据获取失败**
   - 检查网络连接
   - 确认ETF代码正确
   - 查看日志文件获取详细错误信息
   - 检查数据源API是否正常

2. **LLM调用失败**
   - 检查API密钥配置
   - 确认API额度充足
   - 检查网络连接
   - 验证模型名称和参数

3. **交易执行失败**
   - 检查账户资金是否充足
   - 确认ETF代码在监控列表中
   - 检查交易时间是否在交易时段内
   - 查看交易执行记录获取详细错误信息

4. **配置文件错误**
   - 检查YAML格式是否正确
   - 确认文件路径正确
   - 验证配置参数有效性

5. **AI决策异常**
   - 检查市场数据是否完整
   - 确认技术指标计算正常
   - 查看AI决策日志
   - 验证风险控制规则

### 日志查看

查看 `logs/etf_trading.log` 文件获取详细日志信息。

### 交易记录查看

查看 `data/trade_executions/` 目录下的交易执行记录文件。

## 扩展功能

### 添加新的ETF
在 `config/etf_list.yaml` 中添加新的ETF代码和信息。

### 自定义技术指标
在 `src/indicators.py` 中添加新的技术指标计算方法。

### 支持新的LLM提供商
在 `src/llm_client.py` 中添加新的提供商支持。

### 自定义风险控制规则
在 `src/prompt_generator.py` 中修改交易决策请求部分，添加自定义风险控制逻辑。

### 添加新的数据源
在 `src/data_fetcher.py` 中添加新的数据源支持。

## 技术支持

如遇到问题，请：
1. 查看日志文件和交易记录
2. 运行系统测试模式
3. 检查配置文件格式和参数
4. 确认网络连接和API状态
5. 验证账户数据和资金状态

## 常见问题解答

### Q: AI自主交易是否安全？
A: 系统内置多层风险控制机制，包括单次交易金额限制、仓位管理、交易时间验证等。但任何投资都有风险，请在充分了解系统功能的前提下使用。

### Q: 如何停止AI自主交易？
A: 可以通过Ctrl+C停止程序运行，或在配置文件中设置`enable_auto_trade: false`禁用自动交易。

### Q: 系统如何处理非交易时间？
A: 系统会自动判断交易时间，非交易时间不会执行交易操作，但仍会获取市场数据进行分析。

### Q: 如何查看交易历史？
A: 交易记录保存在`data/trade_executions/`目录下，包括决策时间、操作类型、数量、价格等详细信息。

---

**免责声明**：本系统仅供学习和研究使用，不构成投资建议。AI自主交易涉及风险，请在充分了解系统功能和风险控制机制的前提下使用。投资有风险，入市需谨慎。