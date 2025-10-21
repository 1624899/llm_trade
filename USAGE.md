# A股ETF交易分析系统使用说明

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

### 4. 运行系统

```bash
# 单次分析
python main.py --mode single

# 连续分析（每30分钟）
python main.py --mode continuous --interval 30

# 系统测试
python main.py --mode test

# 查看系统状态
python main.py --mode status
```

## 功能说明

### 1. 单次分析模式
执行一次完整的ETF分析流程：
- 获取市场数据
- 计算技术指标
- 更新账户持仓
- 生成交易建议
- 保存分析结果

### 2. 连续分析模式
按照设定间隔自动执行分析，适合长期监控。

### 3. 系统测试
测试各个模块是否正常工作。

### 4. 状态查看
显示当前系统状态和账户信息。

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
    "portfolio_value": 12850.72
  },
  "positions": [
    {
      "symbol": "518880",
      "name": "黄金ETF",
      "quantity": 42.0,
      "entry_price": 218.90,
      "current_price": 228.45,
      "unrealized_pnl": 401.10
    }
  ]
}
```

## 输出文件

系统会在以下目录生成输出文件：

- `outputs/` - 分析语料和交易建议
- `logs/` - 系统日志
- `data/market_data/` - 市场数据缓存

## 注意事项

1. **API密钥安全**：请妥善保管LLM API密钥，不要提交到版本控制系统
2. **交易时间**：系统会自动判断交易时间，非交易时间不会执行分析
3. **网络连接**：确保网络连接正常以获取实时数据
4. **数据准确性**：本系统仅供参考，实际交易请谨慎决策

## 故障排除

### 常见问题

1. **数据获取失败**
   - 检查网络连接
   - 确认ETF代码正确
   - 查看日志文件获取详细错误信息

2. **LLM调用失败**
   - 检查API密钥配置
   - 确认API额度充足
   - 检查网络连接

3. **配置文件错误**
   - 检查YAML格式是否正确
   - 确认文件路径正确

### 日志查看

查看 `logs/etf_trading.log` 文件获取详细日志信息。

## 扩展功能

### 添加新的ETF
在 `config/etf_list.yaml` 中添加新的ETF代码和信息。

### 自定义技术指标
在 `src/indicators.py` 中添加新的技术指标计算方法。

### 支持新的LLM提供商
在 `src/llm_client.py` 中添加新的提供商支持。

## 技术支持

如遇到问题，请：
1. 查看日志文件
2. 运行系统测试
3. 检查配置文件
4. 确认网络连接

---

**免责声明**：本系统仅供学习和研究使用，不构成投资建议。投资有风险，入市需谨慎。