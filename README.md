# A股ETF交易分析系统

## 系统概述

本系统是一个完整的A股ETF交易分析工具，集成了多源行情数据获取、技术指标计算、账户管理和AI交易建议生成功能。系统支持实时数据监控、技术分析和智能交易决策。

## 功能模块

1. **多源行情数据获取** - 使用AkShare和新浪财经API获取A股ETF实时和历史数据
2. **技术指标计算** - 计算EMA、MACD、RSI、KDJ、BOLL、WR、ATR等技术指标
3. **账户数据管理** - 自动更新和管理账户持仓信息
4. **标准化语料生成** - 将市场数据转换为LLM可理解的标准化格式
5. **LLM交易建议** - 调用大模型API获取智能交易建议
6. **并发数据获取** - 支持多ETF并发数据获取，提高效率
7. **市场情绪分析** - 获取行业资金流向等市场情绪数据

## 系统架构

```
etf_trading_system/
├── config/
│   ├── config.yaml          # 系统配置文件
│   ├── config_example.yaml  # 配置文件示例
│   └── etf_list.yaml        # ETF代码列表
├── src/
│   ├── __init__.py
│   ├── data_fetcher.py      # 行情数据获取模块
│   ├── indicators.py        # 技术指标计算模块
│   ├── account.py          # 账户数据管理模块
│   ├── prompt_generator.py  # 标准化语料生成模块
│   ├── llm_client.py       # LLM API调用模块
│   ├── sina_crawler.py     # 新浪财经数据爬取模块
│   └── utils.py            # 工具函数
├── data/
│   ├── account_data.json   # 账户持仓数据
│   ├── trading_analysis.json  # 交易分析结果
│   ├── trading_analysis_history.json  # 历史分析记录
│   └── market_data/        # 市场数据缓存
├── pdf/                    # 输出文件目录
├── logs/                   # 日志文件
├── main.py                # 主程序入口
├── requirements.txt       # 依赖包列表
├── README.md             # 项目说明文档
└── USAGE.md              # 详细使用说明
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

1. 配置系统参数（编辑 `config/config.yaml`）
2. 配置ETF监控列表（编辑 `config/etf_list.yaml`）
3. 录入账户持仓信息（编辑 `data/account_data.json`）
4. 运行主程序：`python main.py`

## 配置说明

### config.yaml
- LLM API配置（支持多种LLM提供商）
- 数据获取配置（更新频率、缓存设置、并发参数）
- 技术指标参数（EMA、MACD、RSI等参数设置）
- 日志配置
- 系统设置（测试模式等）

### etf_list.yaml
- 监控的ETF代码列表（按类别分组）
- ETF名称和分类信息
- 支持黄金、芯片、新能源、医药、科创、军工等多个类别

### account_data.json
- 账户基本信息（总资产、现金、收益等）
- 持仓明细（数量、成本价、当前价、盈亏等）
- 系统会自动更新持仓数据

## 运行模式

```bash
# 单次分析模式
python main.py --mode single

# 连续分析模式（默认30分钟间隔）
python main.py --mode continuous

# 连续分析模式（自定义间隔）
python main.py --mode continuous --interval 60

# 系统测试模式
python main.py --mode test

# 查看系统状态
python main.py --mode status
```

## 输出格式

系统将生成符合要求的标准化语料，包含：
- 当前时间和统计信息
- 各ETF的实时价格和技术指标
- 日内和长期数据序列
- 账户持仓和表现信息
- 市场情绪和资金流向数据
- 买卖盘口和分钟级Tick数据

## 注意事项

- 需要配置有效的LLM API密钥
- 确保网络连接正常以获取实时数据
- 建议定期备份账户数据
- 系统会自动判断交易时间，非交易时间不会执行分析
- 支持测试模式，可在不调用LLM API的情况下验证系统功能

## 测试模式

系统支持测试模式，可以在不调用LLM API的情况下运行，用于验证系统功能和数据获取是否正常。

### 启用测试模式

在 `config/config.yaml` 文件中设置：

```yaml
system:
  test_mode: true  # 设置为true启用测试模式，跳过LLM交互
```

### 测试模式功能

- 跳过LLM API调用
- 使用模拟的交易建议响应
- 仍会获取实时市场数据
- 仍会保存分析结果到文件
- 仍会更新账户信息（但不会增加调用次数）

### 命令行运行测试模式

```bash
# 单次分析测试模式
python main.py --mode single

# 连续分析测试模式
python main.py --mode continuous

# 系统测试（测试模式下会跳过LLM连接测试）
python main.py --mode test

# 查看系统状态
python main.py --mode status
```

## 数据源说明

系统支持多数据源获取ETF数据：

1. **AkShare API** - 主要数据源，提供实时和历史数据
2. **新浪财经API** - 备用数据源，通过sina_crawler模块获取
3. **eFinance API** - 补充数据源，用于获取特定数据

系统会自动处理数据源的切换和异常情况，确保数据获取的稳定性。

## 详细使用说明

更多详细使用说明请参考 [USAGE.md](USAGE.md) 文件，包括：
- 详细的配置参数说明
- 账户数据格式说明
- 故障排除指南
- 扩展功能开发指南

## 免责声明

本系统仅供学习和研究使用，不构成投资建议。投资有风险，入市需谨慎。