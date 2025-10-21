# A股ETF交易分析系统

## 系统概述

本系统是一个完整的A股ETF交易分析工具，集成了行情数据获取、技术指标计算、账户管理和AI交易建议生成功能。

## 功能模块

1. **行情数据获取** - 使用AkShare获取A股ETF实时和历史数据
2. **技术指标计算** - 计算EMA、MACD、RSI、KDJ、BOLL、WR等技术指标
3. **账户数据管理** - 手动录入和管理账户持仓信息
4. **标准化语料生成** - 将数据转换为LLM可理解的标准化格式
5. **LLM交易建议** - 调用大模型API获取交易建议

## 系统架构

```
etf_trading_system/
├── config/
│   ├── config.yaml          # 系统配置文件
│   └── etf_list.yaml        # ETF代码列表
├── src/
│   ├── __init__.py
│   ├── data_fetcher.py      # 行情数据获取模块
│   ├── indicators.py        # 技术指标计算模块
│   ├── account.py          # 账户数据管理模块
│   ├── prompt_generator.py  # 标准化语料生成模块
│   ├── llm_client.py       # LLM API调用模块
│   └── utils.py            # 工具函数
├── data/
│   ├── account_data.json   # 账户持仓数据
│   └── market_data/        # 市场数据缓存
├── logs/                   # 日志文件
├── main.py                # 主程序入口
├── requirements.txt       # 依赖包列表
└── README.md             # 项目说明文档
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

1. 配置系统参数（编辑 `config/config.yaml`）
2. 录入账户持仓信息（编辑 `data/account_data.json`）
3. 运行主程序：`python main.py`

## 配置说明

### config.yaml
- LLM API配置
- 数据更新频率
- 技术指标参数
- 日志级别

### etf_list.yaml
- 监控的ETF代码列表
- ETF名称和分类

### account_data.json
- 账户基本信息
- 持仓明细
- 交易计划

## 输出格式

系统将生成符合要求的标准化语料，包含：
- 当前时间和统计信息
- 各ETF的实时价格和技术指标
- 日内和长期数据序列
- 账户持仓和表现信息
- 绩效指标

## 注意事项

- 需要配置有效的LLM API密钥
- 确保网络连接正常以获取实时数据
- 建议定期备份账户数据