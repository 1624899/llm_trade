# LLM-TRADE 使用说明

LLM-TRADE 当前定位是 A 股多 Agent 选股与观察仓复盘系统。流程是：

```text
盘后数据入湖 -> 规则技术预筛 -> 宏观上下文 -> 个股复核 -> 决策报告 -> 观察仓复盘
```

## 快速开始

1. 安装依赖：

```bash
pip install -r requirements.txt
```

2. 配置密钥环境变量。不要把真实 API Key 写入仓库文件。

```powershell
$env:DEEPSEEK_API_KEY="your-key"
$env:TAVILY_API_KEY="your-key"
```

3. 盘后同步数据：

```bash
python main.py --sync
```

4. 执行自动选股：

```bash
python main.py --pick
```

5. 盘后观察仓诊断与反思：

```bash
python main.py --post
```

## 配置文件

- `config/config.yaml`：LLM、搜索、数据保留窗口、并发数等运行配置。
- `config/stock_picking.yaml`：技术预筛 profile、主题加分、市场状态自动切换等选股配置。

`config/config.yaml` 支持 `env:ENV_NAME` 写法，例如：

```yaml
llm_models:
  deepseek:
    api_key: "env:DEEPSEEK_API_KEY"

web_search:
  api_key: "env:TAVILY_API_KEY"
```

## 数据与输出

- `data/stock_lake.db`：本地 SQLite 数据湖。
- `outputs/latest_report.md`：最近一次选股报告。
- `outputs/screener_audit.json`：技术预筛审计信息。
- `data/rules_book.txt`：观察仓亏损反思沉淀的风控规则。
- `logs/llm_trade.log`：系统日志。

## 选股流程

1. `DataPipeline` 同步股票基础信息、行情快照、K 线和三大指数 K 线。
2. `MarketRegimeDetector` 根据上证指数、沪深300、创业板指判断市场状态，并自动选择 `conservative`、`balanced` 或 `aggressive` profile。
3. `StockScreener` 用确定性规则做技术预筛，并记录每条规则的淘汰统计。
4. `MacroAgent` 输出结构化宏观上下文。
5. `FundamentalAgent`、`TechnicalAgent`、`NewsRiskAgent` 并发复核候选股。
6. `DecisionAgent` 综合排序，过滤高危资讯标的，并输出机器可解析代码列表。
7. `PaperTrading` 将推荐标的加入观察仓，后续由 `--post` 做持仓诊断。

## 数据源策略

系统优先使用免费数据源：

- 行情快照：Tencent、Sina，失败后再降级。
- 历史 K 线：Yahoo Finance 批量拉取、efinance 小批量补洞、AKShare 兜底。
- 资讯搜索：Tavily 主力，DuckDuckGo HTML 免费兜底。

当前不依赖 Tushare 等付费或积分型数据源。

## 常见问题

### `--pick` 没有候选股

先查看 `outputs/screener_audit.json`，确认是市场状态极端风险、数据不足，还是某条技术规则过严。

### LLM 没有输出

检查对应环境变量是否存在，例如 `DEEPSEEK_API_KEY`，并确认 `config/config.yaml` 的 `active_llm` 指向已配置模型。

### 搜索失败

Tavily 没有 Key 或请求失败时会自动降级到 DuckDuckGo。搜索结果会缓存到 `data/web_search_cache.json`，避免同一轮重复请求。

### 数据同步很慢

`DataPipeline` 已做增量检查，`market_bars` 已覆盖最新交易日时会跳过重复拉取。首次建库或大范围缺口补数会较慢。

## 风险声明

系统输出仅用于研究与复盘，不构成投资建议。真实交易前请自行验证数据质量、模型结论和风险承受能力。
