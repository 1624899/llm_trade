# LLM-TRADE

`LLM-TRADE` 是一个面向 A 股的多 Agent 选股、模拟交易、复盘与观察仓管理系统。它的核心思路是：

```text
数据入湖 -> 多策略规则雷达 -> 多 Agent 深度复核 -> 决策输出 -> 观察仓候选池 -> 交易仓模拟调仓 -> 亏损反思沉淀
```

系统不会把所有判断都交给大模型。确定性的行情、财务、技术和风控规则先由代码计算，LLM 负责做最后的综合解释、取舍和报告生成。

## 核心能力

### 1. 本地数据湖

`DataPipeline` 会把基础股票信息、行情快照、多周期 K 线、指数 K 线和财务指标同步到本地 SQLite：

- 数据库：`data/stock_lake.db`
- 行情快照表：`daily_quotes`
- 多周期 K 线表：`market_bars`
- 财务指标表：`financial_metrics`
- 观察仓候选池表：`watchlist_items`
- 模拟交易账户表：`trading_account`
- 模拟交易持仓表：`trading_positions`
- 模拟交易流水表：`trade_orders`
- 旧版观察仓兼容表：`paper_trades`

行情优先使用腾讯/新浪等免费接口，K 线和财务数据通过 AKShare 等源补充。

### 2. 多策略选股雷达

`StockScreener` 位于 `src/stock_screener.py`，是底层规则筛选器，不依赖 LLM。

它已经从单一趋势过滤升级为多策略雷达。股票只要满足任意一种策略探测器，就可以进入候选池：

- 经典趋势突破：右侧交易，均线多头，涨幅适中。
- 优质股底部低吸：低估值、阶段低位、底部量能活跃。
- 财报错杀/洗盘反转：短期急跌、估值有支撑、放量承接。
- 强势主升浪：均线完美发散、动量极强、资金接力，但过滤放量滞涨。
- 支撑回踩：趋势未坏，回踩关键均线或平台附近。

候选股会带上 `strategy_tags`、`strategy_confidence` 和 `screen_reason`，方便后续 Agent 理解它为什么入选。

### 3. 深度财务数据

`FinancialDataProvider` 位于 `src/financial_data.py`。

它通过 AKShare 的东方财富接口抓取并整理：

- 利润表
- 资产负债表
- 现金流量表
- 财务分析指标

归一化后的核心字段包括：

- 营收、营收同比
- 归母净利、归母净利同比
- 扣非归母净利、扣非同比
- 毛利率、净利率、ROE、ROIC
- 资产负债率、流动比率、速动比率、现金比率
- 经营现金流、经营现金流同比
- 经营现金流/归母净利

`FundamentalAgent` 会把这些结构化财务摘要放进 Prompt，避免基本面分析退化成单纯的新闻阅读理解。

### 4. 量化技术信号

`TechnicalSignalProvider` 位于 `src/technical_indicators.py`。

它从本地 `market_bars` 读取 OHLCV，并计算：

- ATR14
- MA5/10/20/60
- 5 日/20 日涨跌幅
- MA20 乖离率
- 20 日/60 日支撑压力
- 量比
- 60 日量能分位

同时生成形态和风险标签：

- 箱体放量突破
- 均线多头发散
- 缩量回踩 MA20
- 临近 20 日支撑/压力
- 恐慌放量
- 强动量延续
- 跌破 20 日平台
- 放量滞涨/上影派发

`TechnicalAgent` 会优先使用这些确定性指标，再结合多周期 K 线摘要和宏观环境给出买点、止损和风险判断。

### 5. 退出机制闭环

`ExitAgent` 位于 `src/agent/exit_agent.py`。

它不依赖 LLM，负责把“只管买，不管卖”的缺口补上第一版：

- 跌破 ATR 动态止损：清仓退出。
- 跌破 20 日平台：清仓退出。
- 已有浮盈但跌回 MA20 下方：移动止盈保护。
- 放量滞涨/上影派发：减仓观察。
- 宏观风险偏好偏低且仍有浮盈：锁定部分利润。

`PaperTrading` 会保留原有收益率止盈止损规则，并在 `ExitAgent` 给出更严重信号时自动升级动作。

### 6. 观察仓与交易仓

`Watchlist` 位于 `src/evaluation/watchlist.py`，负责维护最多 10 只观察标的。`--pick` 只会把最终推荐和完整分析写入观察仓候选池，不再等同于模拟买入。

`--trade` 启动时如果观察仓为空，会先维护候选池：已有交易仓持仓时，对持仓代码执行指定分析并写回观察仓；交易仓也为空时，自动跑一轮选股生成候选。TradingAgent 输出 `REMOVE`，或持仓触发硬风险清仓时，观察仓会同步移出对应标的。

`TradingAccount` 位于 `src/evaluation/trading_account.py`，负责持久模拟账户：

- 默认初始资金 16000 元，只在首次初始化时注入。
- 后续运行持续滚动现金、持仓、交易流水和盈亏。
- 交易仓最多同时持有 5 只股票。
- A 股默认按 100 股一手买入。
- 买入后至少持有 5 个交易日。
- 卖出后 5 个交易日内不回买同一股票。
- 单次运行默认最多 2 笔买入、2 笔卖出，禁止频繁交易。

`TradingAgent` 位于 `src/agent/trading_agent.py`，会读取观察仓、交易仓、宏观环境、退出信号和历史反思规则，输出 BUY/SELL/HOLD/WATCH/REMOVE 等结构化决策。LLM 不可用时会降级为规则兜底：风险清仓优先，未触发风险则持有，未持仓标的默认继续观察。

## Agent 工作流

`AgentCoordinator` 统一调度完整选股流程：

1. `StockScreener` 进行多策略规则海选，按主策略分组配额生成约 20 只候选池，避免单一策略霸榜。
2. `MacroAgent` 判断市场状态、风险偏好和主线方向。
3. `QuickFilterAgent` 读取宏观上下文和候选股极简快照，轻量精筛出最多 8 只进入深度复核；候选不足时直接放行。
4. `FundamentalAgent` 结合财务数据和公告做基本面复核。
5. `TechnicalAgent` 结合量化技术信号和 K 线摘要做技术复核。
6. `NewsRiskAgent` 检查公告、新闻和重大风险词。
7. `DecisionAgent` 综合排序，输出最终推荐报告。
8. `Watchlist` 将推荐和分析沉淀为观察仓候选池。
9. `TradingAgent` 在 `--trade` 中基于观察仓和交易仓执行模拟调仓。
10. `ExitAgent` 和 `ReflectionAgent` 在盘后进行持仓诊断与亏损反思。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

不要把真实 API Key 写入仓库文件。推荐使用环境变量：

```powershell
$env:DEEPSEEK_API_KEY="your-key"
$env:TAVILY_API_KEY="your-key"
$env:ALPHA_VANTAGE_API_KEY="your-key"
```

`config/config.yaml` 支持 `env:ENV_NAME` 写法：

```yaml
llm_models:
  deepseek:
    api_key: "env:DEEPSEEK_API_KEY"

web_search:
  api_key: "env:TAVILY_API_KEY"
```

### 3. 同步数据

盘后同步基础数据、行情快照和 K 线：

```bash
python main.py --sync
```

财务数据同步目前没有默认放进 `run_all()`，避免全市场请求过重。可以在 Python 中按候选股或指定列表同步：

```bash
python -c "from src.data_pipeline import DataPipeline; DataPipeline().sync_financial_metrics(codes=['600519'], periods=8)"
```

### 4. 执行选股

```bash
python main.py --pick
```

最新报告会写入：

- `outputs/latest_report.md`
- `outputs/latest_workflow_audit.json`
- `outputs/screener_audit.json`

`--pick` 会同步更新 `watchlist_items`，但不会直接买入交易仓。

### 5. 执行模拟交易

```bash
python main.py --trade
```

该流程会：

- 初始化或读取持久模拟账户。
- 刷新观察仓和交易仓当前价格。
- 观察仓为空时，先补充候选池：已有持仓则分析持仓，完全空仓则自动选股。
- 获取宏观环境和持仓退出信号。
- 调用 `TradingAgent` 生成买入、卖出、持有或继续观察决策。
- 由 `TradingAccount` 校验现金、100 股一手、最多 5 只持仓、最短持有期和卖出冷却期。
- 写入 `trade_orders`，并输出交易仓执行报告。

### 6. 指定股票单独分析

如果已经有几只想重点看的股票，可以跳过规则海选和观察仓更新，只复用现有 Agent 做逐只分析：

```bash
python main.py --analyze 600519 000001 300750
```

也支持逗号分隔和带市场前缀的写法：

```bash
python main.py --analyze sh600519,sz000001
```

该流程会执行：

- `MacroAgent`：先给出当日宏观环境和风险偏好。
- `FundamentalAgent`：读取财务数据、公告和搜索结果做基本面分析。
- `TechnicalAgent`：读取本地 K 线和技术信号做走势分析。
- `NewsRiskAgent`：检查近期新闻、公告和高风险关键词。

指定分析不会调用 `StockScreener`，也不会把股票自动加入观察仓。报告会写入：

- `outputs/latest_targeted_analysis.md`
- `outputs/latest_targeted_analysis_audit.json`

### 7. 报告内容口径

`outputs/latest_report.md` 是完整选股流程的最终决策报告，结论是候选池内的相对排序，不等同于所有入选股票都是同一强度的买入建议。报告统一使用以下分层：

- 强推荐：基本面强、技术形态明确、资讯风控低，适合作为本轮核心候选。
- 配置/轻仓验证：防御属性、回踩支撑、低风险配置或赔率一般但胜率较稳，只能轻仓或等触发条件。
- 观察：单独看有亮点，但买点、趋势、成长性或风控仍不够清晰。
- 不推荐：基本面、技术面或资讯风控存在明显短板。

`outputs/latest_targeted_analysis.md` 是指定股票的绝对诊断报告，不参与候选池相对排名，也不会自动加入观察仓。因此同一只股票可能在选股报告里属于“配置/轻仓验证”，但在单独分析里显示为中性、谨慎或等待买点；这表示它是弱市下的防御型备选，而不是进攻型强推荐。

### 8. 盘后观察仓与交易仓诊断

```bash
python main.py --post
```

该流程会：

- 更新观察仓和交易仓最新价格。
- 计算观察仓浮动收益和交易仓账户权益。
- 调用宏观环境判断。
- 通过 `ExitAgent` 评估是否减仓或清仓。
- 对观察仓亏损和交易仓亏损案例生成反思并沉淀到规则本。

## 配置文件

- `config/config.yaml`：LLM、搜索、数据保留窗口、并发数等运行配置。
- `config/stock_picking.yaml`：选股 profile、多策略筛选参数、主题加分和市场状态配置。

## 主要输出

- `outputs/latest_report.md`：最近一次选股报告。
- `outputs/latest_workflow_audit.json`：最近一次完整工作流审计。
- `outputs/screener_audit.json`：规则预筛审计。
- `outputs/latest_agent_trace.jsonl`：Agent 调用轨迹。
- `data/rules_book.txt`：亏损反思沉淀出的风控规则。
- `watchlist_items`：观察仓候选池，最多 10 只。
- `trading_account`：模拟交易账户和账户权益。
- `trading_positions`：当前交易仓持仓，最多 5 只。
- `trade_orders`：模拟交易流水和交易理由。

## 常用测试

```bash
python -m unittest test.test_financial_data
python -m unittest test.test_technical_indicators
python -m unittest test.test_exit_agent
python -m unittest test.test_database_and_screener.TradingAccountLifecycleTests
python -m unittest test.test_database_and_screener.WatchlistTests
python -m unittest test.test_runtime_regressions.TradingAgentTests
python -m unittest test.test_database_and_screener.StockScreenerTests
```

完整测试：

```bash
python -m unittest discover -s test
```

## 最近已完成的工程升级

- `src/agent/screener_agent.py` 已迁移为 `src/stock_screener.py`，定位为底层规则筛选器。
- `StockScreener` 已升级为多策略雷达，不再用单一 MA20 趋势条件一刀切。
- 新增 `QuickFilterAgent`，形成“规则分策略海选 → AI 轻量精筛 → Agent 深度复核”的三段式漏斗。
- 新增东方财富财务报表接入和 `financial_metrics` 表。
- `FundamentalAgent` 已接入近几期财务趋势摘要。
- 新增量化技术信号层，`TechnicalAgent` 不再只看文本 K 线。
- 新增 `ExitAgent`，观察仓具备第一版动态退出机制。
- `PaperTrading` 已升级为收益率规则 + 技术退出 + 宏观防守的组合诊断。
- 新增 `Watchlist`，`--pick` 只更新观察仓候选池，不再直接模拟买入。
- 新增 `TradingAccount`，支持 16000 元持久模拟账户、交易持仓和交易流水。
- 新增 `TradingAgent` 和 `--trade`，根据观察仓推荐与交易仓状态执行模拟调仓。
- `ReflectionAgent` 已升级为可结合推荐内容、交易行为和盈亏结果做亏损复盘。

## 风险声明

本项目仅用于研究、复盘和工程实验，不构成任何投资建议。股票市场有风险，真实交易前请自行验证数据质量、模型结论和个人风险承受能力。
