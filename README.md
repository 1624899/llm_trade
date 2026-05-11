# LLM-TRADE

`LLM-TRADE` 是一个面向 A 股的本地化 AI 投研复盘工作台，覆盖多 Agent 策略扫描、遮盖式回测、观察仓、模拟交易、盘后复盘与报告生成。它的核心思路是：

```text
数据入湖 -> 遮盖式走步回测 -> 多策略规则雷达前置校准 -> AI 轻量精筛 -> 资讯硬风控前置排雷 -> 多 Agent 深度复核 -> 决策输出 -> 观察仓候选池 -> 交易仓模拟调仓 -> 亏损反思沉淀
```

系统不会把所有判断都交给大模型。确定性的行情、财务、技术和风控规则先由代码计算，LLM 负责做最后的综合解释、取舍和报告生成。

## 社区版定位

本项目为 **社区版 AI 投研复盘工作台**。核心功能保持开放，不通过功能锁限制社区用户。项目优先通过 Star、Issue、PR、文档补充和使用反馈推动长期迭代。

社区相关文档：

- [社区版说明](COMMUNITY.md)
- [社区版路线图](docs/community_roadmap.md)
- [社区支持](SUPPORT.md)
- [贡献指南](CONTRIBUTING.md)
- [安全反馈](SECURITY.md)
- [风险声明](DISCLAIMER.md)
- [用户协议草案](TERMS.md)
- [隐私政策草案](PRIVACY.md)

社区版不销售确定性荐股结果，不承诺收益，不代客交易，也不替用户作出真实投资决策。

### 支持我们

如果这个项目对你有帮助，欢迎 Star、反馈 Issue、提交 PR，或通过自愿支持帮助项目持续迭代。社区版核心功能保持开放，支持者权益围绕 LLM-TRADE 自身的复盘工作流、配置样例、Prompt 模板和实验功能预览设计，不包含投资建议、收益承诺或代客交易。

| 支持方式 | 支持等级 | 说明 |
| --- | --- | --- |
|每月 0 RMB | Community | 使用全部社区功能；通过 GitHub 获取更新；可参与 Issue、讨论和路线图反馈。 |
|每月 9.9 RMB | Backer | 作者致谢；优先整理和回应复现清晰的 Issue；提供基础配置指导；新增社区功能优先体验。|

## 核心能力

### 1. 本地数据湖

`DataPipeline` 会把基础股票信息、行情快照、多周期 K 线、指数 K 线和财务指标同步到本地 SQLite：

- 数据库：`data/stock_lake.db`
- 行情快照表：`daily_quotes`
- 日线 K 线表：`market_bars`
- 周线 K 线表：`market_bars_weekly`
- 月线 K 线表：`market_bars_monthly`
- 财务指标表：`financial_metrics`
- 观察仓候选池表：`watchlist_items`
- 模拟交易账户表：`trading_account`
- 模拟交易持仓表：`trading_positions`
- 模拟交易流水表：`trade_orders`
- 回测信号快照表：`backtest_signal_snapshots`
- 旧版观察仓兼容表：`paper_trades`

行情快照优先使用腾讯/新浪等免费接口。日线 K 线主源为 Tushare，日常 `--sync`
只做增量维护；历史缺口补全、周线/月线聚合分别由独立命令执行，避免盘后同步被
大批量历史回填或全市场聚合卡住。AKShare、efinance 补全/校验默认关闭，可在
`config/config.yaml` 中按需打开。

### 2. 多策略选股雷达

`StockScreener` 位于 `src/stock_screener.py`，是底层规则筛选器，不依赖 LLM。

它已经从单一趋势过滤升级为多策略雷达。股票只要满足任意一种策略探测器，就可以进入候选池：

- 经典趋势突破：右侧交易，均线多头，涨幅适中。
- 优质股底部低吸：低估值、阶段低位、底部量能活跃。
- 财报错杀/洗盘反转：短期急跌、估值有支撑、放量承接。
- 强势主升浪：均线完美发散、动量极强、资金接力，但过滤放量滞涨。
- 支撑回踩：趋势未坏，回踩关键均线或平台附近。
- 龙回头 / 妖股断板低吸：近 10 日出现过多次涨停，断板后首次大幅回调，回踩 MA5/MA10 附近且成交量未彻底熄火。
- 底部首板强突：把第一次涨停视为独立突破信号，用于捕捉主力启动初期，后续由交易 Agent 等待次日水下或均线附近低吸机会。

为了降低“假突破”和“接飞刀”的概率，规则层还补充了以下技术确认：

- 左侧低吸/恐慌反转必须有技术共振：RSI 超卖、MACD 绿柱改善或底背离，避免只因为短期跌幅大就接基本面恶化的真暴跌。
- 右侧主升浪和趋势突破加入 MA20 乖离率上限，防止连续涨停后价格远离均线过多，接到兑现潮前的最后一棒。
- 支撑回踩要求 K 线出现承接确认，例如收阳、下影线、十字星或收盘位于日内中上部，避免把阴跌破位误判成缩量企稳。
- aggressive 档开放次新股特别通道，可把最小历史长度降低到 30 个交易日；conservative/balanced 默认仍使用 60 日硬过滤。

候选股会带上 `strategy_tags`、`strategy_confidence` 和 `screen_reason`，方便后续 Agent 理解它为什么入选。

`MarketRegimeDetector` 还会统计最高连板高度、当日涨停/跌停数量。系统不会直接追买高位连板股，但会把连板高度作为市场情绪温度计：最高连板达到 7 板以上时偏向 aggressive；最高连板只有 2 板且跌停压力明显时偏向 conservative 或空仓防守。

### 3. 回测模块

回测模块是一个独立评估层，当前优先服务多策略选股雷达。它不直接替代 Agent 决策，也不让当前候选“自证”，而是用历史截面做遮盖式走步回测：先遮住某个历史交易日之后的数据，只允许选股器读取该日及以前的信息生成候选，再揭开后续行情计算胜率与收益表现。

回测结果会沉淀为 `walk_forward_masked` 来源的策略级权重，只用于优化 `src/stock_screener.py` 里的规则雷达打分。真实 `--pick` 时，`StockScreener` 会在规则海选阶段读取这些权重，前置校准 `technical_score`；`quick_filter_agent` 不读取、不计算、也不消费遮盖式走步回测结果，它只负责后续宏观环境判断。这样避免把本轮候选拿来反向证明本轮候选，也避免把回测逻辑散落到 Agent Prompt 里。

第一阶段目标：

- 按 `strategy_tags`、市场状态、行业主题、技术形态和财务质量等维度拆分样本，统计不同因子组合的胜率、收益回撤比、最大回撤、持有周期收益分布和信号衰减速度。
- 对多策略雷达输出的 `strategy_confidence`、主题加分、技术确认、风险扣分等因子做权重评估，识别哪些因子在不同 market regime 下更有效。
- 形成可回放的候选股入选快照，避免只用当前数据库状态复盘历史信号。
- 输出因子权重建议和策略表现摘要，并前置应用到 `StockScreener` 的多策略雷达分数中；后续 Agent 只读取已经筛选和校准后的候选池，不参与回测权重生成。
- 沉淀低效或反向有效的信号，例如高位放量滞涨、假突破、财报错杀失效、龙回头失败形态等，反向补充到规则过滤和风险提示中。

回测模块的定位是“评估与校准”，不是“事后拟合”。走步回测生成历史样本时会关闭已有回测权重，确保每个历史截面只使用当时可见的信息；每次调节权重都应保留原始样本、统计口径和生效时间，方便后续比较新旧规则的真实效果。

### 4. 深度财务数据

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

### 5. 量化技术信号

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

### 6. 退出机制闭环

`ExitAgent` 位于 `src/agent/exit_agent.py`。

它不依赖 LLM，负责把“只管买，不管卖”的缺口补上第一版：

- 跌破 ATR 动态止损：清仓退出。
- 跌破 20 日平台：清仓退出。
- 已有浮盈但跌回 MA20 下方：移动止盈保护。
- 放量滞涨/上影派发：减仓观察。
- 宏观风险偏好偏低且仍有浮盈：锁定部分利润。

`PaperTrading` 会保留原有收益率止盈止损规则，并在 `ExitAgent` 给出更严重信号时自动升级动作。

### 7. 观察仓与交易仓

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

1. `StockScreener` 读取 `walk_forward_masked` 回测权重，进行多策略规则海选和技术分前置校准，按主策略分组配额生成约 20 只候选池，避免单一策略霸榜。
2. `AgentCoordinator` 记录本轮海选信号快照，供未来持有周期完成后进入回测样本；本轮精筛不使用当前候选做“自证”。
3. `MacroAgent` 判断市场状态、风险偏好和主线方向；它与遮盖式走步回测无关。
4. `QuickFilterAgent` 读取宏观上下文和已校准的候选股极简快照，轻量精筛出最多 8 只进入资讯硬风控；候选不足时直接放行。
5. `NewsRiskAgent` 先检查公告、新闻和重大风险词；命中 `hard_exclude` 的候选会立即停止后续基本面和技术面深度复核，避免继续消耗 Agent/LLM 资源。
6. `FundamentalAgent` 仅对通过资讯硬风控的候选，结合财务数据和公告做基本面复核。
7. `TechnicalAgent` 仅对通过资讯硬风控的候选，结合量化技术信号和 K 线摘要做技术复核。
8. `DecisionAgent` 保留最后一道资讯硬风控兜底校验，并综合排序输出最终推荐报告。
9. `Watchlist` 将推荐和分析沉淀为观察仓候选池。
10. `TradingAgent` 在 `--trade` 中基于观察仓和交易仓执行模拟调仓。
11. `ExitAgent` 和 `ReflectionAgent` 在盘后进行持仓诊断与亏损反思。

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
cd frontend
npm install
cd ..
```

### 2. 配置环境变量

不要把真实 API Key 写入仓库文件。推荐使用环境变量：

```powershell
$env:DEEPSEEK_API_KEY="your-key"
$env:TAVILY_API_KEY="your-key"
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

盘后日常增量同步基础数据、行情快照、指数 K 线和最新日线：

```bash
python main.py --sync
```

`--sync` 只做日常增量维护，不会自动补 10 年历史，也不会自动派生周线/月线。

首次建库或历史 K 线被清空后，先单独补全近 10 年日线缺口：

```bash
python main.py --backfill-bars
```

日线补齐后，再基于本地日线库派生周线/月线：

```bash
python main.py --derive-bars
```

推荐顺序：

```text
python main.py --backfill-bars
python main.py --derive-bars
python main.py --sync
python main.py --backtest
```

当前默认数据源策略：

- 日线主源：Tushare。
- AKShare 日线补全：默认关闭，配置项 `enable_akshare_daily_fallback: false`。
- efinance 抽样校验/补全：默认关闭，配置项 `enable_efinance_validation: false`、`enable_efinance_fallback: false`。
- 周线/月线：不从外部数据源拉取，统一由本地日线聚合生成，并分别写入 `market_bars_weekly`、`market_bars_monthly`。

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

### 5. 运行遮盖式走步回测

```bash
python main.py --backtest
```

该命令会从历史交易日中选取多个截面，对每个截面只开放当日及以前的数据给 `StockScreener`，生成当时可见的候选池；随后再用被遮盖的后续 K 线计算 3/5/10/20 日等持有周期的胜率、平均收益和回撤代理指标。结果会写入：

- `backtest_signal_snapshots`：历史截面的候选信号快照。
- `outputs/latest_backtest_report.json`：策略表现、样本数量、收益统计和权重建议。

真实选股时，`StockScreener` 会读取 `walk_forward_masked` 来源的策略权重并前置校准技术分；走步回测自身生成样本时会关闭该权重，避免未来样本污染历史推演。该流程只服务 `StockScreener` 的规则优化，不接入 `src/agent/macro_agent.py`。

### 6. 执行模拟交易

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

### 7. 指定股票单独分析

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

### 8. 报告内容口径

`outputs/latest_report.md` 是完整选股流程的最终决策报告，结论是候选池内的相对排序，不等同于所有入选股票都是同一强度的买入建议。报告统一使用以下分层：

- 强推荐：基本面强、技术形态明确、资讯风控低，适合作为本轮核心候选。
- 配置/轻仓验证：防御属性、回踩支撑、低风险配置或赔率一般但胜率较稳，只能轻仓或等触发条件。
- 观察：单独看有亮点，但买点、趋势、成长性或风控仍不够清晰。
- 不推荐：基本面、技术面或资讯风控存在明显短板。

`outputs/latest_targeted_analysis.md` 是指定股票的绝对诊断报告，不参与候选池相对排名，也不会自动加入观察仓。因此同一只股票可能在选股报告里属于“配置/轻仓验证”，但在单独分析里显示为中性、谨慎或等待买点；这表示它是弱市下的防御型备选，而不是进攻型强推荐。

### 9. 盘后观察仓与交易仓诊断

```bash
python main.py --post
```

该流程会：

- 更新观察仓和交易仓最新价格。
- 计算观察仓浮动收益和交易仓账户权益。
- 调用宏观环境判断。
- 通过 `ExitAgent` 评估是否减仓或清仓。
- 对观察仓亏损和交易仓亏损案例生成反思并沉淀到规则本。

### 10. 启动可视化工作台

工作台前端已迁移到 `frontend/`，使用 Vue + Vite 开发，后端 `--dashboard` 会托管 `frontend/dist` 中的构建产物。

首次启动或修改前端后，先构建前端：

```bash
cd frontend
npm install
npm run build
cd ..
```

然后启动本地工作台：

```bash
python main.py --dashboard
```

默认访问地址：

- <http://127.0.0.1:8765>

工作台会直接读取本地 `outputs` 和 `data/stock_lake.db`，展示最新研报、观察仓、交易仓、交易流水、规则筛选审计、回测摘要和个股近期走势。也可以指定监听地址和端口：

```bash
python main.py --dashboard --dashboard-host 127.0.0.1 --dashboard-port 8765
```

前端源码位于 `frontend/`：

- `frontend/src/`：Vue 页面、组件、样式和 API 调用。
- `frontend/public/`：静态资源。
- `frontend/dist/`：`npm run build` 生成的构建产物，供 `python main.py --dashboard` 托管。
- `frontend/package.json`：前端依赖和开发脚本。

如果要单独调试前端，可以在一个终端启动后端 API：

```bash
python main.py --dashboard
```

另一个终端启动 Vite 开发服务：

```bash
cd frontend
npm run dev
```

当前工作台已经从“只读展示”落地为可操作前端：

- 可以直接查看观察仓、交易仓、交易流水、最新研报、审计摘要、回测摘要和个股近期走势。
- 可以在二级运行配置窗口中编辑 `config/config.yaml` 的中文化参数表单。
- 可以通过按钮触发原来的 bash 命令，包括同步数据、补历史日线、派生周/月线、执行选股、走步回测、模拟调仓和盘后诊断。
- 可以输入股票代码触发指定分析，相当于执行 `python main.py --analyze 600519 000001`。
- 任务控制台的功能按钮会先弹出二次确认窗口，确认后才会启动后台任务，避免误触。
- 每个从前端触发的任务会写入 `outputs/dashboard_jobs/*.log`，页面会展示任务状态和日志尾部。

前端触发任务走本地白名单 API，不接收任意 shell 字符串。对应关系如下：

| 前端按钮 | 后端触发 |
| --- | --- |
| 同步数据 | `python main.py --sync` |
| 补历史日线 | `python main.py --backfill-bars` |
| 派生周/月线 | `python main.py --derive-bars` |
| 执行选股 | `python main.py --pick` |
| 走步回测 | `python main.py --backtest` |
| 模拟调仓 | `python main.py --trade` |
| 盘后诊断 | `python main.py --post` |
| 指定分析 | `python main.py --analyze <codes...>` |

## 配置文件

- `config/config.yaml`：LLM、搜索、数据保留窗口、并发数等运行配置。
- `config/stock_picking.yaml`：选股 profile、多策略筛选参数、主题加分和市场状态配置。

`stock_picking.yaml` 中与新增策略相关的关键参数包括：

- `max_momentum_bias20` / `max_trend_bias20`：控制主升浪、趋势突破相对 MA20 的最大乖离率。
- `allow_new_stock_channel` / `min_new_stock_history_days`：控制是否允许次新股特别通道，当前仅 aggressive 默认开启。
- `limit_up_change_pct` / `limit_down_change_pct`：定义涨停、跌停估算阈值，也用于龙回头、首板和市场情绪统计。
- `hot_limit_streak` / `cold_limit_streak`：定义连板高度的热度/冰点阈值，用于自动切换市场 Regime。

## 主要输出

- `outputs/latest_report.md`：最近一次选股报告。
- `outputs/latest_workflow_audit.json`：最近一次完整工作流审计。
- `outputs/screener_audit.json`：规则预筛审计。
- `outputs/latest_backtest_report.json`：回测模块生成的策略表现和权重参考。
- `outputs/latest_agent_trace.jsonl`：Agent 调用轨迹。
- `data/rules_book.txt`：亏损反思沉淀出的风控规则。
- `watchlist_items`：观察仓候选池，最多 10 只。
- `trading_account`：模拟交易账户和账户权益。
- `trading_positions`：当前交易仓持仓，最多 5 只。
- `trade_orders`：模拟交易流水和交易理由。

## 社区与支持

社区版保持核心功能开放。欢迎通过 Star、Issue、PR、使用反馈和文档补充参与项目。遇到安装配置、模型接入、数据源配置或运行问题时，可以参考 [社区支持](SUPPORT.md) 中的反馈方式。

社区讨论和问题反馈不包含投资建议、收益承诺或代客交易。

## License

本项目采用 [Apache License 2.0](LICENSE)。

## 常用测试

```bash
python -m unittest test.test_financial_data
python -m unittest test.test_technical_indicators
python -m unittest test.test_exit_agent
python -m unittest test.test_backtest_engine
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
- `StockScreener` 已补充 RSI/MACD 反转共振、MA20 乖离率约束、支撑回踩 K 线承接确认，降低假突破和接飞刀风险。
- 新增次新股 aggressive 特别通道、`dragon_pullback` 龙回头策略和 `first_limit_up_breakout` 底部首板强突策略。
- `MarketRegimeDetector` 新增最高连板高度、涨停/跌停数量监测，把市场情绪热度纳入 profile 自动切换。
- 新增遮盖式走步回测模块：用历史截面隐藏未来数据生成候选，再揭开后续行情统计胜率、收益和策略权重。
- `StockScreener` 已接入 `walk_forward_masked` 回测权重，真实选股时前置校准技术分；回测生成样本时关闭该权重，避免未来信息污染。
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

本项目仅用于研究、复盘和工程实验，不构成任何投资建议、投资顾问服务或收益承诺。股票市场有风险，真实交易前请自行验证数据质量、模型结论和个人风险承受能力。
