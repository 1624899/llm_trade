# 交易 Agent 与观察仓重构实施方案

## 1. 目标

本阶段把系统从“推荐股票 + 观察仓复盘”升级为“推荐池 + 交易仓 + 亏损反思”的闭环模拟交易系统。

新增一个独立的交易 Agent，给它一个持久模拟账户，初始资金 16000 元。账户只在首次初始化时注入资金，后续每次运行都沿用数据库中的现金、持仓、交易流水和盈亏状态，不会自动重置。

系统仍然只做研究和模拟，不接入真实券商，不发起实盘下单。

## 2. 核心规则

- 新增独立命令：`python main.py --trade`
- `--pick` 负责生成推荐并更新观察仓，不直接等同买入。
- `--trade` 根据观察仓、交易仓、当前价格和风控信号执行模拟买卖。
- `--trade` 若发现观察仓为空，会先补候选池：已有交易仓持仓时对持仓代码做指定分析并写回观察仓；交易仓也为空时自动执行一轮选股。
- `--post` 负责盘后诊断与亏损反思。
- 观察仓最多维护 10 只股票。
- 交易仓最多同时持有 5 只股票。
- 初始资金为 16000 元，只初始化一次，之后账户持久滚动。
- 每次运行时的当前行情快照就是模拟成交价格。
- 默认按 A 股 100 股一手计算，现金不足一手时不买入。
- 买入后至少持有 5 个交易日。
- 卖出后 5 个交易日内不允许回买同一股票。
- 禁止频繁交易：单次运行不允许同一股票反复买卖，默认最多 2 笔买入、2 笔卖出。
- 如果基本面、技术面或资讯风险明显恶化，可突破最短持有期触发减仓或清仓。

## 3. 数据库设计

### 3.1 观察仓表：`watchlist_items`

用于保存最多 10 只候选股票及其最新分析，不代表真实买入。

建议字段：

- `id`
- `code`
- `name`
- `tier`
- `watch_status`
- `source`
- `added_at`
- `updated_at`
- `entry_price`
- `current_price`
- `return_pct`
- `expected_return_pct`
- `recommend_reason`
- `fundamental_analysis`
- `technical_analysis`
- `news_risk_analysis`
- `macro_context`
- `remove_reason`

唯一约束：`code`。

### 3.2 交易账户表：`trading_account`

用于保存模拟账户现金和总资产快照。

建议字段：

- `id`
- `account_name`
- `initial_cash`
- `cash`
- `total_market_value`
- `total_equity`
- `realized_pnl`
- `unrealized_pnl`
- `created_at`
- `updated_at`

默认账户名：`default`。

### 3.3 交易持仓表：`trading_positions`

用于保存当前交易仓持股。

建议字段：

- `id`
- `account_id`
- `code`
- `name`
- `quantity`
- `avg_cost`
- `current_price`
- `market_value`
- `unrealized_pnl`
- `unrealized_return_pct`
- `opened_at`
- `last_buy_at`
- `last_sell_at`
- `status`
- `linked_watchlist_id`
- `buy_reason`
- `risk_note`

当前持仓只统计 `status = 'OPEN'`，最多 5 只。

### 3.4 交易流水表：`trade_orders`

用于保存每次交易 Agent 的动作与理由，包括买入、卖出和明确持有。

建议字段：

- `id`
- `account_id`
- `code`
- `name`
- `action`
- `quantity`
- `price`
- `amount`
- `cash_before`
- `cash_after`
- `position_before`
- `position_after`
- `reason`
- `decision_snapshot`
- `linked_watchlist_id`
- `created_at`

`action` 取值建议：`BUY`、`SELL`、`HOLD`、`WATCH`、`REMOVE`。

## 4. 模块设计

### 4.1 `Watchlist`

建议新增模块：`src/evaluation/watchlist.py`

职责：

- 接收 `DecisionAgent` 的推荐结果和个股分析。
- 新增或更新观察仓条目。
- 保持观察仓最多 10 只。
- 优先保留强推荐和配置/轻仓验证标的。
- 对风险恶化、长期无效、重复低质量标的做移除或降级。
- 接收 TradingAgent 的 `REMOVE` 决策和硬风险清仓信号，同步把对应标的移出观察仓。
- 为交易 Agent 输出结构化候选池。

### 4.2 `TradingAccount`

建议新增模块：`src/evaluation/trading_account.py`

职责：

- 初始化默认账户，初始资金 16000 元。
- 读取现金、持仓、交易流水。
- 按当前价格刷新持仓市值和浮盈亏。
- 执行模拟成交。
- 写入 `trade_orders`。
- 强制账户级约束：现金不足不买、最多 5 个持仓、100 股一手。

### 4.3 `TradingAgent`

建议新增模块：`src/agent/trading_agent.py`

职责：

- 读取观察仓候选、当前交易仓、现金、宏观环境、ExitAgent 退出信号和历史反思规则。
- 输出结构化交易决策。
- 判断是否买入、继续持有、减仓、清仓或只观察。
- 对低风险且中期预期收益大于 15% 的股票，可以给出清晰配置判断，不因短期波动过度保守。
- 对基本面恶化、技术面破位、放量滞涨、趋势背离或资讯硬风险标的，明确减仓、清仓或回避。

LLM 失败时必须有规则兜底：

- 已持仓且触发 ExitAgent 清仓信号：卖出。
- 已持仓但未触发风险：持有。
- 未持仓：只观察，不自动买入。

## 5. 运行流程

### 5.1 `--pick`

流程：

1. 规则海选。
2. 多 Agent 深度分析。
3. DecisionAgent 生成最终推荐。
4. 将推荐和完整分析写入 `watchlist_items`。
5. 输出选股报告。

`--pick` 不再直接写入交易仓。

### 5.2 `--trade`

流程：

1. 初始化或读取默认交易账户。
2. 刷新观察仓和交易仓当前价格。
3. 若观察仓为空，先补候选池：有持仓则指定分析持仓，无持仓则自动选股。
4. 获取宏观环境。
5. 对已有持仓调用 ExitAgent。
6. 调用 TradingAgent 生成交易决策。
7. 按 `REMOVE` 或硬风险清仓同步维护观察仓。
8. TradingAccount 校验硬约束。
9. 执行模拟买入、卖出或持有。
10. 写入交易流水。
11. 输出交易报告。

### 5.3 `--post`

流程：

1. 刷新交易仓浮盈亏。
2. 输出交易仓盘后诊断。
3. 查找亏损或清仓亏损交易。
4. ReflectionAgent 结合推荐内容、交易行为、持有周期和盈亏结果生成反思。
5. 将反思规则写入 `data/rules_book.txt`。

## 6. 反思升级

亏损反思不再只看观察仓浮亏，而是结合完整交易链路：

- 推荐时的分层和预期收益。
- 基本面、技术面、资讯风险分析。
- 买入时间、价格、数量和理由。
- 卖出时间、价格、数量和理由。
- 持有天数。
- 是否违反交易纪律。
- 最终实现盈亏和最大浮亏。

反思输出必须沉淀为可复用风控规则，供 `DecisionAgent` 和 `TradingAgent` 后续共同加载。

## 7. 实施顺序

1. 新增数据库表和兼容迁移。
2. 新增 Watchlist，替代 `paper_trades` 的观察仓职责。
3. 新增 TradingAccount，完成账户初始化、持仓刷新、成交写入。
4. 新增 TradingAgent，先实现结构化决策和规则兜底。
5. 在 `main.py` 增加 `--trade`。
6. 调整 `AgentCoordinator`：`--pick` 更新观察仓，`--trade` 执行交易流程。
7. 升级 ReflectionAgent，使其读取交易流水和亏损结果。
8. 补齐单元测试和回归测试。

## 8. 测试验收

- 新库启动后自动创建观察仓、交易账户、交易持仓和交易流水表。
- 旧 `paper_trades` 的 HOLD 数据可以迁移或兼容读取。
- `--pick` 后观察仓最多 10 只。
- `--trade` 首次运行创建 16000 元账户。
- `--trade` 在观察仓为空时会自动补充候选池。
- 再次运行 `--trade` 不重置资金。
- 买入后交易仓最多 5 只。
- 买入后 5 个交易日内禁止普通卖出。
- 卖出后 5 个交易日内禁止回买。
- 风险恶化时允许提前减仓或清仓。
- 每笔 BUY/SELL/HOLD 都写入 `trade_orders` 并包含交易理由。
- 亏损反思能关联推荐内容和交易行为。
