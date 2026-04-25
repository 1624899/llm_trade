# LLM-TRADE 演进任务清单

## 1. 补足深度财务数据（短期目标）- 已完成第一版

### 原问题

`FundamentalAgent` 过去主要依赖 PE、PB、总市值、近期公告/新闻，缺少连续财务指标趋势，因此基本面判断容易变成“新闻阅读理解”，难以识别伪成长、利润质量恶化、现金流背离和财报粉饰风险。

### 已落地

- 新增 `src/financial_data.py`
  - 通过 AKShare 的东方财富财报接口抓取利润表、资产负债表、现金流量表和财务分析指标。
  - 支持 A 股代码格式转换：
    - 报表接口：`600519 -> SH600519`
    - 指标接口：`600519 -> 600519.SH`
  - 归一化最近 N 期核心财务指标：
    - 营收、营收同比
    - 归母净利、归母净利同比
    - 扣非归母净利、扣非同比
    - 毛利率、净利率、ROE、ROIC
    - 资产负债率、流动比率、速动比率、现金比率
    - 经营现金流、经营现金流同比
    - 经营现金流/归母净利
    - 总资产、总负债
  - 输出适合喂给 LLM 的财务摘要文本。

- 更新 `src/database.py`
  - 新增 `financial_metrics` 表。
  - 主键：`(code, report_date)`。
  - 增加按股票代码和报告期查询的索引。

- 更新 `src/data_pipeline.py`
  - 新增 `sync_financial_metrics(codes=None, periods=8)`。
  - 可按指定股票列表同步东方财富财务指标到本地数据库。
  - 暂未放进 `run_all()`，避免默认全市场同步造成请求过重。

- 更新 `src/agent/fundamental_agent.py`
  - 在基本面 Prompt 中加入东方财富财务摘要。
  - 要求 LLM 优先分析财务趋势，而不是只解读新闻：
    - 成长性：营收/利润/扣非利润是否连续改善。
    - 盈利质量：毛利率、净利率、ROE、ROIC 是否稳定。
    - 现金流质量：经营现金流是否覆盖利润。
    - 资产负债风险：负债率、流动性指标是否恶化。
    - 背离风险：利润增长但现金流变差、收入增长但毛利率下滑等。

### 已验证

- 单元测试：
  - `python -m unittest test.test_financial_data`
  - `python -m unittest test.test_financial_data test.test_database_and_screener.StockDatabaseUpsertTests`
  - `python -m unittest test.test_financial_data test.test_database_and_screener.StockDatabaseUpsertTests test.test_database_and_screener.DataPipelineNormalizationTests -q`

- 线上接口连通性：
  - 东方财富/AKShare 已成功拉取 `600519` 最近 4 期财务数据。
  - `DataPipeline().sync_financial_metrics(codes=["600519"], periods=4)` 已成功写入本地 `financial_metrics`。

### 后续可增强

- 为 `financial_metrics` 增加缓存/新鲜度判断，避免每天重复抓取同一批历史财报。
- 在 `Coordinator` 或选股主流程中，对入围候选股自动同步财务指标，再交给 `FundamentalAgent` 分析。
- 增加更多财务排雷规则：
  - 利润增长但经营现金流连续低于净利润。
  - 营收增长但毛利率连续下滑。
  - ROE 下滑且负债率上升。
  - 扣非净利显著弱于归母净利。
  - 应收/存货异常扩张（需要继续补充字段）。

## 2. 技术面分析精细化（中期目标）- 已完成第一版

### 当前问题

`TechnicalAgent` 拿到的是文本化 K 线摘要。LLM 对价格数字缺乏直观形态感知，难以稳定识别复杂结构，例如头肩底、箱体突破、缩量洗盘、放量滞涨等。

### 已落地

- 新增 `src/technical_indicators.py`
  - 从本地 `market_bars` 读取日线 OHLCV。
  - 计算 ATR14、MA5/10/20/60、5日/20日涨跌幅、MA20 乖离率、60日量能分位、量比。
  - 计算 20日/60日支撑压力位。
  - 输出交易计划参考：
    - 回踩区。
    - 突破确认价。
    - 初始止损位。
    - 第一目标观察位。

- 形态/风险标签第一版
  - 均线多头发散。
  - 箱体放量突破。
  - 缩量回踩 MA20。
  - 临近 20 日支撑/压力。
  - 恐慌放量。
  - 强动量延续。
  - 跌破 20 日平台。
  - 放量滞涨/上影派发。

- 更新 `src/agent/technical_agent.py`
  - 在 LLM Prompt 中加入“量化技术信号摘要”。
  - 要求 LLM 优先使用确定性指标，再结合多周期 K 线和宏观环境。
  - 要求输出明确的买点、突破触发价、止损/失效位、量能风险和操作建议。
  - 对“放量滞涨/上影派发”“跌破20日平台”等风险标签强制降低追高倾向。

- 新增 `test/test_technical_indicators.py`
  - 覆盖箱体放量突破。
  - 覆盖放量滞涨风险。
  - 覆盖 K 线样本不足。

### 已验证

- `python -m py_compile src/technical_indicators.py src/agent/technical_agent.py`
- `python -m unittest test.test_technical_indicators`
- `python -c "from src.agent.technical_agent import TechnicalAgent; print(TechnicalAgent.__name__)"`
- `python -m unittest test.test_runtime_regressions.CoordinatorConcurrencyTests test.test_technical_indicators`

### 建议方向

- 继续增强量化技术指标层：
  - 更稳健的箱体识别。
  - 趋势线/平台线确认。
  - 波段高低点结构。
  - 量价背离。
  - 涨停/跌停后的承接判断。
  - 可复用到后续 `ExitAgent` 的移动止损逻辑。
- 让 `TechnicalAgent` 输出更具体的交易计划：
  - 理想买点。
  - 失效点。
  - 第一止盈位。
  - 移动止损条件。

## 3. 退出机制闭环（中期目标）- 已完成第一版

### 当前问题

系统重心仍在“选股/Pick”，尚未覆盖真实交易中的持仓生命周期管理。

### 建议方向

- 新增 `src/agent/exit_agent.py`
  - 不依赖 LLM，先用确定性规则完成“是否该卖”的基础闭环。
  - 复用 `TechnicalSignalProvider` 的 ATR、平台支撑、风险标签和均线状态。
  - 输出结构化动作：
    - 继续持有。
    - 等待确认。
    - 减仓观察。
    - 清仓退出。

- 更新 `src/evaluation/paper_trading.py`
  - 保留原有收益率止损/止盈规则。
  - 接入 `ExitAgent`，当技术/宏观信号比收益率规则更严重时，自动升级持仓动作。
  - 支持技术触发：
    - 跌破 ATR 动态止损。
    - 跌破 20 日平台失效价。
    - 技术标签显示跌破 20 日平台。
    - 已有浮盈但跌回 MA20 下方，触发移动止盈保护。
    - 放量滞涨/上影派发，先减仓观察。
  - 支持宏观触发：
    - 宏观风险偏好偏低且持仓仍有浮盈时，建议锁定部分利润。

- 更新 `src/agent/coordinator.py`
  - 盘后例行流程会先获取 `MacroAgent` 的宏观环境，再传给观察仓诊断。

- 新增 `test/test_exit_agent.py`
  - 覆盖平台破位清仓。
  - 覆盖放量滞涨减仓。
  - 覆盖 `PaperTrading` 根据更强退出信号升级动作。

### 已验证

- `python -m py_compile src/agent/exit_agent.py src/evaluation/paper_trading.py src/agent/coordinator.py`
- `python -m unittest test.test_exit_agent test.test_database_and_screener.PaperTradingPostMarketTests test.test_runtime_regressions.CoordinatorConcurrencyTests`

### 后续可增强

- 每天开盘前评估当前持仓是否触发：
  - 宏观逻辑失效。
  - 板块主线退潮。
  - 技术破位。
  - 基本面暴雷。
  - 达到止盈或移动止损。
- 给 `paper_trades` 增加更多持仓字段：
  - 计划买点。
  - 初始止损。
  - 移动止损。
  - 目标价。
  - 入选策略标签。
- 让 `ExitAgent` 后续接入基本面暴雷信号和新闻风险信号，完成“宏观、技术、基本面、消息面”四类退出条件。

## 4. 图表多模态分析（长期目标）

### 建议方向

条件允许时，将 K 线图、成交量、MACD、筹码分布或支撑压力图生成截图，交给支持视觉能力的模型分析。

优先级低于财务数据和退出机制，因为多模态依赖模型能力和图表渲染稳定性，工程复杂度更高。
