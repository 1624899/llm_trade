# AI选股Agent改造 - 任务清单

## Phase 1：基础数据层

- [x] 项目架构分析（16个模块、~8000行代码梳理）
- [x] 改造方案设计（架构图、模块分类、路线图）
- [x] 创建 `src/stock_universe.py` - 全市场股票池模块
  - [x] 全A股实时行情获取（`stock_zh_a_spot_em`）
  - [x] 三级数据降级策略（实时→磁盘缓存→K线补数据）
  - [x] 磁盘缓存机制（读写+过期管理）
  - [x] 列名标准化（中文→英文）
  - [x] 行业/概念板块获取（`stock_board_industry_name_em` 等）
  - [x] 量化筛选器（ST/北交所/市值/PE/PB/换手率/涨跌幅/量比）
  - [x] 策略筛选（value/growth/momentum/oversold/active）
  - [x] 便捷查询（涨幅榜/成交榜/搜索/低PE）
  - [x] LLM数据摘要（`get_stock_summary`）
  - [x] 市场总览（`get_market_overview`）
  - [x] 缓存管理
  - [ ] **Level 3 降级测试通过**（非交易时间限流问题待验证）
- [x] 创建 `config/stock_picking.yaml` - 选股配置文件
- [x] 创建 `test_stock_universe.py` - 测试脚本

## Phase 2：Agent核心引擎

- [ ] 创建 `src/agent/stock_picking_agent.py` - 选股Agent主类
- [ ] 创建 `src/agent/tools.py` - Agent工具集
- [ ] 重写 `src/prompt_generator.py` - 选股Prompt设计
- [ ] 修改 `src/llm_client.py` - 适配选股输出格式
- [ ] Agent多轮推理流程

## Phase 3：分析引擎

- [ ] 创建 `src/stock_screener.py` - 量化筛选器（高级版）
- [ ] 创建 `src/fundamental_analyzer.py` - 基本面分析
- [ ] 创建 `src/scoring_engine.py` - 多维度评分引擎
- [ ] 技术面分析（复用 `indicators.py`）
- [ ] 资金面分析

## Phase 4：输出与优化

- [ ] 创建 `src/report_generator.py` - 选股报告生成
- [ ] 修改 `main.py` - 新增选股模式入口
- [ ] 配置系统完善
- [ ] 端到端测试
- [ ] Web可视化界面（可选）

---

> **当前进度**: Phase 1 基本完成，Level 3 降级策略在非交易时间存在东方财富限流问题，交易时间应正常。
