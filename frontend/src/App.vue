<template>
  <div class="app-container">
    <header>
      <div class="topbar">
        <div>
          <h1>LLM-TRADE 可视化工作台</h1>
          <div class="subtle">本地刷新 {{ overview.generated_at || '加载中...' }}</div>
        </div>
        <div class="actions">
          <input v-model="stockInput" @keyup.enter="handleLoadStock" placeholder="输入代码" maxlength="6" />
          <button @click="handleLoadStock" :disabled="stockLoading">{{ stockLoading ? '读取中...' : '查看' }}</button>
          <button @click="openConfig">运行配置</button>
          <button class="primary" @click="handleRefresh" :disabled="refreshing">{{ refreshing ? '刷新中...' : '刷新' }}</button>
        </div>
      </div>
    </header>

    <main v-if="overview.generated_at">
      <section class="metrics">
        <div class="metric"><label>总权益</label><strong>{{ money(account.total_equity || account.initial_cash) }}</strong><div class="subtle">交易账户</div></div>
        <div class="metric"><label>现金</label><strong>{{ money(account.cash) }}</strong><div class="subtle">可用模拟资金</div></div>
        <div class="metric"><label>已实现盈亏</label><strong :class="clsByNumber(account.realized_pnl)">{{ money(account.realized_pnl) }}</strong><div class="subtle">历史持仓</div></div>
        <div class="metric"><label>浮动盈亏</label><strong>{{ money(account.unrealized_pnl) }}</strong><div class="subtle">未实现</div></div>
        <div class="metric"><label>总盈亏</label><strong :class="clsByNumber(totalPnl)">{{ money(totalPnl) }}</strong><div class="subtle">已实现 + 未实现</div></div>
        <div class="metric"><label>观察标的</label><strong>{{ watchlist.length }}</strong><div class="subtle">ACTIVE</div></div>
        <div class="metric"><label>持仓数量</label><strong>{{ positions.length }}</strong><div class="subtle">OPEN</div></div>
      </section>

      <section class="layout">
        <div class="stack left-column">
          <section class="panel">
            <h2>观察仓</h2>
            <div class="panel-body">
              <div v-if="!watchlist.length" class="empty">观察仓暂无 ACTIVE 标的</div>
              <div class="table-wrap watch-table" v-else>
                <table>
                  <thead><tr><th>代码</th><th>名称</th><th>档位</th><th>入选价</th><th>现价</th><th>收益</th><th>更新</th></tr></thead>
                  <tbody>
                    <tr v-for="row in watchlist" :key="row.code" class="clickable" @click="loadStockDetail(row.code)">
                      <td><strong>{{ row.code }}</strong></td>
                      <td>{{ row.name }}</td>
                      <td><span class="badge accent">{{ row.tier }}</span></td>
                      <td>{{ money(row.entry_price) }}</td>
                      <td>{{ money(row.current_price) }}</td>
                      <td><span :class="['badge', clsByNumber(row.return_pct)]">{{ pct(row.return_pct) }}</span></td>
                      <td>{{ String(row.updated_at).slice(0, 10) }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>交易仓</h2>
            <div class="panel-body">
              <div class="cash-form">
                <input v-model.number="cashInput" type="number" min="0" step="100" placeholder="设置现金" />
                <label class="check-field"><input v-model="resetTradeBaseline" type="checkbox" /> 重置基准</label>
                <button @click="handleSetCash" :disabled="cashSaving">{{ cashSaving ? '设置中...' : '设置现金' }}</button>
              </div>
              <div v-if="!positions.length" class="empty">交易仓当前无 OPEN 持仓</div>
              <div class="table-wrap position-table" v-else>
                <table>
                  <thead><tr><th>代码</th><th>名称</th><th>持有</th><th>已卖</th><th>成本</th><th>现价</th><th>未实现</th><th>已实现</th><th>持仓盈亏</th></tr></thead>
                  <tbody>
                    <tr v-for="row in positions" :key="row.code" class="clickable" @click="loadStockDetail(row.code)">
                      <td><strong>{{ row.code }}</strong></td>
                      <td>{{ row.name }}</td>
                      <td>{{ money(row.quantity) }}</td>
                      <td>{{ money(row.sold_quantity || 0) }}</td>
                      <td>{{ money(row.avg_cost) }}</td>
                      <td>{{ money(row.current_price) }}</td>
                      <td>{{ money(row.unrealized_pnl) }} <span :class="['badge', clsByNumber(row.unrealized_return_pct)]">{{ pct(row.unrealized_return_pct) }}</span></td>
                      <td>{{ money(row.realized_pnl) }}</td>
                      <td>{{ money(positionTotalPnl(row)) }} <span :class="['badge', clsByNumber(positionTotalPnl(row))]">{{ pct(positionTotalReturnPct(row)) }}</span></td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div class="subsection-title">历史持仓</div>
              <div v-if="!historyPositions.length" class="empty compact">暂无 CLOSED 历史持仓</div>
              <div class="table-wrap history-table" v-else>
                <table>
                  <thead><tr><th>代码</th><th>名称</th><th>状态</th><th>卖出数量</th><th>成本</th><th>卖出价</th><th>已实现盈亏</th><th>反思</th></tr></thead>
                  <tbody>
                    <tr v-for="row in historyPositions" :key="row.id" class="clickable" @click="loadStockDetail(row.code)">
                      <td><strong>{{ row.code }}</strong></td>
                      <td>{{ row.name }}</td>
                      <td><span class="badge">{{ historyStatusText(row) }}</span></td>
                      <td>{{ money(row.sold_quantity) }}</td>
                      <td>{{ money(row.avg_cost) }}</td>
                      <td>{{ money(row.current_price) }}</td>
                      <td>{{ money(row.realized_pnl) }} <span :class="['badge', clsByNumber(row.realized_return_pct)]">{{ pct(row.realized_return_pct) }}</span></td>
                      <td>{{ row.last_reflected_at ? '已反思' : '待反思' }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>审计与回测</h2>
            <div class="panel-body">
              <div class="cards">
                <div class="card"><div class="card-title">市场状态</div><div style="white-space:pre-wrap">{{ screener.regime }} / {{ screener.profile }}</div><div class="subtle">{{ screener.regime_reason }}</div></div>
                <div class="card"><div class="card-title">规则筛选</div><div>{{ screener.candidate_count }} / {{ screener.input_stock_count }}</div><div class="subtle">拒绝 {{ screener.rejected_count }} 只</div></div>
                <div class="card"><div class="card-title">拒绝原因 Top</div><div style="white-space:pre-wrap">{{ formatRejects(screener.top_rejects) }}</div></div>
                <div class="card"><div class="card-title">工作流</div><div>{{ audit.prefilter_count }} 入池 / {{ audit.selected_count }} 入选</div><div class="subtle">耗时 {{ money(audit.elapsed_seconds) }} 秒</div></div>
                <div class="card"><div class="card-title">回测</div><div>{{ backtest.window_count }} 窗口 / {{ backtest.evaluated_count }} 样本</div><div class="subtle">{{ backtest.summary }}</div></div>
              </div>
            </div>
          </section>

        </div>

        <div class="stack main-column">
          <section class="panel stock-panel" v-show="activeStock">
            <h2>个股详情 - {{ activeStockTitle }}</h2>
            <div class="panel-body">
              <div id="priceChart" class="chart-container"></div>
              <div class="split stock-summary">
                <div class="card">
                  <div class="card-title">仓位与观察</div>
                  <div class="kv" v-if="activeStock">
                    <span>行业</span><strong>{{ activeStock.basic?.industry || '-' }}</strong>
                    <span>观察档位</span><strong>{{ activeStock.watch?.tier || '-' }}</strong>
                    <span>观察收益</span><strong>{{ pct(activeStock.watch?.return_pct) }}</strong>
                    <span>持仓数量</span><strong>{{ money(activeStock.position?.quantity) }}</strong>
                    <span>持仓状态</span><strong>{{ activeStock.position?.status || '-' }}</strong>
                    <span>持仓收益</span><strong>{{ activeStockReturnText }}</strong>
                    <span>更新</span><strong>{{ activeStock.watch?.updated_at || activeStock.position?.opened_at || '-' }}</strong>
                  </div>
                </div>
                <div class="card" v-if="activeStock">
                  <div class="card-title">最近财务</div>
                  <div v-if="!activeStock.financials?.length && !activeStock.financial_summary" class="empty">暂无财务摘要</div>
                  <div v-else-if="!activeStock.financials?.length" class="finance-summary">
                    {{ activeStock.financial_summary }}
                  </div>
                  <div class="table-wrap finance-table" v-else>
                    <table>
                      <thead><tr><th>报告期</th><th>营收YoY</th><th>净利YoY</th><th>ROE</th></tr></thead>
                      <tbody>
                        <tr v-for="row in activeStock.financials" :key="row.report_date">
                          <td>{{ row.report_date }}</td>
                          <td>{{ pct(row.revenue_yoy) }}</td>
                          <td>{{ pct(row.parent_netprofit_yoy) }}</td>
                          <td>{{ pct(row.roe) }}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </section>

          <section class="panel report-panel">
            <div class="panel-tabs">
              <button
                v-for="tab in reportTabs"
                :key="tab.name"
                :class="['tab-button', { active: activeReportName === tab.name }]"
                @click="loadReport(tab.name)"
                :disabled="reportLoading && activeReportName === tab.name"
              >
                {{ tab.label }}
              </button>
            </div>
            <div class="panel-body">
              <div class="report markdown-body" id="report" v-html="reportHtml"></div>
            </div>
          </section>
        </div>

        <div class="stack right-column">
          <section class="panel">
            <h2>任务控制台</h2>
            <div class="panel-body">
              <div class="task-grid">
                <button v-for="task in availableTasks" :key="task.key" @click="requestTaskConfirmation(task.key)" :disabled="taskLoading[task.key]">
                  {{ taskLoading[task.key] ? task.label + '...' : task.label }}
                </button>
              </div>
              <div class="inline-form">
                <input v-model="analyzeInput" placeholder="指定分析：600519, 000001" />
                <button @click="requestTaskConfirmation('analyze')" :disabled="taskLoading['analyze']">
                  {{ taskLoading['analyze'] ? '分析...' : '分析' }}
                </button>
              </div>
              
              <div v-if="!jobs.length" class="empty">暂无从工作台触发的任务</div>
              <template v-else>
                <table>
                  <thead><tr><th>任务</th><th>状态</th><th>开始</th><th>日志</th></tr></thead>
                  <tbody>
                    <tr v-for="row in jobs.slice(0, 5)" :key="row.id">
                      <td>{{ row.label }}</td>
                      <td><span :class="['badge', statusClass(row.status)]">{{ row.status }}</span></td>
                      <td>{{ String(row.started_at).slice(5, 19) }}</td>
                      <td><button @click="viewJobLog(row.id)">查看</button></td>
                    </tr>
                  </tbody>
                </table>
              </template>
              <div class="logbox" id="jobLog" v-html="currentLogHtml"></div>
            </div>
          </section>

          <section class="panel">
            <h2>最近交易流水</h2>
            <div class="panel-body">
              <div v-if="!orders.length" class="empty">暂无交易流水</div>
              <div class="table-wrap order-table" v-else>
                <table>
                  <thead><tr><th>时间</th><th>动作</th><th>代码</th><th>数量</th><th>价格</th><th>理由</th></tr></thead>
                  <tbody>
                    <tr v-for="row in orders" :key="row.id">
                      <td>{{ String(row.created_at).slice(5, 16) }}</td>
                      <td><span class="badge">{{ row.action }}</span></td>
                      <td>{{ row.code }}</td>
                      <td>{{ money(row.quantity) }}</td>
                      <td>{{ money(row.price) }}</td>
                      <td>{{ String(row.reason).slice(0, 80) }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        </div>
      </section>
    </main>
    <div v-else style="padding: 100px; text-align: center; color: var(--muted)">
      正在连接后端数据湖，请稍候...
    </div>

    <!-- 配置弹窗 -->
    <div :class="['modal-backdrop', { open: showConfig }]" @click.self="showConfig = false">
      <section class="modal-window" v-if="showConfig">
        <div class="modal-header">
          <div>
            <h2>运行配置</h2>
            <div class="subtle">{{ configState.mtime ? `已读取 · ${configState.mtime}` : '加载中...' }}</div>
          </div>
          <div class="actions">
            <button @click="loadConfig">重载</button>
            <button class="primary" @click="handleSaveConfig" :disabled="savingConfig">{{ savingConfig ? '保存中...' : '保存配置' }}</button>
            <button @click="showConfig = false">关闭</button>
          </div>
        </div>
        <div class="modal-body">
          <div class="empty" v-if="configLoading">正在读取配置...</div>
          <div class="empty" v-else-if="!configFields.length">暂无配置内容</div>
          <div v-else class="config-form">
            <section v-for="group in configGroups" :key="group.key" class="config-section">
              <div class="config-section-title">{{ group.label }}</div>
              <div class="config-grid">
                <label v-for="field in group.fields" :key="field.path" class="config-field">
                  <span class="config-label-row">
                    <span class="config-label">{{ field.label }}</span>
                    <span class="config-path">{{ field.path }}</span>
                  </span>
                  <span v-if="field.type === 'boolean'" class="switch-field">
                    <input v-model="field.value" type="checkbox" />
                    <span>{{ field.value ? '已启用' : '已关闭' }}</span>
                  </span>
                  <input
                    v-else-if="field.type === 'integer' || field.type === 'number'"
                    v-model.number="field.value"
                    type="number"
                    :step="field.type === 'integer' ? 1 : 0.01"
                  />
                  <input v-else v-model="field.value" type="text" />
                </label>
              </div>
            </section>
          </div>
        </div>
      </section>
    </div>

    <!-- 任务确认弹窗 -->
    <div :class="['modal-backdrop', { open: showTaskConfirm }]" @click.self="closeTaskConfirmation">
      <section class="confirm-window" v-if="showTaskConfirm">
        <div class="modal-header">
          <div>
            <h2>确认执行任务</h2>
            <div class="subtle">任务会在本机后台启动</div>
          </div>
        </div>
        <div class="modal-body">
          <p class="confirm-title">{{ pendingTaskLabel }}</p>
          <p class="confirm-detail">{{ pendingTaskDetail }}</p>
          <div class="confirm-actions">
            <button @click="closeTaskConfirmation" :disabled="taskLoading[pendingTaskKey]">取消</button>
            <button class="primary" @click="confirmTask" :disabled="taskLoading[pendingTaskKey]">
              {{ taskLoading[pendingTaskKey] ? '启动中...' : '确认执行' }}
            </button>
          </div>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, nextTick } from 'vue'
import { createChart } from 'lightweight-charts'
import { marked } from 'marked'
import * as api from './api'

// 格式化工具
const fmt = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 })
const pct = val => val == null || val === "" ? "-" : `${fmt.format(Number(val))}%`
const money = val => val == null || val === "" ? "-" : fmt.format(Number(val))
const clsByNumber = val => Number(val || 0) >= 0 ? "good" : "bad"
const statusClass = s => s === "failed" ? "bad" : s === "succeeded" ? "good" : "accent"

// 页面状态
const overview = ref({})
const reportHtml = ref('暂无 report')
const reportLoading = ref(false)
const refreshing = ref(false)

const account = computed(() => overview.value.account || {})
const watchlist = computed(() => overview.value.watchlist || [])
const positions = computed(() => overview.value.positions || [])
const historyPositions = computed(() => overview.value.history_positions || [])
const screener = computed(() => overview.value.screener || {})
const audit = computed(() => overview.value.audit || {})
const backtest = computed(() => overview.value.backtest || {})
const orders = computed(() => overview.value.orders || [])
const totalPnl = computed(() => Number(account.value.realized_pnl || 0) + Number(account.value.unrealized_pnl || 0))
const positionTotalPnl = row => Number(row?.realized_pnl || 0) + Number(row?.unrealized_pnl || 0)
const positionTotalReturnPct = row => {
  const avgCost = Number(row?.avg_cost || 0)
  const baseQuantity = Number(row?.quantity || 0) + Number(row?.sold_quantity || 0)
  if (!avgCost || !baseQuantity) return 0
  return positionTotalPnl(row) / (avgCost * baseQuantity) * 100
}
const historyStatusText = row => Number(row?.sold_quantity || 0) > 0 ? '已清仓' : '历史'
const jobs = ref([])
const reportTabs = [
  { name: 'latest_report.md', label: '最新研报' },
  { name: 'latest_targeted_analysis.md', label: '指定分析' }
]
const activeReportName = ref(reportTabs[0].name)

const stockInput = ref('')
const stockLoading = ref(false)
const activeStock = ref(null)
const activeStockTitle = computed(() => {
  const d = activeStock.value
  if (!d) return ''
  return `${d.code} ${d.basic?.name || d.watch?.name || d.position?.name || ''}`
})
const activeStockReturnText = computed(() => {
  const pos = activeStock.value?.position || {}
  if (!pos.status) return '-'
  return pos.status === 'CLOSED' ? pct(pos.realized_return_pct) : pct(pos.unrealized_return_pct)
})

const cashInput = ref('')
const resetTradeBaseline = ref(false)
const cashSaving = ref(false)

const availableTasks = [
  { key: 'sync', label: '同步数据' },
  { key: 'backfill_bars', label: '补历史日线' },
  { key: 'derive_bars', label: '派生周/月线' },
  { key: 'pick', label: '执行选股' },
  { key: 'backtest', label: '走步回测' },
  { key: 'trade', label: '模拟调仓' },
  { key: 'post', label: '交易反思' }
]
const taskLoading = ref({})
const analyzeInput = ref('')
const showTaskConfirm = ref(false)
const pendingTaskKey = ref('')
const pendingAnalyzeCodes = ref('')

const currentJobId = ref(null)
const currentLogHtml = ref('选择任务查看日志')

const showConfig = ref(false)
const configState = ref({})
const configFields = ref([])
const configLoading = ref(false)
const savingConfig = ref(false)

let chartInstance = null
let chartResizeObserver = null
let pollInterval = null

function connectSSE() {
  api.subscribeEvents(
    currentJobId.value,
    (jobsData) => {
      jobs.value = jobsData || [];
    },
    (jobId, excerpt, status) => {
      if (jobId === currentJobId.value) {
        const box = document.getElementById("jobLog");
        let isBottom = false;
        if (box) {
          isBottom = box.scrollHeight - box.clientHeight <= box.scrollTop + 10;
        }
        currentLogHtml.value = ansiToHtml(excerpt || '日志尚未产生');
        if (isBottom) {
          nextTick(() => { if (box) box.scrollTop = box.scrollHeight; });
        }
      }
    }
  );
}

function formatRejects(arr) {
  if (!arr || !arr.length) return '-'
  return arr.map(i => `${i.reason}: ${i.count}`).join('\n')
}

async function handleRefresh() {
  refreshing.value = true
  try {
    overview.value = await api.fetchOverview()
    jobs.value = overview.value.jobs || []
    if (cashInput.value === '' || cashInput.value == null) {
      cashInput.value = Number(overview.value.account?.cash || 0)
    }
    await loadReport(activeReportName.value)
    
    const firstCode = watchlist.value[0]?.code || positions.value[0]?.code
    if (firstCode && !activeStock.value) {
      loadStockDetail(firstCode)
    }
  } catch(e) {
    console.error(e)
  } finally {
    refreshing.value = false
  }
}

async function handleSetCash() {
  const cash = Number(cashInput.value)
  if (!Number.isFinite(cash) || cash < 0) {
    alert('请输入有效现金金额')
    return
  }
  cashSaving.value = true
  try {
    const res = await api.setTradeCash(cash, resetTradeBaseline.value)
    overview.value.account = res.account || overview.value.account
    await handleRefresh()
  } catch(e) {
    alert(e.message || '设置现金失败')
  } finally {
    cashSaving.value = false
  }
}

async function loadReport(name = activeReportName.value) {
  activeReportName.value = name
  reportLoading.value = true
  try {
    const reportData = await api.fetchReport(name)
    reportHtml.value = marked.parse(reportData.content || '暂无 report')
  } catch(e) {
    console.error(e)
    reportHtml.value = marked.parse('暂无 report')
  } finally {
    reportLoading.value = false
  }
}

async function handleLoadStock() {
  if (stockInput.value) loadStockDetail(stockInput.value)
}

async function loadStockDetail(code) {
  stockLoading.value = true
  try {
    activeStock.value = await api.fetchStock(code)
    await nextTick()
    renderChart(activeStock.value.bars || [])
  } catch(e) {
    console.error(e)
    alert(e.message || 'Failed to load stock')
  } finally {
    stockLoading.value = false
  }
}

function renderChart(rows) {
  const container = document.getElementById("priceChart");
  if (!container) return;
  const rightPadding = 34;
  container.innerHTML = "";
  if (chartResizeObserver) {
    chartResizeObserver.disconnect();
    chartResizeObserver = null;
  }
  if (chartInstance) {
    chartInstance.remove();
    chartInstance = null;
  }
  if (!rows.length) {
    container.innerHTML = '<div class="empty">暂无 K 线数据</div>';
    return;
  }
  
  chartInstance = createChart(container, {
    width: Math.max(240, container.clientWidth - rightPadding),
    height: container.clientHeight,
    layout: { background: { type: 'solid', color: 'transparent' }, textColor: '#d1d4dc' },
    grid: { vertLines: { color: '#2b3139' }, horzLines: { color: '#2b3139' } },
    localization: {
      dateFormat: 'yyyy-MM-dd',
      priceFormatter: price => Number(price).toFixed(2),
    },
    timeScale: {
      borderColor: '#2b3139',
      timeVisible: false,
      secondsVisible: false,
      fixLeftEdge: true,
      fixRightEdge: true,
    },
    rightPriceScale: {
      borderColor: '#2b3139',
      minimumWidth: 88,
      entireTextOnly: true,
    },
  });

  const candlestickSeries = chartInstance.addCandlestickSeries({
    upColor: '#f6465d',
    downColor: '#2ebd85',
    borderVisible: false,
    wickUpColor: '#f6465d',
    wickDownColor: '#2ebd85',
    priceFormat: {
      type: 'price',
      precision: 2,
      minMove: 0.01,
    },
  });

  const dataMap = new Map();
  rows.forEach(r => {
    let t = r.trade_date;
    if (t && t.length === 8 && !t.includes('-')) {
      t = `${t.slice(0, 4)}-${t.slice(4, 6)}-${t.slice(6, 8)}`;
    }
    dataMap.set(t, {
      time: toBusinessDay(t),
      open: Number(r.open),
      high: Number(r.high),
      low: Number(r.low),
      close: Number(r.close),
    });
  });

  const data = Array.from(dataMap.values()).sort((a, b) => businessDayKey(a.time).localeCompare(businessDayKey(b.time)));

  candlestickSeries.setData(data);
  chartInstance.timeScale().fitContent();
  chartResizeObserver = new ResizeObserver(() => {
    if (!chartInstance) return;
    chartInstance.applyOptions({
      width: Math.max(240, container.clientWidth - rightPadding),
      height: container.clientHeight,
    });
  });
  chartResizeObserver.observe(container);
}

function toBusinessDay(value) {
  const text = String(value || '')
  const parts = text.split('-').map(Number)
  return { year: parts[0], month: parts[1], day: parts[2] }
}

function businessDayKey(value) {
  return `${value.year}-${String(value.month).padStart(2, '0')}-${String(value.day).padStart(2, '0')}`
}

const taskLabelMap = computed(() => {
  const rows = availableTasks.map(task => [task.key, task.label])
  rows.push(['analyze', '指定分析'])
  return Object.fromEntries(rows)
})

const pendingTaskLabel = computed(() => taskLabelMap.value[pendingTaskKey.value] || '任务')
const pendingTaskDetail = computed(() => {
  if (pendingTaskKey.value === 'analyze') {
    return pendingAnalyzeCodes.value
      ? `将分析：${pendingAnalyzeCodes.value}`
      : '请输入至少一个股票代码后再执行指定分析'
  }
  return `将执行：${pendingTaskLabel.value}`
})

function requestTaskConfirmation(task) {
  const codes = analyzeInput.value.trim()
  if (task === 'analyze' && !codes) {
    alert('请输入至少一个股票代码')
    return
  }
  pendingTaskKey.value = task
  pendingAnalyzeCodes.value = codes
  showTaskConfirm.value = true
}

function closeTaskConfirmation() {
  if (taskLoading.value[pendingTaskKey.value]) return
  showTaskConfirm.value = false
  pendingTaskKey.value = ''
  pendingAnalyzeCodes.value = ''
}

async function confirmTask() {
  const task = pendingTaskKey.value
  const codes = pendingAnalyzeCodes.value
  if (!task) return
  await handleTask(task, codes)
}

async function handleTask(task, confirmedCodes = '') {
  taskLoading.value[task] = true
  try {
    const codes = task === 'analyze' ? confirmedCodes : ''
    const res = await api.startTask(task, codes)
    showTaskConfirm.value = false
    pendingTaskKey.value = ''
    pendingAnalyzeCodes.value = ''
    viewJobLog(res.id)
  } catch(e) {
    alert(e.message)
  } finally {
    taskLoading.value[task] = false
  }
}

async function viewJobLog(id) {
  currentJobId.value = id
  connectSSE() // 切换日志任务后重连 SSE
  try {
    const data = await api.fetchJob(id)
    const box = document.getElementById("jobLog")
    let isBottom = false
    if (box) {
      isBottom = box.scrollHeight - box.clientHeight <= box.scrollTop + 10
    }
    currentLogHtml.value = ansiToHtml(data.log_excerpt || '日志尚未产生')
    if (isBottom) {
      await nextTick()
      if (box) box.scrollTop = box.scrollHeight
    }
  } catch(e) {}
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, char => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  })[char]);
}

function ansiToHtml(str) {
  if (!str) return "";
  let s = escapeHtml(str);
  s = s.replace(/\x1b\[0?m/g, '</span>');
  s = s.replace(/\x1b\[1m/g, '<span style="font-weight:bold;">');
  s = s.replace(/\x1b\[3(\d)m/g, (m, c) => {
    const col = ['#0b0e14', '#f6465d', '#2ebd85', '#f59e0b', '#3b82f6', '#8b5cf6', '#0ea5e9', '#d1d4dc'][c] || 'inherit';
    return `<span style="color:${col};">`;
  });
  s = s.replace(/\x1b\[9(\d)m/g, (m, c) => {
    const col = ['#848e9c', '#f87171', '#4ade80', '#facc15', '#60a5fa', '#c084fc', '#38bdf8', '#ffffff'][c] || 'inherit';
    return `<span style="color:${col};">`;
  });
  return s;
}

async function loadConfig() {
  configLoading.value = true
  try {
    const data = await api.fetchConfig()
    configState.value = data
    configFields.value = (data.schema || []).map(field => ({ ...field }))
  } finally {
    configLoading.value = false
  }
}

async function openConfig() {
  showConfig.value = true
  if (!configFields.value.length) {
    await loadConfig()
  }
}

async function handleSaveConfig() {
  savingConfig.value = true
  try {
    await api.saveConfig(buildConfigFromFields())
    await loadConfig()
    showConfig.value = false
  } catch(e) {
    alert(e.message)
  } finally {
    savingConfig.value = false
  }
}

const configGroups = computed(() => {
  const groups = new Map()
  for (const field of configFields.value) {
    const parts = field.path.split('.')
    const groupKey = parts.length > 1 ? parts.slice(0, -1).join('.') : 'root'
    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        key: groupKey,
        label: groupLabel(groupKey),
        fields: []
      })
    }
    groups.get(groupKey).fields.push(field)
  }
  return Array.from(groups.values())
})

function groupLabel(groupKey) {
  if (groupKey === 'root') return '基础配置'
  const sections = configState.value.sections || {}
  const parts = groupKey.split('.')
  return parts.map(part => sections[part] || fallbackLabel(part)).join(' / ')
}

function fallbackLabel(key) {
  const labels = {
    api: '接口',
    key: '密钥',
    url: '地址',
    model: '模型',
    max: '最大',
    min: '最小',
    workers: '并发',
    timeout: '超时',
    enabled: '启用',
    enable: '启用',
    retention: '保留',
    days: '天数',
    seconds: '秒数',
    period: '周期',
    size: '大小',
    count: '数量',
    level: '级别',
    file: '文件',
    provider: '服务',
    fallback: '兜底',
    search: '搜索',
    depth: '深度'
  }
  return String(key).split('_').map(part => labels[part] || part).join(' ')
}

function buildConfigFromFields() {
  const base = JSON.parse(JSON.stringify(configState.value.data || {}))
  for (const field of configFields.value) {
    const path = field.path.split('.')
    let value = field.value
    if (field.type === 'integer') value = Number.parseInt(value || 0, 10)
    if (field.type === 'number') value = Number.parseFloat(value || 0)
    setNestedValue(base, path, value)
  }
  return base
}

function setNestedValue(target, path, value) {
  let cursor = target
  for (const part of path.slice(0, -1)) {
    if (!cursor[part] || typeof cursor[part] !== 'object') cursor[part] = {}
    cursor = cursor[part]
  }
  cursor[path[path.length - 1]] = value
}

onMounted(() => {
  handleRefresh()
  connectSSE()
})

onUnmounted(() => {
  if (chartResizeObserver) chartResizeObserver.disconnect()
  api.closeEvents()
})
</script>
