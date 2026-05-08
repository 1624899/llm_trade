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
          <button @click="showConfig = true">运行配置</button>
          <button class="primary" @click="handleRefresh" :disabled="refreshing">{{ refreshing ? '刷新中...' : '刷新' }}</button>
        </div>
      </div>
    </header>

    <main v-if="overview.generated_at">
      <section class="metrics">
        <div class="metric"><label>总权益</label><strong>{{ money(account.total_equity || account.initial_cash) }}</strong><div class="subtle">交易账户</div></div>
        <div class="metric"><label>现金</label><strong>{{ money(account.cash) }}</strong><div class="subtle">可用模拟资金</div></div>
        <div class="metric"><label>浮动盈亏</label><strong>{{ money(account.unrealized_pnl) }}</strong><div class="subtle">未实现</div></div>
        <div class="metric"><label>观察标的</label><strong>{{ watchlist.length }}</strong><div class="subtle">ACTIVE</div></div>
        <div class="metric"><label>持仓数量</label><strong>{{ positions.length }}</strong><div class="subtle">OPEN</div></div>
        <div class="metric"><label>候选数量</label><strong>{{ screener.candidate_count || audit.prefilter_count || '-' }}</strong><div class="subtle">{{ screener.profile || '规则雷达' }}</div></div>
      </section>

      <section class="layout">
        <div class="stack">
          <section class="panel">
            <h2>观察仓</h2>
            <div class="panel-body">
              <div v-if="!watchlist.length" class="empty">观察仓暂无 ACTIVE 标的</div>
              <div class="table-wrap" v-else>
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
              <div v-if="!positions.length" class="empty">交易仓当前无 OPEN 持仓</div>
              <div class="table-wrap" v-else>
                <table>
                  <thead><tr><th>代码</th><th>名称</th><th>数量</th><th>成本</th><th>现价</th><th>市值</th><th>浮盈亏</th></tr></thead>
                  <tbody>
                    <tr v-for="row in positions" :key="row.code" class="clickable" @click="loadStockDetail(row.code)">
                      <td><strong>{{ row.code }}</strong></td>
                      <td>{{ row.name }}</td>
                      <td>{{ money(row.quantity) }}</td>
                      <td>{{ money(row.avg_cost) }}</td>
                      <td>{{ money(row.current_price) }}</td>
                      <td>{{ money(row.market_value) }}</td>
                      <td>{{ money(row.unrealized_pnl) }} <span :class="['badge', clsByNumber(row.unrealized_return_pct)]">{{ pct(row.unrealized_return_pct) }}</span></td>
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

          <section class="panel" v-show="activeStock">
            <h2>个股详情 - {{ activeStockTitle }}</h2>
            <div class="panel-body">
              <div id="priceChart" class="chart-container"></div>
              <div class="split">
                <div class="card">
                  <div class="card-title">仓位与观察</div>
                  <div class="kv" v-if="activeStock">
                    <span>行业</span><strong>{{ activeStock.basic?.industry || '-' }}</strong>
                    <span>观察档位</span><strong>{{ activeStock.watch?.tier || '-' }}</strong>
                    <span>观察收益</span><strong>{{ pct(activeStock.watch?.return_pct) }}</strong>
                    <span>持仓数量</span><strong>{{ money(activeStock.position?.quantity) }}</strong>
                    <span>持仓收益</span><strong>{{ pct(activeStock.position?.unrealized_return_pct) }}</strong>
                    <span>更新</span><strong>{{ activeStock.watch?.updated_at || activeStock.position?.opened_at || '-' }}</strong>
                  </div>
                </div>
                <div class="card" v-if="activeStock">
                  <div class="card-title">最近财务</div>
                  <div v-if="!activeStock.financials?.length" class="empty">暂无财务摘要</div>
                  <div class="table-wrap" style="max-height: 200px" v-else>
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
        </div>

        <div class="stack">
          <section class="panel">
            <h2>任务控制台</h2>
            <div class="panel-body">
              <div class="task-grid">
                <button v-for="task in availableTasks" :key="task.key" @click="handleTask(task.key)" :disabled="taskLoading[task.key]">
                  {{ taskLoading[task.key] ? task.label + '...' : task.label }}
                </button>
              </div>
              <div class="inline-form">
                <input v-model="analyzeInput" placeholder="指定分析：600519, 000001" />
                <button @click="handleTask('analyze')" :disabled="taskLoading['analyze']">
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
            <h2>最新研报</h2>
            <div class="panel-body">
              <div class="report markdown-body" id="report" v-html="reportHtml"></div>
            </div>
          </section>
        </div>

        <div class="stack">
          <section class="panel">
            <h2>最近交易流水</h2>
            <div class="panel-body">
              <div v-if="!orders.length" class="empty">暂无交易流水</div>
              <div class="table-wrap" v-else>
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

    <!-- Config Modal -->
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
          <div class="empty" v-if="!configState.data">暂无配置内容</div>
          <!-- Since a full recursive YAML editor in Vue is complex for one file, we will fallback to a raw text editor or let the user edit the yaml file directly for now to save space, but let's provide a textarea. -->
          <textarea v-else v-model="rawYaml" style="width: 100%; height: 500px; background: #111; color: #eee; padding: 12px; font-family: monospace; border-radius: 8px; border: 1px solid var(--line);"></textarea>
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

// Formatting utilities
const fmt = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 })
const pct = val => val == null || val === "" ? "-" : `${fmt.format(Number(val))}%`
const money = val => val == null || val === "" ? "-" : fmt.format(Number(val))
const clsByNumber = val => Number(val || 0) >= 0 ? "good" : "bad"
const statusClass = s => s === "failed" ? "bad" : s === "succeeded" ? "good" : "accent"

// State
const overview = ref({})
const reportHtml = ref('暂无 report')
const refreshing = ref(false)

const account = computed(() => overview.value.account || {})
const watchlist = computed(() => overview.value.watchlist || [])
const positions = computed(() => overview.value.positions || [])
const screener = computed(() => overview.value.screener || {})
const audit = computed(() => overview.value.audit || {})
const backtest = computed(() => overview.value.backtest || {})
const orders = computed(() => overview.value.orders || [])
const jobs = ref([])

const stockInput = ref('')
const stockLoading = ref(false)
const activeStock = ref(null)
const activeStockTitle = computed(() => {
  const d = activeStock.value
  if (!d) return ''
  return `${d.code} ${d.basic?.name || d.watch?.name || d.position?.name || ''}`
})

const availableTasks = [
  { key: 'sync', label: '同步数据' },
  { key: 'backfill_bars', label: '补历史日线' },
  { key: 'derive_bars', label: '派生周/月线' },
  { key: 'pick', label: '执行选股' },
  { key: 'backtest', label: '走步回测' },
  { key: 'trade', label: '模拟调仓' },
  { key: 'post', label: '盘后诊断' }
]
const taskLoading = ref({})
const analyzeInput = ref('')

const currentJobId = ref(null)
const currentLogHtml = ref('选择任务查看日志')

const showConfig = ref(false)
const configState = ref({})
const rawYaml = ref('')
const savingConfig = ref(false)

let chartInstance = null
let pollInterval = null

function formatRejects(arr) {
  if (!arr || !arr.length) return '-'
  return arr.map(i => `${i.reason}: ${i.count}`).join('\n')
}

async function handleRefresh() {
  refreshing.value = true
  try {
    overview.value = await api.fetchOverview()
    jobs.value = overview.value.jobs || []
    const reportData = await api.fetchReport()
    reportHtml.value = marked.parse(reportData.content || '暂无 report')
    
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
  container.innerHTML = "";
  if (chartInstance) {
    chartInstance.remove();
    chartInstance = null;
  }
  if (!rows.length) {
    container.innerHTML = '<div class="empty">暂无 K 线数据</div>';
    return;
  }
  
  chartInstance = createChart(container, {
    layout: { background: { type: 'solid', color: 'transparent' }, textColor: '#d1d4dc' },
    grid: { vertLines: { color: '#2b3139' }, horzLines: { color: '#2b3139' } },
    timeScale: { timeVisible: true, borderColor: '#2b3139' },
    rightPriceScale: { borderColor: '#2b3139' },
  });

  const candlestickSeries = chartInstance.addCandlestickSeries({
    upColor: '#f6465d',
    downColor: '#2ebd85',
    borderVisible: false,
    wickUpColor: '#f6465d',
    wickDownColor: '#2ebd85',
  });

  const dataMap = new Map();
  rows.forEach(r => {
    let t = r.trade_date;
    if (t && t.length === 8 && !t.includes('-')) {
      t = `${t.slice(0, 4)}-${t.slice(4, 6)}-${t.slice(6, 8)}`;
    }
    dataMap.set(t, {
      time: t,
      open: Number(r.open),
      high: Number(r.high),
      low: Number(r.low),
      close: Number(r.close),
    });
  });

  const data = Array.from(dataMap.values()).sort((a, b) => a.time.localeCompare(b.time));

  candlestickSeries.setData(data);
  chartInstance.timeScale().fitContent();
}

async function handleTask(task) {
  taskLoading.value[task] = true
  try {
    const res = await api.startTask(task, analyzeInput.value)
    await pollJobs()
    viewJobLog(res.id)
  } catch(e) {
    alert(e.message)
  } finally {
    taskLoading.value[task] = false
  }
}

async function pollJobs() {
  try {
    jobs.value = await api.fetchJobs()
    if (currentJobId.value && jobs.value.some(r => r.id === currentJobId.value && r.status === 'running')) {
      await viewJobLog(currentJobId.value)
    }
  } catch(e) {}
}

async function viewJobLog(id) {
  currentJobId.value = id
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
  const data = await api.fetchConfig()
  configState.value = data
  rawYaml.value = data.content || ''
}

async function handleSaveConfig() {
  savingConfig.value = true
  try {
    // We send raw yaml content via a small hack to api:
    const res = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content: rawYaml.value })
    });
    if (!res.ok) throw new Error('保存失败')
    await loadConfig()
    showConfig.value = false
  } catch(e) {
    alert(e.message)
  } finally {
    savingConfig.value = false
  }
}

onMounted(() => {
  handleRefresh()
  pollInterval = setInterval(pollJobs, 5000)
})

onUnmounted(() => {
  clearInterval(pollInterval)
})
</script>
