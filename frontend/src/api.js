export async function fetchOverview() {
  const res = await fetch('/api/overview')
  return res.json()
}

export async function fetchConfig() {
  const res = await fetch('/api/config')
  return res.json()
}

export async function saveConfig(data) {
  const res = await fetch('/api/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ data })
  })
  const json = await res.json()
  if (!res.ok) throw new Error(json.error || 'Failed to save config')
  return json
}

export async function fetchJobs() {
  const res = await fetch('/api/jobs')
  return res.json()
}

export async function fetchJob(id) {
  const res = await fetch(`/api/jobs/${id}`)
  return res.json()
}

export async function fetchStock(code) {
  const res = await fetch(`/api/stock/${code}`)
  return res.json()
}

export async function startTask(task, codes = '') {
  const body = task === 'analyze' ? { task, codes } : { task }
  const res = await fetch('/api/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  })
  const data = await res.json()
  if (!res.ok) throw new Error(data.error || 'Task failed')
  return data
}

export async function fetchReport(name = 'latest_report.md') {
  const res = await fetch(`/api/report?name=${name}`)
  return res.json()
}

let eventSource = null;

export function subscribeEvents(watchJobId, onJobsUpdate, onLogUpdate) {
  if (eventSource) {
    eventSource.close();
  }
  const url = watchJobId ? `/api/stream/events?watch_job=${watchJobId}` : `/api/stream/events`;
  eventSource = new EventSource(url);
  
  eventSource.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'jobs') {
      onJobsUpdate(msg.data);
    } else if (msg.type === 'log') {
      onLogUpdate(msg.job_id, msg.excerpt, msg.status);
    }
  };
  
  eventSource.onerror = () => {
    console.error("SSE Connection error, retrying...");
    eventSource.close();
    setTimeout(() => subscribeEvents(watchJobId, onJobsUpdate, onLogUpdate), 3000);
  };
}

export function closeEvents() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
}
