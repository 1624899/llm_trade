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
