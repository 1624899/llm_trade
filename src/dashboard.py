"""Lightweight local web dashboard for LLM-TRADE."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml


ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = ROOT_DIR / "outputs"
DB_PATH = ROOT_DIR / "data" / "stock_lake.db"
CONFIG_PATH = ROOT_DIR / "config" / "config.yaml"
JOB_LOG_DIR = OUTPUTS_DIR / "dashboard_jobs"
WEB_DIR = ROOT_DIR / "frontend" / "dist"

JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
TASK_COMMANDS: dict[str, dict[str, Any]] = {
    "sync": {"label": "同步数据", "args": ["--sync"]},
    "backfill_bars": {"label": "补全历史日线", "args": ["--backfill-bars"]},
    "derive_bars": {"label": "派生周线/月线", "args": ["--derive-bars"]},
    "pick": {"label": "执行选股", "args": ["--pick"]},
    "backtest": {"label": "走步回测", "args": ["--backtest"]},
    "trade": {"label": "模拟调仓", "args": ["--trade"]},
    "post": {"label": "盘后诊断", "args": ["--post"]},
}
CONFIG_SECTION_LABELS = {
    "system": "系统设置",
    "llm_models": "大模型配置",
    "deepseek": "DeepSeek 模型",
    "qwen": "通义千问模型",
    "agent_workflow": "Agent 工作流",
    "web_search": "联网搜索",
    "data": "数据同步与保留",
    "indicators": "技术指标",
    "ema": "EMA 参数",
    "macd": "MACD 参数",
    "rsi": "RSI 参数",
    "kdj": "KDJ 参数",
    "boll": "布林线参数",
    "wr": "WR 参数",
    "logging": "日志设置",
}
CONFIG_FIELD_LABELS = {
    "system.test_mode": "测试模式",
    "llm_models.deepseek.api_key": "DeepSeek API Key",
    "llm_models.deepseek.base_url": "DeepSeek 接口地址",
    "llm_models.deepseek.model": "DeepSeek 模型名称",
    "llm_models.deepseek.max_tokens": "DeepSeek 最大输出 Tokens",
    "llm_models.deepseek.temperature": "DeepSeek 温度",
    "llm_models.qwen.api_key": "通义千问 API Key",
    "llm_models.qwen.base_url": "通义千问接口地址",
    "llm_models.qwen.model": "通义千问模型名称",
    "llm_models.qwen.max_tokens": "通义千问最大输出 Tokens",
    "llm_models.qwen.temperature": "通义千问温度",
    "active_llm": "当前启用模型",
    "agent_workflow.candidate_analysis_max_workers": "候选股并发分析数",
    "web_search.enabled": "启用联网搜索",
    "web_search.provider": "主搜索服务",
    "web_search.fallback_provider": "备用搜索服务",
    "web_search.api_key": "搜索 API Key",
    "web_search.endpoint": "搜索接口地址",
    "web_search.topic": "搜索主题",
    "web_search.search_depth": "搜索深度",
    "web_search.include_answer": "包含搜索答案",
    "web_search.cache_ttl_seconds": "搜索缓存秒数",
    "web_search.timeout": "搜索超时秒数",
    "web_search.max_results": "搜索结果数量",
    "data.enable_cleanup": "启用文件清理",
    "data.enable_database_cleanup": "启用数据库清理",
    "data.enable_database_vacuum": "启用数据库压缩",
    "data.derive_period_bars_on_sync": "同步时派生周月线",
    "data.backfill_history_on_sync": "同步时补历史 K 线",
    "data.enable_daily_bars_incremental_fill": "启用日线增量补洞",
    "data.market_data_retention_days": "市场数据保留天数",
    "data.macro_events_retention_days": "宏观事件保留天数",
    "data.output_retention_days": "输出文件保留天数",
    "data.trade_execution_retention_days": "交易执行记录保留天数",
    "data.daily_quotes_retention_days": "行情快照保留天数",
    "data.market_bars_daily_retention_days": "日线 K 线保留天数",
    "data.market_bars_weekly_retention_days": "周线 K 线保留天数",
    "data.market_bars_monthly_retention_days": "月线 K 线保留天数",
    "data.daily_lhb_retention_days": "龙虎榜保留天数",
    "data.paper_trades_retention_days": "旧观察仓记录保留天数",
    "data.yahoo_batch_size": "Yahoo 批量大小",
    "data.yahoo_max_workers": "Yahoo 最大并发",
    "data.yahoo_batch_pause": "Yahoo 批次暂停秒数",
    "data.efinance_max_codes": "efinance 最大股票数",
    "data.efinance_max_workers": "efinance 最大并发",
    "data.efinance_timeout": "efinance 超时秒数",
    "data.efinance_request_pause": "efinance 请求间隔秒数",
    "data.enable_efinance_validation": "启用 efinance 抽样校验",
    "data.enable_efinance_fallback": "启用 efinance 兜底",
    "data.tushare_token": "Tushare Token",
    "data.tushare_anomaly_pct": "Tushare 异常涨跌幅阈值",
    "data.tushare_history_start_date": "Tushare 历史起始日期",
    "data.tushare_request_interval": "Tushare 请求间隔秒数",
    "data.tushare_history_max_workers": "Tushare 历史同步并发",
    "data.tushare_max_retries": "Tushare 最大重试次数",
    "data.tushare_fetch_adj_factor": "抓取 Tushare 复权因子",
    "data.enable_akshare_daily_fallback": "启用 AKShare 日线兜底",
    "data.daily_update_after_time": "日常更新开始时间",
    "data.efinance_sample_size": "efinance 校验样本数",
    "data.retry_times": "通用重试次数",
    "data.timeout": "通用超时秒数",
    "data.max_workers": "通用最大并发",
    "indicators.ema.short": "EMA 短周期",
    "indicators.ema.long": "EMA 长周期",
    "indicators.macd.fast": "MACD 快线周期",
    "indicators.macd.slow": "MACD 慢线周期",
    "indicators.macd.signal": "MACD 信号线周期",
    "indicators.rsi.period_7": "RSI 7 日周期",
    "indicators.rsi.period_14": "RSI 14 日周期",
    "indicators.kdj.period": "KDJ 计算周期",
    "indicators.kdj.k_period": "KDJ K 值平滑周期",
    "indicators.kdj.d_period": "KDJ D 值平滑周期",
    "indicators.boll.period": "布林线周期",
    "indicators.boll.std_dev": "布林线标准差倍数",
    "indicators.wr.period": "WR 周期",
    "logging.level": "日志级别",
    "logging.file": "日志文件路径",
    "logging.max_size": "单个日志最大大小",
    "logging.backup_count": "日志备份数量",
}


def run_dashboard(host: str = "127.0.0.1", port: int = 8765, open_browser: bool = False) -> None:
    """Start the dashboard server and block until interrupted."""
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    url = f"http://{host}:{port}"
    print(f"LLM-TRADE 可视化工作台已启动: {url}")
    print("按 Ctrl+C 停止服务。")
    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n工作台已停止。")
    finally:
        server.server_close()


class DashboardHandler(BaseHTTPRequestHandler):
    """Small JSON API plus a single-page dashboard."""

    server_version = "LLMTradeDashboard/0.1"

    def do_GET(self) -> None:  # noqa: N802 - stdlib hook
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/":
                self._send_static_file(WEB_DIR / "index.html")
            elif not parsed.path.startswith("/api/"):
                self._send_frontend_asset(parsed.path)
            elif parsed.path == "/api/overview":
                self._send_json(build_overview())
            elif parsed.path == "/api/config":
                self._send_json(read_config_file())
            elif parsed.path == "/api/jobs":
                self._send_json(list_jobs())
            elif parsed.path == "/api/report":
                query = parse_qs(parsed.query)
                name = query.get("name", ["latest_report.md"])[0]
                self._send_json(read_report(name))
            elif parsed.path.startswith("/api/jobs/"):
                job_id = parsed.path.rsplit("/", 1)[-1]
                self._send_json(get_job(job_id))
            elif parsed.path.startswith("/api/stock/"):
                code = parsed.path.rsplit("/", 1)[-1]
                self._send_json(build_stock_detail(code))
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except Exception as exc:  # keep the UI useful even when one panel fails
            self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802 - stdlib hook
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/config":
                payload = self._read_request_json()
                if "data" in payload:
                    self._send_json(write_structured_config(payload.get("data")))
                else:
                    self._send_json(write_config_file(str(payload.get("content", ""))))
            elif parsed.path == "/api/tasks":
                payload = self._read_request_json()
                self._send_json(start_task(payload), status=HTTPStatus.ACCEPTED)
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def log_message(self, format: str, *args: Any) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {format % args}")

    def _read_request_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return data

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_static_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        content_type = {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".json": "application/json; charset=utf-8",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".ico": "image/x-icon",
        }.get(path.suffix.lower(), "application/octet-stream")
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_frontend_asset(self, request_path: str) -> None:
        safe_path = request_path.lstrip("/").replace("\\", "/")
        target = (WEB_DIR / safe_path).resolve()
        web_root = WEB_DIR.resolve()
        if web_root not in target.parents and target != web_root:
            self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
            return
        if target.exists() and target.is_file():
            self._send_static_file(target)
            return
        self._send_static_file(WEB_DIR / "index.html")


def build_overview() -> dict[str, Any]:
    audit = _read_json(OUTPUTS_DIR / "latest_workflow_audit.json")
    screener = _read_json(OUTPUTS_DIR / "screener_audit.json")
    backtest = _read_json(OUTPUTS_DIR / "latest_backtest_report.json")
    report = read_report("latest_report.md")
    targeted = read_report("latest_targeted_analysis.md")

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "files": {
            "latest_report": _file_info(OUTPUTS_DIR / "latest_report.md"),
            "workflow_audit": _file_info(OUTPUTS_DIR / "latest_workflow_audit.json"),
            "screener_audit": _file_info(OUTPUTS_DIR / "screener_audit.json"),
            "backtest_report": _file_info(OUTPUTS_DIR / "latest_backtest_report.json"),
        },
        "account": query_one("SELECT * FROM trading_account WHERE account_name = ? LIMIT 1", ("default",)),
        "positions": query_all(
            """
            SELECT *
            FROM trading_positions
            WHERE status = 'OPEN'
            ORDER BY market_value DESC, opened_at
            """
        ),
        "watchlist": query_all(
            """
            SELECT *
            FROM watchlist_items
            WHERE watch_status = 'ACTIVE'
            ORDER BY
                CASE tier
                    WHEN '强推荐' THEN 0
                    WHEN '配置/轻仓验证' THEN 1
                    WHEN '观察' THEN 2
                    ELSE 3
                END,
                updated_at DESC,
                id DESC
            """
        ),
        "orders": query_all(
            """
            SELECT *
            FROM trade_orders
            ORDER BY created_at DESC, id DESC
            LIMIT 20
            """
        ),
        "audit": summarize_workflow(audit),
        "screener": summarize_screener(screener),
        "backtest": summarize_backtest(backtest),
        "latest_report": {
            "mtime": report.get("mtime"),
            "excerpt": (report.get("content") or "")[:2200],
        },
        "targeted_report": {
            "mtime": targeted.get("mtime"),
            "excerpt": (targeted.get("content") or "")[:1200],
        },
        "jobs": list_jobs(limit=5),
    }


def read_config_file() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {"content": "", "data": {}, "schema": [], "sections": CONFIG_SECTION_LABELS, "mtime": None, "exists": False}
    content = CONFIG_PATH.read_text(encoding="utf-8", errors="replace")
    data = yaml.safe_load(content) or {}
    if not isinstance(data, dict):
        data = {}
    return {
        "content": content,
        "data": data,
        "schema": build_config_schema(data),
        "sections": CONFIG_SECTION_LABELS,
        "mtime": _format_mtime(CONFIG_PATH),
        "exists": True,
    }


def write_config_file(content: str) -> dict[str, Any]:
    if not content.strip():
        raise ValueError("配置内容不能为空")
    try:
        parsed = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"YAML 解析失败: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("配置顶层必须是 YAML 对象")
    CONFIG_PATH.write_text(content, encoding="utf-8")
    return {"ok": True, "mtime": _format_mtime(CONFIG_PATH)}


def write_structured_config(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("配置数据必须是对象")
    content = yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
    CONFIG_PATH.write_text(content, encoding="utf-8")
    return {"ok": True, "mtime": _format_mtime(CONFIG_PATH), "schema": build_config_schema(data)}


def build_config_schema(data: dict[str, Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    _collect_config_fields(data, [], fields)
    return fields


def _collect_config_fields(value: Any, path: list[str], fields: list[dict[str, Any]]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            _collect_config_fields(child, [*path, str(key)], fields)
        return
    joined = ".".join(path)
    fields.append(
        {
            "path": joined,
            "label": CONFIG_FIELD_LABELS.get(joined, _fallback_config_label(path[-1] if path else joined)),
            "type": _config_value_type(value),
            "value": value,
        }
    )


def _config_value_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    return "text"


def _fallback_config_label(key: str) -> str:
    token_labels = {
        "api": "接口",
        "key": "密钥",
        "url": "地址",
        "model": "模型",
        "max": "最大",
        "min": "最小",
        "workers": "并发",
        "timeout": "超时",
        "enabled": "启用",
        "enable": "启用",
        "retention": "保留",
        "days": "天数",
        "seconds": "秒数",
        "period": "周期",
        "size": "大小",
        "count": "数量",
        "level": "级别",
        "file": "文件",
        "provider": "服务",
        "fallback": "兜底",
        "search": "搜索",
        "depth": "深度",
        "temperature": "温度",
        "tokens": "Tokens",
    }
    return " ".join(token_labels.get(part, part) for part in key.split("_"))


def start_task(payload: dict[str, Any]) -> dict[str, Any]:
    task = str(payload.get("task", "")).strip()
    if task == "analyze":
        codes = _normalize_codes(payload.get("codes", ""))
        if not codes:
            raise ValueError("指定分析需要填写至少一个股票代码")
        label = "指定分析"
        args = ["--analyze", *codes]
    elif task in TASK_COMMANDS:
        spec = TASK_COMMANDS[task]
        label = spec["label"]
        args = list(spec["args"])
    else:
        raise ValueError("不支持的任务类型")

    JOB_LOG_DIR.mkdir(parents=True, exist_ok=True)
    job_id = uuid.uuid4().hex[:12]
    log_path = JOB_LOG_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id}.log"
    command = [sys.executable, str(ROOT_DIR / "main.py"), *args]
    job = {
        "id": job_id,
        "task": task,
        "label": label,
        "command": " ".join(command),
        "status": "running",
        "returncode": None,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "ended_at": None,
        "log_path": str(log_path.relative_to(ROOT_DIR)),
        "log_excerpt": "",
    }
    with JOBS_LOCK:
        JOBS[job_id] = job
    threading.Thread(target=_run_job, args=(job_id, command, log_path), daemon=True).start()
    return job


def list_jobs(limit: int = 20) -> list[dict[str, Any]]:
    with JOBS_LOCK:
        rows = list(JOBS.values())
    return sorted(rows, key=lambda item: item["started_at"], reverse=True)[:limit]


def get_job(job_id: str) -> dict[str, Any]:
    with JOBS_LOCK:
        job = dict(JOBS.get(job_id) or {})
    if not job:
        raise ValueError("任务不存在")
    log_path = ROOT_DIR / job["log_path"]
    job["log_excerpt"] = _read_tail(log_path)
    return job


def _run_job(job_id: str, command: list[str], log_path: Path) -> None:
    started = time.time()
    with log_path.open("w", encoding="utf-8", errors="replace") as log_file:
        log_file.write(f"$ {' '.join(command)}\n\n")
        log_file.flush()
        try:
            process = subprocess.Popen(
                command,
                cwd=ROOT_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            assert process.stdout is not None
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
            returncode = process.wait()
            status = "succeeded" if returncode == 0 else "failed"
        except Exception as exc:  # 后台任务失败也要把原因留给前端查看
            log_file.write(f"\n任务启动失败: {exc}\n")
            returncode = -1
            status = "failed"
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(
                {
                    "status": status,
                    "returncode": returncode,
                    "ended_at": datetime.now().isoformat(timespec="seconds"),
                    "elapsed_seconds": round(time.time() - started, 2),
                    "log_excerpt": _read_tail(log_path),
                }
            )


def _normalize_codes(value: Any) -> list[str]:
    if isinstance(value, list):
        raw = " ".join(str(item) for item in value)
    else:
        raw = str(value or "")
    codes = []
    for item in raw.replace(",", " ").split():
        cleaned = "".join(ch for ch in item if ch.isdigit())
        if cleaned:
            codes.append(cleaned.zfill(6)[-6:])
    return codes[:20]


def _read_tail(path: Path, limit: int = 6000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-limit:]


def build_stock_detail(code: str) -> dict[str, Any]:
    normalized = "".join(ch for ch in code if ch.isdigit()).zfill(6)[-6:]
    basic = query_one("SELECT * FROM stock_basic WHERE code = ? LIMIT 1", (normalized,))
    watch = query_one("SELECT * FROM watchlist_items WHERE code = ? LIMIT 1", (normalized,))
    position = query_one(
        "SELECT * FROM trading_positions WHERE code = ? AND status = 'OPEN' LIMIT 1",
        (normalized,),
    )
    bars = query_all(
        """
        SELECT trade_date, open, high, low, close, volume, amount
        FROM market_bars
        WHERE code = ? AND period = 'daily'
        ORDER BY trade_date DESC
        LIMIT 120
        """,
        (normalized,),
    )
    if not bars:
        bars = query_all(
            """
            SELECT trade_date, open, high, low, close, volume, amount
            FROM market_bars
            WHERE code = ?
            ORDER BY trade_date DESC
            LIMIT 120
            """,
            (normalized,),
        )
    bars.reverse()
    financials = query_all(
        """
        SELECT report_date, revenue, revenue_yoy, parent_netprofit, parent_netprofit_yoy,
               gross_margin, net_margin, roe, debt_to_assets, operating_cash_flow
        FROM financial_metrics
        WHERE code = ?
        ORDER BY report_date DESC
        LIMIT 8
        """,
        (normalized,),
    )
    return {
        "code": normalized,
        "basic": basic,
        "watch": watch,
        "position": position,
        "bars": bars,
        "financials": financials,
    }


def read_report(name: str) -> dict[str, Any]:
    safe_name = os.path.basename(name)
    path = OUTPUTS_DIR / safe_name
    if not path.exists() or path.suffix.lower() not in {".md", ".txt"}:
        return {"name": safe_name, "content": "", "mtime": None, "exists": False}
    return {
        "name": safe_name,
        "content": path.read_text(encoding="utf-8", errors="replace"),
        "mtime": _format_mtime(path),
        "exists": True,
    }


def summarize_workflow(data: dict[str, Any]) -> dict[str, Any]:
    prefilter = data.get("prefilter_candidates") or []
    selected = data.get("selected_codes") or data.get("final_selected_codes") or []
    macro = data.get("macro_context") or data.get("market_context") or {}
    if isinstance(macro, dict):
        macro_label = macro.get("regime") or macro.get("risk_appetite") or macro.get("summary")
    else:
        macro_label = str(macro)[:120]
    return {
        "generated_at": data.get("generated_at"),
        "elapsed_seconds": data.get("elapsed_seconds"),
        "prefilter_count": len(prefilter),
        "selected_count": len(selected),
        "selected_codes": selected,
        "macro_label": macro_label,
        "trace_path": data.get("trace_path"),
    }


def summarize_screener(data: dict[str, Any]) -> dict[str, Any]:
    regime = data.get("market_regime") or {}
    metrics = regime.get("metrics") if isinstance(regime, dict) else {}
    reject_counts = data.get("rule_reject_counts") or {}
    top_rejects = sorted(reject_counts.items(), key=lambda item: item[1], reverse=True)[:8]
    return {
        "generated_at": data.get("generated_at"),
        "profile": data.get("profile"),
        "candidate_count": data.get("candidate_count"),
        "input_stock_count": data.get("input_stock_count"),
        "rejected_count": data.get("rejected_count"),
        "regime": regime.get("regime") if isinstance(regime, dict) else None,
        "regime_reason": regime.get("reason") if isinstance(regime, dict) else None,
        "metrics": metrics or {},
        "top_rejects": [{"reason": key, "count": value} for key, value in top_rejects],
    }


def summarize_backtest(data: dict[str, Any]) -> dict[str, Any]:
    if not data:
        return {}
    return {
        "generated_at": data.get("generated_at"),
        "window_count": data.get("window_count"),
        "evaluated_count": data.get("evaluated_count"),
        "summary": data.get("summary"),
        "strategy_stats": data.get("strategy_stats") or data.get("strategy_performance") or {},
        "weight_suggestions": data.get("weight_suggestions") or data.get("weights") or {},
    }


def query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if not DB_PATH.exists():
        return []
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return [_clean_row(dict(row)) for row in rows]


def query_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    rows = query_all(sql, params)
    return rows[0] if rows else {}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except json.JSONDecodeError:
        return {}


def _file_info(path: Path) -> dict[str, Any]:
    return {
        "exists": path.exists(),
        "mtime": _format_mtime(path) if path.exists() else None,
        "size": path.stat().st_size if path.exists() else 0,
    }


def _format_mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _clean_value(value) for key, value in row.items()}


def _clean_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _json_default(value: Any) -> Any:
    return str(value)
