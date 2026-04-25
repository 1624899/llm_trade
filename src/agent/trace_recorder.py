"""轻量级工作流追踪记录器，用于智能体诊断。"""

from __future__ import annotations

import json
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

# 项目根目录路径
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TraceRecorder:
    """
    仅追加（Append-only）的 JSONL 记录器，用于检查智能体的输入和输出。
    主要用于开发调试和决策溯源。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()  # 线程锁，确保写入安全
        self._path: Optional[Path] = None
        self._session_id: Optional[str] = None

    @property
    def path(self) -> Optional[Path]:
        """获取当前追踪文件的路径。"""
        return self._path

    @property
    def session_id(self) -> Optional[str]:
        """获取当前追踪会话的 ID。"""
        return self._session_id

    def start(self, prefix: str = "agent_trace") -> Path:
        """
        开始一个新的追踪会话。
        
        Args:
            prefix: 追踪文件名的前缀。
            
        Returns:
            创建的追踪文件 Path 对象。
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._session_id = timestamp
        output_dir = PROJECT_ROOT / "outputs"
        output_dir.mkdir(parents=True, exist_ok=True)
        self._path = output_dir / f"{prefix}_{timestamp}.jsonl"
        
        self.record("trace_start", {"session_id": self._session_id})
        logger.info(f"[Trace] 正在将智能体追踪记录写入: {self._path}")
        return self._path

    def record(self, event_type: str, payload: Dict[str, Any]) -> None:
        """
        记录一个追踪事件。
        
        Args:
            event_type: 事件类型（如 'llm_call', 'tool_use' 等）。
            payload: 事件携带的数据负载。
        """
        if self._path is None:
            return
            
        event = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "event": event_type,
            "session_id": self._session_id,
            "payload": self._sanitize(payload),  # 对敏感信息进行脱敏处理
        }
        
        line = json.dumps(event, ensure_ascii=False, default=str)
        with self._lock:
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as file:
                    file.write(line + "\n")
            except Exception as exc:
                logger.debug(f"写入智能体追踪记录失败: {exc}")

    def finish(self) -> None:
        """
        结束当前追踪会话，并将其复制一份为 'latest_agent_trace.jsonl'。
        """
        if self._path is None:
            return
            
        self.record("trace_finish", {"elapsed_marker": time.time()})
        latest = PROJECT_ROOT / "outputs" / "latest_agent_trace.jsonl"
        
        with self._lock:
            try:
                shutil.copyfile(self._path, latest)
                logger.info(f"[Trace] 最新的追踪记录已复制到: {latest}")
            except Exception as exc:
                logger.debug(f"复制最新智能体追踪记录失败: {exc}")

    def _sanitize(self, value: Any) -> Any:
        """
        对数据进行脱敏处理，过滤掉包含 API 密钥、Token 或 Secret 等敏感信息的字段。
        """
        if isinstance(value, dict):
            sanitized = {}
            for key, item in value.items():
                key_text = str(key)
                # 检查键名是否包含敏感词
                if any(token in key_text.lower() for token in ("api_key", "authorization", "token", "secret")):
                    sanitized[key_text] = "***REDACTED***"
                else:
                    sanitized[key_text] = self._sanitize(item)
            return sanitized
        if isinstance(value, list):
            return [self._sanitize(item) for item in value]
        if isinstance(value, tuple):
            return [self._sanitize(item) for item in value]
        return value


# 全局单例对象
trace_recorder = TraceRecorder()
