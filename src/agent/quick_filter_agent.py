"""AI 轻量精筛 Agent。

该 Agent 只处理规则海选后的极简快照，不拉取财报、研报或长新闻，
用于在深度复核前把多策略候选池压缩为更小的自选池。
"""

import json
import re
from typing import Any, Dict, List

from loguru import logger

from src.agent.tools import tools


class QuickFilterAgent:
    """结合宏观环境，从多策略候选池中快速精选少量标的。"""

    def filter_candidates(
        self,
        candidates: List[Dict[str, Any]],
        macro_context: Dict[str, Any] | None = None,
        target_n: int = 8,
    ) -> Dict[str, Any]:
        target_n = max(1, int(target_n or 8))
        if len(candidates) <= target_n:
            return {
                "selected_candidates": candidates,
                "selected_codes": [str(item.get("code", "")).zfill(6) for item in candidates],
                "mode": "pass_through",
                "reason": "候选池数量不超过轻量精筛目标数，直接放行。",
                "evaluations": [],
            }

        macro_context = macro_context or {}
        system_prompt = f"""
你是 A 股盘后快速精筛助手。请结合宏观环境和候选股短表，从海选池选出最多 {target_n} 只进入深度复核。
规则：必须覆盖检查全部候选；偏弱/退潮市少追高，优先防守、低吸、回踩确认；强势市可提高突破/主升浪权重；不得编造未提供信息。
只输出紧凑 JSON：{{"evaluations":[{{"code":"000001","score":82,"keep":true,"reason":"短理由"}}],"selected_codes":["000001"],"summary":"短总结"}}
"""
        user_prompt = self._build_compact_prompt(candidates, macro_context, target_n)

        raw = tools.call_llm(system_prompt, user_prompt, temperature=0.15)
        parsed = self._parse_llm_json(raw)
        if not parsed:
            logger.warning("[QuickFilter] LLM 精筛失败，降级为规则排序。")
            return self._rule_based_fallback(
                candidates,
                target_n,
                macro_context,
                reason="LLM 精筛失败，使用宏观适配规则降级。",
            )

        selected_codes = self._normalize_selected_codes(parsed.get("selected_codes"), candidates, target_n)
        if not selected_codes:
            logger.warning("[QuickFilter] LLM 未返回有效代码，降级为规则排序。")
            return self._rule_based_fallback(
                candidates,
                target_n,
                macro_context,
                reason="LLM 未返回有效代码，使用宏观适配规则降级。",
            )

        selected_candidates = self._select_candidates_by_code(candidates, selected_codes)
        return {
            "selected_candidates": selected_candidates,
            "selected_codes": [item.get("code") for item in selected_candidates],
            "mode": "llm",
            "reason": str(parsed.get("summary") or "LLM 结合宏观环境完成轻量精筛。"),
            "evaluations": parsed.get("evaluations") if isinstance(parsed.get("evaluations"), list) else [],
            "raw_response": raw,
        }

    def _build_compact_prompt(
        self,
        candidates: List[Dict[str, Any]],
        macro_context: Dict[str, Any],
        target_n: int,
    ) -> str:
        """构建短表格式 Prompt，避免轻量精筛请求因为上下文过长而超时。"""
        macro_brief = self._format_macro_brief(macro_context)
        header = "序号|代码|名称|行业|主策略|置信|技分|题材|涨幅|5日|20日|量比|PE|PB|市值亿"
        rows = [
            self._format_candidate_row(idx, item)
            for idx, item in enumerate(candidates, start=1)
        ]
        return "\n".join(
            [
                f"目标数量: {target_n}",
                "宏观摘要:",
                macro_brief,
                "候选短表:",
                header,
                *rows,
            ]
        )

    def _format_macro_brief(self, macro_context: Dict[str, Any]) -> str:
        fields = [
            ("market_sentiment", "情绪"),
            ("risk_appetite", "风险偏好"),
            ("liquidity_view", "流动性"),
            ("favorable_sectors", "有利方向"),
            ("avoid_sectors", "规避方向"),
            ("key_risks", "风险"),
            ("analysis_focus", "重点"),
        ]
        lines = []
        for key, label in fields:
            value = macro_context.get(key)
            if value in (None, "", []):
                continue
            lines.append(f"{label}: {self._compact_value(value, max_len=80)}")
        return "\n".join(lines[:7]) or "无明确宏观摘要"

    def _format_candidate_row(
        self,
        idx: int,
        item: Dict[str, Any],
    ) -> str:
        snapshot = self._build_candidate_snapshot(item)
        return "|".join(
            [
                str(idx),
                snapshot["code"],
                self._compact_value(snapshot.get("name"), 8),
                self._compact_value(snapshot.get("industry"), 8),
                self._compact_value(snapshot.get("primary_strategy"), 18),
                self._fmt_num(snapshot.get("strategy_confidence"), 2),
                self._fmt_num(snapshot.get("technical_score"), 1),
                self._fmt_num(snapshot.get("theme_score"), 1),
                self._fmt_num(snapshot.get("change_pct"), 1),
                self._fmt_num(snapshot.get("ret5"), 1),
                self._fmt_num(snapshot.get("ret20"), 1),
                self._fmt_num(snapshot.get("volume_ratio"), 2),
                self._fmt_num(snapshot.get("pe_ttm"), 1),
                self._fmt_num(snapshot.get("pb"), 2),
                self._fmt_market_cap(snapshot.get("total_market_cap")),
            ]
        )

    def _build_candidate_snapshot(self, item: Dict[str, Any]) -> Dict[str, Any]:
        metrics = item.get("key_metrics") if isinstance(item.get("key_metrics"), dict) else {}
        return {
            "code": str(item.get("code", "")).zfill(6),
            "name": item.get("name"),
            "industry": item.get("industry"),
            "primary_strategy": self._primary_strategy(item),
            "strategy_tags": item.get("strategy_tags", []),
            "strategy_confidence": item.get("strategy_confidence"),
            "technical_score": item.get("technical_score"),
            "theme_score": item.get("theme_score", 0.0),
            "price": item.get("price"),
            "change_pct": item.get("change_pct"),
            "ret5": item.get("ret5", metrics.get("ret5")),
            "ret20": item.get("ret20", metrics.get("ret20")),
            "volume_ratio": item.get("volume_ratio", metrics.get("volume_ratio")),
            "turnover_rate": metrics.get("turnover_rate"),
            "pe_ttm": item.get("pe_ttm"),
            "pb": item.get("pb"),
            "total_market_cap": item.get("total_market_cap"),
        }

    def _rule_based_fallback(
        self,
        candidates: List[Dict[str, Any]],
        target_n: int,
        macro_context: Dict[str, Any] | None = None,
        reason: str = "使用宏观适配规则降级。",
    ) -> Dict[str, Any]:
        ranked = sorted(
            candidates,
            key=lambda item: self._fallback_score(item, macro_context or {}),
            reverse=True,
        )
        selected = ranked[:target_n]
        return {
            "selected_candidates": selected,
            "selected_codes": [str(item.get("code", "")).zfill(6) for item in selected],
            "mode": "rule_fallback",
            "reason": reason,
            "evaluations": [
                {
                    "code": str(item.get("code", "")).zfill(6),
                    "score": round(self._fallback_score(item, macro_context or {}), 2),
                    "keep": idx < target_n,
                    "reason": "按宏观风格、策略置信度、技术分和题材分降级排序。",
                }
                for idx, item in enumerate(ranked)
            ],
        }

    def _fallback_score(
        self,
        item: Dict[str, Any],
        macro_context: Dict[str, Any],
    ) -> float:
        score = float(item.get("technical_score") or 0)
        score += float(item.get("strategy_confidence") or 0) * 1000
        score += float(item.get("theme_score") or 0)
        primary = self._primary_strategy(item)
        macro_text = json.dumps(macro_context, ensure_ascii=False).lower()
        weak_market = any(token in macro_text for token in ("偏弱", "退潮", "防守", "谨慎", "risk-off", "weak"))
        strong_market = any(token in macro_text for token in ("偏强", "进攻", "risk-on", "strong", "活跃"))
        ret5 = self._safe_float(item.get("ret5"))
        if ret5 is None and isinstance(item.get("key_metrics"), dict):
            ret5 = self._safe_float(item["key_metrics"].get("ret5"))

        if weak_market:
            if primary in {"value_bottom", "support_pullback", "panic_reversal"}:
                score += 12
            if primary in {"momentum_leader", "trend_breakout"} and ret5 is not None and ret5 > 10:
                score -= 10
        elif strong_market:
            if primary in {"trend_breakout", "momentum_leader"}:
                score += 8
        return score

    def _parse_llm_json(self, text: str) -> Dict[str, Any]:
        if not text:
            return {}
        cleaned = text.strip()
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            data = json.loads(cleaned)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", cleaned, flags=re.S)
            if not match:
                return {}
            try:
                data = json.loads(match.group(0))
                return data if isinstance(data, dict) else {}
            except json.JSONDecodeError:
                return {}

    def _normalize_selected_codes(self, codes: Any, candidates: List[Dict[str, Any]], target_n: int) -> List[str]:
        allowed = {str(item.get("code", "")).zfill(6) for item in candidates}
        normalized: List[str] = []
        if not isinstance(codes, list):
            return normalized
        for code in codes:
            text = re.sub(r"\D", "", str(code or ""))
            if len(text) >= 6:
                text = text[-6:]
            if text in allowed and text not in normalized:
                normalized.append(text)
            if len(normalized) >= target_n:
                break
        return normalized

    def _select_candidates_by_code(self, candidates: List[Dict[str, Any]], codes: List[str]) -> List[Dict[str, Any]]:
        by_code = {str(item.get("code", "")).zfill(6): item for item in candidates}
        return [by_code[code] for code in codes if code in by_code]

    def _primary_strategy(self, item: Dict[str, Any]) -> str:
        tags = item.get("strategy_tags") or []
        if isinstance(tags, list) and tags:
            return str(tags[0] or "unknown")
        return "unknown"

    def _compact_value(self, value: Any, max_len: int = 40) -> str:
        if isinstance(value, list):
            text = ",".join(str(item) for item in value[:5])
        else:
            text = "" if value is None else str(value)
        text = re.sub(r"\s+", "", text)
        if not text:
            return "-"
        return text[:max_len]

    def _fmt_num(self, value: Any, digits: int = 2) -> str:
        number = self._safe_float(value)
        if number is None:
            return "-"
        return f"{number:.{digits}f}"

    def _fmt_market_cap(self, value: Any) -> str:
        number = self._safe_float(value)
        if number is None:
            return "-"
        return f"{number / 1e8:.1f}"

    def _safe_float(self, value: Any) -> float | None:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
