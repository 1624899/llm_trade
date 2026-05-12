"""AI 轻量精筛 Agent。

该 Agent 只处理规则海选后的极简快照，不拉取财报、研报或长新闻，
用于在深度复核前把多策略候选池压缩为更小的自选池。
"""

import json
import re
from typing import Any, Dict, List

from loguru import logger

from src.agent.tools import tools


BACKTEST_STRATEGY_PRIORS = {
    "first_limit_up_breakout": {"bonus": 22.0, "max_share": 0.40},
    "momentum_leader": {"bonus": 8.0, "max_share": 0.30},
    "dragon_pullback": {"bonus": 4.0, "max_share": 0.20},
    "trend_breakout": {"bonus": -18.0, "max_share": 0.45},
    "support_pullback": {"bonus": -10.0, "max_share": 0.20},
}


class QuickFilterAgent:
    """结合宏观环境，从多策略候选池中快速精选少量标的。"""

    def filter_candidates(
        self,
        candidates: List[Dict[str, Any]],
        macro_context: Dict[str, Any] | None = None,
        target_n: int = 8,
        use_llm: bool = True,
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
        if not use_llm:
            return self._rule_based_fallback(
                candidates,
                target_n,
                macro_context,
                reason="回测模式禁用 LLM，使用轻量精筛的确定性规则排序。",
                mode="rule_backtest",
            )

        system_prompt = f"""
你是 A 股盘后快速精筛助手。核心目标不是选最高收益，而是从海选池中选出未来 5/10/20 个交易日胜率更高的股票，最多 {target_n} 只进入深度复核；其中 10 个交易日胜率是主目标，5 日和 20 日作为辅助约束。
硬规则：
1. 必须覆盖检查全部候选，selected_codes 只能来自候选短表，数量不得超过 {target_n}。
2. 胜率优先：优先选择“走势仍健康、位置不过热、量价承接正常、策略周期匹配”的候选；不要只按技术分或置信度排序。
3. 组合约束：不要让 trend_breakout 单一策略占据过多名额；若首板启动、支撑回踩里有量价健康的候选，应预留名额做策略分散。除非其他策略明显不合格，trend_breakout 通常不超过入选数量的约 45%。
4. 趋势突破类：保留 5/10/20 日涨幅温和、MA20 偏离不过大、量比不过热且仍有承接的标的；严杀高 bias20、高 ret20 透支、放量过急的趋势股，尤其是 bias20 接近或高于 1.10、ret20 高于 15%、量比高于 1.55 后仍缺乏继续确认的标的；谨慎保留短期涨幅过大、放量滞涨、连续加速后靠近高位的标的。同一行业/主题出现多只类似趋势股时，只保留量价最健康者，剔除弱承接和补涨尾段。
5. 首板启动/首板突破类：技术分可能偏低，不能因此直接淘汰；若涨幅尚未过热、量能活跃但未失控、10/20 日趋势改善，可以作为短中期胜率候选。低技术分但形态刚启动的候选，优先和高位趋势股比较，而不是直接排在最后。
6. 支撑回踩类：优先缩量回踩、跌幅收敛、位置接近均线/平台支撑的候选；剔除放量跌破、弱反抽、20 日趋势持续走坏的候选。
7. 偏弱/退潮市降低追高和高波动权重；强势市可提高趋势突破、首板启动权重，但仍要排除过热和量价背离。
8. 不得编造未提供信息。reason 要写清保留或剔除的胜率依据，尤其说明其对 10 日胜率的影响。
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
        header = "序号|代码|名称|行业|主策略|持有日|置信|技分|题材|涨幅|5日|10日|20日|量比|换手|偏MA20|PE|PB|市值亿"
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
                str(snapshot.get("preferred_holding_days") or ""),
                self._fmt_num(snapshot.get("strategy_confidence"), 2),
                self._fmt_num(snapshot.get("technical_score"), 1),
                self._fmt_num(snapshot.get("theme_score"), 1),
                self._fmt_num(snapshot.get("change_pct"), 1),
                self._fmt_num(snapshot.get("ret5"), 1),
                self._fmt_num(snapshot.get("ret10"), 1),
                self._fmt_num(snapshot.get("ret20"), 1),
                self._fmt_num(snapshot.get("volume_ratio"), 2),
                self._fmt_num(snapshot.get("turnover_rate"), 2),
                self._fmt_bias20(snapshot.get("bias20_ratio")),
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
            "preferred_holding_days": item.get("preferred_holding_days", metrics.get("preferred_holding_days")),
            "max_holding_days": item.get("max_holding_days", metrics.get("max_holding_days")),
            "exit_hint": item.get("exit_hint", metrics.get("exit_hint")),
            "strategy_confidence": item.get("strategy_confidence"),
            "technical_score": item.get("technical_score"),
            "theme_score": item.get("theme_score", 0.0),
            "price": item.get("price"),
            "change_pct": item.get("change_pct"),
            "ret5": item.get("ret5", metrics.get("ret5")),
            "ret10": item.get("ret10", metrics.get("ret10")),
            "ret20": item.get("ret20", metrics.get("ret20")),
            "volume_ratio": item.get("volume_ratio", metrics.get("volume_ratio")),
            "turnover_rate": metrics.get("turnover_rate"),
            "bias20_ratio": item.get("bias20_ratio", metrics.get("bias20_ratio")),
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
        mode: str = "rule_fallback",
    ) -> Dict[str, Any]:
        ranked = sorted(
            candidates,
            key=lambda item: self._fallback_score(item, macro_context or {}),
            reverse=True,
        )
        selected = self._pick_with_strategy_caps(ranked, target_n)
        selected_codes = {str(item.get("code", "")).zfill(6) for item in selected}
        return {
            "selected_candidates": selected,
            "selected_codes": [str(item.get("code", "")).zfill(6) for item in selected],
            "mode": mode,
            "reason": reason,
            "evaluations": [
                {
                    "code": str(item.get("code", "")).zfill(6),
                    "score": round(self._fallback_score(item, macro_context or {}), 2),
                    "keep": str(item.get("code", "")).zfill(6) in selected_codes,
                    "reason": "按回测策略先验、路径胜率友好指标、宏观风格和策略配额降级排序。",
                }
                for idx, item in enumerate(ranked)
            ],
        }

    def _fallback_score(
        self,
        item: Dict[str, Any],
        macro_context: Dict[str, Any],
    ) -> float:
        score = float(item.get("technical_score") or 0) * 0.55
        score += float(item.get("strategy_confidence") or 0) * 55
        score += float(item.get("theme_score") or 0)
        primary = self._primary_strategy(item)
        score += float(BACKTEST_STRATEGY_PRIORS.get(primary, {}).get("bonus", 0.0))
        macro_text = json.dumps(macro_context, ensure_ascii=False).lower()
        weak_market = any(token in macro_text for token in ("偏弱", "退潮", "防守", "谨慎", "risk-off", "weak"))
        strong_market = any(token in macro_text for token in ("偏强", "进攻", "risk-on", "strong", "活跃"))
        ret5 = self._safe_float(item.get("ret5"))
        if ret5 is None and isinstance(item.get("key_metrics"), dict):
            ret5 = self._safe_float(item["key_metrics"].get("ret5"))
        ret10 = self._metric_float(item, "ret10")
        ret20 = self._metric_float(item, "ret20")
        volume_ratio = self._metric_float(item, "volume_ratio")
        bias20 = self._metric_float(item, "bias20_ratio")

        if weak_market:
            if primary in {"value_bottom", "support_pullback", "panic_reversal"}:
                score += 12
            if primary in {"momentum_leader", "trend_breakout"} and ret5 is not None and ret5 > 10:
                score -= 10
        elif strong_market:
            if primary in {"first_limit_up_breakout", "momentum_leader"}:
                score += 8
            elif primary == "trend_breakout":
                score += 3

        if primary == "first_limit_up_breakout":
            if ret5 is not None and ret5 <= 12:
                score += 7
            if ret20 is not None and ret20 <= 28:
                score += 5
            if volume_ratio is not None and 1.2 <= volume_ratio <= 3.0:
                score += 6
            if bias20 is not None and bias20 <= 1.18:
                score += 4
        elif primary == "trend_breakout":
            if ret5 is not None and ret5 > 8:
                score -= min(12, (ret5 - 8) * 1.2)
            if ret10 is not None and ret10 > 16:
                score -= min(10, (ret10 - 16) * 0.8)
            if bias20 is not None and bias20 > 1.12:
                score -= min(14, (bias20 - 1.12) * 70)
            if volume_ratio is not None and volume_ratio > 2.2:
                score -= min(8, (volume_ratio - 2.2) * 4)
        elif primary == "support_pullback":
            if ret20 is not None and ret20 < -5:
                score -= 8
            if volume_ratio is not None and volume_ratio <= 0.8:
                score += 4
        return score

    def _pick_with_strategy_caps(self, ranked: List[Dict[str, Any]], target_n: int) -> List[Dict[str, Any]]:
        """按策略上限挑选，防止规则精筛再次被单一弱势策略支配。"""
        if not ranked:
            return []
        strategies = {self._primary_strategy(item) for item in ranked}
        if len(strategies) <= 1:
            return ranked[:target_n]

        caps = {
            strategy: max(1, int(target_n * float(prior.get("max_share", 1.0)) + 0.999))
            for strategy, prior in BACKTEST_STRATEGY_PRIORS.items()
        }
        selected: List[Dict[str, Any]] = []
        selected_codes = set()
        counts: Dict[str, int] = {}

        def add(item: Dict[str, Any], enforce_cap: bool) -> bool:
            if len(selected) >= target_n:
                return False
            code = str(item.get("code", "")).zfill(6)
            if code in selected_codes:
                return False
            primary = self._primary_strategy(item)
            if enforce_cap and counts.get(primary, 0) >= caps.get(primary, target_n):
                return False
            selected.append(item)
            selected_codes.add(code)
            counts[primary] = counts.get(primary, 0) + 1
            return True

        for item in ranked:
            add(item, enforce_cap=True)
        for item in ranked:
            add(item, enforce_cap=False)
        return selected[:target_n]

    def _metric_float(self, item: Dict[str, Any], key: str) -> float | None:
        value = self._safe_float(item.get(key))
        if value is None and isinstance(item.get("key_metrics"), dict):
            value = self._safe_float(item["key_metrics"].get(key))
        return value

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

    def _fmt_bias20(self, value: Any) -> str:
        number = self._safe_float(value)
        if number is None:
            return "-"
        return f"{(number - 1.0) * 100:.1f}"

    def _safe_float(self, value: Any) -> float | None:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
