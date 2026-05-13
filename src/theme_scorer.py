"""
轻量级 A 股主题热度标识器。

评分器利用本地信号层的行业/概念资金流数据，为通过了硬性技术筛选规则的股票打上热门主题标签。
这有助于在深度分析或最终报告中标识出处于当前市场热点风口上的标的，但不参与技术分数的直接累加，避免因追逐情绪导致高位接盘。
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

import pandas as pd
from loguru import logger

from src.database import StockDatabase


class ThemeScorer:
    def __init__(self, top_boards_per_kind: int = 8, max_bonus: float = 12.0, db: StockDatabase = None):
        # 每个类别（行业/概念）考虑的热门板块数量
        self.top_boards_per_kind = max(1, int(top_boards_per_kind or 8))
        # 最大加分上限
        self.max_bonus = max(0.0, float(max_bonus or 0.0))
        self.db = db or StockDatabase()

    def score_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        对候选股票列表进行主题评分加分。
        """
        if not candidates:
            return candidates

        # 1. 加载当前热门板块列表
        board_rows = self._load_hot_boards()
        if not board_rows:
            return [self._attach_empty_theme_score(item) for item in candidates]

        # 2. 构建 股票代码 -> 所属热门板块 的映射
        code_to_boards = self._map_codes_to_boards(board_rows)
        
        scored = []
        for item in candidates:
            scored_item = dict(item)
            code = str(scored_item.get("code", "")).zfill(6)
            
            # 3. 匹配热门板块（通过代码映射或行业名称匹配）
            matched_boards = list(code_to_boards.get(code, []))
            matched_boards.extend(self._match_industry_board(scored_item, board_rows))
            
            # 4. 根据匹配到的板块提取主题标签
            theme_info = self._score_board_matches(matched_boards)
            
            scored_item["theme_score"] = 0.0  # 不再进行分数累加，保留字段兼容
            theme_score = float(theme_info["score"])
            scored_item["theme_score"] = round(theme_score, 2)
            scored_item["technical_score"] = round(float(scored_item.get("technical_score") or 0) + theme_score, 2)
            scored_item["theme_reason"] = theme_info["reason"]
            scored_item["matched_themes"] = theme_info["matched_themes"]
            
            if scored_item.get("key_metrics") is None:
                scored_item["key_metrics"] = {}
            scored_item["key_metrics"]["theme_score"] = round(theme_score, 2)
            scored.append(scored_item)
        return scored

    def _load_hot_boards(self) -> List[Dict[str, Any]]:
        """
        从本地信号层获取热门行业和概念板块。
        """
        query = """
            SELECT board_name, kind, trade_date, change_pct, main_net_inflow,
                   main_net_inflow_pct, leader_name, leader_change_pct
            FROM board_fund_flows
            WHERE trade_date = (SELECT MAX(trade_date) FROM board_fund_flows)
        """
        rows: List[Dict[str, Any]] = []
        try:
            boards = self.db.query_to_dataframe(query)
        except Exception as exc:
            logger.warning(f"[ThemeScorer] failed to load board fund flows: {exc}")
            return rows
        if boards is None or boards.empty:
            return rows

        boards = boards.copy()
        boards["change_pct"] = pd.to_numeric(boards.get("change_pct"), errors="coerce")
        boards["main_net_inflow"] = pd.to_numeric(boards.get("main_net_inflow"), errors="coerce")
        boards["leader_change_pct"] = pd.to_numeric(boards.get("leader_change_pct"), errors="coerce")

        for kind in ("industry", "concept"):
            selected = (
                boards[boards["kind"].astype(str) == kind]
                .sort_values(["main_net_inflow", "change_pct"], ascending=False)
                .head(self.top_boards_per_kind)
            )
            for _, row in selected.iterrows():
                rows.append(
                    {
                        "kind": kind,
                        "name": row.get("board_name"),
                        "change_pct": self._float_or_zero(row.get("change_pct")),
                        "main_net_inflow": self._float_or_zero(row.get("main_net_inflow")),
                        "main_net_inflow_pct": self._float_or_zero(row.get("main_net_inflow_pct")),
                        "leader_name": row.get("leader_name"),
                        "leader_change_pct": self._float_or_zero(row.get("leader_change_pct")),
                    }
                )
        return rows

    def _map_codes_to_boards(self, board_rows: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        建立 股票代码 -> 板块信息 的映射字典。
        """
        board_by_name = {str(board.get("name") or ""): board for board in board_rows if board.get("kind") == "concept"}
        if not board_by_name:
            return {}
        placeholders = ", ".join(["?"] * len(board_by_name))
        query = f"""
            SELECT code, concept_name
            FROM stock_concepts
            WHERE concept_name IN ({placeholders})
        """
        try:
            members = self.db.query_to_dataframe(query, tuple(board_by_name.keys()))
        except Exception as exc:
            logger.warning(f"[ThemeScorer] failed to load stock concept members: {exc}")
            return {}
        if members is None or members.empty:
            return {}

        code_to_boards: Dict[str, List[Dict[str, Any]]] = {}
        for _, row in members.iterrows():
            code = str(row.get("code") or "").zfill(6)
            board = board_by_name.get(str(row.get("concept_name") or ""))
            if code and board:
                code_to_boards.setdefault(code, []).append({**board, "up_count": 0, "member_count": 0, "up_ratio": 0.0, "active_amount": 0.0})
        return code_to_boards

    def _summarize_board_members(self, members: pd.DataFrame) -> Dict[str, Any]:
        """统计板块成员的上涨家数、比例及总成交额。"""
        change = pd.to_numeric(members.get("change_pct"), errors="coerce").dropna()
        amount = pd.to_numeric(members.get("amount"), errors="coerce").dropna()
        if change.empty:
            return {"up_count": 0, "member_count": 0, "up_ratio": 0.0, "active_amount": float(amount.sum() or 0)}
        return {
            "up_count": int((change > 0).sum()),
            "member_count": int(len(change)),
            "up_ratio": float((change > 0).sum() / max(len(change), 1)),
            "active_amount": float(amount.sum() or 0),
        }

    def _match_industry_board(
        self,
        candidate: Dict[str, Any],
        board_rows: Iterable[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """基于股票自身的行业字段与热门板块名称进行模糊匹配。"""
        industry = str(candidate.get("industry") or "").strip()
        if not industry:
            return []
        matches = []
        for board in board_rows:
            if board.get("kind") != "industry":
                continue
            board_name = str(board.get("name") or "")
            if industry == board_name or industry in board_name or board_name in industry:
                matches.append({**board, "up_count": 0, "member_count": 0, "up_ratio": 0.0, "active_amount": 0.0})
        return matches

    def _score_board_matches(self, boards: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        提取匹配到的热门板块信息。
        不再进行分数累加，仅作为标识用途。
        """
        if not boards:
            return {"score": 0.0, "reason": "未命中资金流强势行业/概念板块", "matched_themes": []}

        # 去重并保留最强的板块信号
        by_name: Dict[str, Dict[str, Any]] = {}
        for board in boards:
            name = str(board.get("name") or "")
            if not name:
                continue
            prev = by_name.get(name)
            if prev is None or float(board.get("change_pct") or 0) > float(prev.get("change_pct") or 0):
                by_name[name] = board
        unique_boards = sorted(by_name.values(), key=lambda item: float(item.get("change_pct") or 0), reverse=True)

        matched_themes = []
        score = 0.0
        # 提取前 3 个最强板块的信息
        for idx, board in enumerate(unique_boards[:3]):
            change_pct = float(board.get("change_pct") or 0.0)
            up_ratio = float(board.get("up_ratio") or 0.0)
            leader_change_pct = float(board.get("leader_change_pct") or 0.0)
            main_net_inflow = float(board.get("main_net_inflow") or 0.0)
            flow_bonus = min(max(main_net_inflow, 0.0) / 100000000, 5.0)
            weight = [1.0, 0.6, 0.4][idx]
            score += weight * (
                max(0.0, change_pct) * 0.8
                + max(0.0, leader_change_pct) * 0.25
                + flow_bonus
                + max(0.0, up_ratio) * 4.0
            )
            
            matched_themes.append(
                {
                    "name": board.get("name"),
                    "kind": board.get("kind"),
                    "change_pct": change_pct,
                    "up_count": int(board.get("up_count") or 0),
                    "member_count": int(board.get("member_count") or 0),
                    "up_ratio": round(up_ratio, 4),
                    "main_net_inflow": main_net_inflow,
                }
            )

        theme_names = "、".join(str(item["name"]) for item in matched_themes)
        reason = f"当前处于资金流强势行业/概念内：{theme_names}。"
        return {"score": min(self.max_bonus, round(score, 2)), "reason": reason, "matched_themes": matched_themes}

    def _attach_empty_theme_score(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """无法获取主题数据时的回退处理。"""
        scored_item = dict(item)
        scored_item["theme_score"] = 0.0
        scored_item["theme_reason"] = "主题数据不可用，未匹配。"
        scored_item["matched_themes"] = []
        if scored_item.get("key_metrics") is None:
            scored_item["key_metrics"] = {}
        scored_item["key_metrics"]["theme_score"] = 0.0
        return scored_item

    def _float_or_zero(self, value: Any) -> float:
        try:
            if pd.isna(value):
                return 0.0
            return float(value)
        except (TypeError, ValueError):
            return 0.0
