"""观察仓（推荐股票池）管理。

观察仓是一个候选池，而非真实的模拟持仓记录。它存储了
最新的推荐上下文，供后续交易智能体（Trading Agent）使用及参考。
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

from src.database import StockDatabase
from src.quote_sources import fetch_latest_prices


class Watchlist:
    """观察仓类，负责管理和持久化股票的推荐与追踪状态。"""

    def __init__(self, db: Optional[StockDatabase] = None, max_items: int = 10):
        """
        初始化观察仓。

        Args:
            db: 股票数据库实例，如果未提供则默认创建。
            max_items: 观察仓允许的最大容量上限。
        """
        self.db = db or StockDatabase()
        self.max_items = max(1, int(max_items or 10))

    def upsert_recommendations(
        self,
        selected_codes: Iterable[str],
        stock_profiles: Dict[str, Dict[str, Any]],
        final_report: str = "",
        macro_context: Optional[Dict[str, Any]] = None,
        source: str = "pick",
    ) -> int:
        """
        批量添加或更新入选的推荐标的到观察仓中。

        Args:
            selected_codes: 被选中的股票代码列表。
            stock_profiles: 股票画像字典表（代码 -> 画像数据）。
            final_report: 周末或日结生成的最终分析报告文本（用于提取预期收益率和推荐档位）。
            macro_context: 宏观环境上下文。
            source: 来源标记，例如 "pick"。

        Returns:
            成功更新/添加的标的数量。
        """
        count = 0
        for raw_code in selected_codes or []:
            code = str(raw_code).zfill(6)
            profile = stock_profiles.get(code)
            if not profile:
                logger.warning("[观察仓] 未在股票画像数据中找到被选中代码: {}", code)
                continue
            if self.upsert_item(profile, final_report=final_report, macro_context=macro_context, source=source):
                count += 1
                
        # 强制清理超出上限的条目
        self.prune_to_limit()
        return count

    def upsert_item(
        self,
        profile: Dict[str, Any],
        final_report: str = "",
        macro_context: Optional[Dict[str, Any]] = None,
        source: str = "pick",
    ) -> bool:
        """更新单个股票标的到观察仓数据库中。"""
        asset = profile.get("asset_info") or {}
        code = str(asset.get("code") or profile.get("code") or "").zfill(6)
        if not re.fullmatch(r"\d{6}", code):
            logger.warning("[观察仓] 跳过无效代码: {}", code)
            return False

        name = str(asset.get("name") or profile.get("name") or code)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_price = self._get_current_price(code)
        
        existing = self.get_item(code)
        entry_price = self._to_float(existing.get("entry_price")) if existing else current_price
        return_pct = self._return_pct(current_price, entry_price)
        
        tier = self._infer_tier(code, final_report)
        reason = self._compose_reason(profile)

        # 核心插入/更新逻辑 (UPSERT)
        sql = """
            INSERT INTO watchlist_items
            (code, name, tier, watch_status, source, added_at, updated_at, entry_price,
             current_price, return_pct, expected_return_pct, recommend_reason,
             fundamental_analysis, technical_analysis, news_risk_analysis, macro_context, remove_reason)
            VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                tier=excluded.tier,
                watch_status='ACTIVE',
                source=excluded.source,
                updated_at=excluded.updated_at,
                current_price=excluded.current_price,
                return_pct=excluded.return_pct,
                expected_return_pct=excluded.expected_return_pct,
                recommend_reason=excluded.recommend_reason,
                fundamental_analysis=excluded.fundamental_analysis,
                technical_analysis=excluded.technical_analysis,
                news_risk_analysis=excluded.news_risk_analysis,
                macro_context=excluded.macro_context,
                remove_reason=NULL
        """
        return self.db.execute_non_query(
            sql,
            (
                code,
                name,
                tier,
                source,
                now,
                now,
                entry_price,
                current_price,
                return_pct,
                self._infer_expected_return(final_report, code),
                reason,
                str(profile.get("fundamental_analysis") or ""),
                str(profile.get("technical_analysis") or ""),
                str(profile.get("news_risk_analysis") or ""),
                json.dumps(macro_context or {}, ensure_ascii=False, default=str),
            ),
        )

    def refresh_prices(self) -> None:
        """刷新观察仓内所有活跃标的最新价格。"""
        items = self.list_active()
        if not items:
            return
            
        codes = [item["code"] for item in items]
        prices = self._get_latest_prices_batch(codes)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for item in items:
            code = str(item["code"]).zfill(6)
            price = self._to_float(prices.get(code))
            entry_price = self._to_float(item.get("entry_price"))
            
            if price is None or price <= 0:
                continue
                
            self.db.execute_non_query(
                "UPDATE watchlist_items SET current_price = ?, return_pct = ?, updated_at = ? WHERE id = ?",
                (price, self._return_pct(price, entry_price), now, int(item["id"])),
            )

    def list_active(self) -> List[Dict[str, Any]]:
        """
        获取当前处于 ACTIVE 状态的观察列表。
        按推荐档位及更新时间排序。
        """
        df = self.db.query_to_dataframe(
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
        )
        if df.empty:
            return []
        return df.to_dict("records")

    def get_item(self, code: str) -> Dict[str, Any]:
        """根据股票代码获取其在观察仓中的信息。"""
        df = self.db.query_to_dataframe(
            "SELECT * FROM watchlist_items WHERE code = ? LIMIT 1",
            (str(code).zfill(6),),
        )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()

    def apply_trading_decisions(self, decisions: Iterable[Dict[str, Any]]) -> int:
        """
        同步交易智能体（TradingAgent）对观察仓的维护动作，避免候选池只进不出。
        例如硬性风险卖出、或者明确移出的操作，会同步在此移除记录。
        """
        removed = 0
        for decision in decisions or []:
            code = str(decision.get("code") or "").zfill(6)
            if not re.fullmatch(r"\d{6}", code):
                continue
                
            action = str(decision.get("action") or "").upper()
            
            # 手动或交易策略明确移出
            if action == "REMOVE":
                if self.remove_item(code, str(decision.get("reason") or "TradingAgent 移出观察仓")):
                    removed += 1
                continue
                
            # 当交易仓持仓因严重风险被清仓卖出时，同步移出观察仓
            if action == "SELL" and self._is_hard_exit_decision(decision):
                reason = str(decision.get("reason") or "交易仓触发硬风险卖出，观察仓同步移出")
                if self.remove_item(code, reason):
                    removed += 1
                    
        if removed:
            logger.info("[观察仓] 根据交易决策移出 {} 只观察标的", removed)
        return removed

    def remove_item(self, code: str, reason: str) -> bool:
        """将某一标的设为 'REMOVED'（软删除）状态。"""
        existing = self.get_item(code)
        if not existing or str(existing.get("watch_status") or "") != "ACTIVE":
            return False
            
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.db.execute_non_query(
            """
            UPDATE watchlist_items
            SET watch_status = 'REMOVED', remove_reason = ?, updated_at = ?
            WHERE code = ? AND watch_status = 'ACTIVE'
            """,
            (reason, now, str(code).zfill(6)),
        )

    def prune_to_limit(self) -> None:
        """
        裁剪观察仓大小，确保 ACTIVE 的标的不会超过设置的容量上限（max_items）。
        按优先级踢出较低或者较旧的记录。
        """
        active = self.list_active()
        overflow = active[self.max_items :]
        if not overflow:
            return
            
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in overflow:
            self.db.execute_non_query(
                """
                UPDATE watchlist_items
                SET watch_status = 'REMOVED', remove_reason = ?, updated_at = ?
                WHERE id = ?
                """,
                ("观察仓超过上限，移出优先级较低标的", now, int(item["id"])),
            )

    def _get_current_price(self, code: str) -> float:
        """获取单个标的新最新价格。"""
        prices = self._get_latest_prices_batch([code])
        return float(prices.get(str(code).zfill(6), 0) or 0)

    def _get_latest_prices_batch(self, codes: List[str]) -> Dict[str, float]:
        """批量获取标的最新价格。会先尝试在线拉取，缺失的记录退而到日线行情库查询。"""
        normalized = [str(code).zfill(6) for code in codes]
        prices = fetch_latest_prices(normalized)
        missing = [code for code in normalized if code not in prices]
        
        if missing:
            placeholders = ",".join(["?"] * len(missing))
            df = self.db.query_to_dataframe(
                f"""
                SELECT q.code, q.price
                FROM daily_quotes q
                JOIN (
                    SELECT code, MAX(trade_date) AS trade_date
                    FROM daily_quotes
                    WHERE code IN ({placeholders})
                    GROUP BY code
                ) latest ON q.code = latest.code AND q.trade_date = latest.trade_date
                """,
                tuple(missing),
            )
            if not df.empty:
                prices.update(dict(zip(df["code"].astype(str).str.zfill(6), df["price"].astype(float))))
                
        return {str(code).zfill(6): float(price) for code, price in prices.items() if price}

    @staticmethod
    def _compose_reason(profile: Dict[str, Any]) -> str:
        """将股票画像中的基本面、技术面、资讯风险等组合成推荐理由摘要。"""
        parts = []
        for key, label in [
            ("fundamental_analysis", "基本面分析"),
            ("technical_analysis", "技术面分析"),
            ("news_risk_analysis", "风控与资讯"),
        ]:
            value = profile.get(key)
            if value:
                parts.append(f"【{label}】\n{value}")
        return "\n\n".join(parts)

    @staticmethod
    def _infer_tier(code: str, final_report: str) -> str:
        """根据最终研报文本，推断代码的推荐档位（如：强推荐 / 配置验证 / 观察）。"""
        if not final_report:
            return "配置/轻仓验证"
        idx = final_report.find(code)
        if idx < 0:
            return "配置/轻仓验证"
            
        window = final_report[max(0, idx - 120) : idx + 220]
        for tier in ("强推荐", "配置/轻仓验证", "观察", "不推荐"):
            if tier in window:
                return tier
        return "配置/轻仓验证"

    @staticmethod
    def _infer_expected_return(final_report: str, code: str) -> float | None:
        """从研报上下文提取代码相对应的预期收益率数字。"""
        idx = final_report.find(code) if final_report else -1
        if idx < 0:
            return None
            
        window = final_report[max(0, idx - 120) : idx + 260]
        matches = re.findall(r"(\d+(?:\.\d+)?)\s*%", window)
        
        if not matches:
            return None
        values = [float(item) for item in matches]
        return max(values) if values else None

    @staticmethod
    def _return_pct(current_price: float | None, entry_price: float | None) -> float | None:
        """计算累计收益率 %。"""
        if not current_price or not entry_price:
            return None
        return round((float(current_price) / float(entry_price) - 1) * 100, 2)

    @staticmethod
    def _is_hard_exit_decision(decision: Dict[str, Any]) -> bool:
        """判断该决策是否为严重风险导致的硬性退出（例如基本面/财务造假等致命风险）。"""
        if bool(decision.get("risk_override")):
            signal = decision.get("exit_signal") if isinstance(decision.get("exit_signal"), dict) else {}
            try:
                if int(signal.get("action_level", 0) or 0) >= 3:
                    return True
            except (TypeError, ValueError):
                pass
                
        reason = str(decision.get("reason") or "")
        return any(word in reason for word in ("清仓", "回避", "硬风险", "风险恶化", "趋势失效", "基本面恶化"))

    @staticmethod
    def _to_float(value: Any) -> float | None:
        """安全转换为浮点数。"""
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
