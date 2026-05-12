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

    PERFORMANCE_HORIZONS = (5, 10, 20, 30, 60)

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
        decision_snapshot = self._extract_decision_snapshot(final_report, code)
        expected_holding_days = self._infer_expected_holding_days(profile, final_report, code)
        max_holding_days = self._infer_max_holding_days(profile, final_report, code)
        exit_hint = str(asset.get("exit_hint") or profile.get("exit_hint") or "").strip()

        # 核心插入/更新逻辑 (UPSERT)
        sql = """
            INSERT INTO watchlist_items
            (code, name, tier, watch_status, source, added_at, updated_at, entry_price,
             current_price, return_pct, expected_return_pct, expected_holding_days, max_holding_days, exit_hint, recommend_reason,
             fundamental_analysis, technical_analysis, news_risk_analysis, decision_snapshot, macro_context, remove_reason)
            VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                tier=excluded.tier,
                watch_status='ACTIVE',
                source=excluded.source,
                updated_at=excluded.updated_at,
                current_price=excluded.current_price,
                return_pct=excluded.return_pct,
                expected_return_pct=excluded.expected_return_pct,
                expected_holding_days=excluded.expected_holding_days,
                max_holding_days=excluded.max_holding_days,
                exit_hint=excluded.exit_hint,
                recommend_reason=excluded.recommend_reason,
                fundamental_analysis=excluded.fundamental_analysis,
                technical_analysis=excluded.technical_analysis,
                news_risk_analysis=excluded.news_risk_analysis,
                decision_snapshot=excluded.decision_snapshot,
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
                expected_holding_days,
                max_holding_days,
                exit_hint,
                reason,
                str(profile.get("fundamental_analysis") or ""),
                str(profile.get("technical_analysis") or ""),
                str(profile.get("news_risk_analysis") or ""),
                decision_snapshot,
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

    def refresh_recommendation_performance(self, include_removed: bool = True) -> int:
        """刷新全部历史推荐的当前收益、经过天数和关键持有节点收益。"""
        items = self._list_for_performance(include_removed=include_removed)
        if not items:
            return 0

        codes = [str(item.get("code") or "").zfill(6) for item in items if item.get("code")]
        latest_prices = self._get_latest_prices_batch(codes)
        min_entry_date = min(
            (date for date in (self._normalize_trade_date(item.get("added_at")) for item in items) if date),
            default=None,
        )
        bars_by_code = self._load_performance_bars(codes, min_entry_date) if min_entry_date else {}
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated = 0

        for item in items:
            code = str(item.get("code") or "").zfill(6)
            entry_price = self._to_float(item.get("entry_price"))
            if not code or not entry_price:
                continue

            entry_date = self._normalize_trade_date(item.get("added_at"))
            bars = [
                bar
                for bar in bars_by_code.get(code, [])
                if not entry_date or str(bar.get("trade_date") or "") >= entry_date
            ]
            latest_bar = bars[-1] if bars else {}
            latest_trade_date = str(latest_bar.get("trade_date") or "") or None
            latest_bar_price = self._to_float(latest_bar.get("price"))
            current_price = self._to_float(latest_prices.get(code)) or latest_bar_price
            if not current_price:
                continue
            if not latest_trade_date and entry_date:
                latest_trade_date = entry_date

            elapsed_trade_days = max(0, len(bars) - 1) if bars else None
            if elapsed_trade_days is None and latest_trade_date:
                elapsed_trade_days = 0
            elapsed_days = self._elapsed_calendar_days(entry_date, latest_trade_date)
            horizon_returns = self._calculate_horizon_returns(bars, entry_price)
            ok = self.db.execute_non_query(
                """
                UPDATE watchlist_items
                SET current_price = ?,
                    return_pct = ?,
                    elapsed_days = ?,
                    elapsed_trade_days = ?,
                    latest_trade_date = ?,
                    return_5d_pct = ?,
                    return_10d_pct = ?,
                    return_20d_pct = ?,
                    return_30d_pct = ?,
                    return_60d_pct = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    current_price,
                    self._return_pct(current_price, entry_price),
                    elapsed_days,
                    elapsed_trade_days,
                    latest_trade_date,
                    horizon_returns.get(5),
                    horizon_returns.get(10),
                    horizon_returns.get(20),
                    horizon_returns.get(30),
                    horizon_returns.get(60),
                    now,
                    int(item["id"]),
                ),
            )
            if ok:
                updated += 1
        logger.info("[观察仓] 已刷新历史推荐表现 {} 条，include_removed={}", updated, include_removed)
        return updated

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

    def list_history_for_trading(self, limit: int = 30) -> List[Dict[str, Any]]:
        """给交易 Agent 提供历史推荐表现样本，用于校准当前风格。"""
        limit = max(1, min(int(limit or 30), 200))
        df = self.db.query_to_dataframe(
            """
            SELECT
                code, name, tier, watch_status, added_at, updated_at,
                entry_price, current_price, return_pct, elapsed_days,
                elapsed_trade_days, latest_trade_date, return_5d_pct,
                return_10d_pct, return_20d_pct, return_30d_pct,
                return_60d_pct, expected_return_pct, exit_hint, remove_reason
            FROM watchlist_items
            ORDER BY
                CASE WHEN return_pct IS NULL THEN 1 ELSE 0 END,
                COALESCE(added_at, updated_at) DESC,
                id DESC
            LIMIT ?
            """,
            (limit,),
        )
        if df.empty:
            return []
        return df.to_dict(orient="records")

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
            missing = [code for code in normalized if code not in prices]

        if missing:
            placeholders = ",".join(["?"] * len(missing))
            df = self.db.query_to_dataframe(
                f"""
                SELECT b.code, b.close AS price
                FROM market_bars b
                JOIN (
                    SELECT code, MAX(trade_date) AS trade_date
                    FROM market_bars
                    WHERE period = 'daily'
                      AND code IN ({placeholders})
                    GROUP BY code
                ) latest ON b.code = latest.code AND b.trade_date = latest.trade_date
                WHERE b.period = 'daily'
                """,
                tuple(missing),
            )
            if not df.empty:
                prices.update(dict(zip(df["code"].astype(str).str.zfill(6), df["price"].astype(float))))
                
        return {str(code).zfill(6): float(price) for code, price in prices.items() if price}

    def _list_for_performance(self, include_removed: bool) -> List[Dict[str, Any]]:
        """读取需要刷新表现的观察仓记录。"""
        status_clause = "" if include_removed else "WHERE watch_status = 'ACTIVE'"
        df = self.db.query_to_dataframe(
            f"""
            SELECT *
            FROM watchlist_items
            {status_clause}
            ORDER BY added_at, id
            """
        )
        if df.empty:
            return []
        return df.to_dict("records")

    def _load_performance_bars(self, codes: List[str], min_trade_date: str) -> Dict[str, List[Dict[str, Any]]]:
        """加载节点收益所需日线，market_bars 优先，daily_quotes 补缺。"""
        code_list = sorted({str(code).zfill(6) for code in codes if str(code).zfill(6).isdigit()})
        if not code_list or not min_trade_date:
            return {}
        placeholders = ",".join(["?"] * len(code_list))
        params = tuple([min_trade_date] + code_list)
        market_df = self.db.query_to_dataframe(
            f"""
            SELECT code, trade_date, close AS price
            FROM market_bars
            WHERE period = 'daily'
              AND trade_date >= ?
              AND code IN ({placeholders})
            ORDER BY code, trade_date
            """,
            params,
        )
        quote_df = self.db.query_to_dataframe(
            f"""
            SELECT code, trade_date, price
            FROM daily_quotes
            WHERE trade_date >= ?
              AND code IN ({placeholders})
            ORDER BY code, trade_date
            """,
            params,
        )

        rows_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        for df in (quote_df, market_df):
            if df.empty:
                continue
            for _, row in df.iterrows():
                code = str(row["code"]).zfill(6)
                trade_date = str(row["trade_date"])
                price = self._to_float(row.get("price"))
                if not price:
                    continue
                rows_by_key[(code, trade_date)] = {"code": code, "trade_date": trade_date, "price": price}

        bars_by_code: Dict[str, List[Dict[str, Any]]] = {}
        for (code, _trade_date), row in sorted(rows_by_key.items(), key=lambda item: (item[0][0], item[0][1])):
            bars_by_code.setdefault(code, []).append(row)
        return bars_by_code

    @classmethod
    def _calculate_horizon_returns(cls, bars: List[Dict[str, Any]], entry_price: float) -> Dict[int, float | None]:
        """按第 N 个交易日收盘价计算节点收益。"""
        results: Dict[int, float | None] = {}
        if not bars or not entry_price:
            return {day: None for day in cls.PERFORMANCE_HORIZONS}
        for day in cls.PERFORMANCE_HORIZONS:
            if len(bars) <= day:
                results[day] = None
                continue
            price = cls._to_float(bars[day].get("price"))
            results[day] = cls._return_pct(price, entry_price) if price else None
        return results

    @staticmethod
    def _normalize_trade_date(value: Any) -> str | None:
        if value in (None, ""):
            return None
        text = str(value).strip()
        if " " in text:
            text = text.split(" ", 1)[0]
        text = text.replace("-", "")
        return text if len(text) == 8 and text.isdigit() else None

    @staticmethod
    def _elapsed_calendar_days(entry_date: str | None, latest_trade_date: str | None) -> int | None:
        if not entry_date or not latest_trade_date:
            return None
        try:
            entry = datetime.strptime(entry_date, "%Y%m%d")
            latest = datetime.strptime(latest_trade_date, "%Y%m%d")
            return max(0, (latest - entry).days)
        except ValueError:
            return None

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
        """从最终决策语义中提取预期收益率，避免误抓财务指标百分比。"""
        if not final_report:
            return None

        table_value = Watchlist._infer_expected_return_from_table(final_report, code)
        if table_value is not None:
            return table_value

        idx = final_report.find(str(code).zfill(6))
        if idx < 0:
            return None

        window = final_report[max(0, idx - 120) : idx + 260]
        semantic_patterns = [
            r"(?:预期收益空间|中期预期收益空间|预期收益|收益空间|中期空间|预期)[^\d%\n|]{0,40}(\d+(?:\.\d+)?)\s*%",
            r"(\d+(?:\.\d+)?)\s*%[^\d。\n|]{0,16}(?:预期收益空间|中期预期收益空间|预期收益|收益空间|中期空间)",
        ]
        for pattern in semantic_patterns:
            matches = re.findall(pattern, window)
            if matches:
                values = [float(item) for item in matches]
                return max(values) if values else None
        return None

    @staticmethod
    def _infer_expected_return_from_table(final_report: str, code: str) -> float | None:
        """优先从最终决策表的“预期收益空间”列提取收益率。"""
        normalized_code = str(code).zfill(6)
        lines = final_report.splitlines()
        for index, line in enumerate(lines):
            row_text = line.strip()
            if normalized_code not in row_text or not row_text.startswith("|") or not row_text.endswith("|"):
                continue

            row_cells = Watchlist._split_markdown_row(row_text)
            header_cells = Watchlist._find_markdown_header(lines, index)
            if not row_cells:
                continue

            if header_cells:
                for column_index, header in enumerate(header_cells):
                    header_key = re.sub(r"\s+", "", header)
                    if "预期" not in header_key or ("收益" not in header_key and "空间" not in header_key):
                        continue
                    if column_index >= len(row_cells):
                        continue
                    value = Watchlist._extract_percent_value(row_cells[column_index])
                    if value is not None:
                        return value

            # 部分历史 decision_snapshot 只保留数据行。最终决策表约定第 3 列是预期收益空间。
            if len(row_cells) >= 3 and any(tier in "".join(row_cells[:3]) for tier in ("强推荐", "配置/轻仓验证", "观察", "不推荐")):
                value = Watchlist._extract_percent_value(row_cells[2])
                if value is not None:
                    return value
        return None

    @staticmethod
    def _find_markdown_header(lines: List[str], row_index: int) -> List[str]:
        """向上查找 Markdown 表头，跳过分隔行。"""
        for prior in range(row_index - 1, max(-1, row_index - 6), -1):
            text = lines[prior].strip()
            if not text.startswith("|") or not text.endswith("|"):
                continue
            cells = Watchlist._split_markdown_row(text)
            compact = "".join(cells)
            if re.fullmatch(r"[:\-\|\s]+", text):
                continue
            if "股票" in compact and any("预期" in cell for cell in cells):
                return cells
        return []

    @staticmethod
    def _split_markdown_row(row: str) -> List[str]:
        """拆分简单 Markdown 表格行。"""
        return [cell.strip() for cell in row.strip().strip("|").split("|")]

    @staticmethod
    def _extract_percent_value(text: str) -> float | None:
        """从单个语义单元中提取百分比，区间取上限。"""
        matches = re.findall(r"(\d+(?:\.\d+)?)\s*%", text or "")
        if not matches:
            return None
        values = [float(item) for item in matches]
        return max(values) if values else None

    @classmethod
    def _infer_expected_holding_days(cls, profile: Dict[str, Any], final_report: str, code: str) -> int | None:
        """优先使用策略画像中的建议持有期，缺失时再从最终报告中提取。"""
        asset = profile.get("asset_info") or {}
        value = cls._to_int(asset.get("preferred_holding_days") or profile.get("preferred_holding_days"))
        if value:
            return value
        return cls._extract_holding_days_from_report(final_report, code)

    @classmethod
    def _infer_max_holding_days(cls, profile: Dict[str, Any], final_report: str, code: str) -> int | None:
        """提取最长验证期，用于 TradingAgent 区分短线验证和中期配置。"""
        asset = profile.get("asset_info") or {}
        value = cls._to_int(asset.get("max_holding_days") or profile.get("max_holding_days"))
        if value:
            return value
        days = cls._extract_holding_days_from_report(final_report, code)
        return days

    @staticmethod
    def _extract_holding_days_from_report(final_report: str, code: str) -> int | None:
        """从最终报告局部文本中提取“3日/3-5日/20日”等持有期限。"""
        idx = final_report.find(code) if final_report else -1
        if idx < 0:
            return None

        window = final_report[max(0, idx - 160) : idx + 360]
        range_match = re.search(r"(\d+)\s*[-~至到]\s*(\d+)\s*日", window)
        if range_match:
            return int(range_match.group(1))

        match = re.search(r"(?:持有|验证|期限|周期)[^\d]{0,12}(\d+)\s*日", window)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def _extract_decision_snapshot(final_report: str, code: str) -> str:
        """提取最新研报中对应股票的最终决策表行，供调仓 Agent 使用。"""
        if not final_report:
            return ""

        normalized_code = str(code).zfill(6)
        rows = []
        lines = final_report.splitlines()
        for index, line in enumerate(lines):
            text = line.strip()
            if normalized_code not in text:
                continue
            if text.startswith("|") and text.endswith("|"):
                header = []
                for prior in lines[max(0, index - 2) : index]:
                    prior_text = prior.strip()
                    if prior_text.startswith("|") and prior_text.endswith("|"):
                        header.append(re.sub(r"\s+", " ", prior_text))
                rows.append("\n".join(header + [re.sub(r"\s+", " ", text)]))

        if rows:
            return "\n".join(rows)[:2000]

        idx = final_report.find(normalized_code)
        if idx < 0:
            return ""
        snapshot = final_report[max(0, idx - 180) : idx + 360]
        return re.sub(r"\s+", " ", snapshot).strip()[:1200]

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

    @staticmethod
    def _to_int(value: Any) -> int | None:
        """安全转换为正整数。"""
        try:
            if value in (None, ""):
                return None
            number = int(float(value))
            return number if number > 0 else None
        except (TypeError, ValueError):
            return None
