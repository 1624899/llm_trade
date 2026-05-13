"""
离线数据流水线 (Data Pipeline)
负责从各层明确主源下载股票历史 K 线数据和实时报价，
并将其清洗、转换后存储至本地 SQLite 数据湖中。
"""

import akshare as ak
import glob
import os
import pandas as pd
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
from loguru import logger

from src.data_pipeline_config import PipelineConfigMixin
from src.database import StockDatabase
from src.data_pipeline_derivation import PeriodBarDerivationMixin
from src.data_pipeline_index import IndexBarsMixin
from src.data_pipeline_quotes import DailyQuotesMixin
from src.financial_data import FinancialDataProvider
from src.market_extras import (
    fetch_akshare_stock_news,
    fetch_cls_global_news,
    fetch_cninfo_announcements,
    fetch_eastmoney_global_news,
    fetch_eastmoney_research_reports,
    fetch_ths_eps_forecast,
)
from src.mootdx_sources import fetch_mootdx_bars
from src.signal_sources import (
    fetch_baidu_stock_fund_flow_history,
    fetch_baidu_stock_profile,
    fetch_eastmoney_lhb,
    fetch_eastmoney_concept_constituents,
    fetch_eastmoney_restricted_release,
    fetch_ths_concept_boards,
    fetch_ths_board_fund_flow,
    fetch_ths_stock_fund_flow,
)


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


class DataPipeline(PipelineConfigMixin, DailyQuotesMixin, PeriodBarDerivationMixin, IndexBarsMixin):
    def __init__(self):
        """
        初始化数据流水线，配置数据库连接、各API参数及清理设置。
        """
        self.db = StockDatabase()
        # mootdx 行情配置
        self.mootdx_bar_offset = 900
        self.mootdx_history_offset = 5000
        self.mootdx_timeout = 15
        self.market_bars_history_start_date = self._date_days_ago(3650)
        # 每日更新时间
        self.daily_update_after_time = "15:30"
        # 数据清理与保留设置
        self.enable_cleanup = True
        self.enable_database_cleanup = False
        self.enable_database_vacuum = False
        self.derive_period_bars_on_sync = False
        self.backfill_history_on_sync = False
        self.enable_daily_bars_incremental_fill = True
        self.market_data_retention_days = 30
        self.macro_events_retention_days = 30
        self.output_retention_days = 30
        self.trade_execution_retention_days = 365
        self.daily_quotes_retention_days = 45
        self.market_bars_daily_retention_days = 3650
        self.market_bars_weekly_retention_days = 1825
        self.market_bars_monthly_retention_days = 3650
        self.daily_lhb_retention_days = 180
        self.market_signal_lookback_days = 7
        self.restricted_release_forward_days = 60
        self.concept_membership_top_n = 12
        self.baidu_profile_max_codes = 300
        self.baidu_fund_flow_max_codes = 120
        self.baidu_fund_flow_days = 20
        self.info_source_max_codes = 80
        self.research_report_pages = 2
        self.stock_news_limit = 20
        self.global_news_limit = 50
        self.announcement_lookback_days = 365
        self.announcement_limit = 30
        self.paper_trades_retention_days = 365
        # 财务数据提供者
        self.financial_data_provider = FinancialDataProvider()
        self._load_pipeline_settings()
        logger.info("数据流水线初始化完成")

    @staticmethod
    def _date_days_ago(days: int) -> str:
        return (datetime.now() - timedelta(days=max(1, int(days)))).strftime("%Y%m%d")

    def _safe_fetch(self, func, *args, retries=3, delay=5, **kwargs):
        """
        安全地调用数据抓取函数，支持重试机制。
        """
        for i in range(retries):
            try:
                res = func(*args, **kwargs)
                if res is not None and not res.empty:
                    return res
                logger.warning(f"获取数据为空，准备重试({i+1}/{retries})")
            except Exception as e:
                logger.error(f"调用{func.__name__} 失败 {e}")
            if i < retries - 1:
                time.sleep(delay * (i + 1))
        return None

    def sync_market_bars(self, codes=None, periods=("daily", "weekly", "monthly")) -> bool:
        """
        同步市场 K 线数据。
        
        策略：
        1. 增量更新：仅拉取缺失的交易日数据。
        2. 多周期支持：日线、周线、月线。
        3. 周/月线由清洗后的日线聚合而成。
        """
        # 当前策略：mootdx 拉取日线，周/月线由清洗后的日线聚合生成。
        codes = codes or self._load_stock_codes_from_lake()
        if not codes:
            logger.error("没有发现K 线的股票代码，请先同步 stock_basic")
            return False

        all_success = True
        refreshed_daily_codes = set()
        for period in periods:
            period = self._normalize_period(period)
            pending_codes = self._filter_codes_needing_bars(codes, period)
            if not pending_codes:
                logger.info(f"{period} K 线已是最新，无需重新拉取")
                continue

            skipped_count = len(codes) - len(pending_codes)
            logger.info(f"Start syncing {period} bars: pending={len(pending_codes)}, skipped={skipped_count}")

            if period == "daily":
                ok = self.sync_daily_bars_incremental(pending_codes)
                if ok:
                    refreshed_daily_codes.update(str(code).zfill(6) for code in pending_codes)
                all_success = all_success and ok
                continue

            missing_daily_refresh_codes = [
                code for code in pending_codes
                if str(code).zfill(6) not in refreshed_daily_codes
            ]
            if missing_daily_refresh_codes:
                daily_ok = self.sync_daily_bars_incremental(missing_daily_refresh_codes)
                if daily_ok:
                    refreshed_daily_codes.update(str(code).zfill(6) for code in missing_daily_refresh_codes)
            else:
                daily_ok = True
                logger.info(f"{period} K line reuses daily refresh from current sync")
            if not daily_ok:
                logger.warning(f"{period} K line will use existing local daily bars because daily refresh failed")
            start_date = self._current_period_start_date(period)
            logger.info(f"{period} K line incremental derivation window: start_date={start_date}")
            bars_df = self._build_period_bars_from_daily(pending_codes, period, start_date=start_date)

            if bars_df is None or bars_df.empty:
                logger.error(f"{period} K line sync failed")
                all_success = False
                continue

            final_codes = set(bars_df["code"].astype(str).str.zfill(6))
            missing_codes = [code for code in pending_codes if str(code).zfill(6) not in final_codes]
            if missing_codes:
                logger.warning(f"{period} K line still missing {len(missing_codes)} codes")

            bars_df = self._clean_market_bars(bars_df, period)
            ok = self._upsert_derived_period_bars(bars_df, period)
            all_success = all_success and ok
            continue

        return all_success

    def sync_daily_bars_by_trade_date(self, codes=None, trade_date: str = None) -> bool:
        """
        按交易日拉取并存储日线数据。
        主数据源：mootdx。旧的 Yahoo/efinance/AKShare 兜底链路已移除，避免同一表内混入语义不同的 K 线。
        """
        codes = [str(code).zfill(6) for code in (codes or self._load_stock_codes_from_lake())]
        if not codes:
            return True

        trade_date = str(trade_date or self._expected_latest_trade_date("daily")).replace("-", "")[:8]
        bars_df = self._fetch_mootdx_bars(
            codes,
            "daily",
            start_date=trade_date,
            end_date=trade_date,
            offset=getattr(self, "mootdx_bar_offset", 900),
        )

        if bars_df is None or bars_df.empty:
            logger.error("每日 K 线同步失败：mootdx 未返回可用数据")
            return False

        final_codes = set(bars_df["code"].astype(str).str.zfill(6))
        missing_codes = [code for code in codes if code not in final_codes]
        if missing_codes:
            logger.warning(f"mootdx daily K line missing {len(missing_codes)} codes")

        clean_df = self._clean_market_bars(bars_df, "daily")
        if clean_df.empty:
            logger.error("daily K line sync produced no rows after OHLC validation")
            return False

        return self.db.upsert_dataframe(
            "market_bars",
            clean_df,
            key_columns=["code", "period", "trade_date"],
        )

    def sync_daily_bars_incremental(self, codes=None, end_date: str = None) -> bool:
        """
        补齐每只股票从本地最新日线之后到目标交易日之间的缺口。
        """
        codes = [str(code).zfill(6) for code in (codes or self._load_stock_codes_from_lake())]
        if not codes:
            return True

        target_date = str(end_date or self._expected_latest_trade_date("daily")).replace("-", "")[:8]
        if not getattr(self, "enable_daily_bars_incremental_fill", True):
            logger.info(
                "daily K 线增量补齐已关闭，仅更新目标交易日；"
                f"target_date={target_date}, codes={len(codes)}"
            )
            return self.sync_daily_bars_by_trade_date(codes, trade_date=target_date)

        latest_dates = self._latest_bar_dates(codes, "daily")
        parsed_latest = {
            code: self._parse_trade_date(latest_dates.get(code))
            for code in codes
        }
        target = datetime.strptime(target_date, "%Y%m%d").date()

        dated_codes = {}
        stale_dates = [latest for latest in parsed_latest.values() if latest is not None and latest < target]
        trade_dates = []
        if stale_dates:
            start_date = (min(stale_dates) + timedelta(days=1)).strftime("%Y%m%d")
            trade_dates = self._resolve_trade_dates(start_date, target_date)

        for code in codes:
            latest = parsed_latest.get(code)
            if latest is None or latest >= target:
                dated_codes.setdefault(target_date, []).append(code)
                continue
            for trade_date in trade_dates:
                parsed_trade_date = self._parse_trade_date(trade_date)
                if parsed_trade_date and latest < parsed_trade_date <= target:
                    dated_codes.setdefault(trade_date, []).append(code)

        if not dated_codes:
            logger.info("daily K 线没有需要补齐的交易日")
            return True

        total_requests = sum(len(batch_codes) for batch_codes in dated_codes.values())
        logger.info(
            f"daily K 线增量补齐: dates={len(dated_codes)}, code-date pairs={total_requests}, "
            f"end_date={target_date}"
        )
        all_success = True
        for trade_date in sorted(dated_codes):
            all_success = self.sync_daily_bars_by_trade_date(dated_codes[trade_date], trade_date=trade_date) and all_success
        return all_success

    def _sync_daily_bars_by_trade_date(self, codes) -> bool:
        """Backward-compatible wrapper for daily bar refresh."""
        return self.sync_daily_bars_incremental(codes)

    def _sync_daily_bars_for_codes(self, codes) -> bool:
        """Backward-compatible wrapper for daily bar refresh."""
        return self.sync_daily_bars_incremental(codes)

    def sync_market_bars_history(
        self,
        start_date: str = None,
        end_date: str = None,
        codes=None,
        derive_periods=("weekly", "monthly"),
    ) -> bool:
        """
        通过 mootdx 拉取全市场历史日线数据来初始化本地数据库。
        """
        codes = [str(code).zfill(6) for code in (codes or self._load_stock_codes_from_lake())]
        if not codes:
            logger.error("No stock codes available for historical market_bars initialization")
            return False

        start_date = str(start_date or getattr(self, "market_bars_history_start_date", "20000101")).replace("-", "")[:8]
        end_date = str(end_date or self._expected_latest_trade_date("daily")).replace("-", "")[:8]
        logger.info(
            f"Start mootdx historical daily K initialization: codes={len(codes)}, "
            f"start={start_date}, end={end_date}"
        )
        bars_df = self._fetch_mootdx_bars(
            codes,
            "daily",
            start_date=start_date,
            end_date=end_date,
            offset=getattr(self, "mootdx_history_offset", 5000),
        )
        all_success = self._clean_upsert_historical_daily(end_date, bars_df)

        for period in derive_periods or ():
            period = self._normalize_period(period)
            if period == "daily":
                continue
            bars_df = self._build_period_bars_from_daily(codes, period)
            if bars_df is None or bars_df.empty:
                logger.warning(f"Historical {period} bars were not generated")
                all_success = False
                continue
            clean_df = self._clean_market_bars(bars_df, period)
            all_success = self._upsert_derived_period_bars(clean_df, period) and all_success
        return all_success

    def _fetch_mootdx_bars(self, codes, period: str, start_date: str = None, end_date: str = None, offset: int = None):
        """读取 mootdx K 线，集中处理超时和导入失败。"""
        try:
            return fetch_mootdx_bars(
                codes,
                period=period,
                start_date=start_date,
                end_date=end_date,
                offset=offset or getattr(self, "mootdx_bar_offset", 900),
                timeout=getattr(self, "mootdx_timeout", 15),
            )
        except Exception as exc:
            logger.error("mootdx {} K 线主源不可用: {}", period, exc)
            return pd.DataFrame()

    def _clean_upsert_historical_daily(self, trade_date: str, bars_df: pd.DataFrame) -> bool:
        if bars_df is None or bars_df.empty:
            logger.warning(f"Historical daily K line missing for {trade_date}")
            return False

        clean_df = self._clean_market_bars(bars_df, "daily")
        if clean_df.empty:
            logger.warning(f"Historical daily K line empty after cleaning for {trade_date}")
            return False

        return self.db.upsert_dataframe(
            "market_bars",
            clean_df,
            key_columns=["code", "period", "trade_date"],
        )

    def _resolve_trade_dates(self, start_date: str, end_date: str) -> list:
        """
        解析 A 股交易日，优先使用 AKShare 交易日历，最后用工作日兜底。
        """
        start_date = str(start_date).replace("-", "")[:8]
        end_date = str(end_date).replace("-", "")[:8]

        ak_dates = self._resolve_akshare_trade_dates(start_date, end_date)
        if ak_dates:
            return ak_dates

        days = pd.date_range(start=pd.to_datetime(start_date, format="%Y%m%d"), end=pd.to_datetime(end_date, format="%Y%m%d"))
        return [day.strftime("%Y%m%d") for day in days if day.weekday() < 5]

    def _resolve_akshare_trade_dates(self, start_date: str, end_date: str) -> list:
        """Resolve trade dates from AKShare."""
        try:
            cal = ak.tool_trade_date_hist_sina()
        except Exception as exc:
            logger.warning(f"AKShare trade calendar fetch failed, fallback to weekday calendar: {exc}")
            return []

        if cal is None or cal.empty:
            return []

        date_col = None
        for candidate in ("trade_date", "交易日", "calendarDate"):
            if candidate in cal.columns:
                date_col = candidate
                break
        if date_col is None:
            date_col = cal.columns[0]

        dates = pd.to_datetime(cal[date_col], errors="coerce").dt.strftime("%Y%m%d").dropna()
        return sorted(date for date in dates.tolist() if start_date <= date <= end_date)

    def sync_financial_metrics(self, codes=None, periods: int = 8) -> bool:
        """
        同步核心财务指标（来自东方财富）。
        """
        codes = codes or self._load_stock_codes_from_lake()
        if not codes:
            logger.warning("No stock codes available for financial metric sync")
            return False

        all_success = True
        for code in codes:
            try:
                metrics = self.financial_data_provider.fetch_financial_metrics(code, periods=periods)
            except Exception as exc:
                logger.warning(f"Financial metric fetch failed for {code}: {exc}")
                all_success = False
                continue

            if metrics is None or metrics.empty:
                logger.warning(f"Financial metric fetch returned empty data for {code}")
                all_success = False
                continue

            ok = self.db.upsert_dataframe(
                "financial_metrics",
                metrics,
                key_columns=["code", "report_date"],
            )
            all_success = all_success and ok
            time.sleep(0.2)

        return all_success

    def sync_market_signals(
        self,
        start_date: str = None,
        end_date: str = None,
        fund_flow_period: str = "即时",
    ) -> bool:
        """同步信号层数据：资金流、龙虎榜、解禁。"""
        end_date = str(end_date or self._expected_latest_trade_date("daily")).replace("-", "")[:8]
        lookback_days = max(1, int(getattr(self, "market_signal_lookback_days", 7)))
        start_date = str(
            start_date
            or (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")
        ).replace("-", "")[:8]

        ok = True
        stock_flow = fetch_ths_stock_fund_flow(symbol=fund_flow_period)
        ok = self._upsert_signal_frame(
            "stock_fund_flows",
            stock_flow,
            ["code", "trade_date", "period", "source"],
        ) and ok

        for kind in ("industry", "concept"):
            board_flow = fetch_ths_board_fund_flow(kind=kind, symbol=fund_flow_period)
            ok = self._upsert_signal_frame(
                "board_fund_flows",
                board_flow,
                ["board_name", "kind", "trade_date", "period", "source"],
            ) and ok

        lhb = fetch_eastmoney_lhb(start_date=start_date, end_date=end_date)
        ok = self._upsert_signal_frame(
            "lhb_details",
            lhb,
            ["code", "trade_date", "reason", "source"],
        ) and ok

        release_end = (
            datetime.strptime(end_date, "%Y%m%d")
            + timedelta(days=max(1, int(getattr(self, "restricted_release_forward_days", 60))))
        ).strftime("%Y%m%d")
        releases = fetch_eastmoney_restricted_release(start_date=end_date, end_date=release_end)
        ok = self._upsert_signal_frame(
            "restricted_releases",
            releases,
            ["code", "release_date", "holder_type", "source"],
        ) and ok

        ok = self.sync_concept_taxonomy(max_concepts=getattr(self, "concept_membership_top_n", 12)) and ok
        ok = self.sync_baidu_stock_profiles(max_codes=getattr(self, "baidu_profile_max_codes", 300)) and ok
        ok = self.sync_baidu_stock_fund_flows(
            max_codes=getattr(self, "baidu_fund_flow_max_codes", 120),
            days=getattr(self, "baidu_fund_flow_days", 20),
        ) and ok
        return ok

    def sync_information_sources(self, codes=None) -> bool:
        """同步研报、新闻和公告三类信息源。"""
        max_codes = max(1, int(getattr(self, "info_source_max_codes", 80)))
        target_codes = [str(code).zfill(6) for code in (codes or self._select_baidu_profile_codes(max_codes))]
        if not target_codes:
            logger.warning("信息源同步没有待处理股票代码")
            return True

        ok = True
        ok = self.sync_research_reports(codes=target_codes) and ok
        ok = self.sync_news_sources(codes=target_codes) and ok
        ok = self.sync_cninfo_announcements(codes=target_codes) and ok
        return ok

    def sync_research_reports(self, codes=None) -> bool:
        """同步东方财富研报列表、PDF 元数据和同花顺 EPS 一致预期。"""
        codes = [str(code).zfill(6) for code in (codes or self._select_baidu_profile_codes(getattr(self, "info_source_max_codes", 80)))]
        ok = True
        report_frames = []
        eps_frames = []
        for code in codes:
            reports = fetch_eastmoney_research_reports(code, max_pages=getattr(self, "research_report_pages", 2))
            if reports is not None and not reports.empty:
                report_frames.append(reports)
            eps = fetch_ths_eps_forecast(code)
            if eps is not None and not eps.empty:
                eps_frames.append(eps)
            time.sleep(0.15)

        if report_frames:
            ok = self._upsert_signal_frame(
                "research_reports",
                pd.concat(report_frames, ignore_index=True),
                ["code", "info_code", "source"],
            ) and ok
        else:
            logger.warning("research_reports 无可写入研报数据")

        if eps_frames:
            ok = self._upsert_signal_frame(
                "eps_forecasts",
                pd.concat(eps_frames, ignore_index=True),
                ["code", "forecast_year", "source"],
            ) and ok
        else:
            logger.warning("eps_forecasts 无可写入一致预期数据")
        return ok

    def sync_news_sources(self, codes=None) -> bool:
        """同步 akshare 新闻三源：个股新闻、财联社快讯、全球资讯。"""
        codes = [str(code).zfill(6) for code in (codes or self._select_baidu_profile_codes(getattr(self, "info_source_max_codes", 80)))]
        ok = True
        stock_frames = []
        for code in codes:
            df = fetch_akshare_stock_news(code, limit=getattr(self, "stock_news_limit", 20))
            if df is not None and not df.empty:
                stock_frames.append(df)
            time.sleep(0.1)
        if stock_frames:
            ok = self._upsert_signal_frame(
                "stock_news",
                pd.concat(stock_frames, ignore_index=True),
                ["code", "title", "datetime", "source"],
            ) and ok
        else:
            logger.warning("stock_news 无可写入个股新闻")

        market_frames = [
            fetch_cls_global_news(limit=getattr(self, "global_news_limit", 50)),
            fetch_eastmoney_global_news(limit=getattr(self, "global_news_limit", 50)),
        ]
        market_frames = [df for df in market_frames if df is not None and not df.empty]
        if market_frames:
            market_news = pd.concat(market_frames, ignore_index=True)
            market_news = market_news.drop(columns=["code"], errors="ignore")
            ok = self._upsert_signal_frame(
                "market_news",
                market_news,
                ["title", "datetime", "source"],
            ) and ok
        else:
            logger.warning("market_news 无可写入全球资讯")
        return ok

    def sync_cninfo_announcements(self, codes=None) -> bool:
        """同步巨潮公告列表和 PDF 元数据。"""
        codes = [str(code).zfill(6) for code in (codes or self._select_baidu_profile_codes(getattr(self, "info_source_max_codes", 80)))]
        end_date = self._expected_latest_trade_date("daily")
        start_date = (
            datetime.strptime(end_date, "%Y%m%d")
            - timedelta(days=max(1, int(getattr(self, "announcement_lookback_days", 365))))
        ).strftime("%Y%m%d")
        frames = []
        for code in codes:
            df = fetch_cninfo_announcements(
                code,
                limit=getattr(self, "announcement_limit", 30),
                start_date=start_date,
                end_date=end_date,
            )
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(0.1)
        if not frames:
            logger.warning("company_announcements 无可写入巨潮公告")
            return True
        return self._upsert_signal_frame(
            "company_announcements",
            pd.concat(frames, ignore_index=True),
            ["code", "announcement_id", "source"],
        )

    def sync_concept_taxonomy(self, max_concepts: int = 12) -> bool:
        """同步概念目录和热门概念成分股。"""
        boards = fetch_ths_concept_boards()
        ok = self._upsert_signal_frame("concept_boards", boards, ["concept_name", "source"])
        concept_rows = self._select_concepts_for_membership(boards, max_concepts=max_concepts)
        if not concept_rows:
            return ok

        frames = []
        for item in concept_rows:
            concept_name = str(item.get("concept_name") or "").strip()
            if not concept_name:
                continue
            df = fetch_eastmoney_concept_constituents(
                concept_name,
                concept_code=str(item.get("concept_code") or ""),
            )
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(0.1)

        if not frames:
            logger.warning("stock_concepts 无可写入概念成分数据")
            return ok
        members = pd.concat(frames, ignore_index=True)
        return self._upsert_signal_frame(
            "stock_concepts",
            members,
            ["code", "concept_name", "source"],
        ) and ok

    def sync_baidu_stock_profiles(self, codes=None, max_codes: int = 300) -> bool:
        """同步百度股市通行业、概念、地域三维归属。"""
        target_codes = [str(code).zfill(6) for code in (codes or self._select_baidu_profile_codes(max_codes))]
        if not target_codes:
            logger.warning("stock_profiles 无待同步股票代码")
            return True

        frames = []
        for code in target_codes[: max(1, int(max_codes or 1))]:
            df = fetch_baidu_stock_profile(code)
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(0.1)

        if not frames:
            logger.warning("stock_profiles 无可写入百度画像数据")
            return True
        profiles = pd.concat(frames, ignore_index=True)
        return self._upsert_signal_frame("stock_profiles", profiles, ["code", "source"])

    def sync_baidu_stock_fund_flows(self, codes=None, max_codes: int = 120, days: int = 20) -> bool:
        """同步百度股市通日级资金流。"""
        target_codes = [str(code).zfill(6) for code in (codes or self._select_baidu_profile_codes(max_codes))]
        if not target_codes:
            logger.warning("baidu_stock_fund_flows 无待同步股票代码")
            return True

        frames = []
        for code in target_codes[: max(1, int(max_codes or 1))]:
            df = fetch_baidu_stock_fund_flow_history(code, days=days)
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(0.1)

        if not frames:
            logger.warning("baidu_stock_fund_flows 无可写入资金流数据")
            return True
        flows = pd.concat(frames, ignore_index=True)
        return self._upsert_signal_frame("baidu_stock_fund_flows", flows, ["code", "trade_date", "source"])

    def _select_baidu_profile_codes(self, max_codes: int = 300) -> list[str]:
        """优先同步近期成交额活跃股票，避免全市场画像接口过重。"""
        max_codes = max(1, int(max_codes or 1))
        active_df = self.db.query_to_dataframe(
            """
            SELECT dq.code
            FROM daily_quotes dq
            WHERE dq.trade_date = (SELECT MAX(trade_date) FROM daily_quotes)
            ORDER BY COALESCE(dq.amount, 0) DESC, ABS(COALESCE(dq.change_pct, 0)) DESC
            LIMIT ?
            """,
            (max_codes,),
        )
        if active_df is not None and not active_df.empty:
            return active_df["code"].astype(str).str.zfill(6).tolist()
        return self._load_stock_codes_from_lake()[:max_codes]

    def _select_concepts_for_membership(self, boards: pd.DataFrame, max_concepts: int = 12) -> list[dict]:
        """优先选择资金流最强的概念，避免全量成分同步过重。"""
        max_concepts = max(1, int(max_concepts or 1))
        flow_df = self.db.query_to_dataframe(
            """
            SELECT board_name AS concept_name
            FROM board_fund_flows
            WHERE kind = 'concept'
              AND trade_date = (SELECT MAX(trade_date) FROM board_fund_flows WHERE kind = 'concept')
            ORDER BY COALESCE(main_net_inflow, 0) DESC, COALESCE(change_pct, 0) DESC
            LIMIT ?
            """,
            (max_concepts,),
        )

        selected_names = []
        if flow_df is not None and not flow_df.empty:
            selected_names = flow_df["concept_name"].astype(str).dropna().tolist()
        elif boards is not None and not boards.empty:
            selected_names = boards["concept_name"].astype(str).dropna().head(max_concepts).tolist()

        if not selected_names:
            return []

        code_map = {}
        if boards is not None and not boards.empty:
            code_map = dict(zip(boards["concept_name"].astype(str), boards.get("concept_code", "").astype(str)))
        return [{"concept_name": name, "concept_code": code_map.get(name, "")} for name in selected_names[:max_concepts]]

    def _upsert_signal_frame(self, table_name: str, df: pd.DataFrame, key_columns: list[str]) -> bool:
        """统一写入信号层 DataFrame，空数据不让主流程失败。"""
        if df is None or df.empty:
            logger.warning("{} 无可写入信号数据", table_name)
            return True
        return self.db.upsert_dataframe(table_name, df, key_columns=key_columns)

    def _load_stock_codes_from_lake(self):
        """
        从本地 stock_basic 表中加载所有股票代码。
        """
        df = self.db.query_to_dataframe("SELECT code FROM stock_basic ORDER BY code")
        if df is not None and not df.empty:
            return df["code"].astype(str).str.zfill(6).tolist()

        code_df = self._safe_fetch(ak.stock_info_a_code_name, retries=2, delay=2)
        if code_df is None or code_df.empty:
            return []
        if "code" not in code_df.columns and "代码" in code_df.columns:
            code_df = code_df.rename(columns={"代码": "code"})
        if "code" not in code_df.columns:
            return []
        return code_df["code"].astype(str).str.zfill(6).tolist()

    def _normalize_period(self, period: str) -> str:
        aliases = {"1d": "daily", "day": "daily", "1w": "weekly", "week": "weekly", "1mo": "monthly", "month": "monthly"}
        normalized = aliases.get(str(period).lower(), str(period).lower())
        if normalized not in {"daily", "weekly", "monthly"}:
            raise ValueError(f"不支持的 K 线周期 {period}")
        return normalized

    def _filter_codes_needing_bars(self, codes, period: str):
        """
        通过对比数据库中的最新日期，筛选出需要更新 K 线数据的股票代码。
        """
        target_period = "daily" if period in {"weekly", "monthly"} else period
        target_date = self._expected_latest_trade_date(target_period)
        codes = [str(code).zfill(6) for code in codes]

        latest_df = self._latest_bar_status(period)
        if latest_df is None or latest_df.empty:
            logger.info("log message")
            return codes

        latest_df["code"] = latest_df["code"].astype(str).str.zfill(6)
        latest_map = dict(zip(latest_df["code"], latest_df["latest_trade_date"].astype(str)))
        source_map = dict(zip(latest_df["code"], latest_df["source"].fillna("").astype(str)))
        pending_codes = [
            code
            for code in codes
            if self._bar_is_stale(latest_map.get(code), target_date)
            or not self._bar_source_is_trusted(period, source_map.get(code))
        ]
        logger.info(f"{period} bar freshness check: target={target_date}, existing={len(latest_map)}, pending={len(pending_codes)}")
        return pending_codes

    def _latest_bar_status(self, period: str) -> pd.DataFrame:
        if period == "daily":
            sql = (
                "SELECT mb.code, mb.trade_date AS latest_trade_date, mb.source "
                "FROM market_bars mb "
                "JOIN ("
                "    SELECT code, MAX(trade_date) AS latest_trade_date "
                "    FROM market_bars "
                "    WHERE period = ? "
                "    GROUP BY code"
                ") latest "
                "ON latest.code = mb.code AND latest.latest_trade_date = mb.trade_date "
                "WHERE mb.period = ?"
            )
            return self.db.query_to_dataframe(sql, (period, period))

        table_name = self._period_bar_table(period)
        sql = (
            "SELECT mb.code, mb.trade_date AS latest_trade_date, mb.source "
            f"FROM {table_name} mb "
            "JOIN ("
            "    SELECT code, MAX(trade_date) AS latest_trade_date "
            f"    FROM {table_name} "
            "    GROUP BY code"
            ") latest "
            "ON latest.code = mb.code AND latest.latest_trade_date = mb.trade_date"
        )
        return self.db.query_to_dataframe(sql)

    def _current_period_start_date(self, period: str) -> str:
        latest_daily_str = self._expected_latest_trade_date("daily")
        latest_daily = datetime.strptime(latest_daily_str, "%Y%m%d").date()
        if period == "weekly":
            start = latest_daily - timedelta(days=latest_daily.weekday())
        elif period == "monthly":
            start = latest_daily.replace(day=1)
        else:
            raise ValueError(f"Cannot derive incremental window for {period}")

        start_date = start.strftime("%Y%m%d")
        trade_dates = self._resolve_trade_dates(start_date, latest_daily_str)
        if trade_dates:
            return trade_dates[0]
        return start_date

    @staticmethod
    def _bar_source_is_trusted(period: str, source: str) -> bool:
        source = str(source or "")
        if period == "daily":
            return source.startswith(("mootdx", "akshare_index"))
        if period in {"weekly", "monthly"}:
            return source == "derived_daily_qfq" or source.startswith("akshare_index")
        return False

    def _latest_bar_dates(self, codes, period: str) -> dict:
        """
        获取每个代码在本地数据库中的最新 K 线日期，用于规划抓取窗口。
        """
        codes = [str(code).zfill(6) for code in codes]
        if not codes:
            return {}

        sql = (
            "SELECT mb.code, mb.trade_date AS latest_trade_date, mb.source "
            "FROM market_bars mb "
            "JOIN ("
            "    SELECT code, MAX(trade_date) AS latest_trade_date "
            "    FROM market_bars "
            "    WHERE period = ? "
            "    GROUP BY code"
            ") latest "
            "ON latest.code = mb.code AND latest.latest_trade_date = mb.trade_date "
            "WHERE mb.period = ?"
        )
        latest_df = self.db.query_to_dataframe(sql, (period, period))
        if latest_df is None or latest_df.empty:
            return {}
        latest_map = dict(zip(latest_df["code"].astype(str).str.zfill(6), latest_df["latest_trade_date"].astype(str)))
        return {code: latest_map.get(code) for code in codes if code in latest_map}

    def _plan_bar_fetch_groups(self, pending_codes, period: str, latest_dates: dict):
        """
        将待更新的代码分为“短周期增量抓取”和“长周期历史补全”两组。
        """
        pending_codes = [str(code).zfill(6) for code in pending_codes]
        if not pending_codes:
            return []

        long_lookback = {"daily": "2y", "weekly": "5y", "monthly": "10y"}[period]
        short_lookback = {"daily": "10d", "weekly": "3mo", "monthly": "6mo"}[period]
        max_incremental_gap_days = {"daily": 45, "weekly": 120, "monthly": 370}[period]

        target_date = datetime.strptime(self._expected_latest_trade_date(period), "%Y%m%d").date()
        short_codes = []
        long_codes = []
        for code in pending_codes:
            latest = self._parse_trade_date(latest_dates.get(code))
            if latest is None or (target_date - latest).days > max_incremental_gap_days:
                long_codes.append(code)
            else:
                short_codes.append(code)

        groups = []
        if short_codes:
            groups.append((short_codes, short_lookback))
        if long_codes:
            groups.append((long_codes, long_lookback))

        logger.info(
            f"{period} K 线拉取窗口: 增量 ({len(short_codes)} 只, {short_lookback}), "
            f"历史补全 ({len(long_codes)} 只, {long_lookback})"
        )
        return groups

    @staticmethod
    def _parse_trade_date(value):
        if not value or str(value) == "nan":
            return None
        try:
            return datetime.strptime(str(value).replace("-", "")[:8], "%Y%m%d").date()
        except ValueError:
            return None

    def _expected_latest_trade_date(self, period: str) -> str:
        """
        计算本地数据库中应该具有的最新的交易日期（考虑休市和盘后延迟）。
        """
        today = datetime.now().date()
        if period == "daily":
            expected = today
            if expected.weekday() < 5 and not self._is_after_daily_market_close():
                expected -= timedelta(days=1)
            return self._latest_open_trade_date(expected.strftime("%Y%m%d"))

        if period == "weekly":
            return (today - timedelta(days=7)).strftime("%Y%m%d")

        first_day_this_month = today.replace(day=1)
        first_day_prev_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
        return first_day_prev_month.strftime("%Y%m%d")

    def _latest_open_trade_date(self, candidate_date: str) -> str:
        """
        将候选日期回退到最近一个 A 股交易日。
        """
        candidate_date = str(candidate_date).replace("-", "")[:8]
        candidate = datetime.strptime(candidate_date, "%Y%m%d").date()
        start_date = (candidate - timedelta(days=45)).strftime("%Y%m%d")
        trade_dates = self._existing_trade_dates_between(start_date, candidate_date)
        if not trade_dates:
            trade_dates = self._resolve_trade_dates(start_date, candidate_date)
        if trade_dates:
            return trade_dates[-1]

        expected = candidate
        while expected.weekday() >= 5:
            expected -= timedelta(days=1)
        return expected.strftime("%Y%m%d")

    def _existing_trade_dates_between(self, start_date: str, end_date: str) -> list:
        db = getattr(self, "db", None)
        if db is None:
            return []
        try:
            df = db.query_to_dataframe(
                """
                SELECT DISTINCT trade_date
                FROM market_bars
                WHERE period = 'daily'
                  AND trade_date >= ?
                  AND trade_date <= ?
                ORDER BY trade_date
                """,
                (start_date, end_date),
            )
        except Exception:
            return []
        if df is None or df.empty or "trade_date" not in df.columns:
            return []
        return sorted(df["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8].tolist())

    def _is_after_daily_market_close(self) -> bool:
        """
        判断当前是否已经过了设定的盘后同步起始时间。
        """
        raw_value = str(getattr(self, "daily_update_after_time", "15:30") or "15:30")
        try:
            hour, minute = [int(part) for part in raw_value.split(":", 1)]
        except Exception:
            hour, minute = 15, 30
        now = datetime.now()
        return now.hour > hour or (now.hour == hour and now.minute >= minute)

    def _bar_is_stale(self, latest_trade_date, target_date: str) -> bool:
        if not latest_trade_date or latest_trade_date == "nan":
            return True
        latest = str(latest_trade_date).replace("-", "")
        return latest < target_date

    def cleanup_old_data(self) -> bool:
        """
        清理过期的数据库记录和本地数据文件，保持存储空间精简。
        """
        logger.info("正在清理旧数据与临时缓存...")
        ok = True
        if self.enable_database_cleanup:
            ok = self._deduplicate_database_tables(vacuum=self.enable_database_vacuum) and ok
            ok = self._prune_database_history(vacuum=self.enable_database_vacuum) and ok
        else:
            logger.info("跳过数据库深度清理，仅清理临时文件")
        ok = self._cleanup_files("data/market_data/*", self.market_data_retention_days) and ok
        ok = self._cleanup_files("data/Macro events/**/*.csv", self.macro_events_retention_days, recursive=True) and ok
        ok = self._cleanup_files("outputs/trading_prompt_*.md", self.output_retention_days) and ok
        ok = self._cleanup_files("data/trade_executions/execution_*.json", self.trade_execution_retention_days) and ok
        return ok

    def _deduplicate_database_tables(self, vacuum: bool = False) -> bool:
        statements = [
            """
            DELETE FROM stock_basic
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM stock_basic GROUP BY code
            )
            """,
            """
            DELETE FROM daily_quotes
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM daily_quotes GROUP BY code, trade_date
            )
            """,
            """
            DELETE FROM market_bars
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM market_bars GROUP BY code, period, trade_date
            )
            """,
            """
            DELETE FROM market_bars_weekly
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM market_bars_weekly GROUP BY code, trade_date
            )
            """,
            """
            DELETE FROM market_bars_monthly
            WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM market_bars_monthly GROUP BY code, trade_date
            )
            """,
        ]
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                for sql in statements:
                    cursor.execute(sql)
                conn.commit()
                if vacuum:
                    conn.execute("VACUUM")
            logger.info("database deduplicate completed")
            return True
        except Exception as e:
            logger.warning(f"数据库理失败 {e}")
            return False

    def _prune_database_history(self, vacuum: bool = False) -> bool:
        """
        根据配置的保留天数，物理删除数据库中的旧历史数据。
        """
        try:
            retention_specs = [
                ("daily_quotes", "trade_date", None, self.daily_quotes_retention_days),
                ("market_bars", "trade_date", "daily", self._market_bars_retention_days("daily")),
                ("market_bars_weekly", "trade_date", None, self._market_bars_retention_days("weekly")),
                ("market_bars_monthly", "trade_date", None, self._market_bars_retention_days("monthly")),
                ("daily_lhb", "trade_date", None, self.daily_lhb_retention_days),
                ("stock_fund_flows", "trade_date", None, self.daily_lhb_retention_days),
                ("board_fund_flows", "trade_date", None, self.daily_lhb_retention_days),
                ("lhb_details", "trade_date", None, self.daily_lhb_retention_days),
                ("restricted_releases", "release_date", None, self.daily_lhb_retention_days),
            ]

            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                total_deleted = 0

                for table_name, date_column, period, retention_days in retention_specs:
                    if retention_days < 0:
                        continue

                    cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y%m%d")
                    if period is None:
                        cursor.execute(
                            f"DELETE FROM {table_name} WHERE REPLACE({date_column}, '-', '') < ?",
                            (cutoff,),
                        )
                    else:
                        cursor.execute(
                            "DELETE FROM market_bars "
                            "WHERE period = ? AND REPLACE(trade_date, '-', '') < ?",
                            (period, cutoff),
                        )
                    total_deleted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

                if self.paper_trades_retention_days >= 0:
                    cutoff = (datetime.now() - timedelta(days=self.paper_trades_retention_days)).strftime("%Y-%m-%d")
                    cursor.execute(
                        "DELETE FROM paper_trades "
                        "WHERE COALESCE(status, '') != 'HOLD' "
                        "AND COALESCE(recommend_date, '') < ?",
                        (cutoff,),
                    )
                    total_deleted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

                conn.commit()
                if total_deleted and vacuum:
                    conn.execute("VACUUM")
                logger.info(f"database prune completed, deleted={total_deleted}")
            return True
        except Exception as e:
            logger.warning(f"数据库历史清理失败 {e}")
            return False

    def _market_bars_retention_days(self, period: str) -> int:
        retention_map = {
            "daily": self.market_bars_daily_retention_days,
            "weekly": self.market_bars_weekly_retention_days,
            "monthly": self.market_bars_monthly_retention_days,
        }
        return retention_map.get(period, self.market_bars_daily_retention_days)

    def _market_bars_is_empty(self) -> bool:
        df = self.db.query_to_dataframe("SELECT COUNT(*) AS row_count FROM market_bars")
        if df is None or df.empty:
            return True
        row_count = df.iloc[0].get("row_count")
        return not row_count or int(row_count) == 0

    def _market_bars_needs_history_init(self) -> bool:
        target_start = str(getattr(self, "market_bars_history_start_date", "") or "").replace("-", "")[:8]
        if not target_start:
            return self._market_bars_is_empty()

        df = self.db.query_to_dataframe(
            """
            SELECT COUNT(*) AS row_count, MIN(trade_date) AS min_trade_date
            FROM market_bars
            WHERE period = 'daily'
              AND LENGTH(code) = 6
              AND COALESCE(source, '') != 'akshare_index'
            """
        )
        if df is None or df.empty:
            return True

        row_count = int(df.iloc[0].get("row_count") or 0)
        min_trade_date = str(df.iloc[0].get("min_trade_date") or "").replace("-", "")[:8]
        complete_dates = len(self._existing_daily_trade_dates())
        if row_count == 0 or complete_dates < 1000:
            return True
        if not min_trade_date:
            return True
        try:
            first_local = datetime.strptime(min_trade_date, "%Y%m%d")
            target = datetime.strptime(target_start, "%Y%m%d")
            return (first_local - target).days > 7
        except ValueError:
            return min_trade_date > target_start

    def _existing_daily_trade_dates(self) -> set:
        df = self.db.query_to_dataframe(
            """
            SELECT trade_date
            FROM market_bars
            WHERE period = 'daily'
              AND LENGTH(code) = 6
              AND COALESCE(source, '') != 'akshare_index'
            GROUP BY trade_date
            HAVING COUNT(*) >= 1000
            """
        )
        if df is None or df.empty:
            return set()
        return set(df["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8])

    def _cleanup_files(self, pattern: str, retention_days: int, recursive: bool = False) -> bool:
        if retention_days < 0:
            return True

        cutoff = time.time() - retention_days * 86400
        deleted = 0
        for file_path in glob.glob(pattern, recursive=recursive):
            if not os.path.isfile(file_path):
                continue
            if os.path.basename(file_path) in {"latest_report.md", "execution_history.json"}:
                continue
            try:
                if os.path.getmtime(file_path) < cutoff:
                    os.remove(file_path)
                    deleted += 1
            except Exception as e:
                logger.warning(f"删除旧文件失败{file_path}: {e}")
                return False

        if deleted:
            logger.info("log message")
        return True

    def _is_supported_bar_code(self, code: str) -> bool:
        """判断代码是否适合进入 mootdx A 股 K 线同步。"""
        code = str(code).zfill(6)
        return code.startswith(("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689"))

    def _clean_market_bars(self, df: pd.DataFrame, period: str) -> pd.DataFrame:
        """
        清洗 K 线数据，包括 OHLC 一致性校验、数值修正和重复剔除。
        """
        clean_df = df.copy()
        clean_df["code"] = clean_df["code"].astype(str).str.zfill(6)
        clean_df["period"] = period
        clean_df["trade_date"] = clean_df["trade_date"].astype(str).str.replace("-", "", regex=False)

        numeric_cols = ["open", "high", "low", "close", "adj_close", "volume", "amount"]
        for col in numeric_cols:
            clean_df[col] = pd.to_numeric(clean_df.get(col), errors="coerce")
        clean_df["adj_close"] = clean_df["adj_close"].where(clean_df["adj_close"].notna(), clean_df["close"])

        clean_df = clean_df.dropna(subset=["code", "trade_date", "close"])
        before_ohlc_check = len(clean_df)
        ohlc_cols = ["open", "high", "low", "close"]
        clean_df = clean_df.dropna(subset=ohlc_cols)
        valid_ohlc = (
            (clean_df[ohlc_cols] > 0).all(axis=1)
            & (clean_df["high"] >= clean_df["low"])
            & (clean_df["open"] <= clean_df["high"])
            & (clean_df["open"] >= clean_df["low"])
            & (clean_df["close"] <= clean_df["high"])
            & (clean_df["close"] >= clean_df["low"])
        )
        if "volume" in clean_df.columns:
            valid_ohlc = valid_ohlc & (clean_df["volume"].isna() | (clean_df["volume"] >= 0))
        if "amount" in clean_df.columns:
            valid_ohlc = valid_ohlc & (clean_df["amount"].isna() | (clean_df["amount"] >= 0))
        invalid_count = before_ohlc_check - int(valid_ohlc.sum())
        if invalid_count:
            logger.warning(
                "Dropped {} {} market_bars rows with inconsistent OHLC values",
                invalid_count,
                period,
            )
        clean_df = clean_df[valid_ohlc]
        retention_days = self._market_bars_retention_days(period)
        if retention_days >= 0:
            cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y%m%d")
            clean_df = clean_df[clean_df["trade_date"] >= cutoff]
        return clean_df[
            [
                "code", "period", "trade_date", "open", "high", "low", "close",
                "adj_close", "volume", "amount", "source", "fetched_at"
            ]
        ]

    def sync_stock_basic(self):
        """
        同步全市场股票的基础列表（代码、名称、行业等）。
        """
        if self._stock_basic_is_fresh():
            logger.info("log message")
            return True

        logger.info("同步股票基本信息...")
        
        # 获取基本代码名字
        code_df = self._safe_fetch(ak.stock_info_a_code_name)
        if code_df is None or code_df.empty:
            return False

        try:
            code_df = code_df.rename(
                columns={
                    "\u4ee3\u7801": "code",
                    "\u540d\u79f0": "name",
                    "symbol": "code",
                    "stock_code": "code",
                    "stock_name": "name",
                }
            )
            if "code" not in code_df.columns:
                logger.error("stock_basic sync failed: missing code column, columns={}", list(code_df.columns))
                return False

            clean_df = pd.DataFrame()
            clean_df["code"] = code_df["code"].astype(str).str.zfill(6)
            clean_df["name"] = code_df["name"].astype(str) if "name" in code_df.columns else clean_df["code"]
            clean_df["industry"] = code_df["industry"].astype(str) if "industry" in code_df.columns else None
            clean_df["update_date"] = datetime.now().strftime("%Y-%m-%d")
            clean_df = clean_df.drop_duplicates(subset=["code"], keep="last")

            ok = self.db.upsert_dataframe(
                "stock_basic",
                clean_df,
                key_columns=["code"]
            )
            if ok:
                logger.info("stock_basic sync completed: {} rows", len(clean_df))
            return ok
        except Exception as e:
            logger.error(f"stock_basic sync failed: {e}")
            return False

    def _stock_basic_is_fresh(self) -> bool:
        """
        检查本地股票基础信息表是否已是最新（今日已更新）。
        """
        df = self.db.query_to_dataframe("SELECT MAX(update_date) AS latest_update_date FROM stock_basic")
        if df is None or df.empty:
            return False
        latest = df.iloc[0].get("latest_update_date")
        return str(latest) == datetime.now().strftime("%Y-%m-%d")

    def run_all(self):
        """
        执行完整的流水线流程：清理、同步基础表、行情快照、指数及 K 线。
        """
        logger.info("=== 离线数据管道启动===")
        cleanup_ok = self.cleanup_old_data() if self.enable_cleanup else True
        success_basic = self.sync_stock_basic()
        success_quotes = self.sync_daily_quotes()
        success_signals = self.sync_market_signals()
        success_information = self.sync_information_sources()
        success_index_bars = self.sync_index_bars()
        needs_history_init = self._market_bars_needs_history_init() if success_basic else False
        if success_basic and self.backfill_history_on_sync and needs_history_init:
            logger.info(
                "market_bars history is incomplete; initializing 10-year daily history "
                "from mootdx and deriving weekly/monthly bars"
            )
            success_bars = self.sync_market_bars_history(derive_periods=("weekly", "monthly"))
        else:
            sync_periods = ("daily", "weekly", "monthly") if self.derive_period_bars_on_sync else ("daily",)
            if not self.derive_period_bars_on_sync:
                if self.backfill_history_on_sync and not needs_history_init:
                    logger.info("本地 market_bars 历史已完成初始化；每日同步仅更新日线，跳过每周/每月推导以节省时间")
                else:
                    logger.info("在每日同步中跳过每周/每月的推导计算，以节省时间；如果需要完整历史，请启用 backfill_history_on_sync")
            success_bars = self.sync_market_bars(periods=sync_periods) if success_basic else False
        
        if cleanup_ok and success_basic and success_quotes and success_signals and success_information and success_index_bars and success_bars:
            logger.info("=== 离线数据管道运行完成===")
            return True
        else:
            logger.warning("=== 离线数据管道运行完成，但存在部分失败===")
            return False


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_all()





