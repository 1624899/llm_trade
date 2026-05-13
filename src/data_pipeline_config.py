"""
DataPipeline 配置加载逻辑。
"""

import os

from loguru import logger
import yaml


class PipelineConfigMixin:
    def _load_pipeline_settings(self):
        """从 config/config.yaml 加载数据流水线配置。"""
        config_path = os.path.join("config", "config.yaml")
        if not os.path.exists(config_path):
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"加载数据管道配置失败 {e}")
            return

        data_cfg = config.get("data", {}) or {}

        self.enable_cleanup = bool(data_cfg.get("enable_cleanup", self.enable_cleanup))
        self.enable_database_cleanup = bool(
            data_cfg.get("enable_database_cleanup", self.enable_database_cleanup)
        )
        self.enable_database_vacuum = bool(
            data_cfg.get("enable_database_vacuum", self.enable_database_vacuum)
        )
        self.derive_period_bars_on_sync = bool(
            data_cfg.get("derive_period_bars_on_sync", self.derive_period_bars_on_sync)
        )
        self.backfill_history_on_sync = bool(
            data_cfg.get("backfill_history_on_sync", self.backfill_history_on_sync)
        )
        self.enable_daily_bars_incremental_fill = bool(
            data_cfg.get("enable_daily_bars_incremental_fill", self.enable_daily_bars_incremental_fill)
        )
        self.market_data_retention_days = int(
            data_cfg.get("market_data_retention_days", self.market_data_retention_days)
        )
        self.macro_events_retention_days = int(
            data_cfg.get("macro_events_retention_days", self.macro_events_retention_days)
        )
        self.output_retention_days = int(data_cfg.get("output_retention_days", self.output_retention_days))
        self.trade_execution_retention_days = int(
            data_cfg.get("trade_execution_retention_days", self.trade_execution_retention_days)
        )
        self.daily_quotes_retention_days = int(
            data_cfg.get("daily_quotes_retention_days", self.daily_quotes_retention_days)
        )
        self.market_bars_daily_retention_days = int(
            data_cfg.get("market_bars_daily_retention_days", self.market_bars_daily_retention_days)
        )
        self.market_bars_weekly_retention_days = int(
            data_cfg.get("market_bars_weekly_retention_days", self.market_bars_weekly_retention_days)
        )
        self.market_bars_monthly_retention_days = int(
            data_cfg.get("market_bars_monthly_retention_days", self.market_bars_monthly_retention_days)
        )
        self.daily_lhb_retention_days = int(
            data_cfg.get("daily_lhb_retention_days", self.daily_lhb_retention_days)
        )
        self.market_signal_lookback_days = max(
            1,
            int(data_cfg.get("market_signal_lookback_days", self.market_signal_lookback_days)),
        )
        self.restricted_release_forward_days = max(
            1,
            int(data_cfg.get("restricted_release_forward_days", self.restricted_release_forward_days)),
        )
        self.concept_membership_top_n = max(
            1,
            int(data_cfg.get("concept_membership_top_n", self.concept_membership_top_n)),
        )
        self.baidu_profile_max_codes = max(
            1,
            int(data_cfg.get("baidu_profile_max_codes", self.baidu_profile_max_codes)),
        )
        self.baidu_fund_flow_max_codes = max(
            1,
            int(data_cfg.get("baidu_fund_flow_max_codes", self.baidu_fund_flow_max_codes)),
        )
        self.baidu_fund_flow_days = max(
            1,
            int(data_cfg.get("baidu_fund_flow_days", self.baidu_fund_flow_days)),
        )
        self.info_source_max_codes = max(
            1,
            int(data_cfg.get("info_source_max_codes", self.info_source_max_codes)),
        )
        self.research_report_pages = max(
            1,
            int(data_cfg.get("research_report_pages", self.research_report_pages)),
        )
        self.stock_news_limit = max(
            1,
            int(data_cfg.get("stock_news_limit", self.stock_news_limit)),
        )
        self.global_news_limit = max(
            1,
            int(data_cfg.get("global_news_limit", self.global_news_limit)),
        )
        self.announcement_lookback_days = max(
            1,
            int(data_cfg.get("announcement_lookback_days", self.announcement_lookback_days)),
        )
        self.announcement_limit = max(
            1,
            int(data_cfg.get("announcement_limit", self.announcement_limit)),
        )
        self.paper_trades_retention_days = int(
            data_cfg.get("paper_trades_retention_days", self.paper_trades_retention_days)
        )
        self.mootdx_bar_offset = max(1, int(data_cfg.get("mootdx_bar_offset", self.mootdx_bar_offset)))
        self.mootdx_history_offset = max(1, int(data_cfg.get("mootdx_history_offset", self.mootdx_history_offset)))
        self.mootdx_timeout = max(1, int(data_cfg.get("mootdx_timeout", self.mootdx_timeout)))
        configured_history_start = data_cfg.get("market_bars_history_start_date", self.market_bars_history_start_date)
        if configured_history_start in (None, "", "auto_10y"):
            self.market_bars_history_start_date = self._date_days_ago(
                int(data_cfg.get("market_bars_daily_retention_days", self.market_bars_daily_retention_days))
            )
        else:
            self.market_bars_history_start_date = str(configured_history_start).replace("-", "")[:8]
        self.daily_update_after_time = str(
            data_cfg.get("daily_update_after_time", self.daily_update_after_time)
        )

    @staticmethod
    def _resolve_env_value(value):
        if isinstance(value, str) and value.startswith("env:"):
            env_name = value.split(":", 1)[1]
            if env_name == "TUSHARE_TOKEN":
                return os.getenv("TUSHARE_TOKEN") or os.getenv("TUSHARE_API_KEY", "")
            return os.getenv(env_name, "")
        return value
