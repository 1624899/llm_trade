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
        self.paper_trades_retention_days = int(
            data_cfg.get("paper_trades_retention_days", self.paper_trades_retention_days)
        )
        self.yahoo_batch_size = int(data_cfg.get("yahoo_batch_size", self.yahoo_batch_size))
        self.yahoo_max_workers = max(1, min(int(data_cfg.get("yahoo_max_workers", self.yahoo_max_workers)), 8))
        self.yahoo_batch_pause = max(0.0, float(data_cfg.get("yahoo_batch_pause", self.yahoo_batch_pause)))
        self.efinance_max_codes = max(0, int(data_cfg.get("efinance_max_codes", self.efinance_max_codes)))
        self.efinance_max_workers = max(1, min(int(data_cfg.get("efinance_max_workers", self.efinance_max_workers)), 8))
        self.efinance_timeout = max(1.0, float(data_cfg.get("efinance_timeout", self.efinance_timeout)))
        self.efinance_request_pause = max(
            0.0,
            float(data_cfg.get("efinance_request_pause", self.efinance_request_pause)),
        )
        self.enable_efinance_validation = bool(
            data_cfg.get("enable_efinance_validation", self.enable_efinance_validation)
        )
        self.enable_efinance_fallback = bool(
            data_cfg.get("enable_efinance_fallback", self.enable_efinance_fallback)
        )
        self.tushare_token = self._resolve_env_value(data_cfg.get("tushare_token", self.tushare_token))
        self.tushare_anomaly_pct = max(
            0.0,
            float(data_cfg.get("tushare_anomaly_pct", self.tushare_anomaly_pct)),
        )
        self.tushare_request_interval = max(
            0.0,
            float(data_cfg.get("tushare_request_interval", self.tushare_request_interval)),
        )
        self.tushare_max_retries = max(
            1,
            int(data_cfg.get("tushare_max_retries", self.tushare_max_retries)),
        )
        self.tushare_history_max_workers = max(
            1,
            min(int(data_cfg.get("tushare_history_max_workers", self.tushare_history_max_workers)), 8),
        )
        self.tushare_fetch_adj_factor = bool(
            data_cfg.get("tushare_fetch_adj_factor", self.tushare_fetch_adj_factor)
        )
        self.enable_akshare_daily_fallback = bool(
            data_cfg.get("enable_akshare_daily_fallback", self.enable_akshare_daily_fallback)
        )
        configured_history_start = data_cfg.get("tushare_history_start_date", self.tushare_history_start_date)
        if configured_history_start in (None, "", "auto_10y"):
            self.tushare_history_start_date = self._date_days_ago(
                int(data_cfg.get("market_bars_daily_retention_days", self.market_bars_daily_retention_days))
            )
        else:
            self.tushare_history_start_date = str(configured_history_start).replace("-", "")[:8]
        self.daily_update_after_time = str(
            data_cfg.get("daily_update_after_time", self.daily_update_after_time)
        )
        self.efinance_sample_size = max(0, int(data_cfg.get("efinance_sample_size", self.efinance_sample_size)))

    @staticmethod
    def _resolve_env_value(value):
        if isinstance(value, str) and value.startswith("env:"):
            env_name = value.split(":", 1)[1]
            if env_name == "TUSHARE_TOKEN":
                return os.getenv("TUSHARE_TOKEN") or os.getenv("TUSHARE_API_KEY", "")
            return os.getenv(env_name, "")
        return value
