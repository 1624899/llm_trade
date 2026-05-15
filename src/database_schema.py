"""数据库表结构定义与迁移逻辑。

从 database.py 中拆分出来的纯数据/结构定义模块：
- TABLE_DEFINITIONS: 所有 CREATE TABLE IF NOT EXISTS 语句
- COLUMN_MIGRATIONS: 旧表需要补齐的新增列
- UNIQUE_INDEXES: 唯一索引定义
- NON_UNIQUE_INDEXES: 普通索引定义
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


TABLE_DEFINITIONS: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS stock_basic (
        code TEXT PRIMARY KEY,
        name TEXT,
        industry TEXT,
        update_date TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_quotes (
        code TEXT,
        trade_date TEXT,
        price REAL,
        change_pct REAL,
        volume INTEGER,
        amount REAL,
        turnover_rate REAL,
        pe_ttm REAL,
        pb REAL,
        total_market_cap REAL,
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_bars (
        code TEXT,
        period TEXT,
        trade_date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER,
        amount REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, period, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_bars_weekly (
        code TEXT,
        trade_date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER,
        amount REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_bars_monthly (
        code TEXT,
        trade_date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER,
        amount REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_lhb (
        code TEXT,
        trade_date TEXT,
        reason TEXT,
        net_buy REAL,
        buy_amount REAL,
        sell_amount REAL,
        PRIMARY KEY (code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_fund_flows (
        code TEXT,
        trade_date TEXT,
        period TEXT,
        name TEXT,
        latest_price REAL,
        change_pct REAL,
        main_net_inflow REAL,
        main_net_inflow_pct REAL,
        super_large_net_inflow REAL,
        large_net_inflow REAL,
        medium_net_inflow REAL,
        small_net_inflow REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, trade_date, period, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS baidu_stock_fund_flows (
        code TEXT,
        trade_date TEXT,
        close REAL,
        change_pct REAL,
        super_net_inflow REAL,
        large_net_inflow REAL,
        medium_net_inflow REAL,
        small_net_inflow REAL,
        main_net_inflow REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, trade_date, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS board_fund_flows (
        board_name TEXT,
        kind TEXT,
        trade_date TEXT,
        period TEXT,
        change_pct REAL,
        main_net_inflow REAL,
        main_net_inflow_pct REAL,
        leader_name TEXT,
        leader_change_pct REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (board_name, kind, trade_date, period, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lhb_details (
        code TEXT,
        trade_date TEXT,
        name TEXT,
        reason TEXT,
        close REAL,
        change_pct REAL,
        net_buy REAL,
        buy_amount REAL,
        sell_amount REAL,
        turnover REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, trade_date, reason, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS restricted_releases (
        code TEXT,
        release_date TEXT,
        name TEXT,
        release_shares REAL,
        release_market_value REAL,
        release_ratio REAL,
        holder_type TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, release_date, holder_type, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS concept_boards (
        concept_name TEXT,
        concept_code TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (concept_name, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_concepts (
        code TEXT,
        concept_name TEXT,
        concept_code TEXT,
        name TEXT,
        latest_price REAL,
        change_pct REAL,
        turnover_rate REAL,
        pe_ttm REAL,
        pb REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, concept_name, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_profiles (
        code TEXT,
        name TEXT,
        industry TEXT,
        region TEXT,
        industry_blocks TEXT,
        region_blocks TEXT,
        concepts TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_reports (
        code TEXT,
        info_code TEXT,
        title TEXT,
        publish_date TEXT,
        org_name TEXT,
        author TEXT,
        rating TEXT,
        rating_change TEXT,
        industry TEXT,
        eps_this_year REAL,
        eps_next_year REAL,
        eps_next_two_year REAL,
        pdf_url TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, info_code, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS eps_forecasts (
        code TEXT,
        forecast_year TEXT,
        institution_count REAL,
        eps_min REAL,
        eps_avg REAL,
        eps_max REAL,
        industry_avg REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, forecast_year, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_news (
        code TEXT,
        datetime TEXT,
        title TEXT,
        content TEXT,
        url TEXT,
        provider TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, title, datetime, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_news (
        datetime TEXT,
        title TEXT,
        content TEXT,
        url TEXT,
        provider TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (title, datetime, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS company_announcements (
        code TEXT,
        announcement_id TEXT,
        title TEXT,
        date TEXT,
        categories TEXT,
        url TEXT,
        pdf_url TEXT,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, announcement_id, source)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS financial_metrics (
        code TEXT,
        report_date TEXT,
        report_name TEXT,
        notice_date TEXT,
        report_type TEXT,
        revenue REAL,
        revenue_yoy REAL,
        parent_netprofit REAL,
        parent_netprofit_yoy REAL,
        deduct_parent_netprofit REAL,
        deduct_parent_netprofit_yoy REAL,
        gross_margin REAL,
        net_margin REAL,
        roe REAL,
        roic REAL,
        debt_to_assets REAL,
        current_ratio REAL,
        quick_ratio REAL,
        cash_ratio REAL,
        operating_cash_flow REAL,
        operating_cash_flow_yoy REAL,
        cash_to_profit REAL,
        total_assets REAL,
        total_liabilities REAL,
        source TEXT,
        fetched_at TEXT,
        PRIMARY KEY (code, report_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS paper_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT,
        name TEXT,
        recommend_date TEXT,
        recommend_price REAL,
        recommend_reason TEXT,
        current_price REAL,
        return_pct REAL,
        status TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS watchlist_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL,
        name TEXT,
        tier TEXT,
        watch_status TEXT DEFAULT 'ACTIVE',
        source TEXT,
        added_at TEXT,
        updated_at TEXT,
        entry_price REAL,
        current_price REAL,
        return_pct REAL,
        elapsed_days INTEGER,
        elapsed_trade_days INTEGER,
        latest_trade_date TEXT,
        return_5d_pct REAL,
        return_10d_pct REAL,
        return_20d_pct REAL,
        return_30d_pct REAL,
        return_60d_pct REAL,
        expected_return_pct REAL,
        expected_holding_days INTEGER,
        max_holding_days INTEGER,
        exit_hint TEXT,
        recommend_reason TEXT,
        fundamental_analysis TEXT,
        technical_analysis TEXT,
        news_risk_analysis TEXT,
        decision_snapshot TEXT,
        macro_context TEXT,
        remove_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading_account (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_name TEXT NOT NULL,
        initial_cash REAL NOT NULL,
        cash REAL NOT NULL,
        total_market_value REAL DEFAULT 0,
        total_equity REAL DEFAULT 0,
        realized_pnl REAL DEFAULT 0,
        unrealized_pnl REAL DEFAULT 0,
        created_at TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER NOT NULL,
        code TEXT NOT NULL,
        name TEXT,
        quantity INTEGER NOT NULL,
        avg_cost REAL NOT NULL,
        current_price REAL DEFAULT 0,
        market_value REAL DEFAULT 0,
        unrealized_pnl REAL DEFAULT 0,
        unrealized_return_pct REAL DEFAULT 0,
        opened_at TEXT,
        last_buy_at TEXT,
        last_sell_at TEXT,
        status TEXT DEFAULT 'OPEN',
        linked_watchlist_id INTEGER,
        buy_reason TEXT,
        risk_note TEXT,
        realized_pnl REAL DEFAULT 0,
        realized_return_pct REAL DEFAULT 0,
        sold_quantity INTEGER DEFAULT 0,
        closed_at TEXT,
        last_reflected_at TEXT,
        reflection_count INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trade_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER NOT NULL,
        code TEXT,
        name TEXT,
        action TEXT NOT NULL,
        quantity INTEGER DEFAULT 0,
        price REAL DEFAULT 0,
        amount REAL DEFAULT 0,
        cash_before REAL DEFAULT 0,
        cash_after REAL DEFAULT 0,
        position_before INTEGER DEFAULT 0,
        position_after INTEGER DEFAULT 0,
        reason TEXT,
        decision_snapshot TEXT,
        linked_watchlist_id INTEGER,
        created_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backtest_signal_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_date TEXT NOT NULL,
        code TEXT NOT NULL,
        name TEXT,
        source TEXT,
        primary_strategy TEXT,
        strategy_tags TEXT,
        strategy_confidence REAL,
        technical_score REAL,
        theme_score REAL,
        key_metrics TEXT,
        market_regime TEXT,
        created_at TEXT,
        UNIQUE(signal_date, code, source)
    )
    """,
]

COLUMN_MIGRATIONS: Dict[str, Dict[str, str]] = {
    "stock_basic": {
        "industry": "TEXT",
        "update_date": "TEXT",
    },
    "watchlist_items": {
        "decision_snapshot": "TEXT",
        "expected_holding_days": "INTEGER",
        "max_holding_days": "INTEGER",
        "exit_hint": "TEXT",
        "elapsed_days": "INTEGER",
        "elapsed_trade_days": "INTEGER",
        "latest_trade_date": "TEXT",
        "return_5d_pct": "REAL",
        "return_10d_pct": "REAL",
        "return_20d_pct": "REAL",
        "return_30d_pct": "REAL",
        "return_60d_pct": "REAL",
    },
    "trading_positions": {
        "realized_pnl": "REAL DEFAULT 0",
        "realized_return_pct": "REAL DEFAULT 0",
        "sold_quantity": "INTEGER DEFAULT 0",
        "closed_at": "TEXT",
        "last_reflected_at": "TEXT",
        "reflection_count": "INTEGER DEFAULT 0",
    },
}

UNIQUE_INDEXES: List[Tuple[str, str, List[str]]] = [
    ("stock_basic", "idx_stock_basic_code", ["code"]),
    ("daily_quotes", "idx_daily_quotes_code_trade_date", ["code", "trade_date"]),
    ("market_bars", "idx_market_bars_code_period_trade_date", ["code", "period", "trade_date"]),
    ("market_bars_weekly", "idx_market_bars_weekly_code_trade_date", ["code", "trade_date"]),
    ("market_bars_monthly", "idx_market_bars_monthly_code_trade_date", ["code", "trade_date"]),
    ("daily_lhb", "idx_daily_lhb_code_trade_date", ["code", "trade_date"]),
    ("stock_fund_flows", "idx_stock_fund_flows_code_date_period_source", ["code", "trade_date", "period", "source"]),
    ("baidu_stock_fund_flows", "idx_baidu_stock_fund_flows_code_date_source", ["code", "trade_date", "source"]),
    ("board_fund_flows", "idx_board_fund_flows_name_kind_date_period_source", ["board_name", "kind", "trade_date", "period", "source"]),
    ("lhb_details", "idx_lhb_details_code_date_reason_source", ["code", "trade_date", "reason", "source"]),
    ("restricted_releases", "idx_restricted_releases_code_date_type_source", ["code", "release_date", "holder_type", "source"]),
    ("concept_boards", "idx_concept_boards_name_source", ["concept_name", "source"]),
    ("stock_concepts", "idx_stock_concepts_code_name_source", ["code", "concept_name", "source"]),
    ("stock_profiles", "idx_stock_profiles_code_source", ["code", "source"]),
    ("research_reports", "idx_research_reports_code_info_source", ["code", "info_code", "source"]),
    ("eps_forecasts", "idx_eps_forecasts_code_year_source", ["code", "forecast_year", "source"]),
    ("stock_news", "idx_stock_news_code_title_datetime_source", ["code", "title", "datetime", "source"]),
    ("market_news", "idx_market_news_title_datetime_source", ["title", "datetime", "source"]),
    ("company_announcements", "idx_company_announcements_code_id_source", ["code", "announcement_id", "source"]),
    ("financial_metrics", "idx_financial_metrics_code_report_date", ["code", "report_date"]),
    ("watchlist_items", "idx_watchlist_items_code", ["code"]),
    ("trading_account", "idx_trading_account_name", ["account_name"]),
]

NON_UNIQUE_INDEXES: List[Tuple[str, str, List[str]]] = [
    ("market_bars", "idx_market_bars_period_trade_date", ["period", "trade_date"]),
    ("market_bars_weekly", "idx_market_bars_weekly_trade_date", ["trade_date"]),
    ("market_bars_monthly", "idx_market_bars_monthly_trade_date", ["trade_date"]),
    ("daily_quotes", "idx_daily_quotes_trade_date", ["trade_date"]),
    ("stock_fund_flows", "idx_stock_fund_flows_trade_date", ["trade_date"]),
    ("baidu_stock_fund_flows", "idx_baidu_stock_fund_flows_trade_date", ["trade_date"]),
    ("board_fund_flows", "idx_board_fund_flows_trade_date", ["trade_date"]),
    ("lhb_details", "idx_lhb_details_trade_date", ["trade_date"]),
    ("restricted_releases", "idx_restricted_releases_release_date", ["release_date"]),
    ("stock_concepts", "idx_stock_concepts_concept_name", ["concept_name"]),
    ("stock_profiles", "idx_stock_profiles_industry_region", ["industry", "region"]),
    ("research_reports", "idx_research_reports_publish_date", ["publish_date"]),
    ("stock_news", "idx_stock_news_datetime", ["datetime"]),
    ("market_news", "idx_market_news_datetime", ["datetime"]),
    ("company_announcements", "idx_company_announcements_date", ["date"]),
    ("financial_metrics", "idx_financial_metrics_report_date", ["report_date"]),
    ("watchlist_items", "idx_watchlist_items_status", ["watch_status", "updated_at"]),
    ("trading_positions", "idx_trading_positions_account_status", ["account_id", "status"]),
    ("trading_positions", "idx_trading_positions_code_status", ["code", "status"]),
    ("trade_orders", "idx_trade_orders_account_created", ["account_id", "created_at"]),
    ("backtest_signal_snapshots", "idx_backtest_signal_date", ["signal_date"]),
    ("backtest_signal_snapshots", "idx_backtest_signal_strategy", ["primary_strategy", "signal_date"]),
]
