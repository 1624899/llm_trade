"""
全市场股票池模块
提供全A股市场数据获取、行业分类、量化筛选等功能
作为AI选股Agent的基础数据层

数据获取策略（三级降级）：
1. stock_zh_a_spot_em  → 东方财富实时全量行情（交易时间最佳）
2. 磁盘缓存            → 上次成功获取的数据快照
3. stock_info_a_code_name + stock_zh_a_hist → 基础列表 + 批量K线补数据
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from loguru import logger
from src.database import StockDatabase
from src.market_extras import (
    fetch_sina_board_constituents,
    fetch_sina_board_list,
    find_board_node,
)


class StockUniverse:
    """全市场股票池管理器"""

    # 磁盘缓存路径
    DISK_CACHE_DIR = "data/stock_cache"
    DISK_CACHE_FILE = "data/stock_cache/all_stocks_snapshot.csv"
    DISK_CACHE_META = "data/stock_cache/cache_meta.json"

    def __init__(self, config: Dict = None):
        """
        初始化股票池

        Args:
            config: 配置字典，包含筛选条件等
        """
        self.config = config or {}
        self.picking_config = self._load_picking_config()

        # 内存缓存
        self._all_stocks_cache: Optional[pd.DataFrame] = None
        self._all_stocks_cache_time: float = 0
        self._industry_cache: Dict[str, pd.DataFrame] = {}
        self._concept_cache: Dict[str, pd.DataFrame] = {}
        self._industry_list_cache: Optional[pd.DataFrame] = None
        self._concept_list_cache: Optional[pd.DataFrame] = None

        # 缓存配置
        cache_cfg = self.picking_config.get('cache', {})
        self._all_stocks_cache_duration = cache_cfg.get('all_stocks_ttl', 300)  # 5分钟
        self._board_cache_duration = cache_cfg.get('board_ttl', 3600)  # 1小时
        self._board_cache_time: Dict[str, float] = {}

        # 磁盘缓存有效期（默认24小时）
        self._disk_cache_ttl = cache_cfg.get('disk_cache_ttl', 86400)

        # 线程锁
        self._lock = threading.Lock()

        # 请求限流
        self._request_interval = cache_cfg.get('request_interval', 0.5)
        self._last_request_time: float = 0

        # 批量补数据的并发数
        self._batch_workers = cache_cfg.get('batch_workers', 5)

        # 确保缓存目录
        os.makedirs(self.DISK_CACHE_DIR, exist_ok=True)

        logger.info("全市场股票池初始化完成")

    # ========== 配置加载 ==========

    def _load_picking_config(self) -> Dict:
        """加载选股配置"""
        config_path = "config/stock_picking.yaml"
        try:
            if os.path.exists(config_path):
                import yaml
                with open(config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f) or {}
            return {}
        except Exception as e:
            logger.warning(f"加载选股配置失败，使用默认配置: {e}")
            return {}

    def _get_filter_config(self) -> Dict:
        """获取筛选条件配置"""
        return self.picking_config.get('screening', {}).get('filters', {})

    def _get_universe_config(self) -> Dict:
        """获取股票池配置"""
        return self.picking_config.get('universe', {})

    # ========== 请求限流 ==========

    def _rate_limit(self) -> None:
        """对外部数据接口做最小限流，避免短时间连续请求。"""
        with self._lock:
            now = time.time()
            wait_time = self._request_interval - (now - self._last_request_time)
            if wait_time > 0:
                time.sleep(wait_time)
                now = time.time()
            self._last_request_time = now

    def get_all_stocks(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        获取全A股行情数据（从本地数据湖 SQLite 中读取）
        由 data_pipeline.py 每天盘后更新维护
        """
        if not force_refresh and self._all_stocks_cache is not None:
            return self._all_stocks_cache.copy()

        try:
            logger.info("[Local Data Lake] 从 SQLite 数据库读取全市场行情...")
            db = StockDatabase()
            query = """
                SELECT q.*, b.name as basic_name, b.industry 
                FROM daily_quotes q 
                LEFT JOIN stock_basic b ON q.code = b.code
            """
            df = db.query_to_dataframe(query)

            if df is not None and not df.empty:
                # 兼容旧代码，如果没有 name 列则由 basic_name 补充
                if 'name' not in df.columns and 'basic_name' in df.columns:
                    df['name'] = df['basic_name']
                
                # 确保代码列是6位字符串
                df['code'] = df['code'].astype(str).str.zfill(6)

                self._update_memory_cache(df)
                logger.info(f"成功从本地数据库加载了 {len(df)} 只股票的数据")
                return df.copy()
            
            logger.warning("[Local Data Lake] SQLite 数据库为空，请先运行 src/data_pipeline.py 同步数据。")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"从本地数据湖加载数据失败: {e}")
            return pd.DataFrame()


    def _update_memory_cache(self, df: pd.DataFrame) -> None:
        """更新内存缓存"""
        with self._lock:
            self._all_stocks_cache = df
            self._all_stocks_cache_time = time.time()

    def _convert_numeric(self, df: pd.DataFrame) -> None:
        """数据类型转换"""
        numeric_cols = [
            'price', 'change_pct', 'change_amount', 'volume', 'amount',
            'amplitude', 'high', 'low', 'open', 'prev_close',
            'volume_ratio', 'turnover_rate', 'pe_ttm', 'pb',
            'total_market_cap', 'float_market_cap',
            'change_pct_60d', 'change_pct_ytd'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名为英文"""
        column_mapping = {
            '序号': 'index',
            '代码': 'code',
            '名称': 'name',
            '最新价': 'price',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '最高': 'high',
            '最低': 'low',
            '今开': 'open',
            '昨收': 'prev_close',
            '量比': 'volume_ratio',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe_ttm',
            '市净率': 'pb',
            '总市值': 'total_market_cap',
            '流通市值': 'float_market_cap',
            '涨速': 'speed',
            '5分钟涨跌': 'change_5min',
            '60日涨跌幅': 'change_pct_60d',
            '年初至今涨跌幅': 'change_pct_ytd',
        }
        df = df.rename(columns=column_mapping)
        return df

    def get_stock_codes(self) -> List[str]:
        """
        获取全A股代码列表（轻量级，仅代码和名称）
        此方法不依赖东方财富实时接口，稳定性最高
        """
        try:
            self._rate_limit()
            df = ak.stock_info_a_code_name()
            if df is not None and not df.empty:
                codes = df['code'].astype(str).str.zfill(6).tolist()
                logger.info(f"获取到 {len(codes)} 只股票代码")
                return codes
            return []
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []

    def warmup_cache(self) -> bool:
        """
        预热缓存：强制刷新全市场数据并保存到磁盘
        建议在交易时间（9:30-15:00）手动运行一次

        Returns:
            是否预热成功
        """
        logger.info("开始预热股票池缓存...")
        df = self.get_all_stocks(force_refresh=True)
        if not df.empty:
            logger.info(f"预热成功! 缓存了 {len(df)} 只股票数据")
            return True
        logger.error("预热失败: 无法获取数据")
        return False

    # ========== 行业 & 概念板块 ==========

    def get_industry_list(self, force_refresh: bool = False) -> pd.DataFrame:
        """Fetch free Sina industry board list."""
        if not force_refresh and self._industry_list_cache is not None:
            return self._industry_list_cache.copy()
        try:
            df = fetch_sina_board_list("industry")
            self._industry_list_cache = df
            self._board_cache_time["industry_list"] = time.time()
            return df.copy()
        except Exception as e:
            logger.warning(f"Fetch Sina industry board list failed: {e}")
            return pd.DataFrame()

    def get_concept_list(self, force_refresh: bool = False) -> pd.DataFrame:
        """Fetch free Sina concept board list."""
        if not force_refresh and self._concept_list_cache is not None:
            return self._concept_list_cache.copy()
        try:
            df = fetch_sina_board_list("concept")
            self._concept_list_cache = df
            self._board_cache_time["concept_list"] = time.time()
            return df.copy()
        except Exception as e:
            logger.warning(f"Fetch Sina concept board list failed: {e}")
            return pd.DataFrame()

    def get_industry_stocks(self, industry_name: str) -> pd.DataFrame:
        """Fetch free Sina industry board constituents by board name."""
        cache_key = f"industry_{industry_name}"
        if self._is_board_cache_valid(cache_key) and cache_key in self._industry_cache:
            return self._industry_cache[cache_key].copy()
        try:
            boards = self.get_industry_list()
            node = find_board_node(industry_name, boards)
            if not node:
                return pd.DataFrame()
            df = fetch_sina_board_constituents(node)
            self._industry_cache[cache_key] = df
            self._board_cache_time[cache_key] = time.time()
            return df.copy()
        except Exception as e:
            logger.warning(f"Fetch Sina industry constituents failed for {industry_name}: {e}")
            return pd.DataFrame()

    def get_concept_stocks(self, concept_name: str) -> pd.DataFrame:
        """Fetch free Sina concept board constituents by board name."""
        cache_key = f"concept_{concept_name}"
        if self._is_board_cache_valid(cache_key) and cache_key in self._concept_cache:
            return self._concept_cache[cache_key].copy()
        try:
            boards = self.get_concept_list()
            node = find_board_node(concept_name, boards)
            if not node:
                return pd.DataFrame()
            df = fetch_sina_board_constituents(node)
            self._concept_cache[cache_key] = df
            self._board_cache_time[cache_key] = time.time()
            return df.copy()
        except Exception as e:
            logger.warning(f"Fetch Sina concept constituents failed for {concept_name}: {e}")
            return pd.DataFrame()

    def _is_board_cache_valid(self, cache_key: str) -> bool:
        """检查板块缓存是否有效"""
        if cache_key not in self._board_cache_time:
            return False
        return time.time() - self._board_cache_time[cache_key] < self._board_cache_duration

    # ========== 量化筛选器 ==========

    def screen_stocks(self, df: pd.DataFrame = None, filters: Dict = None) -> pd.DataFrame:
        """
        量化条件筛选股票

        Args:
            df: 待筛选的DataFrame，为None则自动获取全市场数据
            filters: 筛选条件字典，为None则使用配置文件中的条件

        Returns:
            筛选后的DataFrame
        """
        if df is None:
            df = self.get_all_stocks()

        if df.empty:
            logger.warning("待筛选数据为空")
            return df

        original_count = len(df)

        # 合并配置文件条件和传入条件
        config_filters = self._get_filter_config()
        if filters:
            config_filters.update(filters)
        filters = config_filters

        universe_cfg = self._get_universe_config()

        # 1. 排除ST股
        if universe_cfg.get('exclude_st', True):
            if 'name' in df.columns:
                mask = ~df['name'].str.contains(r'ST|st|\*ST', na=False)
                df = df[mask]
                logger.info(f"排除ST后: {len(df)} 只 (移除 {original_count - len(df)} 只)")

        # 2. 排除北交所（代码以 4/8 开头的）
        if universe_cfg.get('exclude_bse', True):
            if 'code' in df.columns:
                before = len(df)
                df = df[~df['code'].astype(str).str.startswith(('4', '8'))]
                logger.info(f"排除北交所后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 3. 最小市值筛选（单位：亿）
        min_cap = universe_cfg.get('min_market_cap', 0)
        if min_cap > 0 and 'total_market_cap' in df.columns:
            before = len(df)
            df = df[df['total_market_cap'] >= min_cap * 1e8]  # 转为元
            logger.info(f"市值 >= {min_cap}亿 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 4. 最小日成交量
        min_vol = universe_cfg.get('min_daily_volume', 0)
        if min_vol > 0 and 'volume' in df.columns:
            before = len(df)
            df = df[df['volume'] >= min_vol]
            logger.info(f"成交量 >= {min_vol} 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 5. 最小成交额（单位：万）
        min_amount = universe_cfg.get('min_daily_amount', 0)
        if min_amount > 0 and 'amount' in df.columns:
            before = len(df)
            df = df[df['amount'] >= min_amount * 1e4]
            logger.info(f"成交额 >= {min_amount}万 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 6. 最新价 > 0（排除停牌）
        if universe_cfg.get('exclude_suspended', True) and 'price' in df.columns:
            before = len(df)
            df = df[df['price'] > 0]
            logger.info(f"排除停牌后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 7. PE范围
        pe_range = filters.get('pe_range')
        if pe_range and 'pe_ttm' in df.columns:
            pe_min, pe_max = pe_range
            before = len(df)
            df = df[(df['pe_ttm'] >= pe_min) & (df['pe_ttm'] <= pe_max)]
            logger.info(f"PE [{pe_min}, {pe_max}] 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 8. PB范围
        pb_range = filters.get('pb_range')
        if pb_range and 'pb' in df.columns:
            pb_min, pb_max = pb_range
            before = len(df)
            df = df[(df['pb'] >= pb_min) & (df['pb'] <= pb_max)]
            logger.info(f"PB [{pb_min}, {pb_max}] 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 9. 换手率范围
        turnover_range = filters.get('turnover_rate_range')
        if turnover_range and 'turnover_rate' in df.columns:
            tr_min, tr_max = turnover_range
            before = len(df)
            df = df[(df['turnover_rate'] >= tr_min) & (df['turnover_rate'] <= tr_max)]
            logger.info(f"换手率 [{tr_min}, {tr_max}] 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 10. 涨跌幅范围 (排除涨停/跌停)
        change_range = filters.get('change_pct_range')
        if change_range and 'change_pct' in df.columns:
            c_min, c_max = change_range
            before = len(df)
            df = df[(df['change_pct'] >= c_min) & (df['change_pct'] <= c_max)]
            logger.info(f"涨跌幅 [{c_min}, {c_max}] 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        # 11. 量比筛选
        min_volume_ratio = filters.get('min_volume_ratio', 0)
        if min_volume_ratio > 0 and 'volume_ratio' in df.columns:
            before = len(df)
            df = df[df['volume_ratio'] >= min_volume_ratio]
            logger.info(f"量比 >= {min_volume_ratio} 后: {len(df)} 只 (移除 {before - len(df)} 只)")

        logger.info(f"量化筛选完成: {original_count} → {len(df)} 只股票")
        return df.reset_index(drop=True)

    # ========== 便捷筛选方法 ==========

    def get_top_gainers(self, n: int = 20, exclude_limit_up: bool = True) -> pd.DataFrame:
        """获取涨幅前N名"""
        df = self.get_all_stocks()
        if df.empty:
            return df

        # 基础筛选
        df = self.screen_stocks(df)

        if exclude_limit_up and 'change_pct' in df.columns:
            df = df[df['change_pct'] < 9.8]  # 排除涨停

        return df.nlargest(n, 'change_pct').reset_index(drop=True)

    def get_top_losers(self, n: int = 20, exclude_limit_down: bool = True) -> pd.DataFrame:
        """获取跌幅前N名"""
        df = self.get_all_stocks()
        if df.empty:
            return df

        df = self.screen_stocks(df)

        if exclude_limit_down and 'change_pct' in df.columns:
            df = df[df['change_pct'] > -9.8]

        return df.nsmallest(n, 'change_pct').reset_index(drop=True)

    def get_top_volume(self, n: int = 20) -> pd.DataFrame:
        """获取成交额前N名"""
        df = self.get_all_stocks()
        if df.empty:
            return df

        df = self.screen_stocks(df)
        return df.nlargest(n, 'amount').reset_index(drop=True)

    def get_top_turnover(self, n: int = 20) -> pd.DataFrame:
        """获取换手率前N名"""
        df = self.get_all_stocks()
        if df.empty:
            return df

        df = self.screen_stocks(df)
        return df.nlargest(n, 'turnover_rate').reset_index(drop=True)

    def get_low_pe_stocks(self, max_pe: float = 20, n: int = 50) -> pd.DataFrame:
        """获取低PE股票"""
        df = self.get_all_stocks()
        if df.empty:
            return df

        df = self.screen_stocks(df, filters={'pe_range': [1, max_pe]})
        return df.nsmallest(n, 'pe_ttm').reset_index(drop=True)

    def get_high_market_cap(self, n: int = 50) -> pd.DataFrame:
        """获取市值前N名"""
        df = self.get_all_stocks()
        if df.empty:
            return df

        df = self.screen_stocks(df)
        return df.nlargest(n, 'total_market_cap').reset_index(drop=True)

    def search_stocks(self, keyword: str) -> pd.DataFrame:
        """
        按关键词搜索股票（名称或代码）

        Args:
            keyword: 搜索关键词
        """
        df = self.get_all_stocks()
        if df.empty:
            return df

        mask = (
            df['code'].astype(str).str.contains(keyword, na=False) |
            df['name'].astype(str).str.contains(keyword, na=False, case=False)
        )
        return df[mask].reset_index(drop=True)

    # ========== 高级筛选：多策略组合 ==========

    def screen_by_strategy(self, strategy: str = 'value') -> pd.DataFrame:
        """
        按预设策略筛选股票

        Args:
            strategy: 策略名称
                - 'value': 价值型（低PE、低PB、高市值）
                - 'growth': 成长型（中PE、高换手、60日涨幅正）
                - 'momentum': 动量型（近期强势、量价齐升）
                - 'oversold': 超跌反弹（近期大跌、量比放大）
                - 'active': 活跃型（高换手、高成交额）
        """
        df = self.get_all_stocks()
        if df.empty:
            return df

        # 基础筛选（排除ST、停牌等）
        df = self.screen_stocks(df, filters={})

        strategies = {
            'value': {
                'pe_range': [1, 25],
                'pb_range': [0, 3],
            },
            'growth': {
                'pe_range': [15, 80],
                'turnover_rate_range': [2, 30],
            },
            'momentum': {
                'min_volume_ratio': 1.5,
                'change_pct_range': [1, 9.5],
            },
            'oversold': {
                'change_pct_range': [-9.5, -3],
                'min_volume_ratio': 1.2,
            },
            'active': {
                'turnover_rate_range': [5, 30],
            },
        }

        if strategy not in strategies:
            logger.warning(f"未知策略: {strategy}，使用默认 value 策略")
            strategy = 'value'

        filters = strategies[strategy]
        df = self.screen_stocks(df, filters=filters)

        # 按策略做二次排序
        if strategy == 'value' and 'pe_ttm' in df.columns:
            df = df.sort_values('pe_ttm', ascending=True)
        elif strategy == 'growth' and 'change_pct_60d' in df.columns:
            df = df.sort_values('change_pct_60d', ascending=False)
        elif strategy == 'momentum' and 'change_pct' in df.columns:
            df = df.sort_values('change_pct', ascending=False)
        elif strategy == 'oversold' and 'change_pct_60d' in df.columns:
            df = df.sort_values('change_pct_60d', ascending=True)
        elif strategy == 'active' and 'amount' in df.columns:
            df = df.sort_values('amount', ascending=False)

        logger.info(f"策略 [{strategy}] 筛选完成，结果: {len(df)} 只股票")
        return df.head(100).reset_index(drop=True)

    # ========== 数据打包（为Agent提供结构化数据） ==========

    def get_stock_summary(self, df: pd.DataFrame = None, top_n: int = 30) -> str:
        """
        将股票数据打包为LLM可读的文本摘要

        Args:
            df: 股票数据，为None则使用筛选后的数据
            top_n: 最多包含多少只股票

        Returns:
            格式化的文本摘要
        """
        if df is None:
            df = self.screen_stocks()

        if df.empty:
            return "当前无符合条件的股票数据。"

        df = df.head(top_n)

        summary_lines = [
            f"## 股票池数据摘要（共 {len(df)} 只）",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "| 代码 | 名称 | 最新价 | 涨跌幅 | PE(TTM) | PB | 总市值(亿) | 成交额(万) | 换手率 |",
            "|------|------|--------|--------|---------|-----|-----------|-----------|--------|",
        ]

        for _, row in df.iterrows():
            code = row.get('code', '')
            name = row.get('name', '')
            price = row.get('price', 0)
            change = row.get('change_pct', 0)
            pe = row.get('pe_ttm', 0)
            pb = row.get('pb', 0)
            cap = row.get('total_market_cap', 0) / 1e8 if row.get('total_market_cap', 0) else 0
            amt = row.get('amount', 0) / 1e4 if row.get('amount', 0) else 0
            turnover = row.get('turnover_rate', 0)

            change_str = f"+{change:.2f}%" if change > 0 else f"{change:.2f}%"
            pe_str = f"{pe:.1f}" if pd.notna(pe) and pe != 0 else "-"
            pb_str = f"{pb:.2f}" if pd.notna(pb) and pb != 0 else "-"

            summary_lines.append(
                f"| {code} | {name} | {price:.2f} | {change_str} | "
                f"{pe_str} | {pb_str} | {cap:.1f} | {amt:.0f} | {turnover:.2f}% |"
            )

        return "\n".join(summary_lines)

    def get_market_overview(self) -> Dict[str, Any]:
        """
        获取市场总览数据

        Returns:
            市场总览字典
        """
        df = self.get_all_stocks()
        if df.empty:
            return {}

        # 基础筛选
        valid = df[(df['price'] > 0) & (~df['name'].str.contains(r'ST', na=False))]

        total = len(valid)
        up_count = len(valid[valid['change_pct'] > 0])
        down_count = len(valid[valid['change_pct'] < 0])
        flat_count = total - up_count - down_count
        limit_up = len(valid[valid['change_pct'] >= 9.8])
        limit_down = len(valid[valid['change_pct'] <= -9.8])

        avg_change = valid['change_pct'].mean() if not valid.empty else 0
        median_change = valid['change_pct'].median() if not valid.empty else 0
        total_amount = valid['amount'].sum() / 1e8 if 'amount' in valid.columns else 0  # 亿

        overview = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_stocks': total,
            'up_count': up_count,
            'down_count': down_count,
            'flat_count': flat_count,
            'limit_up': limit_up,
            'limit_down': limit_down,
            'up_ratio': f"{up_count / total * 100:.1f}%" if total > 0 else "0%",
            'avg_change_pct': f"{avg_change:.2f}%",
            'median_change_pct': f"{median_change:.2f}%",
            'total_amount_billion': f"{total_amount:.0f}亿",
        }

        logger.info(f"市场总览: 上涨 {up_count}, 下跌 {down_count}, 涨停 {limit_up}, 跌停 {limit_down}")
        return overview

    def get_market_overview_text(self) -> str:
        """获取市场总览的文本描述"""
        ov = self.get_market_overview()
        if not ov:
            return "无法获取市场总览数据。"

        return (
            f"### 📊 A股市场总览 ({ov['timestamp']})\n\n"
            f"- **总股票数**: {ov['total_stocks']}\n"
            f"- **上涨 / 下跌 / 平盘**: {ov['up_count']} / {ov['down_count']} / {ov['flat_count']}"
            f" (上涨占比 {ov['up_ratio']})\n"
            f"- **涨停 / 跌停**: {ov['limit_up']} / {ov['limit_down']}\n"
            f"- **平均涨跌幅**: {ov['avg_change_pct']} | **中位数**: {ov['median_change_pct']}\n"
            f"- **全市场成交额**: {ov['total_amount_billion']}\n"
        )

    # ========== 数据持久化 ==========

    def save_screened_stocks(self, df: pd.DataFrame, filename: str = None) -> str:
        """
        保存筛选结果到文件

        Args:
            df: 筛选后的DataFrame
            filename: 文件名

        Returns:
            保存的文件路径
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"screened_stocks_{timestamp}.csv"

        output_dir = "data/screened_stocks"
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"筛选结果已保存: {filepath}，共 {len(df)} 只股票")
        return filepath

    # ========== 缓存管理 ==========

    def clear_cache(self):
        """清除所有缓存"""
        with self._lock:
            self._all_stocks_cache = None
            self._all_stocks_cache_time = 0
            self._industry_cache.clear()
            self._concept_cache.clear()
            self._industry_list_cache = None
            self._concept_list_cache = None
            self._board_cache_time.clear()
        logger.info("股票池缓存已清除")

    def get_cache_info(self) -> Dict[str, Any]:
        """获取缓存状态"""
        info = {
            'all_stocks_cached': self._all_stocks_cache is not None,
            'all_stocks_count': len(self._all_stocks_cache) if self._all_stocks_cache is not None else 0,
            'all_stocks_age': f"{time.time() - self._all_stocks_cache_time:.0f}s" if self._all_stocks_cache_time > 0 else "N/A",
            'industry_boards_cached': len(self._industry_cache),
            'concept_boards_cached': len(self._concept_cache),
        }
        return info
