"""临时修复 daily_quotes 指定交易日的换手率字段。"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.database import StockDatabase  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 Tushare daily_basic 回补 daily_quotes 的历史换手率。"
    )
    parser.add_argument("--date", default="20260513", help="交易日，格式 YYYYMMDD")
    parser.add_argument("--db", default="data/stock_lake.db", help="SQLite 数据库路径")
    parser.add_argument("--dry-run", action="store_true", help="只检查和预览，不写入数据库")
    parser.add_argument(
        "--source",
        choices=["auto", "tushare", "akshare", "derive"],
        default="auto",
        help="修复数据源；auto 会先试 Tushare/AKShare，再用邻近日的正常换手率推导",
    )
    parser.add_argument(
        "--reference-date",
        default="",
        help="derive 模式使用的参考交易日，留空时自动选择目标日之后最近的正常 daily_quotes 日期",
    )
    parser.add_argument("--workers", type=int, default=8, help="AKShare 兜底抓取并发数")
    parser.add_argument("--limit", type=int, default=0, help="仅调试前 N 只股票，0 表示全量")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=800,
        help="分批更新 SQLite 的批大小",
    )
    return parser.parse_args()


def normalize_date(value: str) -> str:
    value = str(value).replace("-", "").strip()
    if len(value) != 8 or not value.isdigit():
        raise ValueError("--date 必须是 YYYYMMDD 或 YYYY-MM-DD")
    return value


def load_tushare_daily_basic(trade_date: str) -> pd.DataFrame:
    """读取 Tushare daily_basic，获取历史换手率和估值字段。"""
    load_dotenv(ROOT_DIR / ".env")
    token = (os.getenv("TUSHARE_TOKEN") or os.getenv("TUSHARE_API_KEY") or "").strip()
    if not token:
        raise RuntimeError("未配置 TUSHARE_TOKEN/TUSHARE_API_KEY")

    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError("当前环境未安装 tushare") from exc

    pro = ts.pro_api(token)
    fields = "ts_code,trade_date,close,turnover_rate,pe_ttm,pb,total_mv"
    df = pro.daily_basic(trade_date=trade_date, fields=fields)
    if df is None or df.empty:
        raise RuntimeError(f"Tushare daily_basic 未返回数据: {trade_date}")

    clean_df = df.copy()
    clean_df["code"] = clean_df["ts_code"].astype(str).str.split(".", n=1).str[0].str.zfill(6)
    clean_df["trade_date"] = clean_df["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8]
    clean_df["turnover_rate"] = pd.to_numeric(clean_df["turnover_rate"], errors="coerce")
    clean_df["price"] = pd.to_numeric(clean_df["close"], errors="coerce")
    clean_df["change_pct"] = None
    clean_df["volume"] = None
    clean_df["amount"] = None
    clean_df["pe_ttm"] = pd.to_numeric(clean_df["pe_ttm"], errors="coerce")
    clean_df["pb"] = pd.to_numeric(clean_df["pb"], errors="coerce")
    clean_df["total_market_cap"] = pd.to_numeric(clean_df["total_mv"], errors="coerce") * 10000
    clean_df = clean_df[clean_df["trade_date"] == trade_date]
    clean_df = clean_df.dropna(subset=["code", "turnover_rate"])
    clean_df = clean_df.drop_duplicates(subset=["code"], keep="last")
    return clean_df[
        [
            "code", "price", "change_pct", "volume", "amount", "turnover_rate",
            "pe_ttm", "pb", "total_market_cap"
        ]
    ]


def fetch_akshare_one(code: str, trade_date: str, retries: int = 3) -> dict | None:
    """从 AKShare/东方财富历史日线读取单只股票当日换手率。"""
    import akshare as ak

    df = None
    for attempt in range(1, max(1, int(retries)) + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=str(code).zfill(6),
                period="daily",
                start_date=trade_date,
                end_date=trade_date,
                adjust="",
            )
            break
        except Exception:
            if attempt >= retries:
                return None
            time.sleep(0.8 * attempt)
    if df is None or df.empty or "换手率" not in df.columns:
        return None

    row = df.iloc[-1]
    turnover_rate = pd.to_numeric(pd.Series([row.get("换手率")]), errors="coerce").iloc[0]
    if pd.isna(turnover_rate) or float(turnover_rate) <= 0:
        return None

    return {
        "code": str(row.get("股票代码") or code).zfill(6),
        "price": row.get("收盘"),
        "change_pct": row.get("涨跌幅"),
        "volume": row.get("成交量"),
        "amount": row.get("成交额"),
        "turnover_rate": turnover_rate,
        "pe_ttm": None,
        "pb": None,
        "total_market_cap": None,
    }


def load_akshare_history_quotes(codes: list[str], trade_date: str, workers: int) -> pd.DataFrame:
    """批量从 AKShare 历史日线回补指定日期的换手率。"""
    rows = []
    workers = max(1, int(workers))
    total = len(codes)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_code = {
            executor.submit(fetch_akshare_one, code, trade_date): code
            for code in codes
        }
        for idx, future in enumerate(as_completed(future_to_code), start=1):
            row = future.result()
            if row:
                rows.append(row)
            if idx % 200 == 0 or idx == total:
                print(f"AKShare 进度: {idx}/{total}, 可用={len(rows)}", flush=True)

    if not rows:
        raise RuntimeError("AKShare 历史日线未返回可用换手率")

    clean_df = pd.DataFrame(rows)
    for col in [
        "price", "change_pct", "volume", "amount", "turnover_rate",
        "pe_ttm", "pb", "total_market_cap"
    ]:
        clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")
    clean_df = clean_df.drop_duplicates(subset=["code"], keep="last")
    return clean_df[
        [
            "code", "price", "change_pct", "volume", "amount", "turnover_rate",
            "pe_ttm", "pb", "total_market_cap"
        ]
    ]


def find_reference_date(conn: sqlite3.Connection, trade_date: str) -> str:
    """自动选择目标日之后最近的、换手率覆盖正常的参考日期。"""
    rows = conn.execute(
        """
        SELECT trade_date,
               COUNT(*) AS total,
               SUM(CASE WHEN turnover_rate > 0 THEN 1 ELSE 0 END) AS positive
        FROM daily_quotes
        WHERE trade_date > ?
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        (trade_date,),
    ).fetchall()
    for row in rows:
        total = int(row["total"] or 0)
        positive = int(row["positive"] or 0)
        if total and positive / total >= 0.5:
            return str(row["trade_date"])
    raise RuntimeError("未找到目标日之后换手率覆盖正常的参考日期")


def load_derived_turnover(
    db_path: Path,
    codes: list[str],
    trade_date: str,
    reference_date: str,
) -> pd.DataFrame:
    """用参考日换手率和两日成交量比例推导目标日换手率。"""
    db = StockDatabase(db_path=str(db_path))
    with db.get_connection() as conn:
        if not reference_date:
            reference_date = find_reference_date(conn, trade_date)
        else:
            reference_date = normalize_date(reference_date)

        code_df = pd.DataFrame({"code": [str(code).zfill(6) for code in codes]})
        target_bars = pd.read_sql_query(
            """
            SELECT code, close AS price, volume, amount
            FROM market_bars
            WHERE period = 'daily'
              AND trade_date = ?
            """,
            conn,
            params=(trade_date,),
        )
        reference_bars = pd.read_sql_query(
            """
            SELECT code, volume AS reference_volume
            FROM market_bars
            WHERE period = 'daily'
              AND trade_date = ?
            """,
            conn,
            params=(reference_date,),
        )
        reference_quotes = pd.read_sql_query(
            """
            SELECT code, turnover_rate AS reference_turnover_rate
            FROM daily_quotes
            WHERE trade_date = ?
              AND turnover_rate > 0
            """,
            conn,
            params=(reference_date,),
        )

    merged = code_df.merge(target_bars, on="code", how="inner")
    merged = merged.merge(reference_bars, on="code", how="inner")
    merged = merged.merge(reference_quotes, on="code", how="inner")
    for col in ["volume", "amount", "reference_volume", "reference_turnover_rate", "price"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    usable = (
        merged["volume"].notna()
        & merged["reference_volume"].notna()
        & (merged["reference_volume"] > 0)
        & merged["reference_turnover_rate"].notna()
        & (merged["reference_turnover_rate"] > 0)
    )
    merged = merged[usable].copy()
    merged["turnover_rate"] = merged["volume"] / merged["reference_volume"] * merged["reference_turnover_rate"]
    merged["change_pct"] = None
    merged["pe_ttm"] = None
    merged["pb"] = None
    merged["total_market_cap"] = None
    merged = merged[(merged["turnover_rate"] > 0) & (merged["turnover_rate"] < 1000)]
    print(f"derive 参考日期: {reference_date}")
    return merged[
        [
            "code", "price", "change_pct", "volume", "amount", "turnover_rate",
            "pe_ttm", "pb", "total_market_cap"
        ]
    ]


def load_repair_source(
    source: str,
    codes: list[str],
    trade_date: str,
    workers: int,
    db_path: Path,
    reference_date: str = "",
) -> pd.DataFrame:
    """按配置选择修复源，自动模式会在 Tushare 权限不足时回退 AKShare。"""
    if source in {"auto", "tushare"}:
        try:
            return load_tushare_daily_basic(trade_date)
        except Exception as exc:
            if source == "tushare":
                raise
            print(f"Tushare daily_basic 不可用，改用 AKShare 历史日线: {exc}")
    if source in {"auto", "akshare"}:
        try:
            return load_akshare_history_quotes(codes, trade_date, workers)
        except Exception as exc:
            if source == "akshare":
                raise
            print(f"AKShare 历史日线不可用，改用成交量比例推导: {exc}")
    return load_derived_turnover(db_path, codes, trade_date, reference_date)


def coverage(conn: sqlite3.Connection, trade_date: str) -> dict[str, float]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN turnover_rate > 0 THEN 1 ELSE 0 END) AS positive,
               MIN(turnover_rate) AS min_turnover,
               MAX(turnover_rate) AS max_turnover,
               AVG(turnover_rate) AS avg_turnover
        FROM daily_quotes
        WHERE trade_date = ?
        """,
        (trade_date,),
    ).fetchone()
    total = int(row["total"] or 0)
    positive = int(row["positive"] or 0)
    return {
        "total": total,
        "positive": positive,
        "positive_pct": round(positive * 100 / total, 2) if total else 0.0,
        "min_turnover": row["min_turnover"],
        "max_turnover": row["max_turnover"],
        "avg_turnover": row["avg_turnover"],
    }


def repair_daily_quotes(
    db_path: Path,
    trade_date: str,
    source_df: pd.DataFrame,
    batch_size: int,
    dry_run: bool,
) -> None:
    db = StockDatabase(db_path=str(db_path))
    with db.get_connection() as conn:
        before = coverage(conn, trade_date)
        print(f"修复前: {before}")

        local_df = pd.read_sql_query(
            """
            SELECT code
            FROM daily_quotes
            WHERE trade_date = ?
            """,
            conn,
            params=(trade_date,),
        )
        if local_df.empty:
            raise RuntimeError(f"daily_quotes 没有 {trade_date} 的本地记录")

        local_codes = set(local_df["code"].astype(str).str.zfill(6))
        patch_df = source_df[source_df["code"].isin(local_codes)].copy()
        patch_df = patch_df[patch_df["turnover_rate"] > 0]
        if patch_df.empty:
            raise RuntimeError("Tushare 返回数据与本地记录没有可用交集")

        print(
            "可用于修复: "
            f"{len(patch_df)} / {len(local_codes)} 行，"
            f"换手率覆盖率 {round(len(patch_df) * 100 / len(local_codes), 2)}%"
        )

        if dry_run:
            print("dry-run 模式：未写入数据库")
            return

        backup_table = f"daily_quotes_backup_{trade_date}_{datetime.now().strftime('%H%M%S')}"
        conn.execute(
            f"""
            CREATE TABLE {backup_table} AS
            SELECT *
            FROM daily_quotes
            WHERE trade_date = ?
            """,
            (trade_date,),
        )

        rows = [
            (
                None if pd.isna(row.price) else float(row.price),
                None if pd.isna(row.change_pct) else float(row.change_pct),
                None if pd.isna(row.volume) else float(row.volume),
                None if pd.isna(row.amount) else float(row.amount),
                float(row.turnover_rate),
                None if pd.isna(row.pe_ttm) else float(row.pe_ttm),
                None if pd.isna(row.pb) else float(row.pb),
                None if pd.isna(row.total_market_cap) else float(row.total_market_cap),
                row.code,
                trade_date,
            )
            for row in patch_df.itertuples(index=False)
        ]

        update_sql = """
            UPDATE daily_quotes
            SET price = COALESCE(?, price),
                change_pct = COALESCE(?, change_pct),
                volume = COALESCE(?, volume),
                amount = COALESCE(?, amount),
                turnover_rate = ?,
                pe_ttm = COALESCE(?, pe_ttm),
                pb = COALESCE(?, pb),
                total_market_cap = COALESCE(?, total_market_cap)
            WHERE code = ?
              AND trade_date = ?
        """
        updated = 0
        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]
            conn.executemany(update_sql, batch)
            updated += len(batch)
            time.sleep(0.01)

        conn.commit()
        after = coverage(conn, trade_date)
        print(f"已备份原始记录到表: {backup_table}")
        print(f"已更新行数: {updated}")
        print(f"修复后: {after}")


def main() -> int:
    args = parse_args()
    trade_date = normalize_date(args.date)
    db_path = (ROOT_DIR / args.db).resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    db = StockDatabase(db_path=str(db_path))
    with db.get_connection() as conn:
        local_df = pd.read_sql_query(
            """
            SELECT code
            FROM daily_quotes
            WHERE trade_date = ?
            ORDER BY code
            """,
            conn,
            params=(trade_date,),
        )
    if local_df.empty:
        raise RuntimeError(f"daily_quotes 没有 {trade_date} 的本地记录")

    codes = local_df["code"].astype(str).str.zfill(6).tolist()
    if args.limit and int(args.limit) > 0:
        codes = codes[:int(args.limit)]

    source_df = load_repair_source(
        args.source,
        codes,
        trade_date,
        args.workers,
        db_path,
        args.reference_date,
    )
    repair_daily_quotes(
        db_path=db_path,
        trade_date=trade_date,
        source_df=source_df,
        batch_size=max(1, int(args.batch_size)),
        dry_run=bool(args.dry_run),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
