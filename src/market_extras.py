import html
import json
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from loguru import logger

from src.quote_sources import to_market_symbol


SINA_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
}


def fetch_sina_stock_news(code: str, limit: int = 8) -> pd.DataFrame:
    symbol = to_market_symbol(str(code).zfill(6), style="sina")
    if not symbol:
        return pd.DataFrame()

    url = f"https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllNewsStock/symbol/{symbol}.phtml"
    response = requests.get(url, headers=SINA_HEADERS, timeout=12)
    response.encoding = "gbk"
    response.raise_for_status()

    rows = []
    pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2})&nbsp;(\d{2}:\d{2}).{0,80}?"
        r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(response.text):
        title = _clean_html_text(match.group(4))
        link = html.unescape(match.group(3))
        if not title or not link:
            continue
        rows.append(
            {
                "code": str(code).zfill(6),
                "datetime": f"{match.group(1)} {match.group(2)}",
                "title": title,
                "url": link,
                "source": "sina",
            }
        )
        if len(rows) >= limit:
            break

    return pd.DataFrame(rows)


def fetch_sina_board_list(kind: str = "industry") -> pd.DataFrame:
    if kind == "concept":
        url = "https://vip.stock.finance.sina.com.cn/q/view/newFLJK.php?param=class"
    else:
        url = "https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php"

    response = requests.get(url, headers=SINA_HEADERS, timeout=12)
    response.encoding = "gbk"
    response.raise_for_status()

    payload = _extract_js_object(response.text)
    if not payload:
        return pd.DataFrame()

    rows = []
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for node, raw in payload.items():
        parts = str(raw).split(",")
        if len(parts) < 13:
            continue
        row = {
            "node": parts[0],
            "name": parts[1],
            "stock_count": _to_float(parts[2]),
            "avg_price": _to_float(parts[3]),
            "change_amount": _to_float(parts[4]),
            "change_pct": _to_float(parts[5]),
            "volume": _to_float(parts[6]),
            "amount": _to_float(parts[7]),
            "leader_symbol": parts[8],
            "leader_change_pct": _to_float(parts[9]),
            "leader_price": _to_float(parts[10]),
            "leader_change_amount": _to_float(parts[11]),
            "leader_name": parts[12],
            "kind": kind,
            "source": "sina",
            "fetched_at": fetched_at,
        }
        row.update({"板块代码": row["node"], "板块名称": row["name"], "涨跌幅": row["change_pct"]})
        rows.append(row)

    return pd.DataFrame(rows)


def fetch_sina_board_constituents(node: str, page_size: int = 500) -> pd.DataFrame:
    rows = []
    page = 1
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    while True:
        params = {
            "page": page,
            "num": page_size,
            "sort": "symbol",
            "asc": 1,
            "node": node,
            "symbol": "",
            "_s_r_a": "page",
        }
        response = requests.get(
            "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData",
            params=params,
            headers=SINA_HEADERS,
            timeout=12,
        )
        response.encoding = "gbk"
        response.raise_for_status()
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            logger.warning(f"Failed to decode Sina board constituents for {node}")
            break
        if not data:
            break

        for item in data:
            rows.append(
                {
                    "code": str(item.get("code", "")).zfill(6),
                    "name": item.get("name"),
                    "price": _to_float(item.get("trade")),
                    "change_pct": _to_float(item.get("changepercent")),
                    "volume": _to_float(item.get("volume")),
                    "amount": _to_float(item.get("amount")),
                    "turnover_rate": _to_float(item.get("turnoverratio")),
                    "pe_ttm": _to_float(item.get("per")),
                    "pb": _to_float(item.get("pb")),
                    "total_market_cap": _to_float(item.get("mktcap")),
                    "board_node": node,
                    "source": "sina",
                    "fetched_at": fetched_at,
                }
            )

        if len(data) < page_size:
            break
        page += 1

    return pd.DataFrame(rows)


def find_board_node(board_name: str, boards: pd.DataFrame) -> Optional[str]:
    if boards is None or boards.empty:
        return None
    target = str(board_name).strip()
    if not target:
        return None

    exact = boards[boards["name"].astype(str) == target]
    if not exact.empty:
        return str(exact.iloc[0]["node"])

    contains = boards[boards["name"].astype(str).str.contains(re.escape(target), na=False)]
    if not contains.empty:
        return str(contains.iloc[0]["node"])
    return None


def format_news_for_prompt(news_df: pd.DataFrame, limit: int = 8) -> str:
    if news_df is None or news_df.empty:
        return "No recent stock news found from Sina Finance."
    lines = []
    for _, row in news_df.head(limit).iterrows():
        lines.append(f"{row.get('datetime', '')} {row.get('title', '')} ({row.get('url', '')})")
    return "\n".join(lines)


def _extract_js_object(text: str) -> Dict:
    match = re.search(r"=\s*(\{.*\})\s*;?\s*$", text.strip(), re.DOTALL)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        logger.warning(f"Failed to decode Sina board list: {exc}")
        return {}


def _clean_html_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value)
    return html.unescape(value).strip()


def _to_float(value):
    try:
        if value in ("", None, "-", "--"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 东方财富免费公告接口
# ---------------------------------------------------------------------------

_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.eastmoney.com/",
}


def fetch_eastmoney_announcements(
    code: str,
    limit: int = 10,
    timeout: int = 10,
) -> pd.DataFrame:
    """从东方财富拉取个股最近公告标题和分类标签（免费、无需 API Key）。"""
    code = str(code).zfill(6)
    url = (
        f"https://np-anotice-stock.eastmoney.com/api/security/ann"
        f"?cb=jQuery&sr=-1&page_size={limit}&page_index=1"
        f"&ann_type=SHA&client_source=web"
        f"&stock_list={code}&f_node=0&s_node=0"
    )
    try:
        resp = requests.get(url, headers=_EM_HEADERS, timeout=timeout)
        resp.raise_for_status()
        text = resp.text
        if text.startswith("jQuery"):
            text = text[text.index("(") + 1 : text.rindex(")")]
        data = json.loads(text)
        items = data.get("data", {}).get("list", [])
    except Exception as exc:
        logger.warning(f"[EastMoney] 个股公告拉取失败 {code}: {exc}")
        return pd.DataFrame()

    rows = []
    for item in items:
        columns = item.get("columns") or []
        col_names = [c.get("column_name", "") for c in columns if c.get("column_name")]
        rows.append(
            {
                "code": code,
                "title": item.get("title", ""),
                "date": str(item.get("notice_date", ""))[:10],
                "categories": "、".join(col_names) if col_names else "",
                "source": "eastmoney",
            }
        )
    return pd.DataFrame(rows)


def format_announcements_for_prompt(df: pd.DataFrame, limit: int = 8) -> str:
    """将东方财富公告 DataFrame 格式化为 Prompt 友好的文本。"""
    if df is None or df.empty:
        return ""
    lines = ["东方财富近期公告："]
    for _, row in df.head(limit).iterrows():
        cat = f" [{row.get('categories')}]" if row.get("categories") else ""
        lines.append(f"  {row.get('date', '')} {row.get('title', '')}{cat}")
    return "\n".join(lines)

