import json
import re
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import akshare as ak
import pandas as pd
import requests
from loguru import logger

from src.quote_sources import to_market_symbol


SINA_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
}


def fetch_akshare_stock_news(code: str, limit: int = 8) -> pd.DataFrame:
    """读取东方财富个股新闻。"""
    code = str(code).zfill(6)
    try:
        raw = ak.stock_news_em(symbol=code)
    except Exception as exc:
        logger.warning(f"[AKShare] 个股新闻拉取失败 {code}: {exc}")
        return pd.DataFrame()
    rows = []
    fetched_at = _now()
    for _, row in (raw if raw is not None else pd.DataFrame()).head(limit).iterrows():
        title = _first_value(row, ["新闻标题", "标题", "title"])
        if not title:
            continue
        rows.append(
            {
                "code": code,
                "datetime": _datetime_text(_first_value(row, ["发布时间", "时间", "datetime"])),
                "title": title,
                "content": _first_value(row, ["新闻内容", "内容", "摘要", "content"]),
                "url": _first_value(row, ["新闻链接", "链接", "url"]),
                "provider": _first_value(row, ["文章来源", "来源", "provider"]),
                "source": "eastmoney_stock_news_akshare",
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def fetch_cls_global_news(limit: int = 50, symbol: str = "全部") -> pd.DataFrame:
    """读取财联社快讯。"""
    try:
        raw = ak.stock_info_global_cls(symbol=symbol)
    except Exception as exc:
        logger.warning(f"[AKShare] 财联社快讯拉取失败: {exc}")
        return pd.DataFrame()
    return _normalize_global_news(raw, source="cls_global_news_akshare", limit=limit)


def fetch_eastmoney_global_news(limit: int = 50) -> pd.DataFrame:
    """读取东方财富全球资讯。"""
    try:
        raw = ak.stock_info_global_em()
    except Exception as exc:
        logger.warning(f"[AKShare] 东方财富全球资讯拉取失败: {exc}")
        return pd.DataFrame()
    return _normalize_global_news(raw, source="eastmoney_global_news_akshare", limit=limit)


def fetch_stock_news_bundle(code: str, stock_limit: int = 8, global_limit: int = 20) -> pd.DataFrame:
    """组合个股新闻、财联社快讯、全球资讯三源。"""
    frames = [
        fetch_akshare_stock_news(code, limit=stock_limit),
        fetch_cls_global_news(limit=global_limit),
        fetch_eastmoney_global_news(limit=global_limit),
    ]
    frames = [df for df in frames if df is not None and not df.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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
        return "未找到近期新闻。"
    lines = []
    for _, row in news_df.head(limit).iterrows():
        provider = f"[{row.get('source')}]" if row.get("source") else ""
        content = str(row.get("content") or "").strip()
        suffix = f" - {content[:120]}" if content else ""
        lines.append(f"{row.get('datetime', '')} {provider} {row.get('title', '')}{suffix} ({row.get('url', '')})")
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
    return value.strip()


def _to_float(value):
    try:
        if value in ("", None, "-", "--"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 研报、公告与资讯主源
# ---------------------------------------------------------------------------

_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.eastmoney.com/",
}


def fetch_eastmoney_research_reports(code: str, max_pages: int = 2, timeout: int = 15) -> pd.DataFrame:
    """读取东方财富 reportapi 个股研报列表与 PDF 元数据。"""
    code = str(code).zfill(6)
    rows = []
    fetched_at = _now()
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"})
    for page in range(1, max(1, int(max_pages or 1)) + 1):
        params = {
            "industryCode": "*",
            "pageSize": "100",
            "industry": "*",
            "rating": "*",
            "ratingChange": "*",
            "beginTime": "2000-01-01",
            "endTime": "2035-01-01",
            "pageNo": str(page),
            "qType": "0",
            "code": code,
            "p": str(page),
            "pageNum": str(page),
            "pageNumber": str(page),
        }
        try:
            data = session.get("https://reportapi.eastmoney.com/report/list", params=params, timeout=timeout).json()
        except Exception as exc:
            logger.warning(f"[EastMoney] 研报列表拉取失败 {code}: {exc}")
            break
        items = data.get("data") or []
        if not items:
            break
        for item in items:
            info_code = str(item.get("infoCode") or item.get("INFO_CODE") or "")
            rows.append(
                {
                    "code": code,
                    "info_code": info_code,
                    "title": item.get("title") or item.get("TITLE") or "",
                    "publish_date": _date_text(item.get("publishDate") or item.get("PUBLISH_DATE")),
                    "org_name": item.get("orgSName") or item.get("ORG_S_NAME") or item.get("orgName") or "",
                    "author": item.get("researcher") or item.get("author") or "",
                    "rating": item.get("emRatingName") or item.get("rating") or "",
                    "rating_change": item.get("ratingChange") or item.get("ratingChangeName") or "",
                    "industry": item.get("indvInduName") or item.get("industryName") or "",
                    "eps_this_year": _to_float(item.get("predictThisYearEps")),
                    "eps_next_year": _to_float(item.get("predictNextYearEps")),
                    "eps_next_two_year": _to_float(item.get("predictNextTwoYearEps")),
                    "pdf_url": f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else "",
                    "source": "eastmoney_reportapi",
                    "fetched_at": fetched_at,
                }
            )
        if page >= int(data.get("TotalPage") or data.get("totalPage") or page):
            break
    return pd.DataFrame(rows)


def fetch_ths_eps_forecast(code: str) -> pd.DataFrame:
    """读取同花顺一致预期 EPS。"""
    code = str(code).zfill(6)
    try:
        raw = ak.stock_profit_forecast_ths(symbol=code, indicator="预测年报每股收益")
    except Exception as exc:
        logger.warning(f"[THS] EPS 一致预期拉取失败 {code}: {exc}")
        return pd.DataFrame()
    rows = []
    fetched_at = _now()
    for _, row in (raw if raw is not None else pd.DataFrame()).iterrows():
        year = str(_first_value(row, ["年度", "year"]) or "").strip()
        if not year:
            continue
        rows.append(
            {
                "code": code,
                "forecast_year": year,
                "institution_count": _to_float(_first_value(row, ["预测机构数", "机构数"])),
                "eps_min": _to_float(_first_value(row, ["最小值", "min"])),
                "eps_avg": _to_float(_first_value(row, ["均值", "avg"])),
                "eps_max": _to_float(_first_value(row, ["最大值", "max"])),
                "industry_avg": _to_float(_first_value(row, ["行业平均数", "行业均值"])),
                "source": "ths_eps_forecast_akshare",
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def fetch_cninfo_announcements(
    code: str,
    limit: int = 10,
    start_date: str = "",
    end_date: str = "",
    timeout: int = 15,
) -> pd.DataFrame:
    """读取巨潮沪深北公告列表与 PDF 链接。"""
    code = str(code).zfill(6)
    end_date = str(end_date or datetime.now().strftime("%Y%m%d")).replace("-", "")[:8]
    start_date = str(
        start_date or (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")
    ).replace("-", "")[:8]
    try:
        raw = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=code,
            market="沪深京",
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:
        logger.warning(f"[CNInfo] 公告列表拉取失败 {code}: {exc}")
        return pd.DataFrame()
    rows = []
    fetched_at = _now()
    for _, row in (raw if raw is not None else pd.DataFrame()).head(limit).iterrows():
        title = _first_value(row, ["公告标题", "公告名称", "title"])
        url = _first_value(row, ["公告链接", "PDF链接", "url"])
        if not title:
            continue
        rows.append(
            {
                "code": code,
                "announcement_id": _announcement_id(url, title),
                "title": title,
                "date": _date_text(_first_value(row, ["公告时间", "公告日期", "date"])),
                "categories": _first_value(row, ["公告类型", "分类", "category"]),
                "url": url,
                "pdf_url": url if str(url).lower().endswith(".pdf") else "",
                "source": "cninfo_akshare",
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def fetch_cninfo_announcement_content(url: str, timeout: int = 15) -> Dict:
    """读取巨潮公告正文；PDF 公告保留链接和字节大小，正文由下游按需解析。"""
    url = str(url or "").strip()
    if not url:
        return {}
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "http://www.cninfo.com.cn/"}, timeout=timeout)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"[CNInfo] 公告正文拉取失败 {url}: {exc}")
        return {}
    content_type = resp.headers.get("Content-Type", "")
    if "pdf" in content_type.lower() or resp.content[:4] == b"%PDF":
        return {
            "art_code": _announcement_id(url, ""),
            "title": "",
            "date": "",
            "content": "",
            "attach_url": url,
            "pdf_url": url,
            "pdf_size": len(resp.content),
        }
    resp.encoding = resp.apparent_encoding or resp.encoding
    return {
        "art_code": _announcement_id(url, ""),
        "title": "",
        "date": "",
        "content": _clean_html_text(resp.text),
        "attach_url": url,
        "pdf_url": "",
        "pdf_size": len(resp.content),
    }


def format_announcements_for_prompt(df: pd.DataFrame, limit: int = 8) -> str:
    """将巨潮公告 DataFrame 格式化为 Prompt 友好的文本。"""
    if df is None or df.empty:
        return ""
    lines = ["巨潮近期公告："]
    for _, row in df.head(limit).iterrows():
        cat = f" [{row.get('categories')}]" if row.get("categories") else ""
        url = f" ({row.get('url')})" if row.get("url") else ""
        lines.append(f"  {row.get('date', '')} {row.get('title', '')}{cat}{url}")
    return "\n".join(lines)


def _normalize_global_news(raw: pd.DataFrame | None, source: str, limit: int) -> pd.DataFrame:
    rows = []
    fetched_at = _now()
    for _, row in (raw if raw is not None else pd.DataFrame()).head(limit).iterrows():
        title = _first_value(row, ["标题", "内容", "摘要", "title", "content"])
        if not title:
            continue
        rows.append(
            {
                "code": "",
                "datetime": _datetime_text(_first_value(row, ["发布时间", "时间", "日期", "datetime"])),
                "title": str(title).strip(),
                "content": _first_value(row, ["内容", "摘要", "summary"]),
                "url": _first_value(row, ["链接", "url"]),
                "provider": _first_value(row, ["来源", "provider"]),
                "source": source,
                "fetched_at": fetched_at,
            }
        )
    return pd.DataFrame(rows)


def _first_value(row, names: Iterable[str]):
    for name in names:
        if name in row and not pd.isna(row.get(name)):
            return row.get(name)
    return None


def _datetime_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if not pd.isna(parsed):
        return pd.Timestamp(parsed).strftime("%Y-%m-%d %H:%M:%S")
    return str(value).strip()


def _date_text(value) -> str:
    text = _datetime_text(value)
    return text[:10] if text else ""


def _announcement_id(url: str, title: str) -> str:
    text = str(url or title or "").strip()
    if not text:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", text.split("/")[-1])[:80] or str(abs(hash(text)))


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
