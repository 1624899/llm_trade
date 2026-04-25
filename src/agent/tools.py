"""
通用基础工具集 (Agent Tools)
供各个独立 Agent 共用的核心能力池：
1. LLM 统一请求调用 (免除到处读取 yaml 与拼装 http post 的麻烦)
2. 通用的盘后股票数据获取包 (新闻、K线抽取等)
"""

import hashlib
import html
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import quote_plus, unquote, urlparse, parse_qs

import yaml
import requests
import akshare as ak
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from src.database import StockDatabase
from src.agent.trace_recorder import trace_recorder
from src.market_extras import (
    fetch_eastmoney_announcements,
    fetch_sina_stock_news,
    format_announcements_for_prompt,
    format_news_for_prompt,
)
from src.market_sentiment import MarketSentiment


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

class AgentTools:
    def __init__(self):
        # 初始化只加载一次 LLM 配置
        self._load_config()

    def _load_config(self):
        """加载 LLM API 配置"""
        config_path = PROJECT_ROOT / "config" / "config.yaml"
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            active_name = config.get("active_llm", "xinliu")
            self.llm_cfg = self._resolve_env_config(config.get("llm_models", {}).get(active_name, {}))
            self.web_search_cfg = self._resolve_env_config(config.get("web_search", {}) or {})
        except Exception as e:
            logger.error(f"加载 LLM 配置失败: {e}")
            self.llm_cfg = {}
            self.web_search_cfg = {}

    def _resolve_env_config(self, cfg):
        """Resolve config values like env:DEEPSEEK_API_KEY without storing secrets."""
        if not isinstance(cfg, dict):
            return {}
        resolved = {}
        for key, value in cfg.items():
            if isinstance(value, str) and value.startswith("env:"):
                resolved[key] = os.getenv(value.split(":", 1)[1], "")
            else:
                resolved[key] = value
        return resolved

    def call_llm(self, system_prompt: str, user_prompt: str, temperature: float = 0.1) -> str:
        """
        统一调用 LLM 的底层封装
        """
        api_key = self.llm_cfg.get("api_key")
        base_url = self.llm_cfg.get("base_url")
        model = self.llm_cfg.get("model")
        logger.info(
            f"[LLM] start model={model or 'unknown'} temperature={temperature} "
            f"system_chars={len(system_prompt or '')} user_chars={len(user_prompt or '')}"
        )
        trace_recorder.record(
            "llm_start",
            {
                "model": model,
                "temperature": temperature,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "system_chars": len(system_prompt or ""),
                "user_chars": len(user_prompt or ""),
            },
        )

        if not api_key:
            trace_recorder.record("llm_error", {"model": model, "error": "missing_api_key"})
            logger.error("未找到有效的 API Key")
            return ""

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        data = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": temperature
        }

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                start_time = time.time()
                response = requests.post(f"{base_url}/chat/completions", headers=headers, json=data, timeout=90)
                response.raise_for_status()
                res_json = response.json()
                content = res_json['choices'][0]['message']['content'].strip()
                logger.info(
                    f"[LLM] done model={model or 'unknown'} elapsed={time.time() - start_time:.1f}s "
                    f"output_chars={len(content)}"
                )
                trace_recorder.record(
                    "llm_done",
                    {
                        "model": model,
                        "elapsed_seconds": round(time.time() - start_time, 3),
                        "output": content,
                        "output_chars": len(content),
                    },
                )
                return content
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    logger.warning(f"LLM 网络请求失败 (第 {attempt}/{max_retries} 次尝试): {e}，等待重试...")
                    time.sleep(2 * attempt)
                else:
                    logger.error(f"LLM 网络请求失败 (最终失败): {e}")
                    return ""
            except KeyError as e:
                logger.error(f"LLM 响应格式解析异常: {e}")
                return ""
        return ""

    def web_search(
        self,
        query: str,
        purpose: str = "",
        max_results: int = 5,
        domains: list[str] | None = None,
    ) -> str:
        """检索最新公开信息，默认 Tavily 主力、DuckDuckGo 兜底。"""
        cfg = self.web_search_cfg or {}
        if not cfg.get("enabled", False):
            logger.info(f"[WebSearch] disabled purpose={purpose or '-'} query_chars={len(query or '')}")
            trace_recorder.record(
                "web_search_disabled",
                {"purpose": purpose, "query": query, "max_results": max_results, "domains": domains or []},
            )
            return ""

        # 低于此字符数的搜索结果视为无效，不使用缓存、不写入缓存
        min_result_chars = int(cfg.get("min_result_chars", 30))

        cache_key = self._web_search_cache_key(query, purpose, max_results, domains)
        cached = self._read_web_search_cache(cache_key)
        if cached and len(cached) > min_result_chars:
            logger.info(
                f"[WebSearch] cache_hit provider={cfg.get('provider') or 'tavily'} purpose={purpose or '-'} "
                f"query_chars={len(query or '')} result_chars={len(cached)}"
            )
            trace_recorder.record(
                "web_search_cache_hit",
                {
                    "provider": cfg.get("provider") or "tavily",
                    "purpose": purpose,
                    "query": query,
                    "max_results": max_results,
                    "domains": domains or [],
                    "result": cached,
                    "result_chars": len(cached),
                },
            )
            return cached
        elif cached:
            logger.warning(
                f"[WebSearch] cache_rejected_short result_chars={len(cached)} min={min_result_chars} "
                f"purpose={purpose or '-'} — 缓存结果过短，强制重新搜索。"
            )

        provider = str(cfg.get("provider") or "tavily").lower()
        fallback_provider = str(cfg.get("fallback_provider") or "duckduckgo").lower()
        logger.info(
            f"[WebSearch] start provider={provider} fallback={fallback_provider} purpose={purpose or '-'} "
            f"max_results={max_results} domains={','.join(domains or []) or '-'} query_chars={len(query or '')}"
        )
        trace_recorder.record(
            "web_search_start",
            {
                "provider": provider,
                "fallback_provider": fallback_provider,
                "purpose": purpose,
                "query": query,
                "max_results": max_results,
                "domains": domains or [],
            },
        )

        if provider == "tavily":
            text = self._tavily_web_search(query, purpose, max_results, domains)
            if not text and fallback_provider == "duckduckgo":
                logger.info("Tavily 搜索不可用，降级使用 DuckDuckGo。")
                text = self._free_web_search(query, purpose, max_results, domains)
        elif provider == "duckduckgo":
            text = self._free_web_search(query, purpose, max_results, domains)
        else:
            logger.warning(f"未知 Web Search provider: {provider}，降级使用 DuckDuckGo。")
            text = self._free_web_search(query, purpose, max_results, domains)

        # 统一处理：DuckDuckGo 无结果时返回的哨兵值视为空
        text = self._normalize_empty_search_result(text, min_result_chars)

        # 只缓存有效长度的结果，避免短结果污染后续轮次
        if text and len(text) > min_result_chars:
            self._write_web_search_cache(cache_key, text)
        elif text:
            logger.info(
                f"[WebSearch] skip_cache_write result_chars={len(text)} min={min_result_chars} "
                f"purpose={purpose or '-'} — 结果过短，不写入缓存。"
            )

        logger.info(
            f"[WebSearch] done provider={provider} fallback={fallback_provider} purpose={purpose or '-'} "
            f"result_chars={len(text or '')}"
        )
        trace_recorder.record(
            "web_search_done",
            {
                "provider": provider,
                "fallback_provider": fallback_provider,
                "purpose": purpose,
                "query": query,
                "max_results": max_results,
                "domains": domains or [],
                "result": text or "",
                "result_chars": len(text or ""),
            },
        )
        return text

    def _tavily_web_search(
        self,
        query: str,
        purpose: str = "",
        max_results: int = 5,
        domains: list[str] | None = None,
    ) -> str:
        """使用 Tavily Search API 做主力搜索。"""
        cfg = self.web_search_cfg or {}
        api_key = cfg.get("api_key") or os.getenv("TAVILY_API_KEY")
        if not api_key:
            logger.warning("Tavily 搜索已启用，但未配置 api_key 或 TAVILY_API_KEY。")
            return ""

        timeout = int(cfg.get("timeout", 30))
        search_depth = cfg.get("search_depth") or "basic"
        topic = cfg.get("topic") or "finance"
        endpoint = str(cfg.get("endpoint") or "https://api.tavily.com/search")
        max_results = min(max_results, int(cfg.get("max_results", max_results) or max_results), 20)

        payload = {
            "query": query,
            "topic": topic,
            "search_depth": search_depth,
            "max_results": max_results,
            "include_answer": bool(cfg.get("include_answer", False)),
            "include_raw_content": False,
            "include_images": False,
        }
        if domains:
            payload["include_domains"] = domains
        if cfg.get("time_range"):
            payload["time_range"] = cfg.get("time_range")
        if topic == "news":
            payload["days"] = int(cfg.get("days", 7))

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
            response.raise_for_status()
            return self._format_tavily_results(
                response.json(),
                query=query,
                purpose=purpose,
                max_results=max_results,
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Tavily 搜索请求失败: {e}")
            return ""
        except Exception as e:
            logger.warning(f"Tavily 搜索响应解析失败: {e}")
            return ""

    def _format_tavily_results(
        self,
        result: dict,
        query: str,
        purpose: str,
        max_results: int,
    ) -> str:
        rows = result.get("results") or []
        answer = str(result.get("answer") or "").strip()
        if not rows and not answer:
            return ""

        lines = [f"Tavily 搜索结果摘要（用途：{purpose or '投资研究辅助'}，查询：{query}）："]
        if answer:
            lines.append(f"综合摘要：{self._truncate_text(answer, 500)}")

        seen_urls = set()
        selected = []
        for item in rows:
            url = str(item.get("url") or "").strip()
            normalized_url = self._canonical_search_url(url)
            if not normalized_url or normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)
            selected.append(item)
            if len(selected) >= max_results:
                break

        for idx, item in enumerate(selected, start=1):
            title = str(item.get("title") or "").strip()
            url = str(item.get("url") or "").strip()
            content = self._truncate_text(str(item.get("content") or "").strip(), 360)
            published_date = str(item.get("published_date") or "").strip()
            date_suffix = f" | {published_date}" if published_date else ""
            lines.append(f"{idx}. {title}{date_suffix} | {url}\n   {content}")
        return "\n".join(lines)

    def _truncate_text(self, text: str, max_len: int) -> str:
        text = text.replace("\u200b", "").replace("\ufeff", "")
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) <= max_len:
            return text
        return text[:max_len].rstrip() + "..."

    def _normalize_empty_search_result(self, text: str | None, min_chars: int = 30) -> str:
        """将搜索引擎返回的"无结果"哨兵值统一为空字符串。

        DuckDuckGo 无结果时会返回 '免费搜索未检索到可靠新增信息。'（15 字符），
        这个值会干扰后续的最小长度判断和缓存写入逻辑。
        """
        if not text:
            return ""
        # DuckDuckGo 无结果哨兵值
        sentinel_phrases = ("免费搜索未检索到可靠新增信息",)
        for phrase in sentinel_phrases:
            if phrase in text and len(text) <= min_chars:
                return ""
        return text

    def _free_web_search(
        self,
        query: str,
        purpose: str = "",
        max_results: int = 5,
        domains: list[str] | None = None,
    ) -> str:
        """使用 DuckDuckGo HTML 搜索页做免费检索，不需要 API Key。"""
        cfg = self.web_search_cfg or {}
        timeout = int(cfg.get("timeout", 20))
        domain_expr = ""
        if domains:
            domain_expr = "(" + " OR ".join(f"site:{d}" for d in domains) + ")"
        full_query = f"{query} {domain_expr}".strip()
        url = f"https://duckduckgo.com/html/?q={quote_plus(full_query)}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        }

        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            results = self._parse_duckduckgo_html(response.text, max_results=max_results)
        except requests.exceptions.RequestException as e:
            logger.warning(f"免费 Web Search 请求失败: {e}")
            return ""
        except Exception as e:
            logger.warning(f"免费 Web Search 解析失败: {e}")
            return ""

        if not results:
            return "免费搜索未检索到可靠新增信息。"

        lines = [
            f"免费搜索结果摘要（用途：{purpose or '投资研究辅助'}，查询：{query}）："
        ]
        for idx, item in enumerate(results, start=1):
            snippet = item.get("snippet") or ""
            lines.append(
                f"{idx}. {item.get('title', '').strip()} | {item.get('url', '').strip()}\n"
                f"   {snippet.strip()}"
            )
        return "\n".join(lines)

    def _parse_duckduckgo_html(self, text: str, max_results: int = 5) -> list[dict]:
        """轻量解析 DuckDuckGo HTML 搜索结果，避免额外引入 bs4 依赖。"""
        blocks = re.findall(
            r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>.*?'
            r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>',
            text,
            flags=re.I | re.S,
        )
        results = []
        seen_urls = set()
        for raw_url, raw_title, raw_snippet in blocks:
            title = self._clean_html(raw_title)
            snippet = self._clean_html(raw_snippet)
            url = self._normalize_duckduckgo_url(raw_url)
            normalized_url = self._canonical_search_url(url)
            if self._is_search_ad_or_noise(url, title, snippet):
                continue
            if not normalized_url or normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)
            if title and url:
                results.append({"title": title, "url": url, "snippet": snippet})
            if len(results) >= max_results:
                break
        return results

    def _clean_html(self, text: str) -> str:
        text = re.sub(r"<.*?>", "", text, flags=re.S)
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    def _normalize_duckduckgo_url(self, raw_url: str) -> str:
        raw_url = html.unescape(raw_url)
        parsed = urlparse(raw_url)
        if parsed.query:
            target = parse_qs(parsed.query).get("uddg")
            if target:
                return unquote(target[0])
        return raw_url

    def _canonical_search_url(self, url: str) -> str:
        """Normalize result URLs so mirrors like www/non-www do not duplicate results."""
        if not url:
            return ""

        parsed = urlparse(url.strip())
        if not parsed.scheme or not parsed.netloc:
            return url.strip().rstrip("/")

        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]

        path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/")
        if not path:
            path = "/"

        ignored_query_prefixes = ("utm_",)
        ignored_query_keys = {"spm", "from", "share", "share_token"}
        query_items = []
        for key, values in sorted(parse_qs(parsed.query, keep_blank_values=True).items()):
            lowered = key.lower()
            if lowered in ignored_query_keys or lowered.startswith(ignored_query_prefixes):
                continue
            for value in values:
                query_items.append(f"{lowered}={value}")

        query = "&".join(query_items)
        return f"{netloc}{path}" + (f"?{query}" if query else "")

    def _is_search_ad_or_noise(self, url: str, title: str, snippet: str) -> bool:
        lowered = f"{url} {title} {snippet}".lower()
        if "duckduckgo.com/y.js" in lowered:
            return True
        if "ad_provider=" in lowered or "bing.com/aclick" in lowered:
            return True
        if not title or not url:
            return True
        return False

    def _web_search_cache_key(
        self,
        query: str,
        purpose: str,
        max_results: int,
        domains: list[str] | None,
    ) -> str:
        cfg = self.web_search_cfg or {}
        raw = json.dumps(
            {
                "query": query,
                "purpose": purpose,
                "max_results": max_results,
                "domains": domains or [],
                "provider": cfg.get("provider") or "tavily",
                "fallback_provider": cfg.get("fallback_provider") or "duckduckgo",
                "topic": cfg.get("topic") or "finance",
                "search_depth": cfg.get("search_depth") or "basic",
                "parser_version": 5,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _web_search_cache_path(self) -> Path:
        root = Path(__file__).resolve().parents[2]
        return root / "data" / "web_search_cache.json"

    def _read_web_search_cache(self, cache_key: str) -> str:
        cfg = self.web_search_cfg or {}
        ttl = int(cfg.get("cache_ttl_seconds", 21600))
        if ttl <= 0:
            return ""

        path = self._web_search_cache_path()
        if not path.exists():
            return ""

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            item = payload.get(cache_key)
            if not item:
                return ""
            if time.time() - float(item.get("ts", 0)) > ttl:
                return ""
            return str(item.get("text") or "")
        except Exception as e:
            logger.debug(f"读取 Web Search 缓存失败: {e}")
            return ""

    def _write_web_search_cache(self, cache_key: str, text: str) -> None:
        path = self._web_search_cache_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {}
            if path.exists():
                payload = json.loads(path.read_text(encoding="utf-8"))
            payload[cache_key] = {"ts": time.time(), "text": text}
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.debug(f"写入 Web Search 缓存失败: {e}")

    def fetch_stock_news(self, symbol: str, limit: int = 5) -> str:
        """获取个股近期新闻（新浪财经）。"""
        try:
            df = fetch_sina_stock_news(symbol, limit=limit)
            return format_news_for_prompt(df, limit=limit)
        except Exception as e:
            logger.warning(f"新浪个股新闻拉取失败 {symbol}: {e}")
            return ""

    def fetch_stock_announcements(self, symbol: str, limit: int = 10) -> str:
        """获取个股近期公告（东方财富免费接口），作为排雷和基本面分析的结构化数据源。"""
        try:
            df = fetch_eastmoney_announcements(symbol, limit=limit)
            return format_announcements_for_prompt(df, limit=limit)
        except Exception as e:
            logger.warning(f"东方财富个股公告拉取失败 {symbol}: {e}")
            return ""

    def fetch_market_sentiment(self) -> str:
        """Build a free market sentiment snapshot from local quotes and Sina boards."""
        try:
            return MarketSentiment().format_for_prompt()
        except Exception as e:
            logger.warning(f"Build market sentiment failed: {e}")
            return "Market sentiment snapshot is unavailable."

    def fetch_recent_kline(self, symbol: str, days: int = 10) -> str:
        """获取最近若干天的日K线数据，归纳为文本表格。"""
        local_text = self._fetch_local_kline_text(symbol, "daily", days)
        if local_text:
            return local_text

        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            
            if df is not None and not df.empty:
                df = df.tail(days)
                kline_text = "日期      收盘价   涨跌幅%   成交额\n"
                for _, row in df.iterrows():
                    d = row.get("日期", "")
                    c = row.get("收盘", 0)
                    pct = row.get("涨跌幅", 0)
                    amt = row.get("成交额", 0) / 100000000
                    kline_text += f"{d}  {c}    {pct}%    {amt:.2f}亿\n"
                return kline_text
            return "无法获取K线。"
        except Exception as e:
            logger.warning(f"获取K线数据失败 {symbol}: {e}")
            return "K线数据拉取异常。"

    def fetch_multi_period_kline(self, symbol: str) -> str:
        """获取日/周/月多周期K线摘要，优先使用本地数据湖。"""
        sections = []
        period_specs = [("daily", 12, "日线"), ("weekly", 12, "周线"), ("monthly", 8, "月线")]

        for period, limit, title in period_specs:
            text = self._fetch_local_kline_text(symbol, period, limit, title=title)
            if text:
                sections.append(text)

        if sections:
            return "\n\n".join(sections)

        return self.fetch_recent_kline(symbol, days=12)

    def _fetch_local_kline_text(self, symbol: str, period: str, limit: int, title: str = "日线") -> str:
        """从本地 market_bars 表读取 K 线摘要。"""
        try:
            db = StockDatabase()
            query = """
                SELECT trade_date, close, volume, amount
                FROM market_bars
                WHERE code = ? AND period = ?
                ORDER BY trade_date DESC
                LIMIT ?
            """
            df = db.query_to_dataframe(query, (str(symbol).zfill(6), period, limit))
            if df is None or df.empty:
                return ""

            df = df.sort_values("trade_date")
            kline_text = f"{title}K线  日期      收盘价   涨跌幅%   成交额\n"
            prev_close = None
            for _, row in df.iterrows():
                close = row.get("close", 0)
                pct = ""
                if prev_close and prev_close != 0:
                    pct = f"{(close / prev_close - 1) * 100:.2f}%"
                prev_close = close

                amount = row.get("amount", 0)
                amount_text = "-"
                if amount and amount == amount:
                    amount_text = f"{amount / 100000000:.2f}亿"

                kline_text += f"{row.get('trade_date', '')}  {close:.2f}    {pct or '-'}    {amount_text}\n"
            return kline_text
        except Exception as e:
            logger.warning(f"读取本地K线失败 {symbol}/{period}: {e}")
            return ""

# 提供模块级单例供其他Agent共同使用
tools = AgentTools()
