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
    fetch_eastmoney_announcement_content,
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
                logger.info("Tavily 搜索未返回可用结果，降级使用 DuckDuckGo。")
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
            result = response.json()
            text = self._format_tavily_results(
                result,
                query=query,
                purpose=purpose,
                max_results=max_results,
            )
            if not text:
                logger.warning(
                    "Tavily 搜索成功但结果为空: status={} query_chars={} domains={}",
                    response.status_code,
                    len(query or ""),
                    ",".join(domains or []) or "-",
                )
            return text
        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            body = getattr(getattr(e, "response", None), "text", "") or ""
            logger.warning(
                "Tavily 搜索请求失败: status={} error={} body={}",
                status or "-",
                e,
                body[:300],
            )
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

    def fetch_stock_news_details(self, news_text: str, max_articles: int = 3, max_chars_per_article: int = 700) -> str:
        """按新闻链接抓取正文摘要，供资讯风控 LLM 验证标题背后的实质内容。"""
        urls = self._extract_urls(news_text)
        if not urls:
            return ""

        sections = []
        for index, url in enumerate(urls[: max(0, int(max_articles or 0))], start=1):
            article = self._fetch_article_text(url, max_chars=max_chars_per_article)
            if not article:
                continue
            sections.append(f"{index}. {url}\n{article}")
        return "\n\n".join(sections)

    def _extract_urls(self, text: str) -> list[str]:
        """从标题列表中抽取去重后的 http/https 链接。"""
        urls = []
        seen = set()
        for match in re.finditer(r"https?://[^\s)）]+", text or ""):
            url = match.group(0).strip()
            normalized = self._canonical_search_url(url)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            urls.append(url)
        return urls

    def _fetch_article_text(self, url: str, max_chars: int = 700) -> str:
        """拉取并清洗单篇新闻正文，失败时返回空字符串以保持主流程可用。"""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Referer": "https://finance.sina.com.cn/",
        }
        try:
            response = requests.get(url, headers=headers, timeout=12)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding
            text = self._extract_article_body(response.text)
            return self._truncate_text(text, max_chars)
        except requests.exceptions.RequestException as e:
            logger.warning(f"新闻正文抓取失败 {url}: {e}")
            return ""
        except Exception as e:
            logger.warning(f"新闻正文解析失败 {url}: {e}")
            return ""

    def _extract_article_body(self, html_text: str) -> str:
        """从常见财经新闻页提取主体文本，优先取 article 或正文容器。"""
        text = html_text or ""
        paragraph_text = self._extract_article_paragraphs(text)
        if len(paragraph_text) >= 120:
            return paragraph_text

        candidates = []
        patterns = [
            r"<article[^>]*>(.*?)</article>",
            r"<div[^>]+id=[\"']artibody[\"'][^>]*>(.*?)</div>",
            r"<div[^>]+class=[\"'][^\"']*(?:article|content|main-content)[^\"']*[\"'][^>]*>(.*?)</div>",
        ]
        for pattern in patterns:
            candidates.extend(re.findall(pattern, text, flags=re.I | re.S))

        body = max(candidates, key=len) if candidates else text
        body = re.sub(r"(?is)<script.*?</script>|<style.*?</style>|<noscript.*?</noscript>", " ", body)
        body = re.sub(r"(?is)<br\s*/?>|</p>|</div>|</h\d>", "\n", body)
        body = self._clean_html(body)
        body = re.sub(r"\s+", " ", body).strip()

        noise_patterns = [
            r"责任编辑[:：].*$",
            r"新浪财经.*?讯",
            r"打开APP.*?$",
        ]
        for pattern in noise_patterns:
            body = re.sub(pattern, "", body, flags=re.I).strip()
        if self._looks_like_navigation_text(body):
            meta_text = self._extract_meta_description(text)
            if meta_text:
                return meta_text
        return body

    def _extract_article_paragraphs(self, html_text: str) -> str:
        """抽取页面中的正文段落，过滤明显的导航、版权和工具栏文案。"""
        paragraphs = []
        for raw in re.findall(r"<p[^>]*>(.*?)</p>", html_text or "", flags=re.I | re.S):
            item = self._clean_html(raw)
            item = re.sub(r"\s+", " ", item).strip()
            if self._is_article_noise_line(item):
                continue
            paragraphs.append(item)
        return " ".join(paragraphs)

    def _extract_meta_description(self, html_text: str) -> str:
        """正文容器不可用时，退回使用页面 description 摘要。"""
        patterns = [
            r"<meta[^>]+name=[\"']description[\"'][^>]+content=[\"']([^\"']+)[\"']",
            r"<meta[^>]+content=[\"']([^\"']+)[\"'][^>]+name=[\"']description[\"']",
            r"<meta[^>]+property=[\"']og:description[\"'][^>]+content=[\"']([^\"']+)[\"']",
        ]
        for pattern in patterns:
            match = re.search(pattern, html_text or "", flags=re.I | re.S)
            if match:
                value = self._clean_html(match.group(1))
                if not self._is_article_noise_line(value):
                    return value
        return ""

    def _is_article_noise_line(self, text: str) -> bool:
        """判断单行文本是否是导航、版权或过短噪声。"""
        if not text or len(text) < 12:
            return True
        noise_keywords = [
            "关于头条",
            "如何入驻",
            "发稿平台",
            "奖励机制",
            "版权声明",
            "用户协议",
            "帮助中心",
            "财经头条作者库",
            "股市直播",
            "图文直播间",
            "视频直播间",
            "责任编辑",
            "打开APP",
        ]
        if any(keyword in text for keyword in noise_keywords):
            return True
        chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", text))
        return chinese_chars < max(6, len(text) * 0.2)

    def _looks_like_navigation_text(self, text: str) -> bool:
        """识别整段内容是否主要由站点导航组成。"""
        if not text:
            return True
        noise_hits = sum(
            keyword in text
            for keyword in ["关于头条", "如何入驻", "发稿平台", "版权声明", "股市直播", "直播间"]
        )
        return noise_hits >= 2 or len(text) < 80

    def fetch_stock_announcements(self, symbol: str, limit: int = 10) -> str:
        """获取个股近期公告（东方财富免费接口），作为排雷和基本面分析的结构化数据源。"""
        try:
            df = fetch_eastmoney_announcements(symbol, limit=limit)
            return format_announcements_for_prompt(df, limit=limit)
        except Exception as e:
            logger.warning(f"东方财富个股公告拉取失败 {symbol}: {e}")
            return ""

    def fetch_stock_announcement_details(
        self,
        symbol: str,
        limit: int = 10,
        max_announcements: int = 3,
        max_chars_per_announcement: int = 900,
        financial_reports_covered: bool = True,
    ) -> str:
        """按信息价值筛选公告正文，避免把低价值制度类公告全部塞进 Prompt。"""
        try:
            df = fetch_eastmoney_announcements(symbol, limit=limit)
        except Exception as e:
            logger.warning(f"东方财富重点公告列表拉取失败 {symbol}: {e}")
            return ""
        if df is None or df.empty:
            return ""

        selected = []
        for _, row in df.iterrows():
            if not self._should_fetch_announcement_detail(row, financial_reports_covered=financial_reports_covered):
                continue
            selected.append(row)
            if len(selected) >= max(0, int(max_announcements or 0)):
                break

        sections = []
        for index, row in enumerate(selected, start=1):
            art_code = str(row.get("art_code") or "").strip()
            if not art_code:
                continue
            detail = fetch_eastmoney_announcement_content(art_code)
            content = self._clean_announcement_text(str(detail.get("content") or ""))
            if not content:
                continue
            title = str(detail.get("title") or row.get("title") or "").strip()
            date = str(detail.get("date") or row.get("date") or "").strip()
            attach_url = str(detail.get("attach_url") or row.get("url") or "").strip()
            snippet = self._truncate_text(content, max_chars_per_announcement)
            header = f"{index}. {date} {title}".strip()
            if attach_url:
                header = f"{header} ({attach_url})"
            sections.append(f"{header}\n{snippet}")
        return "\n\n".join(sections)

    def _should_fetch_announcement_detail(self, row, financial_reports_covered: bool = True) -> bool:
        """判断公告正文是否值得补充给 Agent 阅读。"""
        title = str(row.get("title") or "")
        categories = str(row.get("categories") or "")
        text = f"{title} {categories}"

        procedural_keywords = ["前十名股东", "无限售条件股东", "持股情况"]
        if any(keyword in text for keyword in procedural_keywords) and "减持" not in text:
            return False

        high_value_keywords = [
            "月度经营",
            "运营数据",
            "回购",
            "分配预案",
            "利润分配",
            "借贷",
            "专项贷款",
            "资金占用",
            "关联交易",
            "担保",
            "诉讼",
            "仲裁",
            "处罚",
            "问询函",
            "监管函",
            "立案",
            "违规",
            "审计意见",
            "保留意见",
            "非标",
            "减持",
            "质押",
            "解禁",
            "债务",
        ]
        if any(keyword in text for keyword in high_value_keywords):
            return True

        financial_keywords = ["年度报告摘要", "季度报告", "业绩预告", "业绩快报"]
        financial_risk_keywords = ["修正", "预亏", "预减", "下修", "大幅下降", "亏损"]
        if any(keyword in text for keyword in financial_keywords):
            return (not financial_reports_covered) or any(keyword in text for keyword in financial_risk_keywords)

        low_value_keywords = ["管理办法", "制度", "薪酬", "续聘", "述职报告", "履职情况", "ESG", "环境、社会"]
        if any(keyword in text for keyword in low_value_keywords):
            return False

        return False

    def _clean_announcement_text(self, text: str) -> str:
        """清洗公告正文中的页眉、免责声明和过多空白。"""
        text = (text or "").replace("\r", "\n").replace("\u3000", " ")
        lines = []
        for raw_line in text.splitlines():
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            if any(
                noise in line
                for noise in [
                    "本公司董事会及全体董事保证本公告内容不存在任何虚假记载",
                    "证券代码：",
                    "证券简称：",
                    "公告编号：",
                ]
            ):
                continue
            lines.append(line)
        return "\n".join(lines)

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
