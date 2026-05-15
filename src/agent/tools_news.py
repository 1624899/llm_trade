"""AgentTools 新闻与公告抓取能力（Mixin）。

从 tools.py 拆分：个股新闻抓取、公告抓取、正文提取等方法。
"""

import re
import requests
from loguru import logger


class NewsFetchMixin:
    """新闻抓取、公告抓取、正文提取相关方法。"""

    def fetch_stock_news(self, symbol: str, limit: int = 5) -> str:
        """获取个股近期新闻、财联社快讯和全球资讯。"""
        from src.market_extras import fetch_stock_news_bundle, format_news_for_prompt

        try:
            df = fetch_stock_news_bundle(symbol, stock_limit=limit, global_limit=limit)
            return format_news_for_prompt(df, limit=limit)
        except Exception as e:
            logger.warning(f"AKShare 新闻三源拉取失败 {symbol}: {e}")
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
        """获取个股近期公告（巨潮 cninfo），作为排雷和基本面分析的结构化数据源。"""
        from src.agent.tools import fetch_cninfo_announcements, format_announcements_for_prompt  # late import for patch compat
        try:
            df = fetch_cninfo_announcements(symbol, limit=limit)
            return format_announcements_for_prompt(df, limit=limit)
        except Exception as e:
            logger.warning(f"巨潮个股公告拉取失败 {symbol}: {e}")
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
        from src.agent.tools import fetch_cninfo_announcements, fetch_cninfo_announcement_content  # late import for patch compat
        try:
            df = fetch_cninfo_announcements(symbol, limit=limit)
        except Exception as e:
            logger.warning(f"巨潮重点公告列表拉取失败 {symbol}: {e}")
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
            announcement_url = str(row.get("url") or row.get("pdf_url") or "").strip()
            if not announcement_url:
                continue
            detail = fetch_cninfo_announcement_content(announcement_url)
            content = self._clean_announcement_text(str(detail.get("content") or ""))
            if not content and detail.get("pdf_url"):
                content = "PDF 公告正文未本地解析，请结合公告标题、分类和链接核验。"
            if not content:
                continue
            title = str(detail.get("title") or row.get("title") or "").strip()
            date = str(detail.get("date") or row.get("date") or "").strip()
            attach_url = str(detail.get("attach_url") or row.get("url") or row.get("pdf_url") or "").strip()
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
