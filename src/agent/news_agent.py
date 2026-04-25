"""
资讯防雷智能体 (News Risk Agent)

职责：
1. 读取候选股近期免费新闻和网页公告摘要。
2. 用确定性关键词规则识别减持、质押、解禁、监管处罚、诉讼等雷点。
3. 输出结构化风险等级，供 DecisionAgent 在 LLM 前执行硬风控。
"""

import json
import re
from typing import Any, Dict, Optional

from loguru import logger

from src.agent.tools import tools


RISK_KEYWORDS = {
    "block": ["立案调查", "行政处罚", "重大诉讼", "业绩预告修正", "重组终止", "定增失败"],
    "high": ["减持", "大股东减持", "控股股东减持", "质押", "高比例质押", "问询函", "监管函", "解禁"],
    "medium": ["留置", "被查", "诉讼", "仲裁", "处罚", "业绩预告", "商誉减值", "债务逾期"],
}


class NewsRiskAgent:
    def analyze(self, code: str, name: str, macro_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """对单只股票进行近期资讯和防雷预警分析。"""
        logger.info(f"[News Risk Agent] 正在检查 {name}({code}) 近期资讯雷点...")

        # 数据源 1：新浪财经个股新闻
        news_info = tools.fetch_stock_news(code, limit=8)
        # 数据源 2：东方财富个股公告（免费结构化接口，替代 Tavily/DDG 搜索）
        announcement_info = tools.fetch_stock_announcements(code, limit=10)

        macro_text = json.dumps(macro_context or {}, ensure_ascii=False, indent=2)
        # 关键词排雷同时扫描新闻和公告
        combined_text = f"{news_info}\n{announcement_info}"
        assessment = self.assess_keyword_risk(combined_text, code=code, name=name)

        system_prompt = """
你是一位 A 股资讯风控分析师，职责是防雷，而不是寻找买入理由。

请阅读候选股近期新闻和公告标题，只判断是否存在会影响推荐结论的潜在风险。重点检查：
1. 股东或高管减持、清仓式减持、质押风险。
2. 监管函、问询函、立案调查、行政处罚、财务造假。
3. 重大诉讼、债务违约、担保风险、商誉减值。
4. 重组终止、定增失败、业绩预告大幅下修。
5. 突发负面舆情、核心订单取消、行业政策利空。

请结合宏观环境判断风险是否需要放大处理。

请用 150-250 字输出结论，按以下结构组织：
1. **资讯扫描**：近期新闻和公告中发现的关键信息点（列出 2-3 条最重要的）。
2. **风险评估**：逐条分析上述信息对投资安全的影响程度。
3. **宏观放大效应**：当前宏观环境是否会放大上述风险（如熊市中减持影响更大）。
4. **风险等级**：【高危 / 中性偏谨慎 / 未见明显雷点】
5. **建议**：是否允许进入最终推荐 【允许 / 谨慎 / 禁止】，并说明理由。
"""

        user_prompt = f"""
股票：{name}({code})

宏观上下文：
{macro_text}

近期新闻资讯（新浪财经）：
{news_info or "新浪财经未获取到该股近期新闻。"}

近期公告（东方财富）：
{announcement_info or "东方财富未获取到该股近期公告。"}
"""

        report = tools.call_llm(system_prompt, user_prompt, temperature=0.1)
        if not report:
            report = "资讯防雷 LLM 检查暂不可用，已使用关键词规则结果。"

        assessment["llm_report"] = report
        assessment["raw_news"] = news_info
        assessment["announcement_info"] = announcement_info
        assessment["summary"] = self._format_structured_summary(assessment)
        return assessment

    def assess_keyword_risk(self, text: str, code: str = "", name: str = "") -> Dict[str, Any]:
        """Deterministic keyword-based risk gate used before any LLM decision."""
        text = self._filter_risk_text(text or "", code=code, name=name)
        matched = []
        for level, keywords in RISK_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    matched.append({"keyword": keyword, "level": level})

        if any(item["level"] == "block" for item in matched):
            risk_level = "block"
        elif any(item["level"] == "high" for item in matched):
            risk_level = "high"
        elif matched:
            risk_level = "medium"
        else:
            risk_level = "low"

        evidence = self._extract_evidence_lines(text, [item["keyword"] for item in matched])
        hard_exclude = risk_level in {"block", "high"}
        action = "hard_exclude" if hard_exclude else ("watch" if risk_level == "medium" else "pass")
        return {
            "code": str(code).zfill(6) if code else "",
            "name": name,
            "risk_level": risk_level,
            "action": action,
            "hard_exclude": hard_exclude,
            "matched_keywords": matched,
            "evidence": evidence,
        }

    def _filter_risk_text(self, text: str, code: str = "", name: str = "") -> str:
        """Keep direct news plus candidate-related search evidence; skip query boilerplate."""
        code = str(code or "").zfill(6) if code else ""
        name = str(name or "").strip()
        filtered_lines = []
        in_search_context = False
        for raw_line in re.split(r"[\r\n]+", text or ""):
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            if "搜索结果摘要" in line:
                in_search_context = True
                continue
            if "查询：" in line or line.startswith("No recent stock news found"):
                continue
            if in_search_context:
                if (code and code in line) or (name and name in line):
                    filtered_lines.append(line)
                continue
            filtered_lines.append(line)
        return "\n".join(filtered_lines)

    def _extract_evidence_lines(self, text: str, keywords: list[str], limit: int = 5) -> list[str]:
        if not keywords:
            return []
        lines = []
        for raw_line in re.split(r"[\r\n]+", text):
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            if any(keyword in line for keyword in keywords):
                lines.append(line[:260])
            if len(lines) >= limit:
                break
        return lines

    def _format_structured_summary(self, assessment: Dict[str, Any]) -> str:
        keywords = ", ".join(item["keyword"] for item in assessment.get("matched_keywords", [])) or "无"
        evidence = "；".join(assessment.get("evidence", [])[:2]) or "未发现明确负面关键词"
        return (
            f"资讯风险等级：{assessment.get('risk_level')}；"
            f"动作：{assessment.get('action')}；"
            f"命中关键词：{keywords}；证据：{evidence}"
        )
