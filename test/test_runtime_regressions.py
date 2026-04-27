import os
import importlib
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import main
from src.agent.coordinator import AgentCoordinator
from src.agent.decision_agent import DecisionAgent
from src.agent.macro_agent import MACRO_CONTEXT_SCHEMA_FIELDS, MacroAgent
from src.agent.news_agent import NewsRiskAgent
from src.agent.quick_filter_agent import QuickFilterAgent
from src.agent.reflection_agent import ReflectionAgent
from src.agent.trading_agent import TradingAgent
from src.agent.tools import AgentTools
from src.market_extras import fetch_sina_board_list, fetch_sina_stock_news
from src.market_sentiment import MarketSentiment
from src.stock_universe import StockUniverse
from src.theme_scorer import ThemeScorer


class MainEntryTests(unittest.TestCase):
    def test_main_module_imports_without_sys_path_hack(self):
        module = importlib.import_module("main")
        self.assertTrue(hasattr(module, "main"))

    def test_setup_logger_reconfigures_stdout_when_supported(self):
        stdout_mock = MagicMock()

        with patch.object(main.sys, "stdout", stdout_mock), patch.object(main.logger, "remove"), patch.object(main.logger, "add"):
            main.setup_logger()

        stdout_mock.reconfigure.assert_called_once_with(encoding="utf-8", errors="replace")

    @patch("main.DataPipeline")
    def test_sync_uses_run_all(self, mock_pipeline_cls):
        mock_pipeline = mock_pipeline_cls.return_value

        with patch.object(sys, "argv", ["main.py", "--sync"]):
            main.main()

        mock_pipeline.run_all.assert_called_once_with()

    @patch("main.AgentCoordinator")
    def test_analyze_uses_targeted_analysis(self, mock_coordinator_cls):
        mock_coordinator = mock_coordinator_cls.return_value
        mock_coordinator.run_targeted_analysis.return_value = "targeted report"

        with patch.object(sys, "argv", ["main.py", "--analyze", "600519", "000001"]):
            main.main()

        mock_coordinator.run_targeted_analysis.assert_called_once_with(["600519", "000001"])

    @patch("main.AgentCoordinator")
    def test_trade_uses_trading_workflow(self, mock_coordinator_cls):
        mock_coordinator = mock_coordinator_cls.return_value

        with patch.object(sys, "argv", ["main.py", "--trade"]):
            main.main()

        mock_coordinator.run_trading_workflow.assert_called_once_with()

    def test_trade_replenishes_empty_watchlist_from_pick_when_no_positions(self):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)
        coordinator.watchlist = MagicMock()
        coordinator.trading_account = MagicMock()
        coordinator.macro_agent = MagicMock()
        coordinator.trading_agent = MagicMock()
        coordinator.run_picking_workflow = MagicMock()
        coordinator.watchlist.list_active.side_effect = [
            [],
            [{"code": "000001", "name": "sample"}],
            [{"code": "000001", "name": "sample"}],
        ]
        coordinator.watchlist.refresh_prices = MagicMock()
        coordinator.trading_account.refresh_positions.return_value = {"cash": 10000}
        coordinator.trading_account.list_open_positions.return_value = []
        coordinator.macro_agent.analyze_macro_environment.return_value = {}
        coordinator._build_exit_signals = MagicMock(return_value={})
        coordinator.trading_agent.decide.return_value = []
        coordinator.trading_account.execute_decisions.return_value = []
        coordinator.trading_account.format_report.return_value = "report"

        result = coordinator.run_trading_workflow()

        self.assertEqual(result, "report")
        coordinator.run_picking_workflow.assert_called_once_with(max_candidates=10)
        coordinator.watchlist.apply_trading_decisions.assert_called_once_with([])

    def test_trade_replenishes_empty_watchlist_from_position_analysis(self):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)
        coordinator.watchlist = MagicMock()
        coordinator.trading_account = MagicMock()
        coordinator.macro_agent = MagicMock()
        coordinator.trading_agent = MagicMock()
        coordinator.run_targeted_analysis = MagicMock()
        coordinator.watchlist.list_active.side_effect = [
            [],
            [{"code": "000001", "name": "sample"}],
            [{"code": "000001", "name": "sample"}],
        ]
        coordinator.watchlist.refresh_prices = MagicMock()
        coordinator.trading_account.refresh_positions.return_value = {"cash": 9000}
        coordinator.trading_account.list_open_positions.return_value = [{"code": "000001", "name": "sample"}]
        coordinator.macro_agent.analyze_macro_environment.return_value = {}
        coordinator._build_exit_signals = MagicMock(return_value={})
        coordinator.trading_agent.decide.return_value = []
        coordinator.trading_account.execute_decisions.return_value = []
        coordinator.trading_account.format_report.return_value = "report"

        coordinator.run_trading_workflow()

        coordinator.run_targeted_analysis.assert_called_once_with(
            ["000001"],
            update_watchlist=True,
            watchlist_source="trade_position_refresh",
        )

    def test_trade_replenishes_missing_position_analysis_even_when_watchlist_not_empty(self):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)
        coordinator.watchlist = MagicMock()
        coordinator.trading_account = MagicMock()
        coordinator.macro_agent = MagicMock()
        coordinator.trading_agent = MagicMock()
        coordinator.run_targeted_analysis = MagicMock()
        coordinator.watchlist.list_active.side_effect = [
            [{"code": "002142", "name": "watch only"}],
            [{"code": "002142", "name": "watch only"}, {"code": "601021", "name": "held"}],
            [{"code": "002142", "name": "watch only"}, {"code": "601021", "name": "held"}],
        ]
        coordinator.watchlist.refresh_prices = MagicMock()
        coordinator.trading_account.refresh_positions.return_value = {"cash": 1136}
        coordinator.trading_account.list_open_positions.return_value = [{"code": "601021", "name": "held"}]
        coordinator.macro_agent.analyze_macro_environment.return_value = {}
        coordinator._build_exit_signals = MagicMock(return_value={})
        coordinator.trading_agent.decide.return_value = []
        coordinator.trading_account.execute_decisions.return_value = []
        coordinator.trading_account.format_report.return_value = "report"

        coordinator.run_trading_workflow()

        coordinator.run_targeted_analysis.assert_called_once_with(
            ["601021"],
            update_watchlist=True,
            watchlist_source="trade_position_refresh",
        )


class LegacyCleanupTests(unittest.TestCase):
    def test_removed_legacy_etf_modules_are_absent(self):
        removed_files = [
            os.path.join(PROJECT_ROOT, "src", "prompt_generator.py"),
            os.path.join(PROJECT_ROOT, "src", "llm_client.py"),
            os.path.join(PROJECT_ROOT, "src", "data_cache_manager.py"),
        ]

        for file_path in removed_files:
            self.assertFalse(os.path.exists(file_path), msg=f"Legacy file still exists: {file_path}")

    def test_user_facing_config_has_no_legacy_etf_trading_semantics(self):
        checked_files = [
            os.path.join(PROJECT_ROOT, "USAGE.md"),
            os.path.join(PROJECT_ROOT, "config", "config.yaml"),
            os.path.join(PROJECT_ROOT, "config", "config_example.yaml"),
        ]
        forbidden_terms = ["etf_trading", "enable_auto_trade", "config/etf_list.yaml", "sharpe_ratio_api.py"]

        for file_path in checked_files:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read().lower()
            for term in forbidden_terms:
                self.assertNotIn(term.lower(), content, msg=f"{term} still appears in {file_path}")

    def test_runtime_config_uses_env_placeholders_for_secrets(self):
        with open(os.path.join(PROJECT_ROOT, "config", "config.yaml"), "r", encoding="utf-8") as f:
            content = f.read()

        self.assertNotRegex(content, r"sk-[A-Za-z0-9]{16,}")
        self.assertNotRegex(content, r"tvly-[A-Za-z0-9_-]{16,}")
        self.assertIn("env:DEEPSEEK_API_KEY", content)
        self.assertIn("env:TAVILY_API_KEY", content)

    def test_requirements_do_not_add_paid_point_data_source(self):
        with open(os.path.join(PROJECT_ROOT, "requirements.txt"), "r", encoding="utf-8") as f:
            requirements = f.read().lower()

        self.assertNotIn("tushare", requirements)

    @patch.dict(os.environ, {"UNIT_TEST_SECRET": "resolved-secret"}, clear=False)
    def test_agent_tools_resolves_env_config_placeholders(self):
        tool = AgentTools.__new__(AgentTools)

        resolved = tool._resolve_env_config({"api_key": "env:UNIT_TEST_SECRET", "model": "demo"})

        self.assertEqual(resolved["api_key"], "resolved-secret")
        self.assertEqual(resolved["model"], "demo")


class ReflectionAgentTests(unittest.TestCase):
    @patch("src.agent.reflection_agent.tools.call_llm", return_value="风控铁律：破位缩量下跌时不要抄底")
    @patch("src.agent.reflection_agent.tools.fetch_recent_kline", return_value="mock kline")
    @patch.object(ReflectionAgent, "_save_rules")
    @patch("src.agent.reflection_agent.StockDatabase")
    def test_generate_reflection_saves_rules_without_name_error(
        self,
        mock_db_cls,
        mock_save_rules,
        _mock_fetch_recent_kline,
        _mock_call_llm,
    ):
        mock_db = mock_db_cls.return_value
        mock_db.db_path = os.path.join("data", "stock_lake.db")
        mock_db.query_to_dataframe.return_value = pd.DataFrame(
            [
                {
                    "code": "600000",
                    "name": "浦发银行",
                    "recommend_price": 10.0,
                    "current_price": 9.2,
                    "return_pct": -8.0,
                    "recommend_reason": "估值便宜且企稳",
                }
            ]
        )

        agent = ReflectionAgent()
        agent.generate_reflection_for_failures(threshold_pct=-3.0)

        mock_save_rules.assert_called_once()
        saved_rules = mock_save_rules.call_args.args[0]
        self.assertEqual(len(saved_rules), 1)
        self.assertIn("浦发银行", saved_rules[0])

    def test_save_rules_writes_rules_book_file(self):
        rules_dir = os.path.join(PROJECT_ROOT, ".test_tmp")
        os.makedirs(rules_dir, exist_ok=True)
        rules_path = os.path.join(rules_dir, f"rules_{time.time_ns()}.txt")
        agent = ReflectionAgent.__new__(ReflectionAgent)
        agent.rules_path = rules_path

        try:
            agent._save_rules(["[2026-04-25] unit risk rule"])
            with open(rules_path, "r", encoding="utf-8") as file:
                content = file.read()
        finally:
            if os.path.exists(rules_path):
                os.remove(rules_path)

        self.assertIn("unit risk rule", content)


class StockUniverseTests(unittest.TestCase):
    def test_rate_limit_updates_last_request_time(self):
        universe = StockUniverse(config={})
        universe._request_interval = 0

        before = universe._last_request_time
        universe._rate_limit()

        self.assertGreaterEqual(universe._last_request_time, before)

    def test_memory_cache_records_age(self):
        universe = StockUniverse(config={})
        sample_df = pd.DataFrame([{"code": "000001", "name": "平安银行", "price": 10.5}])

        universe._update_memory_cache(sample_df)
        cache_info = universe.get_cache_info()

        self.assertTrue(cache_info["all_stocks_cached"])
        self.assertEqual(cache_info["all_stocks_count"], 1)
        self.assertNotEqual(cache_info["all_stocks_age"], "N/A")


class FreeMarketDataTests(unittest.TestCase):
    @patch("src.market_extras.requests.get")
    def test_sina_stock_news_parser(self, mock_get):
        response = MagicMock()
        response.text = (
            "2026-04-25&nbsp;09:02&nbsp;&nbsp;"
            "<a target='_blank' href='https://finance.sina.com.cn/a.shtml'>Headline A</a><br>"
        )
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        df = fetch_sina_stock_news("600519", limit=1)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["datetime"], "2026-04-25 09:02")
        self.assertEqual(df.iloc[0]["title"], "Headline A")

    @patch("src.market_extras.requests.get")
    def test_sina_board_list_parser(self, mock_get):
        response = MagicMock()
        response.text = (
            'var S_Finance_bankuai_class = {"gn_demo":'
            '"gn_demo,Demo Board,2,10.0,0.5,5.0,1000,2000,sz000001,10.0,12.0,1.0,Leader"};'
        )
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        df = fetch_sina_board_list("concept")

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["node"], "gn_demo")
        self.assertEqual(df.iloc[0]["name"], "Demo Board")
        self.assertEqual(df.iloc[0]["change_pct"], 5.0)

    @patch("src.market_sentiment.fetch_sina_board_list")
    def test_market_sentiment_uses_local_quotes_and_free_boards(self, mock_boards):
        mock_boards.return_value = pd.DataFrame(
            [{"name": "Demo Board", "change_pct": 3.0, "leader_name": "Leader", "leader_change_pct": 10.0}]
        )
        sentiment = MarketSentiment(db=MagicMock())
        sentiment.db.query_to_dataframe.return_value = pd.DataFrame(
            [{"change_pct": 1.0}, {"change_pct": 10.0}, {"change_pct": -2.0}]
        )

        snapshot = sentiment.build_snapshot(top_n=1)

        self.assertIn(snapshot["label"], {"risk-on", "positive", "neutral", "weak", "risk-off"})
        self.assertEqual(snapshot["market"]["up_count"], 2)
        self.assertEqual(snapshot["hot_concepts"][0]["name"], "Demo Board")


class ThemeScorerTests(unittest.TestCase):
    @patch("src.theme_scorer.fetch_sina_board_constituents")
    @patch("src.theme_scorer.fetch_sina_board_list")
    def test_theme_scorer_adds_bonus_from_hot_sina_boards(self, mock_boards, mock_members):
        def board_side_effect(kind):
            if kind == "industry":
                return pd.DataFrame(
                    [
                        {
                            "node": "hy_ai",
                            "name": "人工智能",
                            "change_pct": 4.0,
                            "amount": 3000000000,
                            "leader_name": "leader",
                            "leader_change_pct": 10.0,
                        }
                    ]
                )
            return pd.DataFrame(
                [
                    {
                        "node": "gn_robot",
                        "name": "机器人",
                        "change_pct": 3.0,
                        "amount": 2000000000,
                        "leader_name": "leader",
                        "leader_change_pct": 8.0,
                    }
                ]
            )

        mock_boards.side_effect = board_side_effect
        mock_members.return_value = pd.DataFrame(
            [
                {"code": "000001", "change_pct": 5.0, "amount": 100000000},
                {"code": "000002", "change_pct": -1.0, "amount": 80000000},
            ]
        )
        candidates = [
            {
                "code": "000001",
                "name": "theme hit",
                "industry": "人工智能",
                "technical_score": 50.0,
                "key_metrics": {},
            },
            {
                "code": "600000",
                "name": "theme miss",
                "industry": "银行",
                "technical_score": 50.0,
                "key_metrics": {},
            },
        ]

        scored = ThemeScorer(max_bonus=12).score_candidates(candidates)

        self.assertGreater(scored[0]["theme_score"], 0)
        self.assertGreater(scored[0]["technical_score"], scored[1]["technical_score"])
        self.assertTrue(scored[0]["matched_themes"])
        self.assertEqual(scored[1]["theme_score"], 0.0)

    @patch("src.theme_scorer.fetch_sina_board_list", side_effect=RuntimeError("network down"))
    def test_theme_scorer_keeps_candidates_when_board_data_unavailable(self, _mock_boards):
        candidates = [{"code": "000001", "technical_score": 50.0, "key_metrics": {}}]

        scored = ThemeScorer().score_candidates(candidates)

        self.assertEqual(scored[0]["technical_score"], 50.0)
        self.assertEqual(scored[0]["theme_score"], 0.0)
        self.assertIn("主题数据不可用", scored[0]["theme_reason"])


class WebSearchTests(unittest.TestCase):
    def _build_tool(self, config=None):
        tool = AgentTools.__new__(AgentTools)
        tool.llm_cfg = {}
        tool.web_search_cfg = {
            "enabled": True,
            "provider": "tavily",
            "fallback_provider": "duckduckgo",
            "api_key": "test-key",
            "endpoint": "https://api.tavily.com/search",
            "topic": "finance",
            "search_depth": "basic",
            "include_answer": False,
            "cache_ttl_seconds": 0,
            "timeout": 3,
            "max_results": 5,
        }
        if config:
            tool.web_search_cfg.update(config)
        return tool

    @patch("src.agent.tools.requests.post")
    def test_tavily_search_formats_results_and_passes_domains(self, mock_post):
        response = MagicMock()
        response.json.return_value = {
            "results": [
                {
                    "title": "Company Announcement",
                    "url": "https://example.com/a",
                    "content": "A relevant market risk item.",
                    "published_date": "2026-04-25",
                },
                {
                    "title": "Duplicate Announcement",
                    "url": "https://www.example.com/a/",
                    "content": "The same item from a www mirror.",
                }
            ]
        }
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        tool = self._build_tool()
        text = tool._tavily_web_search("risk query", purpose="risk check", max_results=3, domains=["example.com"])

        self.assertIn("Company Announcement", text)
        self.assertIn("https://example.com/a", text)
        self.assertNotIn("Duplicate Announcement", text)
        called_kwargs = mock_post.call_args.kwargs
        self.assertEqual(called_kwargs["headers"]["Authorization"], "Bearer test-key")
        self.assertEqual(called_kwargs["json"]["include_domains"], ["example.com"])
        self.assertEqual(called_kwargs["json"]["topic"], "finance")

    def test_web_search_falls_back_to_duckduckgo_when_tavily_empty(self):
        tool = self._build_tool()

        with patch.object(tool, "_read_web_search_cache", return_value=""), patch.object(
            tool, "_write_web_search_cache"
        ) as mock_write, patch.object(tool, "_tavily_web_search", return_value="") as mock_tavily, patch.object(
            tool, "_free_web_search", return_value="duckduckgo result with enough useful characters"
        ) as mock_duckduckgo:
            result = tool.web_search("risk query", purpose="risk check", max_results=2)

        self.assertEqual(result, "duckduckgo result with enough useful characters")
        mock_tavily.assert_called_once()
        mock_duckduckgo.assert_called_once()
        mock_write.assert_called_once()

    def test_duckduckgo_html_parser_extracts_clean_results(self):
        tool = self._build_tool()
        html = """
        <div class="result">
          <a class="result__a" href="/l/?kh=-1&amp;uddg=https%3A%2F%2Fexample.com%2Fnews">
            Risk <b>Headline</b>
          </a>
          <a class="result__snippet" href="/l/?kh=-1">
            Important &amp; recent disclosure.
          </a>
        </div>
        <div class="result">
          <a class="result__a" href="/l/?kh=-1&amp;uddg=https%3A%2F%2Fwww.example.com%2Fnews%2F">
            Duplicate Risk Headline
          </a>
          <a class="result__snippet" href="/l/?kh=-1">
            Duplicate mirror.
          </a>
        </div>
        """

        results = tool._parse_duckduckgo_html(html, max_results=3)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Risk Headline")
        self.assertEqual(results[0]["url"], "https://example.com/news")
        self.assertEqual(results[0]["snippet"], "Important & recent disclosure.")

    @patch("src.agent.tools.requests.get")
    def test_stock_news_details_fetches_article_body(self, mock_get):
        response = MagicMock()
        response.text = """
        <html><body>
          <script>ignore()</script>
          <div id="artibody">
            <p>Company revenue improved.</p>
            <p>No major risk event was disclosed.</p>
          </div>
        </body></html>
        """
        response.apparent_encoding = "utf-8"
        response.encoding = "utf-8"
        response.raise_for_status.return_value = None
        mock_get.return_value = response
        tool = self._build_tool()

        details = tool.fetch_stock_news_details(
            "2026-04-24 Direct headline (https://example.com/article.shtml)",
            max_articles=1,
        )

        self.assertIn("https://example.com/article.shtml", details)
        self.assertIn("Company revenue improved.", details)
        self.assertNotIn("ignore()", details)

    @patch("src.agent.tools.fetch_eastmoney_announcement_content")
    @patch("src.agent.tools.fetch_eastmoney_announcements")
    def test_stock_announcement_details_fetches_only_high_value_items(self, mock_announcements, mock_content):
        mock_announcements.return_value = pd.DataFrame(
            [
                {
                    "art_code": "AN_MONTH",
                    "title": "春秋航空2026年3月份主要运营数据公告",
                    "date": "2026-04-16",
                    "categories": "月度经营情况",
                },
                {
                    "art_code": "AN_POLICY",
                    "title": "董事、高级管理人员薪酬管理制度",
                    "date": "2026-04-11",
                    "categories": "管理办法/制度",
                },
                {
                    "art_code": "AN_REPORT",
                    "title": "春秋航空2025年年度报告摘要",
                    "date": "2026-04-11",
                    "categories": "年度报告摘要",
                },
            ]
        )
        mock_content.return_value = {
            "title": "春秋航空2026年3月份主要运营数据公告",
            "date": "2026-04-16",
            "content": "本月无新增飞机。\n截至本月末，公司共运营 134 架空客 A320 系列飞机。\n客座率同比提升。",
            "attach_url": "https://pdf.example.com/month.pdf",
        }
        tool = self._build_tool()

        details = tool.fetch_stock_announcement_details("601021", financial_reports_covered=True)

        self.assertIn("主要运营数据公告", details)
        self.assertIn("134 架", details)
        self.assertNotIn("薪酬管理制度", details)
        self.assertNotIn("年度报告摘要", details)
        mock_content.assert_called_once_with("AN_MONTH")


class MacroContextTests(unittest.TestCase):
    def test_macro_context_normalizer_fills_schema_and_coerces_types(self):
        agent = MacroAgent()

        context = agent._normalize_macro_context(
            {
                "market_sentiment": "偏强",
                "risk_appetite": ["偏进攻", "但需防追高"],
                "favorable_sectors": "AI, 半导体; 券商",
                "avoid_sectors": None,
                "key_risks": {"risk": "缩量新高"},
            }
        )

        self.assertEqual(set(context), set(MACRO_CONTEXT_SCHEMA_FIELDS))
        self.assertEqual(context["risk_appetite"], "偏进攻 但需防追高")
        self.assertEqual(context["favorable_sectors"], ["AI", "半导体", "券商"])
        self.assertIsInstance(context["avoid_sectors"], list)
        self.assertEqual(context["key_risks"], ["{'risk': '缩量新高'}"])

    @patch("src.agent.macro_agent.tools.call_llm", return_value="{bad json")
    @patch("src.agent.macro_agent.tools.web_search", return_value="")
    @patch("src.agent.macro_agent.tools.fetch_market_sentiment", return_value="mock sentiment")
    def test_macro_agent_returns_schema_on_llm_fallback(
        self,
        _mock_sentiment,
        _mock_web_search,
        _mock_call_llm,
    ):
        context = MacroAgent().analyze_macro_environment()

        self.assertEqual(set(context), set(MACRO_CONTEXT_SCHEMA_FIELDS))
        self.assertIsInstance(context["favorable_sectors"], list)
        self.assertIsInstance(context["avoid_sectors"], list)
        self.assertIsInstance(context["key_risks"], list)

    @patch("src.agent.macro_agent.tools.call_llm")
    @patch("src.agent.macro_agent.tools.web_search")
    @patch("src.agent.macro_agent.tools.fetch_market_sentiment", return_value="mock sentiment")
    def test_macro_agent_retries_broad_search_when_primary_empty(
        self,
        _mock_sentiment,
        mock_web_search,
        mock_call_llm,
    ):
        mock_web_search.side_effect = ["", "broad macro result"]
        mock_call_llm.return_value = (
            '{"market_sentiment":"neutral","risk_appetite":"neutral",'
            '"liquidity_view":"ok","favorable_sectors":[],"avoid_sectors":[],'
            '"key_risks":[],"analysis_focus":"focus"}'
        )

        MacroAgent().analyze_macro_environment()

        self.assertEqual(mock_web_search.call_count, 2)
        self.assertEqual(mock_web_search.call_args.kwargs["max_results"], 8)


class CoordinatorConcurrencyTests(unittest.TestCase):
    def _build_coordinator(self, max_workers=3):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)
        coordinator.config = {"agent_workflow": {"candidate_analysis_max_workers": max_workers}}
        coordinator.candidate_analysis_max_workers = coordinator._get_candidate_analysis_max_workers()
        coordinator.fundamental_agent = MagicMock()
        coordinator.technical_agent = MagicMock()
        coordinator.news_risk_agent = MagicMock()
        return coordinator

    def test_candidate_analysis_max_workers_is_capped_for_rate_limit(self):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)
        coordinator.config = {"agent_workflow": {"candidate_analysis_max_workers": 99}}

        self.assertEqual(coordinator._get_candidate_analysis_max_workers(), 5)

    def test_candidate_analysis_runs_concurrently_and_isolates_failures(self):
        coordinator = self._build_coordinator(max_workers=3)
        seen_threads = set()

        def fund_side_effect(code, name, macro_context=None):
            seen_threads.add(threading.current_thread().name)
            time.sleep(0.05)
            if code == "000002":
                raise RuntimeError("mock failure")
            return f"fund {code}"

        coordinator.fundamental_agent.analyze.side_effect = fund_side_effect
        coordinator.technical_agent.analyze.side_effect = lambda code, name, macro_context=None: f"tech {code}"
        coordinator.news_risk_agent.analyze.side_effect = lambda code, name, macro_context=None: {
            "risk_level": "low",
            "hard_exclude": False,
            "summary": f"risk {code}",
        }
        candidates = [
            {"code": "000001", "name": "safe one"},
            {"code": "000002", "name": "fail one"},
            {"code": "000003", "name": "safe two"},
        ]

        reports, errors = coordinator._analyze_candidates_concurrently(candidates, {"risk_appetite": "neutral"})

        self.assertEqual([item["asset_info"]["code"] for item in reports], ["000001", "000002", "000003"])
        self.assertGreaterEqual(len(seen_threads), 2)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["code"], "000002")
        failed_report = reports[1]
        self.assertIn("深度分析失败", failed_report["fundamental_analysis"])
        self.assertFalse(failed_report["news_risk_analysis"]["hard_exclude"])


    def test_target_code_normalizer_keeps_order_and_dedupes(self):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)

        result = coordinator._normalize_target_codes(["sh600519,000001", "000001", "bad", "SZ300750"])

        self.assertEqual(result, ["600519", "000001", "300750"])

    def test_targeted_report_has_summary_and_hides_raw_news_key(self):
        coordinator = AgentCoordinator.__new__(AgentCoordinator)
        reports = [
            {
                "asset_info": {"code": "600519", "name": "sample", "price": 10},
                "fundamental_analysis": "最终评级：中性。基本面稳定。",
                "technical_analysis": "操作建议：观望为主。",
                "news_risk_analysis": {
                    "risk_level": "low",
                    "action": "pass",
                    "hard_exclude": False,
                    "summary": "未见明显雷点",
                    "llm_report": "风控结论正常",
                    "raw_news": "raw noisy item",
                    "relevant_news": "2026-04-26 sample direct headline",
                },
            }
        ]

        text = coordinator._format_targeted_analysis_report(reports, {"market_sentiment": "neutral"}, [])

        self.assertIn("|", text)
        self.assertIn("sample(600519)", text)
        self.assertIn("2026-04-26 sample direct headline", text)
        self.assertNotIn("raw_news", text)
        self.assertNotIn("raw noisy item", text)


class QuickFilterAgentTests(unittest.TestCase):
    @patch("src.agent.quick_filter_agent.tools.call_llm")
    def test_quick_filter_selects_only_allowed_codes(self, mock_call_llm):
        mock_call_llm.return_value = (
            '{"evaluations": [], "selected_codes": ["000002", "999999", "000001"], '
            '"summary": "优先匹配震荡市风格"}'
        )
        candidates = [
            {"code": "000001", "name": "one", "technical_score": 80, "strategy_confidence": 0.7},
            {"code": "000002", "name": "two", "technical_score": 85, "strategy_confidence": 0.8},
            {"code": "000003", "name": "three", "technical_score": 70, "strategy_confidence": 0.6},
        ]

        result = QuickFilterAgent().filter_candidates(candidates, {"risk_appetite": "中性"}, target_n=2)

        self.assertEqual(result["mode"], "llm")
        self.assertEqual(result["selected_codes"], ["000002", "000001"])
        self.assertEqual([item["code"] for item in result["selected_candidates"]], ["000002", "000001"])

    @patch("src.agent.quick_filter_agent.tools.call_llm", return_value="")
    def test_quick_filter_falls_back_to_rule_order(self, _mock_call_llm):
        candidates = [
            {"code": "000001", "technical_score": 80, "strategy_confidence": 0.7},
            {"code": "000002", "technical_score": 85, "strategy_confidence": 0.9},
            {"code": "000003", "technical_score": 95, "strategy_confidence": 0.6},
        ]

        result = QuickFilterAgent().filter_candidates(candidates, target_n=2)

        self.assertEqual(result["mode"], "rule_fallback")
        self.assertEqual(result["selected_codes"], ["000002", "000001"])


class TradingAgentTests(unittest.TestCase):
    @patch.object(TradingAgent, "_llm_decisions", return_value=[])
    def test_fallback_buys_actionable_watchlist_and_sells_hard_exit(self, _mock_llm):
        agent = TradingAgent(max_positions=5, max_buys_per_run=2, max_sells_per_run=2)
        decisions = agent.decide(
            watchlist=[
                {
                    "id": 1,
                    "code": "000001",
                    "name": "buy sample",
                    "tier": "强推荐",
                    "current_price": 10.0,
                    "expected_return_pct": 18,
                    "recommend_reason": "基本面稳定，趋势未失效",
                }
            ],
            positions=[
                {
                    "code": "000002",
                    "name": "sell sample",
                    "quantity": 100,
                    "current_price": 9.0,
                    "unrealized_return_pct": -6,
                }
            ],
            account={"cash": 10000},
            exit_signals={"000002": {"action_level": 3, "reason": "技术破位"}},
            macro_context={},
        )

        actions = {(item["code"], item["action"]) for item in decisions}
        self.assertIn(("000001", "BUY"), actions)
        self.assertIn(("000002", "SELL"), actions)


class NewsRiskDecisionTests(unittest.TestCase):
    def test_news_risk_keyword_assessment_marks_high_risk(self):
        agent = NewsRiskAgent()

        result = agent.assess_keyword_risk(
            "公司公告：控股股东拟减持，另收到交易所监管函。",
            code="600000",
            name="demo",
        )

        self.assertEqual(result["risk_level"], "high")
        self.assertTrue(result["hard_exclude"])
        self.assertEqual(result["action"], "hard_exclude")
        self.assertIn("减持", [item["keyword"] for item in result["matched_keywords"]])

    def test_news_risk_does_not_flag_neutral_earnings_preview_phrase(self):
        agent = NewsRiskAgent()

        neutral = agent.assess_keyword_risk(
            "归母净利润符合业绩预告区间，公告未披露下修或预亏。",
            code="601016",
            name="sample",
        )
        risky = agent.assess_keyword_risk("公司披露业绩预亏，经营压力上升。", code="601016", name="sample")

        self.assertEqual(neutral["risk_level"], "low")
        self.assertEqual(risky["risk_level"], "medium")

    def test_news_risk_promotes_llm_high_risk_verdict(self):
        agent = NewsRiskAgent()
        assessment = agent.assess_keyword_risk("未命中规则关键词", code="601016", name="sample")

        promoted = agent._apply_llm_verdict(
            assessment,
            "风险等级：【高危】\n建议：【禁止】。业绩大幅恶化且弱市放大风险。",
        )

        self.assertEqual(promoted["risk_level"], "high")
        self.assertTrue(promoted["hard_exclude"])
        self.assertEqual(promoted["action"], "hard_exclude")
        self.assertIn("LLM高危/禁止", [item["keyword"] for item in promoted["matched_keywords"]])

    def test_news_risk_ignores_query_keywords_and_unrelated_results(self):
        agent = NewsRiskAgent()

        result = agent.assess_keyword_risk(
            "\n".join(
                [
                    "Tavily 搜索结果摘要（用途：排雷，查询：000001 平安银行 最近30天 减持 质押 立案调查 监管函）：",
                    "1. 年报季业绩预告变脸频发 多家公司收监管函 | https://example.com/news",
                    "正文只提到其他公司收到监管函，没有出现当前候选名称。",
                ]
            ),
            code="000001",
            name="平安银行",
        )

        self.assertEqual(result["risk_level"], "low")
        self.assertFalse(result["hard_exclude"])
        self.assertEqual(result["matched_keywords"], [])

    def test_news_risk_filters_direct_stock_news(self):
        agent = NewsRiskAgent()
        text = "\n".join(
            [
                "2026-04-25 同行业公司出现运营事件",
                "2026-04-24 春秋航空完成客舱 Wi-Fi 适航试验",
                "2026-04-23 601021 发布月度运营数据",
            ]
        )

        result = agent._filter_relevant_news(text, code="601021", name="春秋航空")

        self.assertIn("春秋航空", result)
        self.assertIn("601021", result)
        self.assertNotIn("同行业公司", result)

    @patch("src.agent.decision_agent.tools.call_llm")
    def test_decision_agent_filters_hard_excluded_news_risk(self, mock_call_llm):
        mock_call_llm.return_value = "推荐 safe，误选 blocked。\n[CODE_LIST] 000001, 000002 [/CODE_LIST]"
        reports = [
            {
                "asset_info": {"code": "000001", "name": "safe", "price": 10, "change_pct": 1},
                "fundamental_analysis": "ok",
                "technical_analysis": "ok",
                "news_risk_analysis": {"risk_level": "low", "hard_exclude": False, "summary": "未见明显雷点"},
            },
            {
                "asset_info": {"code": "000002", "name": "blocked", "price": 10, "change_pct": 1},
                "fundamental_analysis": "ok",
                "technical_analysis": "ok",
                "news_risk_analysis": {
                    "risk_level": "block",
                    "hard_exclude": True,
                    "summary": "命中立案调查，禁止推荐",
                },
            },
        ]

        report, selected = DecisionAgent().synthesize_and_elect_winners(
            reports,
            pick_n=2,
            macro_context={
                "market_sentiment": "震荡",
                "risk_appetite": "防守",
                "liquidity_view": "缩量",
                "key_risks": ["避免追高"],
            },
        )

        self.assertIn("000002", mock_call_llm.call_args.args[1])
        self.assertEqual(selected, ["000001"])
        self.assertIn("[CODE_LIST]", report)
        self.assertIn("宏观适配度", report)

    @patch("src.agent.decision_agent.tools.call_llm")
    def test_decision_agent_accepts_full_width_code_list_tags(self, mock_call_llm):
        mock_call_llm.return_value = "最终推荐 safe。\n【CODE_LIST】 000001 【/CODE_LIST】"
        reports = [
            {
                "asset_info": {"code": "000001", "name": "safe", "price": 10, "change_pct": 1},
                "fundamental_analysis": "ok",
                "technical_analysis": "ok",
                "news_risk_analysis": {"risk_level": "low", "hard_exclude": False, "summary": "未见明显雷点"},
            }
        ]

        report, selected = DecisionAgent().synthesize_and_elect_winners(reports, pick_n=1)

        self.assertEqual(selected, ["000001"])
        self.assertIn("[CODE_LIST] 000001 [/CODE_LIST]", report)
        self.assertNotIn("【CODE_LIST】", report)

    def test_decision_agent_enforces_report_date(self):
        report = "### A股最终决策报告\n**决策日期**: 2025年4月24日\n[CODE_LIST] 000001 [/CODE_LIST]"

        fixed = DecisionAgent()._enforce_decision_date(report, "2026-04-25")

        self.assertIn("**决策日期**: 2026-04-25", fixed)
        self.assertNotIn("2025年4月24日", fixed)

    def test_decision_agent_falls_back_to_allowed_code_scan(self):
        reports = [
            {"asset_info": {"code": "000001", "name": "safe"}},
            {"asset_info": {"code": "000002", "name": "also_safe"}},
        ]

        selected = DecisionAgent()._extract_selected_codes(
            "最终推荐 000001，顺带提到非候选 600000，然后观察 000002。",
            reports,
        )

        self.assertEqual(selected, ["000001", "000002"])

    @patch("src.agent.decision_agent.tools.call_llm")
    def test_decision_agent_returns_empty_when_all_candidates_blocked(self, mock_call_llm):
        reports = [
            {
                "asset_info": {"code": "000002", "name": "blocked", "price": 10, "change_pct": 1},
                "news_risk_analysis": {
                    "risk_level": "block",
                    "hard_exclude": True,
                    "summary": "命中行政处罚，禁止推荐",
                },
            }
        ]

        report, selected = DecisionAgent().synthesize_and_elect_winners(reports, pick_n=1)

        mock_call_llm.assert_not_called()
        self.assertEqual(selected, [])
        self.assertIn("本轮不推荐", report)


if __name__ == "__main__":
    unittest.main()
