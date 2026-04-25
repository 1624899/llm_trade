import unittest

from src.agent.exit_agent import ACTION_CLEAR, ACTION_REDUCE, ExitAgent, ExitDecision
from src.evaluation.paper_trading import PaperTrading
from src.technical_indicators import TechnicalSignalReport


class _FakeSignalProvider:
    def __init__(self, report):
        self.report = report

    def build_report(self, code, lookback=120):
        return self.report


class _FakeExitAgent:
    def __init__(self, decision):
        self.decision = decision

    def evaluate_position(self, position, macro_context=None):
        return self.decision


class ExitAgentTests(unittest.TestCase):
    def _report(self, *, close=9.5, risk_flags=None, ma20=10.0):
        return TechnicalSignalReport(
            code="000001",
            metrics={"available": True, "close": close, "ma20": ma20},
            levels={"breakdown_trigger": 9.7, "atr_stop_1_5x": 9.2},
            tags=["形态中性"],
            risk_flags=risk_flags or [],
            trade_plan={},
            generated_at="2026-04-25T20:00:00",
        )

    def test_exit_agent_clears_on_platform_breakdown(self):
        agent = ExitAgent(signal_provider=_FakeSignalProvider(self._report()))

        decision = agent.evaluate_position({"code": "000001", "current_price": 9.5, "return_pct": -1.0})

        self.assertEqual(decision.action, ACTION_CLEAR)
        self.assertIn("跌破20日平台", decision.reason)

    def test_exit_agent_reduces_on_high_volume_stall(self):
        agent = ExitAgent(
            signal_provider=_FakeSignalProvider(
                self._report(close=10.5, risk_flags=["放量滞涨/上影派发"], ma20=10.0)
            )
        )

        decision = agent.evaluate_position({"code": "000001", "current_price": 10.5, "return_pct": 3.0})

        self.assertEqual(decision.action, ACTION_REDUCE)
        self.assertIn("放量滞涨", decision.reason)

    def test_paper_trading_upgrades_action_when_exit_signal_is_stronger(self):
        decision = ExitDecision(
            code="000001",
            action=ACTION_CLEAR,
            action_level=3,
            reason="技术破位，清仓退出。",
        )
        paper = PaperTrading(exit_agent=_FakeExitAgent(decision))
        row = {
            "id": 1,
            "name": "sample",
            "code": "000001",
            "recommend_price": 10.0,
            "current_price": 10.1,
            "return_pct": 1.0,
        }

        diagnostic = paper._diagnose_position(row)

        self.assertEqual(diagnostic["action"], ACTION_CLEAR)
        self.assertEqual(diagnostic["reason"], "技术破位，清仓退出。")


if __name__ == "__main__":
    unittest.main()
