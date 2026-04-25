import os
import sys
import unittest

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.financial_data import FinancialDataProvider, FinancialStatementBundle


class FinancialDataProviderTests(unittest.TestCase):
    def test_symbol_mapping(self):
        self.assertEqual(FinancialDataProvider.to_statement_symbol("600519"), "SH600519")
        self.assertEqual(FinancialDataProvider.to_statement_symbol("000001"), "SZ000001")
        self.assertEqual(FinancialDataProvider.to_indicator_symbol("600519"), "600519.SH")
        self.assertEqual(FinancialDataProvider.to_indicator_symbol("000001"), "000001.SZ")

    def test_normalize_financial_metrics_merges_statement_tables(self):
        provider = FinancialDataProvider()
        indicators = pd.DataFrame(
            [
                {
                    "REPORT_DATE": "2026-03-31",
                    "REPORT_DATE_NAME": "2026一季报",
                    "NOTICE_DATE": "2026-04-25",
                    "REPORT_TYPE": "一季报",
                    "TOTALOPERATEREVE": 100.0,
                    "TOTALOPERATEREVETZ": 12.5,
                    "PARENTNETPROFIT": 20.0,
                    "PARENTNETPROFITTZ": 10.0,
                    "KCFJCXSYJLR": 19.0,
                    "KCFJCXSYJLRTZ": 8.0,
                    "XSMLL": 55.0,
                    "XSJLL": 20.0,
                    "ROEJQ": 9.0,
                    "ROIC": 8.5,
                    "ZCFZL": 35.0,
                    "LD": 2.1,
                    "SD": 1.8,
                }
            ]
        )
        cash_flow = pd.DataFrame(
            [
                {
                    "REPORT_DATE": "2026-03-31",
                    "NETCASH_OPERATE": 25.0,
                    "NETCASH_OPERATE_YOY": 15.0,
                }
            ]
        )
        balance = pd.DataFrame(
            [
                {
                    "REPORT_DATE": "2026-03-31",
                    "TOTAL_ASSETS": 300.0,
                    "TOTAL_LIABILITIES": 105.0,
                }
            ]
        )

        metrics = provider.normalize_financial_metrics(
            "600519",
            FinancialStatementBundle(
                indicators=indicators,
                profit=pd.DataFrame(),
                balance=balance,
                cash_flow=cash_flow,
            ),
        )

        self.assertEqual(len(metrics), 1)
        row = metrics.iloc[0]
        self.assertEqual(row["code"], "600519")
        self.assertEqual(row["report_date"], "20260331")
        self.assertEqual(row["revenue_yoy"], 12.5)
        self.assertEqual(row["parent_netprofit_yoy"], 10.0)
        self.assertEqual(row["cash_to_profit"], 1.25)

        summary = provider.format_metrics_for_prompt(metrics)
        self.assertIn("东方财富财务数据摘要", summary)
        self.assertIn("2026一季报", summary)
        self.assertIn("现金流/利润 1.25", summary)


if __name__ == "__main__":
    unittest.main()
