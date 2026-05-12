import argparse
import sys
import os
from loguru import logger

from src.dashboard import run_dashboard

def setup_logger():
    """初始化终端日志显示格式"""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", level="INFO", colorize=True)

def main():
    setup_logger()
    
    parser = argparse.ArgumentParser(description="全自动 AI 量化智能体选股与自省系统 (LLM-TRADE)")
    parser.add_argument("--sync", action="store_true", help="【盘后首要】执行全市场 A 股盘后数据同步至本地数据湖 (建议 15:30 后执行)")
    parser.add_argument("--backfill-bars", action="store_true", help="【历史补洞】只补全本地缺失的 10 年日线 K 线，不派生周/月线")
    parser.add_argument("--derive-bars", action="store_true", help="【K线聚合】基于本地日线派生周线/月线")
    parser.add_argument("--pick", action="store_true", help="【自动选股】执行技术面预筛选 + 多 Agent 深度分析选股流程")
    parser.add_argument("--trade", action="store_true", help="【模拟交易】根据观察仓推荐和交易仓状态运行 TradingAgent 调仓")
    parser.add_argument("--set-trade-cash", type=float, help="【交易仓资金】手动设置模拟交易仓可用现金")
    parser.add_argument("--reset-trade-baseline", action="store_true", help="设置交易仓现金时同步重置初始资金基准")
    parser.add_argument("--post", action="store_true", help="【交易反思】运行盘后维护：观察仓诊断 + 已清仓交易仓亏损复盘")
    parser.add_argument("--dashboard", action="store_true", help="【可视化工作台】启动本地 Web 工作台，展示报告、观察仓、交易仓和审计摘要")
    parser.add_argument("--dashboard-host", default="127.0.0.1", help="工作台监听地址，默认 127.0.0.1")
    parser.add_argument("--dashboard-port", type=int, default=8765, help="工作台端口，默认 8765")
    parser.add_argument("--open-browser", action="store_true", help="启动工作台后自动打开浏览器")
    
    parser.add_argument("--analyze", nargs="+", help="【指定分析】对指定 A 股代码做单独深度分析，例如：python main.py --analyze 600519 000001")
    parser.add_argument("--backtest", action="store_true", help="遮盖旧数据做走步回测，生成因子权重参考")
    args = parser.parse_args()
    
    if not any([args.sync, args.backfill_bars, args.derive_bars, args.pick, args.trade, args.set_trade_cash is not None, args.analyze, args.post, args.backtest, args.dashboard]):
        parser.print_help()
        logger.info("\n没有输入任何指令。例如执行每日选股： python main.py --pick")
        return

    if args.dashboard:
        run_dashboard(host=args.dashboard_host, port=args.dashboard_port, open_browser=args.open_browser)
        return

    logger.info("============== LLM-TRADE 智能引擎启动 ==============")

    # 1. 盘后数据兜底与同步
    if args.sync:
        from src.data_pipeline import DataPipeline

        logger.info(">>> 收到指令：全量同步云端市场数据至本地 SQLite ...")
        pipeline = DataPipeline()
        pipeline.run_all()

    if args.backfill_bars:
        from src.data_pipeline import DataPipeline

        logger.info(">>> 收到指令：补全本地缺失的历史日线 K 线 ...")
        pipeline = DataPipeline()
        ok = pipeline.sync_market_bars_history(derive_periods=())
        if ok:
            logger.info("历史日线 K 线补洞完成")
        else:
            logger.warning("历史日线 K 线补洞完成，但存在部分缺失日期")

    if args.derive_bars:
        from src.data_pipeline import DataPipeline

        logger.info(">>> 收到指令：基于本地日线派生周线/月线 ...")
        pipeline = DataPipeline()
        ok = pipeline.derive_period_bars(periods=("weekly", "monthly"))
        if ok:
            logger.info("周线/月线派生完成")
        else:
            logger.warning("周线/月线派生完成，但存在部分失败")
        
    # 2. 端到端多 Agent 选股执行
    if args.pick:
        from src.agent.coordinator import AgentCoordinator

        logger.info(">>> 收到选股指令：技术预筛 + 多 Agent 深度分析")
        coordinator = AgentCoordinator()
        report = coordinator.run_picking_workflow(max_candidates=10)
        
        # 将生成的最终报告也落盘进 output 以备查阅
        os.makedirs("outputs", exist_ok=True)
        report_path = os.path.join("outputs", "latest_report.md")
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(report)
            logger.info(f"【研报生成完毕】已经保存至：{report_path}")
        except Exception as e:
            logger.error(f"保存研报失败: {e}")

    # 3. 用户指定股票的单独深度分析
    if args.analyze:
        from src.agent.coordinator import AgentCoordinator

        logger.info(f">>> 收到指定分析指令：{args.analyze}")
        coordinator = AgentCoordinator()
        report = coordinator.run_targeted_analysis(args.analyze)

    if args.set_trade_cash is not None:
        from src.agent.coordinator import AgentCoordinator

        logger.info(">>> 收到交易仓资金设置指令：cash={}", args.set_trade_cash)
        coordinator = AgentCoordinator()
        account = coordinator.trading_account.set_cash(
            args.set_trade_cash,
            reset_baseline=args.reset_trade_baseline,
        )
        print(
            "交易仓现金已更新："
            f"现金={round(float(account.get('cash') or 0), 2)}，"
            f"总权益={round(float(account.get('total_equity') or 0), 2)}"
        )

    if args.trade:
        from src.agent.coordinator import AgentCoordinator

        logger.info(">>> 收到模拟交易指令：TradingAgent 根据观察仓与交易仓执行调仓")
        coordinator = AgentCoordinator()
        coordinator.run_trading_workflow()

    if args.post:
        from src.agent.coordinator import AgentCoordinator

        logger.info(">>> 收到指令：执行盘后仓位结算与 AI 错题反思...")
        coordinator = AgentCoordinator()
        coordinator.run_post_market_routine()
        
    logger.info("============== 任务序列执行完毕 ==============")

    if args.backtest:
        from src.evaluation.backtest import BacktestEngine

        logger.info(">>> 收到回测指令：遮盖历史截面之后的数据，执行走步推演 ...")
        report = BacktestEngine().run_walk_forward_backtest()
        print(f"回测完成，窗口数={report.get('window_count')}，样本数={report.get('evaluated_count')}，摘要：{report.get('summary')}")

if __name__ == "__main__":
    main()
