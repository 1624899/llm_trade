import argparse
import sys
import os
from loguru import logger

from src.data_pipeline import DataPipeline
from src.agent.coordinator import AgentCoordinator

def setup_logger():
    """初始化终端日志显示格式"""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", level="INFO")

def main():
    setup_logger()
    
    parser = argparse.ArgumentParser(description="全自动 AI 量化智能体选股与自省系统 (LLM-TRADE)")
    parser.add_argument("--sync", action="store_true", help="【盘后首要】执行全市场 A 股盘后数据同步至本地数据湖 (建议 15:30 后执行)")
    parser.add_argument("--pick", action="store_true", help="【自动选股】执行技术面预筛选 + 多 Agent 深度分析选股流程")
    parser.add_argument("--post", action="store_true", help="【盘后清算】运行盘后例行维护：虚拟观察仓结算 + 失败错题反思并沉淀风控规则")
    
    parser.add_argument("--analyze", nargs="+", help="【指定分析】对指定 A 股代码做单独深度分析，例如：python main.py --analyze 600519 000001")
    args = parser.parse_args()
    
    if not any([args.sync, args.pick, args.analyze, args.post]):
        parser.print_help()
        logger.info("\n没有输入任何指令。例如执行每日选股： python main.py --pick")
        return

    logger.info("============== LLM-TRADE 智能引擎启动 ==============")

    # 1. 盘后数据兜底与同步
    if args.sync:
        logger.info(">>> 收到指令：全量同步云端市场数据至本地 SQLite ...")
        pipeline = DataPipeline()
        pipeline.run_all()
        
    # 2. 端到端多 Agent 选股执行
    if args.pick:
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

    # 3. 闭环虚拟仓管理与大模型自我反思进化
    # 3. 用户指定股票的单独深度分析
    if args.analyze:
        logger.info(f">>> 收到指定分析指令：{args.analyze}")
        coordinator = AgentCoordinator()
        report = coordinator.run_targeted_analysis(args.analyze)

    if args.post:
        logger.info(">>> 收到指令：执行盘后仓位结算与 AI 错题反思...")
        coordinator = AgentCoordinator()
        coordinator.run_post_market_routine()
        
    logger.info("============== 任务序列执行完毕 ==============")

if __name__ == "__main__":
    main()
