"""
LLM API调用模块
仅支持DeepSeek API调用
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional
from loguru import logger


class LLMClient:
    """LLM客户端"""
    
    def __init__(self, config: Dict):
        """
        初始化LLM客户端
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.llm_config = config.get('llm', {})
        
        self.provider = self.llm_config.get('provider', 'deepseek')
        self.api_key = self.llm_config.get('api_key', '')
        self.base_url = self.llm_config.get('base_url', 'https://api.deepseek.com/v1')
        self.model = self.llm_config.get('model', 'deepseek-chat')
        self.max_tokens = self.llm_config.get('max_tokens', 4000)
        self.temperature = self.llm_config.get('temperature', 0.7)
        
        # 请求头
        self.headers = self._get_headers()
        
        logger.info(f"LLM客户端初始化完成，提供商: {self.provider}")
    
    def _get_headers(self) -> Dict[str, str]:
        """
        获取请求头
        
        Returns:
            请求头字典
        """
        headers = {
            'Content-Type': 'application/json'
        }
        
        # DeepSeek 使用 Bearer token 认证
        headers['Authorization'] = f'Bearer {self.api_key}'
        
        return headers
    
    def _build_request(self, prompt: str) -> Dict[str, Any]:
        """
        构建DeepSeek请求
        
        Args:
            prompt: 提示词
            
        Returns:
            请求数据字典
        """
        return {
            'model': self.model,
            'messages': [
                {
                    'role': 'system',
                    'content': '你是一个专业的ETF交易分析师，具有丰富的技术分析经验和市场洞察力。请基于提供的数据给出专业且详细的交易建议。'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'max_tokens': self.max_tokens,
            'temperature': self.temperature
        }
    
    def _get_api_url(self) -> str:
        """
        获取API URL
        
        Returns:
            API URL字符串
        """
        return f"{self.base_url}/chat/completions"
    
    def _parse_response(self, response_data: Dict[str, Any]) -> str:
        """
        解析DeepSeek响应
        
        Args:
            response_data: 响应数据
            
        Returns:
            解析后的文本
        """
        try:
            return response_data['choices'][0]['message']['content']
        except (KeyError, IndexError) as e:
            logger.error(f"解析DeepSeek响应失败: {e}")
            return ""
    
    def generate_trading_advice(self, prompt: str, 
                              retry_times: int = 3) -> Optional[str]:
        """
        生成交易建议
        
        Args:
            prompt: 提示词
            retry_times: 重试次数
            
        Returns:
            交易建议文本
        """
        if not self.api_key:
            logger.error("LLM API密钥未配置")
            return None
        
        for attempt in range(retry_times):
            try:
                logger.info(f"调用LLM API生成交易建议，尝试次数: {attempt + 1}")
                
                # 构建请求
                request_data = self._build_request(prompt)
                api_url = self._get_api_url()
                
                # 发送请求
                response = requests.post(
                    api_url,
                    headers=self.headers,
                    json=request_data,
                    timeout=60
                )
                
                # 检查响应状态
                response.raise_for_status()
                
                # 解析响应
                response_data = response.json()
                advice = self._parse_response(response_data)
                
                if advice:
                    logger.info("交易建议生成成功")
                    return advice
                else:
                    logger.warning("LLM返回空响应")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"LLM API请求失败 (尝试 {attempt + 1}): {e}")
            except json.JSONDecodeError as e:
                logger.error(f"LLM响应解析失败 (尝试 {attempt + 1}): {e}")
            except Exception as e:
                logger.error(f"生成交易建议失败 (尝试 {attempt + 1}): {e}")
            
            # 重试前等待
            if attempt < retry_times - 1:
                time.sleep(2 ** attempt)  # 指数退避
        
        return None
    
    def generate_streaming_advice(self, prompt: str, 
                                callback_func=None) -> Optional[str]:
        """
        流式生成交易建议
        
        Args:
            prompt: 提示词
            callback_func: 流式回调函数
            
        Returns:
            交易建议文本
        """
        if not self.api_key:
            logger.error("LLM API密钥未配置")
            return None
        
        try:
            logger.info("开始流式生成交易建议")
            
            # 构建请求
            request_data = self._build_request(prompt)
            request_data['stream'] = True
            
            api_url = self._get_api_url()
            
            # 发送流式请求
            response = requests.post(
                api_url,
                headers=self.headers,
                json=request_data,
                timeout=120,
                stream=True
            )
            
            response.raise_for_status()
            
            full_response = ""
            
            # 处理流式响应
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    if line.startswith('data: '):
                        data_str = line[6:]  # 移除 'data: ' 前缀
                        
                        if data_str == '[DONE]':
                            break
                        
                        try:
                            data = json.loads(data_str)
                            
                            # 解析流式数据
                            # DeepSeek流式响应格式
                            if 'choices' in data and len(data['choices']) > 0:
                                delta = data['choices'][0].get('delta', {})
                                content = delta.get('content', '')
                            else:
                                content = ''
                            
                            if content:
                                full_response += content
                                
                                # 调用回调函数
                                if callback_func:
                                    callback_func(content)
                                    
                        except json.JSONDecodeError:
                            continue
            
            logger.info("流式交易建议生成完成")
            return full_response
            
        except Exception as e:
            logger.error(f"流式生成交易建议失败: {e}")
            return None
    
    def test_connection(self) -> bool:
        """
        测试API连接
        
        Returns:
            连接是否成功
        """
        try:
            test_prompt = "请回复'连接成功'"
            response = self.generate_trading_advice(test_prompt, retry_times=1)
            
            if response and "成功" in response:
                logger.info("LLM API连接测试成功")
                return True
            else:
                logger.error("LLM API连接测试失败")
                return False
                
        except Exception as e:
            logger.error(f"LLM API连接测试异常: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        获取模型信息
        
        Returns:
            模型信息字典
        """
        return {
            'provider': self.provider,
            'model': self.model,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'base_url': self.base_url
        }
    
    def save_advice_to_file(self, advice: str, filename: str = None) -> str:
        """
        保存交易建议到文件
        
        Args:
            advice: 交易建议内容
            filename: 文件名
            
        Returns:
            保存的文件路径
        """
        try:
            import os
            from datetime import datetime
            
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"trading_advice_{timestamp}.md"
            
            # 确保输出目录存在
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"# ETF交易建议\n\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"使用模型: {self.provider} - {self.model}\n\n")
                f.write("---\n\n")
                
                # 尝试提取JSON格式的交易建议
                trading_signals = self.extract_trading_signals(advice)
                
                if trading_signals:
                    # 格式化JSON输出
                    json_output = json.dumps(trading_signals, ensure_ascii=False, indent=2)
                    f.write("```json\n")
                    f.write(json_output)
                    f.write("\n```\n\n")
                    
                    # 添加详细分析部分
                    analysis_summary = trading_signals.get('analysis_summary', '')
                    if analysis_summary:
                        f.write("### 详细分析\n\n")
                        f.write(f"#### 市场概况\n{analysis_summary}\n\n")
                    
                    # 添加各个ETF的详细分析
                    recommendations = trading_signals.get('recommendations', [])
                    if recommendations:
                        f.write("#### 1. 市场状况分析\n")
                        for rec in recommendations:
                            symbol = rec.get('symbol', '')
                            name = rec.get('name', '')
                            reason = rec.get('reason', '')
                            f.write(f"**{name}({symbol})**：{reason}\n\n")
                        
                        f.write("#### 2. 持仓风险评估\n")
                        f.write("当前持仓风险分析...\n\n")
                        
                        f.write("#### 3. 风险管理建议\n")
                        for rec in recommendations:
                            symbol = rec.get('symbol', '')
                            name = rec.get('name', '')
                            stop_loss = rec.get('stop_loss', '')
                            take_profit = rec.get('take_profit', '')
                            if stop_loss and take_profit:
                                f.write(f"- **{name}**：止损{stop_loss}，目标{take_profit}\n")
                        f.write("\n")
                        
                        f.write("#### 4. 资金管理建议\n")
                        f.write("资金分配建议...\n\n")
                        
                        f.write("**总体策略**：基于技术分析和风险管理的综合建议。\n")
                else:
                    # 如果无法提取JSON格式，则直接保存原始建议
                    f.write(advice)
            
            logger.info(f"交易建议已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存交易建议失败: {e}")
            return ""
    
    def extract_trading_signals(self, advice: str) -> Optional[Dict[str, Any]]:
        """
        从LLM返回的建议中提取交易信号
        
        Args:
            advice: LLM返回的交易建议文本
            
        Returns:
            提取的交易信号字典
        """
        try:
            import re
            
            # 尝试提取JSON格式的交易建议
            json_pattern = r'```json\s*(.*?)\s*```'
            json_matches = re.findall(json_pattern, advice, re.DOTALL)
            
            if json_matches:
                # 使用第一个找到的JSON块
                json_str = json_matches[0].strip()
                try:
                    trading_data = json.loads(json_str)
                    logger.info("成功提取JSON格式的交易建议")
                    return trading_data
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON解析失败，尝试其他方法: {e}")
            
            # 如果JSON解析失败，尝试正则表达式提取
            return self._extract_with_regex(advice)
            
        except Exception as e:
            logger.error(f"提取交易信号失败: {e}")
            return None
    
    def _extract_with_regex(self, advice: str) -> Optional[Dict[str, Any]]:
        """
        使用正则表达式提取交易信息
        
        Args:
            advice: LLM返回的交易建议文本
            
        Returns:
            提取的交易信号字典
        """
        try:
            import re
            
            # 初始化结果
            trading_data = {
                "analysis_summary": "",
                "recommendations": []
            }
            
            # 提取分析总结
            summary_patterns = [
                r'分析总结[：:]\s*(.*?)(?=\n|$)',
                r'总结[：:]\s*(.*?)(?=\n|$)',
                r'总体分析[：:]\s*(.*?)(?=\n|$)'
            ]
            
            for pattern in summary_patterns:
                match = re.search(pattern, advice, re.IGNORECASE)
                if match:
                    trading_data["analysis_summary"] = match.group(1).strip()
                    break
            
            # 提取交易建议
            # 匹配ETF代码和操作
            etf_pattern = r'(\d{6})[：:]?\s*([买入卖出持有]+)\s*(\d+股?|\d+手?)?'
            etf_matches = re.findall(etf_pattern, advice)
            
            # 匹配止损止盈和买卖价格
            stop_loss_pattern = r'止损[：:]?\s*(\d+\.?\d*)'
            take_profit_pattern = r'止盈[：:]?\s*(\d+\.?\d*)'
            buy_price_pattern = r'买入价格[：:]?\s*(\d+\.?\d*)|买入点位[：:]?\s*(\d+\.?\d*)|建议买入价[：:]?\s*(\d+\.?\d*)'
            sell_price_pattern = r'卖出价格[：:]?\s*(\d+\.?\d*)|卖出点位[：:]?\s*(\d+\.?\d*)|建议卖出价[：:]?\s*(\d+\.?\d*)'
            
            stop_loss_matches = re.findall(stop_loss_pattern, advice)
            take_profit_matches = re.findall(take_profit_pattern, advice)
            buy_price_matches = re.findall(buy_price_pattern, advice)
            sell_price_matches = re.findall(sell_price_pattern, advice)
            
            # 组合交易建议
            for i, (symbol, action, quantity) in enumerate(etf_matches):
                # 尝试从配置文件获取ETF名称
                etf_name = self._get_etf_name_from_config(symbol)
                
                recommendation = {
                    "symbol": symbol,
                    "name": etf_name if etf_name else "",
                    "action": action,
                    "quantity": quantity if quantity else "建议数量",
                    "buy_quantity": "" if action != "买入" else (quantity if quantity else "建议数量"),
                    "sell_quantity": "" if action != "卖出" else (quantity if quantity else "建议数量"),
                    "buy_price": buy_price_matches[i] if i < len(buy_price_matches) else "",
                    "sell_price": sell_price_matches[i] if i < len(sell_price_matches) else "",
                    "stop_loss": stop_loss_matches[i] if i < len(stop_loss_matches) else "",
                    "take_profit": take_profit_matches[i] if i < len(take_profit_matches) else "",
                    "reason": "基于技术分析"
                }
                trading_data["recommendations"].append(recommendation)
            
            if trading_data["recommendations"]:
                logger.info("使用正则表达式成功提取交易建议")
                return trading_data
            else:
                logger.warning("未能提取到有效的交易建议")
                return None
                
        except Exception as e:
            logger.error(f"正则表达式提取交易信号失败: {e}")
            return None
    
    def _get_etf_name_from_config(self, etf_code: str) -> str:
        """
        从配置文件获取ETF标准名称
        
        Args:
            etf_code: ETF代码
            
        Returns:
            ETF标准名称，如果找不到返回None
        """
        try:
            # 动态导入避免循环依赖
            import sys
            import os
            sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
            
            from src.utils import load_etf_list
            etf_list = load_etf_list()
            
            monitored_etfs = etf_list.get('monitored_etfs', [])
            for etf_info in monitored_etfs:
                if etf_info.get('code') == etf_code:
                    return etf_info.get('name')
            return None
        except Exception as e:
            logger.error(f"从配置文件获取ETF名称失败: {e}")
            return None
    
    def save_trading_analysis(self, trading_data: Dict[str, Any],
                            market_data: Dict[str, Any],
                            account_data: Dict[str, Any]) -> str:
        """
        保存交易分析数据到JSON文件，使用固定名称并保留最近3次历史记录
        
        Args:
            trading_data: 提取的交易数据
            market_data: 市场数据
            account_data: 账户数据
            
        Returns:
            保存的文件路径
        """
        try:
            import os
            import glob
            from datetime import datetime
            
            # 创建完整的分析记录
            analysis_record = {
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "analysis_summary": trading_data.get("analysis_summary", ""),
                "recommendations": trading_data.get("recommendations", []),
                "market_snapshot": {},
                "account_snapshot": account_data.get("account_info", {}),
                "current_positions": account_data.get("positions", [])
            }
            
            # 添加市场快照
            for symbol, data in market_data.items():
                current_data = data.get("current_data", {})
                analysis_record["market_snapshot"][symbol] = {
                    "name": data.get("name", ""),
                    "current_price": current_data.get("current_price", 0),
                    "current_ema_long": current_data.get("current_ema_long", 0),
                    "current_macd": current_data.get("current_macd", 0),
                    "current_rsi_7": current_data.get("current_rsi_7", 0)
                }
            
            # 确保data目录存在
            os.makedirs("data", exist_ok=True)
            
            # 读取现有历史记录
            history_file = os.path.join("data", "trading_analysis_history.json")
            history_data = []
            
            if os.path.exists(history_file):
                try:
                    with open(history_file, 'r', encoding='utf-8') as f:
                        history_data = json.load(f)
                    if not isinstance(history_data, list):
                        history_data = []
                except Exception as e:
                    logger.warning(f"读取历史记录文件失败，创建新文件: {e}")
                    history_data = []
            
            # 添加新的分析记录到历史记录开头
            history_data.insert(0, analysis_record)
            
            # 保留最近3次记录
            history_data = history_data[:3]
            
            # 保存历史记录文件
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history_data, f, ensure_ascii=False, indent=2)
            
            # 保存当前分析记录为固定名称文件
            current_file = os.path.join("data", "trading_analysis.json")
            with open(current_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_record, f, ensure_ascii=False, indent=2)
            
            logger.info(f"交易分析数据已保存到: {current_file}")
            logger.info(f"历史记录已更新，保留最近3次记录: {history_file}")
            
            return current_file
            
        except Exception as e:
            logger.error(f"保存交易分析数据失败: {e}")
            return ""