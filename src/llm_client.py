"""
LLM API调用模块 - AI自主交易模式
使用统一的OpenAI兼容格式调用多种LLM服务，专注于AI自主交易决策
"""

import requests
import json
import time
import re
from typing import Dict, List, Any, Optional
from loguru import logger


class LLMClient:
    """LLM客户端 - AI自主交易模式"""
    
    def __init__(self, config: Dict):
        """
        初始化LLM客户端
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.llm_config = config.get('llm_models', {})
        self.active_llm = config.get('active_llm', '')
        
        # 加载当前激活的模型配置
        self.model_config = self._load_active_model_config()
        
        if not self.model_config:
            logger.error(f"无法加载模型配置，激活模型: {self.active_llm}")
            return
        
        # 设置模型参数
        self.api_key = self.model_config.get('api_key', '')
        self.base_url = self.model_config.get('base_url', '')
        self.model = self.model_config.get('model', '')
        self.max_tokens = self.model_config.get('max_tokens', 4000)
        self.temperature = self.model_config.get('temperature', 0.3)
        
        # 请求头
        self.headers = self._get_headers()
        
        # 加载ETF列表用于验证
        self.etf_list = self._load_etf_list()
        self.valid_etf_codes = self._get_valid_etf_codes()
        self.etf_name_to_code = self._create_etf_name_mapping()
        
        logger.info(f"AI自主交易LLM客户端初始化完成，模型: {self.active_llm} ({self.model})")
        logger.info(f"已加载 {len(self.valid_etf_codes)} 个有效ETF代码用于验证")
    
    def _load_active_model_config(self) -> Dict[str, Any]:
        """
        加载当前激活的模型配置
        
        Returns:
            模型配置字典
        """
        if not self.active_llm:
            logger.error("未设置激活的LLM模型")
            return {}
        
        model_config = self.llm_config.get(self.active_llm, {})
        if not model_config:
            logger.error(f"找不到模型配置: {self.active_llm}")
            return {}
        
        # 验证必需的配置项
        required_fields = ['api_key', 'base_url', 'model']
        for field in required_fields:
            if not model_config.get(field):
                logger.error(f"模型 {self.active_llm} 缺少必需配置: {field}")
                return {}
        
        return model_config
    
    def _load_etf_list(self) -> Dict[str, Any]:
        """
        加载ETF列表
        
        Returns:
            ETF列表字典
        """
        try:
            from .utils import load_etf_list
            return load_etf_list()
        except ImportError:
            try:
                from utils import load_etf_list
                return load_etf_list()
            except Exception as e:
                logger.error(f"加载ETF列表失败: {e}")
                return {}
    
    def _get_valid_etf_codes(self) -> set:
        """
        获取有效的ETF代码集合
        
        Returns:
            有效ETF代码集合
        """
        try:
            monitored_etfs = self.etf_list.get('monitored_etfs', [])
            valid_codes = set()
            for etf_info in monitored_etfs:
                code = etf_info.get('code')
                if code and isinstance(code, str) and len(code) == 6 and code.isdigit():
                    valid_codes.add(code)
            return valid_codes
        except Exception as e:
            logger.error(f"获取有效ETF代码失败: {e}")
            return set()
    
    def _create_etf_name_mapping(self) -> Dict[str, str]:
        """
        创建ETF名称到代码的映射
        
        Returns:
            ETF名称到代码的映射字典
        """
        try:
            monitored_etfs = self.etf_list.get('monitored_etfs', [])
            name_mapping = {}
            for etf_info in monitored_etfs:
                code = etf_info.get('code')
                name = etf_info.get('name')
                if code and name:
                    # 支持多种名称格式
                    name_mapping[name] = code
                    # 添加简短名称映射
                    short_name = name.replace('ETF', '').replace('基金', '').strip()
                    if short_name:
                        name_mapping[short_name] = code
            return name_mapping
        except Exception as e:
            logger.error(f"创建ETF名称映射失败: {e}")
            return {}
    
    def _get_headers(self) -> Dict[str, str]:
        """
        获取请求头（统一OpenAI兼容格式）
        
        Returns:
            请求头字典
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        return headers
    
    def _build_decision_request(self, prompt: str) -> Dict[str, Any]:
        """
        构建交易决策请求（统一OpenAI兼容格式）
        
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
                    'content': '你是一个专业的ETF交易决策系统。基于提供的数据，直接输出具体的交易决策，严格按照JSON格式返回。你的决策将被系统直接执行，请确保决策的准确性和安全性。'
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
        获取API URL（统一OpenAI兼容格式）
        
        Returns:
            API URL字符串
        """
        return f"{self.base_url}/chat/completions"
    
    def _parse_response(self, response_data: Dict[str, Any]) -> str:
        """
        解析响应（统一OpenAI兼容格式）
        
        Args:
            response_data: 响应数据
            
        Returns:
            解析后的文本
        """
        try:
            return response_data['choices'][0]['message']['content']
        except (KeyError, IndexError) as e:
            logger.error(f"解析{self.active_llm}响应失败: {e}")
            return ""
    
    def generate_trading_decision(self, prompt: str, 
                                retry_times: int = 3,
                                account_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        生成交易决策（统一OpenAI兼容格式）
        
        Args:
            prompt: 提示词
            retry_times: 重试次数
            account_data: 账户数据，用于风险控制验证
            
        Returns:
            交易决策字典
        """
        if not self.model_config:
            logger.error("LLM模型配置无效")
            return None
        
        for attempt in range(retry_times):
            try:
                logger.info(f"调用{self.active_llm} API生成AI自主交易决策，尝试次数: {attempt + 1}")
                
                # 构建请求
                request_data = self._build_decision_request(prompt)
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
                decision_text = self._parse_response(response_data)
                
                if decision_text:
                    # 提取交易决策
                    decision = self._extract_trading_decision(decision_text)
                    
                    if decision:
                        # 验证交易决策
                        validated_decision = self._validate_trading_decision(decision, account_data)
                        
                        if validated_decision:
                            logger.info("AI自主交易决策生成并验证成功")
                            return validated_decision
                        else:
                            logger.warning("AI自主交易决策验证失败")
                    else:
                        logger.warning("无法提取AI自主交易决策")
                else:
                    logger.warning("LLM返回空响应")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"{self.active_llm} API请求失败 (尝试 {attempt + 1}): {e}")
            except json.JSONDecodeError as e:
                logger.error(f"{self.active_llm}响应解析失败 (尝试 {attempt + 1}): {e}")
            except Exception as e:
                logger.error(f"生成AI自主交易决策失败 (尝试 {attempt + 1}): {e}")
            
            # 重试前等待
            if attempt < retry_times - 1:
                time.sleep(2 ** attempt)  # 指数退避
        
        return None
    
    def _extract_trading_decision(self, decision_text: str) -> Optional[Dict[str, Any]]:
        """
        从LLM返回的文本中提取交易决策（支持多股票格式）
        
        Args:
            decision_text: LLM返回的决策文本
            
        Returns:
            提取的交易决策字典
        """
        try:
            import re
            
            # 尝试提取JSON格式的交易决策
            json_pattern = r'```json\s*(.*?)\s*```'
            json_matches = re.findall(json_pattern, decision_text, re.DOTALL)
            
            if json_matches:
                # 使用第一个找到的JSON块
                json_str = json_matches[0].strip()
                try:
                    decision_data = json.loads(json_str)
                    logger.info("成功提取JSON格式的交易决策")
                    
                    # 检查是否为新的多股票格式
                    if "trading_decisions" in decision_data:
                        logger.info("检测到多股票决策格式")
                        return decision_data
                    else:
                        logger.info("检测到单股票决策格式，转换为多股票格式")
                        # 将单股票格式转换为多股票格式
                        return {"trading_decisions": [decision_data]}
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON解析失败，尝试其他方法: {e}")
            
            # 如果JSON解析失败，尝试直接解析整个文本
            try:
                decision_data = json.loads(decision_text.strip())
                logger.info("成功解析整个文本为JSON格式的交易决策")
                
                # 检查是否为新的多股票格式
                if "trading_decisions" in decision_data:
                    logger.info("检测到多股票决策格式")
                    return decision_data
                else:
                    logger.info("检测到单股票决策格式，转换为多股票格式")
                    # 将单股票格式转换为多股票格式
                    return {"trading_decisions": [decision_data]}
                    
            except json.JSONDecodeError:
                logger.warning("整个文本JSON解析失败")
            
            # 如果JSON解析都失败，尝试正则表达式提取
            single_decision = self._extract_decision_with_regex(decision_text)
            if single_decision:
                logger.info("使用正则表达式提取单股票决策，转换为多股票格式")
                return {"trading_decisions": [single_decision]}
            
            logger.warning("无法提取任何格式的交易决策")
            return None
            
        except Exception as e:
            logger.error(f"提取交易决策失败: {e}")
            return None
    
    def _extract_decision_with_regex(self, decision_text: str) -> Optional[Dict[str, Any]]:
        """
        使用正则表达式提取交易决策
        
        Args:
            decision_text: LLM返回的决策文本
            
        Returns:
            提取的交易决策字典
        """
        try:
            import re
            
            # 初始化结果
            decision_data = {}
            
            # 提取决策类型
            decision_pattern = r'"decision"\s*:\s*"(BUY|SELL|HOLD)"'
            decision_match = re.search(decision_pattern, decision_text, re.IGNORECASE)
            if decision_match:
                decision_data["decision"] = decision_match.group(1).upper()
            
            # 提取ETF代码
            symbol_pattern = r'"symbol"\s*:\s*"(\d{6})"'
            symbol_match = re.search(symbol_pattern, decision_text)
            if symbol_match:
                decision_data["symbol"] = symbol_match.group(1)
            
            # 提取交易金额
            amount_pattern = r'"amount"\s*:\s*(\d+(?:\.\d+)?)'
            amount_match = re.search(amount_pattern, decision_text)
            if amount_match:
                decision_data["amount"] = float(amount_match.group(1))
            
            # 提取交易数量
            quantity_pattern = r'"quantity"\s*:\s*(\d+)'
            quantity_match = re.search(quantity_pattern, decision_text)
            if quantity_match:
                decision_data["quantity"] = int(quantity_match.group(1))
            
            # 提取置信度
            confidence_pattern = r'"confidence"\s*:\s*(\d+(?:\.\d+)?)'
            confidence_match = re.search(confidence_pattern, decision_text)
            if confidence_match:
                decision_data["confidence"] = float(confidence_match.group(1))
            
            # 提取决策理由
            reason_pattern = r'"reason"\s*:\s*"([^"]+)"'
            reason_match = re.search(reason_pattern, decision_text)
            if reason_match:
                decision_data["reason"] = reason_match.group(1)
            
            # 检查是否提取到了必要字段
            required_fields = ["decision", "symbol", "confidence", "reason"]
            if all(field in decision_data for field in required_fields):
                logger.info("使用正则表达式成功提取交易决策")
                return decision_data
            else:
                logger.warning("正则表达式提取的交易决策缺少必要字段")
                return None
                
        except Exception as e:
            logger.error(f"正则表达式提取交易决策失败: {e}")
            return None
    
    def _validate_trading_decision(self, decision: Dict[str, Any],
                                 account_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        验证交易决策的有效性和安全性（支持多股票格式）
        
        Args:
            decision: 交易决策字典（可能是单股票或多股票格式）
            account_data: 账户数据
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            # 检查是否为新的多股票格式
            if "trading_decisions" in decision:
                logger.info("验证多股票交易决策格式")
                return self._validate_multi_stock_decisions(decision, account_data)
            else:
                logger.info("验证单股票交易决策格式，转换为多股票格式")
                # 验证单股票决策
                validated_single = self._validate_single_stock_decision(decision, account_data)
                if validated_single:
                    # 转换为多股票格式
                    return {"trading_decisions": [validated_single]}
                else:
                    return None
            
        except Exception as e:
            logger.error(f"AI自主交易决策验证失败: {e}")
            return None
    
    def _validate_single_stock_decision(self, decision: Dict[str, Any],
                                    account_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        验证单股票交易决策
        
        Args:
            decision: 单股票交易决策字典
            account_data: 账户数据
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            # 检查必要字段
            required_fields = ["decision", "symbol", "confidence", "reason"]
            for field in required_fields:
                if field not in decision:
                    logger.error(f"AI自主交易决策缺少必要字段: {field}")
                    return None
            
            # 检查盈利止损相关字段（新增要求）
            profit_loss_fields = ["profit_target", "stop_loss", "profit_target_pct", "stop_loss_pct"]
            for field in profit_loss_fields:
                if field not in decision:
                    logger.error(f"AI自主交易决策缺少盈利止损字段: {field}")
                    return None
            
            # 验证决策类型
            valid_decisions = ["BUY", "SELL", "HOLD"]
            if decision["decision"] not in valid_decisions:
                logger.error(f"无效的决策类型: {decision['decision']}")
                return None
            
            # 验证ETF代码格式和有效性
            symbol = decision["symbol"]
            logger.info(f"验证ETF代码: {symbol}, 类型: {type(symbol)}")
            
            # 标准化ETF代码
            standardized_symbol = self._standardize_etf_symbol(symbol)
            if not standardized_symbol:
                logger.error(f"无法标准化ETF代码: {symbol}")
                return None
            
            # 验证代码格式
            if not re.match(r'^\d{6}$', standardized_symbol):
                logger.error(f"无效的ETF代码格式: {standardized_symbol}")
                return None
            
            # 验证代码是否在有效列表中
            if standardized_symbol not in self.valid_etf_codes:
                logger.error(f"ETF代码不在监控列表中: {standardized_symbol}")
                logger.info(f"有效ETF代码列表: {sorted(list(self.valid_etf_codes))}")
                return None
            
            # 更新决策中的标准化代码
            decision["symbol"] = standardized_symbol
            
            # 验证置信度范围
            confidence = decision["confidence"]
            if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
                logger.error(f"无效的置信度: {confidence}")
                return None
            
            # 验证盈利止损字段
            # 支持两种字段名：profit_target 和 profit_target_price
            profit_target = decision.get("profit_target") or decision.get("profit_target_price", 0)
            # 支持两种字段名：stop_loss 和 stop_loss_price
            stop_loss = decision.get("stop_loss") or decision.get("stop_loss_price", 0)
            profit_target_pct = decision.get("profit_target_pct", 0)
            stop_loss_pct = decision.get("stop_loss_pct", 0)
            
            # 检查数据类型
            if not isinstance(profit_target, (int, float)) or profit_target <= 0:
                logger.error(f"盈利目标价格必须为正数: {profit_target}")
                return None
            
            if not isinstance(stop_loss, (int, float)) or stop_loss <= 0:
                logger.error(f"止损价格必须为正数: {stop_loss}")
                return None
            
            if not isinstance(profit_target_pct, (int, float)) or profit_target_pct <= 0:
                logger.error(f"盈利目标百分比必须为正数: {profit_target_pct}")
                return None
            
            if not isinstance(stop_loss_pct, (int, float)) or stop_loss_pct <= 0:
                logger.error(f"止损百分比必须为正数: {stop_loss_pct}")
                return None
            
            # 检查百分比范围（合理性检查）
            if profit_target_pct > 50:  # 盈利目标不超过50%
                logger.warning(f"盈利目标百分比过高: {profit_target_pct}%，调整为合理范围")
                decision["profit_target_pct"] = min(profit_target_pct, 50)
            
            if stop_loss_pct > 20:  # 止损不超过20%
                logger.warning(f"止损百分比过高: {stop_loss_pct}%，调整为合理范围")
                decision["stop_loss_pct"] = min(stop_loss_pct, 20)
            
            # 设置默认值（如果未提供）
            if "amount" not in decision:
                decision["amount"] = 0
            if "quantity" not in decision:
                decision["quantity"] = 0
            
            # 风险控制验证
            if account_data:
                validated_decision = self._validate_risk_controls(decision, account_data)
                if not validated_decision:
                    return None
                decision = validated_decision
            
            # 验证金额和数量的合理性
            if decision["decision"] != "HOLD":
                if decision["amount"] <= 0 and decision["quantity"] <= 0:
                    logger.error("非HOLD决策必须指定有效的金额或数量")
                    return None
            
            logger.info("单股票AI自主交易决策验证通过")
            return decision
            
        except Exception as e:
            logger.error(f"单股票AI自主交易决策验证失败: {e}")
            return None
    
    def _validate_multi_stock_decisions(self, decision: Dict[str, Any],
                                   account_data: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        验证多股票交易决策
        
        Args:
            decision: 多股票交易决策字典
            account_data: 账户数据
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            trading_decisions = decision.get("trading_decisions", [])
            
            if not isinstance(trading_decisions, list):
                logger.error("trading_decisions 必须是数组格式")
                return None
            
            if not trading_decisions:
                logger.info("多股票决策数组为空，返回HOLD决策")
                return {"trading_decisions": []}
            
            validated_decisions = []
            
            for i, single_decision in enumerate(trading_decisions):
                logger.info(f"验证第 {i+1} 个交易决策")
                
                validated_single = self._validate_single_stock_decision(single_decision, account_data)
                if validated_single:
                    validated_decisions.append(validated_single)
                else:
                    logger.warning(f"第 {i+1} 个交易决策验证失败，跳过")
            
            if not validated_decisions:
                logger.warning("所有交易决策验证失败")
                return None
            
            logger.info(f"多股票AI自主交易决策验证通过，有效决策数: {len(validated_decisions)}")
            return {"trading_decisions": validated_decisions}
            
        except Exception as e:
            logger.error(f"多股票AI自主交易决策验证失败: {e}")
            return None
    
    def _validate_risk_controls(self, decision: Dict[str, Any], 
                              account_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        验证风险控制
        
        Args:
            decision: 交易决策字典
            account_data: 账户数据
            
        Returns:
            验证后的交易决策字典，如果验证失败返回None
        """
        try:
            account_info = account_data.get('account_info', {})
            total_assets = account_info.get('total_assets', 0)
            available_cash = account_info.get('available_cash', 0)
            positions = account_data.get('positions', [])
            
            # 单次交易金额不超过总资产的10%
            max_single_trade_amount = total_assets * 0.1
            
            if decision["decision"] == "BUY":
                # 检查买入金额是否超过限制
                if decision["amount"] > max_single_trade_amount:
                    logger.warning(f"买入金额{decision['amount']}超过限制{max_single_trade_amount}，调整为最大限制")
                    decision["amount"] = max_single_trade_amount
                
                # 检查现金是否充足
                if decision["amount"] > available_cash:
                    logger.warning(f"买入金额{decision['amount']}超过可用现金{available_cash}，调整为可用现金金额")
                    decision["amount"] = available_cash
                
                # 如果没有指定金额但有数量，尝试估算金额
                if decision["amount"] <= 0 and decision["quantity"] > 0:
                    # 这里需要获取当前价格，暂时使用保守估算
                    estimated_price = 2.0  # 保守估算价格
                    estimated_amount = decision["quantity"] * estimated_price * 100  # ETF每手100股
                    decision["amount"] = min(estimated_amount, available_cash, max_single_trade_amount)
            
            elif decision["decision"] == "SELL":
                # 检查是否有足够的持仓可以卖出
                current_position = None
                for position in positions:
                    if position.get('symbol') == decision["symbol"]:
                        current_position = position
                        break
                
                if not current_position:
                    logger.error(f"没有找到ETF {decision['symbol']} 的持仓，无法卖出")
                    return None
                
                available_quantity = current_position.get('available_quantity', 0)
                
                # 检查卖出数量是否超过可用持仓
                if decision["quantity"] > available_quantity:
                    logger.warning(f"卖出数量{decision['quantity']}超过可用持仓{available_quantity}，调整为可用持仓数量")
                    decision["quantity"] = available_quantity
                
                # 如果没有指定数量但有金额，尝试估算数量
                if decision["quantity"] <= 0 and decision["amount"] > 0:
                    # 这里需要获取当前价格，暂时使用保守估算
                    estimated_price = 2.0  # 保守估算价格
                    estimated_quantity = decision["amount"] // (estimated_price * 100)
                    decision["quantity"] = min(estimated_quantity, available_quantity)
            
            return decision
            
        except Exception as e:
            logger.error(f"风险控制验证失败: {e}")
            return None
    
    def save_trading_decision_to_file(self, decision: Dict[str, Any], filename: str = None) -> str:
        """
        保存AI自主交易决策到文件
        
        Args:
            decision: 交易决策内容
            filename: 文件名
            
        Returns:
            保存的文件路径
        """
        try:
            import os
            from datetime import datetime
            
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"trading_decision_{timestamp}.json"
            
            # 确保输出目录存在
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(decision, f, ensure_ascii=False, indent=2)
            
            logger.info(f"AI自主交易决策已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存AI自主交易决策失败: {e}")
            return ""
    
    def test_connection(self) -> bool:
        """
        测试API连接（统一OpenAI兼容格式）
        
        Returns:
            连接是否成功
        """
        if not self.model_config:
            logger.error("LLM模型配置无效")
            return False
        
        try:
            test_prompt = "请回复'连接成功'"
            test_decision = {
                "decision": "HOLD",
                "symbol": "000000",
                "amount": 0,
                "quantity": 0,
                "confidence": 1.0,
                "reason": "测试连接"
            }
            
            response = self.generate_trading_decision(test_prompt, retry_times=1)
            
            if response:
                logger.info(f"{self.active_llm} API连接测试成功")
                return True
            else:
                logger.error(f"{self.active_llm} API连接测试失败")
                return False
                
        except Exception as e:
            logger.error(f"{self.active_llm} API连接测试异常: {e}")
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        获取模型信息
        
        Returns:
            模型信息字典
        """
        return {
            'active_model': self.active_llm,
            'model': self.model,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'base_url': self.base_url
        }
    
    def _standardize_etf_symbol(self, symbol: str) -> Optional[str]:
        """
        标准化ETF代码
        
        Args:
            symbol: 原始ETF代码或名称
            
        Returns:
            标准化后的6位ETF代码，如果无法标准化返回None
        """
        try:
            if not symbol or not isinstance(symbol, str):
                return None
            
            symbol = str(symbol).strip()
            
            # 如果已经是6位数字，直接返回
            if re.match(r'^\d{6}$', symbol):
                return symbol
            
            # 如果是6位数字但包含其他字符，提取数字部分
            if re.search(r'\d{6}', symbol):
                match = re.search(r'(\d{6})', symbol)
                if match:
                    code = match.group(1)
                    if code in self.valid_etf_codes:
                        return code
            
            # 尝试通过名称映射
            if symbol in self.etf_name_to_code:
                return self.etf_name_to_code[symbol]
            
            # 尝试模糊匹配名称
            for name, code in self.etf_name_to_code.items():
                if symbol in name or name in symbol:
                    logger.info(f"通过模糊匹配将 '{symbol}' 映射到 '{code}'")
                    return code
            
            logger.warning(f"无法标准化ETF代码: {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"标准化ETF代码失败: {e}")
            return None