"""
LLM API调用模块
支持多种大模型API调用，包括OpenAI、Claude、通义千问等
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
        
        self.provider = self.llm_config.get('provider', 'openai')
        self.api_key = self.llm_config.get('api_key', '')
        self.base_url = self.llm_config.get('base_url', '')
        self.model = self.llm_config.get('model', 'gpt-4')
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
        
        if self.provider == 'openai' or self.provider == 'deepseek':
            headers['Authorization'] = f'Bearer {self.api_key}'
        elif self.provider == 'claude':
            headers['x-api-key'] = self.api_key
            headers['anthropic-version'] = '2023-06-01'
        elif self.provider == 'qwen':
            headers['Authorization'] = f'Bearer {self.api_key}'
        
        return headers
    
    def _build_openai_request(self, prompt: str) -> Dict[str, Any]:
        """
        构建OpenAI请求
        
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
                    'content': '你是一个专业的ETF交易分析师，具有丰富的技术分析经验和市场洞察力。请基于提供的数据给出专业的交易建议。'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'max_tokens': self.max_tokens,
            'temperature': self.temperature
        }
    
    def _build_claude_request(self, prompt: str) -> Dict[str, Any]:
        """
        构建Claude请求
        
        Args:
            prompt: 提示词
            
        Returns:
            请求数据字典
        """
        return {
            'model': self.model,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'messages': [
                {
                    'role': 'user',
                    'content': prompt
                }
            ]
        }
    
    def _build_qwen_request(self, prompt: str) -> Dict[str, Any]:
        """
        构建通义千问请求
        
        Args:
            prompt: 提示词
            
        Returns:
            请求数据字典
        """
        return {
            'model': self.model,
            'input': {
                'messages': [
                    {
                        'role': 'system',
                        'content': '你是一个专业的ETF交易分析师，具有丰富的技术分析经验和市场洞察力。请基于提供的数据给出专业的交易建议。'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ]
            },
            'parameters': {
                'max_tokens': self.max_tokens,
                'temperature': self.temperature
            }
        }
    
    def _build_request(self, prompt: str) -> Dict[str, Any]:
        """
        构建请求
        
        Args:
            prompt: 提示词
            
        Returns:
            请求数据字典
        """
        if self.provider == 'openai' or self.provider == 'deepseek':
            return self._build_openai_request(prompt)
        elif self.provider == 'claude':
            return self._build_claude_request(prompt)
        elif self.provider == 'qwen':
            return self._build_qwen_request(prompt)
        else:
            raise ValueError(f"不支持的LLM提供商: {self.provider}")
    
    def _get_api_url(self) -> str:
        """
        获取API URL
        
        Returns:
            API URL字符串
        """
        if self.provider == 'openai' or self.provider == 'deepseek':
            return f"{self.base_url}/chat/completions"
        elif self.provider == 'claude':
            return f"{self.base_url}/v1/messages"
        elif self.provider == 'qwen':
            return f"{self.base_url}/v1/services/aigc/text-generation/generation"
        else:
            raise ValueError(f"不支持的LLM提供商: {self.provider}")
    
    def _parse_openai_response(self, response_data: Dict[str, Any]) -> str:
        """
        解析OpenAI响应
        
        Args:
            response_data: 响应数据
            
        Returns:
            解析后的文本
        """
        try:
            return response_data['choices'][0]['message']['content']
        except (KeyError, IndexError) as e:
            logger.error(f"解析OpenAI响应失败: {e}")
            return ""
    
    def _parse_claude_response(self, response_data: Dict[str, Any]) -> str:
        """
        解析Claude响应
        
        Args:
            response_data: 响应数据
            
        Returns:
            解析后的文本
        """
        try:
            return response_data['content'][0]['text']
        except (KeyError, IndexError) as e:
            logger.error(f"解析Claude响应失败: {e}")
            return ""
    
    def _parse_qwen_response(self, response_data: Dict[str, Any]) -> str:
        """
        解析通义千问响应
        
        Args:
            response_data: 响应数据
            
        Returns:
            解析后的文本
        """
        try:
            return response_data['output']['text']
        except (KeyError, IndexError) as e:
            logger.error(f"解析通义千问响应失败: {e}")
            return ""
    
    def _parse_response(self, response_data: Dict[str, Any]) -> str:
        """
        解析响应
        
        Args:
            response_data: 响应数据
            
        Returns:
            解析后的文本
        """
        if self.provider == 'openai' or self.provider == 'deepseek':
            return self._parse_openai_response(response_data)
        elif self.provider == 'claude':
            return self._parse_claude_response(response_data)
        elif self.provider == 'qwen':
            return self._parse_qwen_response(response_data)
        else:
            raise ValueError(f"不支持的LLM提供商: {self.provider}")
    
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
                            if self.provider == 'openai':
                                content = data['choices'][0]['delta'].get('content', '')
                            elif self.provider == 'claude':
                                # Claude的流式响应格式可能不同
                                content = data.get('content', '')
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
                f.write(advice)
            
            logger.info(f"交易建议已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存交易建议失败: {e}")
            return ""