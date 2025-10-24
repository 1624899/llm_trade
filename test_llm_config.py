#!/usr/bin/env python3
"""
测试LLM配置脚本
用于验证统一OpenAI兼容格式的LLM配置是否正常工作
"""

import yaml
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.llm_client import LLMClient
from loguru import logger

def load_config():
    """加载配置文件"""
    try:
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return None

def test_llm_client():
    """测试LLM客户端配置"""
    logger.info("开始测试LLM配置...")
    
    # 加载配置
    config = load_config()
    if not config:
        logger.error("无法加载配置文件")
        return False
    
    # 创建LLM客户端
    try:
        llm_client = LLMClient(config)
        logger.info("LLM客户端创建成功")
    except Exception as e:
        logger.error(f"创建LLM客户端失败: {e}")
        return False
    
    # 获取模型信息
    model_info = llm_client.get_model_info()
    logger.info(f"当前模型信息: {model_info}")
    
    # 列出可用模型
    available_models = llm_client.list_available_models()
    logger.info(f"可用的模型: {available_models}")
    
    # 测试当前模型连接
    logger.info(f"测试当前模型 {llm_client.active_llm} 的连接...")
    connection_test = llm_client.test_connection()
    if connection_test:
        logger.info("当前模型连接测试成功")
    else:
        logger.warning("当前模型连接测试失败（可能是API密钥未配置或网络问题）")
    
    # 如果有多个可用模型，测试切换
    if len(available_models) > 1:
        logger.info("测试模型切换功能...")
        current_model = llm_client.active_llm
        
        for model in available_models:
            if model != current_model:
                logger.info(f"切换到模型: {model}")
                switch_result = llm_client.switch_model(model)
                if switch_result:
                    logger.info(f"成功切换到 {model}")
                    new_model_info = llm_client.get_model_info()
                    logger.info(f"新模型信息: {new_model_info}")
                    
                    # 测试新模型连接
                    logger.info(f"测试模型 {model} 的连接...")
                    connection_test = llm_client.test_connection()
                    if connection_test:
                        logger.info(f"模型 {model} 连接测试成功")
                    else:
                        logger.warning(f"模型 {model} 连接测试失败")
                else:
                    logger.error(f"切换到模型 {model} 失败")
        
        # 切换回原始模型
        logger.info(f"切换回原始模型: {current_model}")
        llm_client.switch_model(current_model)
    
    logger.info("LLM配置测试完成")
    return True

def print_config_example():
    """打印配置示例"""
    logger.info("统一OpenAI兼容格式配置示例:")
    example_config = """
# 在 config/config.yaml 中配置以下内容:

# LLM模型配置 - 所有模型都使用OpenAI兼容格式
llm_models:
  deepseek:
    api_key: "sk-your-deepseek-api-key"
    base_url: "https://api.deepseek.com/v1"
    model: "deepseek-chat"
    max_tokens: 8000
    temperature: 0.7
  
  openai:
    api_key: "sk-your-openai-api-key"
    base_url: "https://api.openai.com/v1"
    model: "gpt-4"
    max_tokens: 8000
    temperature: 0.7
  
  qwen:
    api_key: "sk-your-qwen-api-key"
    base_url: "https://dashscope.aliyuncs.com/compatible-mode/v1"
    model: "qwen-turbo"
    max_tokens: 8000
    temperature: 0.7
  
  ollama:
    api_key: "ollama"  # Ollama通常不需要真实密钥
    base_url: "http://localhost:11434/v1"
    model: "llama2"
    max_tokens: 8000
    temperature: 0.7

# 当前激活的LLM模型
active_llm: "deepseek"
"""
    print(example_config)

def test_model_switching():
    """测试模型切换功能"""
    logger.info("测试模型切换功能...")
    
    config = load_config()
    if not config:
        return False
    
    llm_client = LLMClient(config)
    available_models = llm_client.list_available_models()
    
    if len(available_models) <= 1:
        logger.info("只有一个可用模型，跳过切换测试")
        return True
    
    # 测试生成交易建议
    test_prompt = "请简单分析一下当前市场状况"
    
    for model in available_models:
        logger.info(f"测试模型: {model}")
        
        # 切换模型
        if llm_client.switch_model(model):
            logger.info(f"成功切换到模型: {model}")
            
            # 生成测试建议
            try:
                advice = llm_client.generate_trading_advice(test_prompt, retry_times=1)
                if advice:
                    logger.info(f"模型 {model} 生成建议成功")
                    logger.debug(f"建议内容: {advice[:100]}...")
                else:
                    logger.warning(f"模型 {model} 生成建议失败")
            except Exception as e:
                logger.error(f"模型 {model} 生成建议异常: {e}")
        else:
            logger.error(f"切换到模型 {model} 失败")
    
    return True

if __name__ == "__main__":
    logger.info("=== 统一LLM配置测试脚本 ===")
    
    # 运行基本测试
    success = test_llm_client()
    
    # 运行模型切换测试
    if success:
        success = test_model_switching()
    
    # 打印配置示例
    print_config_example()
    
    if success:
        logger.info("测试完成")
    else:
        logger.error("测试失败")
        sys.exit(1)