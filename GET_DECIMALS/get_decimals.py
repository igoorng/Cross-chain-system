#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取ERC20代币合约精度(decimals)工具
通过Web3连接各种区块链网络获取代币合约的精度信息
"""

import pandas as pd
import requests
import time
import logging
from typing import Dict, Optional
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DecimalsFetcher:
    def __init__(self, excel_file: str = "test.xlsx"):
        self.excel_file = excel_file
        
        # 配置各链的RPC节点 - 使用免费的公共节点
        self.rpc_endpoints = {
            'ethereum': 'https://eth.llamarpc.com',
            'bsc': 'https://bsc-dataseed.binance.org',
            'polygon': 'https://polygon-rpc.com',
            'arbitrum': 'https://arb1.arbitrum.io/rpc',
            'avalanche': 'https://api.avax.network/ext/bc/C/rpc',
            'fantom': 'https://rpc.ftm.tools',
            'optimism': 'https://mainnet.optimism.io',
            'base': 'https://mainnet.base.org'
        }
        
        # ERC20 decimals() 函数的方法签名
        self.decimals_signature = "0x313ce567"
        
    def get_rpc_endpoint(self, network: str) -> Optional[str]:
        """获取指定网络的RPC端点"""
        network_lower = network.lower()
        return self.rpc_endpoints.get(network_lower)
    
    def call_contract_method(self, rpc_url: str, contract_address: str, method_signature: str) -> Optional[str]:
        """
        调用合约方法
        """
        try:
            # 准备JSON-RPC请求
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [
                    {
                        "to": contract_address,
                        "data": method_signature
                    },
                    "latest"
                ],
                "id": 1
            }
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.post(rpc_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if 'result' in result and result['result']:
                    return result['result']
                else:
                    logger.warning(f"RPC调用返回空结果: {result}")
                    return None
            else:
                logger.error(f"RPC请求失败，状态码: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"调用合约方法时出错: {e}")
            return None
    
    def hex_to_decimal(self, hex_result: str) -> int:
        """
        将十六进制结果转换为十进制
        """
        try:
            if hex_result.startswith('0x'):
                hex_result = hex_result[2:]
            
            # 去除前导零
            hex_result = hex_result.lstrip('0')
            if not hex_result:
                hex_result = '0'
            
            return int(hex_result, 16)
        except Exception as e:
            logger.error(f"十六进制转换错误: {e}")
            return 18  # 默认返回18位精度
    
    def get_token_decimals(self, network: str, contract_address: str) -> int:
        """
        获取代币精度
        """
        try:
            # 获取RPC端点
            rpc_url = self.get_rpc_endpoint(network)
            if not rpc_url:
                logger.error(f"不支持的网络: {network}")
                return 18  # 默认精度
            
            # 确保合约地址格式正确
            if not contract_address.startswith('0x'):
                contract_address = '0x' + contract_address
            
            logger.info(f"正在查询 {network} 网络上的合约 {contract_address} 的精度...")
            
            # 调用decimals()方法
            result = self.call_contract_method(rpc_url, contract_address, self.decimals_signature)
            
            if result:
                decimals = self.hex_to_decimal(result)
                logger.info(f"成功获取精度: {decimals}")
                return decimals
            else:
                logger.warning(f"无法获取精度，使用默认值18")
                return 18
                
        except Exception as e:
            logger.error(f"获取代币精度时出错: {e}")
            return 18  # 默认精度
    
    def process_excel(self):
        """处理Excel文件，获取所有代币的精度"""
        try:
            # 读取Excel文件，保持原始索引
            df = pd.read_excel(self.excel_file)
            logger.info(f"成功读取Excel文件: {self.excel_file}, 共 {len(df)} 行数据")
            
            # 重置索引以确保按原始顺序处理
            df.reset_index(drop=True, inplace=True)
            
            # 添加精度列
            if 'decimals' not in df.columns:
                df['decimals'] = None
            
            # 处理每一行
            for index, row in df.iterrows():
                try:
                    network = str(row.iloc[0]).strip()
                    contract_address = str(row.iloc[1]).strip()
                    
                    if pd.isna(network) or pd.isna(contract_address) or network == 'nan' or contract_address == 'nan':
                        logger.warning(f"第 {index+1} 行数据不完整，设置精度为18")
                        df.at[index, 'decimals'] = 18
                        continue
                    
                    logger.info(f"处理第 {index+1} 行: 网络={network}, 合约地址={contract_address}")
                    
                    # 获取精度
                    decimals = self.get_token_decimals(network, contract_address)
                    
                    # 写入Excel
                    df.at[index, 'decimals'] = decimals
                    
                    logger.info(f"第 {index+1} 行完成: decimals={decimals}")
                    
                    # 延迟1秒避免请求过快
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"处理第 {index+1} 行时出错: {e}")
                    df.at[index, 'decimals'] = 18  # 默认精度
            
            # 创建备份
            backup_file = self.excel_file.replace('.xlsx', '_backup.xlsx')
            df_original = pd.read_excel(self.excel_file)
            df_original.to_excel(backup_file, index=False)
            logger.info(f"已创建备份文件: {backup_file}")
            
            # 保存结果，确保保持原始行顺序
            output_file = self.excel_file.replace('.xlsx', '_with_decimals.xlsx')
            # 按照原始索引顺序排序，确保行顺序不变
            df_sorted = df.sort_index()
            df_sorted.to_excel(output_file, index=False)
            logger.info(f"结果已保存到: {output_file}（保持原始行顺序）")
            
            # 显示统计信息
            logger.info(f"处理完成！")
            logger.info(f"总计处理: {len(df_sorted)} 个代币")
            logger.info(f"精度分布:")
            decimals_counts = df_sorted['decimals'].value_counts().sort_index()
            for decimals, count in decimals_counts.items():
                logger.info(f"  精度 {decimals}: {count} 个代币")
            
            return df_sorted
            
        except Exception as e:
            logger.error(f"处理Excel文件时出错: {e}")
            return None

def main():
    """主函数"""
    logger.info("=== 代币精度获取工具启动 ===")
    
    fetcher = DecimalsFetcher()
    result_df = fetcher.process_excel()
    
    if result_df is not None:
        logger.info("=== 处理完成 ===")
    else:
        logger.error("=== 处理失败 ===")

if __name__ == "__main__":
    main()