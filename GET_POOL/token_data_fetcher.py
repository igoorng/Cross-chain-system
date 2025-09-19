#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代币池子信息获取工具
根据Excel文件中的网络和合约地址信息，从CoinMarketCap获取FDV、Liquidity和24h Volume数据
"""

import pandas as pd
import requests
import time
import re
from bs4 import BeautifulSoup
import logging
from typing import Tuple, Optional
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TokenDataFetcher:
    def __init__(self, excel_file: str = "test.xlsx", max_workers: int = 5):
        """
        初始化数据获取器
        
        Args:
            excel_file: Excel文件路径
            max_workers: 最大线程数
        """
        self.excel_file = excel_file
        self.max_workers = max_workers
        self.base_url = "https://dex.coinmarketcap.com/token"
        self.session = requests.Session()
        self.lock = threading.Lock()  # 线程锁用于文件写入
        # 设置请求头，模拟浏览器访问
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def create_session(self):
        """创建新的session对象用于线程安全"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        return session
        
    def load_excel_data(self) -> pd.DataFrame:
        """
        加载Excel文件数据
        
        Returns:
            包含网络和合约地址信息的DataFrame
        """
        try:
            df = pd.read_excel(self.excel_file)
            logger.info(f"成功加载Excel文件: {self.excel_file}")
            logger.info(f"数据行数: {len(df)}")
            return df
        except FileNotFoundError:
            logger.error(f"Excel文件不存在: {self.excel_file}")
            raise
        except Exception as e:
            logger.error(f"加载Excel文件时出错: {e}")
            raise
    
    def fetch_token_data(self, network: str, contract_address: str, session: requests.Session = None) -> Tuple[str, str, str]:
        """
        获取代币数据
        
        Args:
            network: 网络名称
            contract_address: 合约地址
            session: 可选的session对象
            
        Returns:
            (FDV, Liquidity, 24h Volume) 的元组
        """
        url = f"{self.base_url}/{network}/{contract_address}/"
        
        # 使用传入的session或创建新的
        if session is None:
            session = self.create_session()
        
        try:
            logger.info(f"正在请求: {url}")
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                return self.parse_html_data(response.text)
            elif response.status_code == 404:
                logger.warning(f"页面不存在 (404): {url}")
                return "0", "0", "0"
            else:
                logger.warning(f"请求失败，状态码: {response.status_code}")
                return "0", "0", "0"
                
        except requests.RequestException as e:
            logger.error(f"请求异常: {e}")
            return "0", "0", "0"
    
    def parse_html_data(self, html_content: str) -> Tuple[str, str, str]:
        """
        解析HTML内容，提取FDV、Liquidity和24h Volume数据
        
        Args:
            html_content: HTML内容
            
        Returns:
            (FDV, Liquidity, 24h Volume) 的元组
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 方法1: 优先使用新的正则表达式模式检查是否获取到3个数据
            fdv, liquidity, volume_24h = self.extract_all_values_by_pattern(html_content)
            
            # 如果新模式没有获取到有效数据，尝试其他方法
            if fdv == "0" and liquidity == "0" and volume_24h == "0":
                # 方法2: 尝试查找指定的HTML模式
                fdv = self.extract_value_by_pattern(html_content, "FDV")
                liquidity = self.extract_value_by_pattern(html_content, "liq")
                volume_24h = self.extract_value_by_pattern(html_content, "24h VOL")
                
                # 方法3: 如果方法2失败，尝试查找常见的数据显示模式
                if not fdv:
                    fdv = self.extract_value_by_class(soup, ["fdv", "market-cap", "fully-diluted"])
                if not liquidity:
                    liquidity = self.extract_value_by_class(soup, ["liquidity", "liq"])
                if not volume_24h:
                    volume_24h = self.extract_value_by_class(soup, ["volume", "24h-volume", "vol"])
                
                # 方法4: 尝试从JSON数据中提取
                if not any([fdv, liquidity, volume_24h]):
                    fdv, liquidity, volume_24h = self.extract_from_script_data(html_content)
            
            return fdv or "0", liquidity or "0", volume_24h or "0"
            
        except Exception as e:
            logger.error(f"解析HTML时出错: {e}")
            return "0", "0", "0"
    
    def extract_value_by_pattern(self, html_content: str, label: str) -> Optional[str]:
        """
        根据指定模式提取数值
        
        Args:
            html_content: HTML内容
            label: 要查找的标签
            
        Returns:
            提取的值或None
        """
        try:
            # 根据文档中提到的HTML模式进行匹配
            pattern = rf'</svg></span></div><dd class="static-box-value"><span class="sc-65e7f566-0 bxaIIt base-text"><span>([^<]+)</span></span>'
            
            # 在label附近查找对应的值
            label_pattern = rf'{label}.*?' + pattern
            match = re.search(label_pattern, html_content, re.IGNORECASE | re.DOTALL)
            
            if match:
                value = match.group(1).strip()
                logger.info(f"找到 {label}: {value}")
                return value
                
            # 备用模式匹配
            backup_patterns = [
                rf'{label}[^>]*>([^<]+)<',
                rf'"{label}"[^>]*>([^<]+)<',
                rf'class="[^"]*{label.lower()}[^"]*"[^>]*>([^<]+)<'
            ]
            
            for pattern in backup_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    logger.info(f"找到 {label} (备用模式): {value}")
                    return value
                    
        except Exception as e:
            logger.error(f"提取 {label} 时出错: {e}")
        
        return None
    
    def extract_all_values_by_pattern(self, html_content: str) -> Tuple[str, str, str]:
        """
        使用指定模式提取所有数值，检查是否获取到3个数据
        
        Args:
            html_content: HTML内容
            
        Returns:
            (FDV, Liquidity, 24h Volume) 的元组
        """
        try:
            # 使用您提供的正则表达式模式
            pattern = r'</svg></span></div><dd class="static-box-value"><span class="sc-65e7f566-0 bxaIIt base-text"><span>([^<]+)</span></span>'
            
            # 查找所有匹配的数据
            matches = re.findall(pattern, html_content)
            
            logger.info(f"使用正则表达式找到 {len(matches)} 个数据: {matches}")
            
            # 检查是否正好是3个数据
            if len(matches) == 3:
                # 返回找到的3个数据
                fdv = matches[0].strip()
                liquidity = matches[1].strip() 
                volume_24h = matches[2].strip()
                logger.info(f"正好找到3个数据: FDV={fdv}, Liquidity={liquidity}, Volume={volume_24h}")
                return fdv, liquidity, volume_24h
            else:
                # 如果不是3个数据，都填0
                logger.warning(f"数据数量不是3个（实际: {len(matches)}个），全部填0")
                return "0", "0", "0"
                
        except Exception as e:
            logger.error(f"使用正则表达式提取数据时出错: {e}")
            return "0", "0", "0"
    
    def extract_value_by_class(self, soup: BeautifulSoup, class_keywords: list) -> Optional[str]:
        """
        根据CSS类名关键字提取数值
        
        Args:
            soup: BeautifulSoup对象
            class_keywords: 类名关键字列表
            
        Returns:
            提取的值或None
        """
        try:
            for keyword in class_keywords:
                # 查找包含关键字的类名
                elements = soup.find_all(class_=re.compile(keyword, re.IGNORECASE))
                for element in elements:
                    text = element.get_text(strip=True)
                    if text and self.is_numeric_value(text):
                        return text
                        
                # 查找data属性包含关键字的元素
                elements = soup.find_all(attrs={"data-key": re.compile(keyword, re.IGNORECASE)})
                for element in elements:
                    text = element.get_text(strip=True)
                    if text and self.is_numeric_value(text):
                        return text
                        
        except Exception as e:
            logger.error(f"按类名提取数值时出错: {e}")
        
        return None
    
    def extract_from_script_data(self, html_content: str) -> Tuple[str, str, str]:
        """
        从页面的JavaScript数据中提取信息
        
        Args:
            html_content: HTML内容
            
        Returns:
            (FDV, Liquidity, 24h Volume) 的元组
        """
        try:
            # 查找JSON数据
            script_patterns = [
                r'window\.__NEXT_DATA__\s*=\s*({.+?});',
                r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
                r'"price":\s*"([^"]+)"',
                r'"fdv":\s*"([^"]+)"',
                r'"liquidity":\s*"([^"]+)"',
                r'"volume24h":\s*"([^"]+)"'
            ]
            
            fdv = liquidity = volume_24h = None
            
            for pattern in script_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    # 这里可以进一步解析JSON数据
                    logger.info(f"找到脚本数据匹配: {len(matches)} 个")
                    break
            
            return fdv or "0", liquidity or "0", volume_24h or "0"
            
        except Exception as e:
            logger.error(f"从脚本数据提取信息时出错: {e}")
            return "0", "0", "0"
    
    def is_numeric_value(self, text: str) -> bool:
        """
        检查文本是否为数值型
        
        Args:
            text: 要检查的文本
            
        Returns:
            是否为数值型
        """
        # 移除常见的格式字符
        cleaned = re.sub(r'[$,\s%]', '', text)
        
        # 检查是否为数字、科学计数法或包含K、M、B等单位
        patterns = [
            r'^\d+\.?\d*$',  # 普通数字
            r'^\d+\.?\d*[KMB]$',  # 带单位的数字
            r'^\d+\.?\d*e[+-]?\d+$',  # 科学计数法
        ]
        
        return any(re.match(pattern, cleaned, re.IGNORECASE) for pattern in patterns)
    
    def process_single_row(self, args: tuple) -> tuple:
        """
        处理单个行的数据
        
        Args:
            args: (index, network, contract_address) 的元组
            
        Returns:
            (index, fdv, liquidity, volume_24h) 的元组
        """
        index, network, contract_address = args
        
        try:
            # 为每个线程创建独立的session
            session = self.create_session()
            
            if pd.isna(network) or pd.isna(contract_address) or not str(network).strip() or not str(contract_address).strip():
                logger.warning(f"第 {index+1} 行数据不完整，跳过")
                return index, "0", "0", "0"
            
            network = str(network).strip()
            contract_address = str(contract_address).strip()
            
            logger.info(f"处理第 {index+1} 行: {network} - {contract_address}")
            
            # 获取数据
            fdv, liquidity, volume_24h = self.fetch_token_data(network, contract_address, session)
            
            logger.info(f"第 {index+1} 行数据获取完成: FDV={fdv}, Liquidity={liquidity}, Volume={volume_24h}")
            
            # 添加小延迟避免请求过快
            time.sleep(1)
            
            return index, fdv, liquidity, volume_24h
            
        except Exception as e:
            logger.error(f"处理第 {index+1} 行时出错: {e}")
            return index, "0", "0", "0"
    
    def process_excel_file(self) -> None:
        """
        处理Excel文件，使用多线程获取所有代币数据并写回文件
        """
        try:
            # 加载数据
            df = self.load_excel_data()
            
            # 确保有足够的列
            while len(df.columns) < 5:
                df[f'Column_{len(df.columns)}'] = None
            
            # 设置列名
            df.columns = list(df.columns[:2]) + ['FDV', 'Liquidity', '24h Volume']
            
            # 准备线程池参数
            tasks = []
            for index, row in df.iterrows():
                network = row.iloc[0]
                contract_address = row.iloc[1]
                tasks.append((index, network, contract_address))
            
            logger.info(f"开始使用 {self.max_workers} 个线程处理 {len(tasks)} 个任务")
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有任务
                future_to_index = {executor.submit(self.process_single_row, task): task[0] for task in tasks}
                
                # 获取结果
                for future in as_completed(future_to_index):
                    index = future_to_index[future]
                    try:
                        result_index, fdv, liquidity, volume_24h = future.result()
                        
                        # 线程安全地写入数据
                        with self.lock:
                            df.loc[result_index, 'FDV'] = fdv
                            df.loc[result_index, 'Liquidity'] = liquidity
                            df.loc[result_index, '24h Volume'] = volume_24h
                        
                        logger.info(f"完成任务 {result_index+1}/{len(tasks)}")
                        
                    except Exception as e:
                        logger.error(f"获取第 {index+1} 行结果时出错: {e}")
                        with self.lock:
                            df.loc[index, 'FDV'] = "0"
                            df.loc[index, 'Liquidity'] = "0"
                            df.loc[index, '24h Volume'] = "0"
            
            # 保存结果
            output_file = self.excel_file.replace('.xlsx', '_result.xlsx')
            df.to_excel(output_file, index=False)
            logger.info(f"结果已保存到: {output_file}")
            
            # 同时保存到原文件
            df.to_excel(self.excel_file, index=False)
            logger.info(f"结果已更新到原文件: {self.excel_file}")
            
        except Exception as e:
            logger.error(f"处理Excel文件时出错: {e}")
            raise
    
    def process_excel_file_single_thread(self) -> None:
        """
        单线程版本的Excel文件处理方法（保留作为备用）
        """
        try:
            # 加载数据
            df = self.load_excel_data()
            
            # 确保有足够的列
            while len(df.columns) < 5:
                df[f'Column_{len(df.columns)}'] = None
            
            # 设置列名
            df.columns = list(df.columns[:2]) + ['FDV', 'Liquidity', '24h Volume']
            
            # 处理每一行数据
            for index, row in df.iterrows():
                try:
                    network = str(row.iloc[0]).strip()
                    contract_address = str(row.iloc[1]).strip()
                    
                    if pd.isna(network) or pd.isna(contract_address) or not network or not contract_address:
                        logger.warning(f"第 {index+1} 行数据不完整，跳过")
                        df.loc[index, 'FDV'] = "0"
                        df.loc[index, 'Liquidity'] = "0" 
                        df.loc[index, '24h Volume'] = "0"
                        continue
                    
                    logger.info(f"处理第 {index+1} 行: {network} - {contract_address}")
                    
                    # 获取数据
                    fdv, liquidity, volume_24h = self.fetch_token_data(network, contract_address)
                    
                    # 写入数据
                    df.loc[index, 'FDV'] = fdv
                    df.loc[index, 'Liquidity'] = liquidity
                    df.loc[index, '24h Volume'] = volume_24h
                    
                    logger.info(f"第 {index+1} 行数据获取完成: FDV={fdv}, Liquidity={liquidity}, Volume={volume_24h}")
                    
                    # 添加延迟避免请求过快
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"处理第 {index+1} 行时出错: {e}")
                    df.loc[index, 'FDV'] = "0"
                    df.loc[index, 'Liquidity'] = "0"
                    df.loc[index, '24h Volume'] = "0"
            
            # 保存结果
            output_file = self.excel_file.replace('.xlsx', '_result.xlsx')
            df.to_excel(output_file, index=False)
            logger.info(f"结果已保存到: {output_file}")
            
            # 同时保存到原文件
            df.to_excel(self.excel_file, index=False)
            logger.info(f"结果已更新到原文件: {self.excel_file}")
            
        except Exception as e:
            logger.error(f"处理Excel文件时出错: {e}")
            raise

def main():
    """主函数"""
    try:
        # 创建数据获取器，可以调整max_workers参数来控制线程数
        # max_workers=5 表示同时使用5个线程，可根据需要调整
        fetcher = TokenDataFetcher("test.xlsx", max_workers=10)
        
        # 使用多线程处理Excel文件
        logger.info("开始使用多线程处理Excel文件...")
        fetcher.process_excel_file()
        
        # 如果需要使用单线程版本，可以注释上面一行，取消下面一行的注释
        # fetcher.process_excel_file_single_thread()
        
        logger.info("所有数据处理完成！")
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")

if __name__ == "__main__":
    main()