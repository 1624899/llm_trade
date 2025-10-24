"""
全球宏观日历数据获取模块
使用AKShare从华尔街见闻获取全球宏观经济事件和财经日历数据
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from loguru import logger
import os
import json


class GlobalMacroCalendar:
    """全球宏观日历数据获取器"""
    
    def __init__(self, data_dir: str = "data/Macro events"):
        """
        初始化宏观日历获取器
        
        Args:
            data_dir: 数据保存目录
        """
        self.data_dir = data_dir
        self._ensure_data_dir()
        
        # 重要性等级映射
        self.importance_map = {
            1: "低",
            2: "中", 
            3: "高"
        }
        
        # 地区映射
        self.region_map = {
            "US": "美国",
            "EU": "欧元区", 
            "CN": "中国",
            "JP": "日本",
            "GB": "英国",
            "DE": "德国",
            "FR": "法国",
            "IT": "意大利",
            "CA": "加拿大",
            "AU": "澳大利亚",
            "CH": "瑞士",
            "SE": "瑞典",
            "NO": "挪威",
            "NZ": "新西兰",
            "KR": "韩国",
            "SG": "新加坡",
            "IN": "印度",
            "BR": "巴西",
            "RU": "俄罗斯",
            "ZA": "南非"
        }
        
        logger.info("全球宏观日历获取器初始化完成")
    
    def _ensure_data_dir(self) -> None:
        """确保数据目录存在"""
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _clean_old_csv_files(self, file_pattern: str = None) -> None:
        """
        清理目录中的旧CSV文件
        
        Args:
            file_pattern: 文件名模式，如果为None则删除所有CSV文件
        """
        try:
            if not os.path.exists(self.data_dir):
                return
                
            for filename in os.listdir(self.data_dir):
                if filename.endswith('.csv'):
                    # 如果指定了文件模式，只删除匹配的文件
                    if file_pattern and not filename.startswith(file_pattern):
                        continue
                        
                    file_path = os.path.join(self.data_dir, filename)
                    os.remove(file_path)
                    logger.info(f"已删除旧的CSV文件: {file_path}")
        except Exception as e:
            logger.error(f"清理旧CSV文件失败: {e}")
    
    def get_macro_calendar_by_date(self, date_str: str) -> Optional[pd.DataFrame]:
        """
        获取指定日期的宏观日历数据
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD
            
        Returns:
            宏观日历数据DataFrame
        """
        try:
            logger.info(f"获取{date_str}的宏观日历数据")
            
            # 使用AKShare获取华尔街见闻的宏观日历数据
            df = ak.macro_info_ws(date=date_str)
            
            if df is None or df.empty:
                logger.warning(f"{date_str}的宏观日历数据为空")
                return None
            
            # 数据预处理
            df = self._preprocess_macro_data(df)
            
            logger.info(f"成功获取{date_str}的宏观日历数据，共{len(df)}条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取{date_str}宏观日历数据失败: {e}")
            return None
    
    def get_today_high_importance_events(self) -> Optional[pd.DataFrame]:
        """
        获取当天的高重要性财经事件
        
        Returns:
            高重要性事件DataFrame
        """
        today = datetime.now().strftime("%Y%m%d")
        df = self.get_macro_calendar_by_date(today)
        
        if df is None:
            return None
        
        # 筛选高重要性事件（重要性 >= 2）
        high_importance_df = df[df['重要性'] >= 2].copy()
        
        logger.info(f"今天({today})的高重要性事件共{len(high_importance_df)}条")
        return high_importance_df
    
    def get_today_all_events(self) -> Optional[pd.DataFrame]:
        """
        获取当天的所有财经事件
        
        Returns:
            所有事件DataFrame
        """
        today = datetime.now().strftime("%Y%m%d")
        df = self.get_macro_calendar_by_date(today)
        
        if df is None:
            return None
        
        logger.info(f"今天({today})的所有事件共{len(df)}条")
        return df
    
    def get_next_7_days_high_importance_events(self) -> Dict[str, pd.DataFrame]:
        """
        获取未来7天的高重要性财经事件
        
        Returns:
            按日期分组的高重要性事件字典
        """
        results = {}
        
        for i in range(7):
            target_date = datetime.now() + timedelta(days=i)
            date_str = target_date.strftime("%Y%m%d")
            date_display = target_date.strftime("%Y-%m-%d")
            
            df = self.get_macro_calendar_by_date(date_str)
            
            if df is not None:
                # 筛选高重要性事件
                high_importance_df = df[df['重要性'] >= 2].copy()
                
                if not high_importance_df.empty:
                    results[date_display] = high_importance_df
                    logger.info(f"{date_display}的高重要性事件共{len(high_importance_df)}条")
                else:
                    logger.info(f"{date_display}没有高重要性事件")
            else:
                logger.warning(f"无法获取{date_display}的宏观日历数据")
        
        total_events = sum(len(df) for df in results.values())
        logger.info(f"未来7天高重要性事件总计{total_events}条")
        
        return results
    
    def get_next_7_days_all_events(self) -> Dict[str, pd.DataFrame]:
        """
        获取未来7天的所有财经事件
        
        Returns:
            按日期分组的所有事件字典
        """
        results = {}
        
        for i in range(7):
            target_date = datetime.now() + timedelta(days=i)
            date_str = target_date.strftime("%Y%m%d")
            date_display = target_date.strftime("%Y-%m-%d")
            
            df = self.get_macro_calendar_by_date(date_str)
            
            if df is not None:
                if not df.empty:
                    results[date_display] = df
                    logger.info(f"{date_display}的所有事件共{len(df)}条")
                else:
                    logger.info(f"{date_display}没有事件")
            else:
                logger.warning(f"无法获取{date_display}的宏观日历数据")
        
        total_events = sum(len(df) for df in results.values())
        logger.info(f"未来7天所有事件总计{total_events}条")
        
        return results
    
    def get_macro_events_by_importance(self, date_str: str, min_importance: int = 2) -> Optional[pd.DataFrame]:
        """
        根据重要性级别获取宏观事件
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD
            min_importance: 最小重要性级别（1=低，2=中，3=高）
            
        Returns:
            指定重要性级别的事件DataFrame
        """
        df = self.get_macro_calendar_by_date(date_str)
        
        if df is None:
            return None
        
        filtered_df = df[df['重要性'] >= min_importance].copy()
        logger.info(f"{date_str}重要性>={min_importance}的事件共{len(filtered_df)}条")
        
        return filtered_df
    
    def _preprocess_macro_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理宏观日历数据
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            预处理后的DataFrame
        """
        try:
            # 创建数据副本避免修改原始数据
            processed_df = df.copy()
            
            # 添加重要性描述
            if '重要性' in processed_df.columns:
                processed_df['importance_desc'] = processed_df['重要性'].map(self.importance_map)
                # 同时保留英文列名以便兼容
                processed_df['importance'] = processed_df['重要性']
            
            # 添加地区描述
            if '地区' in processed_df.columns:
                processed_df['region_desc'] = processed_df['地区'].map(self.region_map)
                # 对于没有映射的地区，使用原始代码
                processed_df['region_desc'] = processed_df['region_desc'].fillna(processed_df['地区'])
                # 同时保留英文列名以便兼容
                processed_df['region'] = processed_df['地区']
            
            # 时间格式化
            if '时间' in processed_df.columns:
                processed_df['time'] = pd.to_datetime(processed_df['时间'], errors='coerce')
                # 同时保留中文列名
                processed_df['时间'] = processed_df['time']
            
            # 数值列处理
            numeric_columns = ['今值', '预期', '前值']
            english_columns = ['actual', 'expected', 'previous']
            for chinese_col, english_col in zip(numeric_columns, english_columns):
                if chinese_col in processed_df.columns:
                    processed_df[english_col] = pd.to_numeric(processed_df[chinese_col], errors='coerce')
            
            # 添加数据获取时间
            processed_df['fetch_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return processed_df
            
        except Exception as e:
            logger.error(f"预处理宏观日历数据失败: {e}")
            return df
    
    def save_macro_data_to_csv(self, df: pd.DataFrame, date_str: str, suffix: str = "") -> str:
        """
        保存宏观日历数据到CSV文件
        
        Args:
            df: 要保存的数据DataFrame
            date_str: 日期字符串
            suffix: 文件名后缀
            
        Returns:
            保存的文件路径
        """
        try:
            if df is None or df.empty:
                logger.warning("没有数据可保存")
                return ""
            
            # 清理旧的macro_calendar CSV文件
            self._clean_old_csv_files("macro_calendar")
            
            # 生成文件名
            date_display = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            filename = f"macro_calendar_{date_display}{suffix}.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            # 只保留地区为"中国"或"美国"的条目，并排除链接列
            columns_to_exclude = ['链接']
            df_filtered = df.drop(columns=columns_to_exclude, errors='ignore')
            
            # 过滤地区，只保留中国和美国
            if '地区' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['地区'].isin(['中国', '美国'])]
            elif 'region' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['region'].isin(['中国', '美国'])]
            
            df_filtered.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            logger.info(f"宏观日历数据已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存宏观日历数据失败: {e}")
            return ""
    
    def save_macro_data_to_csv(self, data: Dict, filename: str) -> str:
        """
        保存宏观日历数据到CSV文件（合并所有日期的数据）
        
        Args:
            data: 要保存的数据字典
            filename: 文件名
            
        Returns:
            保存的文件路径
        """
        try:
            # 清理旧的next_7_days CSV文件
            if filename.startswith("next_7_days"):
                self._clean_old_csv_files("next_7_days")
            
            filepath = os.path.join(self.data_dir, filename)
            
            # 合并所有日期的数据
            all_data = []
            for date, df in data.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # 添加日期列
                    df_copy = df.copy()
                    df_copy['事件日期'] = date
                    all_data.append(df_copy)
            
            if not all_data:
                logger.warning("没有数据可保存到CSV")
                return ""
            
            # 合并所有DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 只保留地区为"中国"或"美国"的条目，并排除链接列
            columns_to_exclude = ['链接']
            combined_df_filtered = combined_df.drop(columns=columns_to_exclude, errors='ignore')
            
            # 过滤地区，只保留中国和美国
            if '地区' in combined_df_filtered.columns:
                combined_df_filtered = combined_df_filtered[combined_df_filtered['地区'].isin(['中国', '美国'])]
            elif 'region' in combined_df_filtered.columns:
                combined_df_filtered = combined_df_filtered[combined_df_filtered['region'].isin(['中国', '美国'])]
            
            combined_df_filtered.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            logger.info(f"宏观日历数据已保存到CSV: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存宏观日历CSV数据失败: {e}")
            return ""
    
    def format_macro_events_summary(self, df: pd.DataFrame) -> str:
        """
        格式化宏观事件摘要
        
        Args:
            df: 宏观事件DataFrame
            
        Returns:
            格式化的摘要字符串
        """
        if df is None or df.empty:
            return "暂无宏观事件数据"
        
        summary_lines = []
        summary_lines.append("=" * 80)
        summary_lines.append("全球宏观财经事件摘要")
        summary_lines.append("=" * 80)
        
        # 按重要性分组统计
        if '重要性' in df.columns:
            importance_counts = df['重要性'].value_counts().sort_index(ascending=False)
            summary_lines.append("\n重要性分布:")
            for importance, count in importance_counts.items():
                importance_desc = self.importance_map.get(importance, f"级别{importance}")
                summary_lines.append(f"  {importance_desc}重要性: {count}条")
        
        # 按地区分组统计
        if '地区' in df.columns:
            region_counts = df['地区'].value_counts().head(10)
            summary_lines.append("\n地区分布(前10):")
            for region, count in region_counts.items():
                region_desc = self.region_map.get(region, region)
                summary_lines.append(f"  {region_desc}: {count}条")
        
        # 详细事件列表
        summary_lines.append("\n详细事件列表:")
        summary_lines.append("-" * 80)
        
        for _, row in df.iterrows():
            time_str = ""
            if 'time' in row and pd.notna(row['time']):
                time_str = f" [{row['time'].strftime('%H:%M')}]"
            elif '时间' in row and pd.notna(row['时间']):
                time_str = f" [{pd.to_datetime(row['时间']).strftime('%H:%M')}]"
            
            region_desc = ""
            if 'region_desc' in row and pd.notna(row['region_desc']):
                region_desc = f"[{row['region_desc']}]"
            elif '地区' in row and pd.notna(row['地区']):
                region_desc = f"[{row['地区']}]"
            
            importance_desc = ""
            if 'importance_desc' in row and pd.notna(row['importance_desc']):
                importance_desc = f"[{row['importance_desc']}]"
            elif '重要性' in row and pd.notna(row['重要性']):
                importance_desc = f"[{self.importance_map.get(row['重要性'], '未知')}]"
            
            event_desc = row.get('事件', '未知事件')
            
            # 添加数值信息
            values_info = []
            for col in ['actual', 'expected', 'previous']:
                if col in row and pd.notna(row[col]):
                    col_desc = {'actual': '实际值', 'expected': '预期值', 'previous': '前值'}[col]
                    values_info.append(f"{col_desc}:{row[col]}")
            # 也检查中文列名
            for col in ['今值', '预期', '前值']:
                if col in row and pd.notna(row[col]):
                    values_info.append(f"{col}:{row[col]}")
            
            values_str = f" ({', '.join(values_info)})" if values_info else ""
            
            event_line = f"{time_str} {region_desc} {importance_desc} {event_desc}{values_str}"
            summary_lines.append(event_line)
        
        summary_lines.append("=" * 80)
        
        return "\n".join(summary_lines)
    
    def get_today_events_summary(self) -> str:
        """
        获取今天事件的格式化摘要
        
        Returns:
            格式化的今天事件摘要
        """
        today_events = self.get_today_high_importance_events()
        return self.format_macro_events_summary(today_events)
    
    def get_next_7_days_events_summary(self) -> str:
        """
        获取未来7天事件的格式化摘要
        
        Returns:
            格式化的未来7天事件摘要
        """
        next_7_days_events = self.get_next_7_days_high_importance_events()
        
        if not next_7_days_events:
            return "未来7天没有高重要性财经事件"
        
        summary_lines = []
        summary_lines.append("=" * 80)
        summary_lines.append("未来7天高重要性财经事件摘要")
        summary_lines.append("=" * 80)
        
        total_events = sum(len(df) for df in next_7_days_events.values())
        summary_lines.append(f"\n总计: {total_events}条高重要性事件")
        
        for date, df in next_7_days_events.items():
            summary_lines.append(f"\n📅 {date} ({len(df)}条事件)")
            summary_lines.append("-" * 60)
            
            for _, row in df.iterrows():
                time_str = ""
                if 'time' in row and pd.notna(row['time']):
                    time_str = f" [{row['time'].strftime('%H:%M')}]"
                elif '时间' in row and pd.notna(row['时间']):
                    time_str = f" [{pd.to_datetime(row['时间']).strftime('%H:%M')}]"
                
                region_desc = ""
                if 'region_desc' in row and pd.notna(row['region_desc']):
                    region_desc = f"[{row['region_desc']}]"
                elif '地区' in row and pd.notna(row['地区']):
                    region_desc = f"[{row['地区']}]"
                
                importance_desc = ""
                if 'importance_desc' in row and pd.notna(row['importance_desc']):
                    importance_desc = f"[{row['importance_desc']}]"
                elif '重要性' in row and pd.notna(row['重要性']):
                    importance_desc = f"[{self.importance_map.get(row['重要性'], '未知')}]"
                
                event_desc = row.get('事件', '未知事件')
                
                # 添加数值信息
                values_info = []
                for col in ['actual', 'expected', 'previous']:
                    if col in row and pd.notna(row[col]):
                        col_desc = {'actual': '实际值', 'expected': '预期值', 'previous': '前值'}[col]
                        values_info.append(f"{col_desc}:{row[col]}")
                # 也检查中文列名
                for col in ['今值', '预期', '前值']:
                    if col in row and pd.notna(row[col]):
                        values_info.append(f"{col}:{row[col]}")
                
                values_str = f" ({', '.join(values_info)})" if values_info else ""
                
                event_line = f"{time_str} {region_desc} {importance_desc} {event_desc}{values_str}"
                summary_lines.append(event_line)
        
        summary_lines.append("=" * 80)
        
        return "\n".join(summary_lines)
    
    def save_daily_macro_data(self) -> Tuple[str, str]:
        """
        保存当天的宏观日历数据
        
        Returns:
            (当天数据文件路径, 未来7天数据文件路径)
        """
        today = datetime.now().strftime("%Y%m%d")
        
        # 获取并保存当天的所有事件（包括重要性为1的事件）
        today_events = self.get_today_all_events()
        today_file = ""
        if today_events is not None and not today_events.empty:
            today_file = self.save_macro_data_to_csv(today_events, f"{today}_all_importance")
        
        # 获取并保存未来7天的所有事件（包括重要性为1的事件）
        next_7_days_events = self.get_next_7_days_all_events()
        next_7_days_file = ""
        if next_7_days_events:
            filename = f"next_7_days_all_importance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            next_7_days_file = self.save_macro_data_to_csv(next_7_days_events, filename)
        
        return today_file, next_7_days_file
    
    def get_baidu_economic_events(self, date_str: str = None) -> Optional[pd.DataFrame]:
        """
        获取百度经济事件数据
        
        Args:
            date_str: 日期字符串，格式为YYYYMMDD，默认为今天
            
        Returns:
            百度经济事件数据DataFrame
        """
        try:
            if date_str is None:
                date_str = datetime.now().strftime("%Y%m%d")
                
            logger.info(f"获取{date_str}的百度经济事件数据")
            
            # 使用AKShare获取百度经济事件数据
            df = ak.news_economic_baidu(date=date_str)
            
            if df is None or df.empty:
                logger.warning(f"{date_str}的百度经济事件数据为空")
                return None
            
            logger.info(f"成功获取{date_str}的百度经济事件数据，共{len(df)}条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取{date_str}百度经济事件数据失败: {e}")
            return None
    
    def save_baidu_economic_data_to_csv(self, df: pd.DataFrame, date_str: str) -> str:
        """
        保存百度经济事件数据到CSV文件
        
        Args:
            df: 要保存的数据DataFrame
            date_str: 日期字符串
            
        Returns:
            保存的文件路径
        """
        try:
            if df is None or df.empty:
                logger.warning("没有百度经济事件数据可保存")
                return ""
            
            # 清理旧的baidu_economic CSV文件
            self._clean_old_csv_files("baidu_economic")
            
            # 生成文件名
            filename = f"baidu_economic_events_{date_str}_{datetime.now().strftime('%H%M%S')}.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            # 只保留地区为"中国"或"美国"的条目，并排除链接列
            columns_to_exclude = ['链接']
            df_filtered = df.drop(columns=columns_to_exclude, errors='ignore')
            
            # 过滤地区，只保留中国和美国
            if '地区' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['地区'].isin(['中国', '美国'])]
            elif 'region' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['region'].isin(['中国', '美国'])]
            
            df_filtered.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            logger.info(f"百度经济事件数据已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存百度经济事件数据失败: {e}")
            return ""


def main():
    """主函数，用于测试"""
    calendar = GlobalMacroCalendar()
    
    print("=" * 80)
    print("全球宏观日历数据获取测试")
    print("=" * 80)
    
    # 测试获取当天高重要性事件
    print("\n1. 获取当天高重要性事件:")
    today_summary = calendar.get_today_events_summary()
    print(today_summary)
    
    # 测试获取未来7天高重要性事件
    print("\n2. 获取未来7天高重要性事件:")
    next_7_days_summary = calendar.get_next_7_days_events_summary()
    print(next_7_days_summary)
    
    # 保存数据
    print("\n3. 保存数据:")
    today_file, next_7_days_file = calendar.save_daily_macro_data()
    if today_file:
        print(f"当天数据已保存到: {today_file}")
    if next_7_days_file:
        print(f"未来7天数据已保存到: {next_7_days_file}")

    # 测试百度经济事件数据获取功能
    print("\n4. 测试百度经济事件数据获取功能:")
    baidu_date = datetime.now().strftime("%Y%m%d")
    baidu_economic_data = calendar.get_baidu_economic_events(baidu_date)
    if baidu_economic_data is not None and not baidu_economic_data.empty:
        print(f"成功获取{baidu_date}的百度经济事件数据，共{len(baidu_economic_data)}条记录")
        print("前5条数据:")
        print(baidu_economic_data.head())
    else:
        print(f"未能获取{baidu_date}的百度经济事件数据")

    # 保存百度经济事件数据到CSV文件
    print("\n5. 保存百度经济事件数据到CSV文件:")
    
    # 保存百度经济事件数据
    if baidu_economic_data is not None and not baidu_economic_data.empty:
        baidu_file = calendar.save_baidu_economic_data_to_csv(baidu_economic_data, baidu_date)
        if baidu_file:
            print(f"百度经济事件数据已保存到: {baidu_file}")

if __name__ == "__main__":
    main()