"""
BaoStock API 封装模块
"""
from datetime import datetime, timedelta
import pandas as pd
import baostock as bs


class BaoStockAPI:
    """BaoStock API 封装"""
    def __init__(self):
        self._login()
    
    def _login(self):
        """登录 BaoStock 系统"""
        lg = bs.login()
        print(f'登录 BaoStock 状态: error_code={lg.error_code}, error_msg={lg.error_msg}')
    
    def close(self):
        """关闭 API 连接"""
        bs.logout()
        print('BaoStock 登出成功')
    
    def _normalize_code(self, code: str) -> str:
        """将股票代码转换为小写格式"""
        code = code.strip()
        # 转换为小写
        code = code.lower()
        # 确保格式正确
        if code.startswith(("sh.", "sz.")) and len(code) >= 9:
            return code
        if len(code) == 6:
            if code.startswith(("60", "90", "51")):
                return "sh." + code
            else:
                return "sz." + code
        return code
    
    def fetch_stock_daily(self, code: str, start_date: str | None = None, last_n: int | None = None) -> pd.DataFrame | None:
        """从 BaoStock 获取单只股票的历史数据"""
        try:
            # 确定日期范围
            end_date = datetime.now()
            if start_date is None:
                # 如果没有指定开始日期，则获取近3个月的数据
                start_date = end_date - timedelta(days=30*3)
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            # 标准化股票代码
            normalized_code = self._normalize_code(code)
            
            # 获取股票历史数据
            rs = bs.query_history_k_data_plus(
                normalized_code,
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn",
                start_date=start_date_str,
                end_date=end_date_str,
                frequency="d",      # 'd' 表示日K线
                adjustflag="3"      # 复权类型：3 表示不复权
            )
            
            if rs.error_code != '0':
                print(f"获取股票 {code} 历史数据失败: {rs.error_msg}")
                return None
            
            # 处理数据
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            # 转换为 DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 标准化数据格式
            df = self._normalize_daily_df(df)
            
            # 如果指定了 last_n，返回最近 last_n 天的数据
            if last_n is not None and len(df) >= last_n:
                df = df.tail(last_n).copy()
                
            return df
        except Exception as e:
            print(f"获取股票数据异常: {e}")
            return None
    
    def _normalize_daily_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化数据框的列名和格式"""
        # 转换数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 处理日期列
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors="coerce")
            df = df.dropna(subset=['date'])
            df = df.sort_values(by='date', ascending=True)
        
        # 处理code列，将字母转换为大写
        if 'code' in df.columns:
            df['code'] = df['code'].str.upper()
        
        return df