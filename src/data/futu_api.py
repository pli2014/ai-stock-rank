"""
Futu API 封装模块
"""
from datetime import datetime, timedelta
import pandas as pd
import time
from futu import OpenQuoteContext, Market, SecurityType, KLType, RET_OK
import socket

class FutuAPI:
    """Futu API 封装"""
    def __init__(self, host='127.0.0.1', port=11111, max_qps=120, time_window=60):
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在初始化 Futu API...")
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 尝试连接到 Futu OpenD 服务: {host}:{port}")
        self.host = host
        self.port = port
        self.quote_ctx = None
        self._connect()
        # API 调用频率控制
        self.max_qps = max_qps  # 最大 QPS
        self.time_window = time_window  # 时间窗口（秒）
        self.api_calls = []  # 存储 API 调用时间戳
    
    def _connect(self):
        """建立 API 连接"""
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在尝试连接 Futu OpenD 服务...")
        try:
            # 检查端口是否可达
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 设置5秒超时
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            if result != 0:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 错误: 无法连接到 Futu OpenD 服务 {self.host}:{self.port}")
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 请确认富途牛牛客户端已启动并开启OpenD服务")
                return
            
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Futu OpenD 服务端口 {self.host}:{self.port} 连接正常")
            self.quote_ctx = OpenQuoteContext(host=self.host, port=self.port)
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Futu API 连接创建成功")
        except Exception as e:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 连接 Futu API 失败: {e}")
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 请确认富途牛牛客户端已启动并开启OpenD服务")
            self.quote_ctx = None
    
    def _rate_limit(self):
        """控制 API 调用频率 - 使用时间桶算法"""
        current_time = time.time()
        
        # 清理时间窗口外的调用记录
        self.api_calls = [t for t in self.api_calls if current_time - t < self.time_window]
        
        # 计算当前 QPS
        current_qps = len(self.api_calls) / self.time_window
        
        # 检查是否超过限制
        if len(self.api_calls) >= self.max_qps:
            # 计算需要等待的时间，确保平均 QPS 不超过限制
            # 使用时间桶算法，将时间窗口分成多个小桶，均衡分布请求
            avg_interval = self.time_window / self.max_qps
            
            # 计算最近的调用时间
            if self.api_calls:
                last_call = self.api_calls[-1]
                time_since_last = current_time - last_call
                
                # 如果距离上次调用的时间小于平均间隔，等待到平均间隔
                if time_since_last < avg_interval:
                    wait_time = avg_interval - time_since_last
                    print(f"API 调用过快，等待 {wait_time:.2f} 秒...")
                    time.sleep(wait_time)
            else:
                # 首次调用，不需要等待
                pass
        else:
            # 即使未达到限制，也保持均衡的调用间隔
            if self.api_calls:
                last_call = self.api_calls[-1]
                time_since_last = current_time - last_call
                avg_interval = self.time_window / self.max_qps
                
                # 如果调用过于密集，适当等待
                if time_since_last < avg_interval * 0.5:  # 允许一定的突发
                    wait_time = avg_interval * 0.3  # 等待一小段时间
                    print(f"均衡 API 调用频率，等待 {wait_time:.2f} 秒...")
                    time.sleep(wait_time)
        
        # 记录本次调用
        self.api_calls.append(time.time())
    
    def close(self):
        """关闭 API 连接"""
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在关闭 Futu API 连接...")
        if self.quote_ctx:
            try:
                self.quote_ctx.close()
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Quote context 关闭成功")
            except Exception as e:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 关闭连接失败: {e}")
            finally:
                self.quote_ctx = None
        else:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Futu quote_ctx 未连接，无需关闭")
    
    def code_to_symbol(self, code: str) -> str:
        """将股票代码转换为futu所需的格式"""
        symbol = code.strip()
        if symbol.startswith(("sh.", "sz.")) and len(symbol) >= 9:
            return symbol
        if len(symbol) == 6:
            if symbol.startswith(("60", "90", "51")):
                return "sh." + symbol
            else:
                return "sz." + symbol
        return symbol
    
    def fetch_stock_daily(self, code: str, start_date: str | None = None, last_n: int | None = None) -> pd.DataFrame | None:
        """从 futu 获取单只股票的历史数据"""
        if not self.quote_ctx:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 错误: Futu API 连接未建立，无法获取股票 {code} 的数据")
            return None
        
        try:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取股票 {code} 的历史数据...")
            # 确定日期范围
            end_date = datetime.now()
            if start_date is None:
                # 如果没有指定开始日期，则获取近3个月的数据
                start_date = end_date - timedelta(days=30*3)
            else:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            # 控制 API 调用频率
            self._rate_limit()
            
            # 获取股票历史数据
            ret, history_data, page_req_key = self.quote_ctx.request_history_kline(
                self.code_to_symbol(code), 
                start=start_date_str, 
                end=end_date_str, 
                max_count=1000,  # 每页最多1000条数据
                ktype=KLType.K_DAY
            )
            
            if ret != RET_OK:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票 {code} 历史数据失败: {history_data}")
                return None
            
            # 处理分页数据
            all_data = []
            if not history_data.empty:
                all_data.append(history_data)
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 已获取 {len(history_data)} 条数据")
            
            # 处理分页数据
            while page_req_key is not None:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取股票 {code} 的分页数据...")
                # 控制 API 调用频率
                self._rate_limit()
                
                ret, history_data, page_req_key = self.quote_ctx.request_history_kline(
                    self.code_to_symbol(code), 
                    start=start_date_str, 
                    end=end_date_str, 
                    max_count=1000, 
                    page_req_key=page_req_key,
                    ktype=KLType.K_DAY
                )
                
                if ret == RET_OK and not history_data.empty:
                    all_data.append(history_data)
                    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 已获取 {len(history_data)} 条分页数据")
                else:
                    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 分页数据获取结束，原因: {history_data if ret != RET_OK else '无更多数据'}")
                    break
            
            if not all_data:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 股票 {code} 没有获取到任何数据")
                return None
            
            # 合并所有数据
            df = pd.concat(all_data, ignore_index=True)
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 股票 {code} 总共获取到 {len(df)} 条历史数据")
            
            # 标准化列名和格式
            df = self._normalize_daily_df(df)
            
            # 如果指定了 last_n，返回最近 last_n 天的数据
            if last_n is not None and len(df) >= last_n:
                df = df.tail(last_n).copy()
                
            return df
        except Exception as e:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票 {code} 数据异常: {e}")
            return None
    
    def _normalize_daily_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化数据框的列名和格式"""
        # 重命名列以匹配期望的英文列名
        column_mapping = {
            'time_key': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'turnover': 'amount',
            'turnover_rate': 'turn',
            'change_rate': 'change_rate'
        }
        df = df.rename(columns=column_mapping)
        
        # 移除 last_close 字段
        if 'last_close' in df.columns:
            df = df.drop('last_close', axis=1)
        
        # 确保日期列存在
        if 'date' not in df.columns:
            # 如果没有找到日期列，假设第一列是日期
            df = df.rename(columns={df.columns[0]: 'date'})
        
        df['date'] = pd.to_datetime(df['date'], errors="coerce")
        df = df.dropna(subset=['date'])
        df = df.sort_values(by='date', ascending=True)
        df = df.sort_values(by='date', ascending=True)
        
        return df
    
    def get_stock_basic_info(self, code: str) -> dict | None:
        """获取股票基本信息"""
        if not self.quote_ctx:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 错误: Futu API 连接未建立，无法获取股票 {code} 的基本信息")
            return None
        
        try:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取股票 {code} 的基本信息...")
            # 控制 API 调用频率
            self._rate_limit()
            
            # 尝试从上海证券交易所获取
            ret, basic_data = self.quote_ctx.get_stock_basicinfo(Market.SH, SecurityType.STOCK, [self.code_to_symbol(code)])
            if ret != RET_OK:
                # 控制 API 调用频率
                self._rate_limit()
                
                # 尝试从深圳证券交易所获取
                ret, basic_data = self.quote_ctx.get_stock_basicinfo(Market.SZ, SecurityType.STOCK, [self.code_to_symbol(code)])
                if ret != RET_OK:
                    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票 {code} 基本信息失败: {basic_data}")
                    return None
            
            # 提取基本信息
            basic_info = {
                'code': code,
                'name': basic_data['name'].iloc[0] if 'name' in basic_data.columns else '',
                'listing_date': basic_data['listing_date'].iloc[0] if 'listing_date' in basic_data.columns else ''
            }
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 股票 {code} 基本信息获取成功: {basic_info['name']}")
            return basic_info
        except Exception as e:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票 {code} 基本信息异常: {e}")
            return None
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取A股股票列表"""
        if not self.quote_ctx:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 错误: Futu API 连接未建立，无法获取股票列表")
            return pd.DataFrame(columns=['code', 'name'])
        
        try:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取实时股票列表...")
            # 控制 API 调用频率
            self._rate_limit()
            
            # 获取上海证券交易所股票列表
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取上海市场股票列表...")
            ret, sh_data = self.quote_ctx.get_stock_basicinfo(Market.SH, SecurityType.STOCK)
            
            # 控制 API 调用频率
            self._rate_limit()
            
            # 获取深圳证券交易所股票列表
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取深圳市场股票列表...")
            ret_sz, sz_data = self.quote_ctx.get_stock_basicinfo(Market.SZ, SecurityType.STOCK)
            
            # 合并数据
            if ret == RET_OK and ret_sz == RET_OK:
                df = pd.concat([sh_data, sz_data], ignore_index=True)
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 成功获取上海和深圳市场股票列表，总共 {len(df)} 条记录")
            elif ret == RET_OK:
                df = sh_data
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 成功获取上海市场股票列表，共 {len(df)} 条记录")
            elif ret_sz == RET_OK:
                df = sz_data
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 成功获取深圳市场股票列表，共 {len(df)} 条记录")
            else:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票列表失败: 上海({sh_data}), 深圳({sz_data})")
                return pd.DataFrame(columns=['code', 'name'])
            
            if df.empty:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票列表失败: 空数据")
                return pd.DataFrame(columns=['code', 'name'])
            
            # 重命名列
            column_mapping = {
                'code': 'code',
                'stock_name': 'name',
                'listing_date': 'ipo_date'
            }
            
            # 应用列名映射
            df = df.rename(columns=column_mapping)
            
            # 选择需要的列
            available_cols = ['code', 'name']
            if 'ipo_date' in df.columns:
                available_cols.append('ipo_date')

            df = df[available_cols]
            
            # 移除ST股票（如果name列包含'ST'）
            if 'name' in df.columns:
                df = df[~df['name'].str.contains('ST', na=False)]
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 已过滤ST股票，剩余 {len(df)} 只股票")
            
            # 按代码排序
            df = df.sort_values('code', ascending=True)

            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 最终获得 {len(df)} 只非ST股票")
            return df
        except Exception as e:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取股票列表异常: {e}")
            return pd.DataFrame(columns=['code', 'name'])
    
    def get_market_snapshot(self, symbols: list) -> pd.DataFrame | None:
        """获取市场快照信息"""
        if not self.quote_ctx:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 错误: Futu API 连接未建立，无法获取市场快照")
            return None
        
        try:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 正在获取 {len(symbols)} 只股票的市场快照信息...")
            # 控制 API 调用频率
            self._rate_limit()
            
            ret, data = self.quote_ctx.get_market_snapshot(symbols)
            if ret != RET_OK:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取市场快照失败: {data}")
                return None
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 成功获取 {len(data)} 条市场快照数据")
            return data
        except Exception as e:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 获取市场快照异常: {e}")
            return None