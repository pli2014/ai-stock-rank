"""
股票数据服务模块
"""
from datetime import datetime, timedelta
import pandas as pd
from .cache_manager import CacheManager
from .futu_api import FutuAPI

# 导入分析进度跟踪
from .analysis_state import analysis_progress
print("成功导入 analysis_progress 从 analysis_state.py")


class StockDataService:
    """股票数据服务"""
    def __init__(self):
        self.cache_manager = CacheManager()
        self.futu_api = FutuAPI()
    
    def close(self):
        """关闭资源"""
        self.futu_api.close()
    
    def get_stock_data(self, code: str, last_n: int = 30, use_cache: bool = True) -> pd.DataFrame | None:
        """获取股票数据，优先使用缓存，缓存过期则更新"""
        if use_cache:
            cached = self.cache_manager.load_cache(code)
            if cached:
                # 检查缓存是否需要更新
                cache_expired = not self.cache_manager.is_cache_valid(cached)
                
                # 检查数据是否完整（最后一个交易日是昨天或周五）
                df_cached = pd.DataFrame(cached["records"])
                # 检查日期列名，优先使用'date'，兼容旧缓存的'日期'
                date_column = 'date' if 'date' in df_cached.columns else '日期'
                df_cached[date_column] = pd.to_datetime(df_cached[date_column])
                last_trade_date = df_cached[date_column].max()
                
                # 检查是否是最近24小时内更新的
                updated_at = datetime.fromisoformat(cached["updated_at"])
                within_24_hours = datetime.now() - updated_at < timedelta(hours=24)
                
                # 检查最后一条记录是否是前一日或者周五
                today = datetime.now().date()
                yesterday = today - timedelta(days=1)
                
                # 计算上一个交易日（考虑周末）
                last_workday = yesterday
                while last_workday.weekday() >= 5:  # 5=周六, 6=周日
                    last_workday = last_workday - timedelta(days=1)
                
                # 数据完整的条件：最后一条记录是上一个交易日
                data_complete = last_trade_date.date() == last_workday
                
                if cache_expired or not data_complete or not within_24_hours:
                    # 缓存过期、数据不完整或超过24小时未更新，更新数据
                    df = self._update_stock_data(code)
                else:
                    # 缓存有效且数据完整，直接使用
                    df = df_cached
            else:
                # 缓存不存在，更新数据
                df = self._update_stock_data(code)
        else:
            # 不使用缓存，直接更新数据
            df = self._update_stock_data(code)
        
        if df is not None and not df.empty:
            # 返回最近 last_n 天的数据
            return df.tail(last_n).copy()
        return None
    
    def _update_stock_data(self, code: str) -> pd.DataFrame | None:
        """增量更新股票数据"""
        # 加载现有缓存
        cached = self.cache_manager.load_cache(code)
        
        if cached:
            # 从缓存中读取数据
            df_existing = pd.DataFrame(cached["records"])
            # 检查日期列名，优先使用'date'，兼容旧缓存的'日期'
            date_column = 'date' if 'date' in df_existing.columns else '日期'
            df_existing[date_column] = pd.to_datetime(df_existing[date_column])
            
            # 获取最后一个交易日的日期
            last_date = df_existing[date_column].max()
            # 计算开始日期为最后一个交易日的下一天
            start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # 下载新数据
            df_new = self.futu_api.fetch_stock_daily(code, start_date=start_date)
            
            if df_new is not None and not df_new.empty:
                # 合并数据
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                # 去重，确保日期唯一
                df_combined = df_combined.drop_duplicates(subset=[date_column], keep="last")
                # 按日期排序
                df_combined = df_combined.sort_values(by=[date_column], ascending=True)
                # 保存更新后的数据
                self.cache_manager.save_cache(code, df_combined)
                return df_combined
            else:
                # 没有新数据，返回现有数据
                return df_existing
        else:
            # 缓存不存在，下载全部历史数据
            df = self.futu_api.fetch_stock_daily(code)
            if df is not None:
                self.cache_manager.save_cache(code, df)
            return df
    
    def get_stock_daily_details(self, last_n_days: int = 30, min_market_cap: float = 50.0, max_market_cap: float = 20000.0, limit: int = 10) -> pd.DataFrame:
        """获取股票历史日数据明细"""
        # 1. 获取股票列表
        print("\n获取证券基本资料...")
        analysis_progress.update({
            'current_status': '正在获取股票列表...',
            'last_update': datetime.now()
        })
        stock_list = self._get_stock_list(limit=limit, min_market_cap=min_market_cap)
        columns=['date', 'code', 'name','ipoDate', 'market_cap', 'close', 'volume', 'amount', 'turn']
        
        if stock_list.empty:
            print("未获取到股票列表")
            analysis_progress.update({
                'current_status': '未获取到股票列表',
                'last_update': datetime.now()
            })
            return pd.DataFrame(columns=columns)
        
        print(f"获取到 {len(stock_list)} 只股票")
        analysis_progress.update({
            'current_status': f'获取到 {len(stock_list)} 只股票',
            'last_update': datetime.now()
        })
        
        # 2. 筛选总市值满足条件的股票
        print(f"\n筛选总市值在 {min_market_cap} 亿到 {max_market_cap} 亿之间的股票...")
        analysis_progress.update({
            'current_status': f'筛选总市值在 {min_market_cap} 亿到 {max_market_cap} 亿之间的股票...',
            'last_update': datetime.now()
        })

        
        # 获取市值信息
        stock_codes = stock_list['code'].tolist()
        
        # 分批处理，每次最多200个股票代码
        batch_size = 200
        all_market_data = []
        
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i+batch_size]
            batch_symbols = [self.futu_api.code_to_symbol(code) for code in batch_codes]
            batch_data = self.futu_api.get_market_snapshot(batch_symbols)
            
            if batch_data is not None and not batch_data.empty:
                all_market_data.append(batch_data)
            
            # 更新进度
            progress = (i + len(batch_codes)) / len(stock_codes) * 100
            analysis_progress.update({
                'current_status': f'获取市值信息: {progress:.1f}%',
                'last_update': datetime.now()
            })
        
        if not all_market_data:
            print("未获取到任何市值信息")
            analysis_progress.update({
                'current_status': '未获取到任何市值信息',
                'last_update': datetime.now()
            })
            return pd.DataFrame(columns=columns)
        
        # 合并所有批次的数据
        market_data = pd.concat(all_market_data, ignore_index=True)
        
        # 添加市值列
        stock_list = stock_list.copy()
        stock_list['market_cap'] = 0.0
        
        # 填充市值信息
        for _, row in market_data.iterrows():
                code = row['code'].replace('sh.', '').replace('sz.', '')
                if 'total_market_val' in row:
                    market_cap = row['total_market_val'] / 100000000  # 转换为亿元
                    market_cap = round(market_cap, 2)  # 保留2位小数
                    stock_list.loc[stock_list['code'] == code, 'market_cap'] = market_cap
                else:
                    print(f"股票 {code} 没有市值信息")
        
        # 筛选总市值满足条件的股票
        filtered_stocks = stock_list[(stock_list['market_cap'] >= min_market_cap) & (stock_list['market_cap'] <= max_market_cap)].copy()
        
        # 按照市值降序排序
        filtered_stocks = filtered_stocks.sort_values(by='market_cap', ascending=False)
        
        # 限制数量
        filtered_stocks = filtered_stocks.head(limit)
        
        print(f"筛选后返回 {len(filtered_stocks)} 只股票")
        analysis_progress.update({
            'total_stocks': len(filtered_stocks),
            'current_status': f'筛选后返回 {len(filtered_stocks)} 只股票',
            'last_update': datetime.now()
        })
        
        # 打印股票名称、代码和市值
        for _, stock in filtered_stocks.iterrows():
            code = stock['code']
            name = stock['name']
            market_cap = stock['market_cap']
            print(f"{code} {name}: {market_cap:.2f} 亿元")
        
        if filtered_stocks.empty:
            print("未筛选到满足条件的股票")
            analysis_progress.update({
                'current_status': '未筛选到满足条件的股票',
                'last_update': datetime.now()
            })
            return pd.DataFrame(columns=columns)
        
        # 3. 循环遍历获取每只股票的详细信息和历史数据
        print("\n获取股票详细信息和历史数据...")
        analysis_progress.update({
            'current_status': '正在获取股票详细信息和历史数据...',
            'last_update': datetime.now()
        })
        
        # 存储所有股票的数据
        all_stock_data = []
        completed_count = 0
        total_count = len(filtered_stocks)
        
        for _, stock in filtered_stocks.iterrows():
            code = stock['code']
            name = stock['name']
            market_cap = stock['market_cap']
            
            print(f"\n处理股票: {code} {name} (市值: {market_cap:.2f} 亿元)")
            analysis_progress.update({
                'current_stock': f"{code} {name}",
                'completed_stocks': completed_count,
                'current_status': f'处理股票: {code} {name}',
                'last_update': datetime.now()
            })
            
            # 获取股票基本信息
            print(f"  获取股票基本信息...")
            analysis_progress.update({
                'current_status': f'获取股票 {code} 基本信息...',
                'last_update': datetime.now()
            })
            basic_info = self._get_stock_basic_info(code)
            if basic_info is None:
                print(f"  获取股票 {code} 基本信息失败")
                analysis_progress.update({
                    'current_status': f'获取股票 {code} 基本信息失败',
                    'last_update': datetime.now()
                })
                continue
            
            # 获取上市时间
            ipo_date = basic_info.get('listing_date', "")
            print(f"  上市时间: {ipo_date}")
            
            # 获取股票历史数据
            print(f"  获取股票历史数据...")
            analysis_progress.update({
                'current_status': f'获取股票 {code} 历史数据...',
                'last_update': datetime.now()
            })
            df_stock = self.get_stock_data(code, last_n=last_n_days)
            
            if df_stock is None or df_stock.empty:
                print(f"  未获取到股票 {code} 的历史数据")
                analysis_progress.update({
                    'current_status': f'未获取到股票 {code} 的历史数据',
                    'last_update': datetime.now()
                })
                continue
            
            print(f"  成功获取 {len(df_stock)} 条历史数据")
            
            # 处理历史数据
            print(f"  处理历史数据...")
            analysis_progress.update({
                'current_status': f'处理股票 {code} 历史数据...',
                'last_update': datetime.now()
            })
            for _, row in df_stock.iterrows():
                all_stock_data.append({
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'code': code,
                    'name': name,
                    'ipoDate': ipo_date,
                    'market_cap': round(market_cap, 2),
                    'close': row['close'],
                    'volume': row['volume'],
                    'amount': round(row['amount'], 2),
                    'turn': row['turn']
                })
            
            print(f"  成功处理 {len(df_stock)} 条数据")
            completed_count += 1
            analysis_progress.update({
                'completed_stocks': completed_count,
                'current_status': f'已处理 {completed_count}/{total_count} 只股票',
                'last_update': datetime.now()
            })

        # 4. 生成 pd 数据格式
        print("\n生成股票历史日数据明细...")
        analysis_progress.update({
            'current_status': '生成股票历史日数据明细...',
            'last_update': datetime.now()
        })
        
        if all_stock_data:
            # 创建 DataFrame
            df_all = pd.DataFrame(all_stock_data)
            # 确保列顺序
            columns_order = ['date', 'code', 'name','ipoDate', 'market_cap', 'close', 'volume', 'amount', 'turn']
            df_all = df_all[columns_order]
            
            # 按股票代码和名称分组，打印每个股票的记录数
            stock_groups = df_all.groupby(['code', 'name'])
            print("\n各股票数据记录数：")
            for (code, name), group in stock_groups:
                print(f"股票代码: {code}, 名称: {name}, 记录数: {len(group)}")
            
            print(f"\n成功生成 {len(df_all)} 条股票历史日数据明细")
            analysis_progress.update({
                'current_status': f'成功生成 {len(df_all)} 条股票历史日数据明细',
                'last_update': datetime.now()
            })
            return df_all
        else:
            print("未获取到任何股票数据")
            analysis_progress.update({
                'current_status': '未获取到任何股票数据',
                'last_update': datetime.now()
            })
            return pd.DataFrame(columns=['date', 'code', 'ipoDate', 'market_cap', 'close', 'volume', 'amount', 'turn'])
    
    def _get_stock_basic_info(self, code: str, use_cache: bool = True) -> dict | None:
        """获取股票基本信息，优先使用缓存"""
        if use_cache:
            cached = self.cache_manager.load_cache(code, "basic")
            if cached and self.cache_manager.is_cache_valid(cached, 7):  # 基本信息缓存有效期为7天
                print(f"  从缓存加载股票 {code} 基本信息")
                cached_data = cached.get("data", cached.get("basic_info"))
                # 确保返回的是完整的基本信息字典
                if isinstance(cached_data, dict) and 'code' in cached_data and 'name' in cached_data:
                    return cached_data
        
        print(f"  从API获取股票 {code} 基本信息")
        basic_info = self.futu_api.get_stock_basic_info(code)
        
        if basic_info:
            # 保存到缓存
            try:
                self.cache_manager.save_cache(code, basic_info, "basic")
                print(f"  缓存股票 {code} 基本信息")
            except Exception as e:
                print(f"  缓存股票基本信息失败: {e}")
        
        return basic_info
    
    def _get_stock_list(self, limit: int | None = None, min_market_cap: float = 100.0) -> pd.DataFrame:
        """获取A股股票列表，可按市值筛选（单位：亿元）"""
        # 缓存文件路径
        cache_key = f"stock_list_{min_market_cap}"
        
        # 首先尝试从缓存加载
        cached = self.cache_manager.load_cache(cache_key)
        if cached and self.cache_manager.is_cache_valid(cached):
            df = pd.DataFrame(cached['records'])
            print(f"从缓存加载股票列表 ({len(df)} 只股票)")
            if limit is not None:
                df = df.head(limit)
            return df
        
        # 从 API 获取
        df = self.futu_api.get_stock_list()
        
        # 限制数量
        if limit is not None:
            df = df.head(limit)

        # 保存到缓存
        if not df.empty:
            self.cache_manager.save_cache(cache_key, df)
            print(f"成功获取并缓存股票列表 ({len(df)} 只股票)")
        
        return df


# 全局实例
stock_service = StockDataService()

# 兼容旧接口
def get_stock_daily_details(last_n_days: int = 30, min_market_cap: float = 50.0, max_market_cap: float = 50000, limit: int = 10) -> pd.DataFrame:
    return stock_service.get_stock_daily_details(last_n_days, min_market_cap, max_market_cap, limit)

def close_quote_ctx():
    stock_service.close()

# 在模块被导入时注册清理函数
import atexit
atexit.register(close_quote_ctx)