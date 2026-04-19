"""
测试 futu OpenD API 功能
"""
from futu import OpenQuoteContext, Market, SecurityType, KLType, RET_OK
import pandas as pd
from datetime import datetime, timedelta

def test_futu():
    """
    测试 futu OpenD API 功能，获取 A 股上市公司的股票列表并分析
    
    功能说明：
    1. 初始化 futu OpenD API 连接
    2. 获取证券基本资料（包括上市时间等信息）
    3. 获取市值信息
    4. 筛选总市值满足 100 亿的股票
    5. 遍历获取每个股票最近 30 天的收盘数据
    6. 生成并打印包含指定字段的 pd 数据格式
    
    输出字段：
    - date: 日期
    - code: 证券代码
    - ipoDate: 上市时间
    - market_cap: 总市值（亿元）
    - close: 收盘价
    - volume: 成交量
    - amount: 成交额
    - turn: 换手率
    """
    # 初始化 futu OpenD API 连接
    # 注意：需要先启动 futu OpenD 服务
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    
    try:
        # 1. 获取证券基本资料
        print("\n获取证券基本资料...")
        # 使用 get_stock_basicinfo 接口获取 A 股股票列表
        # 这里获取所有 A 股股票
        ret, data = quote_ctx.get_stock_basicinfo(Market.SH, SecurityType.STOCK)
        
        if ret != RET_OK:
            print(f"获取上海证券交易所股票列表失败: {data}")
            # 尝试获取深圳证券交易所股票列表
            ret, data = quote_ctx.get_stock_basicinfo(Market.SZ, SecurityType.STOCK)
            if ret != RET_OK:
                print(f"获取深圳证券交易所股票列表失败: {data}")
                return
        
        print(f"获取到 {len(data)} 只股票")
        
        # 2. 筛选总市值满足 100 亿的股票
        print("\n筛选总市值满足 100 亿的股票...")
        # 获取股票代码列表
        stock_codes = data['code'].tolist()
        
        # 限制处理的股票数量
        stock_codes = stock_codes[:20] if len(stock_codes) > 20 else stock_codes
        
        # 存储符合条件的股票
        filtered_stocks = []
        
        # 获取市值信息
        ret, market_data = quote_ctx.get_market_snapshot(stock_codes)
        if ret != RET_OK:
            print(f"获取市值信息失败: {market_data}")
            return
        
        # 筛选总市值满足 100 亿的股票
        for _, row in market_data.iterrows():
            # 检查 market_val 字段是否存在
            if 'total_market_val' in row:
                market_cap = row['total_market_val'] / 100000000  # 转换为亿元
                if market_cap >= 100:
                    filtered_stocks.append({
                        'code': row['code'],
                        'market_cap': market_cap
                    })
            else:
                print(f"股票 {row['code']} 没有市值信息")
        
        # 按照市值降序排序
        filtered_stocks.sort(key=lambda x: x['market_cap'], reverse=True)
        
        # 只保留前10只
        filtered_stocks = filtered_stocks[:10]
        
        print(f"筛选后有 {len(filtered_stocks)} 只股票总市值满足100亿")
        
        # 3. 循环遍历获取每只股票的详细信息和历史数据
        print("\n获取股票详细信息和历史数据...")
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # 存储所有股票的数据
        all_stock_data = []
        
        for stock in filtered_stocks:
            code = stock['code']
            market_cap = stock['market_cap']
            print(f"获取股票 {code} 的数据...")
            
            # 获取股票基本信息
            ret, basic_data = quote_ctx.get_stock_basicinfo(Market.SH, SecurityType.STOCK,[code])
            if ret != RET_OK:
                print(f"获取股票 {code} 基本信息失败: {basic_data}")
                continue
            
            # 获取历史数据
            try:
                # 尝试获取历史数据
                # 使用 request_history_kline 接口，按照参考案例的方式
                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
                
                ret, history_data, page_req_key = quote_ctx.request_history_kline(
                    code, 
                    start=start_date_str, 
                    end=end_date_str, 
                    max_count=30,  # 每页30个数据
                    ktype=KLType.K_DAY
                )
                
                if ret != RET_OK:
                    print(f"获取股票 {code} 历史数据失败: {history_data}")
                    continue
                
                # 处理第一页数据
                if not history_data.empty:
                    # 获取基本信息
                    ipo_date = basic_data['list_time'][0] if 'list_time' in basic_data.columns else None
                    
                    # 处理历史数据
                    for _, row in history_data.iterrows():
                        all_stock_data.append({
                            'date': row['time_key'],       # 日期
                            'code': code,                  # 证券代码
                            'ipoDate': ipo_date,           # 上市时间
                            'market_cap': market_cap,      # 总市值（亿元）
                            'close': row['close'],         # 收盘价
                            'volume': row['volume'],       # 成交量
                            'amount': row['turnover'],     # 成交额
                            'turn': row['turnover_rate']   # 换手率
                        })
                
                # 处理分页数据
                while page_req_key is not None:
                    ret, history_data, page_req_key = quote_ctx.request_history_kline(
                        code, 
                        start=start_date_str, 
                        end=end_date_str, 
                        max_count=30, 
                        page_req_key=page_req_key,
                        ktype=KLType.K_DAY
                    )
                    
                    if ret == RET_OK and not history_data.empty:
                        # 处理历史数据
                        for _, row in history_data.iterrows():
                            all_stock_data.append({
                                'date': row['time_key'],       # 日期
                                'code': code,                  # 证券代码
                                'ipoDate': ipo_date,           # 上市时间
                                'market_cap': market_cap,      # 总市值（亿元）
                                'close': row['close'],         # 收盘价
                                'volume': row['volume'],       # 成交量
                                'amount': row['turnover'],     # 成交额
                                'turn': row['turnover_rate']   # 换手率
                            })
                    else:
                        print(f"获取股票 {code} 分页数据失败: {history_data}")
                        break
            except Exception as e:
                print(f"获取股票 {code} 历史数据时出错: {e}")
                continue
            
            # 检查是否获取到数据
            if not all_stock_data or all_stock_data[-1]['code'] != code:
                print(f"未获取到股票 {code} 的历史数据")
        
        # 4. 生成 pd 数据格式并打印前10条
        print("\n前10条收盘数据：")
        if all_stock_data:
            # 创建 DataFrame
            df_all = pd.DataFrame(all_stock_data)
            # 确保列顺序
            columns_order = ['date', 'code', 'ipoDate', 'market_cap', 'close', 'volume', 'amount', 'turn']
            df_all = df_all[columns_order]
            
            # 打印前10条数据
            print(df_all.head(10))
            
            # 输出指定格式的完整信息
            print("\n完整信息输出：")
            for _, row in df_all.head(10).iterrows():
                print(f"date={row['date']},code={row['code']},ipoDate={row['ipoDate']},market_cap={row['market_cap']},close={row['close']},volume={row['volume']},amount={row['amount']},turn={row['turn']}")
        else:
            print("未获取到任何股票数据")
            
    except Exception as e:
        print(f"获取数据失败: {e}")
    finally:
        # 关闭连接
        quote_ctx.close()

if __name__ == "__main__":
    test_futu()