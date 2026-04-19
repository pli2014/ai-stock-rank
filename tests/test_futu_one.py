"""
测试 futu OpenD API 功能
"""
from futu import OpenQuoteContext, Market, SecurityType, KLType, RET_OK
import pandas as pd
from datetime import datetime, timedelta

# 初始化 futu OpenD API 连接
# 注意：需要先启动 futu OpenD 服务
quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
try:
    # 1. 获取证券基本资料
    print("\n获取证券基本资料...")
    # 使用 get_stock_basicinfo 接口获取 A 股股票列表
    # 这里获取所有 A 股股票
    ret, data = quote_ctx.get_stock_basicinfo(Market.SZ, SecurityType.STOCK)
    
    if ret == RET_OK:
            print(data)
    else:
        print(f"获取数据失败: {data}")
except Exception as e:
    print(f"获取数据失败: {e}")
finally:
    # 关闭连接
    quote_ctx.close()
