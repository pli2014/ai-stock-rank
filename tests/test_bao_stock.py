from data.baostock_api import BaoStockAPI

api = BaoStockAPI()
data = api.fetch_stock_daily("SH.600000")  # 会自动转换为小写
print(data)
api.close()