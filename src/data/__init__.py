"""
股票数据模块
"""
from .stock_service import get_stock_daily_details, close_quote_ctx

# 保持与旧接口的兼容性
CACHE_DIR = "cache"
CACHE_TTL_DAYS = 1

__all__ = ['get_stock_daily_details', 'close_quote_ctx', 'CACHE_DIR', 'CACHE_TTL_DAYS']