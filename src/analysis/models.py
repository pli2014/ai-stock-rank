"""
股票分析数据模型
"""
from dataclasses import dataclass


@dataclass
class StockTrend:
    code: str
    name: str
    avg_turnover: float
    price_rise: float  # 30日涨幅
    positive_days: int
    gradual_rise: bool
    reason: str
    status: str
    price_rise_5d: float = 0.0  # 5日涨幅
    price_rise_10d: float = 0.0  # 10日涨幅  
    price_rise_15d: float = 0.0  # 15日涨幅
    price_rise_20d: float = 0.0  # 20日涨幅
    trend_summary: str = ""  # 趋势等级摘要
    market_cap: float = 0.0  # 市值（亿元）
    avg_volume: float = 0.0  # 平均成交额（万元）