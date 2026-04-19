"""
股票趋势分析引擎
"""
import numpy as np
import pandas as pd

from .models import StockTrend


def _to_float_series(df: pd.DataFrame, keys: list[str], percent: bool = False) -> pd.Series:
    for key in keys:
        if key in df.columns:
            series = df[key].astype(str).str.replace("%", "", regex=False)
            series = series.replace(["", "nan", "NaN", None], "0")
            values = pd.to_numeric(series, errors="coerce").fillna(0.0)
            if percent:
                return values
            return values
    raise KeyError(f"None of the keys found in DataFrame: {keys}")


def analyze_trend(df: pd.DataFrame) -> dict:
    # 使用英文列名获取数据
    close = _to_float_series(df, ["close"])
    turnover = _to_float_series(df, ["turn"], percent=True)
    
    # 尝试获取成交额数据
    volume = None
    if "amount" in df.columns:
        vol_series = df["amount"].astype(str).replace(["", "nan", "NaN", None], "0")
        volume = pd.to_numeric(vol_series, errors="coerce").fillna(0.0)
    
    # 计算不同时间周期的涨幅
    def calculate_price_change(period_days: int) -> float:
        if len(close) >= period_days:
            return float((close.iloc[-1] - close.iloc[-period_days]) / close.iloc[-period_days])
        return 0.0
    
    price_change_5d = calculate_price_change(5)
    price_change_10d = calculate_price_change(10)
    price_change_15d = calculate_price_change(15)
    price_change_20d = calculate_price_change(20)
    price_change_30d = calculate_price_change(30)
    
    diff = close.diff().fillna(0)
    positive_days = int((diff > 0).sum())
    avg_turnover = float(turnover.mean())
    avg_volume = float(volume.mean() / 10000) if volume is not None else 0.0  # 转换为万元

    segment_size = max(1, len(close) // 3)
    segment_avgs = [close.iloc[i * segment_size: (i + 1) * segment_size].mean() for i in range(3)]
    gradual_rise = bool(
        len(close) >= 3
        and not any(bool(pd.isna(x)) for x in segment_avgs)
        and segment_avgs[0] < segment_avgs[1] < segment_avgs[2]
    )

    last_5_change = float((close.iloc[-1] - close.iloc[-5]) / close.iloc[-5]) if len(close) >= 5 else 0.0
    recent_up = bool(last_5_change > 0 and int((close.diff().tail(5) > 0).sum()) >= 3)

    reasons = []
    passed = True

    # 放宽换手率条件：从 <5% 改为 <20%
    if avg_turnover >= 10.0:
        passed = False
        reasons.append(f"平均换手率 {avg_turnover:.2f}% ≥ 10%")

    if price_change_30d <= 0.10:
        passed = False
        reasons.append(f"30 日涨幅 {price_change_30d * 100:.2f}% 未超过 10%")

    # 灵活的上涨天数条件：>10天、15天、20天、25天中的任意一个
    valid_positive_days = [10, 15, 20, 25]
    if not any(positive_days > threshold for threshold in valid_positive_days):
        passed = False
        reasons.append(f"上涨交易日 {positive_days} 天不符合条件(需>10、15、20、25天之一)")

    if not gradual_rise:
        passed = False
        reasons.append("未表现出逐步拉升趋势")

    if not recent_up:
        passed = False
        reasons.append("最近 5 日未显示连续上行")

    # 趋势等级评估（基于不同时间周期的涨幅）
    trend_levels = {
        '5d': '弱' if price_change_5d < 0.02 else ('中' if price_change_5d < 0.05 else '强'),
        '10d': '弱' if price_change_10d < 0.03 else ('中' if price_change_10d < 0.08 else '强'),
        '15d': '弱' if price_change_15d < 0.05 else ('中' if price_change_15d < 0.12 else '强'),
        '20d': '弱' if price_change_20d < 0.06 else ('中' if price_change_20d < 0.15 else '强')
    }
    
    # 生成趋势摘要
    trend_summary_parts = []
    for period, level in trend_levels.items():
        trend_summary_parts.append(f"{period}:{level}")
    trend_summary = ",".join(trend_summary_parts)
    
    if passed:
        status = "推荐"
        # 基于满足的信号生成积极建议
        positive_reasons = []
        if avg_turnover < 10.0:
            positive_reasons.append(f"平均换手率 {avg_turnover:.2f}% 适中")
        if price_change_30d > 0.10:
            positive_reasons.append(f"30 日涨幅 {price_change_30d * 100:.2f}% 超过 10%")
        if any(positive_days > threshold for threshold in valid_positive_days):
            positive_reasons.append(f"上涨交易日 {positive_days} 天符合条件")
        if gradual_rise:
            positive_reasons.append("表现出逐步拉升趋势")
        if recent_up:
            positive_reasons.append("最近 5 日显示连续上行")
        reasons = positive_reasons
    else:
        status = "不推荐"
        # 保持不满足信号的原因

    return {
        "avg_turnover": avg_turnover,
        "price_rise": price_change_30d,
        "price_rise_5d": price_change_5d,
        "price_rise_10d": price_change_10d,
        "price_rise_15d": price_change_15d,
        "price_rise_20d": price_change_20d,
        "trend_levels": trend_levels,
        "trend_summary": trend_summary,
        "positive_days": positive_days,
        "gradual_rise": gradual_rise,
        "reason": "; ".join(reasons),
        "status": status,
        "avg_volume": avg_volume,
    }


def build_stock_trend(code: str, name: str, df: pd.DataFrame, market_cap: float = 0.0) -> StockTrend:
    metrics = analyze_trend(df)
    return StockTrend(
        code=code,
        name=name,
        avg_turnover=metrics["avg_turnover"],
        price_rise=metrics["price_rise"],
        price_rise_5d=metrics["price_rise_5d"],
        price_rise_10d=metrics["price_rise_10d"],
        price_rise_15d=metrics["price_rise_15d"],
        price_rise_20d=metrics["price_rise_20d"],
        trend_summary=metrics["trend_summary"],
        positive_days=metrics["positive_days"],
        gradual_rise=metrics["gradual_rise"],
        reason=metrics["reason"],
        status=metrics["status"],
        market_cap=market_cap,
        avg_volume=metrics["avg_volume"],
    )