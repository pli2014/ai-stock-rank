"""
AI Stock Rank - A股趋势分析系统主应用
"""
from flask import Flask, render_template, request, jsonify
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd

# 导入模块化组件 - 使用绝对导入
from data.stock_service import get_stock_daily_details
from analysis import build_stock_trend, StockTrend

# 创建Flask应用（模板目录相对于src的位置）
app = Flask(__name__, template_folder='../templates')

# 自定义JSON编码器处理numpy类型
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'item'):  # numpy类型
            return obj.item()
        return super().default(obj)

# 配置Flask使用自定义编码器
app.json_encoder = NumpyEncoder

# 缓存分析结果
CACHE_FILE = os.path.join('cache', 'analysis_cache.json')
CACHE_DURATION = timedelta(hours=24)

# 并发控制锁
analysis_lock = threading.Lock()
analysis_in_progress = False

# 分析进度跟踪 - 从单独的模块导入
from data.analysis_state import analysis_progress

def load_cached_analysis() -> Dict[str, Any]:
    """加载缓存的分析结果"""
    if not os.path.exists(CACHE_FILE):
        return {}
    
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 检查缓存是否过期
        cached_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
        if datetime.now() - cached_time > CACHE_DURATION:
            return {}
        
        return data
    except:
        return {}

def save_cached_analysis(data: Dict[str, Any]) -> None:
    """保存分析结果到缓存"""
    data['timestamp'] = datetime.now().isoformat()
    
    # 确保cache目录存在
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, cls=NumpyEncoder)

def perform_analysis(limit: int | None = None, min_market_cap: float = 50.0, max_market_cap: float = 50000.0, use_cache: bool = True, max_workers: int = 32) -> List[StockTrend]:
    """执行股票分析
    
    Args:
        limit: 分析股票数量上限
        min_market_cap: 最小市值（亿元）
        use_cache: 是否使用缓存
        max_workers: 最大并发线程数
    """
    global analysis_in_progress, analysis_progress
    
    # 检查是否已有分析在进行
    if analysis_in_progress:
        raise Exception("已有分析任务正在进行中，请稍后再试")
    
    with analysis_lock:
        analysis_in_progress = True
        try:
            # 尝试从缓存加载
            if use_cache:
                cached = load_cached_analysis()
                if cached and 'trends' in cached:
                    trends_data = cached['trends']
                    trends = []
                    for item in trends_data:
                        trend = StockTrend(**item)
                        trends.append(trend)
                    return trends
            
            # 执行新的分析
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 开始获取股票数据...")
            # 初始化进度跟踪
            analysis_progress.update({
                'in_progress': True,
                'total_stocks': 0,
                'completed_stocks': 0,
                'current_stock': '正在初始化...',
                'current_status': '准备开始分析',
                'start_time': datetime.now(),
                'qualified_count': 0,
                'failed_count': 0,
                'last_update': datetime.now()
            })
            analysis_progress['current_status'] = '正在获取股票数据...'
            analysis_progress['last_update'] = datetime.now()
            
            # 使用 get_stock_daily_details 获取股票数据
            df_stock_details = get_stock_daily_details(last_n_days=30, min_market_cap=min_market_cap, max_market_cap=max_market_cap, limit=limit)
            
            if df_stock_details.empty:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 警告：没有获取到股票数据，无法进行分析。")
                analysis_progress.update({
                    'in_progress': False,
                    'total_stocks': 0,
                    'completed_stocks': 0,
                    'current_stock': '无数据可分析',
                    'current_status': '获取股票数据失败',
                    'qualified_count': 0,
                    'last_update': datetime.now()
                })
                return []
            
            # 按股票代码分组分析
            stock_groups = df_stock_details.groupby('code')
            analysis_progress['total_stocks'] = len(stock_groups)
            print(f"获取到 {len(stock_groups)} 只股票的数据。")
            
            analysis_progress['current_status'] = '正在分析股票数据...'
            analysis_progress['last_update'] = datetime.now()
            
            # 分析每个股票
            trends = []
            completed_count = 0
            total_count = len(stock_groups)
            
            for code, group in stock_groups:
                name = group['name'].iloc[0] if 'name' in group.columns else f"股票{code}"
                market_cap = group['market_cap'].iloc[0] if 'market_cap' in group.columns else 0.0
                
                # 更新当前分析的股票
                analysis_progress['current_stock'] = f"{code} {name}"
                analysis_progress['last_update'] = datetime.now()
                
                try:
                    # 构建股票趋势
                    trend = build_stock_trend(code, name, group, market_cap)
                    
                    # 更新合格股票计数
                    if trend and trend.status == "推荐":
                        analysis_progress['qualified_count'] += 1
                    
                    trends.append(trend)
                    print(f"{code} {name}: {trend.status}")
                except Exception as e:
                    analysis_progress['failed_count'] += 1
                    print(f"{code} {name}: 分析失败 - {str(e)}")
                
                completed_count += 1
                analysis_progress['completed_stocks'] = completed_count
                
                if completed_count % 10 == 0 or completed_count == total_count:
                    progress_msg = f"分析进度: {completed_count}/{total_count}"
                    analysis_progress['current_status'] = f'分析进度: {progress_msg}'
                    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {progress_msg}")
            
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 分析完成，共处理 {len(trends)} 只股票。")
            
            # 保存到缓存
            trends_data = [trend.__dict__ for trend in trends]
            save_cached_analysis({'trends': trends_data})
            
            # 更新最终进度
            analysis_progress.update({
                'in_progress': False,
                'current_stock': '分析完成',
                'current_status': f'共处理 {len(trends)} 只股票，符合条件 {analysis_progress["qualified_count"]} 只',
                'last_update': datetime.now()
            })
            
            return trends
        finally:
            analysis_in_progress = False

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/analyze')
def analyze():
    """分析页面"""
    try:
        limit = request.args.get('limit')
        limit = int(limit) if limit else 10000
        min_market_cap = float(request.args.get('min_market_cap', 50.0))
        max_market_cap = float(request.args.get('max_market_cap', 50000.0))
        max_workers = int(request.args.get('max_workers', 32))
        refresh = request.args.get('refresh', 'false').lower() == 'true'
        show_all = request.args.get('show_all', 'true').lower() == 'true'
        
        trends = perform_analysis(limit=limit, min_market_cap=min_market_cap, max_market_cap=max_market_cap, use_cache=not refresh, max_workers=max_workers)
        
        if show_all:
            # 显示所有股票
            stocks_to_show = trends
            qualified_trends = [t for t in trends if t.status == "推荐"]
        else:
            # 只显示符合条件的股票
            stocks_to_show = [t for t in trends if t.status == "推荐"]
            qualified_trends = stocks_to_show
        
        # 默认按30日涨幅倒序排序
        stocks_to_show.sort(key=lambda x: x.price_rise, reverse=True)
        
        # 处理数据格式
        for stock in stocks_to_show:
            stock.market_cap = round(stock.market_cap, 2)
            stock.avg_volume = round(stock.avg_volume, 2)
        
        return render_template('report.html', 
                             stocks=stocks_to_show, 
                             all_stocks=trends,
                             show_all=show_all,
                             generated_at=datetime.now(),
                             total_analyzed=len(trends),
                             qualified_count=len(qualified_trends))
    except Exception as e:
        if "已有分析任务正在进行中" in str(e):
            return render_template('busy.html', message="已有分析任务正在进行中，请稍后再试")
        else:
            return render_template('error.html', error=str(e)), 500
@app.route('/api/status')
def api_status():
    """检查分析状态"""
    progress_info = analysis_progress.copy()
    
    # 计算进度百分比
    if progress_info['total_stocks'] > 0:
        progress_info['progress_percentage'] = round(
            (progress_info['completed_stocks'] / progress_info['total_stocks']) * 100, 1
        )
    else:
        progress_info['progress_percentage'] = 0
    
    # 计算预计剩余时间
    if progress_info['start_time'] and progress_info['completed_stocks'] > 0:
        elapsed_time = datetime.now() - progress_info['start_time']
        if progress_info['completed_stocks'] > 0:
            avg_time_per_stock = elapsed_time.total_seconds() / progress_info['completed_stocks']
            remaining_stocks = progress_info['total_stocks'] - progress_info['completed_stocks']
            estimated_remaining = remaining_stocks * avg_time_per_stock
            progress_info['estimated_remaining_seconds'] = round(estimated_remaining)
        else:
            progress_info['estimated_remaining_seconds'] = 0
    else:
        progress_info['estimated_remaining_seconds'] = 0
    
    # 格式化时间
    if progress_info['start_time']:
        progress_info['start_time'] = progress_info['start_time'].isoformat()
    if progress_info['last_update']:
        progress_info['last_update'] = progress_info['last_update'].isoformat()
    
    return jsonify(progress_info)

def main():
    """应用入口点"""
    app.run(debug=True, host='0.0.0.0', port=80)

if __name__ == '__main__':
    main()