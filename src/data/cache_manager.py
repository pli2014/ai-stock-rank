"""
缓存管理器模块
"""
import json
import os
from datetime import datetime, timedelta
import pandas as pd


class CacheManager:
    """缓存管理器"""
    def __init__(self, cache_dir=None, default_ttl_days=1):
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), "..", "..", "cache")
        self.default_ttl_days = default_ttl_days
        self.ensure_cache_dir()
    
    def ensure_cache_dir(self) -> None:
        """确保缓存目录存在"""
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def get_cache_path(self, code: str, suffix: str = "") -> str:
        """生成缓存文件路径"""
        if suffix:
            return os.path.join(self.cache_dir, f"{code}_{suffix}.json")
        return os.path.join(self.cache_dir, f"{code}.json")
    
    def load_cache(self, code: str, suffix: str = "") -> dict | None:
        """加载缓存"""
        path = self.get_cache_path(code, suffix)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, ValueError):
            # 如果缓存文件损坏，删除它并返回 None
            try:
                os.remove(path)
            except OSError:
                pass
            return None
        return data
    
    def save_cache(self, code: str, data, suffix: str = "") -> None:
        """保存缓存"""
        self.ensure_cache_dir()
        
        # 处理 DataFrame 类型
        if isinstance(data, pd.DataFrame):
            # 复制 DataFrame 以避免修改原始数据
            df_copy = data.copy()
            
            # 确保日期列格式化为 "YYYY-MM-DD" 格式
            if 'date' in df_copy.columns:
                df_copy['date'] = df_copy['date'].dt.strftime('%Y-%m-%d')
            
            records_json = json.loads(
                df_copy.reset_index().to_json(orient="records")
            )
            payload = {
                "updated_at": datetime.now().isoformat(),
                "records": records_json,
            }
        else:
            # 处理字典类型
            payload = {
                "updated_at": datetime.now().isoformat(),
                "data": data,
            }
        
        with open(self.get_cache_path(code, suffix), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    
    def is_cache_valid(self, cached_data: dict, ttl_days: int = None) -> bool:
        """检查缓存是否有效"""
        if not cached_data:
            return False
        
        try:
            updated_at = datetime.fromisoformat(cached_data["updated_at"])
            ttl = ttl_days or self.default_ttl_days
            return datetime.now() - updated_at <= timedelta(days=ttl)
        except Exception:
            return False