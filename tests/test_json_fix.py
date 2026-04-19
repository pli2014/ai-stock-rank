#!/usr/bin/env python3
"""测试JSON序列化修复"""

import sys
import os
# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import json
import numpy as np
from datetime import datetime

# 自定义JSON编码器
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'item'):  # numpy类型
            return obj.item()
        return super().default(obj)

# 测试数据
test_data = {
    'timestamp': datetime.now().isoformat(),
    'numpy_bool': np.bool_(True),
    'numpy_int': np.int64(42),
    'numpy_float': np.float64(3.14),
    'python_bool': True,
    'python_int': 42,
    'python_float': 3.14,
    'stocks': [{
        'code': '000001',
        'name': '平安银行',
        'gradual_rise': bool(np.bool_(True)),
        'avg_turnover': float(np.float64(5.5)),
        'positive_days': int(np.int64(15))
    }]
}

print("测试数据:")
print(f"numpy_bool类型: {type(test_data['numpy_bool'])}")
print(f"gradual_rise类型: {type(test_data['stocks'][0]['gradual_rise'])}")

try:
    # 使用自定义编码器
    json_str = json.dumps(test_data, ensure_ascii=False, indent=2, cls=NumpyEncoder)
    print("\n✅ JSON序列化成功!")
    print("序列化后的数据:")
    print(json_str[:200] + "...")
except Exception as e:
    print(f"\n❌ JSON序列化失败: {e}")

# 测试从JSON加载
try:
    loaded_data = json.loads(json_str)
    print("\n✅ JSON反序列化成功!")
    print(f"loaded gradual_rise: {loaded_data['stocks'][0]['gradual_rise']} (类型: {type(loaded_data['stocks'][0]['gradual_rise'])})")
except Exception as e:
    print(f"\n❌ JSON反序列化失败: {e}")