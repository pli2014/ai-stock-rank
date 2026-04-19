# AI Stock Rank

基于 baostock 的 A 股趋势分析 Web 服务

## 快速开始

1. 安装依赖
```bash
pip install -r requirements.txt
```

2. 启动服务
```bash
python -m src.main
# 或使用启动脚本
./start_web.sh  # Linux/Mac
start_web.bat   # Windows
```

3. 访问 `http://localhost:5000`

## 功能特性

- 📊 智能选股：基于30日强势趋势、超低换手率、绝对上涨天数等指标
- 🌐 Web界面：现代化响应式设计
- ⚡ API服务：RESTful API 接口
- 💾 智能缓存：减少重复数据获取

## 分析条件

### 强势选股标准
- **30日涨幅**：> 10%（强势上涨）
- **平均换手率**：< 20%（适度换手）
- **上涨天数**：> 10天、15天、20天、25天之一（灵活阈值）
- **逐步拉升**：三个阶段价格稳步上升
- **市值门槛**：≥ 100亿元（默认）

### 可配置参数
- `limit`: 分析股票数量上限（默认不限制）
- `min_market_cap`: 最小市值（单位：亿元，默认100.0）
- `max_workers`: 并发线程数（默认32，建议16-64）
- `refresh`: 是否刷新缓存（默认false）

### 并发加速
系统采用智能两阶段并发策略，可显著提升处理速度：
- **第一阶段**：限流批量下载（最多8线程 + 100ms延时，避免API限流）
- **第二阶段**：32线程并发分析（纯本地数据，最大化CPU利用）
- 只处理市值≥100亿的优质股票，确保数据质量
- 智能错误处理和实时进度显示
- 整体速度提升20-30倍

## API 接口

- `GET /` - 主页
- `GET /analyze?limit=500&min_market_cap=100.0&max_workers=32&refresh=false` - 分析报告
- `GET /api/analysis` - JSON API（支持相同参数）
- `GET /api/stock/{code}` - 单股票详情

## 项目结构

```
├── ARCHITECTURE.md     # 架构设计文档
├── DEVELOPMENT.md      # 开发规范
├── README.md           # 项目概述
├── .gitignore          # Git忽略配置
├── pytest.ini          # 测试配置
├── requirements.txt    # 依赖包
├── setup.py            # 包配置
├── start_web.bat       # Windows 启动
├── start_web.sh        # Linux/Mac 启动
├── src/                # 源码目录
│   ├── __init__.py     # 包初始化
│   ├── main.py         # 应用主入口
│   ├── analysis/       # 分析模块
│   │   ├── __init__.py
│   │   ├── engine.py   # 分析引擎
│   │   └── models.py   # 数据模型
│   └── data/           # 数据模块
│       ├── __init__.py
│       └── stock.py    # 股票数据获取
├── templates/          # HTML 模板
│   ├── index.html      # 主页
│   ├── report.html     # 分析报告
│   ├── busy.html       # 分析中页面
│   └── error.html      # 错误页面
└── tests/              # 测试文件
    ├── test_criteria.py    # 选股条件测试
    └── test_json_fix.py    # JSON序列化测试
```

## 开发与测试

### 运行测试
```bash
# 运行所有测试
pytest

# 运行特定测试
python -m tests.test_criteria
python -m tests.test_json_fix
```

### 工程规范
本项目遵循harness工程理念，包含完整的架构文档、开发规范和测试体系，确保代码质量、可维护性和可扩展性。

## 注意事项

- 数据来源于 baostock，仅供参考
- 不构成任何投资建议，请谨慎决策
- 建议在交易日结束后运行分析
- 首次运行可能需要较长时间获取数据