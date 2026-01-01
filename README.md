# Gate.io 成交记录查询与持仓均价统计

查询 Gate.io 现货交易成交记录，缓存到本地 CSV，并计算每个币种的买入均价。

## 功能

- ✅ 查询所有现货成交记录
- ✅ 增量更新，只拉取新的成交记录
- ✅ 本地 CSV 缓存，方便查看和分析
- ✅ 计算每个币种的买入均价（扣除手续费）
- ✅ 每日统计报告

## 安装

### 本地安装

```bash
# 1. 进入项目目录
cd /path/to/gate_trade_history

# 2. 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt
```

### 服务器安装 (Ubuntu 24.04+)

Ubuntu 24.04 开始强制使用虚拟环境，**必须**创建 venv：

```bash
# 1. 进入项目目录
cd /root/gate-spot-average-position-price-calculation

# 2. 创建虚拟环境
python3 -m venv .venv

# 3. 激活虚拟环境
source .venv/bin/activate

# 4. 安装依赖
pip install -r requirements.txt

# 5. 运行脚本
python main.py
```

## 配置

1. 复制环境变量示例文件：

```bash
cp .env.example .env
```

2. 编辑 `.env` 文件，填入你的 Gate.io API 密钥：

```
GATE_API_KEY=your_api_key_here
GATE_API_SECRET=your_api_secret_here
```

> API 密钥可在 [Gate.io API管理](https://www.gate.io/myaccount/api_key_manage) 创建。
> 只需要 **只读权限** 即可。

## 使用

### 手动运行

```bash
# 激活虚拟环境
source .venv/bin/activate

# 运行脚本
python main.py
```

### 输出示例

```
============================================================
  Gate.io 成交记录查询与持仓均价统计
============================================================

本地已有 150 条记录
最后记录时间: 2025-12-31 10:30:00
开始增量拉取...
完成！共 5 个交易对有成交记录，共 3 条记录
已保存 153 条成交记录到 data/trades.csv

============================================================
  持仓均价统计 2026-01-01
============================================================
币种           买入数量     买入金额(USDT)        平均价格
------------------------------------------------------------
HYPE         1000.0000           2500.00        2.500000
SOL            50.0000           5000.00      100.000000
============================================================
  总计: 2 个币种，总买入金额: 7500.00 USDT
============================================================
统计结果已保存到 data/daily_stats.csv
```

### 定时任务 (Crontab)

每天早上 8 点自动运行：

```bash
# 编辑 crontab
crontab -e

# 添加以下内容（注意使用虚拟环境中的 python）
0 8 * * * cd /root/gate-spot-average-position-price-calculation && .venv/bin/python main.py >> logs/cron.log 2>&1
```

## 数据文件

| 文件 | 说明 |
|------|------|
| `data/trades.csv` | 所有成交记录缓存 |
| `data/daily_stats.csv` | 每日统计结果 |
| `logs/cron.log` | 定时任务日志 |

### trades.csv 字段说明

| 字段 | 说明 |
|------|------|
| `id` | 成交ID |
| `create_time` | 成交时间戳（秒） |
| `create_time_ms` | 成交时间戳（毫秒） |
| `currency_pair` | 交易对，如 `HYPE_USDT` |
| `base_currency` | 基础货币，如 `HYPE` |
| `quote_currency` | 计价货币，如 `USDT` |
| `side` | 方向：`buy` 或 `sell` |
| `role` | 角色：`taker` 或 `maker` |
| `amount` | 成交数量 |
| `price` | 成交价格 |
| `order_id` | 订单ID |
| `fee` | 手续费 |
| `fee_currency` | 手续费币种 |

## 均价计算方法

只统计 `side=buy` 的现货买入记录，并**扣除手续费**：

```
净买入数量 = 买入数量 - 手续费（如果手续费用基础货币支付）
净买入金额 = 买入金额 - 手续费（如果手续费用USDT支付）
平均价格 = 净买入金额 / 净买入数量
```

## 常见问题

### Q: 首次运行很慢？

首次运行需要按30天窗口回溯获取历史成交记录，可能需要几分钟。后续运行会使用增量更新，速度会快很多。

### Q: 如何重新拉取全部数据？

删除 `data/trades.csv` 文件后重新运行即可：

```bash
rm data/trades.csv
python main.py
```

### Q: Ubuntu 报错 externally-managed-environment？

Ubuntu 24.04+ 强制使用虚拟环境，请按照"服务器安装"步骤创建 `.venv` 后再安装依赖。

## License

MIT
