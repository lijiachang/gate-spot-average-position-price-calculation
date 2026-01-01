#!/usr/bin/env python3
"""
Gate.io 成交记录查询与持仓均价统计

功能：
1. 查询现货和理财账户的所有成交记录，缓存到本地CSV
2. 增量更新新的成交记录
3. 计算每个币种的买入均价

注意：Gate API 默认只返回7天数据，时间范围不能超过30天，
因此需要按30天为单位分段回溯查询。
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import gate_api
from gate_api.exceptions import ApiException, GateApiException

# 加载环境变量
load_dotenv()

# 项目路径配置
PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data"
TRADES_CSV = DATA_DIR / "trades.csv"
DAILY_STATS_CSV = DATA_DIR / "daily_stats.csv"

# 确保数据目录存在
DATA_DIR.mkdir(exist_ok=True)

# 查询窗口大小（天），Gate API 限制最大30天
QUERY_WINDOW_DAYS = 30
# 连续多少天没有记录就停止回溯
STOP_EMPTY_DAYS = 30


class GateTradeClient:
    """Gate.io 交易记录客户端"""
    
    def __init__(self):
        api_key = os.getenv("GATE_API_KEY")
        api_secret = os.getenv("GATE_API_SECRET")
        
        if not api_key or not api_secret:
            print("错误：请在 .env 文件中配置 GATE_API_KEY 和 GATE_API_SECRET")
            sys.exit(1)
        
        configuration = gate_api.Configuration(
            host="https://api.gateio.ws/api/v4",
            key=api_key,
            secret=api_secret
        )
        self._api_client = gate_api.ApiClient(configuration)
        self._spot_api = gate_api.SpotApi(self._api_client)
        self._earn_api = gate_api.EarnUniApi(self._api_client)
    
    def _fetch_spot_trades_in_range(self, from_ts: int, to_ts: int) -> list[dict]:
        """获取指定时间范围内的现货成交记录"""
        all_trades = []
        limit = 1000
        page = 1
        
        while True:
            try:
                trades = self._spot_api.list_my_trades(
                    limit=limit,
                    page=page,
                    _from=from_ts,
                    to=to_ts,
                )
                
                if not trades:
                    break
                
                for trade in trades:
                    all_trades.append({
                        "id": f"spot_{trade.id}",
                        "source": "spot",
                        "create_time": int(float(trade.create_time)),
                        "create_time_ms": int(float(trade.create_time_ms)) if trade.create_time_ms else int(float(trade.create_time)) * 1000,
                        "currency_pair": trade.currency_pair,
                        "base_currency": trade.currency_pair.split("_")[0],
                        "quote_currency": trade.currency_pair.split("_")[1] if "_" in trade.currency_pair else "USDT",
                        "side": trade.side,
                        "role": trade.role,
                        "amount": float(trade.amount),
                        "price": float(trade.price),
                        "order_id": trade.order_id,
                        "fee": float(trade.fee) if trade.fee else 0,
                        "fee_currency": trade.fee_currency,
                    })
                
                if len(trades) < limit:
                    break
                
                page += 1
                time.sleep(0.05)
                
            except (ApiException, GateApiException) as e:
                if "INVALID_PARAM_VALUE" not in str(e):
                    print(f"    获取现货记录失败: {e}")
                break
        
        return all_trades
    
    def _fetch_earn_records_in_range(self, from_ts: int, to_ts: int) -> list[dict]:
        """获取指定时间范围内的理财记录"""
        all_records = []
        limit = 100
        page = 1
        
        # 理财API使用秒时间戳（返回的create_time是毫秒）
        while True:
            try:
                records = self._earn_api.list_uni_lend_records(
                    limit=limit,
                    page=page,
                    _from=from_ts,
                    to=to_ts,
                )
                
                if not records:
                    break
                
                for r in records:
                    # 理财记录的时间戳是毫秒
                    create_time_ms = int(r.create_time)
                    create_time = create_time_ms // 1000
                    
                    all_records.append({
                        "id": f"earn_{r.currency}_{create_time_ms}",
                        "source": "earn",
                        "create_time": create_time,
                        "create_time_ms": create_time_ms,
                        "currency_pair": f"{r.currency}_USDT",
                        "base_currency": r.currency,
                        "quote_currency": "USDT",
                        "side": "earn",  # 理财记录标记为earn
                        "role": "earn",
                        "amount": float(r.amount),
                        "price": 0,  # 理财记录没有价格
                        "order_id": "",
                        "fee": 0,
                        "fee_currency": r.currency,
                    })
                
                if len(records) < limit:
                    break
                
                page += 1
                time.sleep(0.05)
                
            except (ApiException, GateApiException) as e:
                if "INVALID_PARAM_VALUE" not in str(e):
                    print(f"    获取理财记录失败: {e}")
                break
        
        return all_records
    
    def fetch_all_trades(self) -> list[dict]:
        """
        获取所有历史成交记录
        
        按30天为单位往前回溯，直到连续30天没有记录为止
        """
        all_trades = []
        now = datetime.now()
        window_end = now
        empty_days = 0
        
        print("正在获取历史成交记录（按30天窗口回溯）...")
        print("=" * 60)
        
        while empty_days < STOP_EMPTY_DAYS:
            window_start = window_end - timedelta(days=QUERY_WINDOW_DAYS)
            
            from_ts = int(window_start.timestamp())
            to_ts = int(window_end.timestamp())
            
            period = f"{window_start.strftime('%Y-%m-%d')} ~ {window_end.strftime('%Y-%m-%d')}"
            print(f"查询: {period}")
            
            # 获取现货记录
            spot_trades = self._fetch_spot_trades_in_range(from_ts, to_ts)
            
            # 获取理财记录
            earn_records = self._fetch_earn_records_in_range(from_ts, to_ts)
            
            total_in_window = len(spot_trades) + len(earn_records)
            print(f"  现货: {len(spot_trades)} 条, 理财: {len(earn_records)} 条")
            
            if total_in_window > 0:
                all_trades.extend(spot_trades)
                all_trades.extend(earn_records)
                empty_days = 0  # 重置空窗计数
            else:
                empty_days += QUERY_WINDOW_DAYS
                print(f"  (连续 {empty_days} 天无记录)")
            
            # 移动到上一个窗口
            window_end = window_start
            time.sleep(0.1)
        
        print("=" * 60)
        print(f"完成！共获取 {len(all_trades)} 条记录")
        
        return all_trades
    
    def fetch_trades_since(self, from_time: int) -> list[dict]:
        """
        获取指定时间之后的成交记录（增量更新）
        """
        now = int(datetime.now().timestamp())
        
        print(f"正在获取 {datetime.fromtimestamp(from_time)} 之后的成交记录...")
        
        spot_trades = self._fetch_spot_trades_in_range(from_time, now)
        earn_records = self._fetch_earn_records_in_range(from_time, now)
        
        all_trades = spot_trades + earn_records
        print(f"获取到 {len(spot_trades)} 条现货, {len(earn_records)} 条理财记录")
        
        return all_trades


class TradeDataManager:
    """交易数据管理器"""
    
    def __init__(self, trades_csv: Path = TRADES_CSV):
        self.trades_csv = trades_csv
    
    def load(self) -> pd.DataFrame:
        """加载本地缓存的成交记录"""
        if self.trades_csv.exists():
            return pd.read_csv(self.trades_csv)
        return pd.DataFrame()
    
    def save(self, df: pd.DataFrame) -> None:
        """保存成交记录到CSV"""
        df.to_csv(self.trades_csv, index=False)
        print(f"已保存 {len(df)} 条成交记录到 {self.trades_csv}")
    
    def get_last_trade_time(self, df: pd.DataFrame) -> int | None:
        """获取最后一条记录的时间戳"""
        if df.empty:
            return None
        return int(df["create_time"].max())
    
    def merge_trades(self, cached_df: pd.DataFrame, new_trades: list[dict]) -> pd.DataFrame:
        """合并新旧成交记录并去重"""
        if not new_trades:
            return cached_df
        
        new_df = pd.DataFrame(new_trades)
        
        if cached_df.empty:
            combined_df = new_df
        else:
            combined_df = pd.concat([cached_df, new_df], ignore_index=True)
            # 按id去重，保留最新的
            combined_df = combined_df.drop_duplicates(subset=["id"], keep="last")
        
        # 按时间排序
        combined_df = combined_df.sort_values("create_time").reset_index(drop=True)
        return combined_df


class TradeAnalyzer:
    """交易数据分析器"""
    
    @staticmethod
    def calculate_avg_price(df: pd.DataFrame) -> pd.DataFrame:
        """
        计算每个币种的买入均价
        
        只统计 side=buy 的记录（现货买入）
        净买入数量 = amount - fee（如果手续费币种是基础货币）
        平均价格 = 总买入金额 / 净买入数量
        """
        if df.empty:
            return pd.DataFrame()
        
        # 只统计现货买入记录（side=buy）
        buy_df = df[df["side"] == "buy"].copy()
        
        if buy_df.empty:
            print("没有现货买入记录")
            return pd.DataFrame()
        
        # 计算每笔交易的金额
        buy_df["cost"] = buy_df["price"] * buy_df["amount"]
        
        # 计算净买入数量（扣除用基础货币支付的手续费）
        # 如果 fee_currency == base_currency，则从 amount 中扣除 fee
        buy_df["net_amount"] = buy_df.apply(
            lambda row: row["amount"] - row["fee"] if row["fee_currency"] == row["base_currency"] else row["amount"],
            axis=1
        )
        
        # 如果手续费用计价货币支付，则从 cost 中扣除
        buy_df["net_cost"] = buy_df.apply(
            lambda row: row["cost"] - row["fee"] if row["fee_currency"] == row["quote_currency"] else row["cost"],
            axis=1
        )
        
        # 按基础货币分组统计
        stats = buy_df.groupby("base_currency").agg({
            "net_amount": "sum",
            "net_cost": "sum",
        }).reset_index()
        
        stats.columns = ["currency", "total_amount", "total_cost"]
        
        # 计算均价
        stats["avg_price"] = stats["total_cost"] / stats["total_amount"]
        
        # 按总成本排序
        stats = stats.sort_values("total_cost", ascending=False).reset_index(drop=True)
        
        return stats
    
    @staticmethod
    def print_stats(stats: pd.DataFrame) -> None:
        """打印统计信息到控制台"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        print()
        print(f"{'=' * 60}")
        print(f"  持仓均价统计 {today}")
        print(f"{'=' * 60}")
        print(f"{'币种':<10} {'买入数量':>8} {'买入金额(USDT)':>18} {'平均价格':>8}")
        print(f"{'-' * 60}")
        
        for _, row in stats.iterrows():
            print(f"{row['currency']:<10} {row['total_amount']:>15.5f} {row['total_cost']:>18.2f} {row['avg_price']:>15.6f}")
        
        print(f"{'=' * 60}")
        print(f"  总计: {len(stats)} 个币种，总买入金额: {stats['total_cost'].sum():.2f} USDT")
        print(f"{'=' * 60}")
    
    @staticmethod
    def save_daily_stats(stats: pd.DataFrame, output_path: Path = DAILY_STATS_CSV) -> None:
        """保存每日统计到CSV"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 添加日期列
        daily_stats = stats.copy()
        daily_stats.insert(0, "date", today)
        
        # 追加到文件
        if output_path.exists():
            existing_df = pd.read_csv(output_path)
            # 删除今天已有的记录（避免重复）
            existing_df = existing_df[existing_df["date"] != today]
            combined_df = pd.concat([existing_df, daily_stats], ignore_index=True)
        else:
            combined_df = daily_stats
        
        combined_df.to_csv(output_path, index=False)
        print(f"统计结果已保存到 {output_path}")


def main():
    """主函数"""
    print("=" * 60)
    print("  Gate.io 成交记录查询与持仓均价统计")
    print("=" * 60)
    print()
    
    # 初始化
    client = GateTradeClient()
    data_manager = TradeDataManager()
    analyzer = TradeAnalyzer()
    
    # 1. 加载本地缓存
    cached_df = data_manager.load()
    
    if cached_df.empty:
        print("本地无缓存，开始全量拉取...")
        new_trades = client.fetch_all_trades()
    else:
        print(f"本地已有 {len(cached_df)} 条记录")
        from_time = data_manager.get_last_trade_time(cached_df)
        print(f"最后记录时间: {datetime.fromtimestamp(from_time)}")
        new_trades = client.fetch_trades_since(from_time)
    
    # 2. 合并并保存
    df = data_manager.merge_trades(cached_df, new_trades)
    
    if df.empty:
        print("没有找到任何成交记录")
        return
    
    if new_trades or cached_df.empty:
        data_manager.save(df)
    else:
        print("没有新的成交记录")
    
    # 3. 计算均价（只统计现货买入）
    stats = analyzer.calculate_avg_price(df)
    
    if stats.empty:
        print("没有现货买入记录，无法计算均价")
        return
    
    # 4. 输出统计
    analyzer.print_stats(stats)
    
    # 5. 保存每日统计
    analyzer.save_daily_stats(stats)


if __name__ == "__main__":
    main()
