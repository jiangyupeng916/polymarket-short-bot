"""集中配置 — 所有策略参数在这里调整。

探测确认(2026-08-08, polymarket-client 0.5.0):
- slug 规律: 5m/15m/4h 用时间戳型, 1h 用日期型(EDT=UTC-4)
- minimum_order_size = 5 (USDC notional), 低于会被 CLOB 拒绝
- tick_size: 5m/15m/4h = 0.01, 1h = 0.001 (必须动态读取, 不能写死)
"""
from __future__ import annotations

# ---- 监控对象 ----
COINS = ["btc", "eth"]
PERIODS = ["5m", "15m"]

# 币种缩写 -> 全称 (日期型 slug 用)
COIN_FULL_NAMES = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "xrp",
    "doge": "dogecoin",
    "bnb": "bnb",
    "hype": "hype",
}

# 周期 -> 秒
INTERVAL_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}

# 时间戳型 slug 的周期 (日期型只有 1h)
TIMESTAMP_PERIODS = ("5m", "15m", "4h")

# ---- 下单策略 ----
MIN_BID = 0.98           # 触发阈值: best_bid >= 0.98 才考虑下单
ORDER_PRICE = 0.99        # 固定挂单价: 触发后始终以 0.99 挂限价买单

# 每单下单份数 (shares)。
# ⚠️ notional = ORDER_PRICE × ORDER_SIZE = 0.99 × 5 = 4.95,
#    略低于 CLOB 的 minimum_order_size=5, 如被拒请调大份数 (如 6 份)。
ORDER_SIZE = 5.0

# 冷静期: 轮次前期不交易 (秒)。防止过早入场被噪音误导。
COOLDOWN_SECONDS = {"5m": 270, "15m": 840, "1h": 2700, "4h": 12600}

# ---- 行情超时 (秒) ----
# 高频市场 (5m/15m) 消息密集, 断流 5 秒即告警; 低频 (1h/4h) 可能长时间无成交。
NO_DATA_TIMEOUT = {"5m": 5, "15m": 5, "1h": 300, "4h": 300}

# 重连指数退避 (秒)
RECONNECT_BACKOFF = [1, 2, 5, 10, 20, 30]

# Gamma API 失败重试间隔 (秒)
GAMMA_RETRY_DELAYS = [2.0, 4.0]

# ---- 结算跟踪 ----
# 集中批量查询结算结果的间隔 (秒)。下单后不立即查, 等一段时间后统一查。
RESOLUTION_POLL_SECONDS = 600  # 10 分钟

# ---- 日志 ----
LOG_DIR = "data"                      # 相对项目根目录
LOG_MAX_BYTES = 200 * 1024 * 1024     # 200MB
LOG_BACKUP_COUNT = 5                  # 保留 5 个历史文件
HEARTBEAT_SECONDS = 60                # 心跳日志间隔
