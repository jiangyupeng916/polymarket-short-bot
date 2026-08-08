"""slug 构建 — Polymarket 短线 Up/Down 市场轮次 slug 规律。

两种格式(2026-08-08 真实数据探测验证):
- 时间戳型 (5m/15m/4h): {缩写}-updown-{周期}-{unix秒}
  时间戳 = (UTC秒 // 周期间隔) × 周期间隔
- 日期型 (1h): {全称}-up-or-down-{月}-{日}-{年}-{小时}{ampm}-et
  美东时间 (America/New_York, 自动处理 EDT/EST 夏令时), 月份小写, 小时 12 小时制
"""
from __future__ import annotations

import datetime
import time
from typing import Optional
from zoneinfo import ZoneInfo

from config import COIN_FULL_NAMES, INTERVAL_SECONDS, TIMESTAMP_PERIODS

NEW_YORK = ZoneInfo("America/New_York")


def _ny(now_ts: float) -> datetime.datetime:
    """unix 秒 -> 美东时间 (带时区, 自动 EDT/EST)。"""
    return datetime.datetime.fromtimestamp(now_ts, tz=datetime.timezone.utc).astimezone(
        NEW_YORK
    )


def timestamp_slug(coin: str, period: str, now_ts: Optional[float] = None) -> str:
    """时间戳型 slug: {缩写}-updown-{周期}-{unix秒}, 时间戳对齐到轮次起点。"""
    interval = INTERVAL_SECONDS[period]
    now = time.time() if now_ts is None else now_ts
    ts = int((now // interval) * interval)
    return f"{coin}-updown-{period}-{ts}"


def date_slug(coin: str, now_ts: Optional[float] = None) -> str:
    """日期型 slug (1h): {全称}-up-or-down-{月}-{日}-{年}-{小时}{ampm}-et。"""
    full = COIN_FULL_NAMES[coin]
    now = time.time() if now_ts is None else now_ts
    ny = _ny(now)
    hour12 = ny.hour % 12
    if hour12 == 0:
        hour12 = 12
    ampm = "am" if ny.hour < 12 else "pm"
    return (
        f"{full}-up-or-down-{ny.strftime('%B').lower()}-"
        f"{ny.day}-{ny.year}-{hour12}{ampm}-et"
    )


def current_slug(coin: str, period: str, now_ts: Optional[float] = None) -> str:
    """当前轮次的 slug。"""
    if period in TIMESTAMP_PERIODS:
        return timestamp_slug(coin, period, now_ts)
    return date_slug(coin, now_ts)


def round_start(period: str, now_ts: Optional[float] = None) -> int:
    """当前轮次起点 (unix 秒)。"""
    now = time.time() if now_ts is None else now_ts
    if period in TIMESTAMP_PERIODS:
        interval = INTERVAL_SECONDS[period]
        return int((now // interval) * interval)
    # 1h: 美东整点
    ny = _ny(now).replace(minute=0, second=0, microsecond=0)
    return int(ny.timestamp())


def round_end(period: str, now_ts: Optional[float] = None) -> int:
    """当前轮次终点 (unix 秒)。"""
    return round_start(period, now_ts) + INTERVAL_SECONDS[period]
