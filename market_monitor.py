"""单市场行情监控器 — 每个币种×周期一个实例, 独立任务, 故障隔离。

对应 ARCHITECTURE 第三、六、七节:
- 每个市场 (币种×周期) 独立订阅, 一个市场故障不影响其他市场
- 轮次生命周期: 获取当前轮 slug → 市场 → token → 订阅 → 监控 → 到期切换
- 触发条件 (AND): best_bid >= MIN_BID, 已过冷静期, 该轮未下单, 失败冷却后可重试
- 超时检测: 差异化无数据超时 (高频 5s / 低频 300s), 断流指数退避重连
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

from polymarket.streams import MarketSpec

from config import (
    COOLDOWN_SECONDS,
    GAMMA_RETRY_DELAYS,
    HEARTBEAT_SECONDS,
    MIN_BID,
    NO_DATA_TIMEOUT,
    RECONNECT_BACKOFF,
)
from slug import current_slug, round_end, round_start

logger = logging.getLogger(__name__)
triggers_logger = logging.getLogger("triggers")

MIN_BID_DEC = Decimal(str(MIN_BID))


class MarketMonitor:
    def __init__(self, coin: str, period: str, public_client, order_manager) -> None:
        self.coin = coin
        self.period = period
        self._tag = f"{coin}/{period}"
        self._log = logging.getLogger(f"monitor.{coin}.{period}")
        self._public = public_client
        self._orders = order_manager

        # 当前轮市场状态
        self._up_token: str | None = None
        self._down_token: str | None = None
        self._round_end_ts = 0
        self._cooldown_until = 0.0
        self._tick_size = Decimal("0.01")
        self._min_order_size = Decimal("5")

        # 行情缓存: token_id -> best_bid / best_ask
        self._best_bid: dict[str, Decimal] = {}
        self._best_ask: dict[str, Decimal] = {}

        # 内部状态
        self._stop = False
        self._last_event_time = 0.0
        self._msg_count = 0
        self._order_count = 0
        self._pending: set[str] = set()  # 下单进行中的 token, 防重复 create_task

    def stop(self) -> None:
        self._stop = True

    # ================= 主循环 =================
    async def run(self) -> None:
        while not self._stop:
            try:
                await self._monitor_round()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - 任何异常都不拖垮整个 bot
                self._log.error("监控异常: %s", e, exc_info=True)
                await asyncio.sleep(2)

    async def _monitor_round(self) -> None:
        """监控一轮: 获取市场 → 订阅监控 → 到期切换。"""
        slug = current_slug(self.coin, self.period)
        self._log.info("进入轮次, slug=%s", slug)

        market = await self._fetch_market_with_retry(slug)
        if market is None:
            # 市场获取失败: 等待接近下一轮再重试, 避免反复打空
            await asyncio.sleep(5)
            return

        if not market.state.accepting_orders:
            self._log.debug("市场未开单 (accepting_orders=False), 稍后重试")
            await asyncio.sleep(5)
            return

        # 读取约束与 token
        self._tick_size = market.trading.minimum_tick_size or Decimal("0.01")
        self._min_order_size = market.trading.minimum_order_size or Decimal("5")
        self._up_token = market.outcomes.yes.token_id
        self._down_token = market.outcomes.no.token_id
        self._round_end_ts = round_end(self.period)
        self._cooldown_until = round_start(self.period) + COOLDOWN_SECONDS[self.period]

        self._orders.clear_round()
        self._best_bid = {self._up_token: None, self._down_token: None}
        self._best_ask = {self._up_token: None, self._down_token: None}
        self._msg_count = 0
        self._last_event_time = time.time()

        self._log.info(
            "市场=%s 轮次至 %s tick=%s min_order=%s 冷静期至 %s",
            market.question,
            time.strftime("%H:%M:%S", time.localtime(self._round_end_ts)),
            self._tick_size,
            self._min_order_size,
            time.strftime("%H:%M:%S", time.localtime(self._cooldown_until)),
        )

        # 订阅监控 (含断流重连), 直到轮次结束
        backoff_idx = 0
        while not self._stop and time.time() < self._round_end_ts:
            try:
                await self._consume_stream()
                break  # 正常退出 (轮次结束/主动停止)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - 断流/连接异常 → 退避重连
                delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                backoff_idx += 1
                self._log.warning("订阅断开 (%s), %.0fs 后重连", e, delay)
                await asyncio.sleep(delay)

    # ================= 市场获取 =================
    async def _fetch_market_with_retry(self, slug: str):
        """Gamma 市场获取, 失败按 GAMMA_RETRY_DELAYS 重试。"""
        for delay in GAMMA_RETRY_DELAYS:
            try:
                return await self._public.get_market(slug=slug)
            except Exception as e:  # noqa: BLE001
                self._log.warning("获取市场失败 (%s), %.0fs 后重试", e, delay)
                await asyncio.sleep(delay)
        # 最后一次
        try:
            return await self._public.get_market(slug=slug)
        except Exception as e:  # noqa: BLE001
            self._log.error("获取市场最终失败: %s", e)
            return None

    # ================= 订阅与消费 =================
    async def _consume_stream(self) -> None:
        token_ids = [self._up_token, self._down_token]
        timeout = NO_DATA_TIMEOUT[self.period]

        async with await self._public.subscribe(
            MarketSpec(token_ids=token_ids, custom_feature_enabled=True),
        ) as stream:
            self._log.info("已订阅 token 数=%d, 超时=%ds", len(token_ids), timeout)
            last_heartbeat = time.time()

            while not self._stop and time.time() < self._round_end_ts:
                try:
                    event = await asyncio.wait_for(stream.__anext__(), timeout=timeout)
                except asyncio.TimeoutError:
                    raise ConnectionError(
                        f"无行情数据超过 {timeout}s (period={self.period})"
                    )
                except StopAsyncIteration:
                    raise ConnectionError("行情流正常结束")

                self._handle_event(event)

                # 心跳
                if time.time() - last_heartbeat >= HEARTBEAT_SECONDS:
                    last_heartbeat = time.time()
                    self._log.info(
                        "心跳: 收消息=%d 已下单=%d best_bid=%s",
                        self._msg_count, self._order_count, self._best_bid,
                    )

                self._check_triggers()

    # ================= 事件处理 =================
    def _handle_event(self, event) -> None:
        self._last_event_time = time.time()
        self._msg_count += 1
        p = event.payload

        if event.type == "book":
            if p.token_id in self._best_bid:
                if p.bids:
                    self._best_bid[p.token_id] = p.bids[0].price
                if p.asks:
                    self._best_ask[p.token_id] = p.asks[0].price
                if p.tick_size is not None:
                    self._tick_size = p.tick_size
        elif event.type == "price_change":
            for pc in p.price_changes:
                if pc.token_id in self._best_bid:
                    if pc.best_bid is not None:
                        self._best_bid[pc.token_id] = pc.best_bid
                    if pc.best_ask is not None:
                        self._best_ask[pc.token_id] = pc.best_ask
        elif event.type == "best_bid_ask":
            if p.token_id in self._best_bid:
                if p.best_bid is not None:
                    self._best_bid[p.token_id] = p.best_bid
                if p.best_ask is not None:
                    self._best_ask[p.token_id] = p.best_ask
        elif event.type == "tick_size_change":
            self._tick_size = p.new_tick_size

    # ================= 触发与下单 =================
    def _check_triggers(self) -> None:
        now = time.time()
        if now < self._cooldown_until:
            return

        for token_id, bid in list(self._best_bid.items()):
            if bid is None or bid < MIN_BID_DEC:
                continue
            if self._orders.is_ordered(token_id):
                continue
            if not self._orders.can_retry(token_id, now):
                continue
            if token_id in self._pending:
                continue

            direction = "Up" if token_id == self._up_token else "Down"
            triggers_logger.info(
                "TRIGGER %s %s %s best_bid=%s cooldown_passed",
                self._tag, direction, token_id, bid,
            )
            self._log.info("触发买入 %s best_bid=%s", direction, bid)
            asyncio.create_task(self._try_order(token_id, bid))

    async def _try_order(self, token_id: str, bid: Decimal) -> None:
        if token_id in self._pending:
            return
        self._pending.add(token_id)
        try:
            ok = await self._orders.place(
                token_id=token_id,
                price=bid,
                tick_size=self._tick_size,
                min_order_size=self._min_order_size,
                now=time.time(),
            )
            if ok:
                self._order_count += 1
        finally:
            self._pending.discard(token_id)
