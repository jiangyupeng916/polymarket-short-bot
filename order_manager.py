"""下单管理 — 串行下单、去重、tick 对齐、失败冷却。

设计要点 (对应 ARCHITECTURE 第六节):
- asyncio.Lock 串行化所有下单请求, 避免并发撞 CLOB 限流
- 去重按 market_tag 隔离: 每轮每个 token 最多下一单, 一个市场切轮
  只清自己的记录, 绝不影响其他市场 (4h 等长周期市场)
- 价格必须是对齐到 market tick_size 的倍数, 且 clamp 到 0.99 (CLOB 限价单上限)
- 下单失败进入冷却期 (按市场隔离), 冷却后允许重试 (失败不移除任何已成功标记)
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

from config import ORDER_SIZE

logger = logging.getLogger(__name__)

MAX_PRICE = Decimal("0.99")        # CLOB 限价单价格上限
RETRY_COOLDOWN = 60.0              # 下单失败后的重试冷却 (秒)


class OrderManager:
    def __init__(
        self,
        secure_client,
        order_size: float = ORDER_SIZE,
        dry_run: bool = False,
    ) -> None:
        self._client = secure_client
        self._size = Decimal(str(order_size))
        self._dry_run = dry_run
        self._lock = asyncio.Lock()
        # 去重/冷却按市场隔离: {market_tag: ...}。
        # 绝不能全局共享——一个市场切轮清空会误删其他市场 (如 4h) 的去重记录。
        self._ordered: dict[str, set[str]] = {}          # market_tag -> 已下单 token 集合
        self._failed_at: dict[str, dict[str, float]] = {}  # market_tag -> token -> 失败时间
        self._approvals_done = False

    # ---- 授权 ----
    async def ensure_approvals(self) -> None:
        """首次交易前授权交易所合约花 pUSD 和 token (一次即可, 幂等)。"""
        if self._approvals_done:
            return
        logger.info("首次交易授权 setup_trading_approvals() ...")
        await self._client.setup_trading_approvals()
        self._approvals_done = True
        logger.info("交易授权完成")

    # ---- 工具 ----
    @staticmethod
    def align_price(price: Decimal, tick_size: Decimal) -> Decimal:
        """把价格向下对齐到 tick 网格 (如 tick=0.001 时 0.995 -> 0.995, 0.99 保持)。"""
        if tick_size <= 0:
            return price
        aligned = (price / tick_size).to_integral_value(rounding="ROUND_DOWN") * tick_size
        return aligned

    # ---- 去重 / 冷却 (按市场隔离) ----
    def is_ordered(self, market_tag: str, token_id: str) -> bool:
        return token_id in self._ordered.get(market_tag, ())

    def can_retry(self, market_tag: str, token_id: str, now: float) -> bool:
        last = self._failed_at.get(market_tag, {}).get(token_id)
        return last is None or (now - last) >= RETRY_COOLDOWN

    def mark_failed(self, market_tag: str, token_id: str, now: float) -> None:
        self._failed_at.setdefault(market_tag, {})[token_id] = now

    def clear_round(self, market_tag: str) -> None:
        """该市场轮次切换时清空其去重与冷却记录 (只清本市场, 不动其他市场)。"""
        self._ordered.pop(market_tag, None)
        self._failed_at.pop(market_tag, None)

    # ---- 下单 ----
    async def place(
        self,
        market_tag: str,
        token_id: str,
        price: Decimal,
        tick_size: Decimal,
        now: float,
    ) -> bool:
        """串行限价下单 (BUY, GTC, 固定份数).

        market_tag: 市场标识 (如 "btc/5m"), 用于隔离去重与冷却。
        Returns:
            True 下单成功; False 下单失败/被拒 (已进入冷却)。
        """
        async with self._lock:
            if token_id in self._ordered.get(market_tag, ()):
                return False

            # 价格: clamp 到 0.99 上限后对齐到 tick 网格
            price = self.align_price(min(price, MAX_PRICE), tick_size)

            # size: 固定份数 (config.ORDER_SIZE)
            size = self._size

            # DRY-RUN: 只模拟, 不真实下单 (验证/测试用)
            if self._dry_run:
                logger.info(
                    "[DRY-RUN] 模拟下单 token=%s price=%s size=%s (未真实发送)",
                    token_id, price, size,
                )
                self._ordered.setdefault(market_tag, set()).add(token_id)
                return True

            try:
                await self.ensure_approvals()
                response = await self._client.place_limit_order(
                    token_id=token_id,
                    side="BUY",
                    price=str(price),
                    size=str(size),
                )
            except Exception as e:  # noqa: BLE001 - 任何异常都进入失败处理
                logger.error("下单异常 token=%s: %s", token_id, e)
                self.mark_failed(market_tag, token_id, now)
                return False

            if response.ok:
                logger.info(
                    "下单成功 token=%s price=%s size=%s status=%s order_id=%s",
                    token_id, price, size, response.status, response.order_id,
                )
                self._ordered.setdefault(market_tag, set()).add(token_id)
                return True

            logger.warning(
                "下单被拒 token=%s code=%s msg=%s", token_id, response.code, response.message
            )
            self.mark_failed(market_tag, token_id, now)
            return False
