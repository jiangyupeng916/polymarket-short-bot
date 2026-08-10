"""结算结果跟踪器 — 集中批量查询已下单市场的结算结果。

每轮结束后不立即检查, 而是把已下单的市场注册到待查列表,
由一个独立后台任务定时批量查询 Gamma API, 通过 tokens[].winner 判断胜负。
"""
from __future__ import annotations

import asyncio
import logging
import time

import requests

from config import RESOLUTION_POLL_SECONDS

logger = logging.getLogger(__name__)
results_logger = logging.getLogger("results")

GAMMA_BASE = "https://gamma-api.polymarket.com"


class ResolutionTracker:
    """集中管理待结算市场的查询与结果记录。"""

    def __init__(self, poll_seconds: int = RESOLUTION_POLL_SECONDS) -> None:
        self._pending: list[dict] = []
        self._lock = asyncio.Lock()
        self._poll_seconds = poll_seconds
        self._stop = False
        self._total_win = 0
        self._total_reversal = 0

    @property
    def stats(self) -> tuple[int, int]:
        return self._total_win, self._total_reversal

    def stop(self) -> None:
        self._stop = True

    # ---- MarketMonitor 注册接口 ----
    async def register(
        self,
        slug: str,
        tag: str,
        up_token_id: str,
        down_token_id: str,
        direction: str,
        token_id: str,
    ) -> None:
        """下单成功后注册, 等待后续批量查询结算结果。"""
        async with self._lock:
            self._pending.append({
                "slug": slug,
                "tag": tag,
                "up_token_id": up_token_id,
                "down_token_id": down_token_id,
                "direction": direction,
                "token_id": token_id,
                "time": time.time(),
            })

    # ---- 后台轮询 ----
    async def poll_loop(self) -> None:
        """后台任务: 每隔 _poll_seconds 批量查询一次所有待结算市场。"""
        logger.info("结算跟踪器启动, 轮询间隔=%ds", self._poll_seconds)
        while not self._stop:
            await asyncio.sleep(self._poll_seconds)
            await self._check_pending()

    async def _check_pending(self) -> None:
        """批量查询待结算市场, 记录胜/反转到 results.log。"""
        async with self._lock:
            if not self._pending:
                return
            pending = self._pending[:]
            self._pending.clear()

        resolved: list[dict] = []
        unresolved: list[dict] = []

        for item in pending:
            result = await self._query_resolution(item)
            if result is None:
                unresolved.append(item)
            else:
                resolved.append(result)

        # 未结算的放回队列, 下次再查
        if unresolved:
            async with self._lock:
                self._pending.extend(unresolved)
            logger.debug("仍有 %d 个市场未结算, 下次再查", len(unresolved))

        # 记录已结算的结果
        for r in resolved:
            results_logger.info(
                "%s %s %s %s slug=%s",
                r["tag"], r["direction"], r["token_id"][:12], r["verdict"], r["slug"],
            )
            logger.info(
                "结算 %s/%s: %s -> %s",
                r["tag"], r["direction"], r["token_id"][:12], r["verdict"],
            )
            if r["verdict"] == "WIN":
                self._total_win += 1
            elif r["verdict"] == "REVERSAL":
                self._total_reversal += 1

        if resolved:
            logger.info(
                "批量结算完成: 本轮结算=%d 累计胜=%d 累计反转=%d",
                len(resolved), self._total_win, self._total_reversal,
            )

    async def _query_resolution(self, item: dict) -> dict | None:
        """查询单个市场的结算结果。

        Returns:
            dict with verdict if resolved; None if not yet resolved or query failed.
        """
        slug = item["slug"]
        try:
            # 直接用 Gamma API 获取原始 JSON, SDK 的 Market 模型不暴露 tokens[].winner
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    f"{GAMMA_BASE}/markets/slug/{slug}", timeout=10
                )
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.debug("查询市场结算失败 slug=%s: %s", slug[:30], e)
            # 查询失败放回队列重试 (网络波动, 非最终失败)
            return None

        # 检查市场是否已关闭 (已结算)
        state_closed = data.get("closed", False)
        if not state_closed:
            # 尚未结算, 放回队列
            return None

        # 市场已结算: 通过 tokens[].winner 判断胜负
        tokens = data.get("tokens", [])
        up_won = None  # True=Up赢了, False=Down赢了
        for t in tokens:
            if t.get("token_id") == item["up_token_id"] and t.get("winner"):
                up_won = True
                break
            elif t.get("token_id") == item["down_token_id"] and t.get("winner"):
                up_won = False
                break

        if up_won is None:
            # tokens 中没有 winner 信息, 尝试用 price 兜底
            for t in tokens:
                price = t.get("price")
                if price is None:
                    continue
                price = float(price)
                if t.get("token_id") == item["up_token_id"]:
                    if price > 0.9:
                        up_won = True
                    elif price < 0.1:
                        up_won = False

        if up_won is None:
            logger.debug("无法判断市场胜负 slug=%s tokens=%s", slug[:30], tokens)
            # 已关闭但无法判断 → 标记 UNRESOLVED, 不再放回队列
            item["verdict"] = "UNRESOLVED"
            return item

        won = (
            (item["direction"] == "Up" and up_won)
            or (item["direction"] == "Down" and not up_won)
        )
        item["verdict"] = "WIN" if won else "REVERSAL"
        return item
