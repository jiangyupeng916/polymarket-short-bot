"""结算结果跟踪器 — 集中批量查询已下单市场的结算结果。

每轮结束后不立即检查, 而是把已下单的市场注册到待查列表,
由一个独立后台任务定时批量查询 Gamma API, 通过 outcomePrices 判断胜负。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict

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
        self._by_tag: dict[str, dict[str, int]] = defaultdict(
            lambda: {"win": 0, "reversal": 0}
        )

    @property
    def stats(self) -> dict:
        return {
            "total_win": self._total_win,
            "total_reversal": self._total_reversal,
            "by_tag": dict(self._by_tag),
        }

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
                self._by_tag[r["tag"]]["win"] += 1
            elif r["verdict"] == "REVERSAL":
                self._total_reversal += 1
                self._by_tag[r["tag"]]["reversal"] += 1

        if resolved:
            logger.info(
                "批量结算完成: 本轮结算=%d 累计胜=%d 累计反转=%d",
                len(resolved), self._total_win, self._total_reversal,
            )

    async def _query_resolution(self, item: dict) -> dict | None:
        """查询单个市场的结算结果。

        使用 Gamma API outcomePrices 判断胜负:
        - outcomePrices[0] (Yes/Up) > 0.9 → Up 赢
        - outcomePrices[1] (No/Down) > 0.9 → Down 赢

        Returns:
            dict with verdict if resolved; None if not yet resolved or query failed.
        """
        slug = item["slug"]
        try:
            resp = await asyncio.to_thread(
                lambda: requests.get(
                    f"{GAMMA_BASE}/markets/slug/{slug}", timeout=10
                )
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.debug("查询市场结算失败 slug=%s: %s", slug[:30], e)
            return None

        # ---- 方法1: outcomePrices (Gamma API 标准字段) ----
        prices_str = data.get("outcomePrices")
        if prices_str:
            try:
                prices = (
                    json.loads(prices_str)
                    if isinstance(prices_str, str)
                    else prices_str
                )
                if isinstance(prices, list) and len(prices) >= 2:
                    yes_price = float(prices[0])
                    no_price = float(prices[1])

                    if yes_price > 0.9:
                        up_won = True
                    elif no_price > 0.9:
                        up_won = False
                    else:
                        # 价格不在极端位置: 市场可能尚未结算
                        if not data.get("closed"):
                            return None
                        # 已关闭但价格异常 (极小概率), 放回队列重试
                        logger.debug(
                            "市场已关闭但价格不极端 slug=%s yes=%.4f no=%.4f",
                            slug[:30], yes_price, no_price,
                        )
                        return None

                    won = (
                        (item["direction"] == "Up" and up_won)
                        or (item["direction"] == "Down" and not up_won)
                    )
                    item["verdict"] = "WIN" if won else "REVERSAL"
                    return item

            except (ValueError, TypeError, json.JSONDecodeError):
                pass

        # ---- 方法2: 没有 outcomePrices, 用 closed 兜底 ----
        if not data.get("closed"):
            return None

        # 已关闭但无法判断 → 放回队列重试 (不放弃)
        logger.debug("无法判断市场胜负 slug=%s", slug[:30])
        return None
