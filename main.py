#!/usr/bin/env python3
"""Polymarket 短线 Up/Down 市场自动监控 bot 入口。

用法:
    python main.py            # 默认实例 bot1
    python main.py bot1       # 指定实例 (读取 .env.bot1)

对应开发流程模板 4-8 节:
- 多账户: 每实例独立 .env.<instance> / 日志目录 / screen 会话
- SIGTERM 优雅退出 (start_bot.sh/stop_bot.sh 依赖)
- RotatingFileHandler 日志轮转, 触发日志单独文件
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

from dotenv import load_dotenv
from polymarket import AsyncPublicClient, AsyncSecureClient, RelayerApiKey

import config
from market_monitor import MarketMonitor
from order_manager import OrderManager
from resolution_tracker import ResolutionTracker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("main")

CREDENTIAL_KEYS = (
    "SIGNER_PRIVATE_KEY",
    "POLYMARKET_WALLET_ADDRESS",
    "POLYMARKET_RELAYER_API_KEY",
    "POLYMARKET_RELAYER_API_KEY_ADDRESS",
)


# ================= 日志 =================
def setup_logging(instance: str) -> None:
    log_dir = os.path.join(BASE_DIR, config.LOG_DIR, instance)
    os.makedirs(log_dir, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # 控制台: INFO
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)

    # 文件: DEBUG, 200MB × 5 轮转
    file_h = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(fmt)
    root.addHandler(file_h)

    # 触发日志: 单独文件, 每天轮转
    triggers = logging.getLogger("triggers")
    triggers.setLevel(logging.INFO)
    triggers.propagate = False
    trig_h = TimedRotatingFileHandler(
        os.path.join(log_dir, "triggers.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    trig_h.setLevel(logging.INFO)
    trig_h.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    triggers.addHandler(trig_h)

    # 结果日志: 每单胜负记录, 每天轮转, 保留 90 天
    results = logging.getLogger("results")
    results.setLevel(logging.INFO)
    results.propagate = False
    res_h = TimedRotatingFileHandler(
        os.path.join(log_dir, "results.log"),
        when="midnight",
        backupCount=90,
        encoding="utf-8",
    )
    res_h.setLevel(logging.INFO)
    res_h.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    results.addHandler(res_h)

    # 第三方库日志降噪 (文件 DEBUG 级别下 httpx/websockets 会刷屏)
    for noisy in ("httpx", "httpcore", "hpack", "h2", "websockets", "websockets.client"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ================= 凭据 =================
def load_credentials(instance: str) -> None:
    env_path = os.path.join(BASE_DIR, f".env.{instance}")
    if not os.path.exists(env_path):
        raise SystemExit(f"凭据文件不存在: {env_path}")
    load_dotenv(env_path, override=True)
    missing = [k for k in CREDENTIAL_KEYS if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"凭据文件缺少字段: {', '.join(missing)}")


# ================= 主逻辑 =================
async def _create_secure_client() -> AsyncSecureClient:
    """创建安全客户端。网络瞬时错误 (TLS 握手失败等) 自动重试。"""
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            return await AsyncSecureClient.create(
                private_key=os.environ["SIGNER_PRIVATE_KEY"],
                wallet=os.environ["POLYMARKET_WALLET_ADDRESS"],
                api_key=RelayerApiKey(
                    key=os.environ["POLYMARKET_RELAYER_API_KEY"],
                    address=os.environ["POLYMARKET_RELAYER_API_KEY_ADDRESS"],
                ),
            )
        except Exception as e:  # noqa: BLE001 - 网络瞬时错误重试
            last_error = e
            logger.warning("创建 SecureClient 失败 (第 %d/5 次): %s", attempt, e)
            if attempt < 5:
                await asyncio.sleep(2 * attempt)
    raise RuntimeError(f"SecureClient 创建多次失败: {last_error}")


async def run(instance: str, dry_run: bool) -> None:
    setup_logging(instance)
    load_credentials(instance)
    logger.info("==== 启动 Polymarket 短线监控 bot instance=%s ====", instance)
    logger.info("监控: 币种=%s 周期=%s", config.COINS, config.PERIODS)
    logger.info("阈值 MIN_BID=%s 每单份数=%s", config.MIN_BID, config.ORDER_SIZE)
    logger.info("下单模式: %s", "DRY-RUN (模拟下单, 不真实发送)" if dry_run else "真实下单")

    async with AsyncPublicClient() as public:
        secure = await _create_secure_client()
        async with secure:
            logger.info("账户连接成功 wallet=%s type=%s", secure.wallet, secure.wallet_type)

            order_manager = OrderManager(secure, dry_run=dry_run)
            tracker = ResolutionTracker()
            monitors = [
                MarketMonitor(coin, period, public, order_manager, tracker)
                for coin in config.COINS
                for period in config.PERIODS
            ]
            logger.info("启动 %d 个市场监控任务", len(monitors))

            tasks = [asyncio.create_task(m.run()) for m in monitors]
            poll_task = asyncio.create_task(tracker.poll_loop())

            def shutdown() -> None:
                logger.info("收到退出信号, 优雅关闭 %d 个任务...", len(monitors))
                tracker.stop()
                for m in monitors:
                    m.stop()

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, shutdown)
                except (NotImplementedError, RuntimeError):
                    # Windows 等平台对部分信号不支持 add_signal_handler,
                    # 由 KeyboardInterrupt 兜底
                    pass

            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                pass
            finally:
                for m in monitors:
                    m.stop()
                tracker.stop()
                for t in tasks + [poll_task]:
                    t.cancel()
                await asyncio.gather(*tasks, poll_task, return_exceptions=True)
                s = tracker.stats
                wins, reversals = s["total_win"], s["total_reversal"]
                logger.info(
                    "==== 已全部退出 结算统计: 胜=%d 反转=%d 已结算=%d 胜率=%.1f%% ====",
                    wins, reversals, wins + reversals,
                    wins / (wins + reversals) * 100 if (wins + reversals) > 0 else 0,
                )
                by_tag = s.get("by_tag", {})
                if by_tag:
                    profit_per_win = (1.0 - config.ORDER_PRICE) * config.ORDER_SIZE
                    loss_per_rev = config.ORDER_PRICE * config.ORDER_SIZE
                    logger.info("==== 按市场结算明细 ====")
                    logger.info("%-12s %6s %6s %7s %8s %8s %8s",
                                "市场", "WIN", "REV", "胜率", "盈利", "亏损", "净利")
                    grand_win = 0; grand_rev = 0
                    for tag in sorted(by_tag.keys()):
                        t = by_tag[tag]
                        r = t["win"] + t["reversal"]
                        rate = f"{t['win'] / r * 100:.1f}%" if r > 0 else "N/A"
                        wp = t["win"] * profit_per_win
                        rl = t["reversal"] * loss_per_rev
                        net = wp - rl
                        grand_win += t["win"]; grand_rev += t["reversal"]
                        logger.info("%-12s %6d %6d %7s %+8.2f %+8.2f %+8.2f",
                                    tag, t["win"], t["reversal"], rate, wp, -rl, net)
                    gr = grand_win + grand_rev
                    gwp = grand_win * profit_per_win
                    grl = grand_rev * loss_per_rev
                    gn = gwp - grl
                    logger.info("%-12s %6d %6d %7s %+8.2f %+8.2f %+8.2f",
                                "总计", grand_win, grand_rev,
                                f"{grand_win / gr * 100:.1f}%" if gr > 0 else "N/A",
                                gwp, -grl, gn)
                    inv = gr * config.ORDER_PRICE * config.ORDER_SIZE
                    logger.info("总投入: $%.2f  ROI: %.2f%%", inv, gn / inv * 100 if inv > 0 else 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket 短线监控 bot")
    parser.add_argument("instance", nargs="?", default="bot1", help="实例名, 默认 bot1")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="模拟下单 (不真实发送订单), 用于验证/测试",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(args.instance, dry_run=args.dry_run))
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C, 退出")


if __name__ == "__main__":
    main()
