# Polymarket 短线 Up/Down 自动监控 bot

实时监控 Polymarket 加密货币短线 Up/Down 市场,当某个方向的买一价达到阈值(默认 `best_bid >= 0.99`)时,自动以限价单买入,赌该方向在剩余时间内获胜。

- **监控对象**:BTC / ETH / SOL / XRP / DOGE / BNB / HYPE × 5m / 15m / 1h / 4h 共 **28 个市场**
- **技术栈**:Python 3.12 · `polymarket-client` 0.5.0(SDK)· asyncio
- **部署形态**:多账户独立进程(`bot1`/`bot2`/...),每实例独立 `.env`、日志、screen 会话

---

## 一、整体框架

```
                    ┌──────────────────────────────────────────────┐
                    │               main.py (入口)                   │
                    │  解析实例 → 加载凭据 → 初始化日志 → 建客户端      │
                    └──────┬───────────────────────┬───────────────┘
                           │                       │
              AsyncPublicClient          AsyncSecureClient + OrderManager
              (公开数据)                    (交易, 含 Relayer 免 gas)
                           │                       │
              ┌────────────┼───────────────────────┼────────────┐
              ▼            ▼            ▼            ▼            ▼
        MarketMonitor  MarketMonitor  MarketMonitor  MarketMonitor ...
        (btc/5m)       (btc/15m)      (btc/1h)       (btc/4h)      (28 个任务)
              │            │            │            │
              ▼            ▼            ▼            ▼
        ┌────────────────────────────────────────────────────────┐
        │  每个 MarketMonitor  = 一个独立 asyncio 任务 (故障隔离)     │
        │                                                        │
        │  slug.py 构建 slug ──► 获取市场 ──► 订阅 2 个 token        │
        │                                                        │
        │  监听 book / price_change / best_bid_ask 事件            │
        │  维护 best_bid ──► 触发判断 (阈值/冷静期/去重/冷却)         │
        │                                                        │
        │  触发 ──► OrderManager.place()  (全局串行锁, 防撞限流)     │
        └────────────────────────────────────────────────────────┘
```

**设计核心:一市场一任务**。28 个市场各自独立运行,一个市场的行情源故障、下单异常、轮次切换都不会影响其他市场。

---

## 二、目录结构

```
polymarket-short-monitor/
├── main.py             # 入口: 参数解析、凭据加载、日志、信号处理、启动所有任务
├── config.py           # 集中配置: 所有策略参数
├── slug.py             # slug 构建: 两种格式 + 轮次时间计算
├── market_monitor.py   # 单市场监控器: 订阅、事件处理、触发决策、轮次切换、重连
├── order_manager.py    # 下单管理: 串行化、去重、tick 对齐、失败冷却、dry-run
├── .env.bot1           # 凭据 (不进 git)
├── requirements.txt    # 依赖
├── data/bot1/          # 运行日志 (app.log + triggers.log, 不进 git)
└── README.md
```

---

## 三、模块详解

### 1. `config.py` — 集中配置

**作用**:所有策略参数集中一处,改参数不需要动业务代码。

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `COINS` | `["btc","eth","sol","xrp","doge","bnb","hype"]` | 监控币种 |
| `PERIODS` | `["5m","15m","1h","4h"]` | 监控周期 |
| `COIN_FULL_NAMES` | `btc→bitcoin` 等 | 缩写↔全称映射(日期型 slug 用) |
| `INTERVAL_SECONDS` | `5m:300 / 15m:900 / 1h:3600 / 4h:14400` | 轮次时长 |
| `MIN_BID` | `0.99` | 触发阈值,`best_bid >= 0.99` 才考虑下单 |
| `ORDER_SIZE` | `5.0` | 每单下单份数(shares),价格固定 clamp 0.99 对齐 tick |
| `COOLDOWN_SECONDS` | `5m:210 / 15m:660 / 1h:2700 / 4h:12600` | 轮次前期冷静期,时间换确定性 |
| `NO_DATA_TIMEOUT` | `5m/15m:5s / 1h/4h:300s` | 差异化无数据超时(高频/低频) |
| `RECONNECT_BACKOFF` | `[1,2,5,10,20,30]` | 断流重连指数退避 |
| `GAMMA_RETRY_DELAYS` | `[2,4]` | 市场获取失败重试间隔 |
| `LOG_*` | 200MB×5 轮转 | 日志配置 |

### 2. `slug.py` — slug 构建

**作用**:Polymarket 短线市场的 slug 有固定规律,据此定位"当前轮次"的市场。两种格式(已用真实数据验证, 2026-08-08):

**时间戳型**(5m / 15m / 4h):
```
{缩写}-updown-{周期}-{unix秒}
  例: btc-updown-5m-1786200600
  时间戳 = (当前UTC秒 // 周期间隔) × 周期间隔   ← 对齐到轮次起点
```

**日期型**(1h):
```
{全称}-up-or-down-{月}-{日}-{年}-{小时}{ampm}-et
  例: bitcoin-up-or-down-august-8-2026-10am-et
  时区: America/New_York (zoneinfo 自动处理 EDT/EST 夏令时, 而非固定 UTC-4)
```

| 函数 | 作用 |
|------|------|
| `current_slug(coin, period)` | 当前轮次的 slug |
| `round_start(period)` / `round_end(period)` | 当前轮次起止时间(unix 秒),驱动轮次切换 |

### 3. `market_monitor.py` — 单市场监控器(核心)

**作用**:每个币种×周期一个实例,独立 asyncio 任务,负责从"定位市场"到"触发下单"的全过程。

**轮次生命周期**:
```
run()
 └─ _monitor_round()
     ├─ 构建当前轮 slug ──► get_market() (Gamma API, 失败重试 3 次)
     ├─ 检查 accepting_orders, 读取 tick_size / min_order_size
     ├─ 取出 Up / Down 两个 token (探测确认: yes=Up, no=Down)
     ├─ 清空上轮去重, 计算冷静期截止时间
     └─ 订阅 2 个 token (MarketSpec + custom_feature_enabled)
         └─ 消费事件直到轮次结束
             ├─ 断流超时 → 指数退避重连
             └─ 轮次到期 → 回到 _monitor_round 获取下一轮
```

**事件处理**(`_handle_event`):

| 事件 | 处理 |
|------|------|
| `book` | 完整订单簿,`bids[0].price` 建立初始 best_bid;更新 tick_size |
| `price_change` | 按 `token_id` **过滤**后更新 best_bid/best_ask(⚠️ 实测一个订阅会同时收到 Up/Down 两方向推送,必须过滤) |
| `best_bid_ask` | 更新 best_bid/best_ask |
| `tick_size_change` | 动态更新 tick_size(下单价格必须符合当前 tick) |

**触发决策**(`_check_triggers`,四个条件 **AND**):
```
best_bid >= MIN_BID        (0.99)
&& 当前时间 >= 冷静期截止
&& 该 token 本轮未下单      (去重)
&& 该 token 未在下单/未冷却 (防重复、失败重试节奏)
```
满足则写触发日志 `triggers.log`,并 `create_task` 异步下单。

### 4. `order_manager.py` — 下单管理

**作用**:所有市场共享一个 OrderManager,集中处理下单的串行化、去重、价格合规。

| 职责 | 实现 |
|------|------|
| **串行化** | 全局 `asyncio.Lock`,28 个市场可能同时触发,但下单请求排队执行,避免并发撞 CLOB 限流 |
| **去重** | 按市场隔离的 `ordered` 集合(`market_tag`),每轮每 token 最多下一单;`clear_round(tag)` 只清本市场记录,不影响其他市场(防止 5m 切轮误清 4h) |
| **tick 对齐** | `align_price()` 把价格向下对齐到 `minimum_tick_size` 倍数,再 clamp 到 0.99(CLOB 限价单上限)。⚠️ 1h 市场 tick=0.001(需 "0.990"),其余 tick=0.01(需 "0.99"),必须动态读取 |
| **固定份数** | size 固定为 `config.ORDER_SIZE`(默认 5 份),价格固定 clamp 到 0.99 并对齐 tick。⚠️ notional = 0.99×5 = 4.95,略低于 min_order_size=5,如被拒需调大份数 |
| **幂等防重** | 下单前查 `list_open_orders(token_id=...)`,该 token 已有 BUY 挂单则跳过。防网络超时"实际已下单但响应丢失"后重试导致的重复下单 |
| **失败冷却** | 下单失败/被拒进入 60s 冷却(`can_retry`),冷却后可重试;成功才标记去重 |
| **授权** | `ensure_approvals()` 首次调用 `setup_trading_approvals()` 授权交易所花 pUSD 和 token(一次,幂等) |
| **dry-run** | `dry_run=True` 时只模拟下单打日志,不真实发送(测试/验证用) |

下单参数:方向固定 `BUY`,类型 GTC 限价单(省略 expiration)。

### 5. `main.py` — 入口

**作用**:组织整个进程的生命周期。

```
main() → 解析参数 (instance, --dry-run)
 └─ run()
     ├─ setup_logging(instance)     控制台 INFO + 文件 DEBUG, RotatingFileHandler 200MB×5
     │                               triggers.log 每天轮转 (TimedRotatingFileHandler)
     ├─ load_credentials(instance)  加载 .env.<instance>, 校验 4 个必填字段
     ├─ _create_secure_client()     AsyncSecureClient.create, 网络瞬时错误自动重试 5 次
     ├─ AsyncPublicClient()         公开数据客户端 (28 个市场共用)
     ├─ OrderManager(secure)        下单管理 (可传 --dry-run)
     ├─ 28 个 MarketMonitor 任务    asyncio.create_task 并行启动
     ├─ 信号处理                    SIGTERM/SIGINT → 优雅关闭所有任务
     └─ gather 等待 → finally 兜底 cancel
```

**多账户**:每实例独立 `.env.<instance>`、`data/<instance>/` 日志目录、screen 会话,互不干扰。

---

## 四、核心流程时序

**一轮完整的监控**:
```
[轮次开始]
    │  slug → 市场 → token (Up=yes, Down=no)
    ▼
[订阅建立]  book 事件到达, 建立初始 best_bid
    │
    │  ................ 冷静期内, 只收数据不交易 ................
    │  price_change 事件 → 按 token_id 更新 best_bid
    ▼
[冷静期结束]
    │  任一 token best_bid >= 0.99 ?
    │  ├─ 是 → 未下单? 冷却过? → OrderManager.place() 串行下单
    │  │        └─ 成功 → 记录去重 / 失败 → 60s 冷却
    │  └─ 否 → 继续监控
    ▼
[轮次到期]  停止订阅 → 回到 [轮次开始] 获取下一轮市场
```

**容错设计**:

| 故障场景 | 应对 |
|----------|------|
| 行情断流 | `NO_DATA_TIMEOUT` 差异化超时 → 指数退避重连(1s→...→30s) |
| Gamma 获取失败 | 重试 3 次(2s/4s 间隔) |
| 下单失败/被拒 | 打印错误 + 60s 冷却,之后允许重试 |
| 单市场故障 | 隔离:仅该任务重连/重试,不影响其他 27 个 |
| TLS 瞬时错误 | `_create_secure_client` 自动重试 5 次 |
| 整个进程 | SIGTERM 优雅退出,撤干净任务 |

---

## 五、使用方式

```powershell
# 本地验证 (模拟下单, 不真实发送 —— 推荐先用这个!)
python main.py bot1 --dry-run

# 真实运行
python main.py bot1

# 指定其他实例 (需先有 .env.bot2 和对应凭据)
python main.py bot2
```

**`.env.<instance>` 必填字段**(模板 2.2 节,已在 `.gitignore` 忽略):
```
SIGNER_PRIVATE_KEY=0x...
POLYMARKET_WALLET_ADDRESS=0x...
POLYMARKET_RELAYER_API_KEY=...
POLYMARKET_RELAYER_API_KEY_ADDRESS=...
```

**日志**:
```powershell
tail -f data/bot1/app.log                # 实时 (服务器)
grep -E "ERROR|WARNING" data/bot1/app.log
cat data/bot1/triggers.log               # 每次触发下单的信号记录
```

---

## 六、部署参考(摘要)

完整流程见 `新项目开发流程模板.md`。要点:

1. 服务器 `git clone` 到独立目录,建 venv,`pip install -r requirements.txt`
2. `scp` 传 `.env.bot1`,`chmod 600`
3. 前台 `python main.py bot1` 验证启动日志正常
4. `screen -S bot1` 后台托管,或 `./start_bot.sh bot1` / `./stop_bot.sh bot1`
5. cron 定时停机/重启(注意服务器 UTC 时区换算)

---

## 七、关键经验与注意事项

1. **tick_size 必须动态读**:1h 市场是 `0.001`,其余 `0.01`。写死价格格式会导致下单被拒。已在 `_monitor_round` 读取并用于 `align_price`。
2. **订阅会收到同市场两个方向的推送**:`price_change` 里必须按 `token_id` 过滤,否则会把 Down 的 best_bid 误判为 Up。
3. **`minimum_order_size = 5`**:CLOB 要求每单 notional ≥ 5 pUSD。当前配置 5 份 × 0.99 = 4.95 略低于该值,若下单被拒请调大 `ORDER_SIZE`(如 6 份)。
4. **outcomes 映射**:实测 `market.outcomes.yes = Up`、`outcomes.no = Down`(按 Gamma API 原始数组顺序归一化)。
5. **冷静期是防噪音的关键**:轮次前期价格不稳定,`5m` 冷静期 210s(前 70% 时间),`4h` 冷静期 12600s(前 87.5%),只做后半段。
6. **验证务必用 `--dry-run`**:本仓库在真实验证时曾因触发条件满足产生真实挂单,已加 dry-run 模式规避。

---

> 基于 `ARCHITECTURE.md` 设计 + 2026-08-08 真实数据探测实现。官方文档:[docs.polymarket.com](https://docs.polymarket.com)
