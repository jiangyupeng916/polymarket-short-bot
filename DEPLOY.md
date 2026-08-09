# 服务器部署与运维手册

> 适用版本：Python 3.12+ · Linux (Ubuntu 22.04+ / Debian 12+ / CentOS 9+)

---

## 一、环境要求

| 项目 | 要求 | 检查命令 |
|------|------|----------|
| 操作系统 | Linux x86_64 | `uname -m` |
| Python | 3.12 或更高 | `python3 --version` |
| 网络 | 能访问 api.polymarket.com (443) 和 wss 端点 | `curl -I https://clob.polymarket.com` |
| 磁盘 | ≥ 2GB 空闲（日志 200MB × 5 轮转 × N 实例） | `df -h` |

---

## 二、首次部署

### 2.1 克隆仓库

```bash
cd ~
git clone https://github.com/jiangyupeng916/polymarket-short-bot.git
cd polymarket-short-bot
```

### 2.2 创建虚拟环境并安装依赖

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

验证安装成功：

```bash
python -c "from polymarket import AsyncPublicClient, AsyncSecureClient; print('OK')"
# 输出: OK
```

### 2.3 传输凭据文件

在**本地 Windows 机器**上执行：

```powershell
scp D:\AAA\polymarket-short-monitor\.env.bot1 user@<服务器IP>:~/polymarket-short-bot/
```

回到**服务器**，设置安全权限：

```bash
chmod 600 ~/polymarket-short-bot/.env.bot1
```

检查凭据文件内容（确认 4 个字段完整）：

```bash
cat ~/polymarket-short-bot/.env.bot1
# 应包含:
#   SIGNER_PRIVATE_KEY=0x...
#   POLYMARKET_WALLET_ADDRESS=0x...
#   POLYMARKET_RELAYER_API_KEY=...
#   POLYMARKET_RELAYER_API_KEY_ADDRESS=...
```

### 2.4 调整配置（部署前必读）

编辑 `config.py`，确认以下参数符合你的策略：

```bash
vim ~/polymarket-short-bot/config.py
```

| 参数 | 当前值 | 说明 |
|------|--------|------|
| `COINS` | `["btc", "eth"]` | **部署前确认**：生产环境需改回 7 币种或确认只要这两个 |
| `MIN_BID` | `0.98` | 触发阈值：买一价 ≥ 0.98 才开始决策 |
| `ORDER_PRICE` | `0.99` | 固定挂单价：触发后始终挂 0.99 限价买单 |
| `ORDER_SIZE` | `5.0` | 每单份数，notional = 0.99 × 5 = 4.95 |

> ⚠️ **notional 警告**：`ORDER_PRICE × ORDER_SIZE = 4.95`，略低于 CLOB 的 `minimum_order_size = 5`。如果日志出现 `minimum_order_size` 相关拒绝，把 `ORDER_SIZE` 调到 6。

---

## 三、验证运行（Dry-Run）

**正式下单前必须先用 dry-run 模式验证。**

```bash
cd ~/polymarket-short-bot
source venv/bin/activate
python main.py bot1 --dry-run
```

### 正常启动日志示例

```
INFO [main] ==== 启动 Polymarket 短线监控 bot instance=bot1 ====
INFO [main] 监控: 币种=['btc', 'eth'] 周期=['5m', '15m', '1h', '4h']
INFO [main] 阈值 MIN_BID=0.98 每单份数=5.0
INFO [main] 下单模式: DRY-RUN (模拟下单, 不真实发送)
INFO [main] 账户连接成功 wallet=0x... type=...
INFO [main] 启动 8 个市场监控任务
INFO [monitor.btc.5m] 进入轮次, slug=btc-updown-5m-...
INFO [monitor.btc.5m] 已订阅 token 数=2, 超时=5s
...
```

### 需要关注的信号

| 日志关键词 | 含义 | 行动 |
|-----------|------|------|
| `已订阅 token 数=2` | WebSocket 连接成功 | ✅ 正常 |
| `[DRY-RUN] 模拟下单` | 触发条件满足，模拟下单 | ✅ 策略参数有效 |
| `ERROR` | 异常（连接失败、凭据错误等） | 🔴 排查 |
| `WARNING` | 可恢复错误（断流重连等） | 🟡 关注频率 |
| `无行情数据超过` | WebSocket 断流触发超时 | 🟡 检查网络 |

### 退出

按 `Ctrl+C`，观察是否所有任务优雅退出：

```
INFO [main] 收到退出信号, 优雅关闭 8 个任务...
INFO [main] ==== 已全部退出 ====
```

---

## 四、正式运行

Dry-run 验证通过后，去掉 `--dry-run` 正式启动。

### 方式一：screen（推荐）

```bash
# 启动
screen -S pm-bot1
cd ~/polymarket-short-bot && source venv/bin/activate
python main.py bot1

# 分离会话：Ctrl+A 然后按 D

# 重新连接
screen -r pm-bot1

# 列出所有 screen
screen -ls
```

### 方式二：tmux

```bash
# 启动
tmux new -s bot1
cd ~/polymarket-short-bot && source venv/bin/activate
python main.py bot1

# 分离：Ctrl+B 然后按 D

# 重新连接
tmux attach -t bot1
```

### 方式三：nohup（无交互需求时）

```bash
cd ~/polymarket-short-bot
source venv/bin/activate
nohup python main.py bot1 > /dev/null 2>&1 &
echo $! > bot1.pid    # 记录进程 ID，方便后续 kill
```

### 方式四：systemd（生产推荐，开机自启）

创建服务文件：

```bash
sudo vim /etc/systemd/system/polymarket-bot1.service
```

内容：

```ini
[Unit]
Description=Polymarket Short Bot (bot1)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/home/your-user/polymarket-short-bot
Environment=PATH=/home/your-user/polymarket-short-bot/venv/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/home/your-user/polymarket-short-bot/venv/bin/python main.py bot1
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable polymarket-bot1
sudo systemctl start polymarket-bot1
sudo systemctl status polymarket-bot1
```

---

## 五、日志管理

### 5.1 日志文件位置

```
data/bot1/
├── app.log          # 全量 DEBUG 日志（200MB × 5 轮转）
├── app.log.1        # 历史轮转文件
├── triggers.log     # 触发信号记录（每天轮转，保留 30 天）
└── triggers.log.YYYY-MM-DD
```

### 5.2 实时查看

```bash
# 实时全量日志
tail -f ~/polymarket-short-bot/data/bot1/app.log

# 只看触发记录
tail -f ~/polymarket-short-bot/data/bot1/triggers.log

# systemd 方式
journalctl -u polymarket-bot1 -f
```

### 5.3 错误排查

```bash
# 查看所有错误和警告
grep -E "ERROR|WARNING" ~/polymarket-short-bot/data/bot1/app.log

# 查看最近 50 条错误
grep "ERROR" ~/polymarket-short-bot/data/bot1/app.log | tail -50

# 查看特定市场的日志
grep "monitor.btc.5m" ~/polymarket-short-bot/data/bot1/app.log | tail -20
```

### 5.4 统计触发次数

```bash
# 今日触发次数
grep "TRIGGER" ~/polymarket-short-bot/data/bot1/triggers.log | wc -l

# 历史总触发（所有轮转文件）
cat ~/polymarket-short-bot/data/bot1/triggers.log* | grep "TRIGGER" | wc -l

# 按市场统计
grep "TRIGGER" ~/polymarket-short-bot/data/bot1/triggers.log | awk '{print $3}' | sort | uniq -c | sort -rn
```

### 5.5 磁盘空间

```bash
# 查看日志目录占用
du -sh ~/polymarket-short-bot/data/

# 清理 30 天前的触发日志
find ~/polymarket-short-bot/data/ -name "triggers.log.*" -mtime +30 -delete
```

---

## 六、日常运维

### 6.1 查看运行状态

```bash
# screen 方式
screen -ls                            # 是否在运行
screen -S pm-bot1 -X hardcopy /tmp/bot1_snapshot.txt  # 截取当前画面

# systemd 方式
sudo systemctl status polymarket-bot1

# 通用：检查进程
ps aux | grep "main.py"
```

### 6.2 停止

```bash
# screen 方式：连接进去按 Ctrl+C
screen -r pm-bot1
# 按 Ctrl+C，等待优雅退出

# systemd 方式
sudo systemctl stop polymarket-bot1

# nohup 方式
kill $(cat ~/polymarket-short-bot/bot1.pid)
# 或
pkill -f "main.py bot1"
```

### 6.3 重启

```bash
# systemd 方式
sudo systemctl restart polymarket-bot1

# screen 方式：先停再启
screen -r pm-bot1    → Ctrl+C → python main.py bot1
```

### 6.4 更新代码

```bash
cd ~/polymarket-short-bot

# 1. 先停掉 bot
sudo systemctl stop polymarket-bot1   # 或用 screen/Ctrl+C

# 2. 拉取最新代码
git pull origin main

# 3. 检查依赖是否有变化
source venv/bin/activate
pip install -r requirements.txt

# 4. 重新启动
sudo systemctl start polymarket-bot1
```

---

## 七、多实例运行

如果需要多个账户同时跑（如 bot1、bot2），每个实例需要独立的凭据文件和日志目录。

### 7.1 添加新实例

```bash
# 1. 传输第二个账户的凭据
# 在本地执行：
scp .env.bot2 user@<IP>:~/polymarket-short-bot/
# 在服务器执行：
chmod 600 ~/polymarket-short-bot/.env.bot2

# 2. 启动第二个实例
screen -S pm-bot2
cd ~/polymarket-short-bot && source venv/bin/activate
python main.py bot2
```

### 7.2 systemd 多实例模板

```bash
# 复制服务文件
sudo cp /etc/systemd/system/polymarket-bot1.service /etc/systemd/system/polymarket-bot2.service
# 编辑 bot2.service，把 bot1 改为 bot2
sudo vim /etc/systemd/system/polymarket-bot2.service
sudo systemctl daemon-reload
sudo systemctl enable --now polymarket-bot2
```

---

## 八、常见问题排查

### 8.1 启动失败

| 现象 | 可能原因 | 解决 |
|------|----------|------|
| `凭据文件不存在` | `.env.bot1` 未传输或路径错误 | 检查 `ls -la .env.bot1` |
| `凭据文件缺少字段` | `.env` 内容不完整 | 对照模板补全 4 个字段 |
| `SecureClient 创建多次失败` | 网络不通或凭据无效 | `curl https://clob.polymarket.com`，检查私钥 |
| `ModuleNotFoundError: No module named 'polymarket'` | 未激活 venv 或未安装依赖 | `source venv/bin/activate && pip install -r requirements.txt` |

### 8.2 运行时异常

| 现象 | 可能原因 | 解决 |
|------|----------|------|
| `无行情数据超过 5s` 频繁出现 | 网络不稳定 | 检查服务器到 Polymarket 的延迟；考虑增大 `NO_DATA_TIMEOUT` |
| `获取市场失败` 反复出现 | Gamma API 限流或网络问题 | 检查是否有其他程序也在高频调用；稍等自动恢复 |
| `下单被拒 code=... minimum_order_size` | notional 低于 5 | 调大 `config.py` 的 `ORDER_SIZE` 到 6 |
| `下单被拒 code=... invalid_price` | 价格未对齐 tick_size | 检查 tick_size 是否正确读取（日志中有打印） |

### 8.3 日志中没有触发记录

这不是 bug — 说明没有到达触发条件。检查：

```bash
# 看心跳日志确认是否在正常收数据
grep "心跳" ~/polymarket-short-bot/data/bot1/app.log | tail -5

# 看当前 best_bid 水平
grep "best_bid" ~/polymarket-short-bot/data/bot1/app.log | tail -10
```

如果 best_bid 一直在 0.98 以下 → 正常，市场未到触发水位。

---

## 九、安全注意事项

1. **`.env.*` 文件权限必须是 600**：含私钥，绝不能 `chmod 644` 或更宽松
2. **不要用 root 运行**：用普通用户运行 bot
3. **防火墙**：bot 只需要**出站** 443 端口，不需要开放入站端口
4. **定期检查余额**：确保钱包有足够 pUSD 用于下单
5. **Git 仓库公开**：`.env.*` 和 `data/` 已在 `.gitignore` 中，确认不会误提交

---

## 十、速查命令

```bash
# === 路径 ===
cd ~/polymarket-short-bot && source venv/bin/activate

# === 运行 ===
python main.py bot1 --dry-run    # 验证模式
python main.py bot1              # 正式模式

# === screen ===
screen -S pm-bot1                   # 创建会话
screen -r pm-bot1                   # 重新连接
Ctrl+A D                         # 分离
screen -ls                       # 列出会话

# === 日志 ===
tail -f data/bot1/app.log        # 实时全量
tail -f data/bot1/triggers.log   # 实时触发
grep ERROR data/bot1/app.log     # 错误排查

# === systemd ===
sudo systemctl status polymarket-bot1
sudo systemctl restart polymarket-bot1
sudo systemctl stop polymarket-bot1
journalctl -u polymarket-bot1 -f

# === 进程 ===
ps aux | grep main.py
pkill -f "main.py bot1"
```
