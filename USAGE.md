# Lighter RH 网格 v1.0

本目录是独立的 Robinhood Chain 永续网格，不依赖 CCXT。旧 README 保持原样，本文件说明新版行为。

## Ubuntu 运行

使用 Python 3.10+，在本目录建立独立虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest -v test_light_grid
# 以下命令会直接开始真实交易：
python Light_grid.py
```

`.env` 明确从本目录加载，已有系统环境变量优先。只保留一组策略参数。
首次部署时，将 `.env.example` 复制为 `.env` 并填写账户信息；真实 `.env` 不提交到 Git。
所需账户配置：`LIGHTER_NETWORK`、`LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX`、`LIGHTER_API_PRIVATE_KEY`。
Robinhood 主网网络名为 `robinhood`，测试网为 `robinhood_testnet`；其他网络拒绝启动。
API key 索引使用 4–156 或 158–254。`SYMBOL` 支持 `INTC` 或 `INTC/USDT:USDT`，实际请求使用匹配到的活跃永续市场 ID。

### 启动排查

先运行不连接交易所、不创建日志文件的配置检查：

```bash
python3 Light_grid.py --check-config
```

检查会列出缺失或仍为占位值的字段名，并验证策略参数；不会显示凭据，也不验证密钥是否在平台有效。
从 GitHub 克隆时，真实 `.env` 不会随仓库下载，需要在 `Light_grid.py` 同目录自行配置。
启动失败会显示阶段和缺失字段；正常启动的 `Light_grid.log` 另记代码位置，不记录局部变量或原始敏感响应。

## 交易与限速

- 通过 WebSocket 接收盘口、订单、账户统计、全部永续持仓和资产更新；启动、重连后同步，正常每 `REST_INTERVAL=30` 秒核对账户和挂单。
- `CHECK_INTERVAL=0.8` 是本地策略处理间隔，不代表 REST 请求频率。
- 下单使用 `sendTxBatch`；`BATCH_SIZE=15` 采用官方对 HTTP 批量提交建议的安全大小，避免 CDN 正文限制。清空当前市场使用一笔带市场过滤的 `CancelAllOrders`，也通过批量通道发送。
- 真实 REST 请求及 WebSocket 建连共享滚动 60 秒的 **60 次**请求额度，不再预留额度或设置固定发送间隔。SDK 内部重试关闭，nonce 查询也计数。窗口边界仅增加 1 毫秒，避免计时取整误差。
- Standard 默认交易类型上限为 **40 笔/60 秒**，批量内每笔子交易分别计数；不同交易类型独立计数。下单可用满 40 笔，不再限制为 35 笔，剩余额度不足一整批时自动缩小批次。例如可连续提交 15+15+10 笔下单，之后等待该类型额度释放；60 层网格仍可能跨越一分钟。
- `L2CancelOrder` 单笔撤单不受子交易类型限额约束，仍消耗共享请求额度；`CancelAllOrders` 按自身类型计数。WebSocket 控制消息使用官方 200 条/60 秒上限。
- 撤单等待时优先取得释放的请求额度，但不绕过官方上限。取消预留后，若 60 次通用额度已经耗尽，风控撤单也必须等待窗口释放。
- 遇到限速按服务端提示冷却，无法确认提交结果时不盲目重发。不得把 `code=200` 当作订单已实际生效。
- 默认同一账户/API key 只有本进程写入；同一 L1 地址或 IP 上的其他程序需要共同分配额度，单进程限速无法限制外部流量。

## 策略与风控

上下各挂一半限价单，买单完整成交后在原价加一个 `gridStep` 补卖单，卖单反之。多笔按上一参考价距离排序后合批。边界成交优先撤单重建；重建中心固定为边界参考限价。目标仓位仅展示，不自动调仓。

外部 `canceled*` 订单按原方向、原价、完整 `orderSize` 重挂，包括部分成交后撤单；此规则由用户明确选择，会保留部分成交带来的仓位变化。主动重建/退出撤单不重挂。状态未知不推断成交，非活跃订单查询支持翻页。

账户风控覆盖配置的 `LIGHTER_ACCOUNT_INDEX` 下全部永续持仓，不包括同钱包其他子账户：

- `MMR=0.2`：全部永续持仓维持保证金需求 / 账户净值，达到 20% 触发。需求按平台市场 `maintenance_margin_fraction`（万分比）及持仓名义价值计算。
- `leverage=5`：持仓名义价值绝对值之和 / 账户净值，达到 5 倍触发。它是风控阈值，不会修改交易所设置的开仓杠杆。
- 净值使用 `user_stats` 的 `portfolio_value`；Unified 账户要求完整 `total_stats` 汇总，包含全仓和逐仓，未实现盈亏不再重复加入。关键字段缺失或净值非正时停止。
- 保留当前交易对价格范围、浮亏、绝对仓位和挂单数量上限检查；取消挂单数量下限检查。
- 断线立即暂停新单；超过 `DISCONNECT_TIMEOUT=30` 秒不能恢复完整状态则退出。所有风控/异常退出按 `CANCEL_ON_RISK` 撤当前交易对全部挂单，包含手动单。Ctrl+C/SIGTERM 按 `CANCEL_ON_EXIT` 撤单。均不平仓。
- 重启不恢复旧订单归属，先清理该交易对旧挂单再建立新网格。请避免同账户同交易对启动多个网格进程。

控制台每 5 秒显示简洁状态，不展示订单 ID；详细交易标识保存在本目录 `Light_grid.log`，密钥和 token 不记录。

## 官方依据

- [Robinhood Chain](https://docs.lighter.xyz/developers/robinhood-chain/overview)
- [限速](https://docs.lighter.xyz/developers/reference/rate-limits)
- [WebSocket](https://docs.lighter.xyz/api/websocket)
- [签名和成交确认](https://docs.lighter.xyz/developers/integration-guides/signing-transactions)
- [整型精度及 nonce](https://docs.lighter.xyz/developers/getting-started/core-concepts)

离线测试使用模拟响应，不读取真实账户或提交交易；仍需在目标 Ubuntu 环境安装依赖并确认账户返回字段与本程序口径一致。
