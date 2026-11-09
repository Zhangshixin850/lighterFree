# Lighter Robinhood 中性网格 v5.0

运行于 Ubuntu / Python 3.10+，仅支持 Robinhood Chain 永续市场，不依赖 CCXT。交易使用批量接口，订单、行情及账户状态通过 WebSocket 更新，并用低频 REST 核对。

## v5.0：边缘成交时平移半边网格

“一半”指 `gridLines / 2`。边缘订单完全成交后，以该订单原限价为新中心，保留重叠范围内的策略订单，先分批撤销远端订单并确认，再向成交方向补齐网格。边缘成交仍按普通规则反向补一个步长，买卖两侧对称处理。

例如中心60、6层、步长1，原挂买57/58/59、卖61/62/63。逐层下跌成交并完成普通补单后，买侧边缘57成交：

- 保留已有卖59、60，订单编号与剩余数量不变。
- 撤销远端卖61、62、63。
- 新增买54、55、56，边缘57反向补卖58。
- 最终为买54/55/56、卖58/59/60，不全撤重建。

推送合并、在途订单或并发成交可能改变实际缺口数量；程序按目标差集去重补齐。平移期间新成交继续处理，已成交的目标单不会被原方向重复补回；新边缘事件在当前平移结束后处理。主动移出网格的订单不重挂，撤单竞争中的真实完整成交仍统计。

这会减少正常推进时的重复下单，**不保证减少 HTTP 请求次数**：原全撤是一笔交易，定向撤单需要分批确认。WebSocket 和周期 REST 频率不变。

## Ubuntu 安装与检查

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

首次部署将 `.env.example` 复制为 `.env`，填写账户/API key 配置和策略参数；保留已有 `.env`。系统环境变量优先于脚本目录的 `.env`。

```bash
# 离线回归，不读取真实账户或发送交易
python -m unittest -v test_light_grid
# 离线检查参数，不验证平台密钥有效性
python Light_grid.py --check-config
```

以下命令会直接开始真实交易：

```bash
python Light_grid.py
```

## 配置与风控

- 账户配置：`LIGHTER_NETWORK`、`LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX`、`LIGHTER_API_PRIVATE_KEY`。网络仅接受 `robinhood` / `robinhood_testnet`。
- 策略配置：`SYMBOL`、`gridStep`、正偶数 `gridLines`、`orderSize`、`lowLimit`、`highLimit`、`baseline`、`left`、`maxposi`；目标仓位仅展示，不自动调仓。
- `patch` 控制批量大小，实际还受平台容量和滚动额度限制。`CHECK_INTERVAL` 是本地处理频率；`REST_INTERVAL` 是周期核对间隔。
- 保留账户级 `MMR`、`leverage` 及交易对价格、浮亏、绝对仓位和挂单数风控。结果不明先核对，不直接重发。
- 平移只撤本策略订单；启动清理和按配置退出撤单会清理当前交易对全部挂单，包括手动单。均不自动平仓。
- 真实 `.env`、私钥和日志不提交 Git。避免同一账户、同一交易对启动多个网格进程。

详细配置、限速、异常恢复和统计口径见 [USAGE.md](USAGE.md)，贡献约定见 [AGENTS.md](AGENTS.md)。

## 版本记录

- **v5.0**：边缘成交改为半边平移；保留重叠订单，补充定向撤单确认与竞争成交回归。
- **此前版本**：Robinhood 专用适配、批量提交、WebSocket、账户风控、可配置批量及成交统计。

