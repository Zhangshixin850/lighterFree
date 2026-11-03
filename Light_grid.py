"""Lighter Robinhood 免费账户中性网格。运行入口会直接交易。"""

import argparse
import asyncio
import logging
import math
import os
import signal
import sys
import time
import traceback
from collections import deque
from decimal import Decimal
from pathlib import Path

from lighter_exchange import (
    API_BATCH_LIMIT,
    BATCH_SIZE,
    REQUEST_LIMIT,
    TX_TYPE_LIMIT,
    ExchangeError,
    FatalError,
    LighterExchange,
    number,
)

logger = logging.getLogger("light_grid")
REQUIRED_CONFIG = (
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_KEY_INDEX",
    "LIGHTER_API_PRIVATE_KEY",
    "SYMBOL",
    "gridStep",
    "gridLines",
    "orderSize",
    "lowLimit",
    "highLimit",
    "baseline",
    "left",
    "maxposi",
)


class GridTrader:
    def __init__(self, exchange, config):
        self.exchange = exchange
        self.gridStep = number(config["gridStep"])
        self.gridLines = int(config["gridLines"])
        self.orderSize = number(config["orderSize"])
        self.lowLimit = number(config["lowLimit"])
        self.highLimit = number(config["highLimit"])
        self.baseline = number(config["baseline"])
        self.left = number(config["left"])
        self.maxposi = number(config["maxposi"])
        self.max_loss = number(config.get("MAX_LOSS", "-8000"))
        self.MMR = number(config.get("MMR", "0.2"))
        self.leverage = number(config.get("leverage", "5"))
        self.check_interval = float(config.get("CHECK_INTERVAL", "0.8"))
        self.reconcile_interval = float(config.get("REST_INTERVAL", "30"))
        self.disconnect_timeout = float(config.get("DISCONNECT_TIMEOUT", "30"))
        try:
            self.batch_size = int(
                config.get("patch", config.get("BATCH_SIZE", str(BATCH_SIZE)))
            )
        except (TypeError, ValueError):
            raise ValueError("patch 必须是正整数") from None
        self.cancel_on_exit = config.get("CANCEL_ON_EXIT", "true").lower() == "true"
        self.cancel_on_risk = config.get("CANCEL_ON_RISK", "true").lower() == "true"
        self.critical_error_limit = int(config.get("CRITICAL_ERROR_LIMIT", "3"))
        errors = []
        for key, value in (
            ("gridStep", self.gridStep),
            ("orderSize", self.orderSize),
            ("maxposi", self.maxposi),
            ("leverage", self.leverage),
        ):
            if value <= 0:
                errors.append(f"{key}={value} 必须大于 0")
        checks = (
            (
                "gridLines",
                self.gridLines,
                0 < self.gridLines <= 1000 and self.gridLines % 2 == 0,
                "必须是 1–1000 范围内的正偶数",
            ),
            ("MMR", self.MMR, 0 < self.MMR <= 1, "必须满足 0 < MMR <= 1"),
            (
                "lowLimit/highLimit",
                f"{self.lowLimit}/{self.highLimit}",
                0 < self.lowLimit < self.highLimit,
                "必须满足 0 < lowLimit < highLimit",
            ),
            (
                "CHECK_INTERVAL",
                self.check_interval,
                0.05 <= self.check_interval <= 5,
                "必须在 0.05–5 秒之间",
            ),
            (
                "REST_INTERVAL",
                self.reconcile_interval,
                math.isfinite(self.reconcile_interval)
                and self.reconcile_interval >= 10,
                "必须是至少 10 秒的有限数值",
            ),
            (
                "DISCONNECT_TIMEOUT",
                self.disconnect_timeout,
                10 <= self.disconnect_timeout <= 120,
                "必须在 10–120 秒之间",
            ),
            (
                "patch",
                self.batch_size,
                self.batch_size > 0,
                "必须是正整数",
            ),
            (
                "CRITICAL_ERROR_LIMIT",
                self.critical_error_limit,
                self.critical_error_limit > 0,
                "必须大于 0",
            ),
        )
        errors.extend(
            f"{key}={value} {rule}" for key, value, valid, rule in checks if not valid
        )
        if errors:
            raise ValueError("参数校验失败：" + "；".join(errors))
        self.exchange.batch_size = self.batch_size
        # 核对间隔与过期阈值配套，保留一轮读取或重试的时间。
        self.exchange.reconcile_timeout = max(60, 2 * self.reconcile_interval)
        self.All_orders = {}
        self.last_fill_price = Decimal(0)
        self.myNewPrice = Decimal(0)
        self.switc = True
        self.counter = 0
        self.queue = deque()
        self.operation = None
        self.operation_kind = ""
        self.batch = []
        self.sync_task = None
        self.stop_event = asyncio.Event()
        self.phase = "同步"
        self.critical_errors = 0
        self._unhealthy_since = time.monotonic()
        self._last_display = 0.0
        self._next_sync = 0.0
        self._sync_retry = 2
        self._stopping = False
        self.exchange.before_send = self.risk_reason

    def validate_market(self):
        tick = Decimal(1).scaleb(-int(self.exchange.market["supported_price_decimals"]))
        if self.gridStep % tick:
            raise ValueError("gridStep 必须是平台价格步长的整数倍")
        self.exchange.prepare_order("buy", self.orderSize, self.lowLimit)
        self.exchange.prepare_order("sell", self.orderSize, self.highLimit)

    def risk_reason(self):
        metrics = self.exchange.metrics()
        price = self.exchange.price
        if metrics["mmr"] >= self.MMR:
            return f"账户保证金率 {metrics['mmr']:.2%} 达到上限 {self.MMR:.2%}"
        if metrics["leverage"] >= self.leverage:
            return f"账户有效杠杆 {metrics['leverage']:.2f} 达到上限 {self.leverage}"
        if price and not self.lowLimit <= price <= self.highLimit:
            return "行情价格超出运行区间"
        if metrics["pnl"] < self.max_loss:
            return "当前交易对未实现亏损超过上限"
        if abs(metrics["size"]) > self.maxposi:
            return "当前交易对绝对仓位超过上限"
        active = sum(
            o["status"] in {"open", "pending", "in-progress"}
            for o in self.exchange.orders.values()
        )
        if active > self.gridLines + 4:
            return "当前交易对挂单数量超过上限"
        return ""

    def replacements(self):
        last_price = self.last_fill_price or self.myNewPrice
        queued = {item["origin"] for item in self.queue if item["origin"]}
        queued.update(item["origin"] for item in self.batch if item["origin"])
        eligible = []
        for oid in self.All_orders:
            if oid in queued:
                continue
            status = self.exchange.orders.get(oid, {}).get("status", "")
            if status == "filled" or status.startswith("canceled"):
                eligible.append(oid)
        eligible.sort(
            key=lambda oid: (
                abs(self.All_orders[oid][1] - last_price),
                self.All_orders[oid][1],
                oid,
            )
        )
        edges = [
            oid
            for oid in eligible
            if self.All_orders[oid][0]
            and self.exchange.orders[oid]["status"] == "filled"
        ]
        if edges:
            self.last_fill_price = self.All_orders[edges[-1]][1]
            self.myNewPrice = self.last_fill_price
            self.switc = True
            self.queue.clear()
            logger.info("边界成交 | 重建中心 %s", self.myNewPrice)
            return
        for oid in eligible:
            edge, old_price, old_side = self.All_orders[oid]
            status = self.exchange.orders[oid]["status"]
            canceled = status.startswith("canceled")
            side = old_side if canceled else ("sell" if old_side == "buy" else "buy")
            price = (
                old_price
                if canceled
                else old_price
                + (self.gridStep if old_side == "buy" else -self.gridStep)
            )
            order = self.exchange.prepare_order(side, self.orderSize, price)
            self.queue.append(
                {
                    "order": order,
                    "origin": oid,
                    "edge": edge if canceled else 0,
                    "old_price": old_price,
                    "canceled": canceled,
                }
            )
            if canceled and self.exchange.orders[oid]["filled"]:
                logger.warning(
                    "部分成交后撤单 | 按配置完整数量重挂 %s %s @ %s",
                    side,
                    order["amount"],
                    order["price"],
                )

    def build_grid(self):
        self.All_orders.clear()
        self.queue.clear()
        self.myNewPrice = self.myNewPrice or self.exchange.price
        half = self.gridLines // 2
        for i in range(1, half + 1):
            for side, price in (
                ("buy", self.myNewPrice - i * self.gridStep),
                ("sell", self.myNewPrice + i * self.gridStep),
            ):
                order = self.exchange.prepare_order(side, self.orderSize, price)
                self.queue.append(
                    {
                        "order": order,
                        "origin": None,
                        "edge": int(i == half),
                        "old_price": Decimal(0),
                        "canceled": False,
                    }
                )
        prices = [item["order"]["price"] for item in self.queue]
        if len(set(prices)) != self.gridLines:
            raise FatalError("精度转换后网格价格重叠")
        self.switc = False
        self.phase = "铺单"
        logger.info(
            "铺设网格 | 中心 %s | %s 层 | 间距 %s",
            self.myNewPrice,
            self.gridLines,
            self.gridStep,
        )

    def finish_operation(self):
        self.operation.result()
        self.operation = None
        if self.operation_kind == "cancel":
            # 确认撤单后清除旧网格归属，主动撤单不进入自动重挂。
            self.build_grid()
            return
        filled = canceled = 0
        for item in self.batch:
            order, origin = item["order"], item["origin"]
            self.All_orders[order["id"]] = [item["edge"], order["price"], order["side"]]
            if origin:
                self.All_orders.pop(origin, None)
                self.exchange.orders.pop(origin, None)
                self.exchange.order_versions.pop(origin, None)
                if item["canceled"]:
                    canceled += 1
                else:
                    self.last_fill_price = item["old_price"]
                    self.counter += 1
                    filled += 1
        logger.info(
            "批量确认 | %s 笔 | 成交补单 %s | 撤单重挂 %s",
            len(self.batch),
            filled,
            canceled,
        )
        self.batch = []
        self.phase = "铺单" if self.queue else "运行"

    def display(self):
        if time.monotonic() - self._last_display < 5:
            return
        self._last_display = time.monotonic()
        protected = set(self.All_orders) | {item["order"]["id"] for item in self.batch}
        for oid, order in list(self.exchange.orders.items()):
            if oid not in protected and (
                order["status"] == "filled" or order["status"].startswith("canceled")
            ):
                self.exchange.orders.pop(oid, None)
                self.exchange.order_versions.pop(oid, None)
        try:
            metrics = self.exchange.metrics()
        except (KeyError, ValueError, FatalError):
            logger.info(
                "%s | %s | 成交 %s 笔 | 等待完整账户与行情数据",
                self.exchange.symbol,
                self.phase,
                self.exchange.filled_count,
            )
            return
        active = [o for o in self.exchange.orders.values() if o["status"] == "open"]
        buys = sum(o["side"] == "buy" for o in active)
        target_price = self.last_fill_price or self.myNewPrice or self.exchange.price
        target = (
            self.baseline - target_price
        ) * self.orderSize / self.gridStep + self.left
        self.exchange.budget.delay()  # 清理窗口，便于展示真实用量。
        requests = sum(item[1] for item in self.exchange.budget.requests)
        logger.info(
            "%s | %s | 价 %s | 仓 %s / 目标 %.5f | 目标差额 %+.5f（负值买、正值卖） | 浮盈 %.2f | 成交 %s 笔 | 买/卖 %s/%s | MMR %.1f%% | 杠杆 %.2fx | API %s/%s",
            self.exchange.symbol,
            self.phase,
            self.exchange.price,
            metrics["size"],
            target,
            metrics["size"] - target,
            metrics["pnl"],
            self.exchange.filled_count,
            buys,
            len(active) - buys,
            metrics["mmr"] * 100,
            metrics["leverage"],
            requests,
            REQUEST_LIMIT,
        )

    async def run(self):
        self.validate_market()
        reason = "用户停止"
        risk_exit = False
        try:
            while not self.stop_event.is_set():
                now = time.monotonic()
                if self.exchange.error:
                    raise self.exchange.error
                if self.sync_task and self.sync_task.done():
                    try:
                        self.sync_task.result()
                        self.critical_errors = 0
                        self._sync_retry = 2
                        self._next_sync = now + self.reconcile_interval
                    except FatalError:
                        raise
                    except Exception as exc:
                        self.critical_errors += 1
                        self._next_sync = now + self._sync_retry
                        self._sync_retry = min(self._sync_retry * 2, 30)
                        logger.warning(
                            "账户核对失败（%s）| 连续 %s 次",
                            type(exc).__name__,
                            self.critical_errors,
                        )
                        if self.critical_errors >= self.critical_error_limit:
                            raise FatalError("连续账户核对失败")
                    finally:
                        self.sync_task = None
                if (
                    self.exchange.connected
                    and not self.sync_task
                    and now >= self._next_sync
                ):
                    self.sync_task = asyncio.create_task(
                        self.exchange.reconcile(tuple(self.All_orders))
                    )
                if self.operation and self.operation.done():
                    self.finish_operation()
                ready = self.exchange.ready and now - self.exchange.last_price_at < 30
                self.exchange.trading_enabled = ready
                if ready:
                    self._unhealthy_since = now
                    risk = self.risk_reason()
                    if risk:
                        raise FatalError(risk)
                    if not self.switc:
                        self.replacements()
                    if not self.operation:
                        if self.switc:
                            self.phase = "撤单"
                            self.operation_kind = "cancel"
                            self.operation = asyncio.create_task(
                                self.exchange.cancel_all_orders()
                            )
                        elif self.queue:
                            available = self.exchange.budget.available(14)
                            count = min(
                                self.batch_size,
                                API_BATCH_LIMIT,
                                len(self.queue),
                                available or TX_TYPE_LIMIT,
                            )
                            self.batch = [self.queue.popleft() for _ in range(count)]
                            self.operation_kind = "create"
                            self.operation = asyncio.create_task(
                                self.exchange.create_orders(
                                    [i["order"] for i in self.batch]
                                )
                            )
                else:
                    self.phase = "同步"
                    if self.exchange.reconciled_generation != self.exchange.generation:
                        self._next_sync = min(self._next_sync, now + 1)
                    if now - self._unhealthy_since >= self.disconnect_timeout:
                        raise FatalError("连接或数据同步超过等待上限")
                self.display()
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=self.check_interval
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            reason = "用户停止"
        except Exception as exc:
            # 未知响应不输出原始正文，文件 DEBUG 保存必要交易标识。
            reason = (
                str(exc)
                if isinstance(exc, (FatalError, ValueError))
                else type(exc).__name__
            )
            risk_exit = True
        finally:
            self._stopping = True
            self.exchange.trading_enabled = False
            if self.sync_task:
                self.sync_task.cancel()
                await asyncio.gather(self.sync_task, return_exceptions=True)
            if self.operation:
                # 禁止后续发送；已提交批次先尝试完成确认再撤单。
                try:
                    await asyncio.wait_for(asyncio.shield(self.operation), timeout=15)
                except (Exception, asyncio.CancelledError):
                    self.operation.cancel()
                    await asyncio.gather(self.operation, return_exceptions=True)
            logger.warning("停止 | %s", reason)
            if self.cancel_on_risk if risk_exit else self.cancel_on_exit:
                try:
                    await asyncio.wait_for(
                        self.exchange.cancel_all_orders(), timeout=90
                    )
                    logger.warning("当前交易对挂单已确认撤销 | 持仓保留")
                except Exception as exc:
                    logger.error(
                        "撤单未确认（%s）| 请检查交易所挂单", type(exc).__name__
                    )
                    risk_exit = True
            logger.info("本次完整成交 %s 笔", self.exchange.filled_count)
        return 1 if risk_exit else 0


async def main(check_config=False):
    from dotenv import load_dotenv

    directory = Path(__file__).resolve().parent
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    console = logging.StreamHandler()
    level = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO").upper())
    console.setLevel(
        max(logging.INFO, level if isinstance(level, int) else logging.INFO)
    )
    console.setFormatter(
        logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    )
    logger.handlers[:] = [console]
    if not check_config:
        file_log = logging.FileHandler(directory / "Light_grid.log", encoding="utf-8")
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        logger.handlers.append(file_log)
    exchange = None
    config = {}
    stage = "检查配置"
    try:
        load_dotenv(directory / ".env", encoding="utf-8-sig")
        config = dict(os.environ)
        # Windows 环境变量枚举会大写键名，按原配置名读取以保留 gridStep 等名称。
        for key in (*REQUIRED_CONFIG, "leverage", "patch"):
            value = os.getenv(key)
            if value is not None:
                config[key] = value
        missing = [key for key in REQUIRED_CONFIG if not config.get(key, "").strip()]
        if missing:
            raise ValueError(
                "缺少配置项："
                + ", ".join(missing)
                + f"；请检查 {directory / '.env'} 或系统环境变量"
            )
        placeholders = [
            key for key in REQUIRED_CONFIG if config[key].startswith("YOUR_")
        ]
        if placeholders:
            raise ValueError("配置项仍为占位值：" + ", ".join(placeholders))
        exchange = LighterExchange(
            config.get("LIGHTER_NETWORK", "robinhood").strip(),
            int(config["LIGHTER_ACCOUNT_INDEX"]),
            int(config["LIGHTER_API_KEY_INDEX"]),
            config["LIGHTER_API_PRIVATE_KEY"].strip(),
        )
        trader = GridTrader(exchange, config)
        if check_config:
            logger.info("配置字段和策略参数检查通过（离线，未验证密钥有效性）")
            return 0
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, trader.stop_event.set)
            except NotImplementedError:
                signal.signal(
                    signum, lambda *_: loop.call_soon_threadsafe(trader.stop_event.set)
                )
        stage = "初始化 SDK / 加载市场"
        await exchange.initialize(config["SYMBOL"])
        logger.info(
            "Lighter RH v2.0 | %s | %s | 批量+WebSocket",
            exchange.network,
            exchange.symbol,
        )
        stage = "运行网格"
        return await trader.run()
    except Exception as exc:
        if isinstance(exc, KeyError):
            field = exc.args[0] if exc.args else ""
            detail = (
                f"缺少字段：{field}"
                if isinstance(field, str) and field.isidentifier() and len(field) <= 64
                else "缺少必要字段"
            )
        elif isinstance(exc, ExchangeError) or (
            stage == "检查配置" and isinstance(exc, ValueError)
        ):
            detail = str(exc)
        else:
            detail = type(exc).__name__
        for key, value in config.items():
            if value and any(
                word in key.upper()
                for word in ("PRIVATE_KEY", "SECRET", "TOKEN", "PASSWORD", "WEBHOOK")
            ):
                detail = detail.replace(value, "<redacted>")
                if value.startswith("0x") and len(value) > 16:
                    detail = detail.replace(value[2:], "<redacted>")
        logger.error("启动失败 | 阶段：%s | %s", stage, detail)
        frames = traceback.extract_tb(exc.__traceback__)
        logger.debug(
            "失败位置：%s",
            " -> ".join(
                f"{Path(frame.filename).name}:{frame.lineno} {frame.name}"
                for frame in frames
            ),
        )
        return 1
    finally:
        if exchange:
            await exchange.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lighter Robinhood 永续网格")
    parser.add_argument(
        "--check-config", action="store_true", help="仅离线检查配置，不连接或交易"
    )
    sys.exit(asyncio.run(main(parser.parse_args().check_config)))
