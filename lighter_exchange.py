"""Robinhood Chain 永续适配器；所有交易均经同一批量提交通道。"""

import asyncio
import contextvars
import json
import logging
import time
from collections import Counter, deque
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

logger = logging.getLogger("light_grid")
urgent_request = contextvars.ContextVar("urgent_request", default=False)
transaction_types = contextvars.ContextVar("transaction_types", default=())

REQUEST_LIMIT = 60
TX_TYPE_LIMIT = 40
CONTROL_LIMIT = 200
BATCH_SIZE = 15  # 官方 HTTP 批量建议大小，避免 CDN 正文限制。
WINDOW_SECONDS = 60.001


class ExchangeError(RuntimeError):
    """可恢复的读取错误；错误信息不携带鉴权响应。"""


class FatalError(ExchangeError):
    """交易结果或状态不能安全恢复。"""


class RateLimitError(ExchangeError):
    pass


def number(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("收到非有限数值")
    return result


class RequestBudget:
    """按官方滚动窗口用满额度；撤单优先，不提前扣留配额。"""

    def __init__(self, clock=time.monotonic, sleep=asyncio.sleep):
        self.clock = clock
        self.sleep = sleep
        self.requests = deque()
        self.transactions = {}
        self.control = deque()
        self.cooldown = 0.0
        self.urgent_waiters = 0
        self.wait_seconds = 0.0
        self.lock = asyncio.Lock()

    def delay(self, types=(), control=False):
        now = self.clock()
        queues = (self.requests, self.control, *self.transactions.values())
        for queue in queues:
            while queue and now - queue[0][0] >= WINDOW_SECONDS:
                queue.popleft()
        if control:
            return (
                max(0.0, self.control[0][0] + WINDOW_SECONDS - now)
                if len(self.control) >= CONTROL_LIMIT
                else 0.0
            )
        wait = max(0.0, self.cooldown - now)
        costs = Counter(types)
        if any(count > TX_TYPE_LIMIT for kind, count in costs.items() if kind != 15):
            raise ValueError("单批同类型交易超过每分钟上限")
        limits = [(self.requests, REQUEST_LIMIT, 1)]
        limits += [
            (self.transactions.setdefault(kind, deque()), TX_TYPE_LIMIT, count)
            for kind, count in costs.items()
            if kind != 15
        ]
        for queue, limit, cost in limits:
            used = sum(item[1] for item in queue)
            for stamp, amount in queue:
                if used + cost <= limit:
                    break
                wait = max(wait, stamp + WINDOW_SECONDS - now)
                used -= amount
        return wait

    def available(self, kind):
        self.delay()
        if kind == 15:
            return BATCH_SIZE
        return max(
            0, TX_TYPE_LIMIT - sum(n for _, n in self.transactions.get(kind, ()))
        )

    async def acquire(self, types=(), urgent=False, control=False):
        if len(types) > BATCH_SIZE:
            raise ValueError("批量超过 HTTP 单批大小")
        if urgent and not control:
            self.urgent_waiters += 1
        try:
            while True:
                async with self.lock:
                    wait = self.delay(types, control)
                    if not urgent and not control and self.urgent_waiters:
                        wait = max(wait, 0.05)
                    if wait <= 0:
                        now = self.clock()
                        if control:
                            self.control.append((now, 1))
                        else:
                            self.requests.append((now, 1))
                            for kind, count in Counter(types).items():
                                # 单笔撤单无子交易类型限制，仍计通用请求。
                                if kind != 15:
                                    self.transactions.setdefault(kind, deque()).append(
                                        (now, count)
                                    )
                        return
                delay = min(wait, 0.5)
                self.wait_seconds += delay
                await self.sleep(delay)
        finally:
            if urgent and not control:
                self.urgent_waiters -= 1

    def penalize(self, retry_after=60):
        self.cooldown = max(self.cooldown, self.clock() + max(0, retry_after))


class LighterExchange:
    def __init__(self, network, account_index, api_key_index, private_key):
        if network not in {"robinhood", "robinhood_testnet"}:
            raise ValueError("LIGHTER_NETWORK 仅支持 robinhood / robinhood_testnet")
        if account_index < 0 or api_key_index not in set(range(4, 255)) - {157}:
            raise ValueError("账户索引或 API key 索引无效（0–3、157、255 为保留索引）")
        if not private_key:
            raise ValueError("缺少 LIGHTER_API_PRIVATE_KEY")
        self.network = network
        self.account_index = account_index
        self.api_key_index = api_key_index
        self._private_key = private_key
        self.client = None
        self.account_api = None
        self.budget = RequestBudget()
        self.markets = {}
        self.market = None
        self.symbol = ""
        self.orders = {}
        self.order_versions = {}
        self.positions = {}
        self.assets = {}
        self.stats = {}
        self.bids = {}
        self.asks = {}
        self.book_nonce = None
        self.price = Decimal(0)
        self.last_price_at = 0.0
        self.last_message = 0.0
        self.last_stats_at = 0.0
        self.last_reconcile = 0.0
        self.reconcile_timeout = 60.0
        self.generation = 0
        self.reconciled_generation = -1
        self.snapshots = set()
        self.connected = False
        self.trading_enabled = False
        self.sequence = 0
        self.position_version = 0
        self.error = None
        self._nonce = None
        self._nonce_generation = -1
        self._client_index = 0
        self._sender = asyncio.Lock()
        self._ws_task = None
        self._closing = False
        self._auth_until = 0.0
        self._auth = ""
        self.pending_transactions = []
        self.before_send = None

    @property
    def ready(self):
        required = {
            "order_book",
            "account_all_orders",
            "account_all_positions",
            "account_all_assets",
            "user_stats",
        }
        return (
            self.connected
            and required <= self.snapshots
            and self.reconciled_generation == self.generation
            and time.monotonic() - self.last_message < 30
            and time.monotonic() - self.last_stats_at < 45
            and time.monotonic() - self.last_price_at < 30
            and time.monotonic() - self.last_reconcile < self.reconcile_timeout
            and self.error is None
        )

    def auth_token(self):
        if time.monotonic() >= self._auth_until:
            token, error = self.client.create_auth_token_with_expiry(
                deadline=3600,
                api_key_index=self.api_key_index,
            )
            if error:
                raise FatalError("鉴权 token 签名失败")
            self._auth, self._auth_until = token, time.monotonic() + 3300
        return self._auth

    async def initialize(self, symbol):
        import lighter

        profile = lighter.get_endpoint_profile(self.network)
        self.ws_url = profile.api_url.replace("https://", "wss://") + "/stream"
        self.client = lighter.SignerClient(
            url=profile.api_url,
            account_index=self.account_index,
            api_private_keys={self.api_key_index: self._private_key},
            chain_id=profile.chain_id,
        )
        # 禁用 SDK 内部重试，避免真实请求绕过统一预算。
        rest = self.client.api_client.rest_client
        rest.retry_client = None
        original_request = rest.request

        async def limited_request(method, url, **kwargs):
            urgent = urgent_request.get()
            types = transaction_types.get() if url.endswith("/sendTxBatch") else ()
            await self.budget.acquire(types, urgent)
            if types and not urgent:
                if not self.trading_enabled or not self.ready:
                    raise ExchangeError("交易已暂停，本批未提交")
                if self.before_send:
                    reason = self.before_send()
                    if reason:
                        raise FatalError(reason)
            kwargs["_request_timeout"] = 10
            response = await original_request(method, url, **kwargs)
            if response.status in {405, 429}:
                retry = response.getheader("Retry-After", "60")
                try:
                    delay = float(retry)
                except (TypeError, ValueError):
                    delay = 60
                self.budget.penalize(delay)
                await response.read()
                raise RateLimitError("API 限速，进入冷却")
            return response

        rest.request = limited_request
        self.account_api = lighter.AccountApi(self.client.api_client)
        response = await self.read(self.client.order_api.order_book_details)
        for detail in response["order_book_details"]:
            if detail["market_type"] == "perp" and detail["status"] == "active":
                self.markets[int(detail["market_id"])] = detail
        candidate = symbol.strip().upper()
        matches = [m for m in self.markets.values() if m["symbol"].upper() == candidate]
        if not matches:
            native = candidate.split("/", 1)[0].split(":", 1)[0]
            matches = [
                m for m in self.markets.values() if m["symbol"].upper() == native
            ]
        if len(matches) != 1:
            raise ValueError("SYMBOL 未匹配到唯一的活跃永续市场")
        self.market = matches[0]
        self.symbol = self.market["symbol"]
        self._ws_task = asyncio.create_task(self.websocket(), name="lighter-websocket")

    async def read(self, method, **kwargs):
        try:
            result = await method(**kwargs)
            result = result.to_dict() if hasattr(result, "to_dict") else result
            code = result.get("code")
            if code == 23000:
                self.budget.penalize()
                raise RateLimitError("API 限速，进入冷却")
            if code != 200:
                raise ExchangeError(f"读取被拒绝，code={code}")
            return result
        except (ExchangeError, asyncio.CancelledError):
            raise
        except KeyError as exc:
            field = exc.args[0] if exc.args else ""
            if isinstance(field, str) and field.isidentifier() and len(field) <= 64:
                raise ExchangeError(f"接口响应缺少字段：{field}") from None
            raise ExchangeError("接口响应缺少必要字段") from None
        except Exception as exc:
            raise ExchangeError(f"读取失败：{type(exc).__name__}") from None

    def amount_to_precision(self, amount):
        tick = Decimal(1).scaleb(-int(self.market["supported_size_decimals"]))
        return number(amount).quantize(tick, rounding=ROUND_DOWN)

    def price_to_precision(self, price):
        tick = Decimal(1).scaleb(-int(self.market["supported_price_decimals"]))
        return number(price).quantize(tick, rounding=ROUND_HALF_UP)

    def prepare_order(self, side, amount, price):
        amount, price = self.amount_to_precision(amount), self.price_to_precision(price)
        cost = amount * price
        if side not in {"buy", "sell"} or amount <= 0 or price <= 0:
            raise ValueError("订单方向、数量或价格无效")
        if amount < number(self.market["min_base_amount"]) or cost < number(
            self.market["min_quote_amount"]
        ):
            raise ValueError("订单数量或名义价值低于平台最小值")
        if cost > number(self.market["order_quote_limit"]):
            raise ValueError("订单名义价值超过平台上限")
        self._client_index = max(time.time_ns() // 1_000_000, self._client_index + 1)
        if self._client_index >= 1 << 48:
            raise ValueError("客户端订单编号超过 uint48")
        return {
            "id": str(self._client_index),
            "side": side,
            "amount": amount,
            "price": price,
        }

    def update_order(self, raw):
        client_id = raw.get("client_order_index")
        order_id = str(client_id if client_id else raw.get("order_index") or "")
        if not order_id:
            raise ValueError("订单缺少标识")
        amount = number(raw["initial_base_amount"])
        remaining = number(raw["remaining_base_amount"])
        if amount <= 0 or not 0 <= remaining <= amount:
            raise ValueError("订单数量不一致")
        status = str(raw["status"]).lower()
        if status == "filled" and remaining != 0:
            raise ValueError("成交终态仍有剩余数量")
        old = self.orders.get(order_id)
        if old:
            if old["status"] == "filled" or old["status"].startswith("canceled"):
                return
            if remaining > old["remaining"]:
                return
        self.sequence += 1
        self.orders[order_id] = {
            "id": order_id,
            "status": status,
            "amount": amount,
            "remaining": remaining,
            "filled": amount - remaining,
            "price": number(raw["price"]),
            "side": "sell" if raw["is_ask"] else "buy",
            "order_index": raw.get("order_index"),
        }
        self.order_versions[order_id] = self.sequence

    async def fetch_orders(self, urgent=False):
        token = urgent_request.set(urgent)
        try:
            start = self.sequence
            result = await self.read(
                self.client.order_api.account_active_orders,
                account_index=self.account_index,
                market_id=int(self.market["market_id"]),
                market_type="perp",
                authorization=self.auth_token(),
            )
            active_ids = set()
            for raw in result["orders"]:
                oid = str(raw.get("client_order_index") or raw["order_index"])
                active_ids.add(oid)
                if self.order_versions.get(oid, 0) <= start:
                    self.update_order(raw)
            for oid, order in list(self.orders.items()):
                if (
                    oid not in active_ids
                    and self.order_versions.get(oid, 0) <= start
                    and order["status"] in {"open", "pending", "in-progress"}
                ):
                    # 只移除过期挂单快照，策略归属保留，等待明确终态。
                    self.orders.pop(oid)
            return result["orders"]
        finally:
            urgent_request.reset(token)

    async def reconcile(self, tracked=()):
        generation, version = self.generation, self.position_version
        result = await self.read(
            self.account_api.account,
            by="index",
            value=str(self.account_index),
            active_only=True,
        )
        if result.get("next_cursor"):
            raise FatalError("账户快照需要翻页，不能据不完整仓位执行账户风控")
        account = next(
            (a for a in result["accounts"] if int(a["index"]) == self.account_index),
            None,
        )
        if account is None:
            raise FatalError("账户快照缺失")
        if account.get("bo_positions"):
            raise FatalError("当前账户存在二元期权持仓，永续风控口径不适用")
        if version == self.position_version:
            self.positions = {str(p["market_id"]): p for p in account["positions"]}
        start = self.sequence
        active = await self.fetch_orders()
        active_ids = set()
        for order in active:
            oid = str(order.get("client_order_index") or order["order_index"])
            active_ids.add(oid)
            if self.order_versions.get(oid, 0) <= start:
                self.update_order(order)
        missing = set(tracked) - active_ids
        missing = {
            oid
            for oid in missing
            if self.orders.get(oid, {}).get("status") not in {"filled"}
            and not self.orders.get(oid, {}).get("status", "").startswith("canceled")
        }
        cursor, cursors = None, set()
        while missing:
            kwargs = {"cursor": cursor} if cursor else {}
            page = await self.read(
                self.client.order_api.account_inactive_orders,
                account_index=self.account_index,
                market_id=int(self.market["market_id"]),
                market_type="perp",
                authorization=self.auth_token(),
                limit=100,
                **kwargs,
            )
            for order in page["orders"]:
                oid = str(order.get("client_order_index") or order["order_index"])
                if oid in missing:
                    self.update_order(order)
                    missing.discard(oid)
            cursor = page.get("next_cursor")
            if not cursor:
                break
            if cursor in cursors:
                raise FatalError("历史订单返回重复分页游标")
            cursors.add(cursor)
        # REST 中消失但未查到终态的订单保留原记录，不推断成交。
        if generation == self.generation and self.connected:
            if not self.bids or not self.asks:
                ticker = await self.read(
                    self.client.order_api.order_book_details,
                    market_id=int(self.market["market_id"]),
                )
                detail = ticker["order_book_details"][0]
                price = number(
                    detail.get("mark_price") or detail.get("last_trade_price") or 0
                )
                if price <= 0:
                    raise ExchangeError("盘口和备用行情均不可用")
                self.price = self.price_to_precision(price)
                self.last_price_at = time.monotonic()
            self.reconciled_generation = generation
            self.last_reconcile = time.monotonic()

    def metrics(self):
        # total_stats 含全仓和逐仓；portfolio_value 是已包含未实现盈亏的净值。
        stats = self.stats.get("total_stats") or self.stats
        if self.stats.get("account_trading_mode") == 1 and not self.stats.get(
            "total_stats"
        ):
            raise ValueError("Unified 账户缺少 total_stats 风险汇总")
        equity = number(stats["portfolio_value"])
        gross = Decimal(0)
        maintenance = Decimal(0)
        size = pnl = Decimal(0)
        for mid, position in self.positions.items():
            quantity = abs(number(position["position"]))
            if not quantity:
                continue
            value = abs(number(position["position_value"]))
            if value <= 0:
                raise ValueError("非零持仓缺少名义价值")
            market = self.markets.get(int(mid))
            if market is None:
                raise ValueError("账户其他持仓缺少永续市场风险参数")
            fraction = number(market["maintenance_margin_fraction"]) / 10000
            if not 0 < fraction <= 1:
                raise ValueError("市场维持保证金参数无效")
            gross += value
            maintenance += value * fraction
            if int(mid) == int(self.market["market_id"]):
                size += quantity * int(position["sign"])
                pnl += number(position["unrealized_pnl"])
        if equity <= 0:
            raise FatalError("账户净值非正，停止交易")
        return {
            "equity": equity,
            "mmr": maintenance / equity,
            "leverage": gross / equity,
            "size": size,
            "pnl": pnl,
        }

    def apply_message(self, message):
        self.last_message = time.monotonic()
        kind = message.get("type", "")
        if message.get("error"):
            code = message["error"].get("code")
            if code == 23000:
                self.budget.penalize()
            raise ExchangeError(f"WebSocket 返回错误，code={code}")
        if kind == "shutdown":
            raise ExchangeError("平台要求重新连接")
        if "/" not in kind:
            return
        action, channel = kind.split("/", 1)
        if action not in {"subscribed", "update"}:
            return
        snapshot = action == "subscribed"
        if channel == "order_book":
            book = message["order_book"]
            nonce = int(book["nonce"])
            if snapshot:
                self.bids.clear()
                self.asks.clear()
            elif self.book_nonce is None or int(book["begin_nonce"]) != self.book_nonce:
                raise ExchangeError("盘口序列不连续，重新同步")
            for key, levels in (("bids", self.bids), ("asks", self.asks)):
                for level in book.get(key, []):
                    price, size = number(level["price"]), number(level["size"])
                    if size > 0:
                        levels[price] = size
                    else:
                        levels.pop(price, None)
            self.book_nonce = nonce
            if self.bids and self.asks:
                bid, ask = max(self.bids), min(self.asks)
                if bid > ask:
                    raise ExchangeError("盘口价格交叉")
                self.price = self.price_to_precision((bid + ask) / 2)
                self.last_price_at = time.monotonic()
        elif channel == "account_all_orders":
            orders = message["orders"].get(str(self.market["market_id"]), [])
            for order in orders:
                self.update_order(order)
        elif channel == "account_all_positions":
            if message.get("bo_positions"):
                raise FatalError("账户出现二元期权持仓，停止永续网格")
            positions = message["positions"]
            self.positions = (
                dict(positions) if snapshot else {**self.positions, **positions}
            )
            self.position_version += 1
        elif channel == "account_all_assets":
            assets = message["assets"]
            self.assets = dict(assets) if snapshot else {**self.assets, **assets}
        elif channel == "user_stats":
            self.stats = message["stats"]
            self.last_stats_at = time.monotonic()
        elif channel == "account_tx":
            for tx in message.get("txs", []):
                if tx.get("hash") not in {t["hash"] for t in self.pending_transactions}:
                    continue
                event = tx.get("event_info") or "{}"
                event = json.loads(event) if isinstance(event, str) else event
                if event.get("ae"):
                    logger.debug("交易执行失败 hash=%s event=%s", tx["hash"], event)
                    self.error = FatalError("批量中的交易执行被拒绝")
        if snapshot:
            self.snapshots.add(channel)

    async def websocket(self):
        from websockets.asyncio.client import connect

        backoff = 2
        while not self._closing:
            try:
                await self.budget.acquire()  # GET /stream 也计入通用额度。
                async with connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=10,
                    max_queue=256,
                    max_size=4 * 1024 * 1024,
                ) as ws:
                    self.connected = True
                    self.generation += 1
                    self.snapshots.clear()
                    self.book_nonce = None
                    self._auth_until = 0
                    started = time.monotonic()
                    channels = [f"order_book/{self.market['market_id']}"]
                    channels += [
                        f"{c}/{self.account_index}"
                        for c in (
                            "account_all_orders",
                            "account_all_positions",
                            "account_all_assets",
                            "user_stats",
                            "account_tx",
                        )
                    ]
                    for channel in channels:
                        await self.budget.acquire(control=True)
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "subscribe",
                                    "channel": channel,
                                    "auth": self.auth_token(),
                                }
                            )
                        )
                    backoff = 2
                    while not self._closing:
                        message = json.loads(
                            await asyncio.wait_for(ws.recv(), timeout=25)
                        )
                        if message.get("type") == "ping":
                            await self.budget.acquire(control=True)
                            await ws.send('{"type":"pong"}')
                        else:
                            self.apply_message(message)
                        if (
                            "user_stats" in self.snapshots
                            and time.monotonic() - self.last_stats_at >= 45
                        ):
                            raise ExchangeError("账户统计过期，重新订阅")
                        if time.monotonic() - started >= 3000:
                            break  # 重连刷新鉴权，随后必须重新核对状态。
            except asyncio.CancelledError:
                raise
            except FatalError as exc:
                self.error = exc
                return
            except Exception as exc:
                logger.warning("连接恢复中（%s）", type(exc).__name__)
            finally:
                self.connected = False
                self.trading_enabled = False
            if not self._closing:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _submit(
        self, orders=None, cancel_ids=None, cancel_market=False, urgent=False
    ):
        orders, cancel_ids = orders or [], cancel_ids or []
        if not 1 <= len(orders) + len(cancel_ids) + int(cancel_market) <= BATCH_SIZE:
            raise ValueError(f"批量交易数量必须为 1–{BATCH_SIZE}")
        async with self._sender:
            if not urgent and not self.trading_enabled:
                raise ExchangeError("连接或风控状态不允许新交易")
            if self._nonce is None or self._nonce_generation != self.generation:
                result = await self.read(
                    self.client.tx_api.next_nonce,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )
                self._nonce = max(self._nonce or 0, int(result["nonce"]))
                self._nonce_generation = self.generation
            signed = []
            for order in orders:
                # SDK signer 使用 size_decimals / price_decimals，不是显示精度。
                signed.append(
                    self.client.sign_create_order(
                        market_index=int(self.market["market_id"]),
                        client_order_index=int(order["id"]),
                        base_amount=int(
                            order["amount"] * 10 ** int(self.market["size_decimals"])
                        ),
                        price=int(
                            order["price"] * 10 ** int(self.market["price_decimals"])
                        ),
                        is_ask=order["side"] == "sell",
                        order_type=self.client.ORDER_TYPE_LIMIT,
                        time_in_force=self.client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                        reduce_only=False,
                        order_expiry=self.client.DEFAULT_28_DAY_ORDER_EXPIRY,
                        nonce=self._nonce + len(signed),
                        api_key_index=self.api_key_index,
                    )
                )
            for oid in cancel_ids:
                signed.append(
                    self.client.sign_cancel_order(
                        market_index=int(self.market["market_id"]),
                        order_index=int(oid),
                        nonce=self._nonce + len(signed),
                        api_key_index=self.api_key_index,
                    )
                )
            if cancel_market:
                signed.append(
                    self.client.sign_cancel_all_orders(
                        time_in_force=self.client.CANCEL_ALL_TIF_IMMEDIATE,
                        timestamp_ms=0,
                        cancel_all_market_index=int(self.market["market_id"]),
                        skip_nonce=1,  # 清理时跨过先前被拒绝、未消耗 nonce 的交易。
                        nonce=self._nonce,
                        api_key_index=self.api_key_index,
                    )
                )
            if any(item[3] for item in signed):
                raise FatalError("批量签名失败，本批未发送")
            self.pending_transactions = [
                {"hash": item[2], "nonce": self._nonce + i, "type": item[0]}
                for i, item in enumerate(signed)
            ]
            self._nonce += len(signed)
            logger.debug(
                "提交批量：orders=%s tx=%s",
                [o["id"] for o in orders],
                self.pending_transactions,
            )
            count_token = transaction_types.set(tuple(s[0] for s in signed))
            urgent_token = urgent_request.set(urgent)
            try:
                response = await self.client.send_tx_batch(
                    [s[0] for s in signed], [s[1] for s in signed]
                )
                if response.code == 23000:
                    self.budget.penalize()
                if response.code != 200:
                    self._nonce = None
                    raise FatalError(f"批量提交被拒绝，code={response.code}")
            except FatalError:
                self.pending_transactions = []
                raise
            except Exception as exc:
                # 传输异常也可能已经提交；只确认，不自动重发。
                logger.warning("批量回执待确认（%s）", type(exc).__name__)
            finally:
                transaction_types.reset(count_token)
                urgent_request.reset(urgent_token)
            try:
                for attempt in range(5):
                    if self.error and not urgent:
                        raise self.error
                    if orders:
                        confirmed = all(
                            self.orders.get(o["id"], {}).get("status")
                            in {"open", "filled"}
                            or self.orders.get(o["id"], {})
                            .get("status", "")
                            .startswith("canceled")
                            for o in orders
                        )
                    else:
                        confirmed = False
                    if confirmed:
                        return
                    if attempt == 4:
                        break
                    await asyncio.sleep(1 if attempt == 0 else 2)
                    try:
                        if orders:
                            await self.reconcile([o["id"] for o in orders])
                        else:
                            active = await self.fetch_orders(urgent=urgent)
                            active_ids = {
                                str(o.get("client_order_index") or o["order_index"])
                                for o in active
                            }
                            if (cancel_market and not active) or (
                                not cancel_market and not set(cancel_ids) & active_ids
                            ):
                                return
                    except FatalError:
                        raise
                    except ExchangeError:
                        continue
                raise FatalError("无法确认批量交易结果，停止以避免重复下单")
            finally:
                self.pending_transactions = []

    async def create_orders(self, orders):
        await self._submit(orders=orders)

    async def cancel_orders(self, ids):
        token = urgent_request.set(True)
        try:
            for index in range(0, len(ids), BATCH_SIZE):
                await self._submit(
                    cancel_ids=ids[index : index + BATCH_SIZE], urgent=True
                )
        finally:
            urgent_request.reset(token)

    async def cancel_all_orders(self):
        token = urgent_request.set(True)
        try:
            await self._submit(cancel_market=True, urgent=True)
        finally:
            urgent_request.reset(token)

    async def close(self):
        self._closing = True
        if self._ws_task:
            self._ws_task.cancel()
            await asyncio.gather(self._ws_task, return_exceptions=True)
        if self.client:
            await self.client.close()
