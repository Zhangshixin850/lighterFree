"""离线回归：不读取 .env，不创建真实交易客户端。"""

import asyncio
import inspect
import json
import logging
import os
import tempfile
import time
import unittest
from collections import deque
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from Light_grid import GridTrader, main

from lighter_exchange import (
    ExchangeError,
    FatalError,
    LighterExchange,
    RequestBudget,
    RateLimitError,
    NotSubmittedError,
    transaction_types,
)

MARKET = {
    "market_id": 42,
    "symbol": "INTC",
    "market_type": "perp",
    "status": "active",
    "supported_size_decimals": 1,
    "supported_price_decimals": 2,
    "size_decimals": 3,
    "price_decimals": 4,
    "min_base_amount": "0.1",
    "min_quote_amount": "1",
    "order_quote_limit": "100000",
    "maintenance_margin_fraction": 100,
}
CONFIG = {
    "gridStep": "1",
    "gridLines": "6",
    "orderSize": "1",
    "lowLimit": "10",
    "highLimit": "100",
    "baseline": "60",
    "left": "0",
    "maxposi": "100",
    "MAX_LOSS": "-100",
    "MMR": "0.2",
    "leverage": "5",
    "CHECK_INTERVAL": "0.05",
    "DISCONNECT_TIMEOUT": "10",
    "CANCEL_ON_RISK": "true",
    "CANCEL_ON_EXIT": "true",
}


def exchange():
    venue = LighterExchange("robinhood", 123, 4, "FAKE_KEY")
    venue.market = MARKET.copy()
    venue.markets = {42: venue.market}
    venue.symbol = "INTC"
    venue.stats = {"portfolio_value": "1000"}
    venue.price = Decimal(60)
    venue.connected = True
    venue.snapshots = {
        "order_book",
        "account_all_orders",
        "account_all_positions",
        "account_all_assets",
        "user_stats",
    }
    venue.reconciled_generation = venue.generation
    venue.last_message = venue.last_reconcile = venue.last_price_at = (
        venue.last_stats_at
    ) = time.monotonic()
    venue.bids, venue.asks = {Decimal(59): Decimal(1)}, {Decimal(61): Decimal(1)}
    venue.trading_enabled = True
    return venue


def raw_order(oid="1", status="open", price="59", remaining="1", amount="1", ask=False):
    return {
        "client_order_index": int(oid),
        "order_index": int(oid) + 1000,
        "status": status,
        "price": price,
        "remaining_base_amount": remaining,
        "initial_base_amount": amount,
        "is_ask": ask,
    }


def submission_exchange(send=None):
    venue = exchange()
    venue._nonce, venue._nonce_generation = 100, venue.generation
    venue.client = SimpleNamespace(
        sign_create_order=lambda **kw: (14, "{}", "tx" + str(kw["nonce"]), None),
        sign_cancel_all_orders=lambda **kw: (16, "{}", "cancel" + str(kw["nonce"]), None),
        send_tx_batch=AsyncMock(side_effect=send, return_value=SimpleNamespace(code=200)),
        tx_api=SimpleNamespace(next_nonce=AsyncMock(return_value={"code": 200, "nonce": 100})),
        close=AsyncMock(), ORDER_TYPE_LIMIT=0, ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
        DEFAULT_28_DAY_ORDER_EXPIRY=-1, CANCEL_ALL_TIF_IMMEDIATE=0,
    )
    return venue


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_larger_batches_keep_request_and_transaction_limits(self):
        budget = RequestBudget(lambda: 100.0, AsyncMock())
        await budget.acquire(types=(14,) * 20)
        self.assertEqual(budget.available(14), 20)
        await budget.acquire(types=(14,) * 20)
        self.assertEqual(budget.available(14), 0)
        self.assertEqual(len(budget.requests), 2)
        await budget.acquire(types=(15,) * 50, urgent=True)
        self.assertEqual(len(budget.requests), 3)
        with self.assertRaisesRegex(ValueError, "平台单次容量"):
            await budget.acquire(types=(15,) * 51)

    async def test_full_sixty_request_window_without_reserve(self):
        now = [100.0]

        async def sleep(delay):
            now[0] += delay

        budget = RequestBudget(lambda: now[0], sleep)
        for _ in range(60):
            await budget.acquire()
        self.assertEqual(len(budget.requests), 60)
        self.assertGreater(budget.delay(), 59)
        await budget.acquire(urgent=True)
        stamps = [stamp for stamp, _ in budget.requests]
        self.assertLessEqual(len(stamps), 60)
        self.assertGreaterEqual(now[0], 160.001)

    async def test_children_budget_is_separate_from_http_count(self):
        now = [100.0]

        async def sleep(delay):
            now[0] += delay

        budget = RequestBudget(lambda: now[0], sleep)
        for count in (15, 15, 10):
            await budget.acquire(types=(14,) * count)
        self.assertEqual(len(budget.requests), 3)
        self.assertEqual(budget.available(14), 0)
        self.assertGreater(budget.delay(types=(14,)), 59)
        # 下单用满 40 笔后，撤单和读取仍可使用各自剩余额度。
        await budget.acquire(types=(16,), urgent=True)
        await budget.acquire()
        self.assertEqual(len(budget.requests), 5)
        await budget.acquire(types=(14,))
        self.assertLessEqual(sum(n for _, n in budget.transactions[14]), 40)

    async def test_partial_batch_uses_last_ten_transaction_slots(self):
        budget = RequestBudget(lambda: 100.0, AsyncMock())
        await budget.acquire(types=(14,) * 15)
        await budget.acquire(types=(14,) * 15)
        self.assertEqual(budget.available(14), 10)
        await budget.acquire(types=(14,) * 10)
        self.assertEqual(budget.available(14), 0)

    async def test_single_cancels_only_spend_general_request_quota(self):
        budget = RequestBudget(lambda: 100.0, AsyncMock())
        for _ in range(4):
            await budget.acquire(types=(15,) * 15, urgent=True)
        self.assertEqual(len(budget.requests), 4)
        self.assertNotIn(15, budget.transactions)

    async def test_cooldown_and_control_limit(self):
        now = [100.0]

        async def sleep(delay):
            now[0] += delay

        budget = RequestBudget(lambda: now[0], sleep)
        budget.penalize(60)
        await budget.acquire(urgent=True)
        self.assertGreaterEqual(now[0], 160)
        for _ in range(200):
            await budget.acquire(control=True)
        self.assertGreater(budget.delay(control=True), 59)

    async def test_urgent_request_gets_released_slot_before_ordinary(self):
        now = [100.0]
        release = asyncio.Event()

        async def sleep(delay):
            await release.wait()
            await asyncio.sleep(0)

        budget = RequestBudget(lambda: now[0], sleep)
        for _ in range(60):
            await budget.acquire()
        order = []

        async def request(name, urgent):
            await budget.acquire(urgent=urgent)
            order.append(name)

        ordinary = asyncio.create_task(request("ordinary", False))
        urgent = asyncio.create_task(request("urgent", True))
        await asyncio.sleep(0)
        now[0] = 160.002
        release.set()
        await asyncio.wait_for(asyncio.gather(ordinary, urgent), timeout=1)
        self.assertEqual(order, ["urgent", "ordinary"])
        self.assertEqual(budget.urgent_waiters, 0)


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.venue = exchange()
        self.trader = GridTrader(self.venue, CONFIG)
        self.trader.myNewPrice = Decimal(60)
        self.trader.switc = False

    def test_grid_symmetric_and_outer_edges(self):
        self.trader.build_grid()
        orders = list(self.trader.queue)
        self.assertEqual(
            [i["order"]["price"] for i in orders],
            list(map(Decimal, (59, 61, 58, 62, 57, 63))),
        )
        self.assertEqual([i["edge"] for i in orders], [0, 0, 0, 0, 1, 1])

    def test_patch_default_legacy_precedence_and_validation(self):
        for extra, expected in (
            ({}, 15),
            ({"BATCH_SIZE": "3"}, 3),
            ({"patch": "2", "BATCH_SIZE": "bad"}, 2),
            ({"patch": "1"}, 1),
            ({"patch": "20"}, 20),
            ({"patch": "100000"}, 100000),
        ):
            with self.subTest(extra=extra):
                venue = exchange()
                trader = GridTrader(venue, {**CONFIG, **extra})
                self.assertEqual(trader.batch_size, expected)
                self.assertEqual(venue.batch_size, expected)
        for value in ("0", "-1", "1.5", "", "bad"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "patch"):
                GridTrader(exchange(), {**CONFIG, "patch": value})

    def test_googl_grid_meets_minimum_notional_at_both_limits(self):
        venue = exchange()
        venue.market.update(
            supported_size_decimals=4, supported_price_decimals=2,
            min_base_amount="0.02", min_quote_amount="10",
        )
        config = {
            **CONFIG, "gridLines": "40", "gridStep": "0.04", "orderSize": "0.04",
            "lowLimit": "251", "highLimit": "400", "baseline": "280",
        }
        trader = GridTrader(venue, config)
        trader.validate_market()
        for center in (251, 400):
            trader.myNewPrice = Decimal(center)
            trader.build_grid()
            self.assertEqual(len(trader.queue), 40)
            self.assertTrue(all(
                item["order"]["price"] * item["order"]["amount"] >= 10
                for item in trader.queue
            ))
        invalid = GridTrader(venue, {**config, "lowLimit": "200"})
        with self.assertRaisesRegex(ValueError, "最小值"):
            invalid.validate_market()

    def test_display_reports_fills_without_requests(self):
        self.venue.filled_count = 7
        with patch("Light_grid.logger") as log:
            self.trader.display()
        message, *args = log.info.call_args.args
        self.assertIn("成交 7 笔", message % tuple(args))
        self.assertEqual(len(self.venue.budget.requests), 0)

    def test_user_sixty_second_rest_interval_is_valid(self):
        venue = exchange()
        trader = GridTrader(
            venue,
            {
                **CONFIG,
                "CHECK_INTERVAL": "0.8",
                "REST_INTERVAL": "60",
                "DISCONNECT_TIMEOUT": "30",
                "BATCH_SIZE": "15",
            },
        )
        self.assertEqual(trader.reconcile_interval, 60)
        self.assertEqual(venue.reconcile_timeout, 120)
        venue.last_reconcile = 100
        for now, expected in ((171, True), (221, False)):
            venue.last_message = venue.last_stats_at = venue.last_price_at = now
            with patch("lighter_exchange.time.monotonic", return_value=now):
                self.assertEqual(venue.ready, expected)

    def test_invalid_parameters_list_all_names_and_rules(self):
        with self.assertRaises(ValueError) as caught:
            GridTrader(
                exchange(),
                {**CONFIG, "gridLines": "3", "MMR": "20", "REST_INTERVAL": "5"},
            )
        message = str(caught.exception)
        self.assertIn("gridLines=3", message)
        self.assertIn("正偶数", message)
        self.assertIn("MMR=20", message)
        self.assertIn("REST_INTERVAL=5.0", message)
        self.assertNotIn("DISCONNECT_TIMEOUT", message)

    def test_nonfinite_rest_interval_is_rejected(self):
        for value in ("nan", "inf", "-inf"):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "REST_INTERVAL"),
            ):
                GridTrader(exchange(), {**CONFIG, "REST_INTERVAL": value})

    def test_fill_direction_price_and_reference_sort(self):
        self.trader.All_orders = {
            "1": [0, Decimal(58), "buy"],
            "2": [0, Decimal(61), "sell"],
        }
        self.venue.update_order(raw_order("1", "filled", "58", "0"))
        self.venue.update_order(raw_order("2", "filled", "61", "0", ask=True))
        self.trader.replacements()
        orders = list(self.trader.queue)
        self.assertEqual([i["origin"] for i in orders], ["2", "1"])
        self.assertEqual(
            [(i["order"]["side"], i["order"]["price"]) for i in orders],
            [("buy", Decimal(60)), ("sell", Decimal(59))],
        )
        self.trader.replacements()
        self.assertEqual(len(self.trader.queue), 2)

    def test_edge_wins_over_regular_fill(self):
        self.trader.All_orders = {
            "1": [0, Decimal(59), "buy"],
            "2": [1, Decimal(57), "buy"],
        }
        self.venue._strategy_order_ids.update({"1", "2"})
        self.venue.update_order(raw_order("1", "filled", remaining="0"))
        self.venue.update_order(raw_order("2", "filled", "57", "0"))
        self.trader.replacements()
        self.assertTrue(self.trader.switc)
        self.assertEqual(self.trader.myNewPrice, Decimal(57))
        self.assertFalse(self.trader.queue)
        self.assertEqual(self.venue.filled_count, 2)
        self.assertEqual(self.trader.counter, 0)

    def test_partial_cancellation_replaces_full_quantity(self):
        self.trader.All_orders = {"1": [1, Decimal(57), "buy"]}
        self.venue.update_order(raw_order("1", "canceled-expired", "57", "0.4"))
        self.trader.replacements()
        item = self.trader.queue[0]
        self.assertEqual(item["order"]["amount"], Decimal(1))
        self.assertEqual(item["order"]["price"], Decimal(57))
        self.assertEqual(item["edge"], 1)
        self.assertFalse(self.trader.switc)

    def test_unknown_status_does_not_replace(self):
        self.trader.All_orders = {"1": [0, Decimal(59), "buy"]}
        self.trader.replacements()
        self.assertFalse(self.trader.queue)

    def test_atomic_local_replacement_and_no_duplicate_fill(self):
        self.trader.All_orders = {"1": [0, Decimal(59), "buy"]}
        self.venue.update_order(raw_order("1", "filled", remaining="0"))
        self.trader.replacements()
        self.trader.batch = list(self.trader.queue)
        self.trader.queue.clear()
        self.trader.operation = SimpleNamespace(result=lambda: None)
        self.trader.operation_kind = "create"
        self.trader.finish_operation()
        self.assertNotIn("1", self.trader.All_orders)
        self.assertEqual(self.trader.last_fill_price, 59)
        self.assertEqual(self.trader.counter, 1)
        self.trader.replacements()
        self.assertFalse(self.trader.queue)

    def test_low_order_count_is_not_a_risk(self):
        self.assertEqual(self.trader.risk_reason(), "")

    def test_account_risk_uses_all_markets_and_thresholds(self):
        self.venue.markets[43] = {
            **MARKET,
            "market_id": 43,
            "maintenance_margin_fraction": 1000,
        }
        self.venue.positions = {
            "42": {
                "position": "1",
                "position_value": "1000",
                "sign": 1,
                "unrealized_pnl": "0",
            },
            "43": {
                "position": "1",
                "position_value": "1900",
                "sign": -1,
                "unrealized_pnl": "0",
            },
        }
        self.assertEqual(self.venue.metrics()["mmr"], Decimal("0.2"))
        self.assertIn("保证金率", self.trader.risk_reason())
        self.venue.positions = {
            "42": {
                "position": "1",
                "position_value": "5000",
                "sign": 1,
                "unrealized_pnl": "0",
            }
        }
        self.assertIn("杠杆", self.trader.risk_reason())

    def test_unified_equity_is_not_double_counted(self):
        self.venue.stats = {
            "account_trading_mode": 1,
            "portfolio_value": "99999",
            "total_stats": {"portfolio_value": "1000"},
        }
        self.venue.positions = {
            "42": {
                "position": "1",
                "position_value": "2000",
                "sign": 1,
                "unrealized_pnl": "100",
            }
        }
        self.assertEqual(self.venue.metrics()["equity"], 1000)
        self.assertEqual(self.venue.metrics()["leverage"], 2)
        self.venue.stats["total_stats"] = {}
        with self.assertRaises(ValueError):
            self.venue.metrics()

    def test_missing_risk_fields_and_nonpositive_equity_fail(self):
        self.venue.stats = {}
        with self.assertRaises(KeyError):
            self.venue.metrics()
        self.venue.stats = {"portfolio_value": "0"}
        with self.assertRaises(FatalError):
            self.venue.metrics()


class StreamTests(unittest.TestCase):
    def test_fill_count_is_owned_complete_and_deduplicated(self):
        venue = exchange()
        venue._strategy_order_ids.update({"1", "2"})
        venue.update_order(raw_order("1", remaining="0.4"))
        venue.update_order(raw_order("2", "canceled", remaining="0.4"))
        venue.update_order(raw_order("3", "filled", remaining="0"))  # 手动/旧单
        self.assertEqual(venue.filled_count, 0)
        filled = raw_order("1", "filled", remaining="0")
        venue.update_order(filled)
        venue.generation += 1
        venue.apply_message({"type": "subscribed/account_all_orders", "orders": {"42": [filled]}})
        venue.orders.pop("1")  # 本地终态清理后，重复快照仍不能重复计数。
        venue.update_order(filled)
        self.assertEqual(venue.filled_count, 1)
        self.assertFalse(venue._strategy_order_ids)
        self.assertEqual(exchange().filled_count, 0)

    def test_book_snapshot_delta_and_sequence_gap(self):
        venue = exchange()
        venue.apply_message(
            {
                "type": "subscribed/order_book",
                "order_book": {
                    "nonce": 10,
                    "bids": [{"price": "59", "size": "1"}],
                    "asks": [{"price": "61", "size": "1"}],
                },
            }
        )
        venue.apply_message(
            {
                "type": "update/order_book",
                "order_book": {
                    "begin_nonce": 10,
                    "nonce": 20,
                    "bids": [
                        {"price": "59", "size": "0"},
                        {"price": "58", "size": "1"},
                    ],
                    "asks": [],
                },
            }
        )
        self.assertEqual(venue.price, Decimal("59.50"))
        with self.assertRaises(ExchangeError):
            venue.apply_message(
                {
                    "type": "update/order_book",
                    "order_book": {"begin_nonce": 19, "nonce": 30},
                }
            )

    def test_terminal_cannot_be_reverted_by_stale_open(self):
        venue = exchange()
        venue.update_order(raw_order(status="filled", remaining="0"))
        venue.update_order(raw_order())
        self.assertEqual(venue.orders["1"]["status"], "filled")

    def test_position_snapshot_and_incremental_semantics(self):
        venue = exchange()
        venue.positions = {"42": {"position": "1"}}
        venue.apply_message({"type": "update/account_all_positions", "positions": {}})
        self.assertIn("42", venue.positions)
        venue.apply_message(
            {"type": "subscribed/account_all_positions", "positions": {}}
        )
        self.assertEqual(venue.positions, {})

    def test_reconnect_needs_new_snapshots_and_reconciliation(self):
        venue = exchange()
        self.assertTrue(venue.ready)
        venue.generation += 1
        self.assertFalse(venue.ready)
        venue.reconciled_generation = venue.generation
        venue.snapshots.clear()
        self.assertFalse(venue.ready)


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_patch_twenty_submits_one_twenty_order_batch(self):
        venue = exchange()
        GridTrader(venue, {**CONFIG, "patch": "20"})
        venue._nonce, venue._nonce_generation = 100, venue.generation
        orders = [venue.prepare_order("buy", 1, 59) for _ in range(20)]
        nonces = []

        def sign(**kwargs):
            nonces.append(kwargs["nonce"])
            return 14, "{}", str(kwargs["nonce"]), None

        async def send(types, infos):
            self.assertEqual(len(types), 20)
            for order in orders:
                venue.update_order(raw_order(order["id"]))
            return SimpleNamespace(code=200)

        venue.client = SimpleNamespace(
            sign_create_order=sign, send_tx_batch=AsyncMock(side_effect=send),
            ORDER_TYPE_LIMIT=0, ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
            DEFAULT_28_DAY_ORDER_EXPIRY=-1,
        )
        await venue.create_orders(orders)
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)
        self.assertEqual(nonces, list(range(100, 120)))

    async def test_accepted_orders_return_without_waiting_for_stream_or_rest(self):
        venue = exchange()
        venue._nonce, venue._nonce_generation = 100, venue.generation
        order = venue.prepare_order("buy", 1, 59)
        venue.client = SimpleNamespace(
            sign_create_order=lambda **kwargs: (14, "{}", "tx100", None),
            send_tx_batch=AsyncMock(return_value=SimpleNamespace(code=200)),
            ORDER_TYPE_LIMIT=0, ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
            DEFAULT_28_DAY_ORDER_EXPIRY=-1,
        )
        venue.reconcile = AsyncMock()

        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()) as sleep:
            self.assertTrue(await venue.create_orders([order]))
        sleep.assert_not_awaited()
        venue.reconcile.assert_not_awaited()
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)
        self.assertEqual(venue.orders[order["id"]]["status"], "pending")
        self.assertEqual(venue.filled_count, 0)
        self.assertIsNone(venue.verification_task)
        venue.update_order(raw_order(order["id"], "filled", remaining="0"))
        self.assertEqual(venue.filled_count, 1)

        # 后续补单签名失败，不回退已经确认的成交数。
        venue.client.sign_create_order = lambda **kwargs: (14, "{}", "bad", "error")
        with self.assertRaises(FatalError):
            await venue.create_orders([venue.prepare_order("sell", 1, 60)])
        self.assertEqual(venue.filled_count, 1)

    async def test_patch_limits_cancel_chunks_and_direct_submit(self):
        venue = exchange()
        GridTrader(venue, {**CONFIG, "patch": "2"})
        submit = venue._submit
        venue._submit = AsyncMock()
        await venue.cancel_orders(["1", "2", "3", "4", "5"])
        self.assertEqual(
            [call.kwargs["cancel_ids"] for call in venue._submit.await_args_list],
            [["1", "2"], ["3", "4"], ["5"]],
        )
        self.assertTrue(all(call.kwargs["urgent"] for call in venue._submit.await_args_list))
        with self.assertRaisesRegex(ValueError, "patch"):
            await submit(orders=[{}, {}, {}])
        GridTrader(venue, {**CONFIG, "patch": "100000"})
        venue._submit.reset_mock()
        await venue.cancel_orders([str(i) for i in range(105)])
        self.assertEqual(
            [len(call.kwargs["cancel_ids"]) for call in venue._submit.await_args_list],
            [50, 50, 5],
        )
        with self.assertRaisesRegex(ValueError, "平台容量"):
            await submit(orders=[{}] * 51)

    async def test_initialization_maps_symbol_and_wraps_real_request_boundary(self):
        import lighter

        venue = LighterExchange("robinhood", 123, 4, "FAKE_KEY")
        transport = SimpleNamespace(request=AsyncMock(), retry_client=None)
        original = transport.request

        async def details():
            await transport.request(
                "GET", "https://api.rh.lighter.xyz/api/v1/orderBookDetails"
            )
            return {"code": 200, "order_book_details": [MARKET]}

        client = SimpleNamespace(
            api_client=SimpleNamespace(rest_client=transport),
            order_api=SimpleNamespace(order_book_details=details),
        )
        with (
            patch.object(lighter, "SignerClient", return_value=client),
            patch.object(lighter, "AccountApi", return_value=object()),
            patch.object(venue, "websocket", new=AsyncMock()),
        ):
            await venue.initialize("INTC/USDT:USDT")
        self.assertEqual(venue.symbol, "INTC")
        self.assertEqual(len(venue.budget.requests), 1)
        await asyncio.gather(venue._ws_task)
        # 即使已准备好订单，预算等待结束后也必须重新检查风险。
        sample = exchange()
        venue.connected = True
        venue.snapshots = sample.snapshots
        venue.reconciled_generation = venue.generation
        now = time.monotonic()
        venue.last_message = venue.last_stats_at = venue.last_reconcile = (
            venue.last_price_at
        ) = now
        venue.trading_enabled = True
        venue.before_send = lambda: "模拟发送前风险超限"
        token = transaction_types.set((14,))
        try:
            with self.assertRaises(FatalError):
                await transport.request(
                    "POST", "https://api.rh.lighter.xyz/api/v1/sendTxBatch"
                )
        finally:
            transaction_types.reset(token)
        original.assert_awaited_once()

    async def test_batch_signing_uses_native_scales_and_sequential_nonces(self):
        venue = exchange()
        venue._nonce, venue._nonce_generation = 100, venue.generation
        signatures = []

        def sign(**kwargs):
            signatures.append(kwargs)
            return 14, json.dumps(kwargs), "tx" + str(kwargs["nonce"]), None

        orders = [
            venue.prepare_order("buy", "0.29", "59.126"),
            venue.prepare_order("sell", "0.2", "61"),
        ]

        async def send(types, infos):
            for order in orders:
                venue.update_order(
                    raw_order(
                        order["id"],
                        "open",
                        str(order["price"]),
                        str(order["amount"]),
                        str(order["amount"]),
                    )
                )
            return SimpleNamespace(code=200)

        venue.client = SimpleNamespace(
            sign_create_order=sign,
            send_tx_batch=AsyncMock(side_effect=send),
            ORDER_TYPE_LIMIT=0,
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
            DEFAULT_28_DAY_ORDER_EXPIRY=-1,
        )
        await venue.create_orders(orders)
        self.assertEqual([s["nonce"] for s in signatures], [100, 101])
        self.assertEqual(signatures[0]["base_amount"], 200)
        self.assertEqual(signatures[0]["price"], 591300)
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)

    async def test_uncertain_submit_never_resends(self):
        venue = exchange()
        venue._nonce, venue._nonce_generation = 100, venue.generation
        order = venue.prepare_order("buy", 1, 59)
        venue.client = SimpleNamespace(
            sign_create_order=lambda **kwargs: (14, "{}", "tx100", None),
            send_tx_batch=AsyncMock(side_effect=TimeoutError()),
            ORDER_TYPE_LIMIT=0,
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
            DEFAULT_28_DAY_ORDER_EXPIRY=-1,
        )
        venue.reconcile = AsyncMock()
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            self.assertFalse(await venue.create_orders([order]))
            with self.assertRaises(FatalError):
                await venue.verification_task
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)
        self.assertGreaterEqual(venue.reconcile.await_count, 1)

    async def test_market_cancel_is_filtered_single_transaction_and_confirmed(self):
        venue = exchange()
        venue._nonce, venue._nonce_generation = 100, venue.generation
        venue.client = SimpleNamespace(
            sign_cancel_all_orders=lambda **kwargs: (
                16,
                json.dumps(kwargs),
                "cancel100",
                None,
            ),
            send_tx_batch=AsyncMock(return_value=SimpleNamespace(code=200)),
            CANCEL_ALL_TIF_IMMEDIATE=0,
        )
        venue.fetch_orders = AsyncMock(return_value=[])
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            await venue.cancel_all_orders()
        types, infos = venue.client.send_tx_batch.await_args.args
        self.assertEqual(types, [16])
        self.assertEqual(json.loads(infos[0])["cancel_all_market_index"], 42)
        self.assertEqual(json.loads(infos[0])["skip_nonce"], 1)
        venue.fetch_orders.assert_awaited()

    async def test_inactive_pagination_and_unknown_order(self):
        venue = exchange()
        account = {"code": 200, "accounts": [{"index": 123, "positions": []}]}
        pages = deque(
            [
                {"code": 200, "orders": [], "next_cursor": "page2"},
                {
                    "code": 200,
                    "orders": [raw_order("1", "filled", remaining="0")],
                    "next_cursor": "",
                },
            ]
        )

        async def read(method, **kwargs):
            return account if method == "account" else pages.popleft()

        venue.account_api = SimpleNamespace(account="account")
        venue.client = SimpleNamespace(
            order_api=SimpleNamespace(account_inactive_orders="inactive")
        )
        venue.auth_token = lambda: "FAKE_TOKEN"
        venue.read = read
        venue.fetch_orders = AsyncMock(return_value=[])
        await venue.reconcile(["1", "2"])
        self.assertEqual(venue.orders["1"]["status"], "filled")
        self.assertNotIn("2", venue.orders)

    async def test_sdk_signatures_without_constructing_trading_client(self):
        from importlib.metadata import version

        import lighter

        self.assertEqual(version("lighter-sdk"), "1.1.8")
        parameters = inspect.signature(
            lighter.SignerClient.sign_cancel_all_orders
        ).parameters
        self.assertIn("cancel_all_market_index", parameters)
        self.assertIn("skip_nonce", parameters)
        self.assertEqual(lighter.get_endpoint_profile("robinhood").chain_id, 466324)

    async def test_active_snapshot_does_not_overwrite_concurrent_fill(self):
        venue = exchange()
        venue.update_order(raw_order())

        async def read(*args, **kwargs):
            venue.update_order(raw_order(status="filled", remaining="0"))
            return {"code": 200, "orders": [raw_order()]}

        venue.client = SimpleNamespace(
            order_api=SimpleNamespace(account_active_orders="active")
        )
        venue.auth_token = lambda: "FAKE_TOKEN"
        venue.read = read
        await venue.fetch_orders()
        self.assertEqual(venue.orders["1"]["status"], "filled")

    async def test_missing_active_order_is_not_labeled_filled(self):
        venue = exchange()
        venue.update_order(raw_order())
        venue.client = SimpleNamespace(
            order_api=SimpleNamespace(account_active_orders="active")
        )
        venue.auth_token = lambda: "FAKE_TOKEN"
        venue.read = AsyncMock(return_value={"code": 200, "orders": []})
        await venue.fetch_orders()
        self.assertNotIn("1", venue.orders)

    async def test_repeated_history_cursor_fails_closed(self):
        venue = exchange()
        venue.account_api = SimpleNamespace(account="account")
        venue.client = SimpleNamespace(
            order_api=SimpleNamespace(account_inactive_orders="inactive")
        )
        venue.auth_token = lambda: "FAKE_TOKEN"
        venue.fetch_orders = AsyncMock(return_value=[])

        async def read(method, **kwargs):
            if method == "account":
                return {"code": 200, "accounts": [{"index": 123, "positions": []}]}
            return {"code": 200, "orders": [], "next_cursor": "same"}

        venue.read = read
        with self.assertRaises(FatalError):
            await venue.reconcile(["unknown"])

    async def test_late_confirmation_on_last_lookup_is_accepted(self):
        venue = exchange()
        venue._nonce, venue._nonce_generation = 100, venue.generation
        order = venue.prepare_order("buy", 1, 59)
        venue.client = SimpleNamespace(
            sign_create_order=lambda **kwargs: (14, "{}", "tx100", None),
            send_tx_batch=AsyncMock(side_effect=TimeoutError()),
            ORDER_TYPE_LIMIT=0,
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
            DEFAULT_28_DAY_ORDER_EXPIRY=-1,
        )
        lookups = []

        async def reconcile(ids, *, force_history=False):
            self.assertTrue(force_history)
            lookups.append(ids)
            if len(lookups) == 4:
                venue.update_order(raw_order(order["id"]))

        venue.reconcile = reconcile
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            self.assertFalse(await venue.create_orders([order]))
            await venue.verification_task
        self.assertEqual(len(lookups), 4)

    async def test_rate_rejection_preserves_nonce_and_does_not_verify(self):
        venue = exchange()
        venue._nonce, venue._nonce_generation = 100, venue.generation
        venue.client = SimpleNamespace(
            sign_create_order=lambda **kwargs: (14, "{}", "tx100", None),
            send_tx_batch=AsyncMock(return_value=SimpleNamespace(code=23000)),
            ORDER_TYPE_LIMIT=0,
            ORDER_TIME_IN_FORCE_GOOD_TILL_TIME=1,
            DEFAULT_28_DAY_ORDER_EXPIRY=-1,
        )
        venue.reconcile = AsyncMock()
        with self.assertRaises(RateLimitError):
            await venue.create_orders([venue.prepare_order("buy", 1, 59)])
        self.assertEqual(venue._nonce, 100)
        self.assertEqual(venue._nonce_generation, -1)
        self.assertGreater(venue.budget.cooldown, time.monotonic())
        self.assertFalse(venue.orders)
        self.assertFalse(venue.pending_transactions)
        self.assertFalse(venue._strategy_order_ids)
        self.assertIsNone(venue.verification_task)
        venue.reconcile.assert_not_awaited()


class SubmissionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_rate_status_is_preserved_even_if_body_read_fails(self):
        import lighter

        for status in (405, 429):
            with self.subTest(status=status):
                venue = LighterExchange("robinhood", 123, 4, "FAKE_KEY")
                response = SimpleNamespace(
                    status=status, getheader=lambda *args: "60",
                    read=AsyncMock(side_effect=OSError("SENSITIVE_RESPONSE_BODY")),
                )
                transport = SimpleNamespace(request=AsyncMock(side_effect=[
                    SimpleNamespace(status=200), response,
                ]), retry_client=None)

                async def details():
                    await transport.request("GET", "https://example.invalid/orderBookDetails")
                    return {"code": 200, "order_book_details": [MARKET]}

                client = SimpleNamespace(
                    api_client=SimpleNamespace(rest_client=transport),
                    order_api=SimpleNamespace(order_book_details=details),
                )
                with (
                    patch.object(lighter, "SignerClient", return_value=client),
                    patch.object(lighter, "AccountApi", return_value=object()),
                    patch.object(venue, "websocket", new=AsyncMock()),
                ):
                    await venue.initialize("INTC")
                await venue._ws_task
                sample = exchange()
                for name in ("connected", "trading_enabled", "snapshots", "reconciled_generation",
                             "last_message", "last_price_at", "last_stats_at", "last_reconcile"):
                    setattr(venue, name, getattr(sample, name))
                token = transaction_types.set((14,))
                try:
                    with self.assertRaises(RateLimitError) as caught:
                        await transport.request("POST", "https://example.invalid/sendTxBatch")
                finally:
                    transaction_types.reset(token)
                self.assertIn(str(status), str(caught.exception))
                self.assertNotIn("SENSITIVE_RESPONSE_BODY", str(caught.exception))
                self.assertGreater(venue.budget.cooldown, time.monotonic())

    async def test_rate_limited_replacement_keeps_origin_and_retries_same_order(self):
        venue = submission_exchange()
        trader = GridTrader(venue, CONFIG)
        trader.switc = False
        trader.myNewPrice = Decimal(60)
        trader.All_orders = {"1": [0, Decimal(59), "buy"]}
        venue.update_order(raw_order("1", "filled", remaining="0"))
        trader.replacements()
        order = trader.queue[0]["order"]
        venue.reconcile = AsyncMock()
        venue.client.send_tx_batch.side_effect = [
            RateLimitError("HTTP 429 限速"), SimpleNamespace(code=200),
        ]
        for attempt in range(2):
            trader.batch = [trader.queue.popleft()]
            trader.operation_kind = "create"
            trader.operation = asyncio.create_task(venue.create_orders([order]))
            await asyncio.gather(trader.operation, return_exceptions=True)
            trader.finish_operation()
            if attempt == 0:
                self.assertIn("1", trader.All_orders)
                self.assertEqual(trader.counter, 0)
                self.assertEqual(trader.queue[0]["order"]["id"], order["id"])
                self.assertFalse(trader.batch)
                self.assertFalse(venue.orders.get(order["id"]))
                self.assertEqual(venue._nonce, 100)
                self.assertEqual(trader.phase, "限速冷却")
                venue.budget.cooldown = 0  # 模拟服务端冷却结束。
        self.assertNotIn("1", trader.All_orders)
        self.assertIn(order["id"], trader.All_orders)
        self.assertEqual(trader.counter, 1)
        self.assertEqual(venue.client.send_tx_batch.await_count, 2)
        venue.client.tx_api.next_nonce.assert_awaited_once()
        venue.reconcile.assert_not_awaited()
        trader.replacements()
        self.assertFalse(trader.queue)

    async def test_not_sent_replacement_is_not_registered_as_success(self):
        venue = submission_exchange(NotSubmittedError("连接暂停，本批未提交"))
        trader = GridTrader(venue, CONFIG)
        trader.switc = False
        trader.All_orders = {"1": [0, Decimal(59), "buy"]}
        venue.update_order(raw_order("1", "filled", remaining="0"))
        trader.replacements()
        trader.batch = [trader.queue.popleft()]
        trader.operation_kind = "create"
        trader.operation = asyncio.create_task(venue.create_orders([trader.batch[0]["order"]]))
        await asyncio.gather(trader.operation, return_exceptions=True)
        trader.finish_operation()
        self.assertEqual(trader.counter, 0)
        self.assertIn("1", trader.All_orders)
        self.assertEqual(len(trader.queue), 1)
        self.assertFalse(venue.pending_transactions)
        self.assertEqual(venue._nonce, 100)

    async def test_successful_receipt_does_not_wait_or_query(self):
        venue = submission_exchange()
        order = venue.prepare_order("buy", 1, 59)
        venue.reconcile = AsyncMock()
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()) as sleep:
            self.assertTrue(await venue.create_orders([order]))
        sleep.assert_not_awaited()
        venue.reconcile.assert_not_awaited()
        self.assertEqual(venue.orders[order["id"]]["status"], "pending")
        self.assertEqual(venue.filled_count, 0)
        venue.update_order(raw_order(order["id"], "filled", remaining="0"))
        self.assertEqual(venue.filled_count, 1)

    async def test_accepted_pending_order_survives_older_empty_active_snapshot(self):
        venue = submission_exchange()
        order = venue.prepare_order("buy", 1, 59)
        await venue.create_orders([order])
        venue.auth_token = lambda: "FAKE_TOKEN"
        venue.client.order_api = SimpleNamespace(account_active_orders=AsyncMock(
            return_value={"code": 200, "orders": []}
        ))
        await venue.fetch_orders()
        self.assertEqual(venue.orders[order["id"]]["status"], "pending")
        self.assertEqual(len(venue.pending_transactions), 1)

    async def test_execution_rejection_from_earlier_accepted_batch_is_tracked(self):
        venue = submission_exchange()
        first = venue.prepare_order("buy", 1, 59)
        second = venue.prepare_order("sell", 1, 61)
        await venue.create_orders([first])
        await venue.create_orders([second])
        self.assertEqual(len(venue.pending_transactions), 2)
        venue.apply_message({"type": "update/account_tx", "txs": [
            {"hash": "tx100", "event_info": {"ae": "execution rejected"}},
        ]})
        self.assertIsInstance(venue.error, FatalError)

    async def test_observed_orders_are_never_retried_after_a_rate_response(self):
        venue = submission_exchange()
        order = venue.prepare_order("buy", 1, 59)

        async def send(*args):
            venue.update_order(raw_order(order["id"], "filled", remaining="0"))
            raise RateLimitError("HTTP 429")

        venue.client.send_tx_batch.side_effect = send
        self.assertFalse(await venue.create_orders([order]))
        await venue.verification_task
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)
        self.assertEqual(venue.filled_count, 1)

    async def test_unknown_result_keeps_origin_until_background_lookup_completes(self):
        venue = submission_exchange(TimeoutError())
        trader = GridTrader(venue, CONFIG)
        trader.switc = False
        trader.All_orders = {"1": [0, Decimal(59), "buy"]}
        venue.update_order(raw_order("1", "filled", remaining="0"))
        trader.replacements()
        trader.batch = [trader.queue.popleft()]
        order = trader.batch[0]["order"]
        started, release = asyncio.Event(), asyncio.Event()

        async def reconcile(ids, **kwargs):
            started.set()
            await release.wait()
            venue.update_order(raw_order(order["id"]))

        venue.reconcile = reconcile
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            trader.operation_kind = "create"
            trader.operation = asyncio.create_task(venue.create_orders([order]))
            await trader.operation
            trader.finish_operation()
            await started.wait()
            self.assertEqual(trader.operation_kind, "verify")
            self.assertIn("1", trader.All_orders)
            self.assertEqual(trader.counter, 0)
            trader.replacements()
            self.assertFalse(trader.queue)
            release.set()
            await trader.operation
            trader.finish_operation()
        self.assertEqual(trader.counter, 1)
        self.assertNotIn("1", trader.All_orders)
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)

    async def test_pending_orders_count_toward_the_forty_plus_four_guard(self):
        venue = submission_exchange()
        trader = GridTrader(venue, {**CONFIG, "gridLines": "40"})
        venue._register_pending([venue.prepare_order("buy", 1, 59) for _ in range(44)])
        self.assertEqual(trader.risk_reason(), "")
        venue._register_pending([venue.prepare_order("buy", 1, 59)])
        self.assertIn("挂单数量", trader.risk_reason())

    async def test_background_lookup_can_wait_through_rate_cooldown(self):
        venue = submission_exchange(TimeoutError())
        order = venue.prepare_order("buy", 1, 59)
        calls = []

        async def reconcile(ids, **kwargs):
            calls.append(ids)
            if len(calls) == 1:
                venue.budget.penalize()
                raise RateLimitError("HTTP 429")
            venue.update_order(raw_order(order["id"]))

        venue.reconcile = reconcile
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            self.assertFalse(await venue.create_orders([order]))
            await venue.verification_task
        self.assertEqual(len(calls), 2)
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)

    async def test_non_rate_rejection_is_not_registered_or_retried(self):
        venue = submission_exchange()
        venue.client.send_tx_batch.return_value = SimpleNamespace(code=21000)
        with self.assertRaises(FatalError):
            await venue.create_orders([venue.prepare_order("buy", 1, 59)])
        self.assertFalse(venue.orders)
        self.assertFalse(venue.pending_transactions)
        self.assertEqual(venue.client.send_tx_batch.await_count, 1)

    async def test_cleanup_cancel_recovers_from_rate_rejection(self):
        venue = submission_exchange()
        venue.client.send_tx_batch.side_effect = [RateLimitError("HTTP 429"), SimpleNamespace(code=200)]
        venue.fetch_orders = AsyncMock(return_value=[])
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            await venue.cancel_all_orders()
        self.assertEqual(venue.client.send_tx_batch.await_count, 2)
        venue.fetch_orders.assert_awaited_once()

    async def test_close_cancels_unknown_result_lookup(self):
        venue = submission_exchange(TimeoutError())
        started = asyncio.Event()

        async def reconcile(*args, **kwargs):
            started.set()
            await asyncio.Event().wait()

        venue.reconcile = reconcile
        with patch("lighter_exchange.asyncio.sleep", new=AsyncMock()):
            await venue.create_orders([venue.prepare_order("buy", 1, 59)])
            await started.wait()
            await venue.close()
        self.assertTrue(venue.verification_task.cancelled())


class QueryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.venue = exchange()
        self.venue.account_api = SimpleNamespace(account="account")
        self.venue.client = SimpleNamespace(
            order_api=SimpleNamespace(account_inactive_orders="history"), close=AsyncMock()
        )
        self.venue.auth_token = lambda: "FAKE_TOKEN"
        self.venue.fetch_orders = AsyncMock(return_value=[])
        self.account = AsyncMock(return_value={
            "code": 200, "accounts": [{"index": 123, "positions": []}],
        })
        self.history = AsyncMock(return_value={"code": 200, "orders": []})

        async def read(method, **kwargs):
            return await (self.account if method == "account" else self.history)(**kwargs)

        self.venue.read = read

    async def test_concurrent_reconciles_share_reads_and_include_late_ids(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def history(**kwargs):
            if self.history.await_count == 1:
                started.set()
                await release.wait()
                return {"code": 200, "orders": [raw_order("1", "filled", remaining="0")]}
            return {"code": 200, "orders": [raw_order("2", "filled", remaining="0")]}

        self.history.side_effect = history
        self.venue._strategy_order_ids.update({"1", "2"})
        first = asyncio.create_task(self.venue.reconcile(["1"]))
        await started.wait()
        second = asyncio.create_task(self.venue.reconcile(["2"], force_history=True))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, second)
        self.assertEqual(self.account.await_count, 1)
        self.assertEqual(self.venue.fetch_orders.await_count, 1)
        self.assertEqual(self.history.await_count, 2)
        self.assertEqual(self.venue.filled_count, 2)

    async def test_cancelled_waiter_does_not_cancel_shared_work(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def account(**kwargs):
            started.set()
            await release.wait()
            return {"code": 200, "accounts": [{"index": 123, "positions": []}]}

        self.account.side_effect = account
        first = asyncio.create_task(self.venue.reconcile())
        await started.wait()
        second = asyncio.create_task(self.venue.reconcile())
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        self.assertFalse(self.venue._reconcile_task.cancelled())
        release.set()
        await second
        self.assertEqual(self.account.await_count, 1)
        self.assertEqual(self.venue.fetch_orders.await_count, 1)

    async def test_unknown_history_cooldown_force_and_reconnect(self):
        self.venue.update_order(raw_order("1"))
        now = [100]
        with patch("lighter_exchange.time", SimpleNamespace(monotonic=lambda: now[0])):
            await self.venue.reconcile(["1"])
            self.assertEqual(self.venue.orders["1"]["status"], "open")
            self.assertEqual(self.venue._history_next["1"], 220)
            now[0] = 160
            await self.venue.reconcile(["1"])
            self.assertEqual(self.history.await_count, 1)
            await self.venue.reconcile(["1"], force_history=True)
            self.assertEqual(self.history.await_count, 2)
            now[0] = 281
            await self.venue.reconcile(["1"])
            self.assertEqual(self.history.await_count, 3)
            self.venue.generation += 1
            await self.venue.reconcile(["1"])
            self.assertEqual(self.history.await_count, 4)
        self.assertEqual(self.venue.filled_count, 0)
        self.venue.update_order(raw_order("1", "filled", remaining="0"))
        self.assertNotIn("1", self.venue._history_next)
        await self.venue.reconcile(["1"])
        self.assertEqual(self.history.await_count, 4)

    async def test_force_joining_deferred_history_is_not_skipped(self):
        self.venue._history_generation = self.venue.generation
        self.venue._history_next["1"] = time.monotonic() + 120
        started, release = asyncio.Event(), asyncio.Event()

        async def history(**kwargs):
            if self.history.await_count == 1:
                started.set()
                await release.wait()
                return {"code": 200, "orders": []}
            return {"code": 200, "orders": [raw_order("1", "filled", remaining="0")]}

        self.history.side_effect = history
        first = asyncio.create_task(self.venue.reconcile(["1", "2"]))
        await started.wait()
        second = asyncio.create_task(self.venue.reconcile(["1"], force_history=True))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, second)
        self.assertEqual(self.venue.orders["1"]["status"], "filled")
        self.assertEqual(self.account.await_count, 1)
        self.assertEqual(self.venue.fetch_orders.await_count, 1)

    async def test_reconnect_during_read_cannot_validate_stale_generation(self):
        async def account(**kwargs):
            self.venue.generation += 1
            self.venue.positions = {"42": {"position": "2"}}
            return {"code": 200, "accounts": [{"index": 123, "positions": []}]}

        self.account.side_effect = account
        await self.venue.reconcile()
        self.assertFalse(self.venue.ready)
        self.assertIn("42", self.venue.positions)

    async def test_close_cancels_shared_work_and_rejects_new_reads(self):
        started = asyncio.Event()

        async def account(**kwargs):
            started.set()
            await asyncio.Event().wait()

        self.account.side_effect = account
        waiter = asyncio.create_task(self.venue.reconcile())
        await started.wait()
        await self.venue.close()
        await asyncio.gather(waiter, return_exceptions=True)
        self.assertTrue(self.venue._reconcile_task.cancelled())
        self.venue.client.close.assert_awaited_once()
        with self.assertRaises(ExchangeError):
            await self.venue.reconcile()


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_rate_limited_fill_recovers_without_stopping_or_duplicate_registration(self):
        venue = submission_exchange()
        trader = GridTrader(venue, CONFIG)
        trader.switc = False
        trader.myNewPrice = Decimal(60)
        trader._next_sync = float("inf")
        trader.All_orders = {"1": [0, Decimal(59), "buy"]}
        venue.update_order(raw_order("1", "filled", remaining="0"))
        venue.client.send_tx_batch.side_effect = [RateLimitError("HTTP 429"), SimpleNamespace(code=200)]
        venue.cancel_all_orders = AsyncMock()
        clock = [time.monotonic()]
        phases = []

        def display():
            phases.append(trader.phase)
            if trader.phase == "限速冷却":
                clock[0] = venue.budget.cooldown + 1
                venue.last_price_at = clock[0]
            if trader.counter == 1:
                trader.stop_event.set()

        trader.display = display
        with patch("Light_grid.time", SimpleNamespace(monotonic=lambda: clock[0])):
            self.assertEqual(await asyncio.wait_for(trader.run(), timeout=3), 0)
        self.assertIn("限速冷却", phases)
        self.assertEqual(venue.client.send_tx_batch.await_count, 2)
        self.assertEqual(trader.counter, 1)
        self.assertEqual(len(trader.All_orders), 1)
        venue.cancel_all_orders.assert_awaited_once()

    async def test_rate_limited_account_read_does_not_count_as_critical_failure(self):
        venue = submission_exchange()
        venue.budget.penalize()
        trader = GridTrader(venue, CONFIG)
        trader.switc = False
        trader.sync_task = asyncio.get_running_loop().create_future()
        trader.sync_task.set_exception(RateLimitError("HTTP 429"))
        trader.display = trader.stop_event.set
        venue.cancel_all_orders = AsyncMock()
        self.assertEqual(await trader.run(), 0)
        self.assertEqual(trader.critical_errors, 0)
        self.assertIsNone(trader.sync_task)

    async def test_known_cooldown_does_not_trigger_disconnect_timeout(self):
        venue = submission_exchange()
        venue.connected = False
        venue.budget.penalize()
        trader = GridTrader(venue, CONFIG)
        trader._unhealthy_since = time.monotonic() - 100
        trader.display = trader.stop_event.set
        venue.cancel_all_orders = AsyncMock()
        self.assertEqual(await trader.run(), 0)
        self.assertEqual(trader._unhealthy_since, venue.budget.cooldown)

    async def test_large_patch_splits_grid_even_when_quota_is_exhausted(self):
        venue = exchange()
        trader = GridTrader(venue, {**CONFIG, "gridLines": "60", "patch": "100000"})
        trader._next_sync = time.monotonic() + 30
        checks = []

        def available(kind):
            checks.append(kind)
            return 0 if len(checks) <= 2 else 40

        venue.budget.available = available
        chunks = []

        async def create(orders):
            chunks.append(len(orders))
            if sum(chunks) == trader.gridLines:
                trader.stop_event.set()

        venue.create_orders = create
        venue.cancel_all_orders = AsyncMock()
        self.assertEqual(await asyncio.wait_for(trader.run(), timeout=3), 0)
        self.assertEqual(chunks, [40, 20])
        self.assertGreaterEqual(len(checks), 4)

    async def test_run_builds_chunks_then_cancels_current_market_on_stop(self):
        venue = exchange()
        trader = GridTrader(venue, {**CONFIG, "patch": "2"})
        trader._next_sync = time.monotonic() + 30
        chunks = []

        async def create(orders):
            chunks.append(orders)
            for order in orders:
                venue.update_order(
                    raw_order(
                        order["id"],
                        price=str(order["price"]),
                        ask=order["side"] == "sell",
                    )
                )
            if sum(map(len, chunks)) == trader.gridLines:
                trader.stop_event.set()

        async def cancel():
            venue.orders.clear()

        venue.create_orders = create
        venue.cancel_all_orders = AsyncMock(side_effect=cancel)
        result = await asyncio.wait_for(trader.run(), timeout=3)
        self.assertEqual(result, 0)
        self.assertEqual([len(chunk) for chunk in chunks], [2, 2, 2])
        self.assertEqual(venue.cancel_all_orders.await_count, 2)

    async def test_exit_reports_fills_even_without_display_tick(self):
        venue = exchange()
        trader = GridTrader(venue, CONFIG)
        trader.stop_event.set()

        async def cancel():
            venue._strategy_order_ids.add("1")
            venue.update_order(raw_order("1", "filled", remaining="0"))

        venue.cancel_all_orders = cancel
        with patch("Light_grid.logger") as log:
            self.assertEqual(await trader.run(), 0)
        log.info.assert_called_with("本次完整成交 %s 笔", 1)

    async def test_disconnect_timeout_cancels_and_exits(self):
        venue = exchange()
        venue.connected = False
        trader = GridTrader(venue, CONFIG)
        trader._unhealthy_since = time.monotonic() - 11
        venue.cancel_all_orders = AsyncMock()
        result = await trader.run()
        self.assertEqual(result, 1)
        venue.cancel_all_orders.assert_awaited_once()


class StartupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            **CONFIG,
            "LIGHTER_ACCOUNT_INDEX": "123",
            "LIGHTER_API_KEY_INDEX": "4",
            "LIGHTER_API_PRIVATE_KEY": "FAKE_PRIVATE_KEY_FOR_OFFLINE_TEST",
            "SYMBOL": "INTC",
        }

    async def test_missing_configuration_lists_names_without_credentials(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("dotenv.load_dotenv"),
            patch("Light_grid.logger") as log,
            patch("logging.FileHandler") as file_log,
            patch.object(
                LighterExchange, "initialize", new_callable=AsyncMock
            ) as initialize,
        ):
            self.assertEqual(await main(check_config=True), 1)
        detail = log.error.call_args.args[2]
        self.assertIn("LIGHTER_ACCOUNT_INDEX", detail)
        self.assertIn("LIGHTER_API_PRIVATE_KEY", detail)
        self.assertIn("gridStep", detail)
        file_log.assert_not_called()
        initialize.assert_not_awaited()

    async def test_check_config_never_connects_or_creates_log(self):
        with (
            patch.dict(os.environ, self.config, clear=True),
            patch("dotenv.load_dotenv") as load,
            patch("Light_grid.logger"),
            patch("logging.FileHandler") as file_log,
            patch.object(
                LighterExchange, "initialize", new_callable=AsyncMock
            ) as initialize,
            patch.object(GridTrader, "run", new_callable=AsyncMock) as run,
        ):
            self.assertEqual(await main(check_config=True), 0)
        self.assertEqual(load.call_args.kwargs["encoding"], "utf-8-sig")
        file_log.assert_not_called()
        initialize.assert_not_awaited()
        run.assert_not_awaited()

    async def test_placeholders_are_reported_by_name(self):
        with (
            patch.dict(
                os.environ,
                {**self.config, "LIGHTER_API_PRIVATE_KEY": "YOUR_PRIVATE_KEY"},
                clear=True,
            ),
            patch("dotenv.load_dotenv"),
            patch("Light_grid.logger") as log,
        ):
            self.assertEqual(await main(check_config=True), 1)
        detail = log.error.call_args.args[2]
        self.assertIn("LIGHTER_API_PRIVATE_KEY", detail)
        self.assertNotIn("YOUR_PRIVATE_KEY", detail)

    async def test_sdk_key_error_reports_stage_and_field(self):
        with (
            patch.dict(os.environ, self.config, clear=True),
            patch("dotenv.load_dotenv"),
            patch("Light_grid.logger") as log,
            patch("logging.FileHandler", return_value=logging.NullHandler()),
            patch("signal.signal"),
            patch.object(
                LighterExchange,
                "initialize",
                new=AsyncMock(side_effect=KeyError("market_type")),
            ),
        ):
            self.assertEqual(await main(), 1)
        self.assertEqual(log.error.call_args.args[1], "初始化 SDK / 加载市场")
        self.assertIn("market_type", log.error.call_args.args[2])
        self.assertNotIn(self.config["LIGHTER_API_PRIVATE_KEY"], str(log.mock_calls))

    async def test_sensitive_value_is_redacted_from_configuration_error(self):
        with (
            patch.dict(os.environ, self.config, clear=True),
            patch("dotenv.load_dotenv"),
            patch("Light_grid.logger") as log,
            patch(
                "Light_grid.GridTrader",
                side_effect=ValueError(self.config["LIGHTER_API_PRIVATE_KEY"]),
            ),
        ):
            self.assertEqual(await main(check_config=True), 1)
        self.assertEqual(log.error.call_args.args[2], "<redacted>")

    async def test_utf8_bom_configuration_and_environment_precedence(self):
        with tempfile.TemporaryDirectory(prefix="lighter-config-") as folder:
            directory = Path(folder)
            config = {**self.config, "LIGHTER_API_KEY_INDEX": "0"}
            (directory / ".env").write_text(
                "\n".join(f"{key}={value}" for key, value in config.items()),
                encoding="utf-8-sig",
            )
            with (
                patch.dict(os.environ, {"LIGHTER_API_KEY_INDEX": "4"}, clear=True),
                patch("Light_grid.__file__", str(directory / "Light_grid.py")),
                patch("Light_grid.logger"),
                patch.object(
                    LighterExchange, "initialize", new_callable=AsyncMock
                ) as initialize,
            ):
                self.assertEqual(await main(check_config=True), 0)
            self.assertFalse((directory / "Light_grid.log").exists())
            initialize.assert_not_awaited()

    async def test_response_key_error_identifies_missing_field(self):
        class BrokenResponse:
            def to_dict(self):
                raise KeyError("orders")

        venue = exchange()
        with self.assertRaisesRegex(ExchangeError, "orders"):
            await venue.read(AsyncMock(return_value=BrokenResponse()))

    async def test_account_risk_cancels_without_placing(self):
        venue = exchange()
        venue.stats = {"portfolio_value": "10"}
        venue.positions = {
            "42": {
                "position": "1",
                "position_value": "60",
                "sign": 1,
                "unrealized_pnl": "0",
            }
        }
        trader = GridTrader(venue, CONFIG)
        trader._next_sync = time.monotonic() + 30
        venue.create_orders = AsyncMock()
        venue.cancel_all_orders = AsyncMock()
        result = await trader.run()
        self.assertEqual(result, 1)
        venue.create_orders.assert_not_awaited()
        venue.cancel_all_orders.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
