"""Intentionally naive Alpaca PAPER experiment. Run exactly one instance/account."""

import asyncio
import csv
import json
import logging
import math
import os
import random
import signal
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from dotenv import load_dotenv

SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "JPM", "AVGO", "LLY", "V",
    "MA", "XOM", "COST", "WMT", "HD", "PG", "JNJ", "ABBV", "BAC", "ORCL",
    "NFLX", "CRM", "AMD", "TSLA", "UNH", "KO", "PEP", "CSCO", "GE", "IBM",
]
LOG = logging.getLogger("paper_bot")
PAPER_URL = "https://paper-api.alpaca.markets"
TERMINAL = {"filled", "canceled", "expired", "rejected", "replaced"}
CSV_FIELDS = ["timestamp_utc", "symbol", "action", "previous_price", "current_price",
              "order_id", "notional_or_quantity", "status"]


class FatalError(RuntimeError):
    pass


def utcnow():
    return datetime.now(timezone.utc)


def value(enum):
    return getattr(enum, "value", enum)


@dataclass
class Config:
    key: str = field(repr=False)
    secret: str = field(repr=False)
    notional: float = 1000
    interval: float = 15
    cooldown: float = 20


def load_config():
    load_dotenv()
    key = os.getenv("ALPACA_API_KEY", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        raise FatalError("Supply ALPACA_API_KEY and ALPACA_SECRET_KEY (paper account only).")
    numbers = []
    for name, default in [("NOTIONAL_PER_TRADE", "1000"),
                          ("SIGNAL_INTERVAL_SECONDS", "15"),
                          ("ORDER_COOLDOWN_SECONDS", "20")]:
        try:
            number = float(os.getenv(name, default))
            if not math.isfinite(number) or number <= 0:
                raise ValueError()
        except ValueError:
            raise FatalError(f"{name} must be a finite positive number.") from None
        numbers.append(number)
    if not SYMBOLS or len(SYMBOLS) != len(set(SYMBOLS)):
        raise FatalError("SYMBOLS must be nonempty and unique.")
    return Config(key, secret, *numbers)


def make_client(config):
    client = TradingClient(config.key, config.secret, paper=True)
    # SDK 0.44.0 stores paper=True as _sandbox; check both flag and actual URL.
    if client._sandbox is not True or client._base_url != PAPER_URL:
        raise FatalError("Paper-mode verification failed; refusing to start.")
    client._retry = 0  # Never let SDK silently retry a POST after uncertainty.
    original_request = client._session.request

    def guarded_request(method, url, **kwargs):
        if not url.startswith(PAPER_URL + "/v2/"):
            raise FatalError("Blocked request outside the paper trading endpoint.")
        kwargs["timeout"] = (5, 15)
        kwargs["allow_redirects"] = False
        return original_request(method, url, **kwargs)

    client._session.request = guarded_request
    return client


def transient(exc):
    if isinstance(exc, APIError):
        return exc.status_code in (408, 429) or (exc.status_code or 0) >= 500
    # requests is already an alpaca-py dependency; avoid broad retries of code errors.
    from requests.exceptions import ConnectionError, Timeout
    return isinstance(exc, (ConnectionError, Timeout))


def read_api(stop, function, *args, **kwargs):
    """Bounded retries for read-only calls. Caller backs off again between cycles."""
    for attempt in range(4):
        if stop.is_set():
            raise InterruptedError("Stopping")
        try:
            return function(*args, **kwargs)
        except APIError as exc:
            if exc.status_code in (401, 403):
                raise FatalError(f"Alpaca authentication/permission error: {exc}") from exc
            if not transient(exc):
                raise
            error = exc
        except Exception as exc:
            if not transient(exc):
                raise
            error = exc
        if attempt == 3:
            raise error
        delay = min(30, 2 ** (attempt + 1)) + random.random()
        LOG.warning("API RETRY | %s | wait=%.1fs | %s", function.__name__, delay, error)
        stop.wait(delay)


class Prices:
    def __init__(self):
        self.lock = threading.Lock()
        self.latest = {}
        self.received = {}
        self.generation = 0

    async def on_trade(self, trade):
        price = Decimal(str(trade.price))
        if price.is_finite() and price > 0:
            with self.lock:
                self.latest[trade.symbol] = price
                self.received[trade.symbol] = time.monotonic()

    def clear(self):
        with self.lock:
            self.latest.clear()
            self.received.clear()
            self.generation += 1

    def snapshot(self):
        with self.lock:
            return self.latest.copy(), self.received.copy(), self.generation


class PaperStream(StockDataStream):
    """Small pinned-SDK hooks: stop permanent failures and reset reconnect baselines.

    SDK run() owns the sole socket and reconnects sequentially with 1–30s backoff.
    Its default error handler otherwise keeps retrying invalid credentials.
    """
    def __init__(self, config, prices, stop):
        super().__init__(config.key, config.secret, feed=DataFeed.IEX,
                         websocket_params={"ping_interval": 20, "ping_timeout": 20,
                                           "open_timeout": 15, "close_timeout": 5},
                         data_timeout=None)
        self.prices = prices
        self.stop_event = stop
        self.failed = False

    async def _start_ws(self):
        self.prices.clear()
        await asyncio.wait_for(super()._start_ws(), timeout=35)

    async def _auth(self):
        try:
            await asyncio.wait_for(super()._auth(), timeout=20)
        except ValueError as exc:
            message = str(exc).lower()
            if any(word in message for word in ("auth", "subscription", "forbidden", "invalid")):
                self.failed = True
                self.stop_event.set()
                await self.stop_ws()
            raise

    async def _dispatch(self, msg):
        if msg.get("T") == "error" and msg.get("code") in (400, 401, 402, 404, 405, 409, 410):
            LOG.error("STREAM CONFIGURATION ERROR | %s", msg)
            self.failed = True
            self.stop_event.set()
            await self.stop_ws()
        await super()._dispatch(msg)


@contextmanager
def single_instance():
    """OS lock is released on crash; persisted file is harmless."""
    handle = open("bot.lock", "a+b")
    try:
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise FatalError("Another bot uses this working directory.") from exc
        yield
    finally:
        handle.close()


class Journal:
    """Durable order intentions, not a position ledger. Write BEFORE submitting."""
    def __init__(self, account_id):
        self.path = Path("order_state.json")
        self.account_id = str(account_id)
        self.orders = {}
        if self.path.exists():
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data["account_id"] != self.account_id:
                raise FatalError("State belongs to another account; use a separate data directory.")
            self.orders = data["orders"]

    def save(self):
        temp = self.path.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump({"account_id": self.account_id, "orders": self.orders}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, self.path)


def write_trade(symbol, entry, status, order_id=""):
    path = Path("trades.csv")
    has_header = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if not has_header:
            writer.writeheader()
        writer.writerow(dict(timestamp_utc=utcnow().isoformat(), symbol=symbol,
                             action=entry["action"], previous_price=entry["previous"],
                             current_price=entry["current"], order_id=order_id,
                             notional_or_quantity=entry["amount"], status=status))
        handle.flush()
        os.fsync(handle.fileno())


def account_state(client, stop, journal, cooldown):
    """Look up local intentions first, then fetch positions after order states."""
    resolved = {}
    for symbol, entry in journal.orders.items():
        if entry.get("rejected"):
            resolved[symbol] = None
            continue
        try:
            resolved[symbol] = read_api(stop, client.get_order_by_client_id, entry["client_id"])
        except APIError as exc:
            if exc.status_code != 404:
                raise
            LOG.error("ORDER UNRESOLVED | %s | client_id=%s | symbol blocked; no resubmit",
                      symbol, entry["client_id"])
    open_orders = read_api(stop, client.get_orders, filter=GetOrdersRequest(
        status=QueryOrderStatus.OPEN, symbols=SYMBOLS, limit=500))
    recent = read_api(stop, client.get_orders, filter=GetOrdersRequest(
        status=QueryOrderStatus.ALL, symbols=SYMBOLS, limit=500,
        after=utcnow() - timedelta(seconds=cooldown)))
    if len(open_orders) == 500 or len(recent) == 500:
        raise FatalError("Order query hit 500-row limit; refusing an incomplete order snapshot.")
    positions = {p.symbol: p for p in read_api(stop, client.get_all_positions)}
    blocked = {o.symbol for o in open_orders + recent}
    for symbol, order in resolved.items():
        entry = journal.orders[symbol]
        if (utcnow() - datetime.fromisoformat(entry["sent_at"])).total_seconds() < cooldown:
            continue
        if order is not None:
            if value(order.status) not in TERMINAL or symbol in blocked:
                continue
            # Even terminal orders stay locked until the positions endpoint agrees.
            filled = Decimal(str(order.filled_qty or "0"))
            expected = Decimal(entry["start_qty"]) + (filled if entry["action"] == "BUY" else -filled)
            actual = Decimal(positions[symbol].qty) if symbol in positions else Decimal(0)
            if actual != expected:
                LOG.warning("POSITION RECONCILING | %s | expected=%s actual=%s", symbol, expected, actual)
                continue
            write_trade(symbol, entry, value(order.status), str(order.id))
        del journal.orders[symbol]
        journal.save()
    blocked.update(journal.orders)
    return positions, blocked


def send_order(client, stop, journal, config, symbol, previous, current, position):
    # Recheck the server clock immediately before each mutation. No POST retries.
    clock = read_api(stop, client.get_clock)
    if stop.is_set() or not clock.is_open:
        return
    # Avoid queuing a DAY order at the closing boundary during network latency.
    if (clock.next_close - clock.timestamp).total_seconds() <= 20:
        return
    quantity = Decimal(position.qty) if position else Decimal(0)
    if quantity < 0:
        return
    action = "SELL" if position else "BUY"
    client_id = "naive-" + uuid.uuid4().hex
    amount = str(quantity) if position else str(config.notional)
    request = MarketOrderRequest(
        symbol=symbol, qty=amount if position else None,
        notional=None if position else config.notional,
        side=OrderSide.SELL if position else OrderSide.BUY,
        time_in_force=TimeInForce.DAY, extended_hours=False, client_order_id=client_id)
    entry = dict(client_id=client_id, action=action, previous=str(previous),
                 current=str(current), amount=amount, start_qty=str(quantity),
                 sent_at=utcnow().isoformat())
    journal.orders[symbol] = entry
    journal.save()
    LOG.info("%s SIGNAL | %s | %s -> %s", action, symbol, previous, current)
    write_trade(symbol, entry, "submitting")
    try:
        order = client.submit_order(order_data=request)
    except Exception as exc:
        # Timeout/5xx/duplicate-ID responses may hide an accepted order. Preserve lock.
        if isinstance(exc, APIError) and exc.status_code in (400, 401, 403, 422, 429):
            if "duplicate" not in str(exc).lower() and "client_order_id" not in str(exc).lower():
                entry["rejected"] = True
                journal.save()
        write_trade(symbol, entry, "rejected" if entry.get("rejected") else "unknown")
        LOG.error("ORDER ERROR | %s | client_id=%s | %s", symbol, client_id, exc)
        if isinstance(exc, APIError) and exc.status_code in (401, 403):
            raise FatalError("Trading authentication/permission error; stopping.") from exc
        if transient(exc):
            raise  # Back off the whole cycle after a rate limit or outage.
        if not isinstance(exc, APIError):
            raise
        return
    # Refresh cooldown from response time (the request may have been slow).
    entry["sent_at"] = utcnow().isoformat()
    journal.save()
    write_trade(symbol, entry, value(order.status), str(order.id))
    LOG.info("%s ORDER SENT | %s | %s%s | order=%s", action, symbol,
             "$" if action == "BUY" else "qty=", amount, order.id)


def run_strategy(client, config, prices, stop, journal, stream):
    previous = {}
    generation = -1
    was_open = False
    last_closed_log = 0.0
    failures = 0
    deadline = time.monotonic()
    while not stop.is_set():
        try:
            clock = read_api(stop, client.get_clock)
            if not clock.is_open:
                previous.clear()
                was_open = False
                stream._data_timeout = None  # Quiet overnight is normal.
                if time.monotonic() - last_closed_log > 300 or not last_closed_log:
                    LOG.info("MARKET CLOSED | next_open=%s", clock.next_open.isoformat())
                    last_closed_log = time.monotonic()
                stop.wait(60)
                deadline = time.monotonic()
                continue
            stream._data_timeout = 120  # SDK reconnects a connected-but-silent feed.
            if not was_open:
                prices.clear()  # Require a new regular-session baseline.
                previous.clear()
                was_open = True
            positions, blocked = account_state(client, stop, journal, config.cooldown)
            latest, received, new_generation = prices.snapshot()
            if generation != new_generation:
                previous.clear()
                generation = new_generation
            for symbol in SYMBOLS:
                current = latest.get(symbol)
                # Data-health guard only; not an extra price/signal filter.
                if current is None or time.monotonic() - received[symbol] > 120:
                    previous.pop(symbol, None)
                    continue
                old = previous.get(symbol)
                previous[symbol] = current
                if old is None or symbol in blocked or stop.is_set():
                    continue
                position = positions.get(symbol)
                if position and Decimal(position.qty) <= 0:
                    LOG.warning("NON-LONG POSITION | %s | ignoring", symbol)
                    continue
                if (current > old and position is None) or (current < old and position is not None):
                    send_order(client, stop, journal, config, symbol, old, current, position)
            LOG.info("CYCLE COMPLETE | prices=%s/%s | positions=%s | blocked=%s",
                     len(previous), len(SYMBOLS), len(positions), len(blocked))
            failures = 0
        except InterruptedError:
            break
        except Exception as exc:
            if not transient(exc):
                raise
            failures += 1
            delay = min(60, 2 ** min(failures + 1, 6)) + random.random()
            LOG.error("CYCLE FAILED | backoff=%.1fs | %s", delay, exc)
            previous.clear()
            stop.wait(delay)
        # Fixed cadence; skip missed slots rather than burst after slow API calls.
        deadline += config.interval
        now = time.monotonic()
        if deadline < now:
            deadline += (math.floor((now - deadline) / config.interval) + 1) * config.interval
        stop.wait(max(0, deadline - time.monotonic()))


def main():
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ | %(levelname)s | %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        config = load_config()
        LOG.warning("PAPER TRADING MODE — NO LIVE ORDERS")
        with single_instance():
            client = make_client(config)
            while not stop.is_set():
                try:
                    account = read_api(stop, client.get_account)
                    break
                except Exception as exc:
                    if not transient(exc):
                        raise
                    LOG.error("STARTUP RETRY | %s", exc)
                    stop.wait(60)
            if stop.is_set():
                return 0
            if account.trading_blocked or account.account_blocked:
                raise FatalError("Paper account is blocked for trading.")
            LOG.info("ACCOUNT | equity=%s | cash=%s | buying_power=%s | symbols=%s | notional=%s | interval=%ss",
                     account.equity, account.cash, account.buying_power, len(SYMBOLS), config.notional, config.interval)
            journal = Journal(account.id)
            prices = Prices()
            stream = PaperStream(config, prices, stop)
            stream.subscribe_trades(prices.on_trade, *SYMBOLS)

            def stream_worker():
                try:
                    stream.run()
                except Exception:
                    stream.failed = True
                    LOG.exception("STREAM STOPPED unexpectedly")
                finally:
                    if not stop.is_set():
                        stream.failed = True
                    stop.set()

            worker = threading.Thread(target=stream_worker, name="iex-stream", daemon=True)
            worker.start()
            try:
                run_strategy(client, config, prices, stop, journal, stream)
            finally:
                stop.set()
                if stream._loop is not None and stream._loop.is_running():
                    stream.stop()
                worker.join(timeout=25)
                client._session.close()
                LOG.info("STOPPED | pending orders and positions remain in the paper account")
            return 1 if stream.failed else 0
    except InterruptedError:
        return 0
    except Exception:
        LOG.exception("FATAL | bot stopped")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
