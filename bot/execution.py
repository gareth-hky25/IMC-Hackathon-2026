"""Order execution: priority rate limiter, session-based HTTP overrides, order/position ledger with the
hard position guard, and the order manager that runs cancel/send batches, snipes and reconciles."""
from __future__ import annotations

import heapq
import itertools
import logging
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, fields, replace
from enum import IntEnum
from typing import Any, Callable

import requests

from models import OrderRequest, OrderResponse, Side

from .config import LoopParams, RiskParams

log = logging.getLogger(__name__)
order_log = logging.getLogger("bot.orders")

LIVE_STATUSES = ("ACTIVE", "PART_FILLED")
STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}


class Prio(IntEnum):
    """Token-bucket priority, highest first (spec section 2)."""

    SNIPE = 1
    URGENT = 2
    CANCEL = 3
    QUOTE = 4
    POLL = 5


# ----------------------------------------------------------------------------------------------
# Rate limiter
# ----------------------------------------------------------------------------------------------
class PriorityRateLimiter:
    """Token bucket (burst = rate) that serves waiting callers in priority order, then FIFO."""

    def __init__(self, rate: float) -> None:
        self._base_rate = rate
        self._tokens = rate
        self._burst = rate
        self._stamp = time.monotonic()
        self._penalty_until = 0.0
        self._cond = threading.Condition()
        self._heap: list[tuple[int, int]] = []
        self._seq = itertools.count()

    def _rate(self, now: float) -> float:
        return self._base_rate * (0.5 if now < self._penalty_until else 1.0)

    def _refill(self, now: float) -> None:
        self._tokens = min(self._burst, self._tokens + (now - self._stamp) * self._rate(now))
        self._stamp = now

    def penalize(self, duration: float = 30.0) -> None:
        """HTTP 429: halve the rate for `duration` seconds."""
        with self._cond:
            self._penalty_until = time.monotonic() + duration
            self._tokens = min(self._tokens, 0.0)
        log.warning("HTTP 429: request rate halved for %.0fs", duration)

    def acquire(self, prio: Prio, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        ticket = (int(prio), next(self._seq))
        with self._cond:
            heapq.heappush(self._heap, ticket)
            try:
                while True:
                    now = time.monotonic()
                    self._refill(now)
                    if self._heap[0] == ticket and self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return True
                    if now >= deadline:
                        return False
                    need = max(1.0 - self._tokens, 0.0) / self._rate(now)
                    self._cond.wait(min(deadline - now, max(need, 0.005)))
            finally:
                self._heap.remove(ticket)
                heapq.heapify(self._heap)
                self._cond.notify_all()


# ----------------------------------------------------------------------------------------------
# HTTP overrides
# ----------------------------------------------------------------------------------------------
def parse_order_response(data: Any) -> OrderResponse:
    """Tolerant OrderResponse parse: unknown keys are dropped; missing required fields raise ValueError."""
    if not isinstance(data, dict):
        raise ValueError(f"order response is not an object: {str(data)[:120]}")
    names = {f.name for f in fields(OrderResponse)}
    try:
        return OrderResponse(**{k: v for k, v in data.items() if k in names})
    except TypeError as exc:
        raise ValueError(f"unparseable order response {str(data)[:200]}: {exc}") from exc


class SessionApiMixin:
    """Mixin for BaseBot overriding the REST calls we use with the same endpoints and payloads, but through a
    thread-local `requests.Session` (connection reuse) with a timeout. Place it before BaseBot in the bases.

    Network exceptions propagate to the caller (the OrderManager reconciles on them); a non-200 reply returns
    None as in the base class. `status_hook` is told every non-200 status (used for 429 backoff).
    """

    http_timeout: float = 2.0
    dry_run: bool = False  # gate for every state-changing call: log instead of sending
    status_hook: Callable[[int], None] | None = None
    _dry_ids = itertools.count(1)
    _cmi_url: str
    _tls = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._tls, "session", None)
        if session is None:
            session = self._tls.session = requests.Session()
        return session

    def _call(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        if self.dry_run and method != "GET":  # backstop: no write can leave the process in a dry run
            raise RuntimeError(f"dry run: refusing {method} {path}")
        resp = self._session().request(
            method,
            f"{self._cmi_url}{path}",
            headers=self._get_headers(),  # type: ignore[attr-defined]
            timeout=self.http_timeout,
            **kwargs,
        )
        if resp.status_code != 200 and self.status_hook is not None:
            self.status_hook(resp.status_code)
        return resp

    def send_order(self, order_request: OrderRequest) -> OrderResponse | None:
        if order_request.volume <= 0:
            raise ValueError("Order volume must be greater than 0")
        if self.dry_run:
            order_log.info("DRY-RUN would SEND %s %s %s @ %s", order_request.product, order_request.side, order_request.volume, order_request.price)
            return OrderResponse(f"dry-{next(self._dry_ids)}", "ACTIVE", order_request.product, order_request.side,  # type: ignore[arg-type]
                                 order_request.price, order_request.volume, 0, "dry-run", "")
        payload = {k: v for k, v in asdict(order_request).items() if v is not None}
        resp = self._call("POST", "/api/order", json=payload)
        if resp.status_code == 200:
            return parse_order_response(resp.json())
        order_log.warning("send_order failed %s -> HTTP %d %s", order_request, resp.status_code, resp.content[:200])
        return None

    def cancel_order_by_id(self, order_id: str) -> dict | None:
        if self.dry_run:
            order_log.info("DRY-RUN would CANCEL %s", order_id)
            return {}
        resp = self._call("DELETE", f"/api/order/{order_id}")
        if resp.status_code == 200:
            return resp.json()
        order_log.warning("cancel %s failed -> HTTP %d %s", order_id, resp.status_code, resp.content[:200])
        return None

    def cancel_order(self, product: str, price: float) -> dict | None:
        if self.dry_run:
            order_log.info("DRY-RUN would CANCEL %s @ %s", product, price)
            return {}
        return super().cancel_order(product, price)  # type: ignore[misc]

    def cancel_all_orders(self) -> None:
        if self.dry_run:
            order_log.info("DRY-RUN would CANCEL ALL")
            return
        super().cancel_all_orders()  # type: ignore[misc]

    def request_all_orders(self) -> list[dict] | None:
        resp = self._call("GET", "/api/order/current-user")
        if resp.status_code == 200:
            return resp.json()
        order_log.warning("request_all_orders failed -> HTTP %d", resp.status_code)
        return None

    def request_net_positions(self) -> dict[str, int] | None:
        resp = self._call("GET", "/api/position/current-user")
        if resp.status_code == 200:
            return {p["product"]: p.get("netPosition", p.get("net_position", p.get("volume", 0))) for p in resp.json()}
        order_log.warning("request_net_positions failed -> HTTP %d", resp.status_code)
        return None


# ----------------------------------------------------------------------------------------------
# Ledger: order table, positions, reservations, hard guard
# ----------------------------------------------------------------------------------------------
@dataclass
class LiveOrder:
    id: str
    product: str
    side: Side
    price: float
    volume: int
    remaining: int
    created: float  # monotonic
    canceling: bool = False
    kind: str = "quote"  # "quote" | "stub" | "snipe"


@dataclass(frozen=True)
class NewOrder:
    product: str
    side: Side
    price: float
    volume: int
    lim: int  # position limit the guard checks this order against
    kind: str = "quote"


@dataclass
class Reservation:
    rid: int
    product: str
    side: Side
    volume: int
    held: bool = False  # kept after an exception/timeout until the next reconcile completes


@dataclass(frozen=True)
class FillResult:
    is_new: bool
    passive_volume: int  # part of the fill that matched our resting orders


class Ledger:
    """Everything the hard position guard needs, under one lock (spec section 2).

    pos_est follows our own SSE fills between polls. A fill that the order response already reported
    (`credit_response_fill`) is not applied a second time when its SSE trade arrives.
    """

    CREDIT_TTL_S = 3.0

    def __init__(self, products: list[str]) -> None:
        self._lock = threading.RLock()
        self._orders: dict[str, LiveOrder] = {}
        self._res: dict[int, Reservation] = {}
        self._rid = itertools.count(1)
        self._last_rid = 0
        self.pos_polled = {p: 0 for p in products}
        self.pos_est = {p: 0 for p in products}
        self.pos_start: dict[str, int] = {}
        self.cash_px = {p: 0.0 for p in products}
        self._seen: set[str] = set()
        self._seen_q: deque[str] = deque()
        self._credit: dict[tuple[str, Side], list[list[float]]] = {}

    # -- positions -------------------------------------------------------------------------
    def set_polled(self, product: str, pos: int) -> None:
        with self._lock:
            self.pos_polled[product] = pos
            self.pos_est[product] = pos
            self.pos_start.setdefault(product, pos)

    def positions(self, product: str) -> tuple[int, int]:
        """(pos_polled, pos_est)."""
        with self._lock:
            return self.pos_polled[product], self.pos_est[product]

    # -- guard -----------------------------------------------------------------------------
    def _caps(self, product: str, side: Side, lim: int, own_book_vol: int, passive: bool) -> int:
        """Hard-guard capacity. Passive quotes (checked against the soft limit) ignore resting stubs."""
        polled, est = self.pos_polled[product], self.pos_est[product]
        mine = [o for o in self._orders.values() if o.product == product and o.side == side]
        stub_vol = sum(o.remaining for o in mine if o.kind == "stub") if passive else 0
        open_vol = max(sum(o.remaining for o in mine) - stub_vol, own_book_vol - stub_vol)
        inflight = sum(r.volume for r in self._res.values() if r.product == product and r.side == side)
        if side == Side.BUY:
            return lim - max(polled, est) - open_vol - inflight
        return lim + min(polled, est) - open_vol - inflight

    def capacity(self, product: str, side: Side, lim: int, own_book_vol: int, passive: bool = False) -> int:
        with self._lock:
            return max(0, self._caps(product, side, lim, own_book_vol, passive))

    def reserve(self, product: str, side: Side, volume: int, lim: int, own_book_vol: int, passive: bool) -> Reservation | None:
        """Atomically reserve min(volume, capacity); None if nothing fits."""
        with self._lock:
            granted = min(int(volume), self._caps(product, side, lim, own_book_vol, passive))
            if granted <= 0:
                return None
            res = Reservation(next(self._rid), product, side, granted)
            self._last_rid = res.rid
            self._res[res.rid] = res
            return res

    def release(self, res: Reservation) -> None:
        with self._lock:
            self._res.pop(res.rid, None)

    def hold(self, res: Reservation) -> None:
        with self._lock:
            res.held = True

    def release_held(self, upto_rid: int) -> None:
        with self._lock:
            for rid in [r for r, v in self._res.items() if v.held and r <= upto_rid]:
                del self._res[rid]

    def last_rid(self) -> int:
        with self._lock:
            return self._last_rid

    def ack(self, res: Reservation, order: LiveOrder | None) -> None:
        """Order answered: drop the reservation and record the resting remainder in one step."""
        with self._lock:
            self._res.pop(res.rid, None)
            if order is not None:
                self._orders[order.id] = order

    # -- order table -----------------------------------------------------------------------
    def live_orders(self, product: str) -> list[LiveOrder]:
        with self._lock:
            return [replace(o) for o in self._orders.values() if o.product == product]

    def order_ids(self) -> list[str]:
        with self._lock:
            return list(self._orders)

    def mark_canceling(self, ids: list[str], flag: bool = True) -> None:
        with self._lock:
            for oid in ids:
                if oid in self._orders:
                    self._orders[oid].canceling = flag

    def remove_order(self, oid: str) -> None:
        with self._lock:
            self._orders.pop(oid, None)

    def replace_orders(self, rows: list[LiveOrder], since_mono: float) -> None:
        """Adopt the exchange's view; keep orders acked after the fetch began (they may be missing from it)."""
        with self._lock:
            fresh = {o.id: o for o in rows}
            for oid, o in self._orders.items():
                if oid not in fresh and o.created >= since_mono:
                    fresh[oid] = o
                elif oid in fresh:
                    fresh[oid].canceling = o.canceling
                    fresh[oid].created = o.created
                    fresh[oid].kind = o.kind
            self._orders = fresh

    # -- fills -----------------------------------------------------------------------------
    def credit_response_fill(self, product: str, side: Side, vol: int) -> None:
        """An order response already reported `vol` filled: apply it now and skip it on the SSE feed."""
        if vol <= 0:
            return
        with self._lock:
            signed = vol if side == Side.BUY else -vol
            self.pos_est[product] += signed
            self._credit.setdefault((product, side), []).append([vol, time.monotonic() + self.CREDIT_TTL_S])

    def apply_fill(self, product: str, side: Side, price: float, vol: int, key: str) -> FillResult:
        with self._lock:
            if key in self._seen:
                return FillResult(False, 0)
            self._seen.add(key)
            self._seen_q.append(key)
            if len(self._seen_q) > 20000:
                self._seen.discard(self._seen_q.popleft())
            signed = vol if side == Side.BUY else -vol
            self.cash_px[product] -= signed * price
            skip = 0
            now = time.monotonic()
            credits = [c for c in self._credit.get((product, side), []) if c[1] > now and c[0] > 0]
            for c in credits:
                take = min(c[0], vol - skip)
                c[0] -= take
                skip += int(take)
            self._credit[(product, side)] = [c for c in credits if c[0] > 0]
            self.pos_est[product] += (vol - skip) if side == Side.BUY else -(vol - skip)
            passive = 0
            resting = sorted(
                (o for o in self._orders.values() if o.product == product and o.side == side and o.price == price),
                key=lambda o: o.created,
            )
            for o in resting:
                take = min(o.remaining, vol - passive)
                o.remaining -= take
                passive += take
                if o.remaining <= 0:
                    del self._orders[o.id]
                if passive >= vol:
                    break
            return FillResult(True, passive)


# ----------------------------------------------------------------------------------------------
# Order manager
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class SnipeJob:
    product: str
    side: Side
    price: float
    volume: int
    lim: int  # position limit for the guard (hard limit L, or the opening-phase q_open)
    cancel_ids: tuple[str, ...] = ()  # same-side passive orders to cancel first ("making room")
    reason: str = ""


class OrderManager:
    """Runs all REST order traffic off the strategy/SSE threads.

    Thread pools: `io` for leaf HTTP calls, `coord` for batch coordinators (never the same pool, so a batch
    cannot deadlock waiting on its own leaves) and `snipe` as the dedicated high-priority snipe workers.
    """

    def __init__(
        self,
        api: Any,
        ledger: Ledger,
        limiter: PriorityRateLimiter,
        loop: LoopParams,
        risk: RiskParams,
        own_book_vol: Callable[[str, Side], int],
        symbols: list[str],
    ) -> None:
        self._api = api
        self.ledger = ledger
        self._limiter = limiter
        self._risk = risk
        self._own_book_vol = own_book_vol
        self._symbols = symbols
        self._io = ThreadPoolExecutor(loop.workers, thread_name_prefix="io")
        self._coord = ThreadPoolExecutor(6, thread_name_prefix="coord")
        self._snipe = ThreadPoolExecutor(2, thread_name_prefix="snipe")
        self._lock = threading.Lock()
        self._batch_inflight: set[str] = set()
        self._failures: deque[float] = deque()
        self._paused_until = 0.0
        self._tripping = False
        self._rec_lock = threading.Lock()
        self._rec_again = False
        api.status_hook = self._on_http_status

    def shutdown(self) -> None:
        for pool in (self._snipe, self._coord, self._io):
            pool.shutdown(wait=False, cancel_futures=True)

    def _on_http_status(self, status: int) -> None:
        if status == 429:
            self._limiter.penalize()

    # -- state -----------------------------------------------------------------------------
    def is_paused(self) -> bool:
        return time.monotonic() < self._paused_until

    def batch_in_flight(self, product: str) -> bool:
        with self._lock:
            return product in self._batch_inflight

    # -- failures --------------------------------------------------------------------------
    def _record_failure(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._failures.append(now)
            while self._failures and now - self._failures[0] > self._risk.fail_window_s:
                self._failures.popleft()
            trip = len(self._failures) >= self._risk.fail_n and not self._tripping
            if trip:
                self._tripping = True
        if trip:
            log.error("%d order failures in %.0fs: cancel all, pausing %.0fs", self._risk.fail_n, self._risk.fail_window_s, self._risk.pause_s)
            self._coord.submit(self._trip)

    def _trip(self) -> None:
        try:
            self._paused_until = time.monotonic() + self._risk.pause_s
            self.cancel_all_blocking()
            time.sleep(self._risk.pause_s)
            self.reconcile()
            self.poll_positions()
        except Exception:
            log.exception("kill/resync failed")
        finally:
            with self._lock:
                self._failures.clear()
                self._tripping = False

    # -- leaf calls ------------------------------------------------------------------------
    def _cancel_leaf(self, oid: str, prio: Prio) -> bool:
        if not self._limiter.acquire(prio, 5.0):
            self.ledger.mark_canceling([oid], False)
            return False
        try:
            reply = self._api.cancel_order_by_id(oid)
        except Exception as exc:
            order_log.warning("cancel %s raised %s: %s", oid, type(exc).__name__, exc)
            self.ledger.mark_canceling([oid], False)
            self.request_reconcile()
            return False
        if reply is None:
            self.ledger.mark_canceling([oid], False)
            self.request_reconcile()
            return False
        self.ledger.remove_order(oid)
        order_log.info("cancelled %s", oid)
        return True

    def _send_leaf(self, order: NewOrder, prio: Prio, tag: str) -> OrderResponse | None:
        res = self.ledger.reserve(order.product, order.side, order.volume, order.lim, self._own_book_vol(order.product, order.side), order.kind == "quote")
        if res is None:
            order_log.info("%s %s %s %s@%s skipped: no guard capacity", tag, order.product, order.side, order.volume, order.price)
            return None
        if not self._limiter.acquire(prio, 5.0):
            self.ledger.release(res)
            order_log.warning("%s send dropped: rate limiter timeout", tag)
            return None
        request = OrderRequest(order.product, order.side, order.price, res.volume)
        try:
            resp = self._api.send_order(request)
        except Exception as exc:
            order_log.warning("%s send raised %s: %s; reservation held until reconcile", tag, type(exc).__name__, exc)
            self.ledger.hold(res)
            self._record_failure()
            self.request_reconcile()
            return None
        if resp is None:
            self.ledger.release(res)
            self._record_failure()
            return None
        try:
            remaining = int(round(resp.volume - resp.filled))
            live = remaining > 0 and str(resp.status) in LIVE_STATUSES
            row = LiveOrder(resp.id, resp.product, Side(resp.side), float(resp.price), int(round(resp.volume)), remaining, time.monotonic(), kind=order.kind) if live else None
            filled = int(round(resp.filled))
        except (TypeError, ValueError) as exc:
            order_log.warning("%s response unusable (%s): %s; reservation held until reconcile", tag, exc, resp)
            self.ledger.hold(res)
            self.request_reconcile()
            return None
        self.ledger.ack(res, row)
        self.ledger.credit_response_fill(order.product, order.side, filled)
        order_log.info("%s sent %s %s %s@%s -> id=%s status=%s filled=%s", tag, order.product, order.side, res.volume, order.price, resp.id, resp.status, resp.filled)
        return resp

    # -- batches ---------------------------------------------------------------------------
    def submit_batch(self, product: str, cancel_ids: list[str], news: list[NewOrder]) -> bool:
        """Cancel (parallel, waited) then send (parallel). False if a batch for the product is already in flight."""
        with self._lock:
            if product in self._batch_inflight:
                return False
            self._batch_inflight.add(product)
        self.ledger.mark_canceling(cancel_ids)
        self._coord.submit(self._run_batch, product, cancel_ids, news)
        return True

    def _run_batch(self, product: str, cancel_ids: list[str], news: list[NewOrder]) -> None:
        try:
            cancels = [self._io.submit(self._cancel_leaf, oid, Prio.CANCEL) for oid in cancel_ids]
            for fut in cancels:
                fut.result()
            if self.is_paused():
                return
            sends = [self._io.submit(self._send_leaf, o, Prio.QUOTE, o.kind) for o in news]
            for fut in sends:
                fut.result()
        except Exception:
            log.exception("batch for %s failed", product)
            self.request_reconcile()
        finally:
            with self._lock:
                self._batch_inflight.discard(product)

    def cancel_urgent(self, ids: list[str]) -> None:
        """Urgent / kill cancels: not subject to the batch-in-flight or interval limits."""
        if not ids:
            return
        self.ledger.mark_canceling(ids)
        for oid in ids:
            self._io.submit(self._cancel_leaf, oid, Prio.URGENT)

    def cancel_all_blocking(self) -> None:
        """Kill switch: cancel every order we know of, in parallel by id."""
        ids = set(self.ledger.order_ids())
        try:
            for row in self._api.request_all_orders() or []:
                if isinstance(row, dict) and row.get("id"):
                    ids.add(str(row["id"]))
        except Exception as exc:
            log.warning("cancel_all: could not list orders: %s", exc)
        self.ledger.mark_canceling(list(ids))
        futs: list[Future] = [self._io.submit(self._cancel_leaf, oid, Prio.URGENT) for oid in ids]
        for fut in futs:
            fut.result()
        log.info("cancel_all: %d orders cancelled", len(ids))

    # -- snipes ----------------------------------------------------------------------------
    def submit_snipe(self, job: SnipeJob) -> None:
        """Non-blocking hand-off to the snipe workers (safe to call from the SSE thread)."""
        self._snipe.submit(self._run_snipe, job)

    def _run_snipe(self, job: SnipeJob) -> None:
        try:
            if job.cancel_ids:
                self.ledger.mark_canceling(list(job.cancel_ids))
                for fut in [self._io.submit(self._cancel_leaf, oid, Prio.SNIPE) for oid in job.cancel_ids]:
                    fut.result()
            order = NewOrder(job.product, job.side, job.price, job.volume, job.lim, "snipe")
            resp = self._send_leaf(order, Prio.SNIPE, f"SNIPE[{job.reason}]")
            if resp is None:
                return
            if resp.volume - resp.filled > 0 and str(resp.status) in LIVE_STATUSES:
                # IOC emulation: cancel whatever rests.
                if not self._cancel_leaf(resp.id, Prio.SNIPE):
                    self.request_reconcile()
        except Exception:
            log.exception("snipe failed")
            self.request_reconcile()

    # -- reconcile / polls -----------------------------------------------------------------
    def request_reconcile(self) -> None:
        self._coord.submit(self.reconcile)

    def reconcile(self) -> None:
        """Replace the order table with `request_all_orders()`; single flight, coalescing repeats."""
        if getattr(self._api, "dry_run", False):
            return  # dry-run orders exist only locally
        if not self._rec_lock.acquire(blocking=False):
            self._rec_again = True
            return
        try:
            while True:
                self._rec_again = False
                since = time.monotonic()
                upto = self.ledger.last_rid()
                if not self._limiter.acquire(Prio.POLL, 3.0):
                    return
                try:
                    rows = self._api.request_all_orders()
                except Exception as exc:
                    log.warning("reconcile failed: %s: %s", type(exc).__name__, exc)
                    return
                if rows is None:
                    return
                self.ledger.replace_orders(self._parse_rows(rows), since)
                self.ledger.release_held(upto)
                if not self._rec_again:
                    return
        finally:
            self._rec_lock.release()

    @staticmethod
    def _parse_rows(rows: list[dict]) -> list[LiveOrder]:
        out: list[LiveOrder] = []
        for row in rows:
            try:
                status = str(row.get("status", "ACTIVE"))
                volume = int(round(float(row["volume"])))
                remaining = volume - int(round(float(row.get("filled", 0))))
                if remaining <= 0 or status not in LIVE_STATUSES:
                    continue
                out.append(LiveOrder(str(row["id"]), str(row["product"]), Side(row["side"]), float(row["price"]), volume, remaining, 0.0))
            except (KeyError, TypeError, ValueError) as exc:
                log.warning("reconcile: skipping malformed order row %s (%s)", str(row)[:150], exc)
        return out

    def poll_positions(self) -> dict[str, int] | None:
        if not self._limiter.acquire(Prio.POLL, 2.0):
            return None
        try:
            net = self._api.request_net_positions()
        except Exception as exc:
            log.warning("position poll failed: %s: %s", type(exc).__name__, exc)
            return None
        if net is None:
            return None
        for symbol in self._symbols:
            self.ledger.set_polled(symbol, int(net.get(symbol, 0)))
        return net
