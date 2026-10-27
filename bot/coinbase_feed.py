"""Coinbase BTC-USD feed: websocket thread, shared state, REST helper (seeding, backfill)."""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, NamedTuple

import requests
from websockets.sync.client import connect

from .config import CB_PRODUCT_ID, CbParams, parse_utc
from .models_fv import (
    ExternalSignals,
    RateEstimator,
    Timing,
    VolEstimator,
    compute_timing,
    PHASE_POST,
    PHASE_PRE,
    sigma2_from_candles,
)

log = logging.getLogger(__name__)

CB_WS_URL = "wss://ws-feed.exchange.coinbase.com"
CB_REST_URL = "https://api.exchange.coinbase.com"
PRODUCT_ID = CB_PRODUCT_ID
MAX_BACKFILL_PAGES = 25


class CbTrade(NamedTuple):
    ts: float
    id: int
    price: float


def _parse_trade(raw: dict[str, Any]) -> CbTrade | None:
    """Parse a websocket match or a REST trade row; None when malformed."""
    try:
        tid = raw.get("trade_id")
        ts = parse_utc(raw.get("time"))
        if tid is None or ts is None:
            return None
        return CbTrade(ts, int(tid), float(raw["price"]))
    except (KeyError, TypeError, ValueError):
        return None


# ----------------------------------------------------------------------------------------------
# Shared state
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class CbSnapshot:
    """Consistent copy of everything the strategy needs from Coinbase."""

    timing: Timing
    window: tuple[float, float] | None
    connected: bool
    ever_live: bool
    stale: bool
    ready: bool  # connected, past the resume-heartbeat count
    gap_pending: bool  # a Range backfill is queued or running
    final: bool  # a message at/after the window end was seen: n_obs / range are exact
    t_live: float
    last_price: float | None
    n_obs: int | None  # None while unknown (e.g. mid-window start before the REST seed)
    range_now: float | None
    m_max: float | None
    m_min: float | None
    elapsed: float  # seconds since the window start (0 before it)
    lam_hat: float
    lam0: float  # trades/s prior (seasonal + regime blend)
    lam_regime: float | None
    lam_season: float | None
    lam_fast: float | None
    c_inf_eff: float
    lam_burst: float | None  # 5 s-half-life EWMA of the heartbeat trade rate
    sigma_hat: float
    sigma_anchor: float
    sigma_fast: float
    sigma_regime: float | None
    sigma_season: float | None


class CoinbaseState:
    """Thread-safe Coinbase state. The WS thread writes; the strategy reads `snapshot()`.

    Window membership uses Coinbase message times. Matches and heartbeats are kept in rolling buffers so
    L_start, M and m can be (re)computed retroactively whenever the window start moves.
    """

    def __init__(
        self,
        rate: RateEstimator,
        vol: VolEstimator,
        cb: CbParams,
        default_len_s: float,
        c_event_mult: float = 1.0,
        regime_anchor: tuple[float, int] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._rate = rate
        self._vol = vol
        self._cb = cb
        self._default_len = default_len_s
        self._c_event_mult = c_event_mult
        self._matches: deque[CbTrade] = deque(maxlen=150_000)
        self._hbs: deque[tuple[float, int]] = deque(maxlen=4_000)
        self._win: tuple[float, float] | None = None
        self._l_start: int | None = None
        self._l_end: int | None = None
        self._hi: float | None = None
        self._lo: float | None = None
        self._end_seen = False
        self._last_id: int | None = None
        self._last_price: float | None = None
        self._prev_hb: tuple[float, int] | None = None
        self._first_hb_ts: float | None = None
        self._covered_from: float | None = None
        self._connected = False
        self._ever_live = False
        self._hb_since_connect = 0
        self._last_msg_mono: float | None = None
        self._gaps: deque[tuple[int, int]] = deque()
        self._gap_running = 0
        self._stat_count = 0
        self._stat_first_id: int | None = None
        self._stat_last_id: int | None = None
        self._skew_s = 0.0
        self._last_ts: float | None = None
        self._lam_burst: float | None = None
        self._regime_anchor = regime_anchor  # (time, trade_id) 30 min before the reference time

    # -- connection bookkeeping -------------------------------------------------------------
    def on_connect(self) -> None:
        with self._lock:
            self._connected = True
            self._hb_since_connect = 0

    def on_disconnect(self) -> None:
        with self._lock:
            self._connected = False

    def touch(self) -> None:
        with self._lock:
            self._last_msg_mono = time.monotonic()

    def idle_for(self) -> float:
        with self._lock:
            return math.inf if self._last_msg_mono is None else time.monotonic() - self._last_msg_mono

    # -- priors ----------------------------------------------------------------------------
    def set_regime_anchor(self, t_then: float, id_then: int) -> None:
        with self._lock:
            self._regime_anchor = (t_then, id_then)

    def _lam_regime(self) -> float | None:
        """Rolling (pre-window) or frozen-at-start (in-window) 30-minute trade rate from trade ids."""
        if self._regime_anchor is None:
            return None
        t_then, id_then = self._regime_anchor
        if self._win is not None and self._l_start is not None and self._last_ts is not None and self._last_ts >= self._win[0]:
            t_ref, id_ref = self._win[0], self._l_start
        elif self._last_id is not None and self._last_ts is not None:
            t_ref, id_ref = self._last_ts, self._last_id
        else:
            return None
        if t_ref - t_then < 60.0 or id_ref <= id_then:
            return None
        return (id_ref - id_then) / (t_ref - t_then)

    # -- window ----------------------------------------------------------------------------
    def set_window(self, start: float, end: float) -> bool:
        """Set the window and recompute L_start / L_end / M / m from the buffers.

        Returns True when the buffers do not reach back to `start` and a REST history seed is needed.
        """
        with self._lock:
            self._win = (start, end)
            self._vol.set_window_start(start)
            self._l_start = self._l_end = self._hi = self._lo = None
            self._end_seen = False
            for m in self._matches:
                self._apply_match(m)
            for ts, lid in self._hbs:
                self._apply_hb(ts, lid)
            return start <= time.time() and (
                self._covered_from is None or self._covered_from > start or self._l_start is None
            )

    def first_heartbeat_ts(self) -> float | None:
        with self._lock:
            return self._first_hb_ts

    def _apply_match(self, m: CbTrade) -> None:
        if self._win is None:
            return
        start, end = self._win
        if m.ts < start:
            self._l_start = m.id if self._l_start is None else max(self._l_start, m.id)
            self._l_end = m.id if self._l_end is None else max(self._l_end, m.id)
        elif m.ts < end:
            self._l_end = m.id if self._l_end is None else max(self._l_end, m.id)
            self._hi = m.price if self._hi is None else max(self._hi, m.price)
            self._lo = m.price if self._lo is None else min(self._lo, m.price)
        else:
            self._end_seen = True

    def _apply_hb(self, ts: float, last_id: int) -> None:
        if self._win is None:
            return
        start, end = self._win
        if ts < start:
            self._l_start = last_id if self._l_start is None else max(self._l_start, last_id)
        if ts < end:
            self._l_end = last_id if self._l_end is None else max(self._l_end, last_id)
        else:
            self._end_seen = True

    # -- ingestion --------------------------------------------------------------------------
    def on_match(self, msg: dict[str, Any]) -> None:
        trade = _parse_trade(msg)
        if trade is None:
            log.warning("malformed match message ignored: %s", str(msg)[:200])
            return
        is_last_match = msg.get("type") == "last_match"
        with self._lock:
            if self._covered_from is None or trade.ts < self._covered_from:
                self._covered_from = trade.ts
            fresh = self._last_id is None or trade.id > self._last_id
            if self._last_id is not None and trade.id > self._last_id + 1:
                self._gaps.append((self._last_id, trade.id))
                log.warning("match gap: ids %d..%d missing; backfill queued", self._last_id + 1, trade.id - 1)
            if fresh:
                self._last_id, self._last_price = trade.id, trade.price
                self._last_ts = trade.ts
                if not is_last_match:  # the replayed last match is stale: do not feed the bars with it
                    self._vol.on_trade(trade.ts, trade.price)
                    self._stat_count += 1
                    if self._stat_first_id is None:
                        self._stat_first_id = trade.id
                    self._stat_last_id = trade.id
            self._matches.append(trade)
            self._apply_match(trade)

    def on_heartbeat(self, msg: dict[str, Any]) -> None:
        ts = parse_utc(msg.get("time"))
        try:
            last_id = int(msg["last_trade_id"])
        except (KeyError, TypeError, ValueError):
            last_id = None
        if ts is None or last_id is None:
            log.warning("malformed heartbeat ignored: %s", str(msg)[:200])
            return
        with self._lock:
            if self._covered_from is None or ts < self._covered_from:
                self._covered_from = ts
            if self._first_hb_ts is None:
                self._first_hb_ts = ts
            if self._prev_hb is not None:
                lam0, _ = self._rate.lam0(self._lam_regime(), self._default_len, False)
                self._rate.on_interval(last_id - self._prev_hb[1], ts - self._prev_hb[0], lam0)
                dt = ts - self._prev_hb[0]
                if dt > 0 and last_id >= self._prev_hb[1]:
                    x = (last_id - self._prev_hb[1]) / dt
                    prev = x if self._lam_burst is None else self._lam_burst
                    self._lam_burst = prev + (1.0 - 2.0 ** (-dt / 5.0)) * (x - prev)
            if self._last_ts is None or ts > self._last_ts:
                self._last_ts = ts
            self._prev_hb = (ts, last_id)
            self._hbs.append((ts, last_id))
            self._apply_hb(ts, last_id)
            self._skew_s = ts - time.time()
            self._hb_since_connect += 1
            if self._hb_since_connect >= self._cb.resume_heartbeats:
                self._ever_live = True

    def ingest_history(self, trades: list[CbTrade]) -> None:
        """Add REST trades (backfill / mid-window seed). Max/min are idempotent so overlap is harmless."""
        if not trades:
            return
        with self._lock:
            for t in sorted(trades, key=lambda x: x.id):
                self._matches.append(t)
                self._apply_match(t)
                self._vol.on_trade(t.ts, t.price)
            oldest = min(t.ts for t in trades)
            if self._covered_from is None or oldest < self._covered_from:
                self._covered_from = oldest
            newest = max(trades, key=lambda t: t.id)
            if self._last_id is None or newest.id > self._last_id:
                self._last_id, self._last_price = newest.id, newest.price

    # -- gaps ------------------------------------------------------------------------------
    def take_gap(self) -> tuple[int, int] | None:
        with self._lock:
            if not self._gaps:
                return None
            self._gap_running += 1
            return self._gaps.popleft()

    def gap_done(self) -> None:
        with self._lock:
            self._gap_running -= 1

    # -- stats / reads ---------------------------------------------------------------------
    def pop_minute_stats(self) -> tuple[int, int, float]:
        """(matches received, id span, heartbeat-vs-local clock skew in s) since the last call."""
        with self._lock:
            span = 0 if self._stat_first_id is None else (self._stat_last_id or 0) - self._stat_first_id + 1
            out = (self._stat_count, span, self._skew_s)
            self._stat_count, self._stat_first_id, self._stat_last_id = 0, None, None
            return out

    def last_price(self) -> float | None:
        with self._lock:
            return self._last_price

    def rate_ratio(self, event_active: bool) -> float | None:
        """CB fast rate over the prior-based slow rate (for the optional Binance relative-activity factor)."""
        with self._lock:
            if self._rate.lam_fast is None:
                return None
            lam0, _ = self._rate.lam0(self._lam_regime(), self._default_len, event_active)
            return self._rate.lam_fast / lam0

    def snapshot(self, now: float, ext: ExternalSignals, event_active: bool) -> CbSnapshot:
        with self._lock:
            timing = compute_timing(now, self._win, self._default_len)
            mono = time.monotonic()
            stale = self._last_msg_mono is None or mono - self._last_msg_mono > self._cb.stale_s
            s = self._last_price
            n_obs: int | None
            range_now: float | None
            m_max: float | None
            m_min: float | None
            if timing.phase == PHASE_PRE:
                n_obs = 0
                m_max = m_min = s
                range_now = None if s is None else 0.0
            else:
                n_obs = None if self._l_start is None or self._l_end is None else self._l_end - self._l_start
                covered = self._covered_from is not None and self._win is not None and self._covered_from <= self._win[0]
                if not covered or s is None:
                    m_max = m_min = range_now = None
                else:
                    m_max = self._hi if self._hi is not None else s
                    m_min = self._lo if self._lo is not None else s
                    range_now = m_max - m_min
            c_mult = self._c_event_mult if event_active else 1.0
            elapsed = max(now - self._win[0], 0.0) if self._win is not None and timing.phase != PHASE_PRE else 0.0
            lam_regime = self._lam_regime()
            lam0, c_inf = self._rate.lam0(lam_regime, timing.length, event_active)
            lam_slow = self._rate.lam_slow(lam0, n_obs, min(elapsed, timing.length))
            vol = self._vol
            return CbSnapshot(
                timing=timing,
                window=self._win,
                connected=self._connected,
                ever_live=self._ever_live,
                stale=stale,
                ready=self._connected and self._hb_since_connect >= self._cb.resume_heartbeats,
                gap_pending=bool(self._gaps) or self._gap_running > 0,
                final=timing.phase == PHASE_POST and self._end_seen,
                t_live=self._rate.t_live,
                last_price=s,
                n_obs=n_obs,
                range_now=range_now,
                m_max=m_max,
                m_min=m_min,
                elapsed=elapsed,
                lam_hat=self._rate.lam_hat(timing.tau_end, lam0, lam_slow, ext),
                lam0=lam0,
                lam_regime=lam_regime,
                lam_season=self._rate.p.season_count / timing.length if self._rate.p.season_enable else None,
                lam_fast=self._rate.lam_fast,
                c_inf_eff=c_inf * c_mult,
                lam_burst=self._lam_burst,
                sigma_hat=vol.sigma_hat(timing.tau_end, min(elapsed, timing.length), ext, event_active),
                sigma_anchor=vol.sigma_anchor(ext, event_active),
                sigma_fast=vol.sigma_fast(),
                sigma_regime=vol.sigma_regime(),
                sigma_season=vol.sigma_season(),
            )


# ----------------------------------------------------------------------------------------------
# Websocket thread
# ----------------------------------------------------------------------------------------------
class CoinbaseWsThread(threading.Thread):
    """Subscribes to matches + heartbeat, feeds CoinbaseState, reconnects with 0.5/1/2/5 s backoff."""

    def __init__(self, state: CoinbaseState, cb: CbParams) -> None:
        super().__init__(name="cb-ws", daemon=True)
        self._state = state
        self._cb = cb
        self._stop_evt = threading.Event()
        self._last_stats = time.monotonic()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        backoff = 0.5
        subscribe = json.dumps(
            {"type": "subscribe", "product_ids": [PRODUCT_ID], "channels": ["matches", "heartbeat"]}
        )
        while not self._stop_evt.is_set():
            try:
                with connect(CB_WS_URL, open_timeout=10, close_timeout=2, max_size=2**22) as ws:
                    ws.send(subscribe)
                    self._state.on_connect()
                    self._state.touch()
                    log.info("Coinbase websocket connected")
                    while not self._stop_evt.is_set():
                        try:
                            raw = ws.recv(timeout=1.0)
                        except TimeoutError:
                            if self._state.idle_for() > self._cb.idle_reconnect_s:
                                raise ConnectionError("no Coinbase message for %.0fs" % self._cb.idle_reconnect_s)
                            continue
                        backoff = 0.5
                        self._on_message(raw)
                        self._maybe_log_stats()
            except Exception as exc:
                if not self._stop_evt.is_set():
                    log.warning("Coinbase websocket error: %s: %s (reconnect in %.1fs)", type(exc).__name__, exc, backoff)
            finally:
                self._state.on_disconnect()
            self._stop_evt.wait(backoff)
            backoff = min(backoff * 2.0, 5.0)

    def _on_message(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            log.warning("non-JSON Coinbase frame ignored")
            return
        if not isinstance(msg, dict):
            return
        self._state.touch()
        kind = msg.get("type")
        if kind in ("match", "last_match"):
            self._state.on_match(msg)
        elif kind == "heartbeat":
            self._state.on_heartbeat(msg)
        elif kind == "error":
            log.error("Coinbase error message: %s", msg)

    def _maybe_log_stats(self) -> None:
        if time.monotonic() - self._last_stats >= 60.0:
            self._last_stats = time.monotonic()
            count, span, skew = self._state.pop_minute_stats()
            log.info("CB matches received %d / id span %d per minute; heartbeat clock skew %+.3fs", count, span, skew)


# ----------------------------------------------------------------------------------------------
# REST helper
# ----------------------------------------------------------------------------------------------
class CoinbaseRest:
    """Rate-limited, failure-tolerant access to the Coinbase Exchange REST API (every call may return None)."""

    def __init__(self, max_rps: float, timeout: float = 5.0) -> None:
        self._min_interval = 1.0 / max_rps
        self._timeout = timeout
        self._lock = threading.Lock()
        self._next_ok = 0.0
        self._session = requests.Session()
        self._cursor_checked = False

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any | None:
        for attempt in range(3):
            with self._lock:  # serialise to honour the RPS limit
                wait = self._next_ok - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                self._next_ok = time.monotonic() + self._min_interval
            try:
                resp = self._session.get(CB_REST_URL + path, params=params, timeout=self._timeout)
            except requests.exceptions.SSLError as exc:
                log.error("Coinbase REST TLS failure (install `truststore`?): %s", exc)
                return None
            except requests.RequestException as exc:
                log.warning("Coinbase REST %s failed: %s", path, exc)
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    log.warning("Coinbase REST %s: invalid JSON", path)
                    return None
            if resp.status_code == 429:
                time.sleep(1.0 + attempt)
                continue
            log.warning("Coinbase REST %s -> HTTP %d", path, resp.status_code)
            return None
        return None

    # -- trades ----------------------------------------------------------------------------
    def trades_page(self, after: int | None = None) -> list[CbTrade] | None:
        """Up to 1000 trades, newest first. `after` returns trades older than that id (verified once)."""
        params: dict[str, Any] = {"limit": 1000}
        if after is not None:
            params["after"] = after
        data = self._get(f"/products/{PRODUCT_ID}/trades", params)
        if not isinstance(data, list):
            return None
        trades = [t for t in (_parse_trade(r) for r in data if isinstance(r, dict)) if t is not None]
        trades.sort(key=lambda t: -t.id)
        if after is not None and trades and not self._cursor_checked:
            self._cursor_checked = True
            if trades[0].id >= after:
                log.error("Coinbase `after` cursor returned ids >= cursor: direction differs from spec assumption")
                return None
        return trades

    def latest_trade(self) -> CbTrade | None:
        page = self.trades_page()
        return page[0] if page else None

    def find_last_trade_before(self, t_target: float, anchor: CbTrade, lam_guess: float, max_calls: int = 8) -> CbTrade | None:
        """Highest-id trade with time < t_target, by interpolation search (spec 1.3).

        `anchor` must be a trade with time >= t_target. Each call fetches a page of 1000 ids placed
        around the interpolated guess; the bracket and rate estimate tighten on every call.
        """
        hi, lo = anchor, None
        lam = max(lam_guess, 0.5)
        for _ in range(max_calls):
            if lo is None:
                guess = hi.id - lam * (hi.ts - t_target)
            else:
                if hi.id - lo.id <= 1:
                    return lo
                span = max(hi.ts - lo.ts, 1e-6)
                guess = lo.id + (hi.id - lo.id) * (t_target - lo.ts) / span
            after = int(guess) + 500
            after = min(after, hi.id)
            if lo is not None:
                after = max(after, lo.id + 2)
            page = self.trades_page(after)
            if not page:
                return None
            newest, oldest = page[0], page[-1]
            if newest.ts < t_target:
                lo = newest
            elif oldest.ts >= t_target:
                hi = oldest
            else:
                return next(t for t in page if t.ts < t_target)
            if newest.ts > oldest.ts:
                lam = max((newest.id - oldest.id) / (newest.ts - oldest.ts), 0.5)
        return None

    def regime_anchor(self, t_then: float, lam_guess: float) -> tuple[float, int] | None:
        """(time, trade id) of the last trade before `t_then` (interpolation search); None if REST fails."""
        latest = self.latest_trade()
        if latest is None or latest.ts < t_then:
            return None
        then = self.find_last_trade_before(t_then, latest, lam_guess)
        return None if then is None else (then.ts, then.id)

    def trades_since(self, t_start: float, max_pages: int = 40) -> list[CbTrade] | None:
        """All trades with time >= t_start plus the newest one before it (so L_start can be read off)."""
        out: list[CbTrade] = []
        after: int | None = None
        for _ in range(max_pages):
            page = self.trades_page(after)
            if page is None:
                return out or None
            if not page:
                break
            out.extend(page)
            if page[-1].ts < t_start:
                break
            after = page[-1].id
        else:
            log.warning("trades_since: %d pages did not reach %.0f", max_pages, t_start)
        return out

    def backfill(self, newer_id: int, older_id: int) -> list[CbTrade] | None:
        """Trades with older_id < id < newer_id (spec 1.2 dropped-match recovery)."""
        out: list[CbTrade] = []
        after = newer_id
        for _ in range(MAX_BACKFILL_PAGES):
            page = self.trades_page(after)
            if page is None:
                return out or None
            if not page:
                break
            out.extend(t for t in page if older_id < t.id < newer_id)
            if page[-1].id <= older_id + 1:
                break
            after = page[-1].id
        return out

    # -- candles ---------------------------------------------------------------------------
    def candles(self, start: float, end: float, granularity: int = 60) -> list[tuple[float, float, float]] | None:
        """(time, low, high) rows oldest first for [start, end), paged in blocks of 300 candles."""
        rows: dict[float, tuple[float, float, float]] = {}
        block = 300 * granularity
        t = start
        while t < end:
            t_end = min(t + block, end)
            iso = lambda x: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(x))  # noqa: E731
            data = self._get(
                f"/products/{PRODUCT_ID}/candles",
                {"granularity": granularity, "start": iso(t), "end": iso(t_end)},
            )
            if not isinstance(data, list):
                return None
            for r in data:
                try:
                    rows[float(r[0])] = (float(r[0]), float(r[1]), float(r[2]))
                except (IndexError, TypeError, ValueError):
                    continue
            t = t_end
        return sorted(rows.values())

    def sigma2_regime(self, t_ref: float, minutes: int = 30) -> float | None:
        """Parkinson variance rate of the 1-min candles in [t_ref - minutes, t_ref); None if REST fails."""
        c = self.candles(t_ref - minutes * 60, t_ref)
        return sigma2_from_candles(c) if c else None

class GapBackfiller(threading.Thread):
    """Services CoinbaseState gap requests with REST backfills (feeds M/m and the open bar)."""

    def __init__(self, state: CoinbaseState, rest: CoinbaseRest) -> None:
        super().__init__(name="cb-backfill", daemon=True)
        self._state = state
        self._rest = rest
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        while not self._stop_evt.wait(0.2):
            gap = self._state.take_gap()
            if gap is None:
                continue
            try:
                older, newer = gap
                trades = self._rest.backfill(newer, older)
                if trades:
                    self._state.ingest_history(trades)
                    log.info("backfilled %d trades for ids %d..%d", len(trades), older + 1, newer - 1)
                else:
                    log.warning("backfill for ids %d..%d returned nothing", older + 1, newer - 1)
            except Exception:
                log.exception("backfill failed")
            finally:
                self._state.gap_done()


def seed_window_history(state: CoinbaseState, rest: CoinbaseRest, start: float) -> bool:
    """Seed L_start, M and m for a window that started in the past (mid-window launch / moved start)."""
    trades = rest.trades_since(start)
    if not trades:
        log.error("could not seed window history from REST")
        return False
    state.ingest_history(trades)
    log.info("seeded %d trades from REST back to %s", len(trades), time.strftime("%H:%M:%S", time.gmtime(min(t.ts for t in trades))))
    return True
