"""Pure-MM quoting (addendum 2 section 4), hybrid opening (3), sniper (5), endgame and risk controls.

The decision functions (`compute_quotes`, `diff_quotes`, `find_plan`) are pure so they can be exercised without IO;
`Strategy` owns the state. Snipes are evaluated on the dedicated `SnipeWorker` thread fed by the SSE callback and
never make REST calls themselves: they hand `SnipeJob`s to the OrderManager.
"""
from __future__ import annotations

import csv
import logging
import math
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Callable, Sequence

from models import Order, OrderBook, Side

from .coinbase_feed import CbSnapshot, CoinbaseState
from .config import (
    LOG_DIR,
    PRODUCT_RANGE,
    PRODUCT_TRADES,
    Config,
    MmParams,
    ProductParams,
    QuoteParams,
    WindowManager,
    fmt_utc,
    parse_utc,
)
from .execution import Ledger, LiveOrder, NewOrder, OrderManager, SnipeJob
from .external_feeds import ExternalHub
from .models_fv import (
    PHASE_LIVE,
    PHASE_POST,
    PHASE_PRE,
    ExternalSignals,
    FairValue,
    Timing,
    range_fair_value,
    trades_fair_value,
)

log = logging.getLogger(__name__)
snipe_log = logging.getLogger("bot.snipe")

MODE_LIVE, MODE_DEGRADED, MODE_NONE = "live", "degraded", "none"
EPS = 1e-9


# ----------------------------------------------------------------------------------------------
# Price helpers
# ----------------------------------------------------------------------------------------------
def floor_k(x: float, k: float) -> float:
    return round(math.floor(x / k + EPS) * k, 6)


def ceil_k(x: float, k: float) -> float:
    return round(math.ceil(x / k - EPS) * k, 6)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


MKT_BLEND = 0.6  # weight on the external mid when centring passive quotes

# Manual settlement override: {"TRADES": value|null, "RANGE": value|null, "EDGE_TRADES": x, "EDGE_RANGE": y}.
# While a product has a value, F := value, passive quoting and endgame flattening stop, and every level at
# least EDGE through the value is swept up to the hard position limit (a deliberate directional bet).
OVERRIDE_PATH = str(__import__("pathlib").Path(__file__).resolve().parents[1] / "settle_override.json")
_override_cache: dict = {"mtime": None, "data": {}, "checked": 0.0}


def settle_override(symbol: str) -> tuple[float, float] | None:
    """(value, edge) for `symbol` from OVERRIDE_PATH, re-read at most once a second; None if unset."""
    import json
    import os

    now = time.monotonic()
    if now - _override_cache["checked"] >= 1.0:
        _override_cache["checked"] = now
        try:
            mtime = os.path.getmtime(OVERRIDE_PATH)
            if mtime != _override_cache["mtime"]:
                with open(OVERRIDE_PATH, encoding="utf-8") as fh:
                    _override_cache["data"] = json.load(fh)
                _override_cache["mtime"] = mtime
                logging.getLogger(__name__).warning("SETTLE OVERRIDE loaded: %s", _override_cache["data"])
        except FileNotFoundError:
            _override_cache["data"] = {}
        except Exception as exc:  # keep the last good override on a half-written / bad file
            logging.getLogger(__name__).warning("settle override unreadable: %s", exc)
    data = _override_cache["data"]
    value = data.get(symbol.upper())
    if value is None:
        return None
    try:
        return float(value), float(data.get(f"EDGE_{symbol.upper()}", 0.0))
    except (TypeError, ValueError):
        return None


def best_external(levels: Sequence[Order]) -> float | None:
    """Best price with volume left after removing our own (levels are sorted best first)."""
    for lvl in levels:
        if lvl.volume - lvl.own_volume > 0:
            return lvl.price
    return None


def mid_external(book: OrderBook) -> float | None:
    bid, ask = best_external(book.buy_orders), best_external(book.sell_orders)
    if bid is not None and ask is not None:
        return 0.5 * (bid + ask)
    return bid if bid is not None else ask


# ----------------------------------------------------------------------------------------------
# Books
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class BookView:
    book: OrderBook
    seq: int
    mono: float
    med_step: float | None  # median adjacent-level gap over the last snapshots (book-gap detector)


class BookStore:
    """Latest order book per product (a fresh snapshot always replaces the old one)."""

    def __init__(self, gap_history: int) -> None:
        self._lock = threading.Lock()
        self._books: dict[str, BookView] = {}
        self._steps: dict[str, deque[float]] = {}
        self._gap_history = gap_history
        self._volume_checked = False

    def set(self, book: OrderBook) -> BookView:
        steps = [b.price - a.price for a, b in zip(book.sell_orders, book.sell_orders[1:])]
        steps += [a.price - b.price for a, b in zip(book.buy_orders, book.buy_orders[1:])]
        with self._lock:
            hist = self._steps.setdefault(book.product, deque(maxlen=self._gap_history))
            if steps:
                hist.append(statistics.median(steps))
            prev = self._books.get(book.product)
            view = BookView(book, 1 if prev is None else prev.seq + 1, time.monotonic(), statistics.median(hist) if hist else None)
            self._books[book.product] = view
        if not self._volume_checked:
            levels = list(book.buy_orders) + list(book.sell_orders)
            if any(lvl.own_volume > 0 for lvl in levels):
                self._volume_checked = True
                if any(lvl.own_volume > lvl.volume for lvl in levels):
                    log.warning("book check: own_volume exceeds volume, so `volume` may EXCLUDE our own volume")
                else:
                    log.info("book check: volume includes own_volume (own <= total on every level)")
        return view

    def get(self, symbol: str) -> BookView | None:
        with self._lock:
            return self._books.get(symbol)

    def own_volume(self, symbol: str, side: Side) -> int:
        view = self.get(symbol)
        if view is None:
            return 0
        levels = view.book.buy_orders if side == Side.BUY else view.book.sell_orders
        return int(sum(lvl.own_volume for lvl in levels))


# ----------------------------------------------------------------------------------------------
# Quoting (pure)
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Market:
    symbol: str
    params: ProductParams
    tick: float
    lo: float
    hi: float
    contract_size: float

    @property
    def kind(self) -> str:
        return self.params.kind


@dataclass(frozen=True)
class Quote:
    side: Side
    level: int  # 0 = innermost
    price: float
    size: int


@dataclass(frozen=True)
class QuoteRequest:
    fv: FairValue
    market: Market
    pos: int
    q_target: float
    q_mm: float  # effective inventory scale Q_MM (shrinks in the endgame)
    budgets: dict[Side, int]  # volume each side may still add under the soft limit (guard view)
    allowed: frozenset[Side]
    beta: float
    k_give: float
    h_mult: float = 1.0
    side_h_mult: dict[Side, float] = field(default_factory=dict)
    size_mult: float = 1.0
    book: OrderBook | None = None
    penny: dict[Side, bool] = field(default_factory=dict)  # per-side penny throttle result
    levels: int = 2
    ask_extra_abs: float = 0.0  # spike / burst guards: asks stay at least this far (plus ask_extra_eadv * e_adv) above F
    ask_extra_eadv: float = 0.0
    ask_size_mult: float = 1.0


@dataclass(frozen=True)
class QuotePlan:
    quotes: tuple[Quote, ...]
    bid_max: float  # highest bid we accept to rest (live bids above it are invalid)
    ask_min: float
    h: float
    e_adv: float
    x: float


def compute_quotes(req: QuoteRequest, qp: QuoteParams, mm: MmParams) -> QuotePlan:
    """Signed-skew quotes anchored to the book (addendum 2 section 4)."""
    m, k, fv = req.market, req.market.tick, req.fv
    sigma = fv.sigma
    h = clamp(qp.k_h * sigma, m.params.h_min_ticks * k, m.params.h_max_ticks * k) * req.h_mult
    e_adv = max(m.params.h_min_ticks * k, mm.k_adv * sigma)
    e_min = max(k, qp.k_floor * sigma)
    q_rel = req.pos - req.q_target
    x = clamp(q_rel / max(req.q_mm, 1e-9), -2.0, 2.0)
    ax = abs(x)
    beta = req.beta
    give = req.k_give * sigma
    ext_bid = best_external(req.book.buy_orders) if req.book else None
    ext_ask = best_external(req.book.sell_orders) if req.book else None
    delta2 = max(2 * k, 0.5 * h)

    # Bid: reduces |q_rel| when x < 0, increases it otherwise. Ask is the mirror image.
    bid_inc, ask_inc = x >= 0, x <= 0
    hb = h * req.side_h_mult.get(Side.BUY, 1.0)
    ha = h * req.side_h_mult.get(Side.SELL, 1.0)
    bid_model = fv.f - hb * (1.0 + beta * x)
    ask_model = fv.f + ha * (1.0 - beta * x)
    cap_bid = fv.f - e_adv * (1.0 + beta * ax) if bid_inc else min(bid_model, fv.f + give)
    cap_ask = fv.f + e_adv * (1.0 + beta * ax) if ask_inc else max(ask_model, fv.f - give)
    extra = req.ask_extra_abs + req.ask_extra_eadv * e_adv
    if extra > 0.0:
        cap_ask = max(cap_ask, fv.f + extra)
    ask_floor = fv.lb + e_min
    bid_max = floor_k(cap_bid, k)
    ask_min = ceil_k(max(cap_ask, ask_floor), k)
    if ext_ask is not None:
        bid_max = min(bid_max, round(ext_ask - k, 6))
    if ext_bid is not None:
        ask_min = max(ask_min, round(ext_bid + k, 6))
    # Note: bid_max / ask_min here are validity limits; the reduce side may legitimately sit through F.

    prices: dict[Side, list[float | None]] = {}
    for side in (Side.BUY, Side.SELL):
        if side not in req.allowed:
            continue
        if side == Side.BUY:
            if ext_bid is not None:  # join or penny the book, but never closer to F than cap_bid
                p0 = min(floor_k(cap_bid, k), round(ext_bid + k, 6) if req.penny.get(side, True) else ext_bid)
            else:
                p0 = floor_k(min(bid_model, cap_bid) if bid_inc else bid_model, k)
            p0 = min(p0, bid_max)
            raw = [floor_k(p0 - i * delta2, k) for i in range(req.levels)]
        else:
            if ext_ask is not None:
                p0 = max(ceil_k(cap_ask, k), round(ext_ask - k, 6) if req.penny.get(side, True) else ext_ask)
            else:
                p0 = ceil_k(max(ask_model, cap_ask) if ask_inc else ask_model, k)
            p0 = max(p0, ask_min)
            raw = [ceil_k(p0 + i * delta2, k) for i in range(req.levels)]
        prices[side] = [p if m.lo - EPS <= p <= m.hi + EPS else None for p in raw]

    bid0 = prices.get(Side.BUY, [None])[0]
    ask0 = prices.get(Side.SELL, [None])[0]
    if bid0 is not None and ask0 is not None and ask0 - bid0 < 2 * k - EPS and bid_inc and ask_inc:
        prices[Side.BUY][0] = prices[Side.SELL][0] = None

    out: list[Quote] = []
    for side, plist in prices.items():
        increases = bid_inc if side == Side.BUY else ask_inc
        budget = req.budgets.get(side, 0)
        reduce_left = int(abs(q_rel))
        for level, price in enumerate(plist):
            if price is None:
                continue
            base = m.params.sizes[min(level, len(m.params.sizes) - 1)]
            smult = req.size_mult * (req.ask_size_mult if side == Side.SELL else 1.0)
            if increases:
                size = int(base * max(0.0, 1.0 - ax) * smult)
                size = min(size, budget)
                budget -= max(size, 0)
            else:
                size = min(reduce_left, int(base * (1.0 + ax) * smult))
                reduce_left -= max(size, 0)
            if size > 0:
                out.append(Quote(side, level, price, size))
    return QuotePlan(tuple(out), bid_max, ask_min, h, e_adv, x)


def order_invalid(o: LiveOrder, plan: QuotePlan, market: Market) -> bool:
    """A live order that is now more aggressive than the plan allows (cancel urgently)."""
    if o.side == Side.BUY:
        bad = o.price > plan.bid_max + EPS
    else:
        bad = o.price < plan.ask_min - EPS
    return bad or not (market.lo - EPS <= o.price <= market.hi + EPS)


@dataclass(frozen=True)
class QuoteDiff:
    urgent: tuple[LiveOrder, ...]
    cancels: tuple[LiveOrder, ...]
    news: tuple[Quote, ...]


def diff_quotes(
    desired: Sequence[Quote], live: Sequence[LiveOrder], plan: QuotePlan, market: Market, qp: QuoteParams, now_mono: float
) -> QuoteDiff:
    """Match desired levels to live orders by side and price; decide what to cancel and what to send."""
    k = market.tick
    thresh = max(market.params.rq_min_ticks * k, qp.rq_frac * plan.h)
    urgent: list[LiveOrder] = []
    cancels: list[LiveOrder] = []
    news: list[Quote] = []
    for side in (Side.BUY, Side.SELL):
        pool = [o for o in live if o.side == side and not o.canceling]
        for o in list(pool):
            if order_invalid(o, plan, market):
                urgent.append(o)
                pool.remove(o)
        for q in sorted((d for d in desired if d.side == side), key=lambda d: d.level):
            best = min(pool, key=lambda o: abs(o.price - q.price), default=None)
            if best is None or abs(best.price - q.price) >= thresh - EPS:
                news.append(q)
                continue
            pool.remove(best)
            if best.remaining < qp.topup_frac * q.size and now_mono - best.created > qp.topup_min_age_s:
                cancels.append(best)
                news.append(q)
        cancels.extend(pool)  # level no longer desired
    return QuoteDiff(tuple(urgent), tuple(cancels), tuple(news))


# ----------------------------------------------------------------------------------------------
# Sniping (pure)
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class SnipePlan:
    side: Side
    levels: tuple[tuple[float, int], ...]  # qualifying levels, best first: (price, external volume)
    rule: str
    next_price: float | None  # first non-qualifying level on the same side (None if none)

    @property
    def available(self) -> int:
        return sum(v for _, v in self.levels)

    def price_for(self, volume: int) -> float:
        """Worst price needed to take `volume` through the qualifying levels."""
        cum = 0
        for price, avail in self.levels:
            cum += avail
            if cum >= volume:
                return price
        return self.levels[-1][0]


def find_plan(side: Side, book: OrderBook, test: Callable[[int, float], str | None]) -> SnipePlan | None:
    """Qualifying prefix of the book for a snipe on `side` (asks for a buy, bids for a sell).

    `test(index, price)` returns a rule label or None. Qualification is monotone in price, so the prefix ends
    at the first level that fails or that holds only our own volume.
    """
    levels = book.sell_orders if side == Side.BUY else book.buy_orders
    taken: list[tuple[float, int]] = []
    rules: set[str] = set()
    nxt: float | None = None
    for i, lvl in enumerate(levels):
        why = test(i, lvl.price)
        avail = lvl.volume - lvl.own_volume
        if why is None or avail <= 0:
            nxt = lvl.price
            break
        taken.append((lvl.price, avail))
        rules.add(why)
    if not taken:
        return None
    return SnipePlan(side, tuple(taken), "".join(sorted(rules)), nxt)


# ----------------------------------------------------------------------------------------------
# Runtime state
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class FvState:
    fv: FairValue | None
    mode: str
    obs_valid: bool  # n_obs / R_now valid: the deterministic lower bound may be used
    seeded: bool  # priors available (model seeded)
    gates_ok: bool  # hybrid validity gates for a directional q*
    timing: Timing
    final: bool
    sigma_log_emp: float


@dataclass
class OpenPhase:
    started_wall: float | None = None
    ended_wall: float | None = None
    q_last: float = 0.0
    q_end: float = 0.0
    calm_since: float | None = None


@dataclass
class ProductRuntime:
    market: Market
    fvs: FvState | None = None
    fill_times: dict[Side, deque[float]] = field(default_factory=lambda: {Side.BUY: deque(), Side.SELL: deque()})
    tox_until: dict[Side, float] = field(default_factory=lambda: {Side.BUY: 0.0, Side.SELL: 0.0})
    snipe_last: dict[tuple[Side, float], tuple[int, float]] = field(default_factory=dict)
    snipe_lock: threading.Lock = field(default_factory=threading.Lock)
    last_batch: float = 0.0
    opening: OpenPhase = field(default_factory=OpenPhase)
    disagree_since: float | None = None
    disagree_active: bool = False
    pnl: float = 0.0
    q_star: float = 0.0
    f_hist: deque[tuple[float, float]] = field(default_factory=deque)
    f_at_quote: dict[Side, float] = field(default_factory=dict)
    reprice_times: dict[Side, deque[float]] = field(default_factory=lambda: {Side.BUY: deque(), Side.SELL: deque()})
    last_reprice: dict[Side, float] = field(default_factory=lambda: {Side.BUY: 0.0, Side.SELL: 0.0})
    inv_since: float = 0.0
    last_abs_q: int = 0
    last_plan: QuotePlan | None = None
    last_quotes: tuple[Quote, ...] = ()
    guard_until: float = 0.0  # spike / burst guard active until this monotonic time
    spread_pnl: float = 0.0  # sum of (F_at_fill - price) * side * contract
    inv_pnl: float = 0.0  # sum of position * dF * contract
    prev_f: float | None = None
    prev_pos: int = 0
    inv_alert_since: float | None = None
    flatten_last: float = 0.0


class SnipeWorker(threading.Thread):
    """Dedicated snipe thread: blocks on a single-slot latest-book queue filled by the SSE callback."""

    def __init__(self, evaluate: Callable[[BookView], None]) -> None:
        super().__init__(name="snipe-eval", daemon=True)
        self._evaluate = evaluate
        self._cond = threading.Condition()
        self._slot: dict[str, BookView] = {}
        self._stop_flag = False

    def offer(self, view: BookView) -> None:
        with self._cond:
            self._slot[view.book.product] = view
            self._cond.notify()

    def stop(self) -> None:
        with self._cond:
            self._stop_flag = True
            self._cond.notify()

    def run(self) -> None:
        while True:
            with self._cond:
                while not self._slot and not self._stop_flag:
                    self._cond.wait()
                if self._stop_flag:
                    return
                views = list(self._slot.values())
                self._slot.clear()
            for view in views:
                try:
                    self._evaluate(view)
                except Exception:
                    log.exception("snipe evaluation failed")


class Strategy:
    """Owns the per-loop decision logic. Everything that touches REST goes through the OrderManager."""

    def __init__(
        self,
        cfg: Config,
        markets: Sequence[Market],
        cb: CoinbaseState,
        hub: ExternalHub,
        om: OrderManager,
        ledger: Ledger,
        books: BookStore,
        windows: WindowManager,
        seed_history: Callable[[float], None],
        first_cmi_event: Callable[[], float | None],
    ) -> None:
        self.cfg = cfg
        self.rt = {m.symbol: ProductRuntime(m) for m in markets}
        self.cb, self.hub, self.om, self.ledger, self.books = cb, hub, om, ledger, books
        self.windows = windows
        self._seed_history = seed_history
        self._first_cmi_event = first_cmi_event
        self.wake = threading.Event()
        self._stop = threading.Event()
        self.halted = False
        self._seeding = threading.Lock()
        self._corr_side: Side | None = None
        self._ext = ExternalSignals()
        self._snap: CbSnapshot | None = None
        self._event_active = False
        self._events = tuple(t for t in (parse_utc(s) for s in cfg.events.times_utc) if t is not None)
        self._csv_file = None
        self._csv: csv.writer | None = None
        self._last_csv = 0.0
        self._cb_stale_logged = False
        self._thread: threading.Thread | None = None
        self.sniper = SnipeWorker(self._evaluate_view)

    # -- lifecycle -------------------------------------------------------------------------
    def start(self) -> None:
        self.sniper.start()
        self._thread = threading.Thread(target=self._run, name="strategy", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self.wake.set()
        self.sniper.stop()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        if self._csv_file is not None:
            self._csv_file.close()

    def _run(self) -> None:
        while not self._stop.is_set() and not self.halted:
            self.wake.wait(self.cfg.loop.loop_ms / 1000.0)
            self.wake.clear()
            try:
                self.step()
            except Exception:
                log.exception("strategy step failed")

    def on_book(self, view: BookView) -> None:
        """SSE callback hook: hand the snapshot to the snipe thread and wake the loop. No IO."""
        self.sniper.offer(view)
        self.wake.set()

    # -- helpers ---------------------------------------------------------------------------
    def tradable(self, now: float) -> bool:
        window = self.windows.window
        if window is None or self.halted:
            return False
        if not self.cfg.startup.trade_before_start and now < window[0]:
            return False
        close = self.windows.close_ts
        return close is None or now < close

    def _qmm_eff(self, rt: ProductRuntime, timing: Timing) -> float:
        """Q_MM, shrinking linearly over the last T_FLAT seconds."""
        q = rt.market.params.limit_soft
        t_flat = self.cfg.mm.t_flat
        if timing.phase == PHASE_LIVE and timing.tau_end <= t_flat:
            q *= max(timing.tau_end, 1.0) / t_flat
        return max(q, 1.0)

    def _k_give(self, timing: Timing) -> float:
        mm = self.cfg.mm
        if timing.phase == PHASE_LIVE and timing.tau_end <= mm.t_flat:
            return mm.k_give + (mm.k_give_end - mm.k_give) * (1.0 - timing.tau_end / mm.t_flat)
        return mm.k_give

    # -- opening phase / target inventory --------------------------------------------------
    def _z_score(self, fvs: FvState, mid: float | None) -> float | None:
        if fvs.fv is None or mid is None or mid <= 0 or fvs.fv.f <= 0:
            return None
        return math.log(fvs.fv.f / mid) / fvs.sigma_log_emp

    def _update_target(self, rt: ProductRuntime, fvs: FvState, mid: float | None, now: float, mono: float) -> float:
        """Opening-phase q* (hybrid: log-space z with validity gates; directional: addendum 1; pure: 0)."""
        mm, op, cfg = self.cfg.mm, rt.opening, self.cfg.opening
        if mm.mode == "pure":
            return 0.0
        z = self._z_score(fvs, mid)
        window = self.windows.window
        if op.started_wall is None:
            if self.tradable(now) and mid is not None:
                op.started_wall = now
                log.info("[%s] opening phase starts at %s", rt.market.symbol, fmt_utc(now))
        elif op.ended_wall is None:
            ended = None
            if window is not None and now >= window[0] + cfg.phase_s:
                ended = "time limit"
            elif z is not None and abs(z) < cfg.exit_z:
                op.calm_since = mono if op.calm_since is None else op.calm_since
                if mono - op.calm_since >= cfg.exit_hold_s:
                    ended = "gap closed"
            else:
                op.calm_since = None
            if ended:
                op.ended_wall, op.q_end = now, op.q_last
                log.info("[%s] opening phase ended (%s); q*=%.1f decays over %.0fs", rt.market.symbol, ended, op.q_end, cfg.decay_s)
        if op.started_wall is not None and op.ended_wall is None:
            q_max = mm.q_dir_frac * rt.market.params.limit_hard
            ok = z is not None and fvs.gates_ok and fvs.mode == MODE_LIVE
            if ok:
                assert z is not None
                q = math.copysign(q_max * clamp((abs(z) - mm.z_open_dir) / mm.z_ramp, 0.0, 1.0), z)
            else:
                q = 0.0
            op.q_last = q
            return q
        if op.ended_wall is not None:
            return op.q_end * max(0.0, 1.0 - (now - op.ended_wall) / cfg.decay_s)
        return 0.0

    # -- fair values -----------------------------------------------------------------------
    def _fair_value(self, rt: ProductRuntime, snap: CbSnapshot) -> FvState:
        m, cfg = rt.market, self.cfg
        feed_live = snap.connected and not snap.stale and snap.ready
        fv: FairValue | None = None
        obs_valid = False
        gates = False
        emp = cfg.mm.sigma_log_emp_trades if m.kind == PRODUCT_TRADES else cfg.mm.sigma_log_emp_range
        if m.kind == PRODUCT_TRADES:
            if snap.n_obs is not None:
                fv = trades_fair_value(cfg.trades_model, snap.n_obs, snap.timing, snap.lam_hat, snap.c_inf_eff, m.hi)
                obs_valid = True
            both = snap.lam_regime is not None and snap.lam_season is not None
            agree = both and abs(math.log(snap.lam_regime / snap.lam_season)) <= cfg.mm.agree_max
            confirm = snap.t_live >= cfg.mm.confirm_live_s and snap.lam_fast is not None and 0.75 <= snap.lam_fast / snap.lam0 <= 1.33
            seeded = True
        else:
            feed_live = feed_live and not snap.gap_pending
            if snap.range_now is not None and snap.m_max is not None and snap.m_min is not None and snap.last_price is not None:
                c_sigma = cfg.range_model.c_sigma
                if self._event_active or self._ext.iv_event:
                    c_sigma *= cfg.events.c_mult
                fv = range_fair_value(cfg.range_model, snap.m_max, snap.m_min, snap.last_price, snap.sigma_hat, snap.timing, c_sigma, m.hi)
                obs_valid = True
            both = snap.sigma_regime is not None and snap.sigma_season is not None
            agree = both and abs(math.log(snap.sigma_regime / snap.sigma_season)) <= cfg.mm.agree_max
            confirm = snap.t_live >= cfg.mm.confirm_live_s and snap.sigma_anchor > 0 and 0.6 <= snap.sigma_fast / snap.sigma_anchor <= 1.6
            seeded = True
        fresh = self.cb.idle_for() < cfg.mm.fresh_s
        gates = fresh and snap.t_live >= cfg.mm.min_live_s and agree and confirm
        sigma_log = emp
        if fv is not None:
            sigma_log = max(emp * math.sqrt(snap.timing.tau_f / snap.timing.length), fv.sigma / max(fv.f, 1.0))
        if feed_live and fv is not None:
            mode = MODE_LIVE
        elif fv is not None and not snap.ever_live and snap.timing.phase == PHASE_PRE:
            mode = MODE_DEGRADED  # feed down at start: priors only, wider and smaller
            fv = replace(fv, sigma=fv.sigma * cfg.risk.feed_down_sigma_mult)
        else:
            mode = MODE_NONE
        ovr = settle_override(m.symbol)
        if ovr is not None:
            value, edge = ovr
            sigma = max(edge, m.tick)
            fv = replace(fv, f=value, sigma=sigma) if fv is not None else FairValue(value, sigma, -1e18, 0.0, 0.0)
            mode, seeded = MODE_LIVE, True
        return FvState(fv, mode, obs_valid, seeded, gates, snap.timing, snap.final, sigma_log)

    # -- main step -------------------------------------------------------------------------
    def step(self) -> None:
        now, mono = time.time(), time.monotonic()
        window, changed = self.windows.refresh(now, self._first_cmi_event(), self.cb.first_heartbeat_ts())
        if changed and window is not None:
            self._on_window_changed(window)
        self._event_active = bool(window) and any(window[0] <= t < window[1] for t in self._events)
        self._ext = self.hub.latest(self.cb.last_price(), self.cb.rate_ratio(self._event_active))
        snap = self._snap = self.cb.snapshot(now, self._ext, self._event_active)

        close = self.windows.close_ts
        if close is not None and now >= close and not self.halted:
            self._hard_stop(close)
            return

        for rt in self.rt.values():
            rt.fvs = self._fair_value(rt, snap)
            if rt.fvs.fv is not None:
                rt.f_hist.append((mono, rt.fvs.fv.f))
                while rt.f_hist and mono - rt.f_hist[0][0] > self.cfg.snipe.jump_window_s + 2.0:
                    rt.f_hist.popleft()
        if snap.stale and not self._cb_stale_logged and snap.ever_live:
            self._cb_stale_logged = True
            log.warning("Coinbase feed stale: passive quotes pulled, only rule-b snipes allowed")
        elif not snap.stale:
            self._cb_stale_logged = False
        self._update_corr()
        for rt in self.rt.values():
            self._step_product(rt, now, mono)
        if now - self._last_csv >= 1.0:
            self._last_csv = now
            self._write_csv(now)
            if self.cfg.dry_run:
                self._status_lines(now)

    def _on_window_changed(self, window: tuple[float, float]) -> None:
        needs_history = self.cb.set_window(*window)
        log.warning("WINDOW set: start %s end %s (needs REST history seed: %s)", fmt_utc(window[0]), fmt_utc(window[1]), needs_history)
        if needs_history and self._seeding.acquire(blocking=False):
            def work() -> None:
                try:
                    self._seed_history(window[0])
                    self.cb.set_window(*window)
                finally:
                    self._seeding.release()

            threading.Thread(target=work, name="window-seed", daemon=True).start()

    def _hard_stop(self, close: float) -> None:
        log.warning("HARD STOP at %s: cancelling all orders", fmt_utc(close))
        self.halted = True
        self.om.cancel_all_blocking()
        self.om.poll_positions()
        for rt in self.rt.values():
            pol, est = self.ledger.positions(rt.market.symbol)
            log.warning("[%s] final position polled=%d est=%d pnl~%.1f", rt.market.symbol, pol, est, rt.pnl)

    def _update_corr(self) -> None:
        risk = self.cfg.risk
        self._corr_side = None
        if not risk.corr_risk or len(self.rt) < 2:
            return
        ratios = [self.ledger.positions(rt.market.symbol)[1] / rt.market.params.limit_hard for rt in self.rt.values()]
        if all(r > risk.corr_threshold for r in ratios):
            self._corr_side = Side.BUY
        elif all(r < -risk.corr_threshold for r in ratios):
            self._corr_side = Side.SELL

    # -- per product -----------------------------------------------------------------------
    def _step_product(self, rt: ProductRuntime, now: float, mono: float) -> None:
        fvs = rt.fvs
        if fvs is None:
            return
        view = self.books.get(rt.market.symbol)
        mid = mid_external(view.book) if view else None
        self._update_pnl(rt, fvs)
        rt.q_star = self._update_target(rt, fvs, mid, now, mono)
        self._update_disagreement(rt, fvs, mid, mono)
        self._track_inventory_age(rt, mono)
        if self.om.is_paused():
            return
        self._flatten(rt, fvs, view, now, mono)
        self._requote(rt, fvs, view, now, mono)

    def _update_pnl(self, rt: ProductRuntime, fvs: FvState) -> None:
        if fvs.fv is None:
            return
        sym = rt.market.symbol
        _, est = self.ledger.positions(sym)
        start = self.ledger.pos_start.get(sym, est)
        rt.pnl = rt.market.contract_size * (self.ledger.cash_px[sym] + (est - start) * fvs.fv.f)
        if rt.prev_f is not None:
            rt.inv_pnl += rt.prev_pos * (fvs.fv.f - rt.prev_f) * rt.market.contract_size
        rt.prev_f, rt.prev_pos = fvs.fv.f, est
        mono = time.monotonic()
        if abs(rt.inv_pnl) > abs(rt.spread_pnl):
            rt.inv_alert_since = mono if rt.inv_alert_since is None else rt.inv_alert_since
            if mono - rt.inv_alert_since > 300.0:
                log.warning("[%s] ALERT inventory P&L %.0f exceeds spread P&L %.0f for >5 min", sym, rt.inv_pnl, rt.spread_pnl)
                rt.inv_alert_since = mono
        else:
            rt.inv_alert_since = None

    def _update_disagreement(self, rt: ProductRuntime, fvs: FvState, mid: float | None, mono: float) -> None:
        risk = self.cfg.risk
        far = fvs.fv is not None and mid is not None and fvs.mode == MODE_LIVE and abs(mid - fvs.fv.f) > risk.disagree_z * fvs.fv.sigma
        if not far:
            if rt.disagree_active:
                log.info("[%s] model/market disagreement cleared", rt.market.symbol)
            rt.disagree_since, rt.disagree_active = None, False
            return
        rt.disagree_since = mono if rt.disagree_since is None else rt.disagree_since
        if not rt.disagree_active and mono - rt.disagree_since >= risk.disagree_hold_s:
            rt.disagree_active = True
            log.warning("[%s] WARN model F=%.1f vs external mid=%.1f for %.0fs: halving sizes, doubling Z_INC",
                        rt.market.symbol, fvs.fv.f, mid, risk.disagree_hold_s)

    def _track_inventory_age(self, rt: ProductRuntime, mono: float) -> None:
        _, est = self.ledger.positions(rt.market.symbol)
        if abs(est) < rt.last_abs_q or abs(est) == 0 or rt.inv_since == 0.0:
            rt.inv_since = mono
        rt.last_abs_q = abs(est)

    # -- endgame flatten (tau <= T_HIT) ----------------------------------------------------
    def _flatten(self, rt: ProductRuntime, fvs: FvState, view: BookView | None, now: float, mono: float) -> None:
        mm, m = self.cfg.mm, rt.market
        fv = fvs.fv
        if fv is None or view is None or fvs.mode != MODE_LIVE or not self.tradable(now):
            return
        if not (fvs.timing.phase == PHASE_LIVE and fvs.timing.tau_end <= mm.t_hit):
            return
        if mono - rt.flatten_last < self.cfg.snipe.cooldown_s:
            return
        if settle_override(m.symbol) is not None:  # directional bet on the override: never flatten
            return
        _, est = self.ledger.positions(m.symbol)
        if est == 0:
            return
        side = Side.SELL if est > 0 else Side.BUY
        reach = mm.k_give_end * fv.sigma
        test = (lambda i, p: "f" if p >= fv.f - reach - EPS else None) if side == Side.SELL else (
            lambda i, p: "f" if p <= fv.f + reach + EPS else None
        )
        plan = find_plan(side, view.book, test)
        if plan is None:
            return
        vol = min(plan.available, abs(est))
        if self._launch(rt, view, plan, vol, m.params.limit_hard, "flatten", mono, respect_cooldown=False):
            rt.flatten_last = mono

    # -- sniper (runs on the SnipeWorker thread) ---------------------------------------------
    def _evaluate_view(self, view: BookView) -> None:
        rt = self.rt.get(view.book.product)
        if rt is None or self.om.is_paused():
            return
        fvs = rt.fvs
        if fvs is None or fvs.fv is None:
            return
        now, mono = time.time(), time.monotonic()
        if not self.tradable(now) or mono - view.mono > self.cfg.risk.cmi_stale_s:
            return
        sp, m, fv = self.cfg.snipe, rt.market, fvs.fv
        post = fvs.timing.phase == PHASE_POST
        allow_model = fvs.mode == MODE_LIVE and (not post or fvs.final) and fvs.seeded
        allow_lb = fvs.obs_valid and not post
        if not (allow_model or allow_lb):
            return
        _, est = self.ledger.positions(m.symbol)
        z_inc = sp.z_inc * (2.0 if rt.disagree_active else 1.0)
        sigma_c = max(fv.sigma, fvs.sigma_log_emp * fv.f)  # outlier tests use the wider of model and empirical sigma
        ovr = settle_override(m.symbol)
        if ovr is not None:
            value, edge = ovr
            edge = max(edge, m.tick)
            for side in (Side.BUY, Side.SELL):
                sgn = -1.0 if side == Side.BUY else 1.0
                plan = find_plan(side, view.book, lambda i, p, s=sgn: "O" if s * (p - value) >= edge - EPS else None)
                if plan is not None:
                    self._launch(rt, view, plan, plan.available, m.params.limit_hard, "override", mono)
            return
        for side in (Side.BUY, Side.SELL):
            increases = (est >= 0) if side == Side.BUY else (est <= 0)
            theta = max(sp.theta_min_ticks * m.tick, (z_inc if increases else sp.z_red) * fv.sigma)
            if post:
                theta = max(sp.theta_min_ticks * m.tick, 2.0 * fv.sigma)
            sgn = -1.0 if side == Side.BUY else 1.0  # buys want prices below F, sells above

            def beyond(price: float, dist: float) -> bool:
                return sgn * (price - fv.f) >= dist - EPS

            def fat_test(i: int, p: float) -> str | None:
                if not allow_model:
                    return None
                if beyond(p, sp.z_fat * sigma_c):
                    return "A"
                if abs(p - (m.lo if side == Side.BUY else m.hi)) < EPS:
                    return "B"
                return None

            def cons_test(i: int, p: float) -> str | None:
                if allow_model and beyond(p, theta):
                    return "a"
                if allow_lb and side == Side.BUY and p <= fv.lb + EPS:
                    return "b"
                return None

            fat = find_plan(side, view.book, fat_test)
            cons = find_plan(side, view.book, cons_test)
            # Class 2 sweeps up to the hard limit, but only after the pre-large checks.
            if fat is not None:
                if self._large_ok(rt, fvs, view, fat, sigma_c):
                    if self._launch(rt, view, fat, fat.available, m.params.limit_hard, "fat" + fat.rule, mono):
                        continue
                elif self._launch(rt, view, fat, min(fat.available, m.params.snipe_max_lots), m.params.snipe_max_lots, "fat-capped", mono):
                    continue
            if cons is not None:
                lots = cons.available if cons.rule == "b" else min(cons.available, m.params.snipe_max_lots)
                if self._launch(rt, view, cons, lots, m.params.limit_hard if cons.rule == "b" else m.params.snipe_max_lots, "cons" + cons.rule, mono):
                    continue
            if allow_model and view.med_step:
                gap = self._gap_plan(side, view, fv, m)
                if gap is not None:
                    self._launch(rt, view, gap, min(gap.available, m.params.snipe_max_lots), m.params.snipe_max_lots, "gap", mono)

    def _gap_plan(self, side: Side, view: BookView, fv: FairValue, m: Market) -> SnipePlan | None:
        """Book-gap detector (c): cheap levels isolated below/above a large gap and on the right side of F."""
        sp = self.cfg.snipe
        levels = view.book.sell_orders if side == Side.BUY else view.book.buy_orders
        thr = max(sp.gap_min_ticks * m.tick, sp.gap_k * (view.med_step or 0.0))
        taken: list[tuple[float, int]] = []
        for i, lvl in enumerate(levels):
            avail = lvl.volume - lvl.own_volume
            right_side = lvl.price <= fv.f if side == Side.BUY else lvl.price >= fv.f
            if avail <= 0 or not right_side:
                return None
            taken.append((lvl.price, avail))
            if i + 1 < len(levels) and abs(levels[i + 1].price - lvl.price) >= thr - EPS:
                return SnipePlan(side, tuple(taken), "gap", levels[i + 1].price)
        return None

    def _large_ok(self, rt: ProductRuntime, fvs: FvState, view: BookView, plan: SnipePlan, sigma_c: float) -> bool:
        """Checks required before sweeping above SNIPE_MAX_LOTS (addendum 2 section 5)."""
        sp, fv = self.cfg.snipe, fvs.fv
        assert fv is not None
        mono = time.monotonic()
        if self.cb.idle_for() >= self.cfg.mm.fresh_s or not fvs.seeded or mono < rt.guard_until:
            return False
        old = next((f for t, f in rt.f_hist if mono - t <= sp.jump_window_s), None)
        if old is not None and abs(fv.f - old) >= sp.jump_frac * fv.sigma:
            return False
        # External mid excluding the outlier levels must agree with F.
        book = view.book
        if plan.side == Side.BUY:
            bid, ask = best_external(book.buy_orders), plan.next_price
        else:
            bid, ask = plan.next_price, best_external(book.sell_orders)
        refs = [p for p in (bid, ask) if p is not None]
        if not refs:
            return False
        mid = sum(refs) / len(refs)
        return abs(mid - fv.f) <= sp.z_cons * fv.sigma

    def _launch(self, rt: ProductRuntime, view: BookView, plan: SnipePlan, want: int, lim: int, tag: str, mono: float, respect_cooldown: bool = True) -> bool:
        """Size, make room, self-trade-proof and hand a snipe to the OrderManager."""
        m = rt.market
        side = plan.side
        want = min(int(want), plan.available)
        if want <= 0:
            return False
        cap = self.ledger.capacity(m.symbol, side, lim, self.books.own_volume(m.symbol, side), False)
        orders = [o for o in self.ledger.live_orders(m.symbol) if not o.canceling]
        same_side = [o for o in orders if o.side == side]
        vol = min(want, cap)
        cancel_ids: set[str] = set()
        if cap < want and same_side:  # making room: pull our same-side quotes first
            vol = min(want, cap + sum(o.remaining for o in same_side))
            cancel_ids.update(o.id for o in same_side)
        if vol <= 0:
            return False
        price = plan.price_for(vol)
        # Self-trade: reduce-side quotes can sit through F, so cancel any opposite order the snipe would hit.
        for o in orders:
            if o.side != side and ((side == Side.BUY and o.price <= price + EPS) or (side == Side.SELL and o.price >= price - EPS)):
                cancel_ids.add(o.id)
        key = (side, price)
        with rt.snipe_lock:
            last = rt.snipe_last.get(key)
            if respect_cooldown and last is not None and (view.seq == last[0] or mono - last[1] < self.cfg.snipe.cooldown_s):
                return False
            rt.snipe_last[key] = (view.seq, mono)
        fvs = rt.fvs
        assert fvs is not None and fvs.fv is not None
        snipe_log.info(
            "[%s] SNIPE %s %d@%s %s F=%.2f sd=%.2f LB=%.2f q=%d cap=%d cancels=%d",
            m.symbol, side, vol, price, tag, fvs.fv.f, fvs.fv.sigma, fvs.fv.lb,
            self.ledger.positions(m.symbol)[1], cap, len(cancel_ids),
        )
        self.om.submit_snipe(SnipeJob(m.symbol, side, price, vol, lim, tuple(cancel_ids), tag))
        return True

    # -- quoting ---------------------------------------------------------------------------
    def _penny_ok(self, rt: ProductRuntime, side: Side, mono: float) -> bool:
        """Penny throttle: one reprice per side per 0.5 s and at most PENNY_MAX_RQ per side per window."""
        mm = self.cfg.mm
        times = rt.reprice_times[side]
        while times and mono - times[0] > mm.penny_rq_window_s:
            times.popleft()
        return mono - rt.last_reprice[side] >= mm.penny_min_gap_s and len(times) < mm.penny_max_rq

    def _requote(self, rt: ProductRuntime, fvs: FvState, view: BookView | None, now: float, mono: float) -> None:
        m, cfg = rt.market, self.cfg
        qp, mm = cfg.quote, cfg.mm
        all_live = self.ledger.live_orders(m.symbol)
        live = [o for o in all_live if o.kind != "stub"]
        stubs = [o for o in all_live if o.kind == "stub"]
        fv = fvs.fv
        book = view.book if view else None
        book_fresh = view is not None and mono - view.mono <= cfg.risk.cmi_stale_s
        timing = fvs.timing
        if fv is None:
            self.om.cancel_urgent([o.id for o in all_live if not o.canceling])
            return
        # Passive quotes centre on a model/market blend so inventory can mean-revert to 0 even when
        # the market disagrees with the model; snipes keep using the pure model F.
        if settle_override(m.symbol) is not None:  # override = directional mode: sweeps only, no passive quotes
            self.om.cancel_urgent([o.id for o in all_live if not o.canceling])
            rt.last_quotes = ()
            return
        if book is not None and book_fresh:
            b_ext, a_ext = best_external(book.buy_orders), best_external(book.sell_orders)
            if b_ext is not None and a_ext is not None and a_ext > b_ext:
                fv = replace(fv, f=MKT_BLEND * 0.5 * (b_ext + a_ext) + (1.0 - MKT_BLEND) * fv.f)

        passive_ok = fvs.mode in (MODE_LIVE, MODE_DEGRADED) and self.tradable(now) and timing.phase in (PHASE_LIVE, PHASE_PRE)
        if timing.phase == PHASE_LIVE and timing.tau_end <= cfg.endgame.t_passive_off:
            passive_ok = False
        if not passive_ok:  # feed stale / endgame / outside the window: pull everything now
            self.om.cancel_urgent([o.id for o in all_live if not o.canceling])
            rt.last_quotes = ()
            return

        pol, est = self.ledger.positions(m.symbol)
        soft = m.params.limit_soft
        pos_hi, pos_lo = max(pol, est), min(pol, est)
        budgets = {Side.BUY: max(0, soft - pos_hi), Side.SELL: max(0, soft + pos_lo)}
        allowed = frozenset((Side.BUY, Side.SELL))
        if rt.pnl < -cfg.risk.dd_stop:  # reducing side only
            allowed = frozenset((Side.SELL,) if est > 0 else (Side.BUY,) if est < 0 else ())
        side_h = {
            s: (qp.tox_mult if mono < rt.tox_until[s] else 1.0) * (cfg.risk.corr_mult if self._corr_side == s else 1.0)
            for s in (Side.BUY, Side.SELL)
        }
        size_mult = (cfg.risk.feed_down_size_mult if fvs.mode == MODE_DEGRADED else 1.0) * (0.5 if rt.disagree_active else 1.0)
        req = QuoteRequest(
            fv=fv, market=m, pos=est, q_target=rt.q_star, q_mm=self._qmm_eff(rt, timing), budgets=budgets, allowed=allowed,
            beta=self._beta(rt, mono),
            # Over the MM inventory limit: give up to K_GIVE_END on the reduce side to get back towards 0.
            k_give=max(self._k_give(timing), cfg.mm.k_give_end if abs(est - rt.q_star) > self._qmm_eff(rt, timing) else 0.0)
            + self._age_give(rt, mono),
            h_mult=2.0 if rt.pnl < -cfg.risk.dd_widen else 1.0, side_h_mult=side_h, size_mult=size_mult,
            book=book if book_fresh else None,
            penny={s: self._penny_ok(rt, s, mono) for s in (Side.BUY, Side.SELL)}, levels=qp.levels,
            **self._guards(rt, fvs, mono),
        )
        plan = compute_quotes(req, qp, mm)
        rt.last_plan, rt.last_quotes = plan, plan.quotes

        diff = diff_quotes(plan.quotes, live, plan, m, qp, mono)
        urgent = list(diff.urgent)
        cancels = list(diff.cancels)
        news = list(diff.news)
        bypass = False
        for side in (Side.BUY, Side.SELL):  # F-jump: replace that side urgently
            ref = rt.f_at_quote.get(side)
            if ref is not None and abs(fv.f - ref) >= 0.5 * plan.e_adv:
                moved = [o for o in cancels if o.side == side]
                urgent.extend(moved)
                cancels = [o for o in cancels if o.side != side]
                bypass = bypass or bool(moved or any(q.side == side for q in news))
        stub_cancels, stub_news = self._stub_diff(rt, fvs, plan, stubs, mono)
        urgent.extend(o for o in stub_cancels if mono < rt.guard_until and o.side == Side.SELL)
        if urgent:
            self.om.cancel_urgent([o.id for o in urgent])
        stub_cancels = [o for o in stub_cancels if o not in urgent]
        if not (cancels or news or stub_cancels or stub_news):
            return
        if self.om.batch_in_flight(m.symbol) or (not bypass and mono - rt.last_batch < cfg.loop.min_requote_interval):
            return
        # Penny throttle: drop reprices on sides that were repriced too recently.
        for side in (Side.BUY, Side.SELL):
            if not self._penny_ok(rt, side, mono) and not bypass:
                cancels = [o for o in cancels if o.side != side or order_invalid(o, plan, m)]
                news = [q for q in news if q.side != side or not any(o.side == side and not o.canceling for o in live)]
        if not (cancels or news or stub_cancels or stub_news):
            return
        order_news = [NewOrder(m.symbol, q.side, q.price, q.size, m.params.limit_soft) for q in news] + stub_news
        if self.om.submit_batch(m.symbol, [o.id for o in cancels + stub_cancels], order_news):
            rt.last_batch = mono
            for side in (Side.BUY, Side.SELL):
                if any(o.side == side for o in cancels) or any(q.side == side for q in news):
                    rt.last_reprice[side] = mono
                    rt.reprice_times[side].append(mono)
                    rt.f_at_quote[side] = fv.f

    def _stub_diff(self, rt: ProductRuntime, fvs: FvState, plan: QuotePlan, stubs: list[LiveOrder], mono: float) -> tuple[list[LiveOrder], list[NewOrder]]:
        """Deep resting stubs at F -/+ STUB_Z sigma (and optionally next to the price bounds) to catch fat-finger orders."""
        m, mm, fv = rt.market, self.cfg.mm, fvs.fv
        assert fv is not None
        k = m.tick
        wanted: dict[tuple[Side, str], tuple[float, int]] = {}
        for side in (Side.BUY, Side.SELL):
            if side == Side.SELL and mono < rt.guard_until:
                continue  # ask stubs are pulled while a spike / burst guard is active
            own = self.books.own_volume(m.symbol, side)
            held = sum(o.remaining for o in stubs if o.side == side)
            spare = self.ledger.capacity(m.symbol, side, m.params.limit_hard, own, False) + held - m.params.snipe_max_lots
            if spare <= 0:
                continue
            price = floor_k(fv.f - mm.stub_z * fv.sigma, k) if side == Side.BUY else ceil_k(fv.f + mm.stub_z * fv.sigma, k)
            if m.lo <= price <= m.hi and (price < fv.f if side == Side.BUY else price > fv.f):
                wanted[(side, "deep")] = (price, min(m.params.stub_lots, spare))
            if mm.stub_extreme and spare > 1:
                edge = round(m.lo + k, 6) if side == Side.BUY else round(m.hi - k, 6)
                if (edge < fv.f if side == Side.BUY else edge > fv.f) and (not (side, "deep") in wanted or wanted[(side, "deep")][0] != edge):
                    wanted[(side, "edge")] = (edge, 1)
        cancels: list[LiveOrder] = []
        news: list[NewOrder] = []
        pool = [o for o in stubs if not o.canceling]
        for (side, _), (price, size) in wanted.items():
            near = [o for o in pool if o.side == side and abs(o.price - price) <= fv.sigma]
            if near:
                pool.remove(min(near, key=lambda o: abs(o.price - price)))
                continue
            news.append(NewOrder(m.symbol, side, price, int(size), m.params.limit_hard, "stub"))
        cancels.extend(pool)
        return cancels, news

    def _guards(self, rt: ProductRuntime, fvs: FvState, mono: float) -> dict[str, float]:
        """Range spike guard / Trades burst guard: keep asks away from F (and smaller) while active."""
        mm, snap, m = self.cfg.mm, self._snap, rt.market
        if snap is None or fvs.fv is None:
            return {}
        if m.kind == PRODUCT_RANGE:
            if snap.last_price is None or snap.m_max is None or snap.m_min is None:
                return {}
            delta = max(mm.spike_abs, snap.sigma_hat * math.sqrt(mm.spike_h_s))
            if snap.last_price >= snap.m_max - delta or snap.last_price <= snap.m_min + delta:
                rt.guard_until = max(rt.guard_until, mono + 1.0)
                return {"ask_extra_abs": delta, "ask_size_mult": 0.5}
            return {}
        if snap.lam_burst is not None and snap.lam_burst > mm.burst_mult * snap.lam_hat:
            if mono >= rt.guard_until:
                log.warning("[%s] trade burst %.1f/s > %.1fx lambda_hat: asks widened", m.symbol, snap.lam_burst, mm.burst_mult)
            rt.guard_until = max(rt.guard_until, mono + mm.burst_pull_s)
        return {"ask_extra_eadv": 2.0} if mono < rt.guard_until else {}

    def _beta(self, rt: ProductRuntime, mono: float) -> float:
        """Adaptive beta: +0.5 per inventory half-life target exceeded by 2x, capped at beta_max."""
        mm = self.cfg.mm
        held = mono - rt.inv_since if rt.last_abs_q > 0 else 0.0
        extra = 0.5 * max(0, int(held // (2.0 * mm.inv_hl_target)))
        return min(mm.beta + extra, mm.beta_max)

    def _age_give(self, rt: ProductRuntime, mono: float) -> float:
        """Age escalator: after INV_AGE_S without a decrease, give up another tick per 10 s (as a fraction of sigma)."""
        mm = self.cfg.mm
        held = mono - rt.inv_since if rt.last_abs_q > 0 else 0.0
        if held <= mm.inv_age_s or rt.fvs is None or rt.fvs.fv is None:
            return 0.0
        steps = int((held - mm.inv_age_s) // 10.0)
        return steps * rt.market.tick / max(rt.fvs.fv.sigma, 1e-9)

    # -- fills -----------------------------------------------------------------------------
    def on_own_fill(self, symbol: str, side: Side, price: float, volume: int, passive_volume: int) -> None:
        """Spread P&L bookkeeping and toxicity: TOX_N passive fills on one side within TOX_WINDOW_S widen that side."""
        rt = self.rt.get(symbol)
        if rt is None:
            return
        fvs = rt.fvs
        if fvs is not None and fvs.fv is not None:
            sgn = 1.0 if side == Side.BUY else -1.0
            rt.spread_pnl += (fvs.fv.f - price) * sgn * volume * rt.market.contract_size
        if passive_volume <= 0:
            return
        mono = time.monotonic()
        qp = self.cfg.quote
        times = rt.fill_times[side]
        times.append(mono)
        while times and mono - times[0] > qp.tox_window_s:
            times.popleft()
        if len(times) >= qp.tox_n and mono >= rt.tox_until[side]:
            rt.tox_until[side] = mono + qp.tox_duration_s
            log.warning("[%s] toxicity: %d %s fills within %.0fs, widening for %.0fs", symbol, len(times), side, qp.tox_window_s, qp.tox_duration_s)

    def _status_lines(self, now: float) -> None:
        """Dry-run status: one line per product per second."""
        window = self.windows.window
        for rt in self.rt.values():
            fvs = rt.fvs
            if fvs is None:
                continue
            fv = fvs.fv
            view = self.books.get(rt.market.symbol)
            bid = best_external(view.book.buy_orders) if view else None
            ask = best_external(view.book.sell_orders) if view else None
            quotes = " ".join(f"{'B' if q.side == Side.BUY else 'S'}{q.level}:{q.size}@{q.price}" for q in rt.last_quotes)
            wstate = "no-window" if window is None else f"{fvs.timing.phase} {window[0]:.0f}->{window[1]:.0f} tau={fvs.timing.tau_end:.0f}s"
            log.info("STATUS [%s] %s mode=%s F=%s sd=%s LB=%s book=%s/%s q*=%.1f quotes: %s", rt.market.symbol, wstate, fvs.mode,
                     None if fv is None else round(fv.f, 2), None if fv is None else round(fv.sigma, 2),
                     None if fv is None else round(fv.lb, 2), bid, ask, rt.q_star, quotes or "-")

    # -- csv -------------------------------------------------------------------------------
    def _write_csv(self, now: float) -> None:
        if self._csv is None:
            LOG_DIR.mkdir(exist_ok=True)
            path = LOG_DIR / time.strftime("state_%Y%m%d_%H%M%S.csv", time.gmtime(now))
            self._csv_file = open(path, "w", newline="", encoding="utf-8")
            self._csv = csv.writer(self._csv_file)
            self._csv.writerow([
                "utc", "product", "mode", "phase", "tau_end", "F", "sigma", "LB", "rate", "obs",
                "pos_est", "pos_polled", "pnl", "spread_pnl", "inv_pnl", "q_star", "bids", "asks", "ext_bid", "ext_ask", "paused",
            ])
        for rt in self.rt.values():
            fvs = rt.fvs
            if fvs is None:
                continue
            fv = fvs.fv
            sym = rt.market.symbol
            view = self.books.get(sym)
            pol, est = self.ledger.positions(sym)
            bids = "|".join(f"{q.size}@{q.price}" for q in rt.last_quotes if q.side == Side.BUY)
            asks = "|".join(f"{q.size}@{q.price}" for q in rt.last_quotes if q.side == Side.SELL)
            self._csv.writerow([
                fmt_utc(now), sym, fvs.mode, fvs.timing.phase, f"{fvs.timing.tau_end:.1f}",
                "" if fv is None else f"{fv.f:.3f}", "" if fv is None else f"{fv.sigma:.3f}",
                "" if fv is None else f"{fv.lb:.3f}", "" if fv is None else f"{fv.rate:.4f}",
                "" if fv is None else f"{fv.obs:.2f}", est, pol, f"{rt.pnl:.1f}", f"{rt.spread_pnl:.1f}", f"{rt.inv_pnl:.1f}", f"{rt.q_star:.2f}", bids, asks,
                "" if view is None else best_external(view.book.buy_orders),
                "" if view is None else best_external(view.book.sell_orders), int(self.om.is_paused()),
            ])
        self._csv_file.flush()
