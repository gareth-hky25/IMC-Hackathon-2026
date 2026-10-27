"""Entry point: `python -m bot.main_bot` (or `python run_bot.py`) from the repo root."""
from __future__ import annotations

import logging
import math
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from base_bot import BaseBot  # noqa: E402  (exchange-provided SDK; must not be modified)
from models import OrderBook, Product, Side, Trade  # noqa: E402

from bot import config as cfgmod  # noqa: E402
from bot.coinbase_feed import CoinbaseRest, CoinbaseState, CoinbaseWsThread, GapBackfiller, seed_window_history  # noqa: E402
from bot.config import CONFIG, Config  # noqa: E402
from bot.execution import Ledger, OrderManager, PriorityRateLimiter, SessionApiMixin  # noqa: E402
from bot.external_feeds import BinanceRateFeed, DeribitIvFeed, ExternalHub  # noqa: E402
from bot.models_fv import RateEstimator, VolEstimator  # noqa: E402
from bot.strategy import BookStore, Market, Strategy  # noqa: E402

log = logging.getLogger("bot.main")


def setup_logging() -> None:
    cfgmod.LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)-7s %(threadName)s %(name)s: %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    path = cfgmod.LOG_DIR / time.strftime("bot_%Y%m%d_%H%M%S.log", time.gmtime())
    file = logging.FileHandler(path, encoding="utf-8")
    file.setFormatter(fmt)
    root.addHandler(console)
    root.addHandler(file)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


class MarketMakerBot(SessionApiMixin, BaseBot):
    """Wires the feeds, models, executor and strategy around the exchange SDK."""

    def __init__(self, cfg: Config) -> None:
        super().__init__(cfgmod.CMI_URL, cfgmod.USERNAME, cfgmod.PASSWORD, cfgmod.INVITE)
        self.http_timeout = cfg.loop.http_timeout
        self.dry_run = cfg.dry_run
        self.cfg = cfg
        self.books = BookStore(cfg.snipe.gap_history)
        self.ledger: Ledger | None = None
        self.strategy: Strategy | None = None
        self._first_cmi: float | None = None
        self._first_fill_logged = False
        self._fill_seen = threading.Event()
        self._stop = threading.Event()

    # -- SSE callbacks (no REST calls here) -------------------------------------------------
    def _mark_cmi_event(self) -> None:
        if self._first_cmi is None:
            self._first_cmi = time.time()
            log.warning("first CMI SSE event at %s", cfgmod.fmt_utc(self._first_cmi))

    def first_cmi_event(self) -> float | None:
        return self._first_cmi

    def on_orderbook(self, orderbook: OrderBook) -> None:
        self._mark_cmi_event()
        view = self.books.set(orderbook)
        if self.strategy is not None:
            self.strategy.on_book(view)

    def on_trades(self, trades: list[Trade]) -> None:
        self._mark_cmi_event()
        if self.ledger is None or self.strategy is None:
            return
        for t in trades:
            buyer, seller = t.buyer == self.username, t.seller == self.username
            if buyer == seller:  # not ours, or a self-trade (net zero)
                continue
            side = Side.BUY if buyer else Side.SELL
            vol = int(round(float(t.volume)))
            key = t.id or f"{t.timestamp}|{t.price}|{t.volume}|{t.buyer}|{t.seller}"
            res = self.ledger.apply_fill(t.product, side, float(t.price), vol, str(key))
            if not res.is_new:
                continue
            logging.getLogger("bot.orders").info("FILL %s %s %d@%s passive=%d aggressor=%s", t.product, side, vol, t.price, res.passive_volume, t.aggressor)
            self.strategy.on_own_fill(t.product, side, float(t.price), vol, res.passive_volume)
            self._fill_seen.set()

    # -- startup ---------------------------------------------------------------------------
    def _load_markets(self) -> list[Market]:
        products = self.request_all_products()
        if not products:
            raise RuntimeError("could not load products from the exchange")
        by_name = {p.symbol.lower(): p for p in products}
        log.info("exchange products: %s", [(p.symbol, p.tickSize, p.startingPrice, p.minPrice, p.maxPrice, p.contractSize) for p in products])
        markets: list[Market] = []
        for pp in self.cfg.products:
            prod: Product | None = by_name.get(pp.default_symbol.lower())
            if prod is None:
                log.warning("product %r (%s) not found among %s: set %s_SYMBOL; this product will not be traded",
                            pp.default_symbol, pp.kind, [p.symbol for p in products], pp.kind.upper())
                continue
            markets.append(Market(
                symbol=prod.symbol, params=pp, tick=float(prod.tickSize),
                lo=float(prod.minPrice) if prod.minPrice is not None else pp.fallback_min,
                hi=float(prod.maxPrice) if prod.maxPrice is not None else pp.fallback_max,
                contract_size=float(prod.contractSize) if prod.contractSize is not None else pp.contract_size,
            ))
        if not markets:
            raise RuntimeError("no configured product found on the exchange")
        self._starting = {p.symbol: float(p.startingPrice) for p in products}
        return markets

    def run(self) -> None:
        cfg = self.cfg
        cfgmod.setup_tls()
        markets = self._load_markets()
        symbols = [m.symbol for m in markets]
        self.ledger = ledger = Ledger(symbols)
        limiter = PriorityRateLimiter(cfg.loop.max_rps)
        om = OrderManager(self, ledger, limiter, cfg.loop, cfg.risk, self.books.own_volume, symbols)

        om.cancel_all_blocking()
        try:
            log.info("startup positions: request_positions=%s request_net_positions=%s", self.request_positions(), self.request_net_positions())
        except Exception as exc:
            log.warning("startup position log failed: %s", exc)
        om.poll_positions()

        windows = cfgmod.WindowManager(
            cfgmod.load_window_config(cfgmod.WINDOW_CONFIG_PATH, cfg.window), cfgmod.WINDOW_CONFIG_PATH,
            cfg.window_reload_s, cfg.endgame.hard_stop_at_end,
        )
        length = windows.cfg.len_s
        now = time.time()
        start0 = cfgmod.parse_utc(windows.cfg.start_utc)

        # --- seeding (REST) ---
        rest = CoinbaseRest(cfg.cb.rest_max_rps)
        by_kind = {m.kind: m for m in markets}
        t_ref = min(start0 if start0 is not None else now, now)
        t_then = min((start0 if start0 is not None else now) - cfg.trades_model.regime_lookback_s, now - 120.0)
        anchor = rest.regime_anchor(t_then, cfg.trades_model.season_count / length)
        sig2_regime = rest.sigma2_regime(t_ref)
        log.info("seed: regime anchor=%s sigma2_regime=%s (t_ref %s)", anchor, sig2_regime, cfgmod.fmt_utc(t_ref))
        trades_m, range_m = by_kind.get(cfgmod.PRODUCT_TRADES), by_kind.get(cfgmod.PRODUCT_RANGE)
        lam_fallback = (self._starting[trades_m.symbol] / length) if trades_m else 5.0
        sig_fallback = (self._starting[range_m.symbol] / (1.596 * math.sqrt(length))) if range_m else 3.0

        rate = RateEstimator(cfg.trades_model, lam_fallback)
        vol = VolEstimator(cfg.range_model, sig2_regime, sig_fallback**2, length)
        state = CoinbaseState(rate, vol, cfg.cb, length, cfg.events.c_mult, anchor)

        deribit = DeribitIvFeed(cfg.external) if cfg.external.deribit_enable else None
        binance = BinanceRateFeed(cfg.external) if cfg.external.binance_enable else None
        hub = ExternalHub(deribit, binance)
        if deribit is not None:
            deribit.refresh_once()
        hub.start()

        ws = CoinbaseWsThread(state, cfg.cb)
        ws.start()
        GapBackfiller(state, rest).start()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not state.snapshot(time.time(), hub.latest(None, None), False).ready:
            time.sleep(0.2)

        self.strategy = strategy = Strategy(
            cfg, markets, state, hub, om, ledger, self.books, windows,
            lambda start: seed_window_history(state, rest, start), self.first_cmi_event,
        )
        poller = threading.Thread(target=self._poll_loop, args=(om,), name="poller", daemon=True)
        poller.start()
        self.start()  # SSE thread
        strategy.start()
        log.info("bot running%s; window config: %s", " (DRY RUN: no orders will be sent)" if cfg.dry_run else "", windows.cfg)
        try:
            while not strategy.halted and not self._stop.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            log.warning("interrupted")
        finally:
            self._shutdown(om, strategy, ws, hub)

    def _poll_loop(self, om: OrderManager) -> None:
        last_orders = 0.0
        while not self._stop.wait(self.cfg.loop.pos_poll_s):
            om.poll_positions()
            if time.monotonic() - last_orders >= self.cfg.loop.orders_poll_s:
                last_orders = time.monotonic()
                om.reconcile()
            if self._fill_seen.is_set() and not self._first_fill_logged:
                self._first_fill_logged = True
                try:
                    log.info("after first fill: request_positions=%s request_net_positions=%s", self.request_positions(), self.request_net_positions())
                except Exception as exc:
                    log.warning("position log failed: %s", exc)

    def _shutdown(self, om: OrderManager, strategy: Strategy, ws: CoinbaseWsThread, hub: ExternalHub) -> None:
        self._stop.set()
        strategy.stop()
        try:
            om.cancel_all_blocking()
        except Exception:
            log.exception("final cancel-all failed")
        ws.stop()
        hub.stop()
        self.stop()
        om.shutdown()
        log.info("shutdown complete")


def main() -> None:
    setup_logging()
    cfgmod.setup_tls()
    MarketMakerBot(CONFIG).run()


if __name__ == "__main__":
    main()
