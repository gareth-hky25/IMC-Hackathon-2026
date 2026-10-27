"""Optional external data sources (addendum A2): Deribit implied vol for Range, Binance trade rate for Trades.

Every source degrades to "no information" (ExternalSignals defaults) on any failure, including TLS errors.
`ExternalHub` implements the `ExternalSignalSource` protocol consumed by the strategy.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

from .config import ExternalParams
from .models_fv import ExternalSignals, iv_to_sigma

log = logging.getLogger(__name__)

DERIBIT_URL = "https://www.deribit.com/api/v2/public"
BINANCE_TRADES_URL = "https://data-api.binance.vision/api/v3/trades"
_MONTHS = {m: i + 1 for i, m in enumerate(["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"])}


def _get_json(session: requests.Session, url: str, params: dict[str, Any], timeout: float, what: str) -> Any | None:
    try:
        resp = session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.SSLError as exc:
        log.error("%s: TLS verification failed (is `truststore` installed?): %s", what, exc)
    except (requests.RequestException, ValueError) as exc:
        log.warning("%s: request failed: %s", what, exc)
    return None


def parse_option_name(name: str) -> tuple[float, float, str] | None:
    """`BTC-31OCT26-87000-C` -> (expiry epoch at 08:00 UTC, strike, 'C'/'P'); None if malformed."""
    try:
        _, date, strike, kind = name.split("-")
        day = int(date[:-5])
        month = _MONTHS[date[-5:-2].upper()]
        year = 2000 + int(date[-2:])
        expiry = datetime(year, month, day, 8, tzinfo=timezone.utc).timestamp()
        return expiry, float(strike), kind.upper()
    except (ValueError, KeyError):
        return None


def pick_atm_iv(summaries: list[dict[str, Any]], now: float, min_tte_s: float) -> float | None:
    """Mean mark_iv (%) of call and put at the strike nearest the underlying, for the nearest expiry with
    at least `min_tte_s` to go. Instruments with a zero or missing mark_iv are skipped."""
    by_expiry: dict[float, dict[float, dict[str, float]]] = {}
    underlying: dict[float, float] = {}
    for row in summaries:
        parsed = parse_option_name(str(row.get("instrument_name", "")))
        if parsed is None:
            continue
        expiry, strike, kind = parsed
        try:
            iv = float(row.get("mark_iv") or 0.0)
            und = float(row["underlying_price"])
        except (KeyError, TypeError, ValueError):
            continue
        if expiry - now < min_tte_s or iv <= 0.0:
            continue
        by_expiry.setdefault(expiry, {}).setdefault(strike, {})[kind] = iv
        underlying[expiry] = und
    for expiry in sorted(by_expiry):
        strikes = by_expiry[expiry]
        for strike in sorted(strikes, key=lambda k: abs(k - underlying[expiry])):
            ivs = list(strikes[strike].values())
            if ivs:
                return sum(ivs) / len(ivs)
    return None


class DeribitIvFeed(threading.Thread):
    """Polls short-dated ATM implied vol (primary) and DVOL (fallback / event ratio) every IV_REFRESH_S."""

    def __init__(self, params: ExternalParams) -> None:
        super().__init__(name="deribit-iv", daemon=True)
        self._p = params
        self._session = requests.Session()
        self._lock = threading.Lock()
        self._atm_iv: float | None = None
        self._dvol: float | None = None
        self._updated_mono: float | None = None
        self.iv_rv_ratio = params.iv_rv_ratio
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                self.refresh_once()
            except Exception:
                log.exception("Deribit refresh crashed")
            self._stop_evt.wait(self._p.iv_refresh_s)

    def refresh_once(self) -> bool:
        now = time.time()
        data = _get_json(
            self._session,
            f"{DERIBIT_URL}/get_book_summary_by_currency",
            {"currency": "BTC", "kind": "option"},
            self._p.http_timeout_s,
            "Deribit book summary",
        )
        atm = None
        if isinstance(data, dict) and isinstance(data.get("result"), list):
            atm = pick_atm_iv(data["result"], now, self._p.iv_min_tte_h * 3600.0)
        dvol = self._fetch_dvol(now)
        with self._lock:
            if atm is not None or dvol is not None:
                self._atm_iv, self._dvol, self._updated_mono = atm, dvol, time.monotonic()
        if atm is None and dvol is None:
            log.warning("Deribit: no implied-vol data; W_IV falls back to 0")
            return False
        log.info("Deribit IV: ATM %s%%  DVOL %s%%", None if atm is None else round(atm, 1), None if dvol is None else round(dvol, 1))
        return True

    def _fetch_dvol(self, now: float) -> float | None:
        data = _get_json(
            self._session,
            f"{DERIBIT_URL}/get_volatility_index_data",
            {
                "currency": "BTC",
                "start_timestamp": int((now - 600) * 1000),
                "end_timestamp": int(now * 1000),
                "resolution": 60,
            },
            self._p.http_timeout_s,
            "Deribit DVOL",
        )
        try:
            return float(data["result"]["data"][-1][4])
        except (TypeError, KeyError, IndexError, ValueError):
            return None

    def signals(self, price: float | None) -> ExternalSignals:
        with self._lock:
            atm, dvol, updated = self._atm_iv, self._dvol, self._updated_mono
        iv = atm if atm is not None else dvol
        if iv is None or updated is None or price is None or time.monotonic() - updated > self._p.iv_stale_s:
            return ExternalSignals()
        sigma = self.iv_rv_ratio * iv_to_sigma(iv, price)
        event = atm is not None and dvol is not None and dvol > 0 and atm / dvol > self._p.iv_event_ratio
        return ExternalSignals(sigma_iv=sigma, w_iv=self._p.w_iv, iv_event=event)


class BinanceRateFeed(threading.Thread):
    """BTCUSDT trade rate from trade-id differences (REST poll, ~1 Hz): fast/slow EWMAs for relative activity."""

    def __init__(self, params: ExternalParams, hl_fast: float = 30.0, hl_slow: float = 600.0) -> None:
        super().__init__(name="binance-rate", daemon=True)
        self._p = params
        self._hl = (hl_fast, hl_slow)
        self._session = requests.Session()
        self._lock = threading.Lock()
        self._fast: float | None = None
        self._slow: float | None = None
        self._t_live = 0.0
        self._prev: tuple[float, int] | None = None
        self._updated_mono: float | None = None
        self._stop_evt = threading.Event()

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        while not self._stop_evt.wait(1.0):
            try:
                self._poll()
            except Exception:
                log.exception("Binance poll crashed")

    def _poll(self) -> None:
        data = _get_json(self._session, BINANCE_TRADES_URL, {"symbol": "BTCUSDT", "limit": 1}, self._p.http_timeout_s, "Binance trades")
        try:
            tid = int(data[0]["id"])
        except (TypeError, KeyError, IndexError, ValueError):
            return
        now = time.monotonic()
        with self._lock:
            if self._prev is not None and now > self._prev[0]:
                dt = now - self._prev[0]
                x = (tid - self._prev[1]) / dt
                if self._fast is None or self._slow is None:
                    self._fast = self._slow = x
                else:
                    self._fast += (1.0 - 2.0 ** (-dt / self._hl[0])) * (x - self._fast)
                    self._slow += (1.0 - 2.0 ** (-dt / self._hl[1])) * (x - self._slow)
                self._t_live += dt
                self._updated_mono = now
            self._prev = (now, tid)

    def factor(self, cb_rate_ratio: float | None) -> float:
        """lambda_fast multiplier clip((rB/rCB)^W_BIN, 0.67, 1.5); 1 when unavailable or still warming up."""
        with self._lock:
            fast, slow, t_live, updated = self._fast, self._slow, self._t_live, self._updated_mono
        if (
            fast is None or slow is None or slow <= 0 or cb_rate_ratio is None or cb_rate_ratio <= 0
            or t_live < 120.0 or updated is None or time.monotonic() - updated > 30.0
        ):
            return 1.0
        return min(max(((fast / slow) / cb_rate_ratio) ** self._p.w_bin, 0.67), 1.5)


class ExternalHub:
    """Combines the enabled sources into one `ExternalSignals` (implements ExternalSignalSource)."""

    def __init__(self, deribit: DeribitIvFeed | None, binance: BinanceRateFeed | None) -> None:
        self.deribit = deribit
        self.binance = binance

    def start(self) -> None:
        for src in (self.deribit, self.binance):
            if src is not None:
                src.start()

    def stop(self) -> None:
        for src in (self.deribit, self.binance):
            if src is not None:
                src.stop()

    def latest(self, price: float | None, cb_rate_ratio: float | None) -> ExternalSignals:
        base = self.deribit.signals(price) if self.deribit is not None else ExternalSignals()
        factor = self.binance.factor(cb_rate_ratio) if self.binance is not None else 1.0
        if not math.isfinite(factor):
            factor = 1.0
        return ExternalSignals(base.sigma_iv, base.w_iv, base.iv_event, factor)
