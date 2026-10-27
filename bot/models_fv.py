"""Fair-value, uncertainty and lower-bound models for Trades and Range (spec section 1 + addendum).

Pure code: no IO, no clocks, no locks. Callers own thread-safety.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, NamedTuple, Protocol, Sequence

from .config import RangeModelParams, TradesModelParams

LN2 = math.log(2.0)
SQRT_2PI = math.sqrt(2.0 * math.pi)
SECONDS_PER_YEAR = 365.0 * 86400.0

PHASE_PRE, PHASE_LIVE, PHASE_POST = "pre", "live", "post"


# ----------------------------------------------------------------------------------------------
# Window timing
# ----------------------------------------------------------------------------------------------
class Timing(NamedTuple):
    phase: str
    tau_end: float  # seconds to the window end (> length before the start, 0 after the end)
    tau_f: float  # remaining horizon the process still runs for (length before start, 0 after end)
    length: float


def compute_timing(now: float, window: tuple[float, float] | None, default_len: float) -> Timing:
    """Phase and horizons. With no window known the full default horizon is assumed (pre-window)."""
    if window is None:
        return Timing(PHASE_PRE, default_len, default_len, default_len)
    start, end = window
    length = end - start
    if now < start:
        return Timing(PHASE_PRE, (start - now) + length, length, length)
    if now < end:
        return Timing(PHASE_LIVE, end - now, end - now, length)
    return Timing(PHASE_POST, 0.0, 0.0, length)


# ----------------------------------------------------------------------------------------------
# Optional external inputs (addendum A2): produced by bot/external_feeds.py
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ExternalSignals:
    """Optional model inputs. The defaults mean "no external information"."""

    sigma_iv: float | None = None  # IV-implied, already realised-vol-scaled sigma in $/sqrt(s)
    w_iv: float = 0.0  # weight of sigma_iv^2 inside the slow variance anchor
    iv_event: bool = False  # short-dated ATM IV / DVOL above the event threshold
    fast_rate_factor: float = 1.0  # multiplier on the CB fast trade rate (Binance relative activity)


class ExternalSignalSource(Protocol):
    def latest(self, price: float | None, cb_rate_ratio: float | None) -> ExternalSignals:
        """Current signals. `price` is the last Coinbase price, `cb_rate_ratio` the CB fast/slow rate."""


# ----------------------------------------------------------------------------------------------
# Normal-distribution helpers (spec section 1.2)
# ----------------------------------------------------------------------------------------------
def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI


def norm_sf(x: float) -> float:
    return 0.5 * math.erfc(x / math.sqrt(2.0))


def g1(x: float) -> float:
    return norm_pdf(x) - x * norm_sf(x)


def g2(x: float) -> float:
    return (1.0 + x * x) * norm_sf(x) - x * norm_pdf(x)


def parkinson_var(high: float, low: float, width_s: float) -> float:
    """Parkinson variance rate ($^2/s) of one bar."""
    return (high - low) ** 2 / (4.0 * LN2 * width_s)


def _ewma_alpha(dt: float, half_life: float) -> float:
    return 1.0 - 2.0 ** (-dt / half_life)


# ----------------------------------------------------------------------------------------------
# Trades rate estimator (spec 1.1 + addendum 2 section 2)
# ----------------------------------------------------------------------------------------------
def prior_lambda(p: TradesModelParams, lam_regime: float | None, length: float) -> tuple[float | None, float]:
    """(lambda_0, C_INF) from the seasonal and regime priors, in log space; (None, C_INF) if neither exists."""
    season = p.season_count / length if p.season_enable else None
    regime = lam_regime if lam_regime is not None and lam_regime > 0 else None
    if season is not None and regime is not None:
        # Regime far outside the calibration sample: trust the live regime more and widen sigma.
        if abs(math.log(regime / season)) > 0.30:
            lam0 = math.exp(0.6 * math.log(regime) + 0.4 * math.log(season))
            return lam0, p.c_inf_regime_only
        lam0 = math.exp(p.w_regime * math.log(regime) + (1.0 - p.w_regime) * math.log(season))
        return lam0, p.c_inf_both
    if season is not None:
        return season, p.c_inf_season_only
    if regime is not None:
        return regime, p.c_inf_regime_only
    return None, p.c_inf_regime_only


class RateEstimator:
    """Fast trade-rate EWMA (heartbeat rates) plus the prior-to-window slow rate.

    Slow rate: lambda_0 before the window; inside it ln lam_slow = w ln lam_0 + (1-w) ln lam_win with
    w = T0 / (T0 + elapsed) and lam_win = n_obs / elapsed (once elapsed >= 10 s).
    """

    def __init__(self, params: TradesModelParams, lam_fallback: float) -> None:
        self.p = params
        self.lam_fallback = lam_fallback
        self.lam_fast: float | None = None
        self.t_live = 0.0

    def on_interval(self, dn: float, dt: float, lam_seed: float) -> None:
        """One heartbeat interval: `dn` new trades over `dt` seconds. The fast EWMA starts at `lam_seed`."""
        if dt <= 0.0 or dn < 0.0:
            return
        if self.lam_fast is None:
            self.lam_fast = lam_seed
        self.lam_fast += _ewma_alpha(dt, self.p.hl_fast) * (dn / dt - self.lam_fast)
        self.t_live += dt

    def lam0(self, lam_regime: float | None, length: float, event_active: bool) -> tuple[float, float]:
        """(lambda_0, C_INF) with the event multiplier applied to lambda_0."""
        lam0, c_inf = prior_lambda(self.p, lam_regime, length)
        if lam0 is None:
            lam0 = self.lam_fallback
        return lam0 * (self.p.event_rate_mult if event_active else 1.0), c_inf

    def lam_slow(self, lam0: float, n_obs: float | None, elapsed: float) -> float:
        if n_obs is None or elapsed < self.p.min_elapsed_s:
            return lam0
        w = self.p.t0 / (self.p.t0 + elapsed)
        lam_win = max(n_obs / elapsed, 1e-6)
        return math.exp(w * math.log(lam0) + (1.0 - w) * math.log(lam_win))

    def lam_hat(self, tau_end: float, lam0: float, lam_slow: float, ext: ExternalSignals) -> float:
        w = math.exp(-max(tau_end, 0.0) / self.p.t_mr)
        fast = (self.lam_fast if self.lam_fast is not None else lam0) * ext.fast_rate_factor
        return w * fast + (1.0 - w) * lam_slow


# ----------------------------------------------------------------------------------------------
# Range volatility estimator (spec 1.2 + addendum 2 section 2)
# ----------------------------------------------------------------------------------------------
class _BarEwma:
    """Parkinson variance rate of fixed-width bars built from trade prices; EWMA or a close callback."""

    def __init__(
        self, width_s: float, half_life: float, init_var: float, on_close: Callable[[float, float], None] | None = None
    ) -> None:
        self.width = width_s
        self.hl = half_life
        self.var = init_var
        self._on_close = on_close
        self._bucket: int | None = None
        self._hi = 0.0
        self._lo = 0.0

    def add(self, ts: float, price: float) -> None:
        bucket = int(ts // self.width)
        if self._bucket is None or bucket > self._bucket:
            self._close()
            self._bucket, self._hi, self._lo = bucket, price, price
        elif bucket == self._bucket:
            self._hi = max(self._hi, price)
            self._lo = min(self._lo, price)
        # Trades older than the open bar (late backfill) only matter for M/m, not for the bars.

    def _close(self) -> None:
        if self._bucket is None:
            return
        v = parkinson_var(self._hi, self._lo, self.width)
        if self._on_close is not None:
            self._on_close(self._bucket * self.width, v)
        else:
            self.var += _ewma_alpha(self.width, self.hl) * (v - self.var)


class VolEstimator:
    """Range volatility.

    - fast: 10 s bar Parkinson EWMA.
    - regime: 1-min bar Parkinson EWMA seeded from the 30 min before the window; frozen once the window starts.
    - live: plain mean of the in-window 1-min bar variances.
    sigma_anchor = exp(W ln sigma_regime + (1-W) ln sigma_season) (optionally IV-anchored), and
    sig2_slow = w sig2_anchor + (1-w) sig2_live with w = T0 / (T0 + elapsed).
    """

    def __init__(self, params: RangeModelParams, sig2_regime: float | None, sig2_fallback: float, length: float) -> None:
        self.p = params
        self._season2 = (params.season_range / (1.596 * math.sqrt(length))) ** 2 if params.season_enable else None
        init = sig2_regime if sig2_regime is not None else (self._season2 or sig2_fallback)
        self.regime_ok = sig2_regime is not None
        self.season_ok = self._season2 is not None
        self._fallback2 = sig2_fallback
        self._fast = _BarEwma(10.0, params.hl_fast, init)
        self._regime2 = init
        self._slow = _BarEwma(60.0, params.hl_slow, init, on_close=self._on_minute)
        self._win_start: float | None = None
        self._live_sum = 0.0
        self._live_n = 0

    def set_window_start(self, start: float) -> None:
        self._win_start = start

    def _on_minute(self, bucket_start: float, var: float) -> None:
        if self._win_start is not None and bucket_start >= self._win_start:
            self._live_sum += var
            self._live_n += 1
        else:
            self._regime2 += _ewma_alpha(60.0, self.p.hl_slow) * (var - self._regime2)

    def on_trade(self, ts: float, price: float) -> None:
        self._fast.add(ts, price)
        self._slow.add(ts, price)

    def sigma_regime(self) -> float | None:
        return math.sqrt(self._regime2) if self.regime_ok else None

    def sigma_season(self) -> float | None:
        return None if self._season2 is None else math.sqrt(self._season2)

    def sigma_fast(self) -> float:
        return math.sqrt(max(self._fast.var, 0.0))

    def sigma_anchor(self, ext: ExternalSignals, event_active: bool) -> float:
        reg = self._regime2 if self.regime_ok else None
        if reg is not None and self._season2 is not None:
            w = self.p.w_regime
            sig2 = math.exp(w * math.log(reg) + (1.0 - w) * math.log(self._season2))
        else:
            sig2 = reg if reg is not None else (self._season2 or self._fallback2)
        sig2 *= self.p.event_sigma_mult**2 if event_active else 1.0
        if ext.sigma_iv is not None and ext.w_iv > 0.0:
            sig2 = ext.w_iv * ext.sigma_iv**2 + (1.0 - ext.w_iv) * sig2
        return math.sqrt(sig2)

    def sigma_hat(self, tau_end: float, elapsed: float, ext: ExternalSignals, event_active: bool) -> float:
        anchor2 = self.sigma_anchor(ext, event_active) ** 2
        if self._live_n > 0 and elapsed > 0.0:
            w_a = self.p.t0 / (self.p.t0 + elapsed)
            slow = w_a * anchor2 + (1.0 - w_a) * (self._live_sum / self._live_n)
        else:
            slow = anchor2
        w = math.exp(-max(tau_end, 0.0) / self.p.t_mr)
        return math.sqrt(max(w * self._fast.var + (1.0 - w) * slow, 0.0))


# ----------------------------------------------------------------------------------------------
# Fair values
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class FairValue:
    f: float  # fair value
    sigma: float  # standard deviation of the settlement around f
    lb: float  # hard lower bound on the settlement
    rate: float  # lambda_hat (trades/s) or sigma_hat ($/sqrt(s))
    obs: float  # n_obs or R_now


def trades_fair_value(
    p: TradesModelParams, n_obs: float, timing: Timing, lam_hat: float, c_inf: float, max_price: float
) -> FairValue:
    """Spec 1.1. `c_inf` is the effective C_INF (static-only / event adjustments already applied)."""
    mu_rem = lam_hat * timing.tau_f
    c = c_inf * math.sqrt(timing.tau_f / timing.length)
    var = p.phi * mu_rem + (c * mu_rem) ** 2 + p.sigma_conv**2
    f = min(max(n_obs + mu_rem, n_obs), max_price)
    lb = n_obs - (p.lb_margin_const + lam_hat * p.lb_margin_rate_s)
    return FairValue(f=f, sigma=math.sqrt(var), lb=lb, rate=lam_hat, obs=n_obs)


def range_extension(m_max: float, m_min: float, last: float, s: float) -> tuple[float, float, float, float]:
    """(U, D, VU, VD): expected extension of the running max (up) and min (down) and their variances."""
    if s <= 0.0:
        return 0.0, 0.0, 0.0, 0.0
    u = (m_max - last) / s
    d = (last - m_min) / s
    up = 2.0 * s * g1(u)
    down = 2.0 * s * g1(d)
    vu = max(2.0 * s * s * g2(u) - up * up, 0.0)
    vd = max(2.0 * s * s * g2(d) - down * down, 0.0)
    return up, down, vu, vd


def range_fair_value(
    p: RangeModelParams,
    m_max: float,
    m_min: float,
    last: float,
    sigma_hat: float,
    timing: Timing,
    c_sigma: float,
    max_price: float,
) -> FairValue:
    """Spec 1.2. Before the window M = m = S so the caller passes m_max = m_min = last."""
    r_now = m_max - m_min
    s = sigma_hat * math.sqrt(max(timing.tau_f, 0.0))
    up, down, vu, vd = range_extension(m_max, m_min, last, s)
    ext = up + down
    var = (
        p.kappa**2 * (vu + vd + 2.0 * p.rho_ud * math.sqrt(vu * vd))
        + (c_sigma * p.kappa * ext) ** 2
        + p.sigma_conv**2
    )
    f = min(max(r_now + p.kappa * ext, r_now), max_price)
    return FairValue(f=f, sigma=math.sqrt(max(var, 0.0)), lb=r_now - p.lb_margin, rate=sigma_hat, obs=r_now)


# ----------------------------------------------------------------------------------------------
# Priors (pure parts; the REST fetching lives in coinbase_feed / external_feeds)
# ----------------------------------------------------------------------------------------------
def sigma2_from_candles(candles: Sequence[tuple[float, float, float]]) -> float | None:
    """Mean Parkinson variance rate of 1-minute candles given as (time, low, high); None if empty."""
    if not candles:
        return None
    return sum((hi - lo) ** 2 for _, lo, hi in candles) / len(candles) / (4.0 * LN2 * 60.0)


def iv_to_sigma(iv_pct: float, price: float) -> float:
    """Annualised implied vol (in percent) to $/sqrt(s)."""
    return (iv_pct / 100.0) * price / math.sqrt(SECONDS_PER_YEAR)
