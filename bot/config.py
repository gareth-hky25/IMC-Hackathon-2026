"""All tunable parameters (spec section 6 plus the addendum) and the live-reloadable window config.

Environment variables (with defaults):
    CMI_URL, CMI_USERNAME, CMI_PASSWORD, CMI_INVITE, TRADES_SYMBOL, RANGE_SYMBOL, DRY_RUN

The plain ``USERNAME`` variable is deliberately not used: Windows sets it to the OS account name.
"""
from __future__ import annotations

import json
import logging
import time
import os
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs"
WINDOW_CONFIG_PATH = REPO_ROOT / "window_config.json"

def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Set the {name} environment variable (see docs/STRATEGY.md, 'Running the bot').")
    return value


CMI_URL = _require_env("CMI_URL")
USERNAME = _require_env("CMI_USERNAME")
PASSWORD = _require_env("CMI_PASSWORD")
INVITE = os.environ.get("CMI_INVITE") or None
TRADES_SYMBOL = os.environ.get("TRADES_SYMBOL", "Trades")
RANGE_SYMBOL = os.environ.get("RANGE_SYMBOL", "Range")
# DRY_RUN=1: everything runs except order / cancel REST calls, which are only logged.
DRY_RUN = os.environ.get("DRY_RUN", "0").strip().lower() in ("1", "true", "yes", "on")

# The only price source for both products: Coinbase BTC-USD (never BTC-USDT / BTC-USDC / Binance BTCUSDT).
CB_PRODUCT_ID = "BTC-USD"

PRODUCT_TRADES = "trades"
PRODUCT_RANGE = "range"


def parse_utc(text: object) -> float | None:
    """ISO-8601 timestamp (``Z`` suffix, any fractional digits) to epoch seconds; None if unparseable."""
    if not isinstance(text, str) or not text:
        return None
    try:
        dt = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def fmt_utc(ts: float | None) -> str:
    if ts is None:
        return "None"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def setup_tls() -> None:
    """Use the OS trust store when `truststore` is installed (addendum A4); never raises."""
    try:
        import truststore  # type: ignore[import-not-found]

        truststore.inject_into_ssl()
        log.info("truststore injected: using the OS certificate store")
    except ImportError:
        log.info("truststore not installed: using the default certificate bundle")
    except Exception:  # pragma: no cover - defensive
        log.exception("truststore injection failed; continuing with the default certificate bundle")


# ----------------------------------------------------------------------------------------------
# Window (reloadable)
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class WindowConfig:
    """Settlement window settings, re-read from `window_config.json` every 5 s.

    `start_utc` (WINDOW_START_UTC), when set, always wins over the mode's auto-detection.
    Modes: "cmi_open" (start = first CMI SSE event), "launch" (first CB heartbeat),
    "utc" (start_utc required), "aligned" (floor to align_min).
    """

    mode: str = "cmi_open"
    start_utc: str | None = None
    len_s: float = 900.0
    align_min: int = 15
    align_next: bool = False
    cmi_close_utc: str | None = None


_WINDOW_KEYS = {f.name for f in fields(WindowConfig)}


def load_window_config(path: Path, current: WindowConfig) -> WindowConfig:
    """Read the JSON file; keys are case-insensitive and may carry a WINDOW_ prefix. On any
    problem the `current` config is returned unchanged."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("top-level JSON value must be an object")
    except FileNotFoundError:
        return current
    except (OSError, ValueError) as exc:
        log.warning("window config %s unreadable (%s); keeping previous settings", path, exc)
        return current
    values: dict[str, object] = {}
    for key, value in raw.items():
        name = str(key).lower()
        name = name.removeprefix("window_")
        if name in _WINDOW_KEYS:
            values[name] = value
        else:
            log.warning("window config: unknown key %r ignored", key)
    try:
        cfg = WindowConfig(**{**current.__dict__, **values})
        cfg = WindowConfig(
            mode=str(cfg.mode).lower(),
            start_utc=cfg.start_utc or None,
            len_s=float(cfg.len_s),
            align_min=int(cfg.align_min),
            align_next=bool(cfg.align_next),
            cmi_close_utc=cfg.cmi_close_utc or None,
        )
    except (TypeError, ValueError) as exc:
        log.warning("window config invalid (%s); keeping previous settings", exc)
        return current
    if cfg.mode not in ("cmi_open", "launch", "utc", "aligned"):
        log.warning("window config: unknown mode %r; keeping previous settings", cfg.mode)
        return current
    if cfg.start_utc is not None and parse_utc(cfg.start_utc) is None:
        log.warning("window config: unparseable WINDOW_START_UTC %r; keeping previous settings", cfg.start_utc)
        return current
    if cfg.len_s <= 0 or cfg.align_min <= 0:
        log.warning("window config: non-positive length; keeping previous settings")
        return current
    return cfg


# ----------------------------------------------------------------------------------------------
# Static parameters
# ----------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class CbParams:
    stale_s: float = 3.0
    resume_heartbeats: int = 2
    rest_max_rps: float = 5.0
    idle_reconnect_s: float = 10.0


@dataclass(frozen=True)
class TradesModelParams:
    hl_fast: float = 30.0
    t_mr: float = 120.0
    phi: float = 3.0
    sigma_conv: float = 20.0
    lb_margin_const: float = 20.0
    lb_margin_rate_s: float = 1.0
    # Addendum 2 section 2: seasonal + regime prior and in-window update.
    season_enable: bool = True
    season_count: float = 5280.0
    w_regime: float = 0.25
    t0: float = 300.0
    min_elapsed_s: float = 10.0
    regime_lookback_s: float = 1800.0
    c_inf_both: float = 0.10
    c_inf_season_only: float = 0.15
    c_inf_regime_only: float = 0.25
    event_rate_mult: float = 1.5


@dataclass(frozen=True)
class RangeModelParams:
    # Units: everything here is in USD of the Coinbase BTC-USD price (sigma in USD/sqrt(s), range in USD).
    hl_fast: float = 60.0
    hl_slow: float = 900.0
    t_mr: float = 300.0
    rho_ud: float = -0.69
    c_sigma: float = 0.10
    kappa: float = 1.0
    sigma_conv: float = 1.0
    lb_margin: float = 2.0
    event_sigma_mult: float = 1.5
    season_enable: bool = True
    season_range: float = 166.0
    w_regime: float = 0.70
    t0: float = 300.0
    regime_lookback_s: float = 1800.0


@dataclass(frozen=True)
class EventParams:
    """Scheduled-event handling (addendum A1); `times_utc` holds ISO strings."""

    times_utc: tuple[str, ...] = ()
    c_mult: float = 1.5


@dataclass(frozen=True)
class ExternalParams:
    deribit_enable: bool = False
    w_iv: float = 0.25
    iv_rv_ratio: float = 0.65
    iv_refresh_s: float = 60.0
    iv_stale_s: float = 300.0
    iv_min_tte_h: float = 2.0
    iv_event_ratio: float = 1.3
    binance_enable: bool = False
    w_bin: float = 0.5
    http_timeout_s: float = 5.0


@dataclass(frozen=True)
class QuoteParams:
    k_h: float = 0.15
    k_floor: float = 0.05
    beta: float = 1.0
    levels: int = 2
    penny: bool = True
    rq_frac: float = 0.3
    topup_frac: float = 0.5
    topup_min_age_s: float = 2.0
    tox_n: int = 3
    tox_mult: float = 1.5
    tox_window_s: float = 15.0
    tox_duration_s: float = 30.0


@dataclass(frozen=True)
class SnipeParams:
    z_inc: float = 0.5
    z_red: float = 0.25
    theta_min_ticks: float = 3.0
    cooldown_s: float = 0.3
    # Class 2 (outlier / fat finger), addendum 2 section 5.
    z_fat: float = 2.0
    z_cons: float = 1.0
    gap_k: float = 5.0
    gap_min_ticks: float = 5.0
    gap_history: int = 50
    fresh_s: float = 1.5
    jump_window_s: float = 10.0
    jump_frac: float = 0.5


@dataclass(frozen=True)
class MmParams:
    """Pure-MM quoting and hybrid opening (addendum 2 sections 3-4)."""

    mode: str = "hybrid"  # "hybrid" | "pure" | "directional"
    q_mm_frac: float = 0.5
    beta: float = 2.0
    beta_max: float = 4.0
    k_give: float = 0.10
    k_give_end: float = 0.5
    k_adv: float = 0.05
    t_flat: float = 120.0
    t_hit: float = 30.0
    inv_age_s: float = 45.0
    inv_hl_target: float = 60.0
    penny_min_gap_s: float = 0.5
    penny_max_rq: int = 10
    penny_rq_window_s: float = 10.0
    lock_frac: float = 0.5
    # hybrid opening
    z_open_dir: float = 1.5
    z_ramp: float = 1.0
    q_dir_frac: float = 0.5
    agree_max: float = 0.30
    sigma_log_emp_trades: float = 0.10
    sigma_log_emp_range: float = 0.30
    fresh_s: float = 1.5
    min_live_s: float = 120.0
    confirm_live_s: float = 60.0
    # guards
    spike_abs: float = 3.0
    spike_h_s: float = 5.0
    burst_mult: float = 2.0
    burst_pull_s: float = 10.0
    # stubs
    stub_z: float = 3.0
    stub_extreme: bool = True


@dataclass(frozen=True)
class OpenParams:
    """Opening phase lifetime (addendum 2 section 3)."""

    phase_s: float = 300.0
    exit_z: float = 0.25
    exit_hold_s: float = 30.0
    decay_s: float = 120.0


@dataclass(frozen=True)
class LoopParams:
    loop_ms: float = 200.0
    pos_poll_s: float = 1.0
    orders_poll_s: float = 5.0
    min_requote_interval: float = 0.5
    max_rps: float = 15.0
    http_timeout: float = 2.0
    workers: int = 6


@dataclass(frozen=True)
class EndgameParams:
    t_passive_off: float = 5.0
    post_window_quote: bool = False
    h_post_ticks: float = 2.0
    # The exchange closes at the window end: cancel everything and stop there (CMI_CLOSE = end).
    hard_stop_at_end: bool = True


@dataclass(frozen=True)
class RiskParams:
    fail_n: int = 5
    fail_window_s: float = 10.0
    pause_s: float = 5.0
    cmi_stale_s: float = 5.0
    dd_widen: float = 3000.0
    dd_stop: float = 8000.0
    disagree_z: float = 3.0
    disagree_hold_s: float = 60.0
    corr_risk: bool = True
    corr_threshold: float = 0.7
    corr_mult: float = 1.5
    feed_down_sigma_mult: float = 1.5
    feed_down_size_mult: float = 0.5


@dataclass(frozen=True)
class StartupParams:
    # Quoting/sniping before the window start is off unless enabled (cmi_open: exchange is silent anyway).
    trade_before_start: bool = True


@dataclass(frozen=True)
class ProductParams:
    kind: str
    default_symbol: str
    fallback_min: float
    fallback_max: float
    limit_hard: int
    limit_soft: int  # Q_MM = 0.5 * L (the other half is reserved for snipes)
    stub_lots: int
    h_min_ticks: float
    h_max_ticks: float
    sizes: tuple[int, ...]
    rq_min_ticks: float
    snipe_max_lots: int
    contract_size: float  # fallback when the exchange reports none; affects P&L only


TRADES_PRODUCT = ProductParams(
    kind=PRODUCT_TRADES, default_symbol=TRADES_SYMBOL, fallback_min=0.0, fallback_max=100000.0,
    limit_hard=20, limit_soft=10, stub_lots=5, h_min_ticks=2, h_max_ticks=100, sizes=(2, 3),
    rq_min_ticks=1, snipe_max_lots=3, contract_size=1.0,
)
RANGE_PRODUCT = ProductParams(
    kind=PRODUCT_RANGE, default_symbol=RANGE_SYMBOL, fallback_min=0.0, fallback_max=10000.0,
    limit_hard=60, limit_soft=30, stub_lots=15, h_min_ticks=2, h_max_ticks=30, sizes=(5, 10),
    rq_min_ticks=2, snipe_max_lots=10, contract_size=10.0,
)


@dataclass(frozen=True)
class Config:
    cb: CbParams = field(default_factory=CbParams)
    trades_model: TradesModelParams = field(default_factory=TradesModelParams)
    range_model: RangeModelParams = field(default_factory=RangeModelParams)
    events: EventParams = field(default_factory=EventParams)
    external: ExternalParams = field(default_factory=ExternalParams)
    quote: QuoteParams = field(default_factory=QuoteParams)
    snipe: SnipeParams = field(default_factory=SnipeParams)
    opening: OpenParams = field(default_factory=OpenParams)
    mm: MmParams = field(default_factory=MmParams)
    loop: LoopParams = field(default_factory=LoopParams)
    endgame: EndgameParams = field(default_factory=EndgameParams)
    risk: RiskParams = field(default_factory=RiskParams)
    startup: StartupParams = field(default_factory=StartupParams)
    products: tuple[ProductParams, ProductParams] = (TRADES_PRODUCT, RANGE_PRODUCT)
    window: WindowConfig = field(default_factory=WindowConfig)
    window_reload_s: float = 5.0
    dry_run: bool = DRY_RUN


CONFIG = Config()


class WindowManager:
    """Resolves the settlement window from the live config and the auto-detect sources.

    Priority: WINDOW_START_UTC override > mode ("cmi_open": first CMI SSE event, "launch": first CB heartbeat,
    "aligned": floored to ALIGN_MIN, "utc" without a start falls back to "cmi_open"). The config file is re-read
    every `reload_s` seconds, so a human can correct the start live. `refresh` is called from one thread only.
    """

    def __init__(self, cfg: WindowConfig, path: Path, reload_s: float, hard_stop_at_end: bool) -> None:
        self.cfg = cfg
        self._path = path
        self._reload_s = reload_s
        self._hard_stop = hard_stop_at_end
        self._last_reload = 0.0
        self._aligned: float | None = None
        self.window: tuple[float, float] | None = None

    @property
    def close_ts(self) -> float | None:
        """Time at which everything is cancelled and the bot stops (CMI_CLOSE_UTC, else the window end)."""
        explicit = parse_utc(self.cfg.cmi_close_utc)
        if explicit is not None:
            return explicit
        return self.window[1] if self.window is not None and self._hard_stop else None

    def _resolve_start(self, now: float, first_cmi: float | None, first_hb: float | None) -> float | None:
        override = parse_utc(self.cfg.start_utc)
        if override is not None:
            return override
        mode = self.cfg.mode
        if mode == "launch":
            return first_hb
        if mode == "aligned":
            if self._aligned is None:
                step = self.cfg.align_min * 60.0
                self._aligned = (now // step) * step + (step if self.cfg.align_next else 0.0)
            return self._aligned
        return first_cmi  # "cmi_open", and "utc" without a start

    def refresh(self, now: float, first_cmi: float | None, first_hb: float | None) -> tuple[tuple[float, float] | None, bool]:
        """Returns (window, changed)."""
        mono = time.monotonic()
        if mono - self._last_reload >= self._reload_s:
            self._last_reload = mono
            new = load_window_config(self._path, self.cfg)
            if new != self.cfg:
                log.warning("window config changed: %s", new)
                self.cfg, self._aligned = new, None
        start = self._resolve_start(now, first_cmi, first_hb)
        window = None if start is None else (start, start + self.cfg.len_s)
        changed = window != self.window and window is not None
        if changed:
            source = "WINDOW_START_UTC override" if parse_utc(self.cfg.start_utc) is not None else f"auto-detect ({self.cfg.mode})"
            log.warning("##### WINDOW START %s -> END %s via %s #####", fmt_utc(window[0]), fmt_utc(window[1]), source)
        self.window = window if window is not None else self.window
        return self.window, changed
