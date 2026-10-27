# Spec addendum: opening prior and external data

Extends or replaces the matching parts of `strategy_spec.md`. Calibrated against live public data on 2026-10-27 at about 18:20 UTC, with BTC at about $80.5k.

## Data check

**Trades.** Aligned 15-minute BTC-USD counts over the last 6 hours, from trade_id differences:
- range 3,211–7,364, mean 4,838, median 4,706;
- the last 30 minutes ran at 4.67 trades/s, about 4,200 per 15 minutes;
- std of log(N_k/N_{k−1}) between consecutive windows = 0.25.

The 5,840 figure is a long-run average, above the current regime. The gap between 5,840 and 5,000 is well under one standard deviation of the window-to-window spread. A static prior must never decide direction on its own, so the regime estimate at startup is **mandatory**.

**Range.** Realised 15-minute ranges over 24 hours (100 blocks): mean $181, median $157, span $45–$416. A Parkinson estimate from 1-minute candles (σ = 3.51 $/√s) predicts E[range] = $168, so `KAPPA_R` ≈ 1.05. The exchange's opening price of 120 looks low.

**Base-spec change:** `C_INF` = 0.25 (was 0.30), the measured value.

## A1. Trades opening prior (replaces the Trades part of §1.3, changes §1.1)

**Layered prior `λ_0`**
- `λ_static = N_STATIC/900`, with N_STATIC = 5800, configurable.
- Regime estimate at startup (mandatory): `λ_regime = (id_now − id(t_now − REGIME_LOOKBACK_S))/REGIME_LOOKBACK_S`, found with the trade_id interpolation search from §1.3.
- Blend in log space: `ln λ_0 = W_STATIC·ln λ_static + (1−W_STATIC)·ln λ_regime`, with W_STATIC = 0.2.
- If REST fails: `λ_0 = λ_static`, `C_INF_EFF = C_INF_STATIC_ONLY` (0.40) until `t_live ≥ 120 s`, and the opening-phase target inventory is disabled.

**Prior decay.** `t_live` = seconds of CB heartbeat data since bot start, including time before the window opens.
- `w_prior = T0/(T0 + t_live)`, with T0 = `PRIOR_PSEUDO_S` (120 s with a regime estimate, 60 s without).
- `λ_slow_eff = w_prior·λ_0 + (1 − w_prior)·λ_live_slow`, where `λ_live_slow` is a bias-corrected EWMA (HL 600 s) of live heartbeat rates only: divide the raw EWMA by `1 − 2^(−t_live/600)`.
- `λ_fast` is seeded at λ_0. Use `λ_slow_eff` wherever §1.1 uses `λ_slow`.

**Scheduled events (optional).** `EVENT_TIMES_UTC` (a list, default empty). If the window contains an event: λ_0 ×`EVENT_RATE_MULT` (1.5), σ²_slow ×`EVENT_SIGMA_MULT`² (1.5²), and C_INF and C_SIGMA ×1.5.

**Opening phase (new §3a; applies to both products)**
- **External mid:** `mid_ext` = mid of the best external bid and ask (excluding our own volume). If only one side exists, use it. If the book is empty, the phase does nothing.
- **Gap:** `g = F − mid_ext`.
- **Active:** from the first CMI book snapshot until whichever comes first:
  - `OPEN_PHASE_S` (300 s) after the window start, or
  - `|g| < OPEN_EXIT_Z·σ` (0.25) for 30 consecutive seconds.
- **Aggressive take:** take offers ≤ `F − K_OPEN·σ` (K_OPEN = 0.35) through the snipe/IOC path while `q < q_open = OPEN_MAX_FRAC·L` (0.5). Mirror for bids ≥ `F + K_OPEN·σ` down to −q_open. Beyond ±q_open, only the normal `Z_INC` snipe applies. The §2 hard guard still applies.
- **Target inventory**
  - `q* = clip((g/σ)/K_OPEN, −1, 1)·q_open`.
  - `r = F − BETA·((q − q*)/Q)·h`. The size taper uses `(q − q*)`.
  - `bid ≤ F − e_min` and `ask ≥ F + e_min` still hold.
  - When the phase ends, q* decays linearly to 0 over `OPEN_DECAY_S` (120 s).
  - q* = 0 if F rests on the static prior only, or if the CB feed is stale.
- **Worked example, today's numbers**
  - Trades: F ≈ 4,480, σ ≈ 1,126. A book at 5,000 gives q* = −10; sell bids ≥ 4,874.
  - Range: F ≈ 168, σ ≈ 61. A book at 120 gives q* = +30; lift offers ≤ 147.

## A2. External data (each source optional and config-flagged)

**Polymarket: excluded.** It prices direction, not variance or activity, and its windows do not match ours.

**Deribit implied vol for Range (`DERIBIT_ENABLE = True`, no auth)**
- **Primary: short-dated ATM IV.** `GET https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option`.
  - Read `result[]`: `instrument_name` (e.g. `BTC-31OCT26-87000-C`; expiry is 08:00 UTC), `mark_iv` (%), `underlying_price`.
  - Pick the nearest expiry with at least `IV_MIN_TTE_H` (2 h) to go, then the strike nearest `underlying_price`.
  - IV = mean of the call and put `mark_iv` at that strike. Skip an instrument whose `mark_iv` is 0 or missing.
- **Fallback: DVOL.** `GET https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC&start_timestamp=<now−600000 ms>&end_timestamp=<now ms>&resolution=60`. Read `result.data[-1][4]`; rows are `[ts_ms, open, high, low, close]`.
- **Convert to dollars:** `σ_iv = (IV/100)·S/√(365·86400)` in $/√s, where S = last Coinbase price.
- **Scale to realised vol:** `σ_iv_adj = IV_RV_RATIO·σ_iv`, default 0.65. With `CALIBRATE_ON_START`, set the ratio = σ_Parkinson(24 h) / mean σ_iv(24 h, DVOL resolution=3600), clipped to [0.4, 1.0].
- **Combine:** `σ²_anchor = W_IV·σ²_iv_adj + (1 − W_IV)·σ²_slow`, W_IV = 0.25. It replaces σ²_slow in the §1.2 blend. If the candle seed fails, seed σ²_slow from σ²_iv_adj.
- **Event flag (optional):** if short-dated ATM IV / DVOL > 1.3, C_SIGMA ×1.5.
- **Refresh and fallback:** refresh every `IV_REFRESH_S` (60 s). On failure or data older than `IV_STALE_S` (300 s), set W_IV = 0.

**Binance BTCUSDT trade rate for Trades (`BINANCE_ENABLE = False`)**
- Sources: REST `https://data-api.binance.vision/api/v3/trades?symbol=BTCUSDT&limit=1` (field `[0].id`), or WS `wss://stream.binance.com:9443/ws/btcusdt@trade` (fields `t`, `T`).
- Build fast (HL 30 s) and slow (HL 600 s) rates per venue.
- `λ_fast_eff = λ_fast_CB·clip((rB/rCB)^W_BIN, 0.67, 1.5)`, with r = fast/slow on each venue and W_BIN = 0.5. On failure, the factor is 1.

## A3. Parameters

| Param | Default |
|---|---|
| N_STATIC / W_STATIC / REGIME_LOOKBACK_S | 5800 / 0.2 / 1800 |
| PRIOR_PSEUDO_S (with regime / static only) | 120 / 60 |
| C_INF (changed) / C_INF_STATIC_ONLY | 0.25 / 0.40 |
| KAPPA_R (changed) | 1.05 |
| OPEN_PHASE_S / OPEN_EXIT_Z / K_OPEN / OPEN_MAX_FRAC / OPEN_DECAY_S | 300 / 0.25 / 0.35 / 0.5 / 120 |
| EVENT_TIMES_UTC / EVENT_RATE_MULT / EVENT_SIGMA_MULT | [] / 1.5 / 1.5 |
| DERIBIT_ENABLE / W_IV / IV_RV_RATIO / IV_REFRESH_S / IV_STALE_S / IV_MIN_TTE_H | True / 0.25 / 0.65 / 60 / 300 / 2 |
| BINANCE_ENABLE / W_BIN | False / 0.5 |

## A4. TLS

Anaconda Python's `requests` failed TLS verification for `api.exchange.coinbase.com` on the dev machine, while curl succeeded. At startup, call `truststore.inject_into_ssl()` if available. Log TLS failures loudly and degrade every external source to its fallback without crashing.
