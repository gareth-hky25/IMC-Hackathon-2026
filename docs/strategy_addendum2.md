# Addendum 2: window-specific fair value, MM soundness, hybrid opening, fat-finger capture

Target: the window **19:15:00–19:30:00 UTC, Tue 2026-10-27**. This addendum **overrides** the base spec and Addendum 1 wherever they conflict.

## 1. Calibration: 19:15–19:30 UTC over the last 14 days (weekdays, n = 10)

| Day | Pre-30 per 15m | Window count | Count ratio | Range predicted by regime | Actual range | Range ratio |
|---|---|---|---|---|---|---|
| Mon 26 | 6706 | 5222 | 0.78 | 181 | 171 | 0.94 |
| Fri 23 | 4920 | 5303 | 1.08 | 160 | 101 | 0.63 |
| Thu 22 | 5093 | 5081 | 1.00 | 160 | 205 | 1.28 |
| Wed 21 | 7491 | 5330 | 0.71 | 308 | 187 | 0.61 |
| Tue 20 | 4015 | 4681 | 1.17 | 120 | 190 | 1.58 |
| Mon 19 | 4548 | 4909 | 1.08 | 137 | 134 | 0.98 |
| Fri 16 | 7808 | 5777 | 0.74 | 406 | 427 | 1.05 |
| Thu 15 | 4632 | 5315 | 1.15 | 117 | 145 | 1.25 |
| Wed 14 | 6426 | 6333 | 0.99 | 179 | 116 | 0.64 |
| Tue 13 | 4402 | 5036 | 1.14 | 139 | 131 | 0.94 |

**Takeaways**
- The 19:15 count is stable: mean 5,299, geometric mean ≈ 5,280.
- Counts mean-revert against the pre-window regime.
- Range ratio mean 0.99, so `KAPPA_R = 1.0`. Window ranges: mean $181, geometric mean $166.

**Forecast error, leave-one-out, RMSE of log(actual/forecast)**

| Forecast | Trades | Range |
|---|---|---|
| Regime only | 0.185 | 0.313 |
| Seasonal only | 0.090 | 0.425 |
| **Blend** | **0.065** (25% regime) | **0.294** (70% regime) |

**Data sources**
- **Kept:** weekday seasonality for Trades and Range (`SEASON_ENABLE = True`).
- **Deribit IV: disabled** (`DERIBIT_ENABLE = False`, code path kept). The nearest expiry prices FOMC on Wed 28 Oct, not today.
- **Dropped** (Binance stays behind its flag, off):
  - Coinbase /stats, level2 and ticker
  - perp funding and open interest
  - liquidations
  - cross-venue activity
- `EVENT_TIMES_UTC = []`. The operator should check a macro calendar.
- `SEASON_RECALC = False`; use the hard-coded defaults.

## 2. Revised fair value

### Trades (replaces Addendum 1 §A1)

**Prior**
- `λ_season = SEASON_COUNT/900`, with SEASON_COUNT = 5280.
- `λ_regime` = rolling 30-min rate from heartbeat `last_trade_id`. Keep a deque of (time, id), seeded from REST, and freeze it at window start as `(L_start − id(start − 1800))/1800`.
- `ln λ_0 = W_REGIME·ln λ_regime + (1 − W_REGIME)·ln λ_season`, with W_REGIME = 0.25.

**In-window update** (once elapsed ≥ 10 s)
- `λ_win = n_obs/elapsed`
- `w = T0/(T0 + elapsed)`, T0 = 300 s
- `ln λ_slow = w·ln λ_0 + (1 − w)·ln λ_win`
- Horizon blend with `λ_fast` as in base §1.1.
- Before the window starts: `λ_slow = λ_0`.

**Uncertainty:** `c(τ) = C_INF·sqrt(τ_f/T)`, with C_INF set by which prior inputs are available:

| Available prior | C_INF |
|---|---|
| Seasonal and regime | 0.10 |
| Seasonal only | 0.15 |
| Regime only | 0.25 |

**Today's estimate:** with the 18:20 regime of ~4,200 per 15 min, `F_0 ≈ 4,990` and `σ_T ≈ 500`. The CMI open at 5,000 looks fair.

### Range (replaces the σ anchor in base §1.2)

**Prior**
- `σ_season = SEASON_RANGE/(1.596·√900)`, with SEASON_RANGE = 166 (3.47 $/√s).
- `σ_regime` = Parkinson estimate on 1-min bars over [start − 30 min, start).
- `ln σ_anchor = W_R_REGIME·ln σ_regime + (1 − W_R_REGIME)·ln σ_season`, with W_R_REGIME = 0.7.

**In-window update**
- `σ²_slow = w·σ²_anchor + (1 − w)·σ²_live`, where σ²_live is the bias-corrected Parkinson average over in-window 1-min bars.
- `w = 300/(300 + elapsed)`.
- `KAPPA_R = 1.0`, `C_SIGMA = 0.10`.

### Empirical forecast σ (directional gating only)
- `σlog_emp_T = 0.10`, `σlog_emp_R = 0.30`.
- Both scaled by `sqrt(τ_f/T)` and floored at `σ_model/F`.

## 3. MM_MODE and the hybrid opening (replaces Addendum 1 §3a)

`MM_MODE ∈ {"hybrid" (default), "pure", "directional"}`:
- `pure` sets q* ≡ 0.
- `directional` is Addendum 1's rule.
- `hybrid` is defined below.

**Gap in log space:** `z = ln(F/mid_ext)/σlog_emp`.

**Validity gates.** All must hold, or q* = 0:
- CB feed fresh (< 1.5 s).
- `t_live ≥ 120 s`.
- Priors agree: `|ln λ_regime − ln λ_season| ≤ AGREE_MAX` (0.30). For Range, use the σ equivalent.
- The live rate confirms the prior (needs ≥ 60 s of live data):
  - Trades: `λ_fast/λ_0 ∈ [0.75, 1.33]`.
  - Range: `σ_fast/σ_anchor ∈ [0.6, 1.6]`.

**Target inventory:** `q* = sign(z)·q_dir_max·clip((|z| − Z_OPEN_DIR)/Z_RAMP, 0, 1)`, with Z_OPEN_DIR = 1.5, Z_RAMP = 1.0, q_dir_max = 0.5·L.

**Phase lifetime:** 300 s. It ends early if |z| < 0.25 for 30 s, then q* decays to 0 over 120 s.

**Today's check: neither product qualifies.** Trades |z| ≈ 0.02; Range ≈ 1.10, below 1.5.

## 4. Pure-MM quoting (replaces base §2 skew, sizes and penny)

**Inventory scale:** `Q_MM = 0.5·L`, `q_rel = q − q*`, `x = q_rel/Q_MM`. The other 0.5·L is reserved for snipes.

**Model quotes**
- `h = clamp(K_H·σ, H_MIN_TICKS·k, H_MAX_TICKS·k)`
- `bid_model = F − h·(1 + β·x)`
- `ask_model = F + h·(1 − β·x)`, with β = 2
- Reduce side is floored at `F ∓ K_GIVE·σ` (K_GIVE = 0.10). Asks are floored at ≥ LB + e_min.
- The increasing side stays ≥ e_adv·(1 + β|x|) from F.

**Adverse-selection floor:** `e_adv = max(H_MIN_TICKS·k, K_ADV·σ)`, K_ADV = 0.05.

**Book anchoring**
- `cap_bid` = `F − e_adv·(1 + β|x|)` if the bid is the increasing side, or `bid_model` if it is the reduce side.
- External bid exists: `bid = min(cap_bid, B_ext + k)`. No external bid: `bid = bid_model`.
- Always keep `bid ≤ A_ext − k`. Mirror for asks.
- Penny throttle: at most 1 reprice per side per 0.5 s and at most `PENNY_MAX_RQ` (10) per side per 10 s.

**Sizes**
- Increasing side: `base·max(0, 1 − |x|)`.
- Reduce side: `min(|q_rel|, base·(1 + |x|))`. It never flips the position.

**Inventory half-life**
- Age escalator: if |q| has not decreased for `INV_AGE_S` (45 s), move the reduce price 1 tick toward the give-up floor every 10 s.
- Adaptive β: if the realised half-life over 5 min exceeds 2× `INV_HL_TARGET` (60 s), β += 0.5, up to a maximum of 4.

**Pulling quotes on adverse moves**
- **F jump:** if |F_now − F_at_quote| ≥ 0.5·e_adv, cancel and replace that side urgently.
- **Range spike guard:** `δ = max(SPIKE_ABS, σ̂·√SPIKE_H)`, with SPIKE_ABS = $3 and SPIKE_H = 5 s.
  - If S ≥ M − δ or S ≤ m + δ: cancel Range asks immediately, then requote at ≥ F + δ with half size.
  - A new M or m print triggers an urgent requote.
- **Trades burst guard:** if λ over 5 s (EWMA HL 5 s) exceeds `BURST_MULT`·λ̂ (2), pull Trades asks and widen them by 2·e_adv for 10 s.
- Both guards fire from the CB thread through the wake event.

**Endgame (pure and hybrid)**
- τ ≤ `T_FLAT` (120 s): Q_MM(τ) = Q_MM·τ/T_FLAT, and K_GIVE rises linearly to `K_GIVE_END` (0.5).
- τ ≤ 30 s with |q| > 0: hit external orders at prices within F ± K_GIVE_END·σ.
- τ ≤ 5 s: pull passive quotes and stubs. Snipes continue.

**P&L decomposition** (log every second)
- Spread P&L = Σ over fills of `(F_at_fill − price)·side·contract`.
- Inventory P&L = Σ `q·ΔF·contract`.
- Alert if |inventory P&L| > spread P&L for more than 5 min.

## 5. Fat-finger and mistake capture (extends base §3)

**Snipe thread:** blocks on a single-slot latest-book queue filled by the SSE callback, evaluates every update and submits at top priority. The strategy loop no longer runs snipes.

**Class 1: consensus mispricing.** The normal Z_INC/Z_RED rule, capped at `SNIPE_MAX_LOTS` = 3 (Trades) and 10 (Range). Anything beyond that is governed by hybrid q*.

**Class 2: outlier or fat-finger.** An order qualifies if any of:
- (a) its price is beyond `F ∓ Z_FAT·σ` (Z_FAT = 2.0), with `σ = max(σ_model, σlog_emp·F)`;
- (b) it sits at minPrice or maxPrice;
- (c) **book gap:** on the ask side, `A_next − p ≥ max(GAP_MIN_TICKS·k, GAP_K·med_step)` and `p ≤ F`, where med_step is the median adjacent-level gap over the last 50 snapshots, GAP_K = 5 and GAP_MIN_TICKS = 5. Mirror for bids.

Sizing:
- (a) and (b) sweep every qualifying level up to the hard limit L (`FAT_MAX_LOTS = L`), at the worst qualifying price, with an IOC cancel.
- (c) alone is capped at `SNIPE_MAX_LOTS`.

**Checks before going above SNIPE_MAX_LOTS.** All must hold:
- CB feed fresh (< 1.5 s).
- `|F_now − F_10s_ago| < 0.5·σ`.
- No spike or burst guard active.
- Model seeded.
- The external mid, excluding the outlier level(s), is within `Z_CONS·σ` of F (Z_CONS = 1.0).

**Making room:** before a fat-finger sweep, cancel our same-side stubs and passive quotes in parallel, then send.

**Self-trade:** reduce-side quotes can sit through F. Before a buy snipe at limit p, cancel our asks priced ≤ p (mirror for sells). Remove the base-spec assert that this cannot happen.

**Unwinding a snipe.** Snipe and fat-finger fills go into a snipe bucket with VWAP `c`. Work it off with priority:
- Passive reduce side may use floor `max(F − K_GIVE·σ, c + LOCK_FRAC·(F − c))` for a long position (mirror for short), with LOCK_FRAC = 0.5.
- Aggressively hit external orders at prices that lock ≥ LOCK_FRAC of the captured edge.

**Stub orders**
- Each side rests one stub at `F ∓ STUB_Z·σ` (STUB_Z = 3), size `min(STUB_LOTS, spare)`. STUB_LOTS = 5 (Trades), 15 (Range).
- `spare` = hard-guard capacity after passive orders and the snipe reserve.
- Optional 1-lot stubs at minPrice + k and maxPrice − k (`STUB_EXTREME = True`).
- Stubs count in the guard, have the lowest priority and are cancelled first to make room.
- Requote a stub only when F moves more than 1σ.
- Range and Trades ask stubs are pulled while a spike or burst guard is active.
- Pull all stubs at τ ≤ 5 s.

## 6. Fixes to the base spec and Addendum 1

1. The self-trade invariant is false under reduce-side give-up. Fixed in §5.
2. `KAPPA_R` goes from 1.05 to 1.0.
3. Prior overlap: use in-window `λ_win`/`σ²_live` with T0 = 300 s.
4. The base penny rule is replaced by the §4 anchoring rule.
5. `ask ≥ F + e_min` applies to the increasing side only.
6. Snipe sizing is split into consensus and outlier classes.
7. `pos_est` double counting:
   - Dedupe fills by id.
   - If `pos_est ≠ pos_polled` on 2 consecutive polls with no new fills, snap to the polled value.
   - The guard keeps its max/min.
8. The soft limit is now `Q_MM = 0.5·L`.
9. `SEASON_COUNT = 5280` replaces `N_STATIC = 5800`. C_INF = 0.10.

## 7. Parameter delta

| Parameter | Value |
|---|---|
| MM_MODE | "hybrid" |
| SEASON_ENABLE / SEASON_COUNT / SEASON_RANGE / SEASON_RECALC | True / 5280 / 166 / False |
| W_REGIME / W_R_REGIME / T0 | 0.25 / 0.70 / 300 |
| C_INF (seasonal+regime / seasonal only / regime only) | 0.10 / 0.15 / 0.25 |
| KAPPA_R / C_SIGMA | 1.0 / 0.10 |
| σlog_emp_T / σlog_emp_R / AGREE_MAX | 0.10 / 0.30 / 0.30 |
| Z_OPEN_DIR / Z_RAMP / q_dir_max | 1.5 / 1.0 / 0.5·L |
| Q_MM / β / β_max / K_GIVE / K_GIVE_END | 0.5·L / 2 / 4 / 0.10 / 0.5 |
| K_ADV / INV_AGE_S / INV_HL_TARGET / T_FLAT / PENNY_MAX_RQ | 0.05 / 45 / 60 / 120 / 10 |
| SPIKE_ABS / SPIKE_H / BURST_MULT | $3 / 5 s / 2 |
| SNIPE_MAX_LOTS (T/R) / FAT_MAX_LOTS | 3/10 / L |
| Z_FAT / Z_CONS / GAP_K / GAP_MIN_TICKS / LOCK_FRAC | 2.0 / 1.0 / 5 / 5 / 0.5 |
| STUB_Z / STUB_LOTS (T/R) / STUB_EXTREME | 3 / 5/15 / True |
| DERIBIT_ENABLE / BINANCE_ENABLE / EVENT_TIMES_UTC | False / False / [] |

Deprecated: `N_STATIC`, `W_STATIC`, `PRIOR_PSEUDO_S`, `L_SOFT`, `OPEN_MAX_FRAC` as a direct target, and `K_JOIN`.
