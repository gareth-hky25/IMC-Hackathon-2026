# Market-making spec: CMI "Trades" and "Range" (BTC-USD, 15-minute window)

All times are UTC. τ = seconds remaining in the window, T = 900 s. φ = standard normal pdf, Φ̄(x) = 0.5·erfc(x/√2), so only `math` is needed.

## 0. Architecture (required)

**Threads**
1. **CB-WS thread.** Connect to `wss://ws-feed.exchange.coinbase.com` and subscribe with `{"type":"subscribe","product_ids":["BTC-USD"],"channels":["matches","heartbeat"]}` (public, no auth). Update a `CoinbaseState` under a lock. Reconnect with backoff 0.5 s, 1 s, 2 s, max 5 s.
2. **CMI SSE thread.** Started via `BaseBot.start()`. Callbacks only store the latest `OrderBook` per product, record our own fills (`trade.buyer == username` → +vol, `trade.seller == username` → −vol, deduped by `trade.id`) and set a `threading.Event` to wake the strategy. **Callbacks never make REST calls.**
3. **Strategy loop thread.** Wakes on that event or every `LOOP_MS`. Computes fair values, then snipes, then diffs quotes, then hands REST work to the executor.
4. **Executor.** `ThreadPoolExecutor(WORKERS)`. Override `send_order`, `cancel_order_by_id`, `request_all_orders` and `request_net_positions` to hit the same endpoints with the same payloads, but through a thread-local `requests.Session` with `timeout=HTTP_TIMEOUT`. The base class sets no timeout and opens a new TCP connection on every call. Wrap `OrderResponse(**json)` in try/except: unexpected keys or statuses raise `TypeError`.
5. **CB-REST helper.** Seeding and backfill against `https://api.exchange.coinbase.com`, rate-limited to `CB_REST_MAX_RPS`.

**Startup**
1. `request_all_products()` → symbol, tick, min, max, contract size. If min/max is `None`, fall back to Trades [0, 100000] and Range [0, 10000].
2. Cancel all our existing orders, in parallel by id.
3. Read the position with `request_net_positions()`. Log both `request_positions()` and `request_net_positions()` at startup and after the first fill, to confirm which one is the signed net position. Use `net_position`.
4. Seed the models (§1.3), connect to Coinbase, start quoting after `CB_RESUME_HEARTBEATS` heartbeats.

**Window** (confirmed: the window runs from when the CMI exchange opens until it closes 15 minutes later)
- `WINDOW_MODE`:
  - `cmi_open` (**default**): start = wall-clock UTC time of the first CMI SSE event after the bot starts; end = start + 900 s = exchange close. The bot does not quote before that event, but keeps the Coinbase feed warm and holds a rolling buffer of about 120 s of heartbeats and matches, so `L_start`, `M` and `m` can be set after the fact. `WINDOW_START_UTC` in `window_config.json` overrides auto-detection.
  - `launch`: start = first CB heartbeat.
  - `utc`: start = `WINDOW_START_UTC`.
  - `aligned`: start = now floored to `ALIGN_MIN` = 15 min (or the next boundary if `ALIGN_NEXT`).
- End = start + `WINDOW_LEN_S`.
- Use the **Coinbase message `time`** for window membership and the local monotonic clock for staleness.
- Re-read the window settings from a small JSON config file every 5 s, so a human can correct the window live.
- Membership: a trade counts if `start ≤ time < end`.

## 1. Fair value models

### 1.1 Trades (settles to the count of BTC-USD trades in the window)

**Observed count (robust to dropped messages)**
- `L_now` = max(trade_id seen in matches, `last_trade_id` from the latest heartbeat).
- `L_start` = highest trade_id with time < start. Take it from the last heartbeat or match before start, then refine with matches whose time is just around start. If the bot starts mid-window, find it with a REST interpolation search (§1.3).
- `n_obs`:
  - before the window starts: 0
  - inside the window: `L_now − L_start`
  - after the end: `L_end − L_start`, where `L_end` = highest id with time < end

**Rate estimate**
- On each heartbeat (about 1 s): Δn = Δlast_trade_id, Δt from heartbeat times, `x = Δn/Δt`.
- Two EWMAs with α = 1 − 2^(−Δt/HL), both starting at `λ_prior`:
  - `λ_fast`, HL = `HL_FAST_LAMBDA` (30 s)
  - `λ_slow`, HL = `HL_SLOW_LAMBDA` (600 s)
- Blend: `w = exp(−τ_end/T_MR_LAMBDA)`, `λ̂ = w·λ_fast + (1−w)·λ_slow`.

**Fair value and uncertainty**
- `τ_f` = τ inside the window, T before it starts.
- `μ_rem = λ̂·τ_f`, `F_T = n_obs + μ_rem`.
- `c(τ) = C_INF·sqrt(min(τ_end, T)/T)`.
- `Var_T = PHI·μ_rem + (c(τ)·μ_rem)² + SIGMA_CONV_T²`, `σ_T = sqrt(Var_T)`.
- Clamp `F_T` to [n_obs, maxPrice].
- Hard lower bound: `LB_T = n_obs − LB_MARGIN_T`, with `LB_MARGIN_T = 20 + λ̂·1s`.

### 1.2 Range (settles to max − min of the BTC-USD trade price in the window)

**Running state**
- In the window: `M`/`m` = max/min match price, `S` = last price, `R_now = M − m`.
- Before the window: `M = m = S`, `R_now = 0`.

**Volatility ($/√s, Parkinson estimator on bars built from match prices)**
- Fast: 10 s bars, `v = (H−L)²/(4 ln2 · 10)`, EWMA HL = `HL_FAST_SIG` (60 s).
- Slow: 1-min bars, `v = (H−L)²/(4 ln2 · 60)`, EWMA HL = `HL_SLOW_SIG` (900 s), seeded from candles.
- Blend: `σ̂² = w·σ²_fast + (1−w)·σ²_slow`, `w = exp(−τ_end/T_MR_SIG)`.

**Expected extension (driftless Brownian motion; exact by linearity of expectation)**
- `s = σ̂·sqrt(τ_f)`, `u = (M − S)/s`, `d = (S − m)/s`.
- `g1(x) = φ(x) − x·Φ̄(x)`.
- `g2(x) = (1+x²)·Φ̄(x) − x·φ(x)`.
- Extensions: `U = 2s·g1(u)` (up), `D = 2s·g1(d)` (down).
- Variances: `VU = 2s²·g2(u) − U²`, `VD = 2s²·g2(d) − D²`.
- `F_R = R_now + KAPPA_R·(U + D)`.
- `Var_R = KAPPA_R²·(VU + VD + 2·RHO_UD·sqrt(VU·VD)) + (C_SIGMA·KAPPA_R·(U+D))² + SIGMA_CONV_R²`, `σ_R = sqrt(Var_R)`.
- If s = 0: U = D = VU = VD = 0.
- Sanity check at u = d = 0: E = 1.596·s, sd = 0.4755·s. RHO_UD = −0.69 reproduces that sd.
- Hard lower bound: `LB_R = R_now − LB_MARGIN_R`.

**Dropped matches:** if a received trade_id > `last_id + 1`, queue a REST backfill `GET /products/BTC-USD/trades?after=<new_id>&limit=1000`, paged until it reaches `last_id`. Update M, m and the bars from the backfilled trades. Trades needs no backfill.

### 1.3 Priors, seeding and fallbacks

**Trades `λ_prior`**
- Latest trade (`GET /trades?limit=1`) → `(id_now, t_now)`.
- Interpolation-search for the id at `t_now − 1800 s`:
  1. Guess `id_g = id_now − λ_guess·Δt`.
  2. `GET /trades?after=id_g&limit=1000` returns ids below `id_g`, newest first. Verify the cursor direction empirically.
  3. If the target time falls inside the page, read off the id. Otherwise re-guess from the rate implied by the page (2–5 calls).
- `λ_prior = (id_now − id_then)/1800`.
- The same routine gives `L_start` mid-window.
- Fallback: `λ_prior = startingPrice_T/900`.

**Range `σ²_slow` seed**
- `GET /products/BTC-USD/candles?granularity=60` for the last 60 min. Rows are `[time, low, high, open, close, vol]`, newest first, max 300 per call.
- `σ²_prior = mean((H−L)²)/(4 ln2 · 60)`.
- Mid-window start: M and m from the full-minute candles inside the window, plus REST trades for the partial minutes.
- Fallback: `σ_prior = startingPrice_R/(1.596·√900)`.

**Optional calibration (`CALIBRATE_ON_START`, about 60 s of REST calls)**
- Range: from 24 h of 1-min candles, per 15-min block compute `ratio = actual range / (1.596·σ_park(prev 60 min)·√900)`. Set `KAPPA_R = median(ratio)`, clipped to [0.7, 1.5].
- Trades: 15-min counts N_k for the last 6 h. Set `C_INF = std(log(N_k/N_{k−1}))`, clipped to [0.15, 0.5].

**Feed down at start:** use the priors with σ ×1.5 and passive sizes halved. With no CB data at all, do not quote passively; allow only the lower-bound snipes (§3 rule b), and only if `n_obs`/`R_now` is valid.

## 2. Quoting (per product, each loop)

**Inputs:** F, σ, tick k, hard limit L (20 / 60), soft limit Q = `L_SOFT` (16 / 48), position q = `pos_est`.

**Spread and edge floors**
- `h = clamp(K_H·σ, H_MIN_TICKS·k, H_MAX_TICKS·k)`.
- `e_min = max(k, K_FLOOR·σ)`.
- `e_join = max(H_MIN_TICKS·k, K_JOIN·h)`.
- Toxicity: `TOX_N` fills on the same side within 15 s multiply that side's h by `TOX_MULT` for 30 s.

**Reservation price:** `r = F − BETA·(q/Q)·h`.

**Levels** (`LEVELS = 2`), level i:
- `Δ2 = max(2k, 0.5h)`.
- `bid_i = floor_k(r − h − (i−1)·Δ2)`.
- `ask_i = ceil_k(r + h + (i−1)·Δ2)`.

**Penny** (when `PENNY` is on; ignore levels with `volume − own_volume ≤ 0`): let B = best external bid. If `B + k > bid_1` and `B + k ≤ r − e_join`, set `bid_1 = B + k`. Mirror for the ask.

**Constraints** (apply in order; drop the level if any fails):
1. `bid ≤ F − e_min` and `ask ≥ F + e_min`.
2. `ask ≥ LB + e_min`.
3. `bid ≤ best_ext_ask − k` and `ask ≥ best_ext_bid + k` (passive orders never cross).
4. `minPrice ≤ price ≤ maxPrice`.
5. `ask_1 − bid_1 ≥ 2k`.

Snap prices with `round(n·k, 6)`.

**Sizes**
- Base sizes: `SIZE_T = [2, 3]`, `SIZE_R = [5, 10]`.
- Scale the side that would increase |q| by `max(0, 1 − |q|/Q)`. The reducing side gets full size.
- Cap by guard capacity, round down to int; skip if 0.

**Hard position guard** (every order, passive or snipe; computed under a lock)
- `pos_hi = max(pos_polled, pos_est)`, `pos_lo = min(pos_polled, pos_est)`.
- `open_buy` = max(remaining volume of our live bids in the local order table, Σ `own_volume` over book bid levels). `open_sell` likewise.
- `inflight_buy` / `inflight_sell` = volume reserved for orders sent but not yet answered.
- `buy_cap = Lim − pos_hi − open_buy − inflight_buy`.
- `sell_cap = Lim + pos_lo − open_sell − inflight_sell`.
- `Lim = Q` for passive orders, `L` for snipes.
- Reserve before sending. Release on a failure response. Keep the reservation on an exception or timeout until the reconcile completes.

**Position tracking:** every `POS_POLL_S`, `pos_polled` = `request_net_positions()` and `pos_est` is reset to it. Between polls, add own SSE fills to `pos_est`.

**Order table:** `{id: product, side, price, remaining}`, where remaining = `volume − filled` from the `OrderResponse`. Reconcile against `request_all_orders()` every `ORDERS_POLL_S` and after any error.

**Requote / diff**
- Match desired levels to live orders by side and price.
- Cancel a live order if:
  - it violates a constraint (urgent: bypasses the interval limit);
  - `|p_des − p_live| ≥ max(RQ_MIN_TICKS·k, RQ_FRAC·h)`;
  - its level is no longer desired;
  - `remaining < TOPUP_FRAC·size` and it is older than 2 s.
- Send a new order for each desired level that has no live order.
- Per product: at most one batch in flight, no more often than `MIN_REQUOTE_INTERVAL` (urgent cancels excepted).
- Each batch: run the cancels in parallel and wait for them, then send the new orders in parallel. Always cancel before sending.

**Priority** (global token bucket `MAX_RPS`), highest first:
1. snipe sends and snipe-remainder cancels
2. urgent / kill cancels
3. normal cancels
4. new quotes
5. polls

## 3. Catch dumb trades (runs before quoting, on every book update)

**Thresholds**
- `θ_inc = max(THETA_MIN_TICKS·k, Z_INC·σ)` for a trade that increases |q|.
- `θ_red = max(THETA_MIN_TICKS·k, Z_RED·σ)` for a trade that reduces |q|.

**Buy snipe.** An ask level qualifies if any of these hold:
- (a) `price ≤ F − θ`
- (b) `price ≤ LB` (deterministic arbitrage; still valid when CB is stale)
- (c) `price ≤ F/FAT_RATIO`
- (d) `price == minPrice`

`avail` = Σ(volume − own_volume) over qualifying levels. `vol = int(min(avail, buy_cap with Lim = L, SNIPE_MAX_LOTS))`. If vol > 0, send one BUY at the worst qualifying price; it fills through all better levels.

**Sell snipe.** Mirror image: a bid level qualifies if `price ≥ F + θ`, `price ≥ F·FAT_RATIO` or `price == maxPrice`. There is no deterministic upper bound.

**Making room:** if capacity is the binding constraint and we have passive orders on the same side, cancel those first (in parallel), then snipe.

**IOC emulation:** after the response, if `volume − filled > 0` and the status is ACTIVE or PART_FILLED, immediately `cancel_order_by_id(id)`. If that cancel fails, reconcile.

**Rules**
- Do not re-fire on the same (product, side, price) until a newer book arrives, with `SNIPE_COOLDOWN` 0.3 s.
- Assert that snipe buys are at or below F − θ while our asks are at or above F + e_min (no self-trade).
- Disable rules a/c/d when CB is stale, the model is unseeded or the CMI book is stale. Rule b stays allowed while `n_obs`/`R_now` is valid.

## 4. Endgame

- Spreads tighten through σ(τ). Do not flatten; skew alone handles inventory.
- At `τ ≤ T_PASSIVE_OFF` (5 s): cancel passive quotes. Snipes continue.
- After the end, F is exact (`L_end − L_start`, and the range is `M − m`) and σ = `SIGMA_CONV`.
  - Snipe anything at least `max(THETA_MIN_TICKS·k, 2σ_conv)` through F.
  - If `POST_WINDOW_QUOTE`: quote at `F ± max(H_POST_TICKS·k, 2σ_conv)` with level-1 sizes.
- Optional `CMI_CLOSE_UTC`: stop and cancel everything at that time.

## 5. Risk controls

- **CB stale** (no message for more than `CB_STALE_S`): cancel passive quotes on both products, in parallel. Keep only rule-b snipes. After reconnecting, wait `CB_RESUME_HEARTBEATS` and backfill the Range gap before quoting.
- **CMI book stale** (no snapshot for more than `CMI_STALE_S`): turn off snipes and penny logic. Keep model quotes.
- **Order failures**
  - `send_order` returns `None`: release the reservation and count a failure.
  - Exception or timeout: reconcile immediately.
  - `FAIL_N` failures within `FAIL_WINDOW_S`: cancel all, pause `PAUSE_S`, re-sync.
  - A failed cancel: reconcile.
- **Rate:** token bucket at `MAX_RPS` (burst `MAX_RPS`). On HTTP 429, halve it for 30 s.
- **P&L and position**
  - P&L per product = `cash + pos·F·contractSize`.
  - P&L < −`DD_WIDEN`: h ×2.
  - P&L < −`DD_STOP`: cancel passive quotes on that product; reducing-side orders only.
  - |pos_polled| > L: reducing-side orders only.
- **Model disagreement:** if the CMI external mid differs from F by more than `DISAGREE_Z·σ` for 60 s straight, log a WARN, halve passive sizes and double `Z_INC`.
- **Correlated risk (optional):** if `q_T/L_T` and `q_R/L_R` have the same sign and both exceed 0.7, multiply h by 1.5 on the sides that increase either position.
- **Logging:** one CSV row per second per product (F, σ, LB, λ̂/σ̂, quotes, position, P&L). Log every order, response, fill and snipe.

## 6. Parameters (Trades / Range where they differ)

| Group | Param | Default |
|---|---|---|
| Window | WINDOW_MODE / WINDOW_START_UTC / WINDOW_LEN_S / ALIGN_MIN / ALIGN_NEXT | "launch" / None / 900 / 15 / False |
| CB | CB_STALE_S / CB_RESUME_HEARTBEATS / CB_REST_MAX_RPS | 3 / 2 / 5 |
| Trades model | HL_FAST_LAMBDA / HL_SLOW_LAMBDA / T_MR_LAMBDA | 30 / 600 / 120 s |
| | PHI / C_INF / SIGMA_CONV_T / LB_MARGIN_T | 3 / 0.30 / 20 / 20 + λ̂·1 s |
| Range model | HL_FAST_SIG / HL_SLOW_SIG / T_MR_SIG | 60 / 900 / 300 s |
| | RHO_UD / C_SIGMA / KAPPA_R / SIGMA_CONV_R / LB_MARGIN_R | −0.69 / 0.20 / 1.0 / 1.0 / 2 |
| Quoting | K_H / K_FLOOR / K_JOIN / BETA | 0.15 / 0.05 / 0.5 / 1.0 |
| | H_MIN_TICKS / H_MAX_TICKS | 2 / 100 (T); 2 / 30 (R) |
| | L_SOFT | 16 (T); 48 (R) |
| | LEVELS / SIZES | 2 / [2,3] (T); [5,10] (R) |
| | RQ_FRAC / RQ_MIN_TICKS / TOPUP_FRAC / PENNY | 0.3 / 1 (T), 2 (R) / 0.5 / True |
| | TOX_N / TOX_MULT | 3 / 1.5 |
| Snipe | Z_INC / Z_RED / THETA_MIN_TICKS / FAT_RATIO | 0.5 / 0.25 / 3 / 3.0 |
| | SNIPE_MAX_LOTS / SNIPE_COOLDOWN | 10 (T), 30 (R) / 0.3 s |
| Loop/IO | LOOP_MS / POS_POLL_S / ORDERS_POLL_S / MIN_REQUOTE_INTERVAL | 200 / 1 / 5 / 0.5 s |
| | MAX_RPS / HTTP_TIMEOUT / WORKERS | 15 / 2 s / 6 |
| Endgame | T_PASSIVE_OFF / POST_WINDOW_QUOTE / H_POST_TICKS / CMI_CLOSE_UTC | 5 s / True / 2 / None |
| Risk | FAIL_N / FAIL_WINDOW_S / PAUSE_S / CMI_STALE_S | 5 / 10 / 5 / 5 |
| | DD_WIDEN / DD_STOP / DISAGREE_Z | 3000 / 8000 / 3 |
| Startup | CALIBRATE_ON_START | False |

Rough magnitudes at window start, as a sense check:
- Trades, λ ≈ 8/s: F ≈ 7200, σ ≈ 2200, h ≈ 330.
- Range, s ≈ 75: F ≈ 120, σ ≈ 43, h ≈ 6.5.

## 7. Edge cases

**Prices and volumes**
- Bids floor to the tick and asks ceil to the tick; then clamp to [min, max]. Never send a price out of bounds.
- Volume must be an `int` > 0.

**Book data**
- A book side can be empty. "Best external" excludes our own volume (`volume − own_volume`); with no external levels, skip penny and snipe logic for that side.
- Verify on the first live snapshot that `volume` is the total, including `own_volume`.

**Response handling**
- Parse `OrderResponse` defensively.
- Products may have `None` min/max.

**Coinbase data**
- Before the window starts: `n_obs = 0`, `R_now = 0`, full horizon T.
- Coinbase `time` is ISO with microseconds and `Z`; parse it to UTC.
- The first matches message has `type == "last_match"`: use it to initialise `last_id` and S, but do not count it.
- Log (matches received)/(id span) per minute, to sanity-check that trade ids are contiguous.

**Restarts and recovery**
- On restart: cancel stale orders, sync positions, seed `L_start`, M and m via REST.
- The base `cancel_all_orders()` is sequential, so the kill switch cancels in parallel by id.
- A fresh SSE snapshot replaces the old book; never merge snapshots.

**Concurrency:** lock shared state, and compute fair values from a consistent copy.

**Units:** contract size affects P&L only (Range = 10 per point). Limits are in lots.

**Settlement assumptions:** Trades counts every Coinbase BTC-USD match (trade_id); Range uses trade prices.
