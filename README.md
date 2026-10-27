# Market-making bot for BTC trade count and price range (IMC Hackathon 2026, 1st place)

I won the IMC Hackathon 2026 overall as a solo entrant. I built a Python bot that made markets in two exchange products whose settlement values come from Coinbase BTC-USD data over one 15 minute window. It finished first with 41,900 P&L against 40,440 for second place, and ended the window flat in one product and 12 lots short in the other.

## Summary

- **The products:** TRADES settled to the number of BTC-USD trades on Coinbase in the window (2,845). RANGE settled to the highest minus the lowest BTC-USD trade price in the window, in dollars (94).
- **The idea:** both settlement values are partly known before the window ends, since the count and the range only grow. So fair value is what has already happened, plus a forecast of what is left, with uncertainty that shrinks to zero at the close.
- **How it made money:** it quoted around that fair value, leaned its prices against its inventory to stay near flat, and also took orders from other participants that were clearly mispriced.
- **What the data shows:** about 92% of P&L came from matched round trips (buying and selling the same product at a profit). The rest was a small residual short in TRADES that settled in my favour. The mispriced-order logic contributed very little on the day.
- **Main caveat:** this was a single 15 minute window with a 3.6% winning margin, and I validated the bot with a dry run and a short live test, not a backtest. See "Limitations".

## 1. The problem

| Product | Settles to | Tick | Position limit | Contract size | Settled at |
|---|---|---|---|---|---|
| TRADES | Number of BTC-USD trades on Coinbase in the window | 10 | ±20 | 1 | 2,845 |
| RANGE | Max minus min BTC-USD trade price in the window, in USD | 1 | ±60 | 10 | 94 |

Both products opened at fixed prices (TRADES 5,000, RANGE 120) with no obvious relation to the day's actual activity, and other participants were also trading them. A market maker here faces two risks: holding a position when the final value comes in far from where it traded, and quoting stale prices while the underlying jumps. The design below is aimed at those two.

## 2. Fair value

The bot recomputed a fair value `F` and a standard deviation `σ` for each product every 200 ms.

### TRADES

**What is already known.** Coinbase numbers every trade sequentially, so the count so far is `latest trade_id − trade_id at window start`. This is robust to dropped websocket messages, unlike counting messages received.

**What is still to come.** `F = count so far + λ̂ × time remaining`, where `λ̂` is the trade rate per second, estimated as a blend of a fast average (30 second half-life) and a slow one (600 seconds). Near the end the fast one dominates, and early on the slow one does.

**Starting rate.** Before any in-window data exists, the rate is a geometric blend of two priors: 75% a same-time-of-day average (5,280 trades per 15 minutes, from the last 10 weekdays at this time of day) and 25% the rate over the previous 30 minutes. As the window progresses, the slow rate moves from that prior to the observed in-window rate, with weight `300 / (300 + seconds elapsed)` on the prior.

**Uncertainty.** `Var = 3 × (expected remaining trades) + (c × expected remaining trades)² + 20²`, with `c = 0.10 × √(time remaining / 900 s)`. The first term allows for trade arrivals being burstier than Poisson, the second for error in the rate, and the third for small counting differences at the window boundary.

**Hard floor.** The count cannot fall, so the bot never offered below the count already observed.

### RANGE

**What is already known.** The maximum `M`, minimum `m` and last price `S` of Coinbase BTC-USD trades inside the window, in dollars. The range so far is `M − m`.

**What is still to come.** Under driftless Brownian motion, the expected amount by which the range grows over the remaining time has a closed form. With `s = σ̂ × √(time remaining)`, `u = (M − S) / s` and `d = (S − m) / s`:

`F = (M − m) + 2s × (g(u) + g(d))`, where `g(x) = φ(x) − x × (1 − Φ(x))`

`φ` and `Φ` are the standard normal density and distribution. The extension is largest when the price sits near the current high or low, and falls away as it moves toward the middle. At the window start (`u = d = 0`) this reduces to the textbook `1.596 s`.

**Volatility `σ̂`.** Parkinson (high-low) estimates from 10 second and 1 minute bars, blended by time remaining. The starting value is 70% the previous 30 minutes and 30% a same-time-of-day average (a 166 dollar range).

**Uncertainty.** The exact variance of that extension, plus 10% for error in `σ̂`.

**Hard floor.** The range cannot shrink, so the bot never offered below `M − m`.

### How fair value behaved on the day

| Time (UTC) | TRADES `F` | TRADES market | RANGE `F` | RANGE market |
|---|---|---|---|---|
| 19:09 (before the window) | 3,750 to 3,800 | 3,520 to 3,700 | 129 to 136 | 138 to 178 |
| 19:20:30 | 3,500 | none quoted | 132 | none quoted |
| 19:22:30 | 3,193 | 2,960 / 3,000 | 119 | 138 / 139 |
| 19:25:00 | 2,824 | 2,810 / 2,850 | 100 | 114 / 115 |
| 19:30:00 | 2,684 | 2,880 / 2,960 | 92 | 101 / 103 |
| 19:34:50 | 2,779 | 2,840 / 2,860 | 94.1 | 94 / 95 |
| **Settled** | **2,845** | | **94** | |

The same-time-of-day prior for TRADES (5,280) was far too high for this day, since the actual count was 2,845. The model corrected quickly because the in-window rate takes over, but it means the early quotes were centred too high. My final Coinbase count was 2,777 against 2,845 on the exchange, about 2.4% low, which suggests the organisers counted slightly differently.

## 3. Quoting and inventory control

The bot posted limit orders on both sides of `F`, in two levels per side. The goal was spread income with inventory staying near zero, not a directional view.

- **Spread.** The half-spread is `h = clamp(0.15 × σ, 2 ticks, 100 ticks)` for TRADES and `clamp(0.15 × σ, 2 ticks, 30 ticks)` for RANGE. It is wide early, when `σ` is large, and falls to the 2 tick minimum near the end.
- **Inventory skew.** With `x = (position − target) / Q`, where `Q` is half the position limit (10 lots for TRADES, 30 for RANGE), the bid is `F − h(1 + 2x)` and the ask is `F + h(1 − 2x)`. Long, and the bid drops while the ask comes toward `F`, so selling is easier than buying. At half the quoting limit the reducing side sits at fair value.
- **Adverse-selection floor.** Quotes on the side that adds inventory are kept at least `max(2 ticks, 0.05σ)` from `F`, scaled by `(1 + 2|x|)`, so a skewed quote cannot drift into fair value.
- **Giving up edge to flatten.** The reducing side may be placed up to `0.10σ` through fair value. This rises to `0.5σ` over the last two minutes and whenever inventory is beyond `Q`, and after 45 seconds without the position shrinking it gives up another tick every 10 seconds.
- **Joining the book.** The bot improves the best external bid or offer by one tick when that stays within the floors above, which gets fills in a thin book. It is limited to one reprice per side every 0.5 seconds.
- **Anchoring to the market.** Passive quotes are centred on 60% of the external mid and 40% of the model's fair value. This was added during live testing (see section 7).
- **Sizes.** Base sizes are 2 and 3 lots for TRADES and 5 and 10 for RANGE. The side that adds inventory shrinks as `1 − |x|`. The reducing side can grow to `1 + |x|` times base, capped at what would bring the position to target, so a quote never flips the position.
- **Position budget.** Only half of each limit is used for market making. The other half is kept free for sweeping mispriced orders. A hard guard also counts every resting and in-flight order, so a full fill of everything I have in the book can never breach a limit.
- **Toxicity.** Three fills on one side within 15 seconds widen that side by 1.5 times for 30 seconds.

## 4. Taking mispriced orders

A separate thread evaluated every order-book update, so it did not wait for the quoting loop.

| Rule | Trigger | Size |
|---|---|---|
| Below the guaranteed floor | An offer at or below the lowest value the product can settle at | No cap (risk-free) |
| Fat finger | More than 2σ through fair value, or at the minimum or maximum price | Up to the full limit, after checks |
| Moderate mispricing | 0.5σ through fair value (0.25σ if it reduces inventory) | 3 lots TRADES, 10 lots RANGE in total |
| Isolated order | An order far beyond a large gap within its own side of the book | Same cap as moderate |
| Resting traps | Small orders 3σ from fair value, and one tick inside the price limits | Whatever fills |

The checks before a large sweep were: the Coinbase data is under 1.5 seconds old, fair value has not moved by more than half of `σ` in 10 seconds, and the rest of the book agrees with my fair value. The unfilled remainder of a sweep is cancelled immediately, like an immediate-or-cancel order, and my own orders on the same side are cancelled first to make room and to avoid trading with myself.

## 5. Risk and endgame

- **Range spike guard.** When the price is within `max($3, σ√5s)` of the window high or low, RANGE asks are pulled and come back at half size, because a new extreme is about to move settlement.
- **Trades burst guard.** When the 5 second trade rate exceeds twice my estimate, TRADES asks are pulled for 10 seconds.
- **Fair value jumps.** A quote is replaced immediately if `F` moves by half the adverse-selection floor.
- **Flattening.** Over the last 120 seconds the quoting limit shrinks to zero and the give-up above rises, so the bot reaches settlement near flat. In the last 30 seconds it trades against external orders within `0.5σ` of fair value to close what is left, and in the last 5 seconds all quotes come off.
- **Kill switch.** All quotes are pulled if Coinbase has been silent for more than 3 seconds. A drawdown widens spreads and then stops the product.

## 6. Results

| | TRADES | RANGE |
|---|---|---|
| Lots bought / sold | 114 / 126 | 257 / 257 |
| Average buy / sell (exchange units) | 2,969.39 / 3,121.91 | 1,026.30 / 1,108.76 |
| Final position | −12 | 0 |
| P&L | 20,710 | 21,190 |

The exchange shows RANGE prices multiplied by the contract size of 10, so the 82 point gap between average buy and sell is about 8.2 dollars of price per lot.

**Attribution.** TRADES P&L splits into 17,387 from matched round trips (114 × 152.5) and 3,323 from the 12 lots still short at settlement (12 × (3,121.9 − 2,845)). RANGE ended flat, so all of its profit was realised on round trips. Together the round trips account for about 92% of the total. The mispriced-order logic was a small part of that: in the final run I sent about 2,900 quotes but only about 10 sweeps, with roughly 6 lots filled, and most sweep opportunities were skipped because resting quotes and inventory had already used the position-guard capacity.

**Context.** Several of the other top finishers ended near their limits, for example −20 in TRADES and −50 to −60 in RANGE, so their results depended much more on the settlement values than mine did.

## 7. What I tried and dropped

| Idea | Outcome |
|---|---|
| A static 5,840 trades per 15 minutes from a web search, which suggested buying TRADES against the 5,000 opening price | Dropped. Live Coinbase data showed a recent rate well below that, so I used a recent-rate estimate and same-time-of-day history. On the day, both were still too high. |
| Polymarket BTC markets as a fair-value input | Dropped. They price whether BTC goes up or down, which says little about trade count or range. |
| Deribit implied volatility for RANGE | Built, then switched off. The nearest expiries were pricing the next day's FOMC meeting, and the gap between implied and 15 minute realised volatility could not be calibrated in time. |
| Binance trade rate as an early signal for TRADES | Built, then switched off. I already see Coinbase's own trade rate live, and Binance gave no useful lead. |
| Funding rates, open interest and liquidation feeds | Dropped. No measurable improvement in 15 minute forecasts. |
| Same-time-of-day history for TRADES | Kept. In a leave-one-out test on 10 weekdays it cut pre-window forecast error about threefold against the recent rate alone (log RMSE 0.065 against 0.185). It still misjudged this particular day. |
| A directional position at the open when the opening book looked mispriced | Restricted to cases where the gap is at least 1.5 times the typical forecast error, with all data sources agreeing. It never triggered. |
| Centring all quotes on model fair value | Changed. With RANGE at 130 in the model and 167 to 178 in the market, my buy-backs never filled and I sat at −50. Blending the market mid into quote placement and allowing a larger give-up over the limit fixed it. |
| Repeating capped sweeps without a total cap | Changed. Each sweep was within its cap, but they repeated, and RANGE reached −50. The cap now applies to the total. |
| Closing out swept positions by locking in at least half of the captured edge | Designed, not built. Swept positions were closed through normal quoting. |
| Pulling quotes directly from the Coinbase thread | Not built. The spike and burst guards run on the 200 ms loop. |

## 8. Manual settlement override (built, not used)

The exchange displays a live settlement value for both products. Near the end of the window I could read that value and use it as a better estimate than the model's, so I built an override for it. I put the expected final value for a product in `settle_override.json`, and the bot reloaded the file within a second. While a value was set for a product, the bot:

- used my number as fair value and swept every order at least a set edge through it, up to the full position limit (the edge is `EDGE_TRADES` or `EDGE_RANGE`, 100 and 5 by default);
- stopped passive quoting for that product and turned off the endgame flattening, so the position was held to settlement;
- went back to normal market making as soon as the value was set back to `null`.

The intended use was a directional bet when the market traded well away from the displayed settlement value. I did not use it. I was only leading by a small margin in the last few minutes, and the override deliberately holds a large position through settlement, so a wrong reading would have cost more than I stood to gain. The result above is entirely from the market-making and sweep logic.

## 9. Limitations and what I would do next

- **One window, small margin.** The result comes from a single 15 minute window and a 3.6% lead. It shows the design worked on the day, not that it would win every time.
- **No backtest.** I validated with a dry-run mode (which logs the orders the bot would send without sending any), a short live test, then the window itself. The one quantitative check on the fair-value prior was a leave-one-out test on 10 weekdays, and the blend weights were chosen on those same days.
- **Prior too high.** The TRADES prior was far above the day's outcome. A longer history, with a term for market-wide volatility, would calibrate it better, and a small prior weight would limit the damage.
- **Count mismatch.** My Coinbase count finished 2.4% under the exchange's. I would confirm the exact counting rule with the organisers beforehand.
- **Sweeps blocked by the guard.** Most sweep opportunities were skipped for lack of position-guard capacity. Reserving capacity for sweeps, or cancelling resting quotes first when a large sweep appears, would use more of them.
- **Sizes are not optimised.** The spread multiplier (0.15σ), skew strength and sizes came from reasoning, not a parameter search, because there was no historical data to fit them on. With recorded Coinbase and exchange data I would tune them with a replay.

## How it was built

I split the work between two Claude agents in Claude Code. Claude Opus researched the strategy and calibrated it on live public Coinbase data, producing the written specifications in `docs/`. Claude Sonnet implemented them in Python on top of the exchange's starter SDK (`base_bot.py` and `models.py`, left unmodified). I directed both, chose which data sources to try, set the priorities and tested the bot against the live exchange, which is where the bugs in section 7 were found.

## Files

- `bot/models_fv.py`: TRADES and RANGE fair value, uncertainty and floor models.
- `bot/strategy.py`: quoting, sweeping, endgame and risk controls.
- `bot/coinbase_feed.py`: Coinbase websocket and REST seeding and backfill.
- `bot/execution.py`: rate limiter, order ledger and position guard.
- `bot/config.py` and `bot/main_bot.py`: parameters, window configuration and wiring.
- `docs/`: the specifications the bot was built from.
