# Horizon-First Stock Picking Design

## Goal

Make short-, mid-, and long-horizon stock picking the primary scanner model. Keep the existing signal detectors, but use them as evidence inside the new horizon classifications instead of exposing them as the main user workflow.

## Current State

The scanner already runs all enabled detectors, stores raw hits, and appends six ranked pools:

- `pool_short_a`
- `pool_short_b`
- `pool_mid_a`
- `pool_mid_b`
- `pool_long_a`
- `pool_long_b`

The ranked pool layer lives in `src/services/scan_ranker.rs`. It is useful but still behaves like a post-processing layer over raw signal hits. A stock only enters ranking when at least one old detector fires, and the Telegram menu still presents scanner use as signal categories first.

## Design Principles

The new system treats horizon as the product surface and raw signals as supporting evidence.

- Users choose a horizon first: short, mid, or long.
- Each horizon produces A and B tiers.
- Raw signals remain available for debugging, statistics, and explanation.
- Existing pool IDs remain stable to preserve API, Redis cache, database history, chart contexts, and auto-trading integrations.
- The scanner should explain why each stock appears: trigger, score, reasons, risks, and supporting old signals.

## Horizon Definitions

### Short Horizon

Short horizon targets 1-5 trading days. It should favor high momentum, recent strength, controlled consolidation, and clean reclaim behavior.

Primary setups:

- Strong disagreement reclaim: a strong stock pulls back or shakes out, then closes strong again.
- Platform breakout: a recent leader consolidates tightly, then breaks short resistance.
- Volume-price acceleration: price breaks a recent high with usable volume expansion and good close quality.

Risk controls:

- Reject obvious ST, delisting, invalid data, and illiquid names.
- Penalize extreme distance above MA20.
- Penalize weak close position.
- Prefer limited recent range compression before breakout.

### Mid Horizon

Mid horizon targets 5-20 trading days. It should favor trend continuation, breakout from an established structure, and low-risk pullback resumes.

Primary setups:

- Trend breakout: price sits above MA20, MA20 is not below MA60, and price breaks a 20-day high with volume.
- Pullback resume: trend remains intact, price tests MA10 or MA20, then resumes upward.
- Volume-confirmed trend: old trend and volume signals confirm the same direction.

Risk controls:

- Avoid stocks extended too far from MA20.
- Require enough history for MA60.
- Penalize weak turnover and poor close quality.

### Long Horizon

Long horizon targets 20 or more trading days. It should favor base repair, bottom accumulation, and long-cycle reversal rather than short-term spikes.

Primary setups:

- Bottom box breakout: stock builds a lower base, keeps the range controlled, then leaves the box with volume.
- Reversal repair: downtrend stabilizes, MA20 turns flat or upward, and price reclaims key moving-average levels.
- Long-cycle confirmation: weekly/monthly or old long-cycle signals support the repair.

Risk controls:

- Use longer history than the current 70-bar minimum when available.
- Avoid names already far above the recent base.
- Penalize low liquidity and failed close quality.

## Old Signal Adaptation

Old signals map into the new model as evidence:

- Short evidence: `strong_first_neg`, `broken_board`, `fanbao`, `kuangbiao`, `breakout`, `startup`, `volume_surge`, `volume_price`.
- Mid evidence: `ma_bullish`, `ma_pullback`, `strong_pullback`, `uptrend_breakout`, `breakout`, `volume_price`, `startup`.
- Long evidence: `low_accumulation`, `bottom_quick_start`, `downtrend_reversal`, `long_cycle_reversal`, `weekly_monthly_bullish`.

The old signal buckets should still be stored and can remain visible under an advanced or legacy section. They should not be the default navigation path.

## Scanner Changes

`ScannerService::run_full_scan` should continue to run raw detectors and collect raw hits. Ranking should then evaluate every stock with enough bars, not only stocks that already have raw hits. Raw hits become optional evidence, not a hard gate.

`RankInput` should keep:

- stock identity
- daily bars
- raw signal hits

The ranker should produce one or more horizon candidates per stock. The first version can keep the current "best pool per stock" behavior if duplicate output is too noisy, but the metadata should preserve all matched setup candidates so we can later expose multi-horizon matches.

## Bot Entry Changes

Telegram should expose horizon-first navigation:

- `/pick short`
- `/pick mid`
- `/pick long`
- `/pick` for the horizon menu

The scan menu should lead with:

- Short picking
- Mid picking
- Long picking
- Prestart
- Stats
- Advanced signals

`/scan` should remain valid. It should run the same scanner and present horizon pools first. Raw signal categories move behind an "Advanced signals" submenu.

## API And Storage Compatibility

Keep these IDs:

- `pool_short_a`
- `pool_short_b`
- `pool_mid_a`
- `pool_mid_b`
- `pool_long_a`
- `pool_long_b`

This avoids a migration for existing scan results and keeps the auto-trading code that already references A-tier pools working.

Optional follow-up endpoints can add cleaner aliases:

- `/api/pick/short`
- `/api/pick/mid`
- `/api/pick/long`

The first implementation can reuse `/api/scan/latest`.

## Output Contract

Each pool hit should include:

- `line_type`: `short`, `mid`, or `long`
- `tier`: `A` or `B`
- `trigger_id`
- `trigger_name`
- `score`
- `reasons`
- `risk_flags`
- `factor_breakdown`
- `supporting_signals`
- `matched_setups` when more than one setup matches

Telegram buttons should show stock name, code, and score. The panel text should describe the horizon and top reasons, not just say "signal hit".

## Testing

Add or update tests for:

- ranker evaluates stocks without raw signal hits when price/volume structure qualifies
- old raw signals add evidence to new horizon scoring
- short, mid, and long pools still sort before raw signals in summaries
- `/pick` help text and menu buttons exist
- `/scan` still works and exposes advanced raw signals
- existing pool IDs remain unchanged

## Non-Goals

This change will not remove old signals, rewrite auto-trading, or change historical database schema. It will not attempt portfolio sizing or live intraday execution.

## Implementation Order

1. Refactor ranker gates so raw signals are evidence, not required input.
2. Add structure-only classifiers for short, mid, and long setups.
3. Preserve existing pool IDs and metadata.
4. Add `/pick` command parsing and Telegram menu entries.
5. Move old signal categories behind advanced navigation.
6. Extend tests around ranking, menu routing, and compatibility.
