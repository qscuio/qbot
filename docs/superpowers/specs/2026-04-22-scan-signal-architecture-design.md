# Scan Signal Architecture Design

## Goal

Upgrade the scanner from a flat list of loosely overlapping signals into a practical ticket-selection system that produces actionable `A` and `B` pools for short-, mid-, and long-horizon setups.

## Problems in the Current Scanner

- Many signals are shape detectors rather than tradable ticket rules.
- Several signals overlap heavily, especially around breakout and startup behavior.
- Output has no pool hierarchy, so the same detector is asked to both discover ideas and rank final tickets.
- Existing conditions miss practical trading filters such as tradability, liquidity, extension risk, and structural freshness.

## Design Summary

The first version keeps the existing raw signal detectors intact and adds a ranking layer on top of them:

1. Run the existing scanner and collect `signal_id -> hits`.
2. Evaluate each stock against a shared tradability filter.
3. Group stocks into three independent lines:
   - `short`: short-term strong continuation / relay
   - `mid`: medium-term trend acceleration
   - `long`: long-term bottom repair / reversal
4. For each line:
   - trigger one or more core structures
   - score supporting factors
   - apply risk penalties
   - assign `A`, `B`, or reject
5. Publish the ranked output back into scan results as six pool ids:
   - `pool_short_a`
   - `pool_short_b`
   - `pool_mid_a`
   - `pool_mid_b`
   - `pool_long_a`
   - `pool_long_b`

This keeps current routes and Telegram flows working while adding immediately usable ticket pools.

## Shared Filtering

Hard reject:

- `ST` / `*ST`
- names containing obvious delisting markers such as `退`
- invalid latest bar data
- insufficient bar history for the relevant line

Soft penalties:

- low price
- weak 20-day average turnover
- excessive extension from key moving averages
- weak close quality
- nearby resistance after breakout

## Core Structures

### Short Line

- `strong_reclaim`: recent strong move, controlled pullback, latest bar reclaims strength
- `platform_breakout`: recent leadership followed by a short consolidation and breakout

### Mid Line

- `trend_breakout`: established MA20/MA60 trend with breakout and healthy volume
- `pullback_resume`: trend intact, pullback to MA10/MA20, then renewed expansion

### Long Line

- `box_breakout`: extended low-base consolidation followed by clean departure
- `reversal_repair`: downtrend stabilizes, moving averages repair, price reclaims key mid-term levels

## Output Contract

Each ranked pool item should include:

- stock identity
- selected line and tier
- trigger id and trigger name
- numeric score
- reasons
- risk flags
- factor breakdown

The outer scan response remains backward compatible because pools are represented as additional `SignalHit` buckets.

## Integration Points

- New ranking logic lives outside `src/signals/*`.
- `ScannerService::run_full_scan` remains the entry point.
- Ranked pools are appended after raw signal detection.
- Telegram/API summary logic learns the new pool ids so users can directly inspect the pools.

## First Version Scope

- Implement shared filtering and scoring.
- Implement six ranked pools.
- Reuse existing signal detectors as supporting evidence, not as final ticket output.
- Do not replace auto trading in this change.
- Do not remove legacy signals in this change.

## Verification

- unit tests for line classification and pool generation
- route metadata tests for new pool labels
- targeted scanner tests to ensure ranked pools are injected into scan results
