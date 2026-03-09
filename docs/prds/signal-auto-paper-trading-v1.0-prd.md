# Signal Auto Paper Trading - Product Requirements Document (PRD)

## Requirements Description

### Background
- **Business Problem**: The scanner can find daily signal hits, but there is no automated way to convert those hits into consistent paper-trading decisions, execution logic, or strategy-level PnL tracking.
- **Target Users**: The bot operator and channel users who want to observe how each signal behaves as a standalone strategy account.
- **Value Proposition**: Turn each signal into an independent paper-trading strategy with transparent entry, exit, and reporting rules.

### Feature Overview
- **Core Features**:
  - One strategy account per enabled signal
  - Independent capital per account, initialized at `100000`
  - Daily candidate selection from that signal's own hit list
  - Next-session intraday monitored entry with deterministic buy rules
  - `-5%` hard stop-loss and trailing-stop exit logic
  - Immediate Telegram push for buy/sell actions with reason text
  - End-of-day strategy report with balance, holdings, and PnL
- **Feature Boundaries**:
  - No real brokerage integration
  - No multi-position portfolio inside one signal account in v1
  - No user-customized strategy parameters in v1
  - No minute-bar storage; execution uses realtime snapshot quotes
- **User Scenarios**:
  - After the daily scan, each signal account picks its top-ranked stock
  - On the next trading session, the engine watches realtime quotes and buys when the entry rule is satisfied
  - While holding, the engine watches price and exits on stop-loss or trailing-stop conditions
  - At the close, users receive a daily strategy report

### Detailed Requirements
- **Input/Output**:
  - Input: scanner hits, daily bars, realtime Sina quotes
  - Output: strategy candidates, simulated positions, trade logs, Telegram messages, end-of-day report
- **User Interaction**:
  - Automatic execution via background loop and scheduler
  - Manual visibility via API and Telegram status/report commands
- **Data Requirements**:
  - Strategy account table keyed by `signal_id`
  - Candidate table for daily selected stocks
  - Position table for open/closed paper trades
  - Event log table for action traceability
- **Edge Cases**:
  - Weekend/holiday: candidate waits until next trading session
  - Candidate not bought before afternoon cutoff: expire with reason
  - Same-day sell forbidden to respect A-share `T+1`
  - If account cash cannot buy one lot, candidate is skipped with reason

## Execution Rules

### Account Model
- Each enabled signal has one strategy account
- One open position maximum per account
- Each account starts with `100000.00` cash

### Candidate Selection
- Run after the daily signal scan
- For each signal:
  - rank only stocks hit by that signal
  - select the highest score
  - save score and selection reasons

### Entry Rule
- Monitor the selected candidate on the next trading session
- Earliest buy time: `09:35`
- Latest buy time: `14:30`
- Buy only when:
  - price is above both today's open and yesterday's close
  - price is not more than `+3.5%` above yesterday's close
  - intraday price is in the stronger half of the day's range
- Use all available account cash, rounded down to one board lot (`100` shares)

### Exit Rule
- No same-day exit
- Hard stop-loss: sell when price is at or below `entry_price * 0.95`
- Trailing stop: after new highs are made, sell when price falls below `peak_price * (1 - trailing_stop_pct)`
- Default trailing stop pct in v1: `3.5%`

### Reporting
- Push buy/sell actions immediately with:
  - account / signal
  - selected stock
  - score
  - execution price
  - logic explanation
- Generate a daily report after market close with:
  - account cash
  - current holdings
  - realized / unrealized PnL
  - today's actions and skipped trades

## Acceptance Criteria
- Strategy accounts are auto-created for enabled signals
- After scan job completion, top candidates are persisted per signal
- During trading hours, the engine can buy qualifying candidates from realtime quotes
- Open positions are monitored and can be sold by stop-loss or trailing-stop logic
- Buy/sell actions are pushed to Telegram with explanation text
- End-of-day report is saved and retrievable
