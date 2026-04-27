# Ferrum Flow

Rust CLI for basic microstructure analytics around order flow imbalance.

It currently supports:

- OFI from buyer- and seller-initiated trades
- Normalized OFI (NOFI)
- GOFI-style depth aggregation across book snapshots
- Linear price impact estimate using `lambda * OFI`
- Simple signal rules for momentum, absorption reversal, and VWAP confirmation

## What It Does

The project reads a batch of trade prints and, optionally, order book snapshots, then turns them into a small set of microstructure metrics and proposed short-term actions.

### Calculations

- `OFI`: sums signed trade volume across the input window.
  - `buy` trades add volume.
  - `sell` trades subtract volume.
  - A positive result means net aggressive buying pressure.
  - A negative result means net aggressive selling pressure.
- `NOFI`: normalizes `OFI` by total traded volume in the same window.
  - This makes the imbalance comparable across liquid and illiquid names.
  - A value near `1.0` means trade flow was overwhelmingly buy-initiated.
  - A value near `-1.0` means trade flow was overwhelmingly sell-initiated.
- `GOFI`: compares book depth between two snapshots and sums bid-size increases minus ask-size increases across the configured number of levels.
  - Rising bid depth and falling ask depth pushes GOFI positive.
  - Falling bid depth and rising ask depth pushes GOFI negative.
- `VWAP`: computes volume-weighted average trade price for the loaded trade window.
- `Observed Mid-Price Change`: if book snapshots are provided, the tool compares the first and last mid-price.
- `Expected Price Change`: applies the simple linear model `lambda * OFI`.

### Proposed Actions

The tool does not place trades. It proposes a short-term action from the metrics above.

- `EnterLong`
  - Proposed when `NOFI` is above the configured momentum threshold.
  - Rationale: strong positive imbalance suggests aggressive buyers are consuming offer liquidity and may push price upward.
- `EnterShort`
  - Proposed when `NOFI` is below the negative momentum threshold.
  - Rationale: strong negative imbalance suggests aggressive sellers are hitting bids and may push price downward.
- `ConfirmLongAtVwap`
  - Proposed when the last trade price is close to VWAP and `NOFI` is strongly positive.
  - Rationale: this treats VWAP as a broader reference level and OFI as the real-time confirmation that buyers are defending that area.
- `ReverseShort`
  - Proposed when buy-side imbalance is strong but observed price movement is flat within the configured epsilon.
  - Rationale: this is a simple absorption heuristic. If buy pressure is large but price does not advance, the current implementation assumes hidden or replenishing sell liquidity is absorbing the flow and that upside momentum may be exhausted.
- `NoTrade`
  - Proposed when none of the above conditions are met.
  - Rationale: the observed imbalance is not strong enough to justify a directional view.

### Execution Mode

Each proposal also includes an execution posture.

- `Aggressive`
  - Used for positive momentum and the current absorption-reversal rule.
  - Rationale: if the signal is time-sensitive, the model assumes waiting may reduce edge.
- `Passive`
  - Used for negative momentum in the current rules.
  - Rationale: this mirrors the idea of slowing or staging execution instead of crossing the spread immediately.
- `Neutral`
  - Used when there is no trade proposal.

### Important Limits

- The current logic works on the full loaded dataset as one analysis window; it does not yet compute rolling sub-minute windows.
- The absorption rule is heuristic, not a calibrated statistical model.
- GOFI here is based on level-to-level size deltas only; it does not yet model queue position, hidden liquidity, or full quote-state transitions.
- The action labels are research outputs, not production execution instructions.

## Trade CSV

`direction` must be `buy` or `sell`.

```csv
timestamp,price,volume,direction
2026-04-27T09:30:00.100Z,100.10,250,buy
2026-04-27T09:30:00.140Z,100.11,120,sell
```

## Book CSV

Each row is one level at one timestamp.

```csv
timestamp,level,bid_price,bid_size,ask_price,ask_size
2026-04-27T09:30:00.100Z,1,100.09,1200,100.10,900
2026-04-27T09:30:00.100Z,2,100.08,1400,100.11,1100
2026-04-27T09:30:00.200Z,1,100.10,1500,100.11,700
2026-04-27T09:30:00.200Z,2,100.09,1450,100.12,1000
```

## Run

```bash
cargo run -- \
  --trades trades.csv \
  --books book.csv \
  --depth 2 \
  --momentum-threshold 0.20 \
  --absorption-ratio-threshold 3.0 \
  --absorption-price-epsilon 0.01 \
  --lambda 0.0001
```

## Notes

- This is a research scaffold, not a production trading system.
- The current absorption heuristic is intentionally simple and should be recalibrated with real market data.
- GOFI here uses size deltas across levels; if you want queue-position logic or quote-shift-aware decomposition, that can be added next.
