use crate::data::{BookSnapshot, TradeEvent};

#[derive(Debug, Clone, PartialEq)]
pub struct OfiMetrics {
    pub ofi: f64,
    pub total_volume: f64,
    pub normalized_ofi: f64,
}

pub fn calculate_ofi(trades: &[TradeEvent]) -> OfiMetrics {
    let ofi = trades
        .iter()
        .map(|trade| trade.direction.signed_volume(trade.volume))
        .sum::<f64>();

    let total_volume = trades.iter().map(|trade| trade.volume).sum::<f64>();
    let normalized_ofi = if total_volume > 0.0 {
        ofi / total_volume
    } else {
        0.0
    };

    OfiMetrics {
        ofi,
        total_volume,
        normalized_ofi,
    }
}

pub fn estimate_price_impact(ofi: f64, lambda: f64) -> f64 {
    lambda * ofi
}

pub fn calculate_gofi(previous: &BookSnapshot, current: &BookSnapshot, depth: usize) -> f64 {
    previous
        .levels
        .iter()
        .zip(current.levels.iter())
        .take(depth)
        .map(|(prev, curr)| (curr.bid_size - prev.bid_size) - (curr.ask_size - prev.ask_size))
        .sum()
}

pub fn price_change(previous: &BookSnapshot, current: &BookSnapshot) -> Option<f64> {
    Some(current.mid_price()? - previous.mid_price()?)
}

pub fn vwap(trades: &[TradeEvent]) -> Option<f64> {
    let total_notional = trades
        .iter()
        .map(|trade| trade.price * trade.volume)
        .sum::<f64>();
    let total_volume = trades.iter().map(|trade| trade.volume).sum::<f64>();

    if total_volume > 0.0 {
        Some(total_notional / total_volume)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{BookLevel, BookSnapshot, TradeDirection, TradeEvent};

    use super::{calculate_gofi, calculate_ofi, estimate_price_impact, price_change, vwap};

    #[test]
    fn computes_ofi_and_normalization() {
        let trades = vec![
            TradeEvent {
                timestamp: "t1".into(),
                price: 100.0,
                volume: 10.0,
                direction: TradeDirection::Buy,
            },
            TradeEvent {
                timestamp: "t2".into(),
                price: 99.5,
                volume: 4.0,
                direction: TradeDirection::Sell,
            },
        ];

        let metrics = calculate_ofi(&trades);
        assert_eq!(metrics.ofi, 6.0);
        assert_eq!(metrics.total_volume, 14.0);
        assert!((metrics.normalized_ofi - (6.0 / 14.0)).abs() < 1e-9);
    }

    #[test]
    fn computes_gofi_and_price_change() {
        let previous = BookSnapshot {
            timestamp: "t1".into(),
            levels: vec![
                BookLevel {
                    level: 1,
                    bid_price: 100.0,
                    bid_size: 40.0,
                    ask_price: 100.5,
                    ask_size: 50.0,
                },
                BookLevel {
                    level: 2,
                    bid_price: 99.5,
                    bid_size: 60.0,
                    ask_price: 101.0,
                    ask_size: 65.0,
                },
            ],
        };
        let current = BookSnapshot {
            timestamp: "t2".into(),
            levels: vec![
                BookLevel {
                    level: 1,
                    bid_price: 100.5,
                    bid_size: 50.0,
                    ask_price: 101.0,
                    ask_size: 40.0,
                },
                BookLevel {
                    level: 2,
                    bid_price: 100.0,
                    bid_size: 66.0,
                    ask_price: 101.5,
                    ask_size: 62.0,
                },
            ],
        };

        assert_eq!(calculate_gofi(&previous, &current, 2), 29.0);
        assert_eq!(price_change(&previous, &current), Some(0.5));
    }

    #[test]
    fn computes_vwap_and_impact() {
        let trades = vec![
            TradeEvent {
                timestamp: "t1".into(),
                price: 100.0,
                volume: 2.0,
                direction: TradeDirection::Buy,
            },
            TradeEvent {
                timestamp: "t2".into(),
                price: 101.0,
                volume: 1.0,
                direction: TradeDirection::Sell,
            },
        ];

        assert_eq!(estimate_price_impact(8.0, 0.25), 2.0);
        assert_eq!(vwap(&trades), Some(301.0 / 3.0));
    }
}
