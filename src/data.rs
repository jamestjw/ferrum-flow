use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeDirection {
    Buy,
    Sell,
}

impl TradeDirection {
    pub fn signed_volume(self, volume: f64) -> f64 {
        match self {
            Self::Buy => volume,
            Self::Sell => -volume,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradeEvent {
    pub timestamp: String,
    pub price: f64,
    pub volume: f64,
    pub direction: TradeDirection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BookLevelRow {
    pub timestamp: String,
    pub level: usize,
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookLevel {
    pub level: usize,
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookSnapshot {
    pub timestamp: String,
    pub levels: Vec<BookLevel>,
}

impl BookSnapshot {
    pub fn best_bid(&self) -> Option<f64> {
        self.levels.first().map(|level| level.bid_price)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.levels.first().map(|level| level.ask_price)
    }

    pub fn mid_price(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }
}

pub fn load_trades(path: impl AsRef<Path>) -> Result<Vec<TradeEvent>> {
    let path = path.as_ref();
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("failed to open trades CSV: {}", path.display()))?;

    let mut trades = Vec::new();
    for row in reader.deserialize() {
        let trade: TradeEvent =
            row.with_context(|| format!("failed to parse trade row from {}", path.display()))?;
        trades.push(trade);
    }

    Ok(trades)
}

pub fn load_book_snapshots(path: impl AsRef<Path>) -> Result<Vec<BookSnapshot>> {
    let path = path.as_ref();
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("failed to open book CSV: {}", path.display()))?;

    let mut grouped = BTreeMap::<String, Vec<BookLevel>>::new();
    for row in reader.deserialize() {
        let entry: BookLevelRow =
            row.with_context(|| format!("failed to parse book row from {}", path.display()))?;

        grouped.entry(entry.timestamp).or_default().push(BookLevel {
            level: entry.level,
            bid_price: entry.bid_price,
            bid_size: entry.bid_size,
            ask_price: entry.ask_price,
            ask_size: entry.ask_size,
        });
    }

    let mut snapshots = Vec::with_capacity(grouped.len());
    for (timestamp, mut levels) in grouped {
        levels.sort_by_key(|level| level.level);
        snapshots.push(BookSnapshot { timestamp, levels });
    }

    Ok(snapshots)
}
