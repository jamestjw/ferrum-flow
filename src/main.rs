use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use ferrum_flow::analytics::{calculate_gofi, calculate_ofi, price_change, vwap};
use ferrum_flow::data::{load_book_snapshots, load_trades};
use ferrum_flow::signal::{SignalConfig, evaluate_signal};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Analyze OFI, NOFI, GOFI, and short-term order flow signals"
)]
struct Cli {
    #[arg(long)]
    trades: PathBuf,

    #[arg(long)]
    books: Option<PathBuf>,

    #[arg(long, default_value_t = 1)]
    depth: usize,

    #[arg(long, default_value_t = 0.20)]
    momentum_threshold: f64,

    #[arg(long, default_value_t = 3.0)]
    absorption_ratio_threshold: f64,

    #[arg(long, default_value_t = 0.01)]
    absorption_price_epsilon: f64,

    #[arg(long, default_value_t = 0.0001)]
    lambda: f64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let trades = load_trades(&cli.trades)?;
    let metrics = calculate_ofi(&trades);
    let derived_vwap = vwap(&trades);
    let last_trade_price = trades.last().map(|trade| trade.price);

    let (gofi, observed_price_change) = if let Some(path) = &cli.books {
        let snapshots = load_book_snapshots(path)?;
        let previous = snapshots
            .first()
            .context("book CSV must contain at least one snapshot")?;
        let current = snapshots
            .last()
            .context("book CSV must contain at least one snapshot")?;
        (
            Some(calculate_gofi(previous, current, cli.depth)),
            price_change(previous, current),
        )
    } else {
        (None, None)
    };

    let config = SignalConfig {
        momentum_threshold: cli.momentum_threshold,
        absorption_ratio_threshold: cli.absorption_ratio_threshold,
        absorption_price_epsilon: cli.absorption_price_epsilon,
        lambda: cli.lambda,
    };

    let decision = evaluate_signal(
        &metrics,
        observed_price_change,
        last_trade_price.zip(derived_vwap),
        &config,
    );

    println!("OFI: {:.6}", metrics.ofi);
    println!("NOFI: {:.6}", metrics.normalized_ofi);
    println!("Total Volume: {:.6}", metrics.total_volume);

    if let Some(gofi) = gofi {
        println!("GOFI(depth={}): {:.6}", cli.depth, gofi);
    }

    if let Some(vwap) = derived_vwap {
        println!("VWAP: {:.6}", vwap);
    }

    if let Some(delta) = observed_price_change {
        println!("Observed Mid-Price Change: {:.6}", delta);
    }

    println!(
        "Expected Price Change: {:.6}",
        decision.expected_price_change
    );
    println!("Bias: {:?}", decision.bias);
    println!("Execution: {:?}", decision.execution);
    println!("Action: {:?}", decision.action);
    println!("Absorption Detected: {}", decision.absorption_detected);

    Ok(())
}
