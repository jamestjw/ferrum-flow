use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, SecondsFormat, TimeDelta, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use clap::Parser;
use ferrum_flow::alpaca::AlpacaClient;
use ferrum_flow::analytics::{calculate_gofi, calculate_ofi, price_change, vwap};
use ferrum_flow::data::{BookSnapshot, TradeEvent, load_book_snapshots, load_trades};
use ferrum_flow::signal::{SignalConfig, evaluate_signal};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Analyze OFI, NOFI, GOFI, and short-term order flow signals"
)]
struct Cli {
    #[arg(long)]
    symbol: Option<String>,

    #[arg(long)]
    start: Option<String>,

    #[arg(long)]
    end: Option<String>,

    #[arg(long, default_value = "iex")]
    feed: String,

    #[arg(long, default_value_t = false)]
    watch: bool,

    #[arg(long, default_value_t = 300)]
    window_seconds: i64,

    #[arg(long, default_value_t = 5)]
    poll_interval_seconds: u64,

    #[arg(long, default_value_t = 900)]
    data_delay_seconds: i64,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    market_hours_only: bool,

    #[arg(long)]
    max_iterations: Option<usize>,

    #[arg(long)]
    csv_trades: Option<PathBuf>,

    #[arg(long)]
    csv_books: Option<PathBuf>,

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
    let config = SignalConfig {
        momentum_threshold: cli.momentum_threshold,
        absorption_ratio_threshold: cli.absorption_ratio_threshold,
        absorption_price_epsilon: cli.absorption_price_epsilon,
        lambda: cli.lambda,
    };

    if cli.watch {
        return run_watch_mode(&cli, &config);
    }

    let (trades, snapshots) = load_input_data(&cli)?;
    render_analysis(&trades, &snapshots, cli.depth, &config, None);

    Ok(())
}

fn run_watch_mode(cli: &Cli, config: &SignalConfig) -> Result<()> {
    let symbol = cli.symbol.as_deref().context("--watch requires --symbol")?;

    if cli.csv_trades.is_some() || cli.csv_books.is_some() {
        anyhow::bail!("--watch currently supports Alpaca mode only");
    }

    if cli.start.is_some() || cli.end.is_some() {
        anyhow::bail!("--watch uses a trailing window; omit --start and --end");
    }

    if cli.window_seconds <= 0 {
        anyhow::bail!("--window-seconds must be greater than zero");
    }

    if cli.data_delay_seconds < 0 {
        anyhow::bail!("--data-delay-seconds must be zero or greater");
    }

    let client = AlpacaClient::from_env()?;
    let mut iteration = 0usize;

    loop {
        if let Some(limit) = cli.max_iterations {
            if iteration >= limit {
                break;
            }
        }

        let end = Utc::now() - TimeDelta::seconds(cli.data_delay_seconds);
        let start = end - TimeDelta::seconds(cli.window_seconds);
        let start_text = start.to_rfc3339_opts(SecondsFormat::Secs, true);
        let end_text = end.to_rfc3339_opts(SecondsFormat::Secs, true);

        print_watch_banner(symbol, iteration + 1, &start_text, &end_text);

        if cli.market_hours_only && !is_regular_market_window(end) {
            println!("Market Status: outside regular US equity hours, skipping fetch");
            iteration += 1;
            thread::sleep(Duration::from_secs(cli.poll_interval_seconds));
            continue;
        }

        match client.fetch_market_data(symbol, &start_text, &end_text, &cli.feed) {
            Ok((trades, snapshots)) => {
                render_analysis(&trades, &snapshots, cli.depth, config, Some(iteration + 1));
            }
            Err(error) => {
                println!("Fetch Error: {error:#}");
            }
        }

        iteration += 1;
        thread::sleep(Duration::from_secs(cli.poll_interval_seconds));
    }

    Ok(())
}

fn load_input_data(cli: &Cli) -> Result<(Vec<TradeEvent>, Vec<BookSnapshot>)> {
    match (
        cli.symbol.as_deref(),
        cli.start.as_deref(),
        cli.end.as_deref(),
        cli.csv_trades.as_ref(),
    ) {
        (Some(symbol), Some(start), Some(end), None) => AlpacaClient::from_env()?
            .fetch_market_data(symbol, start, end, &cli.feed)
            .with_context(|| format!("failed to fetch Alpaca market data for {symbol}")),
        (None, None, None, Some(trades_path)) => {
            let trades = load_trades(trades_path)?;
            let snapshots = if let Some(path) = &cli.csv_books {
                load_book_snapshots(path)?
            } else {
                Vec::new()
            };
            Ok((trades, snapshots))
        }
        _ => anyhow::bail!(
            "provide either --symbol/--start/--end for Alpaca mode, --symbol with --watch for live mode, or --csv-trades for CSV mode"
        ),
    }
}

fn render_analysis(
    trades: &[TradeEvent],
    snapshots: &[BookSnapshot],
    depth: usize,
    config: &SignalConfig,
    iteration: Option<usize>,
) {
    let metrics = calculate_ofi(trades);
    let derived_vwap = vwap(trades);
    let last_trade_price = trades.last().map(|trade| trade.price);

    let (gofi, observed_price_change) = if snapshots.is_empty() {
        (None, None)
    } else {
        let previous = snapshots.first().expect("non-empty snapshots");
        let current = snapshots.last().expect("non-empty snapshots");
        (
            Some(calculate_gofi(previous, current, depth)),
            price_change(previous, current),
        )
    };

    let decision = evaluate_signal(
        &metrics,
        observed_price_change,
        last_trade_price.zip(derived_vwap),
        config,
    );

    if let Some(iteration) = iteration {
        println!("Iteration: {iteration}");
    }

    println!("OFI: {:.6}", metrics.ofi);
    println!("NOFI: {:.6}", metrics.normalized_ofi);
    println!("Total Volume: {:.6}", metrics.total_volume);
    println!("Trades: {}", trades.len());

    if let Some(gofi) = gofi {
        println!("GOFI(depth={}): {:.6}", depth, gofi);
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
}

fn print_watch_banner(symbol: &str, iteration: usize, start: &str, end: &str) {
    println!("---");
    println!("Symbol: {symbol}");
    println!("Window: {start} -> {end}");
    println!("Poll Iteration: {iteration}");
}

fn is_regular_market_window(timestamp: DateTime<Utc>) -> bool {
    let eastern = timestamp.with_timezone(&New_York);

    if matches!(eastern.weekday(), Weekday::Sat | Weekday::Sun) {
        return false;
    }

    let minutes = eastern.hour() * 60 + eastern.minute();
    let market_open = 9 * 60 + 30;
    let market_close = 16 * 60;

    minutes >= market_open && minutes < market_close
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::is_regular_market_window;

    #[test]
    fn detects_regular_market_hours() {
        let open_window = chrono::Utc.with_ymd_and_hms(2026, 4, 24, 14, 0, 0).unwrap();
        let closed_window = chrono::Utc.with_ymd_and_hms(2026, 4, 24, 21, 0, 0).unwrap();
        let weekend = chrono::Utc.with_ymd_and_hms(2026, 4, 25, 14, 0, 0).unwrap();

        assert!(is_regular_market_window(open_window));
        assert!(!is_regular_market_window(closed_window));
        assert!(!is_regular_market_window(weekend));
    }
}
