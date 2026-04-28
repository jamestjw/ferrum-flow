use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, SecondsFormat, TimeDelta, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use clap::Parser;
use ferrum_flow::alpaca::AlpacaClient;
use ferrum_flow::analytics::{OfiMetrics, calculate_gofi, calculate_ofi, price_change, vwap};
use ferrum_flow::data::{BookSnapshot, TradeEvent, load_book_snapshots, load_trades};
use ferrum_flow::db::{self, DbConfig, SignalRecord};
use ferrum_flow::signal::{SignalConfig, SignalDecision, evaluate_signal};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Analyze OFI, NOFI, GOFI, and short-term order flow signals"
)]
struct Cli {
    #[arg(long, short)]
    config: Option<PathBuf>,

    #[arg(long)]
    symbol: Option<String>,

    #[arg(long)]
    start: Option<String>,

    #[arg(long)]
    end: Option<String>,

    #[arg(long)]
    feed: Option<String>,

    #[arg(long, default_value_t = false)]
    batch: bool,

    #[arg(long)]
    window_seconds: Option<i64>,

    #[arg(long)]
    poll_interval_seconds: Option<u64>,

    #[arg(long)]
    data_delay_seconds: Option<i64>,

    #[arg(long)]
    market_hours_only: Option<bool>,

    #[arg(long)]
    max_iterations: Option<usize>,

    #[arg(long)]
    csv_trades: Option<PathBuf>,

    #[arg(long)]
    csv_books: Option<PathBuf>,

    #[arg(long)]
    depth: Option<usize>,

    #[arg(long)]
    momentum_threshold: Option<f64>,

    #[arg(long)]
    absorption_ratio_threshold: Option<f64>,

    #[arg(long)]
    absorption_price_epsilon: Option<f64>,

    #[arg(long)]
    lambda: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct AppConfig {
    #[serde(default)]
    pub shared: SharedConfig,
    #[serde(default)]
    pub tickers: Vec<TickerConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
struct SharedConfig {
    pub market: MarketConfig,
    pub signal: SignalConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TickerConfig {
    pub symbol: String,
    pub market: Option<TickerMarketConfig>,
    pub signal: Option<TickerSignalConfig>,
}

impl TickerConfig {
    pub fn resolve(&self, shared: &SharedConfig) -> (MarketConfig, SignalConfig) {
        let mut market = shared.market.clone();
        let mut signal = shared.signal.clone();

        // Apply ticker market overrides
        if let Some(ref over) = self.market {
            if let Some(ref f) = over.feed { market.feed = f.clone(); }
            if let Some(w) = over.window_seconds { market.window_seconds = w; }
            if let Some(p) = over.poll_interval_seconds { market.poll_interval_seconds = p; }
            if let Some(d) = over.data_delay_seconds { market.data_delay_seconds = d; }
            if let Some(m) = over.market_hours_only { market.market_hours_only = m; }
            if let Some(dp) = over.depth { market.depth = dp; }
        }

        // Apply ticker signal overrides
        if let Some(ref over) = self.signal {
            if let Some(mt) = over.momentum_threshold { signal.momentum_threshold = mt; }
            if let Some(ar) = over.absorption_ratio_threshold { signal.absorption_ratio_threshold = ar; }
            if let Some(ae) = over.absorption_price_epsilon { signal.absorption_price_epsilon = ae; }
            if let Some(l) = over.lambda { signal.lambda = l; }
        }

        (market, signal)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
struct TickerMarketConfig {
    pub feed: Option<String>,
    pub window_seconds: Option<i64>,
    pub poll_interval_seconds: Option<u64>,
    pub data_delay_seconds: Option<i64>,
    pub market_hours_only: Option<bool>,
    pub depth: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
struct TickerSignalConfig {
    pub momentum_threshold: Option<f64>,
    pub absorption_ratio_threshold: Option<f64>,
    pub absorption_price_epsilon: Option<f64>,
    pub lambda: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct MarketConfig {
    pub symbol: Option<String>,
    #[serde(default = "default_feed")]
    pub feed: String,
    #[serde(default = "default_window")]
    pub window_seconds: i64,
    #[serde(default = "default_poll")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_delay")]
    pub data_delay_seconds: i64,
    #[serde(default = "default_market_hours")]
    pub market_hours_only: bool,
    #[serde(default = "default_depth")]
    pub depth: usize,
}

fn default_feed() -> String {
    "iex".to_string()
}
fn default_window() -> i64 {
    300
}
fn default_poll() -> u64 {
    60
}
fn default_delay() -> i64 {
    900
}
fn default_market_hours() -> bool {
    true
}
fn default_depth() -> usize {
    1
}

impl Default for MarketConfig {
    fn default() -> Self {
        Self {
            symbol: None,
            feed: default_feed(),
            window_seconds: default_window(),
            poll_interval_seconds: default_poll(),
            data_delay_seconds: default_delay(),
            market_hours_only: default_market_hours(),
            depth: default_depth(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            shared: SharedConfig::default(),
            tickers: Vec::new(),
        }
    }
}

impl AppConfig {
    pub fn load(cli: &Cli) -> Result<Self> {
        let mut config = if let Some(path) = &cli.config {
            let content = fs::read_to_string(path)
                .with_context(|| format!("failed to read config file: {}", path.display()))?;
            serde_yaml::from_str(&content).context("failed to parse yaml config")?
        } else if fs::metadata("config.yaml").is_ok() {
            let content =
                fs::read_to_string("config.yaml").context("failed to read config.yaml")?;
            serde_yaml::from_str(&content).context("failed to parse config.yaml")?
        } else {
            AppConfig::default()
        };

        // Global CLI overrides for shared config
        if let Some(ref f) = cli.feed {
            config.shared.market.feed = f.clone();
        }
        if let Some(w) = cli.window_seconds {
            config.shared.market.window_seconds = w;
        }
        if let Some(p) = cli.poll_interval_seconds {
            config.shared.market.poll_interval_seconds = p;
        }
        if let Some(d) = cli.data_delay_seconds {
            config.shared.market.data_delay_seconds = d;
        }
        if let Some(m) = cli.market_hours_only {
            config.shared.market.market_hours_only = m;
        }
        if let Some(dp) = cli.depth {
            config.shared.market.depth = dp;
        }

        if let Some(mt) = cli.momentum_threshold {
            config.shared.signal.momentum_threshold = mt;
        }
        if let Some(ar) = cli.absorption_ratio_threshold {
            config.shared.signal.absorption_ratio_threshold = ar;
        }
        if let Some(ae) = cli.absorption_price_epsilon {
            config.shared.signal.absorption_price_epsilon = ae;
        }
        if let Some(l) = cli.lambda {
            config.shared.signal.lambda = l;
        }

        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let app_config = AppConfig::load(&cli)?;

    let is_watch = !cli.batch && cli.csv_trades.is_none();

    if is_watch {
        return run_multi_ticker_watch(&cli, &app_config).await;
    }

    run_batch_mode(&cli, &app_config).await
}

async fn run_multi_ticker_watch(cli: &Cli, app_config: &AppConfig) -> Result<()> {
    let db_cfg = DbConfig::from_env()?;
    let pool = db::connect(&db_cfg.url()).await?;

    // Determine list of tickers to watch
    let tickers = if let Some(ref cli_symbol) = cli.symbol {
        vec![TickerConfig {
            symbol: cli_symbol.clone(),
            market: None,
            signal: None,
        }]
    } else if !app_config.tickers.is_empty() {
        app_config.tickers.clone()
    } else if let Some(ref shared_symbol) = app_config.shared.market.symbol {
        vec![TickerConfig {
            symbol: shared_symbol.clone(),
            market: None,
            signal: None,
        }]
    } else {
        anyhow::bail!("no symbol provided via CLI or config file");
    };

    let mut handles = Vec::new();
    for ticker in tickers {
        let (market, signal) = ticker.resolve(&app_config.shared);
        let pool = pool.clone();
        let symbol = ticker.symbol.clone();
        let max_iterations = cli.max_iterations;

        handles.push(tokio::spawn(async move {
            run_watch_mode(symbol, market, signal, pool, max_iterations).await
        }));
    }


    use futures::future::join_all;
    let results = join_all(handles).await;
    for res in results {
        res??;
    }

    Ok(())
}

async fn run_batch_mode(cli: &Cli, app_config: &AppConfig) -> Result<()> {
    let market = app_config.shared.market.clone();
    let signal = app_config.shared.signal.clone();

    // For batch mode, we only handle one symbol at a time from CLI
    let symbol = cli
        .symbol
        .as_deref()
        .or(app_config.shared.market.symbol.as_deref())
        .context("symbol must be provided for batch analysis")?;

    let (trades, snapshots) = load_input_data(cli, &market, symbol).await?;
    let _ = analyze_and_render(&trades, &snapshots, market.depth, &signal, None, None, None);

    Ok(())
}

async fn run_watch_mode(
    symbol: String,
    market: MarketConfig,
    signal: SignalConfig,
    pool: Pool<Postgres>,
    max_iterations: Option<usize>,
) -> Result<()> {
    if market.window_seconds <= 0 {
        anyhow::bail!("window_seconds must be greater than zero");
    }

    if market.data_delay_seconds < 0 {
        anyhow::bail!("data_delay_seconds must be zero or greater");
    }

    let client = AlpacaClient::from_env()?;
    let mut iteration = 0usize;

    loop {
        if let Some(limit) = max_iterations {
            if iteration >= limit {
                break;
            }
        }

        let end = Utc::now() - TimeDelta::seconds(market.data_delay_seconds);
        let start = end - TimeDelta::seconds(market.window_seconds);
        let start_text = start.to_rfc3339_opts(SecondsFormat::Secs, true);
        let end_text = end.to_rfc3339_opts(SecondsFormat::Secs, true);

        print_watch_banner(&symbol, iteration + 1, &start_text, &end_text);

        if market.market_hours_only && !is_regular_market_window(end) {
            println!(
                "[{}] Market Status: outside regular US equity hours, skipping fetch",
                symbol
            );
            iteration += 1;
            tokio::time::sleep(Duration::from_secs(market.poll_interval_seconds)).await;
            continue;
        }

        match client
            .fetch_market_data(&symbol, &start_text, &end_text, &market.feed)
            .await
        {
            Ok((trades, snapshots)) => {
                let last_signal = db::get_last_signal(&pool, &symbol).await.ok().flatten();
                if let Some(ref last) = last_signal {
                    println!(
                        "[{}] Last Recommendation: {:?} (Action: {:?})",
                        symbol, last.bias, last.action
                    );
                }

                let (metrics, decision) = analyze_and_render(
                    &trades,
                    &snapshots,
                    market.depth,
                    &signal,
                    Some(iteration + 1),
                    last_signal,
                    Some(&symbol),
                );

                let vwap_val = vwap(&trades);
                let obs_change = if snapshots.len() >= 2 {
                    price_change(&snapshots[0], &snapshots[snapshots.len() - 1])
                } else {
                    None
                };

                let record = SignalRecord {
                    timestamp: Utc::now(),
                    symbol: symbol.clone(),
                    ofi: metrics.ofi,
                    normalized_ofi: metrics.normalized_ofi,
                    total_volume: metrics.total_volume,
                    vwap: vwap_val,
                    observed_price_change: obs_change,
                    expected_price_change: decision.expected_price_change,
                    bias: decision.bias,
                    action: decision.action,
                    execution: decision.execution,
                    absorption_detected: decision.absorption_detected,
                };

                if let Err(e) = db::save_signal(&pool, &record).await {
                    println!("[{}] DB Save Error: {e:#}", symbol);
                }
            }
            Err(error) => {
                println!("[{}] Fetch Error: {error:#}", symbol);
            }
        }

        iteration += 1;
        tokio::time::sleep(Duration::from_secs(market.poll_interval_seconds)).await;
    }

    Ok(())
}

async fn load_input_data(
    cli: &Cli,
    market: &MarketConfig,
    symbol: &str,
) -> Result<(Vec<TradeEvent>, Vec<BookSnapshot>)> {
    match (
        cli.start.as_deref(),
        cli.end.as_deref(),
        cli.csv_trades.as_ref(),
    ) {
        (Some(start), Some(end), None) => AlpacaClient::from_env()?
            .fetch_market_data(
                symbol,
                start,
                end,
                cli.feed.as_deref().unwrap_or(&market.feed),
            )
            .await
            .with_context(|| format!("failed to fetch Alpaca market data for {symbol}")),
        (None, None, Some(trades_path)) => {
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

fn analyze_and_render(
    trades: &[TradeEvent],
    snapshots: &[BookSnapshot],
    depth: usize,
    config: &SignalConfig,
    iteration: Option<usize>,
    _last_signal: Option<SignalDecision>,
    _symbol: Option<&str>,
) -> (OfiMetrics, SignalDecision) {
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

    (metrics, decision)
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
