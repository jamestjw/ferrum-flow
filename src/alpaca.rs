use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde::{Deserialize, Deserializer};

use crate::data::{BookLevel, BookSnapshot, TradeDirection, TradeEvent};

const ALPACA_DATA_BASE_URL: &str = "https://data.alpaca.markets";

pub struct AlpacaClient {
    http: Client,
    key_id: String,
    secret_key: String,
    base_url: String,
}

impl AlpacaClient {
    pub fn from_env() -> Result<Self> {
        let key_id = std::env::var("APCA_API_KEY_ID")
            .context("missing APCA_API_KEY_ID environment variable")?;
        let secret_key = std::env::var("APCA_API_SECRET_KEY")
            .context("missing APCA_API_SECRET_KEY environment variable")?;

        Ok(Self {
            http: Client::new(),
            key_id,
            secret_key,
            base_url: ALPACA_DATA_BASE_URL.to_string(),
        })
    }

    pub fn fetch_market_data(
        &self,
        symbol: &str,
        start: &str,
        end: &str,
        feed: &str,
    ) -> Result<(Vec<TradeEvent>, Vec<BookSnapshot>)> {
        let raw_quotes = self.fetch_quotes(symbol, start, end, feed)?;
        let raw_trades = self.fetch_trades(symbol, start, end, feed)?;

        if raw_trades.is_empty() {
            bail!("alpaca returned no trades for {symbol} in the requested window");
        }

        let trade_events = classify_trades(raw_trades, &raw_quotes);
        let book_snapshots = raw_quotes
            .into_iter()
            .map(|quote| BookSnapshot {
                timestamp: quote.timestamp,
                levels: vec![BookLevel {
                    level: 1,
                    bid_price: quote.bid_price,
                    bid_size: quote.bid_size,
                    ask_price: quote.ask_price,
                    ask_size: quote.ask_size,
                }],
            })
            .collect();

        Ok((trade_events, book_snapshots))
    }

    fn fetch_trades(
        &self,
        symbol: &str,
        start: &str,
        end: &str,
        feed: &str,
    ) -> Result<Vec<AlpacaTrade>> {
        let mut trades = Vec::new();
        let mut page_token = None::<String>;

        loop {
            let mut request = self
                .http
                .get(format!("{}/v2/stocks/{}/trades", self.base_url, symbol))
                .header("APCA-API-KEY-ID", &self.key_id)
                .header("APCA-API-SECRET-KEY", &self.secret_key)
                .query(&[
                    ("start", start),
                    ("end", end),
                    ("feed", feed),
                    ("limit", "10000"),
                ]);

            if let Some(token) = &page_token {
                request = request.query(&[("page_token", token.as_str())]);
            }

            let response = request
                .send()
                .with_context(|| format!("failed to request Alpaca trades for {symbol}"))?
                .error_for_status()
                .with_context(|| format!("Alpaca trades request failed for {symbol}"))?;

            let payload: AlpacaTradesResponse = response
                .json()
                .with_context(|| format!("failed to decode Alpaca trades response for {symbol}"))?;

            trades.extend(payload.trades);

            match payload.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }

        Ok(trades)
    }

    fn fetch_quotes(
        &self,
        symbol: &str,
        start: &str,
        end: &str,
        feed: &str,
    ) -> Result<Vec<AlpacaQuote>> {
        let mut quotes = Vec::new();
        let mut page_token = None::<String>;

        loop {
            let mut request = self
                .http
                .get(format!("{}/v2/stocks/{}/quotes", self.base_url, symbol))
                .header("APCA-API-KEY-ID", &self.key_id)
                .header("APCA-API-SECRET-KEY", &self.secret_key)
                .query(&[
                    ("start", start),
                    ("end", end),
                    ("feed", feed),
                    ("limit", "10000"),
                ]);

            if let Some(token) = &page_token {
                request = request.query(&[("page_token", token.as_str())]);
            }

            let response = request
                .send()
                .with_context(|| format!("failed to request Alpaca quotes for {symbol}"))?
                .error_for_status()
                .with_context(|| format!("Alpaca quotes request failed for {symbol}"))?;

            let payload: AlpacaQuotesResponse = response
                .json()
                .with_context(|| format!("failed to decode Alpaca quotes response for {symbol}"))?;

            quotes.extend(payload.quotes);

            match payload.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }

        Ok(quotes)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct AlpacaTradesResponse {
    #[serde(default, deserialize_with = "null_vec")]
    trades: Vec<AlpacaTrade>,
    next_page_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AlpacaQuotesResponse {
    #[serde(default, deserialize_with = "null_vec")]
    quotes: Vec<AlpacaQuote>,
    next_page_token: Option<String>,
}

fn null_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Deserialize)]
struct AlpacaTrade {
    #[serde(rename = "t")]
    timestamp: String,
    #[serde(rename = "p")]
    price: f64,
    #[serde(rename = "s")]
    size: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct AlpacaQuote {
    #[serde(rename = "t")]
    timestamp: String,
    #[serde(rename = "bp")]
    bid_price: f64,
    #[serde(rename = "bs")]
    bid_size: f64,
    #[serde(rename = "ap")]
    ask_price: f64,
    #[serde(rename = "as")]
    ask_size: f64,
}

fn classify_trades(trades: Vec<AlpacaTrade>, quotes: &[AlpacaQuote]) -> Vec<TradeEvent> {
    let mut classified = Vec::with_capacity(trades.len());
    let mut quote_idx = 0usize;
    let mut active_quote = quotes.first();
    let mut previous_trade_price = None::<f64>;
    let mut previous_direction = TradeDirection::Buy;

    for trade in trades {
        while quote_idx + 1 < quotes.len() && quotes[quote_idx + 1].timestamp <= trade.timestamp {
            quote_idx += 1;
            active_quote = Some(&quotes[quote_idx]);
        }

        let direction = infer_trade_direction(
            trade.price,
            active_quote,
            previous_trade_price,
            previous_direction,
        );

        previous_trade_price = Some(trade.price);
        previous_direction = direction;

        classified.push(TradeEvent {
            timestamp: trade.timestamp,
            price: trade.price,
            volume: trade.size,
            direction,
        });
    }

    classified
}

fn infer_trade_direction(
    trade_price: f64,
    quote: Option<&AlpacaQuote>,
    previous_trade_price: Option<f64>,
    previous_direction: TradeDirection,
) -> TradeDirection {
    if let Some(quote) = quote {
        if trade_price >= quote.ask_price {
            return TradeDirection::Buy;
        }

        if trade_price <= quote.bid_price {
            return TradeDirection::Sell;
        }

        let midpoint = (quote.bid_price + quote.ask_price) / 2.0;
        if trade_price > midpoint {
            return TradeDirection::Buy;
        }

        if trade_price < midpoint {
            return TradeDirection::Sell;
        }
    }

    if let Some(previous_price) = previous_trade_price {
        if trade_price > previous_price {
            return TradeDirection::Buy;
        }

        if trade_price < previous_price {
            return TradeDirection::Sell;
        }
    }

    previous_direction
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::data::TradeDirection;

    use super::{
        AlpacaQuote, AlpacaQuotesResponse, AlpacaTrade, AlpacaTradesResponse, classify_trades,
        infer_trade_direction,
    };

    #[test]
    fn infers_direction_from_midpoint() {
        let quote = AlpacaQuote {
            timestamp: "2026-04-27T09:30:00.100Z".into(),
            bid_price: 100.0,
            bid_size: 10.0,
            ask_price: 100.1,
            ask_size: 12.0,
        };

        assert_eq!(
            infer_trade_direction(100.09, Some(&quote), None, TradeDirection::Buy),
            TradeDirection::Buy
        );
        assert_eq!(
            infer_trade_direction(100.01, Some(&quote), None, TradeDirection::Buy),
            TradeDirection::Sell
        );
    }

    #[test]
    fn falls_back_to_tick_rule() {
        assert_eq!(
            infer_trade_direction(100.2, None, Some(100.1), TradeDirection::Sell),
            TradeDirection::Buy
        );
        assert_eq!(
            infer_trade_direction(100.0, None, Some(100.1), TradeDirection::Buy),
            TradeDirection::Sell
        );
    }

    #[test]
    fn classifies_trades_against_most_recent_quote() {
        let quotes = vec![
            AlpacaQuote {
                timestamp: "2026-04-27T09:30:00.100Z".into(),
                bid_price: 100.0,
                bid_size: 10.0,
                ask_price: 100.1,
                ask_size: 10.0,
            },
            AlpacaQuote {
                timestamp: "2026-04-27T09:30:00.200Z".into(),
                bid_price: 100.1,
                bid_size: 12.0,
                ask_price: 100.2,
                ask_size: 8.0,
            },
        ];
        let trades = vec![
            AlpacaTrade {
                timestamp: "2026-04-27T09:30:00.150Z".into(),
                price: 100.1,
                size: 50.0,
            },
            AlpacaTrade {
                timestamp: "2026-04-27T09:30:00.250Z".into(),
                price: 100.1,
                size: 25.0,
            },
        ];

        let classified = classify_trades(trades, &quotes);
        assert_eq!(classified[0].direction, TradeDirection::Buy);
        assert_eq!(classified[1].direction, TradeDirection::Sell);
    }

    #[test]
    fn deserializes_null_quote_and_trade_arrays() {
        let quotes: AlpacaQuotesResponse = serde_json::from_value(json!({
            "quotes": null,
            "next_page_token": null
        }))
        .unwrap();
        let trades: AlpacaTradesResponse = serde_json::from_value(json!({
            "trades": null,
            "next_page_token": null
        }))
        .unwrap();

        assert!(quotes.quotes.is_empty());
        assert!(trades.trades.is_empty());
    }
}
