use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

use crate::signal::{Action, Bias, ExecutionMode, SignalDecision};

pub struct DbConfig {
    pub host: String,
    pub port: String,
    pub user: String,
    pub pass: String,
    pub name: String,
}

impl DbConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            host: std::env::var("DB_HOST").context("DB_HOST missing")?,
            port: std::env::var("DB_PORT").context("DB_PORT missing")?,
            user: std::env::var("DB_USER").context("DB_USER missing")?,
            pass: std::env::var("DB_PASSWORD").context("DB_PASSWORD missing")?,
            name: std::env::var("DB_NAME").context("DB_NAME missing")?,
        })
    }

    pub fn url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.pass, self.host, self.port, self.name
        )
    }
}

pub async fn connect(url: &str) -> Result<Pool<Postgres>> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(url)
        .await
        .context("failed to connect to postgres")
}

pub struct SignalRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub ofi: f64,
    pub normalized_ofi: f64,
    pub total_volume: f64,
    pub vwap: Option<f64>,
    pub observed_price_change: Option<f64>,
    pub expected_price_change: f64,
    pub bias: Bias,
    pub action: Action,
    pub execution: ExecutionMode,
    pub absorption_detected: bool,
}

pub async fn save_signal(pool: &Pool<Postgres>, record: &SignalRecord) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO signals (
            timestamp, symbol, ofi, normalized_ofi, total_volume, vwap,
            observed_price_change, expected_price_change, bias, action,
            execution, absorption_detected
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#,
    )
    .bind(record.timestamp)
    .bind(&record.symbol)
    .bind(record.ofi)
    .bind(record.normalized_ofi)
    .bind(record.total_volume)
    .bind(record.vwap)
    .bind(record.observed_price_change)
    .bind(record.expected_price_change)
    .bind(bias_to_int(&record.bias))
    .bind(action_to_int(&record.action))
    .bind(execution_to_int(&record.execution))
    .bind(record.absorption_detected)
    .execute(pool)
    .await
    .context("failed to insert signal into db")?;

    Ok(())
}

pub async fn get_last_signal(
    pool: &Pool<Postgres>,
    symbol: &str,
) -> Result<Option<SignalDecision>> {
    let row = sqlx::query(
        r#"
        SELECT bias, action, execution, expected_price_change, absorption_detected
        FROM signals
        WHERE symbol = $1
        ORDER BY timestamp DESC
        LIMIT 1
        "#,
    )
    .bind(symbol)
    .fetch_optional(pool)
    .await
    .context("failed to fetch last signal from db")?;

    if let Some(row) = row {
        use sqlx::Row;
        Ok(Some(SignalDecision {
            bias: int_to_bias(row.try_get("bias")?),
            action: int_to_action(row.try_get("action")?),
            execution: int_to_execution(row.try_get("execution")?),
            expected_price_change: row.try_get("expected_price_change")?,
            absorption_detected: row.try_get("absorption_detected")?,
        }))
    } else {
        Ok(None)
    }
}

fn bias_to_int(bias: &Bias) -> i16 {
    match bias {
        Bias::Long => 1,
        Bias::Short => -1,
        Bias::Neutral => 0,
    }
}

fn int_to_bias(val: i16) -> Bias {
    match val {
        1 => Bias::Long,
        -1 => Bias::Short,
        _ => Bias::Neutral,
    }
}

fn execution_to_int(exec: &ExecutionMode) -> i16 {
    match exec {
        ExecutionMode::Aggressive => 1,
        ExecutionMode::Passive => 2,
        ExecutionMode::Neutral => 0,
    }
}

fn int_to_execution(val: i16) -> ExecutionMode {
    match val {
        1 => ExecutionMode::Aggressive,
        2 => ExecutionMode::Passive,
        _ => ExecutionMode::Neutral,
    }
}

fn action_to_int(action: &Action) -> i16 {
    match action {
        Action::NoTrade => 0,
        Action::EnterLong => 1,
        Action::EnterShort => 2,
        Action::ExitLong => 3,
        Action::WaitPassive => 4,
        Action::ReverseShort => 5,
        Action::ConfirmLongAtVwap => 6,
    }
}

fn int_to_action(val: i16) -> Action {
    match val {
        1 => Action::EnterLong,
        2 => Action::EnterShort,
        3 => Action::ExitLong,
        4 => Action::WaitPassive,
        5 => Action::ReverseShort,
        6 => Action::ConfirmLongAtVwap,
        _ => Action::NoTrade,
    }
}
