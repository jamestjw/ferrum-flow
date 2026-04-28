CREATE TABLE signals (
  id BIGSERIAL PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  ofi DOUBLE PRECISION NOT NULL,
  normalized_ofi DOUBLE PRECISION NOT NULL,
  total_volume DOUBLE PRECISION NOT NULL,
  vwap DOUBLE PRECISION,
  observed_price_change DOUBLE PRECISION,
  expected_price_change DOUBLE PRECISION NOT NULL,
  bias SMALLINT NOT NULL,
  action SMALLINT NOT NULL,
  execution SMALLINT NOT NULL,
  absorption_detected BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX signals_symbol_timestamp_idx
  ON signals (symbol, timestamp);
