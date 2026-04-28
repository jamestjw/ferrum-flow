# Build Stage
FROM rust:1.95-slim AS builder

# Install system dependencies needed for compilation (e.g., pkg-config, openssl)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build the release binary
RUN cargo build --release

# Final Runtime Stage
FROM debian:bookworm-slim

# Install libssl for runtime
RUN apt-get update && apt-get install -y \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/ferrum_flow /usr/local/bin/ferrum_flow

# Run the binary
ENTRYPOINT ["/usr/local/bin/ferrum_flow"]
