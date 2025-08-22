# Multi-stage build for smaller final image
FROM rust:1.89.0-bookworm AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install nightly toolchain
RUN rustup toolchain install nightly
RUN rustup default nightly

WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo +nightly build --release && rm -rf src

# Copy real source and build
COPY src ./src
RUN cargo +nightly build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/data-lander .
CMD ["./data-lander"]