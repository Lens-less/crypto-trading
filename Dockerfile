# syntax=docker/dockerfile:1.7

FROM rust:1.89-bookworm AS builder
WORKDIR /workspace/rust

COPY rust/Cargo.toml rust/Cargo.lock rust/rust-toolchain.toml ./
COPY rust/crates ./crates
COPY rust/fixtures ./fixtures
COPY rust/config ./config

RUN cargo build \
    --release \
    --locked \
    --package crypto-trading-web-app \
    --bin crypto-trading-web

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/rust/target/release/crypto-trading-web /usr/local/bin/crypto-trading-web

RUN install --directory --owner=65532 --group=65532 /var/lib/crypto-trading

USER 65532:65532
WORKDIR /var/lib/crypto-trading

EXPOSE 8787

ENTRYPOINT ["/usr/local/bin/crypto-trading-web"]
