# syntax=docker/dockerfile:1.7

FROM node:22-bookworm-slim AS ui-builder
WORKDIR /workspace/frontend

# The lockfile-pinned pnpm from the packageManager field is activated through
# corepack, so the image build uses exactly the toolchain the repo declares.
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN corepack enable \
    && pnpm install --frozen-lockfile

COPY frontend/index.html frontend/vite.config.ts frontend/tsconfig.json ./
COPY frontend/public ./public
COPY frontend/src ./src
RUN pnpm build

FROM rust:1.89-bookworm AS builder
WORKDIR /workspace/rust

COPY rust/Cargo.toml rust/Cargo.lock rust/rust-toolchain.toml ./
COPY rust/crates ./crates
COPY rust/fixtures ./fixtures
COPY rust/config ./config

# The web crate's build script embeds this bundle into the binary; it must be
# in place before cargo runs so the image never ships the placeholder shell.
COPY --from=ui-builder /workspace/frontend/dist /workspace/frontend/dist

RUN cargo build \
    --release \
    --locked \
    --package crypto-trading-web-app \
    --bin crypto-trading-web

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/rust/target/release/crypto-trading-web /usr/local/bin/crypto-trading-web

RUN install --directory --owner=65532 --group=65532 /var/lib/crypto-trading

USER 65532:65532
WORKDIR /var/lib/crypto-trading

EXPOSE 8787

ENTRYPOINT ["/usr/local/bin/crypto-trading-web"]
