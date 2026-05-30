# syntax=docker/dockerfile:1
# ── Base: chef + build deps ───────────────────────────────────────────────────
FROM rust:1.95-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build

# ── Planner: capture the full dependency graph ────────────────────────────────
FROM chef AS planner
COPY . .
RUN --mount=type=cache,id=shared-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=shared-cargo-registry,target=/usr/local/cargo/registry \
    cargo chef prepare --recipe-path recipe.json

# ── Builder ───────────────────────────────────────────────────────────────────
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json

RUN --mount=type=cache,id=shared-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=shared-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=email-bot-target,target=/build/target \
    cargo chef cook --release --locked --recipe-path recipe.json

COPY . .
RUN --mount=type=cache,id=shared-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=shared-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=email-bot-target,target=/build/target \
    cargo build --release --locked && \
    cp target/release/email-bot /email-bot

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /email-bot /usr/local/bin/email-bot

VOLUME /app/store
VOLUME /app/config
WORKDIR /app
ENV RUST_LOG=info
CMD ["email-bot", "/app/config/config.toml"]
