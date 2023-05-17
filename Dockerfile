FROM lukemathwalker/cargo-chef:latest-rust-1.67.1-slim AS chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder 
COPY --from=planner recipe.json recipe.json
RUN apt-get update && apt-get install -y libudev-dev clang pkg-config libssl-dev build-essential cmake
RUN rustup component add rustfmt && update-ca-certificates
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bins

FROM debian:bullseye-slim as base_image
RUN apt-get update && apt-get -y install ca-certificates libssl1.1

# We do not need the Rust toolchain to run the binary!
FROM base_image AS runtime
COPY --from=builder /target/release/server /usr/local/bin
COPY --from=builder /target/release/worker /usr/local/bin
COPY --from=builder markets.json .
COPY --from=builder ca.cer .
