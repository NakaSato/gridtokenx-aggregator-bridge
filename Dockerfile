# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM rust:1.91-bookworm AS builder

# Install build dependencies with cache mount
RUN <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        clang \
        git \
        curl \
        protobuf-compiler
EOT

# Set working directory
WORKDIR /app

# Copy project subdirectories needed for the build
COPY gridtokenx-aggregator-bridge/ gridtokenx-aggregator-bridge/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/
COPY gridtokenx-iam-service/ gridtokenx-iam-service/
COPY gridtokenx-telemetry/ gridtokenx-telemetry/

WORKDIR /app/gridtokenx-aggregator-bridge

# Build in release mode with cargo cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/gridtokenx-aggregator-bridge/target \
    cargo build --release --bin gridtokenx-aggregator-bridge && \
    strip target/release/gridtokenx-aggregator-bridge && \
    cp target/release/gridtokenx-aggregator-bridge /app/aggregator-bridge-bin

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Minimal Debian)
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN <<EOT
    apt-get update
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tzdata
EOT

# Create non-root user
RUN <<EOT
    groupadd -g 1000 appgroup
    useradd -u 1000 -g appgroup -s /bin/sh appuser
EOT

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/aggregator-bridge-bin /app/aggregator-bridge

# Set ownership
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Run the binary
ENTRYPOINT ["/app/aggregator-bridge"]
