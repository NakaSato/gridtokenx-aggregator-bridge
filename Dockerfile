# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM rust:1.88-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    cmake \
    clang \
    git \
    curl \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project subdirectories needed for the build
COPY gridtokenx-oracle-bridge/ gridtokenx-oracle-bridge/
COPY gridtokenx-blockchain-core/ gridtokenx-blockchain-core/
COPY gridtokenx-iam-service/crates/iam-protocol/proto/ gridtokenx-iam-service/crates/iam-protocol/proto/

WORKDIR /app/gridtokenx-oracle-bridge

# Build in release mode
RUN cargo build --release --bin gridtokenx-oracle-bridge

# Strip binary to reduce size
RUN strip target/release/gridtokenx-oracle-bridge

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Minimal Debian)
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/sh appuser

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/gridtokenx-oracle-bridge/target/release/gridtokenx-oracle-bridge /app/oracle-bridge

# Set ownership
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Run the binary
ENTRYPOINT ["/app/oracle-bridge"]
