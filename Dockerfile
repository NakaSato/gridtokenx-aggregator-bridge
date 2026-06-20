# syntax=docker/dockerfile:1.7
# -----------------------------------------------------------------------------
# Aggregator Bridge — distroless image: binary + its shared libs only.
# No Rust toolchain, no target/ in the image (target lives in a BuildKit cache).
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

# Collect the binary + its non-glibc shared libs into a flat lib/ folder.
# glibc core + the dynamic loader come from the distroless/cc base — skip them.
RUN set -eux; \
    BIN=/app/aggregator-bridge-bin; \
    mkdir -p /out/lib; \
    cp "$BIN" /out/aggregator-bridge; \
    ldd "$BIN" | awk '/=>/{print $3} !/=>/{print $1}' | grep -E '^/' | sort -u | while read -r lib; do \
        case "$lib" in \
            */ld-linux*|*/libc.so*|*/libm.so*|*/libpthread*|*/libdl.so*|*/librt.so*) continue;; \
        esac; \
        cp -Lv "$lib" /out/lib/; \
    done

# -----------------------------------------------------------------------------
# Stage 2: Runtime (distroless, non-root uid 65532)
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app

# Copy binary + its lib folder from the builder stage
COPY --from=builder /out/aggregator-bridge /app/aggregator-bridge
COPY --from=builder /out/lib/ /app/lib/

ENV LD_LIBRARY_PATH=/app/lib

# Run the binary
ENTRYPOINT ["/app/aggregator-bridge"]
