#!/usr/bin/env bash
#
# e2e-settlement-replay.sh — on-chain exactly-once proof for Path B generation mint.
#
# Proves the residual that the app-side Redis MINTED_SET marker alone cannot close:
# a crash between mint-submit and bin-eviction (or a Redis outage that loses the
# marker) MUST NOT double-mint. The guarantee lives on-chain — a per-(meter, window)
# GenerationMintRecord PDA in the energy-token program makes a replayed mint a no-op
# (see ARCHITECTURE.md §3 "Settlement", and gridtokenx-anchor mint_generation).
#
# What this script does (single-meter, controlled run):
#   1. Ingest signed telemetry for one meter until a 15-min billing bin completes
#      and is persisted to the durable bin store (gridtokenx:settlement:bins).
#   2. Snapshot that bin hash via Redis COPY (exact serialized bytes, no parsing).
#   3. Let the settlement engine tick: it mints GRX on-chain, then evicts the bin
#      and stamps the MINTED_SET marker. Record the recipient balance B1.
#   4. Reconstruct the crash state: STOP the bridge, COPY the snapshot back into the
#      live bins hash, and DEL the MINTED_SET marker. This is exactly "submitted but
#      never evicted, marker lost" — the unrecoverable case for an app-side guard.
#   5. START the bridge. On boot it reloads the bin (load_all) and the next tick
#      re-submits the mint for the SAME (meter, window). The on-chain PDA already
#      has minted == true, so mint_generation returns Ok(()) without minting.
#   6. Record the balance B2 and assert B2 == B1. A double mint (B2 > B1) FAILS.
#
# Prerequisites (operator brings these up — this script does NOT bootstrap them):
#   - A bootstrapped validator with the GRX Token-2022 mint + token_info_2022, and
#     the energy-token program REDEPLOYED with mint_generation (Stage 2).
#   - Chain Bridge reachable and able to sign (Vault or dev keypair).
#   - The aggregator bridge running, with REDIS_URL pointed at a reachable Redis.
#   - The meter ONBOARDED (registered in IAM with a resolvable wallet) so settlement
#     can attribute the mint. The default ingest path uses the simulator's --onboard.
#   - redis-cli, spl-token, curl, jq on PATH.
#
# Usage:
#   Configure the vars below (or export them), then:
#     ./scripts/e2e-settlement-replay.sh
#   Exit 0 = exactly-once held. Exit 1 = double mint or setup failure.

set -euo pipefail

# ----------------------------------------------------------------------------
# Config — override via environment.
# ----------------------------------------------------------------------------
REDIS_URL="${REDIS_URL:-redis://localhost:7010}"
BRIDGE_URL="${AGGREGATOR_BRIDGE_URL:-http://localhost:4010}"
SOLANA_RPC="${SOLANA_RPC:-http://localhost:8899}"

# The onboarded recipient wallet and the GRX Token-2022 mint. REQUIRED — read these
# off the bootstrap output / IAM after onboarding the test meter.
WALLET="${WALLET:?set WALLET to the onboarded recipient pubkey}"
GRX_MINT="${GRX_MINT:?set GRX_MINT to the energy-token GRX (mint_2022) pubkey}"
TOKEN_2022_PROGRAM_ID="${TOKEN_2022_PROGRAM_ID:-TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb}"

# Redis keys — keep in sync with crates/aggregator-api/src/ingester/bin_store.rs.
BINS_HASH="gridtokenx:settlement:bins"
MINTED_SET="gridtokenx:settlement:minted"
BACKUP_HASH="gridtokenx:settlement:bins:e2e-replay-backup"

# How telemetry is generated. Default drives the superproject smart-meter simulator
# with --onboard (registers the meter→wallet) for a single meter. Override INGEST_CMD
# to plug in your own signed-telemetry source.
INGEST_CMD="${INGEST_CMD:-}"

# How the bridge process is stopped/started for the crash-replay. These MUST restart
# the SAME bridge instance (so it reloads the re-injected bin at boot). Defaults
# assume the superproject orchestrator; override for your run topology.
BRIDGE_STOP_CMD="${BRIDGE_STOP_CMD:-}"
BRIDGE_START_CMD="${BRIDGE_START_CMD:-}"

# Timing.
SETTLE_TICK_SECS="${SETTLE_TICK_SECS:-60}"   # settlement engine interval (run loop)
BIN_WAIT_SECS="${BIN_WAIT_SECS:-1200}"        # max wait for a bin to complete (15-min window + slack)
SETTLE_WAIT_SECS="${SETTLE_WAIT_SECS:-180}"   # max wait for a tick to mint

# ----------------------------------------------------------------------------
# Helpers.
# ----------------------------------------------------------------------------
log()  { printf '\033[36m[e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# redis-cli bound to REDIS_URL.
r() { redis-cli -u "$REDIS_URL" "$@"; }

# On-chain GRX balance for the recipient's Token-2022 ATA. Prints a bare number
# (0 if the ATA does not exist yet).
grx_balance() {
  spl-token balance "$GRX_MINT" \
    --owner "$WALLET" \
    --program-id "$TOKEN_2022_PROGRAM_ID" \
    --url "$SOLANA_RPC" 2>/dev/null || echo "0"
}

# Poll a command until it prints "1" (truthy) or the deadline passes.
wait_until() {
  local desc="$1" deadline_secs="$2" probe="$3"
  local waited=0
  while true; do
    if [[ "$(eval "$probe")" == "1" ]]; then return 0; fi
    if (( waited >= deadline_secs )); then
      fail "timed out after ${deadline_secs}s waiting for: ${desc}"
    fi
    sleep 5; waited=$((waited + 5))
  done
}

require() { command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"; }

bridge_healthy() {
  curl -fsS --max-time 3 "${BRIDGE_URL}/health" >/dev/null 2>&1 && echo 1 || echo 0
}

# ----------------------------------------------------------------------------
# Phase 0 — preflight.
# ----------------------------------------------------------------------------
log "Phase 0: preflight"
require redis-cli; require spl-token; require curl
r ping >/dev/null || fail "Redis unreachable at ${REDIS_URL}"
[[ "$(bridge_healthy)" == "1" ]] || fail "bridge /health not OK at ${BRIDGE_URL}"
[[ -n "$BRIDGE_STOP_CMD" && -n "$BRIDGE_START_CMD" ]] \
  || fail "set BRIDGE_STOP_CMD and BRIDGE_START_CMD (script must restart the bridge for the replay)"
log "Redis OK, bridge healthy, recipient ${WALLET}"

# Snapshot starting balance so the assertion is independent of any prior state.
B0="$(grx_balance)"
log "starting GRX balance B0 = ${B0}"

# ----------------------------------------------------------------------------
# Phase 1 — ingest until a bin completes and is persisted.
# ----------------------------------------------------------------------------
log "Phase 1: ingest telemetry"
if [[ -n "$INGEST_CMD" ]]; then
  log "running INGEST_CMD"
  eval "$INGEST_CMD" || fail "INGEST_CMD failed"
else
  log "no INGEST_CMD set — drive the superproject simulator manually, e.g.:"
  log "  just send-meter-reading meters=1 interval=15   (with --onboard for a fresh meter)"
  log "waiting for a bin to appear in ${BINS_HASH} ..."
fi

# Wait for at least one completed bin to be persisted (15-min window must close).
wait_until "a completed bin in ${BINS_HASH}" "$BIN_WAIT_SECS" \
  '[[ "$(r hlen "$BINS_HASH")" -gt 0 ]] && echo 1 || echo 0'
log "bin persisted: HLEN ${BINS_HASH} = $(r hlen "$BINS_HASH")"

# ----------------------------------------------------------------------------
# Phase 2 — snapshot the bin BEFORE the settlement tick evicts it.
# ----------------------------------------------------------------------------
log "Phase 2: snapshot bin hash (Redis COPY — exact bytes, no parsing)"
r del "$BACKUP_HASH" >/dev/null
r copy "$BINS_HASH" "$BACKUP_HASH" >/dev/null \
  || fail "COPY failed (needs Redis 6.2+); cannot snapshot bin"
log "snapshot stored in ${BACKUP_HASH} (HLEN $(r hlen "$BACKUP_HASH"))"

# ----------------------------------------------------------------------------
# Phase 3 — let the engine mint; record B1.
# ----------------------------------------------------------------------------
log "Phase 3: wait for the settlement tick to mint on-chain"
# The marker is stamped only after a confirmed submit, so its appearance is the
# signal that the mint went out.
wait_until "MINTED_SET marker (mint submitted)" "$SETTLE_WAIT_SECS" \
  '[[ "$(r scard "$MINTED_SET")" -gt 0 ]] && echo 1 || echo 0'
# Give finalization a moment so the balance read reflects the mint.
sleep "$SETTLE_TICK_SECS"
B1="$(grx_balance)"
log "post-settlement GRX balance B1 = ${B1}"
[[ "$B1" != "$B0" ]] || fail "balance did not grow after settlement (B1 == B0 == ${B0}); mint never landed — check Chain Bridge / onboarding"

# ----------------------------------------------------------------------------
# Phase 4 — reconstruct the crash state: bin un-evicted, marker lost.
# ----------------------------------------------------------------------------
log "Phase 4: STOP bridge, re-inject bin, drop marker (simulate crash-before-evict + lost marker)"
eval "$BRIDGE_STOP_CMD" || fail "BRIDGE_STOP_CMD failed"
wait_until "bridge to stop" 60 '[[ "$(bridge_healthy)" == "0" ]] && echo 1 || echo 0'

r copy "$BACKUP_HASH" "$BINS_HASH" replace >/dev/null || fail "restore COPY failed"
r del "$MINTED_SET" >/dev/null
log "bins hash restored (HLEN $(r hlen "$BINS_HASH")), MINTED_SET cleared (SCARD $(r scard "$MINTED_SET"))"

# ----------------------------------------------------------------------------
# Phase 5 — restart; the replayed mint must be an on-chain no-op.
# ----------------------------------------------------------------------------
log "Phase 5: START bridge — it reloads the bin and re-submits the mint"
eval "$BRIDGE_START_CMD" || fail "BRIDGE_START_CMD failed"
wait_until "bridge to come healthy" 120 "[[ \"\$(bridge_healthy)\" == \"1\" ]] && echo 1 || echo 0"

# Wait for the re-settlement tick to re-submit (marker reappears), then for finality.
wait_until "re-settlement tick (replay submitted)" "$SETTLE_WAIT_SECS" \
  '[[ "$(r scard "$MINTED_SET")" -gt 0 ]] && echo 1 || echo 0'
sleep "$SETTLE_TICK_SECS"
B2="$(grx_balance)"
log "post-replay GRX balance B2 = ${B2}"

# ----------------------------------------------------------------------------
# Phase 6 — assert exactly-once.
# ----------------------------------------------------------------------------
r del "$BACKUP_HASH" >/dev/null || true
if [[ "$B2" == "$B1" ]]; then
  ok "exactly-once held: replay was an on-chain no-op (B1 == B2 == ${B1}, started ${B0})"
  exit 0
else
  fail "DOUBLE MINT: balance grew on replay (B1 = ${B1} → B2 = ${B2}); on-chain idempotency regressed"
fi
