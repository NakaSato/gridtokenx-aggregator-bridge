//! Single-attempt surplus-mint policy shared by the settlement loop's
//! immediate attempt and the [`crate::mint_outbox`] drain loop's retries.
//!
//! Moved out of `src/main.rs` (which must stay wiring-only) so the
//! wallet-resolution / retry-vs-settle decision logic is reachable by
//! `cargo test -p aggregator-logic` instead of only the binary.

use aggregator_persistence::infra::meter_registry::MeterRegistry;
use aggregator_persistence::infra::mint::MintGateway;
use tracing::{info, warn};

use crate::metrics;
use crate::mint_outbox::PendingMint;

/// Attempts one on-chain mint for a pending surplus. Resolves the recipient
/// wallet fresh (so a meter that registers after its window still mints on a
/// retry), then asks Chain Bridge to mint. Returns `true` only when the mint is
/// **confirmed** — the caller then drops the outbox entry. Returns `false`
/// (keep + retry) for an unregistered wallet, a lookup error, or a mint failure
/// (bridge/validator down, sim rejection). Idempotent: the bridge dedups on
/// `mint:{serial}:{window_start_ms}` + the on-chain PDA, so a retry of a mint
/// that already landed does not double-mint.
pub async fn attempt_mint(gw: &MintGateway, reg: &MeterRegistry, p: &PendingMint) -> bool {
    let wallet = match reg.resolve_wallet(&p.meter_serial).await {
        Ok(Some(w)) => w,
        Ok(None) => {
            // Unregistered meter: kept in the outbox so it mints once the owner
            // registers a wallet. Counted so the skip is visible on dashboards.
            warn!(
                "surplus mint deferred: no wallet registered for meter {} (kept for retry)",
                p.meter_serial
            );
            metrics::record_mint_outcome("skipped", "no_wallet");
            return false;
        }
        Err(e) => {
            warn!(
                "surplus mint deferred: wallet lookup failed for {} ({e}); kept for retry",
                p.meter_serial
            );
            metrics::record_mint_outcome("skipped", "resolve_err");
            return false;
        }
    };
    match gw
        .mint(
            &wallet,
            p.energy_kwh,
            *p.meter_id.as_bytes(),
            &p.meter_serial,
            p.window_start_ms,
        )
        .await
    {
        Ok(out) => {
            info!(
                "⚡ minted {} kWh surplus for meter {} (sig={}, slot={})",
                p.energy_kwh, p.meter_serial, out.signature, out.slot
            );
            metrics::record_mint_outcome("settled", "ok");
            true
        }
        Err(e) => {
            warn!(
                "surplus mint failed for meter {} ({e}); kept in outbox for retry",
                p.meter_serial
            );
            metrics::record_mint_outcome("failed", "mint_err");
            false
        }
    }
}
