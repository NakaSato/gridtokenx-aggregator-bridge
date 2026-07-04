//! Single-attempt surplus-mint policy shared by the settlement loop's
//! immediate attempt and the [`crate::mint_outbox`] drain loop's retries.
//!
//! Moved out of `src/main.rs` (which must stay wiring-only) so the
//! wallet-resolution / retry-vs-settle decision logic is reachable by
//! `cargo test -p aggregator-logic` instead of only the binary.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use aggregator_persistence::infra::meter_registry::MeterRegistry;
use aggregator_persistence::infra::mint::{MintGateway, MintReplyTimeout};
use tracing::{info, warn};

use crate::metrics;
use crate::mint_outbox::PendingMint;

/// Process-wide set of mint idempotency keys (`{serial}:{window_start_ms}`)
/// with an attempt currently in flight. The settlement sweep's immediate
/// attempt and the outbox drain's 30s retry tick share one instance, so the
/// drain no longer re-publishes an entry whose reply is still pending from
/// the sweep (the bridge rejected every such duplicate with "Submit already
/// in flight" — one wasted publish per meter per burst, ~10k at fleet scale).
/// Process-local by design: cross-process/replay dedup stays with the
/// bridge's idempotency guard and the on-chain `(meter_id, window)` PDA.
#[derive(Default)]
pub struct MintInFlight(Mutex<HashSet<String>>);

impl MintInFlight {
    /// Claims `key` for one attempt. `None` ⇒ another attempt on the same key
    /// is still awaiting its reply — skip; the outbox retries next tick.
    pub fn try_begin(self: &Arc<Self>, key: String) -> Option<MintInFlightGuard> {
        let mut set = self.0.lock().unwrap_or_else(|e| e.into_inner());
        set.insert(key.clone()).then(|| MintInFlightGuard {
            set: Arc::clone(self),
            key,
        })
    }
}

/// Releases the claimed key on drop (success, failure, or panic alike).
pub struct MintInFlightGuard {
    set: Arc<MintInFlight>,
    key: String,
}

impl Drop for MintInFlightGuard {
    fn drop(&mut self) {
        self.set
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

/// Attempts one on-chain mint for a pending surplus. Resolves the recipient
/// wallet fresh (so a meter that registers after its window still mints on a
/// retry), then asks Chain Bridge to mint. Returns `true` only when the mint is
/// **confirmed** — the caller then drops the outbox entry. Returns `false`
/// (keep + retry) for an unregistered wallet, a lookup error, a mint failure
/// (bridge/validator down, sim rejection), or an attempt for the same key
/// already in flight in this process. Idempotent: the bridge dedups on
/// `mint:{serial}:{window_start_ms}` + the on-chain PDA, so a retry of a mint
/// that already landed does not double-mint.
pub async fn attempt_mint(
    gw: &MintGateway,
    reg: &MeterRegistry,
    inflight: &Arc<MintInFlight>,
    p: &PendingMint,
) -> bool {
    let Some(_guard) = inflight.try_begin(p.field()) else {
        // A prior attempt (sweep or earlier drain tick) is still awaiting its
        // reply — publishing again would only bounce off the bridge's
        // in-flight guard. Keep the entry; the drain retries next tick.
        metrics::record_mint_outcome("skipped", "in_flight");
        return false;
    };
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
            metrics::record_mint_outcome("failed", mint_failure_reason(&e));
            false
        }
    }
}

/// Maps a mint error to its metric `reason` label. A reply timeout is counted
/// apart from hard failures: the intent was durably queued and may have landed
/// on-chain, so a rising `reply_timeout` series points at a lossy reply path
/// (bridge overload, slow confirm), not at mint rejections.
fn mint_failure_reason(e: &anyhow::Error) -> &'static str {
    if e.chain()
        .any(|c| c.downcast_ref::<MintReplyTimeout>().is_some())
    {
        "reply_timeout"
    } else {
        "mint_err"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Context};

    #[test]
    fn reply_timeout_classified_even_when_wrapped_with_context() {
        let e = anyhow::Error::new(MintReplyTimeout).context("mint attempt 3");
        assert_eq!(mint_failure_reason(&e), "reply_timeout");
    }

    #[test]
    fn bare_reply_timeout_classified() {
        let e = anyhow::Error::new(MintReplyTimeout);
        assert_eq!(mint_failure_reason(&e), "reply_timeout");
    }

    #[test]
    fn other_errors_stay_mint_err() {
        let e = anyhow!("bridge rejected: Custom(6000)");
        assert_eq!(mint_failure_reason(&e), "mint_err");
    }

    #[test]
    fn in_flight_key_claimed_once_until_guard_drops() {
        let inflight = Arc::new(MintInFlight::default());
        let g = inflight.try_begin("m1:900000".into());
        assert!(g.is_some());
        // Same key while held ⇒ rejected; different key ⇒ fine.
        assert!(inflight.try_begin("m1:900000".into()).is_none());
        assert!(inflight.try_begin("m2:900000".into()).is_some());
        drop(g);
        assert!(inflight.try_begin("m1:900000".into()).is_some());
    }

    #[test]
    fn in_flight_guard_releases_on_panic() {
        let inflight = Arc::new(MintInFlight::default());
        let inner = inflight.clone();
        let _ = std::panic::catch_unwind(move || {
            let _g = inner.try_begin("m1:900000".into());
            panic!("attempt blew up");
        });
        assert!(inflight.try_begin("m1:900000".into()).is_some());
    }
}
