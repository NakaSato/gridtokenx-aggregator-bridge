//! Single-attempt surplus-mint policy shared by the settlement loop's
//! immediate attempt and the [`crate::mint_outbox`] drain loop's retries.
//!
//! Moved out of `src/main.rs` (which must stay wiring-only) so the
//! wallet-resolution / retry-vs-settle decision logic is reachable by
//! `cargo test -p aggregator-logic` instead of only the binary.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use aggregator_persistence::infra::meter_registry::MeterRegistry;
use aggregator_persistence::infra::mint::{
    wallet_is_valid, BatchMintInput, MintBatchItemResult, MintGateway, MintReplyTimeout,
};
use aggregator_persistence::infra::pg_readings::PgReadingsWriter;
use tracing::{debug, info, warn};

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
    writeback: Option<&PgReadingsWriter>,
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
        Ok(Some(w)) if !wallet_is_valid(&w) => {
            // A wallet the bridge can never parse (`Pubkey::from_str` rejects
            // it) — publishing would just burn a NATS round-trip per retry.
            // Kept: the wallet is re-resolved each attempt, so a corrected
            // registry entry still mints; the outbox age bound eventually
            // parks entries that never heal.
            warn!(
                "surplus mint deferred: invalid recipient wallet '{w}' for meter {} (kept for retry)",
                p.meter_serial
            );
            metrics::record_mint_outcome("skipped", "invalid_wallet");
            return false;
        }
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
            // Reflect the confirmed mint onto the reading rows so the trading UI's
            // Reading History flips pending → minted and shows the signature.
            // Best-effort: a DB fault must not fail the (already on-chain) mint.
            if let Some(wb) = writeback {
                match wb
                    .mark_minted(&p.meter_serial, p.window_start_ms, &out.signature, out.slot)
                    .await
                {
                    Ok(n) => debug!(
                        "mint write-back marked {n} reading row(s) minted for meter {} window {}",
                        p.meter_serial, p.window_start_ms
                    ),
                    Err(e) => warn!(
                        "mint write-back failed for meter {} window {} ({e}); Reading History stays pending until a retry re-confirms",
                        p.meter_serial, p.window_start_ms
                    ),
                }
            }
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

/// Classification of a batch reply — which outbox `field`s to remove (confirmed
/// on-chain) and how many recipients confirmed vs failed. Pure (no I/O, no
/// metrics) so the confirm/keep mapping is unit-testable without a gateway.
#[derive(Debug, Default, PartialEq)]
pub struct BatchClassification {
    /// Outbox fields (`{serial}:{window}`) of confirmed mints — caller removes.
    pub confirmed_fields: Vec<String>,
    pub confirmed: usize,
    pub failed: usize,
}

/// Maps the bridge's per-item batch results back to outbox fields. A result's
/// `idempotency_key` is `mint:{serial}:{window}`; the outbox `field` is the same
/// without the `mint:` prefix (see `PendingMint::field`).
fn classify_batch_results(results: &[MintBatchItemResult]) -> BatchClassification {
    let mut c = BatchClassification::default();
    for r in results {
        if r.success {
            let field = r
                .idempotency_key
                .strip_prefix("mint:")
                .unwrap_or(&r.idempotency_key)
                .to_string();
            c.confirmed_fields.push(field);
        } else {
            c.failed += 1;
        }
    }
    c.confirmed = c.confirmed_fields.len();
    c
}

/// Batched sibling of [`attempt_mint`]: resolves + mints a slice of pending
/// surpluses in ONE Chain Bridge round-trip. Returns the outbox `field`s of the
/// recipients that **confirmed** on-chain — the caller removes exactly those and
/// leaves the rest for the next drain tick.
///
/// Per-recipient skips (in-flight, unregistered/invalid wallet, lookup error)
/// mirror [`attempt_mint`] and simply exclude that recipient from the batch — it
/// stays in the outbox. An envelope-level failure (publish/ack/reply-timeout)
/// keeps every attempted recipient. Idempotent: the bridge dedups each item on
/// `mint:{serial}:{window}` + the on-chain PDA, so a retry never double-mints.
pub async fn attempt_mint_batch(
    gw: &MintGateway,
    reg: &MeterRegistry,
    inflight: &Arc<MintInFlight>,
    writeback: Option<&PgReadingsWriter>,
    pending: &[PendingMint],
) -> Vec<String> {
    // Resolve wallets and claim in-flight keys, holding each guard across the
    // batch round-trip so a concurrent tick can't re-publish the same key.
    let mut inputs: Vec<BatchMintInput> = Vec::new();
    let mut guards: Vec<MintInFlightGuard> = Vec::new();
    for p in pending {
        let Some(guard) = inflight.try_begin(p.field()) else {
            metrics::record_mint_outcome("skipped", "in_flight");
            continue;
        };
        let wallet = match reg.resolve_wallet(&p.meter_serial).await {
            Ok(Some(w)) if !wallet_is_valid(&w) => {
                warn!(
                    "surplus mint deferred: invalid recipient wallet '{w}' for meter {} (kept for retry)",
                    p.meter_serial
                );
                metrics::record_mint_outcome("skipped", "invalid_wallet");
                continue; // guard drops → key released; nothing published
            }
            Ok(Some(w)) => w,
            Ok(None) => {
                warn!(
                    "surplus mint deferred: no wallet registered for meter {} (kept for retry)",
                    p.meter_serial
                );
                metrics::record_mint_outcome("skipped", "no_wallet");
                continue;
            }
            Err(e) => {
                warn!(
                    "surplus mint deferred: wallet lookup failed for {} ({e}); kept for retry",
                    p.meter_serial
                );
                metrics::record_mint_outcome("skipped", "resolve_err");
                continue;
            }
        };
        inputs.push(BatchMintInput {
            recipient_wallet: wallet,
            energy_kwh: p.energy_kwh,
            meter_id: *p.meter_id.as_bytes(),
            meter_serial: p.meter_serial.clone(),
            window_start_ms: p.window_start_ms,
        });
        guards.push(guard);
    }

    if inputs.is_empty() {
        return Vec::new();
    }

    let result = match gw.mint_batch(&inputs).await {
        Ok(results) => {
            let c = classify_batch_results(&results);
            info!(
                "⚡ batch surplus mint: {} confirmed, {} failed of {} submitted",
                c.confirmed,
                c.failed,
                inputs.len()
            );
            for _ in 0..c.confirmed {
                metrics::record_mint_outcome("settled", "ok");
            }
            for _ in 0..c.failed {
                metrics::record_mint_outcome("failed", "mint_err");
            }
            // Write each confirmed mint back onto its window's reading rows (same
            // pending → minted flip as `attempt_mint`). Best-effort per item.
            if let Some(wb) = writeback {
                write_back_confirmed(wb, &results).await;
            }
            c.confirmed_fields
        }
        Err(e) => {
            // Envelope-level failure — the whole batch is kept for retry.
            let reason = mint_failure_reason(&e);
            warn!(
                "batch surplus mint failed ({e}); {} entries kept in outbox for retry",
                inputs.len()
            );
            for _ in 0..inputs.len() {
                metrics::record_mint_outcome("failed", reason);
            }
            Vec::new()
        }
    };
    // `guards` drop here, releasing every claimed in-flight key.
    result
}

/// Parse a batch item's `idempotency_key` (`mint:{serial}:{window_start_ms}`)
/// into `(meter_serial, window_start_ms)` for the mint write-back. The serial is
/// a UUID (no colon); the window is the trailing all-digit segment, so splitting
/// on the LAST colon separates them even though the `mint:` prefix adds one.
/// Returns `None` for an unexpected shape or unparsable window (skip write-back,
/// never panic — the outbox removal already keyed off the same field).
fn parse_mint_field(idempotency_key: &str) -> Option<(&str, i64)> {
    let field = idempotency_key
        .strip_prefix("mint:")
        .unwrap_or(idempotency_key);
    let (serial, window) = field.rsplit_once(':')?;
    let window_start_ms = window.parse::<i64>().ok()?;
    Some((serial, window_start_ms))
}

/// Write every confirmed batch item back onto its window's reading rows. Skips
/// items that failed, carry no signature, or whose key doesn't parse. Each row
/// UPDATE is best-effort: a DB error is logged, never propagated (the mints are
/// already on-chain; the outbox has already dropped these fields).
async fn write_back_confirmed(wb: &PgReadingsWriter, results: &[MintBatchItemResult]) {
    for r in results {
        if !r.success {
            continue;
        }
        let Some(sig) = r.signature.as_deref() else {
            continue;
        };
        let Some((serial, window_start_ms)) = parse_mint_field(&r.idempotency_key) else {
            warn!(
                "mint write-back skipped: unparsable idempotency_key '{}'",
                r.idempotency_key
            );
            continue;
        };
        match wb.mark_minted(serial, window_start_ms, sig, r.slot).await {
            Ok(n) => debug!(
                "mint write-back marked {n} reading row(s) minted for meter {serial} window {window_start_ms}"
            ),
            Err(e) => warn!(
                "mint write-back failed for meter {serial} window {window_start_ms} ({e}); Reading History stays pending until a retry re-confirms"
            ),
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

    fn item(idem: &str, success: bool) -> MintBatchItemResult {
        MintBatchItemResult {
            index: 0,
            idempotency_key: idem.to_string(),
            success,
            signature: success.then(|| "sig".to_string()),
            slot: if success { 100 } else { 0 },
            error: (!success).then(|| "unconfirmed".to_string()),
        }
    }

    #[test]
    fn classify_maps_confirmed_items_to_outbox_fields() {
        let results = vec![
            item("mint:MTR-1:1000", true),
            item("mint:MTR-2:1000", false),
            item("mint:MTR-3:2000", true),
        ];
        let c = classify_batch_results(&results);
        // Only the confirmed items become removable fields (mint: prefix stripped).
        assert_eq!(c.confirmed_fields, vec!["MTR-1:1000", "MTR-3:2000"]);
        assert_eq!(c.confirmed, 2);
        assert_eq!(c.failed, 1);
    }

    #[test]
    fn classify_all_failed_removes_nothing() {
        let results = vec![item("mint:A:1", false), item("mint:B:1", false)];
        let c = classify_batch_results(&results);
        assert!(c.confirmed_fields.is_empty());
        assert_eq!(c.confirmed, 0);
        assert_eq!(c.failed, 2);
    }

    #[test]
    fn classify_tolerates_key_without_mint_prefix() {
        // Defensive: an unexpected key shape must not panic — it maps verbatim.
        let c = classify_batch_results(&[item("A:1", true)]);
        assert_eq!(c.confirmed_fields, vec!["A:1"]);
    }

    #[test]
    fn classify_empty_is_empty() {
        assert_eq!(classify_batch_results(&[]), BatchClassification::default());
    }

    #[test]
    fn parse_mint_field_splits_uuid_serial_and_window() {
        // Real key shape: UUID serial (contains no colon) + trailing window ms.
        let (serial, window) =
            parse_mint_field("mint:3eb13b90-4668-4257-bdd6-40fb06671ad1:1781078400000").unwrap();
        assert_eq!(serial, "3eb13b90-4668-4257-bdd6-40fb06671ad1");
        assert_eq!(window, 1_781_078_400_000);
    }

    #[test]
    fn parse_mint_field_tolerates_missing_prefix() {
        let (serial, window) = parse_mint_field("MTR-1:2000").unwrap();
        assert_eq!(serial, "MTR-1");
        assert_eq!(window, 2000);
    }

    #[test]
    fn parse_mint_field_rejects_bad_shapes() {
        assert!(parse_mint_field("mint:no-window").is_none()); // no colon after strip
        assert!(parse_mint_field("mint:MTR-1:notanumber").is_none()); // window unparsable
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
