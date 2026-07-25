# Plan: Terminate OCPP 2.0.1 in the Aggregator Bridge (Bridge-as-CSMS)

> Status: **design only — no code written.** Scope is this service alone; nothing here changes
> chain-bridge, meter-service, or IAM. Derived from a multi-source research pass (2026-07-25),
> 23 sources → 115 extracted claims → 25 adversarially verified (3-vote), 15 confirmed / 10 refuted.
> Claims below are marked **[verified]**, **[refuted]**, or **[design]** (our inference, not cited).

## The goal

Let EV charge points speak **OCPP 2.0.1 natively to this service** — the bridge acts as the CSMS,
stations open WebSocket connections to it — so charging telemetry joins the same verify → zone
Redis Streams → InfluxDB → 15-min billing-bin path that DLMS meters already use, and so the
dispatch engine can actuate charging as a flexibility resource.

Today the only meter protocol is DLMS/COSEM (`crates/aggregator-stacks/src/stacks/dlms.rs:71`),
and EV/battery state arrives only as manufacturer-specific OBIS registers decoded into reading
metadata (`crates/aggregator-stacks/src/stacks/dlms.rs:18`). That is a *proxy* for charger state
reported by a meter — not a channel to the charger.

**Target OCPP 2.0.1, not 2.1.** [verified] 2.1 is a deliberate backwards-compatible superset: all
64 of 2.0.1's messages retained plus 27 new (91 across 19 blocks), and OCA states application logic
written for 2.0.1 continues to work in 2.1 (1.6 explicitly does not). WebSocket subprotocol
negotiation (`ocpp2.0.1` vs `ocpp2.1` in `Sec-WebSocket-Protocol`) means a 2.0.1-only CSMS never has
to parse a 2.1-only construct. 2.1 was released Jan 2025 and IEC-published Dec 2025 — roughly 18
months old, and field maturity of its new blocks is unmeasured in every source found. Building 2.0.1
first is not a dead end.

---

## Three collisions with this service's existing invariants

### A. Trust — OCPP authenticates the connection, not the reading

This is the one that needs an explicit decision, because it cannot be satisfied cleanly.

[verified] OCPP 2.0.1/2.1 define exactly three security profiles. Only **Profile 3** is mutual TLS,
binding a client certificate whose CN RDN must contain the station's unique serial number and whose
O RDN must contain the CSO name, over TLS 1.2+ (spec requirements A00.FR.402 / A00.FR.405 /
A00.FR.416). There is **no mandatory per-message or per-reading signature**. Vendor material
describing Profile 3 as "TLS with client certs *and* message signing" conflates it with the separate
optional feature below.

[verified] That optional feature is `SignedMeterValueType` inside `SampledValue` (`signedMeterData`
base64, `signingMethod`, `encodingMethod`, `publicKey`), gated on `AlignedDataCtrlr.SignReadings` /
`SampledDataCtrlr.SignReadings`, **defaulting to false**. It is produced by the calibrated meter, and
because `signingMethod`/`encodingMethod` are unconstrained free strings (field practice: OCMF,
EDL40, Alfen), a CSMS cannot generically verify it without out-of-band format knowledge.

Our invariant is the opposite shape: every reading carries an Ed25519 signature verified against a
per-device pubkey in Redis, fail-closed and loud
(`crates/aggregator-persistence/src/infra/crypto.rs:97`, `:258`).

**[design] Chosen option: bridge-side signing on behalf of the mTLS-authenticated station.** The
OCPP listener terminates Profile 3, extracts the station serial from the client cert CN, and signs
the normalized `DeviceReading` with a bridge-held key before it enters the existing pipeline.
`SignedMeterValue` is carried through as an opaque metadata attestation where the station emits it.

Be honest in the docs about what this concedes: the Ed25519 signature then attests *"the bridge
received this over an authenticated Profile 3 connection"*, **not** *"the meter produced this"*. The
trust anchor moves from the device to the transport plus the bridge. The two alternatives are worse
for us — treating mTLS as the anchor with no per-reading signature drops non-repudiation entirely
and breaks the downstream contract; making `SignedMeterValue` the anchor is the only true
meter-origin proof but it is optional, per-vendor, and unverifiable by a generic CSMS.

**ISO 15118 Plug and Charge is NOT available as a substitute anchor.** [verified] Its authorization
signature covers only a station-issued random challenge — no station identity, no timestamp — so it
is valid at any station. Relay attack demonstrated on real hardware (arXiv:2512.15966v2, TH
Ingolstadt, responsibly disclosed to CharIN); the paper states ISO 15118-20 does not close it,
because payment handling is identical across versions.

[refuted] Two adjacent alarms did **not** survive verification, and both are good news here: the
relay attack does *not* silently bypass the OCPP/CSMS authorization path, and ISO 15118 PKI station
TLS certificates are *not* unbound-and-interchangeable across stations. **mTLS station identity
survives as a genuine per-station anchor** — which is what makes the chosen option defensible at all.

### B. Identity and ordering

[verified] OCPP addresses a charge point as a three-tier **ChargingStation / EVSE / Connector**
hierarchy forming an implicit location-based addressing scheme. Our keying is flat
(`gridtokenx:devices:{meter_id}:pubkey`, zone partitioning per meter), so a composite mapping is
required either way — a synthesized composite `device_id`, or one logical device per EVSE.

[verified] Worse for the ingest loop: **the charging station generates the transaction ID**, the
chronological-ordering requirement was *lifted* in 2.0.1 in favour of a per-event `seqNo`, and
`TransactionEventRequest` carries an explicit `offline` boolean for buffered/replayed events.
Therefore:

- dedupe key = `(chargingStationId, transactionId)` — `transactionId` is unique **per station**, not
  globally
- `seqNo` (integer ≥ 0, increments per event) gives intra-transaction ordering and gap detection
- ingest must be **out-of-order tolerant**; cross-transaction chronology still rests on timestamps

There is no 2.0.1 message by which the CSMS hands a `transactionId` back to the station — the 1.6
`StartTransaction.conf` return path was removed — so CSMS-assigned IDs are not an option.

### C. Actuation overlaps the existing dispatch adapters

[verified] OCPP Smart Charging composes limits deterministically: four `ChargingProfilePurpose`s
(`ChargingStationMaxProfile`, `TxDefaultProfile`, `TxProfile`, `ChargingStationExternalConstraints`),
stacking within a purpose by `stackLevel` (higher wins; duplicate stackLevel+purpose on one EVSE
forbidden), and a Composite Schedule taking the **lowest** limit across purposes per interval —
**except that `TxProfile` always overrules `TxDefaultProfile`**. Verified verbatim against the OCA
edition-2 specification PDF. Note that widely-read secondary sources (`ocpp.md`, some vendor docs)
deny this exception and are wrong; do not design from them.

**Trap for a bridge-as-CSMS:** requirement K01.FR.22 **forbids the CSMS from setting
`ChargingStationExternalConstraints`** (it is the purpose a station uses to report a limit set by
some *other* external system; `evse.Id` must be 0). Our DR-derived limits must therefore be
expressed as `ChargingStationMaxProfile` / `TxDefaultProfile` / `TxProfile`.

[verified] The OpenADR Alliance's own documented pattern is **CSMS-as-VEN**: the OCPP central system
registers with the VTN as a single VEN and aggregates its charge points behind that one
registration, rather than registering each station. That maps directly onto our existing VEN
listener (`crates/aggregator-logic/src/standards/openleadr_ven.rs:47`) — OCPP becomes a downstream
actuation leg beside `ieee`/`grpc` in `select_adapters`
(`crates/aggregator-logic/src/dispatch/engine.rs:113`), not a competing layer.

⚠️ Material caveat: that OpenADR document is **March 2016**, scoped to OCPP 1.5/1.6 and OpenADR
2.0a/2.0b, whose service names do not exist in OpenADR 3.0's REST/OAuth model. The high-level
pattern survives; [refuted] **every detailed mapping mechanism drawn from it failed verification**
(MarketContext→SetChargingProfile matching, and report-service re-shaping of session data into
periodic interval data). We design the mapping; we do not cite it.

[verified] OCPP 2.1 adds functional block R **DER Control** (`GetDERControl` / `SetDERControl` /
`ClearDERControl` / `ReportDERControl` / `NotifyDERAlarm` / `NotifyDERStartStop`) carrying IEEE
1547-style volt-var, volt-watt, frequency-watt and power-factor curves. This genuinely overlaps the
downstream role of our IEEE 2030.5 adapter — but only *because we are the CSMS*. Out of scope for
2.0.1; noted so the eventual 2.1 migration does not accidentally build a second dispatch layer.

---

## Two convenient premises that were refuted — do not let these into the design

Both would have made the DLMS mapping easy. Both failed **0-3**.

1. [refuted] OCPP `.Register` measurands (default `Energy.Active.Import.Register`) are **not**
   established as spec-mandated monotonic non-volatile cumulative registers reported raw and never
   re-based at transaction start. They are **not** demonstrably the same model as DLMS registers, and
   session energy is **not** reliably a derived difference.
2. [refuted] The spec does **not** document 900 s aligned intervals as 96 fixed midnight-anchored
   windows delivered as standalone `MeterValuesRequest` messages.

[verified] What *is* true: clock-aligned reporting exists (`AlignedDataCtrlr.Interval` /
`.Measurands` / `.SendDuringIdle` with `triggerReason` `MeterValueClock`, plus
`SampledDataCtrlr.TxUpdatedInterval` with `MeterValuePeriodic`), and `Interval=900` is the canonical
15-minute configuration. But it is per-device configurable and conditional — best-effort, not a
guaranteed uniform stream — with three qualifications that drive the design:

- transaction-related meter values are **never** sent in `MeterValuesRequest`; they are embedded in
  `TransactionEventRequest.meterValue`, while `MeterValuesRequest` carries only idle/non-transaction
  values. **Two parse paths.**
- idle reporting depends on `AlignedDataCtrlr.Supported` / `.Enabled` / `.SendDuringIdle`, and field
  stations commonly report only during sessions — expect long gaps.
- interval and measurand set are per-device configurable.

**[design] Consequence:** normalize at the CSMS boundary. Derive an interval energy delta per
`(station, EVSE)` per bin from whatever the station actually emits — register deltas when the values
are cumulative, session accumulations when they are not — rather than assuming a wire-level
cumulative register. **This is the single largest ingest-side design risk and must be settled
empirically per charge-point model, not from the specification.**

[verified] Variable monitors (`SetVariableMonitoringRequest`, `MonitorPeriodic` /
`MonitorPeriodicClockAligned` where `monitorValue` is an interval in seconds) can approximate fixed
cadence, but the Monitoring block is **optional**, restricted by a monitor-type-vs-datatype
compatibility table, and fires `NotifyEventRequest` on the diagnostics path rather than
`MeterValues`. Fallback only.

---

## Design

A new `aggregator-api` server surface plus a new `aggregator-stacks` decoder, reusing everything
downstream unchanged.

```
Charge point ──WSS (Profile 3 mTLS, subprotocol ocpp2.0.1)──▶ OcppServer  (aggregator-api)
                                                                  │  station serial from client-cert CN
                                                                  ▼
                                                            OcppStack     (aggregator-stacks, pure)
                                                                  │  TransactionEvent / MeterValues → DeviceReading
                                                                  ▼
                                                        bridge-side Ed25519 sign
                                                                  │
                                                                  ▼
                              existing: Router::disseminate → zone Redis Streams + InfluxDB
                                        → 15-min BillingBin → settlement / mint
```

Dependency direction is preserved (`core ← protocol ← stacks ← persistence ← logic ← api`): the
OCPP **message decoding** is pure and lives in `aggregator-stacks` with no persistence dependency,
exactly like `DlmsStack`; the WebSocket server, TLS termination, cert handling and session state
live in `aggregator-api`. The station→owner mapping reuses `MeterRegistry`
(`crates/aggregator-persistence/src/infra/meter_registry.rs:205`, `:278`) with the station serial as
the lookup key.

### Identity mapping convention

```
gridtokenx:devices:{meter_id}:pubkey          # existing — Ed25519 verify key (DLMS devices)
gridtokenx:ocpp:{station_id}:evse:{evse_id}   # NEW — maps an OCPP EVSE to a logical meter_id
gridtokenx:ocpp:tx:{station_id}:{tx_id}       # NEW — dedupe/seqNo watermark, TTL'd
```

One logical device per **EVSE** (not per station, not per connector): it is the level at which
energy is metered and at which charging profiles are scoped, and it keeps the downstream `meter_id`
contract intact.

### Secure mode

`AGGREGATOR_REQUIRE_SECURE=true` (`crates/aggregator-api/src/handlers.rs:38`) must extend to this
path: it forces **Profile 3 only** — Profile 1 (HTTP Basic) and Profile 2 (server-side TLS + Basic)
are refused at the WebSocket handshake, no exceptions and no dev override, mirroring how secure mode
already neutralizes every other ingest bypass.

---

## Implementation checklist

Nothing below is started.

### 0. Prerequisite — settle the Rust story (blocking)
- [ ] Assess `rust-ocpp` / `ocpp-rs` and any other Rust OCPP crate for **2.0.1 message coverage**,
      schema fidelity, and maintenance. The research pass could **not** answer this (see Open
      questions) and it is decisive.
- [ ] Decide: Rust-native CSMS, versus proxying/binding EVerest `libocpp` (C++) or CitrineOS
      (TypeScript). A non-Rust dependency would be the first in this workspace — weigh accordingly.
- [ ] If Rust-native: confirm the crate models `TransactionEventRequest`, `MeterValuesRequest`,
      `SampledValue.signedMeterValue`, and the `SetChargingProfile` family, or budget for hand-rolling
      them from the JSON schemas.

### 1. Decoder — `OcppStack` (`aggregator-stacks`)
- [ ] Implement `ProtocolStack` (`crates/aggregator-stacks/src/stacks/mod.rs:11`) so OCPP joins DLMS
      behind the same `handle_message` contract (`:15`).
- [ ] Map `TransactionEventRequest.meterValue[].sampledValue[]` → `DeviceReading`, keyed by measurand:
      `Energy.Active.Import.Register` → `consumed_kwh`, `Energy.Active.Export.Register` →
      `generated_kwh`, `Power.Active.Import` → `ev_charging_kw` (reusing the metadata key already
      emitted by the DLMS OBIS decoder), plus SoC where present.
- [ ] Map `MeterValuesRequest` (idle/non-transaction) through the same normalizer — **separate parse
      path**, per the two-path finding above.
- [ ] Carry `signedMeterValue` verbatim into `DeviceReading.metadata` as an opaque attestation
      (`signed_meter_data`, `signing_method`, `encoding_method`) — never parsed, never trusted as
      verification. Unknown measurands pass through to metadata, mirroring the DLMS `_` fallback arm
      (`crates/aggregator-stacks/src/stacks/dlms.rs:71`).
- [ ] Cumulative-vs-session normalization: emit an **interval delta** per `(station, evse)`, with the
      mode configurable per charge-point model. Default to treating registers as cumulative and
      differencing, with an explicit per-model override — and log loud on a negative delta, which is
      the signal the model guessed wrong.
- [ ] Pure crate — **no** Redis, no persistence dependency, all state passed in.

### 2. Session/transaction state (`aggregator-persistence`)
- [ ] `OcppTxRegistry`: dedupe on `(chargingStationId, transactionId)`, track the `seqNo` high-water
      mark, detect gaps, tolerate out-of-order arrival. Self-healing Redis connection with
      rebuild-and-retry-once, mirroring `SignatureVerifier`
      (`crates/aggregator-persistence/src/infra/crypto.rs:165`).
- [ ] Honour the `offline` flag: a replayed event is accepted but tagged, so downstream can tell live
      telemetry from backfill.
- [ ] TTL transaction state so an abandoned session cannot leak the key space.

### 3. OCPP WebSocket server (`aggregator-api`)
- [ ] New listener on `OCPP_PORT` (proposed default `4011`, beside the `4010` IoT gateway), advertising
      subprotocol `ocpp2.0.1` and refusing anything else.
- [ ] TLS Profile 3: require a client certificate, extract the station serial from the **CN RDN** and
      verify the O RDN carries the expected CSO name. Reject on mismatch — this is now the identity
      anchor, so it is fail-closed.
- [ ] Under `AGGREGATOR_REQUIRE_SECURE=true`, refuse Profile 1 and Profile 2 outright.
- [ ] OCPP RPC framing: `CALL` (2) / `CALLRESULT` (3) / `CALLERROR` (4). Do **not** implement
      `CALLRESULTERROR` (5) or `SEND` (6) — they are 2.1-only and unreachable on a negotiated
      `ocpp2.0.1` connection.
- [ ] Minimum message set to be operational: `BootNotification`, `Heartbeat`, `StatusNotification`,
      `TransactionEvent`, `MeterValues`, `Authorize`, `GetVariables`/`SetVariables`.
- [ ] On boot, push our metering cadence: `SetVariables` for `AlignedDataCtrlr.Interval = 900`,
      `AlignedDataCtrlr.SendDuringIdle = true`, `SampledDataCtrlr.TxUpdatedInterval`, and the measurand
      list. Treat every one as **best-effort** — read back with `GetVariables` and log what the station
      actually accepted.

### 4. Bridge-side signing
- [ ] After decode, sign the normalized `DeviceReading` with a bridge-held Ed25519 key so the
      downstream verify path is unchanged.
- [ ] Record provenance in metadata (`sig_origin: "bridge-ocpp-mtls"`) so a reading signed *by the
      bridge on behalf of a station* is never mistaken for one signed *by a device*. Do not skip this
      — it is the difference between an honest and a misleading audit trail.
- [ ] `/metrics`: label the settlement-path gauge so OCPP-sourced energy is separable from DLMS.

### 5. Dispatch adapter — `ocpp` (`aggregator-logic`)
- [ ] New adapter beside `ieee` / `grpc` / `openleadr`, selectable via `DISPATCH_ADAPTERS`
      (`crates/aggregator-logic/src/dispatch/engine.rs:113`), sharing the existing per-`(action,
      adapter)` cooldown and partial-failure isolation.
- [ ] Translate a signed setpoint (kW) into `SetChargingProfile`. Positive/negative maps to the same
      FLEX_UP / FLEX_DOWN convention the VEN listener already uses
      (`crates/aggregator-logic/src/standards/openleadr_ven.rs:47`).
- [ ] Use `TxDefaultProfile` for standing limits and `TxProfile` for event-scoped ones. **Never**
      `ChargingStationExternalConstraints` — K01.FR.22 forbids it for a CSMS.
- [ ] Set `stackLevel` deliberately and document the scheme; a duplicate `stackLevel`+purpose on one
      EVSE is a spec violation.
- [ ] Verify the applied limit with `GetCompositeSchedule` rather than trusting the `Accepted`
      response, and conformance-test the composite rules (lowest-across-purposes, TxProfile-overrules-
      TxDefaultProfile) against EVerest/libocpp or CitrineOS rather than against station behaviour.

### 6. Billing and settlement
- [ ] Apportion a session that straddles a bin boundary using intra-session aligned samples; fall back
      to linear interpolation only when the station reports at start/stop only, and mark the bin as
      interpolated.
- [ ] **Decide the late-arrival policy** — see Open questions. The `offline` flag *guarantees* a
      replayed session will eventually land in an already-flushed, already-evicted bin
      (`src/main.rs:538`, `:543`), and the current evict-on-flush loop has no answer.
- [ ] **EV load is consumption-only by default.** Do not feed OCPP energy into
      `BillingBin::net_surplus_kwh` (`crates/aggregator-logic/src/aggregator.rs:58`) as generation
      without explicit capability negotiation — a wrong surplus here mints tokens on-chain.
- [ ] V2G export stays gated and off. [verified] OCA deferred bidirectional to 2.1 (block Q,
      `AFRRSignal` / `NotifyAllowedEnergyTransfer`, ISO 15118-20). Vendors *do* ship it on 2.0.1 by
      overloading `chargingSchedulePeriod.limit` with **negative** values, but interoperability is poor
      and stations commonly reject it. Note `Energy.Active.Export.Register` already exists in 2.0.1, so
      export *metering* predates 2.1 — what 2.1 adds is the standardized control path.

### 7. Docs
- [ ] [ARCHITECTURE.md](ARCHITECTURE.md) §3: add OCPP to the ingestion pipeline with `path:line`
      citations.
- [ ] [CLAUDE.md](CLAUDE.md) security invariants: document that OCPP readings are **bridge-signed, not
      device-signed**, and what that concedes. This is the single most important thing for a future
      reader not to get wrong.
- [ ] [PROTOCOL.md](PROTOCOL.md): note OCPP as a second wire protocol beside the UTT-S+ v4 frame.
- [ ] `.env.example`: `OCPP_PORT`, `OCPP_CSO_NAME`, cert paths, cadence defaults.
- [ ] Run `just lint-docs` from the superproject before committing.

---

## Test checklist

### Decoder (`aggregator-stacks`, inline `#[cfg(test)]`)
- [ ] `TransactionEventRequest` with `Energy.Active.Import.Register` → correct `consumed_kwh`.
- [ ] `MeterValuesRequest` (idle) decodes through the second path — proves both paths exist.
- [ ] Cumulative-mode: two successive registers → the **delta**, not the raw total.
- [ ] Session-mode: a re-based (zero-at-start) register does not double-count.
- [ ] Negative delta ⇒ logged and rejected, never a negative energy reading.
- [ ] `signedMeterValue` survives verbatim into metadata and is never interpreted.
- [ ] Unknown measurand falls through to metadata (mirrors the DLMS `_` arm).

### Transaction registry (`aggregator-persistence`, inline)
- [ ] Duplicate `(station, tx, seqNo)` ⇒ ingested once.
- [ ] Out-of-order `seqNo` ⇒ both accepted, ordering reconstructed.
- [ ] `seqNo` gap ⇒ detected and logged.
- [ ] Same `transactionId` from **two different stations** ⇒ two distinct transactions (guards the
      per-station-uniqueness trap).
- [ ] `offline=true` ⇒ accepted and tagged as backfill.
- [ ] Redis unreachable ⇒ loud `Err`, never a silent accept.

### Server / auth (`aggregator-api`, inline where possible)
- [ ] Handshake advertising a subprotocol other than `ocpp2.0.1` ⇒ refused.
- [ ] Missing client cert under Profile 3 ⇒ refused.
- [ ] Client cert whose CN lacks the station serial ⇒ refused.
- [ ] Cert O RDN mismatching the configured CSO ⇒ refused.
- [ ] `AGGREGATOR_REQUIRE_SECURE=true` + Profile 1 or 2 attempt ⇒ refused, no dev override honoured.
- [ ] Extract the policy into a pure free fn (Redis-free, `AppState`-free) so the branch matrix is unit
      testable without a harness — the `apply_dlms_key_policy` pattern
      (`crates/aggregator-api/src/grpc/service.rs:106`) is the precedent.

### Dispatch adapter (`aggregator-logic`, inline)
- [ ] Positive setpoint → `TxProfile` with the correct limit; negative → FLEX_DOWN path.
- [ ] Adapter never emits `ChargingStationExternalConstraints` (assert on the serialized purpose).
- [ ] Adapter failure is isolated — other adapters in `DISPATCH_ADAPTERS` still fire.
- [ ] Cooldown is tracked per `(action, adapter)`, not globally.

### Settlement
- [ ] Session straddling a bin boundary apportions correctly across both bins.
- [ ] OCPP consumption never contributes to `net_surplus_kwh` under default config.
- [ ] Late `offline` session landing in an evicted bin behaves per whatever policy is chosen —
      **write this test only once the policy is decided**, and make it assert the decision rather than
      the current accidental behaviour.

### Regression guards
- [ ] Existing DLMS tests unchanged and green (`cargo test -p aggregator-stacks`,
      `-p aggregator-logic`).
- [ ] `cargo check` green across all 6 crates.

---

## Open questions

1. **Rust OCPP crate maturity — blocking, and unanswered by the research pass.** No verified claim
   survived about `rust-ocpp`, `ocpp-rs`, EVerest `libocpp`, CitrineOS, SteVe or MaEVe 2.0.1/2.1
   coverage, beyond EVerest's own statement that its 2.1 support is built on its 2.0.1 implementation.
   Is a Rust-native CSMS realistic, or does this require binding to / proxying a C++ or TypeScript
   implementation? Everything in step 1 depends on the answer.
2. **Do real 2.0.1 charge points report `Energy.Active.Import.Register` as a lifetime cumulative
   register or a session-rebased value, and how many actually support *and enable*
   `AlignedDataCtrlr` with `SendDuringIdle`?** The spec permits both. The bin-derivation logic cannot
   be chosen from documentation — this needs a real charger or a conformance simulator.
3. **Late offline-replayed transactions vs. evict-on-flush.** Reopen the bin, emit a correction mint,
   or drop the energy? Each has a different on-chain consequence and the current design has no answer.
   OCPP's `offline` flag guarantees the case occurs.
4. **Does a bridge-generated Ed25519 signature have standing for billable settlement**, or does
   anything money-touching require the meter's own `SignedMeterValue` (OCMF) preserved end-to-end and
   verified by external transparency software? Bears directly on whether OCPP energy may ever mint.
5. **Is an OCPI layer needed at all** if the bridge settles on-chain rather than through eMSP
   clearing? Entirely unresearched — no OCPI source was gathered in this round.

## Research coverage gaps

Stated so nobody treats this plan as better-evidenced than it is:

- **Rust/implementation maturity: not assessed** (question 1). Biggest practical unknown.
- **OCPI: zero sources gathered.** Roaming and cross-operator settlement are unresearched.
- **Eichrecht / OCMF:** reached only indirectly via the OCA signed-meter-values whitepaper; the
  regulatory text and the OCMF format spec were not read.
- **OCPP-specific attack literature: thin.** Only the ISO 15118 PnC relay paper surfaced — an
  unpeer-reviewed arXiv preprint (with a working hardware PoC and responsible disclosure) targeting
  the EV↔EVSE link, **not** OCPP itself. The security picture here is incomplete, not clean.
- The primary OCPP 2.0.1 Part 2 specification on regulations.gov returns **HTTP 403** to automated
  fetchers; several verifications leaned on an independently downloaded OCA edition-2 mirror plus
  cross-checks against implementations. `ocpp.md` is explicitly AI-generated and was caught
  contradicting the normative text twice; `ocpp-spec.org` is a third-party rendering, not OCA-operated.
  Neither is authoritative.
