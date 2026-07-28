# Physics-derived zone ids — design

> Status: **design**, except §3.2.1 (the CSV/MATPOWER zone rule), which is
> **implemented** — see the note at the head of that section. G1–G5 are open.
> Scope: how a reading's `zone_code` is derived from the electrical topology
> rather than a round-robin label, and what the Aggregator Bridge must do with it.
> Last reviewed: 2026-07-28

The zone id is the partition key for the operational path: `Router::disseminate`
routes each verified reading to `gridtokenx:events:zone_<n>`
(`crates/aggregator-logic/src/router.rs:188`), and that stream is what VPP
forecasting, dispatch and per-zone analytics consume. A zone id that does not
correspond to an electrical grouping makes every per-zone aggregate meaningless —
"zone 3 is importing" says nothing if zone 3 is 8 meters picked by `i % 10`.

---

## 0. Status: most of this already exists

Topology-derived zoning **landed in the simulator on 2026-07-28** as
`2bdeaff feat(zones): derive zones from transformer topology, not groupid`. The
default GLM grid is the 80-bus rural reference feeder with four distribution
transformers `pcc_1..pcc_4`, and the loader partitions the line-only graph behind
each one into a zone (header comment,
`../gridtokenx-smartmeter-simulator/backend/src/smart_meter_simulator/data/grids/grid_bus_network.glm:3`).

Loading that topology with today's code yields four real zones:

```
zone_code counts  {2: 30, 1: 29, 3: 17, 4: 3, 0: 1}
zones  1: pcc_1  der=ref_lv_bus_4   29 buses  islandable
       2: pcc_2  der=ref_lv_bus_31  30 buses  islandable
       3: pcc_3  der=ref_lv_bus_62  17 buses  islandable
       4: pcc_4  der=(none)          3 buses  islandable
```

The live streams nevertheless carry a round-robin `zone_code` (8 meters in each of
zones 1..10). That is **not** a missing feature — it is a stale process:

| | |
|---|---|
| simulator process started | `2026-07-28T04:53:35Z` (= 11:53:35 +07) |
| zone-derivation commit `2bdeaff` | `2026-07-28T12:59:51+07` |
| `/app/src/smart_meter_simulator` | **bind mount** of the working tree |

The fleet was built 66 minutes before the code existed. A restart of
`gridtokenx-smartmeter-simulator` adopts the new derivation with no image rebuild.

**So this document is not "add physics zoning". It is: close the five gaps that
remain once the restart happens.**

---

## 1. The chain, end to end

| Step | Where | What it does |
|---|---|---|
| 1. Derive | `adapters/glm_topology_loader.py:442` `_build_zones`, codes from `:68` `_derive_zone_codes` | line-only graph behind each PCC transformer → one `ZoneSpec` |
| 2. Carry | `core/topology.py:28` `GridBus.zone_code`, `core/topology.py:149` `ZoneSpec` | numeric code on every bus; `0` = unzoned |
| 3. Fleet | `core/engine.py:102` builds `zone_code_by_node`, passed at `:114` | bus name → code, **non-zero codes only** |
| 4. Config | `meter_generator.py:153` / `:285` look up the node; `:479` fallback | meter config gets `zone_code` |
| 5. Egress map | `core/engine.py:261` `zones={…}` | meter_id → code, **truthy codes only** |
| 6. Wire | `transport/aggregator_bridge.py:1199`, serialized at `:302` | `payload["zone_code"] = str(code)` |
| 7. Route | `crates/aggregator-logic/src/router.rs:445` `calculate_zone_index` | numeric suffix → stream index; else hash |
| 8. Store | `crates/aggregator-persistence/src/infra/influxdb.rs:61` | `zone_code` InfluxDB tag |

Simulator paths are relative to
`../gridtokenx-smartmeter-simulator/backend/src/smart_meter_simulator/`.

### Independent check of the partition

Recomputing the partition straight from the MATPOWER CSVs — BFS from the slack
bus (`bus_i=1`, `type=3`), one zone per first-level branch — reproduces the GLM
loader's grouping exactly, which confirms `pcc_1..pcc_4` really are the four
feeders and not an arbitrary naming:

| zone | head bus | buses | load buses | ΣPd (pu) | max ǀZǀ (pu) | max length (km) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2 | 29 | 11 | 0.0147 | 0.0935 | 0.724 |
| 2 | 31 | 30 | 10 | 0.0035 | 0.1583 | 1.096 |
| 3 | 61 | 17 | 4 | 0.0049 | 0.1764 | 0.763 |
| 4 | 78 | 3 | 1 | 0.0028 | 0.0418 | 0.420 |

80 buses, 79 branches, 26 load buses, radial, depth 12, all 0.23 kV. Impedance
from `mpc_branch.csv` (`r`, `x`), length from `branch_extra.csv` (`Length [km]`).
The dataset's own `zone`/`area` columns are flat (`1.0` for all 80 buses) — they
carry no partition, which is why the grouping has to be derived from the graph.

---

## 2. Gaps

### G1 — the substation meter round-robins into a real zone

Slack bus `ref_lv_bus_1` is the MV busbar; it sits in front of every PCC, so it
has `zone_code = 0`. Step 3 filters non-zero codes and step 4's lookup returns
`0`, which is falsy, so the fallback at `meter_generator.py:479` assigns
`(meter_id % 10) + 1` — the substation meter lands in **zone 2**, silently
inflating it (measured: 31 meters against zone 2's 30 buses).

The fallback comment (`meter_generator.py:474`) is explicit that it exists
because "zone_code 0 was falsy and got dropped from the payload". The falsiness
is the defect; the round-robin is a workaround for it.

### G2 — the fallback range exceeds the bridge's zone bound

With `IOT_NUM_ZONES=10` (`src/main.rs:144`) valid stream indices are `0..9`, but
the fallback emits `1..10`. `calculate_zone_index` accepts a parsed suffix only
when `idx < num_zones` (`crates/aggregator-logic/src/router.rs:452`) and
otherwise hashes the *string*, so `zone_code="10"` is silently routed by hash.

Measured on the live streams before the restart: `zone_code` `1` and `10` both
landed in `zone_1` (XLEN 10002 vs 6624 for every other zone), while `zone_0` sat
empty. Two labels, one stream, no warning anywhere.

### G3 — four zones, ten streams

Post-restart the fleet emits codes `1..4`; streams `zone_0` and `zone_5..zone_9`
go permanently idle and the four zones are unbalanced (29/30/17/3 buses). This is
electrically honest — the feeder really does have four unequal branches — but it
should be a deliberate choice, not a leftover. See §3.6 for the sub-partition
option if even zones matter more than feeder fidelity.

### G4 — the physical zone never reaches Postgres

- `meters.zone_id` is `NULL` for all 82 rows in `gridtokenx_meter`; nothing
  writes it (this service is read-only on `meters`).
- `meter_readings.zone_id` exists (`migrations/0003_meter_readings.sql:75`) but
  the optional Postgres sink's INSERT column list omits it
  (`crates/aggregator-persistence/src/infra/pg_readings.rs:308`).

So the zone survives only on the Redis stream and as an InfluxDB tag. Any
SQL-side per-zone question is unanswerable.

### G5 — a second, divergent copy of the routing rule

`ZoneIngester::get_zone_index`
(`crates/aggregator-api/src/ingester/zone_ingester.rs:232`) hashes `zone_code`
unconditionally instead of parsing its numeric suffix — it would scatter a
correctly-zoned fleet across all streams. It is `#[allow(dead_code)]` today, so
nothing is broken, but it is a live trap for the next caller.

---

## 3. Design

### 3.1 Zone code contract (normative)

| Code | Meaning |
|---|---|
| `0` | substation / unzoned — in front of every PCC |
| `1..N` | physical zones, one per PCC transformer |
| — | `N < IOT_NUM_ZONES` must hold, else routing silently degrades to hashing |

Emitted as a decimal string in the DLMS payload (`zone_code`), matching
`DeviceReading.zone_code: Option<String>` (`crates/aggregator-core/src/models.rs:39`).

### 3.2 Derivation rule

Already implemented for GLM; stated here so the reference-grid loader can match:

1. Build the graph from **line edges only** (`_zone_partition:396`). Transformers
   are not lines, so each connected component is exactly one transformer's
   downstream set. **Normally-open** tie switches are cut too (a tie is
   inter-zone by construction); **closed** switches stay in as ordinary
   sectionalizing edges inside a zone. Components and members come back in bus
   load order, so the result is stable across runs.
2. Per component, collect transformers whose `lv_bus` is a member: zero ⇒
   unzoned (code `0`, the grid-edge/utility side); one ⇒ that is the PCC; more
   than one ⇒ `warn` and take the first in declaration order, since opening one
   alone will not island the group.
3. `pcc_transformer` = that transformer; every zone is islandable, because a
   zone is *defined* by having one.
4. `der_bus` = member bus with the most dispatchable capacity — PV `capacity_kw`
   **plus BESS `power_kw`**, so a battery can be the island slack in a PV-less
   zone. Ties break to the first member in load order; empty ⇒ dark on island.
5. Codes key on the **PCC transformer name**, not on any authored `groupid`
   (`_derive_zone_codes:68`): pure-integer label ⇒ itself; else its trailing
   digit run (`pcc_3` ⇒ 3); else the smallest unused positive integer. Two
   labels sharing a suffix number (`pcc_1`, `tx_1`) collapse to one code — keep
   numeric suffixes unique per transformer.

### 3.2.1 CSV / MATPOWER path (`reference-grid:`) — IMPLEMENTED

> Implemented in the simulator as `_derive_zones`
> (`adapters/reference_grid_loader.py:247`), called from
> `load_reference_grid_topology:37`. Tests: `tests/test_reference_grid_zones.py`
> (14 cases; full suite 299 passed). Two changes fell out of it, both noted
> inline below: the PCC names a real **line**, and `ZoneController` now faults
> lines as well as transformers.

Before this, `load_reference_grid_topology` set no `zone_code` and left
`GridTopology.zones` empty, so a `reference-grid:` run fell through to the
round-robin. It needs its own rule, because **the CSVs model
no transformer at all** — verified on the bundled grid: all 79 rows of
`mpc_branch.csv` have `ratio = 0` (plain lines, no tap changer), `status = 1`,
and `branch_extra.csv`'s `Branch type` is a conductor spec (`EX 3x25/50/95 Al`),
not an equipment class. Every bus is 0.23 kV. This is a pure LV feeder whose
slack bus **is** the MV/LV substation's LV terminal, so §3.2 rule 2 has nothing
to key on.

The physical analogue of "the transformer you open to island a zone" is then
**the branch leaving the substation**:

1. **Root** = the bus with `type == 3` (slack). None or several ⇒ `warn` and
   take the lowest `bus_i`, so a malformed dataset still loads.
2. **Edges** = branches with `status == 1` **and `ratio == 0`**. Out-of-service
   branches are excluded (mirrors cutting normally-open ties); `ratio != 0`
   marks a MATPOWER transformer branch and is cut as well — so on a CINELDI grid
   that *does* model transformers this rule degenerates to §3.2's, and one
   implementation covers both.
3. **Zone** = each connected component of that graph with the root removed.
4. **PCC** = the branch(es) joining the component to the root. `pcc_bus` = the
   component's head bus; `label` = `ref_pcc_<head_bus>`; `pcc_transformer` =
   **the real joining line's name** (`Line_1`, `Line_9`, …).
   `islandable = (number of joining branches == 1)`.

   > A synthetic PCC name would have been a lie: `GridManager.apply_fault`
   > returns `False` for an unknown element and `ZoneController.island()`
   > discarded that boolean, so islanding a CSV zone would have silently done
   > nothing while `islandable` advertised `True`. `island()` now tries
   > `transformer` then `line` and **raises** when neither resolves — which also
   > closes the same silent no-op on the GLM path.
5. **Codes** = `1..N` by ascending head-bus id (deterministic, no labels to
   parse). The root itself is code `0`, consistent with §3.1.
6. **`der_bus`** = `""`. The CSVs carry no PV or BESS, so these zones go dark on
   island. If a dataset ships `mpc_gen.csv`, use the largest `Pg` member instead.

**Meshed input.** Step 4's islandable test is the safety valve: if a component
hangs off the root by more than one branch, opening one will not island it, so
mark `islandable = False` and `warn` — the same posture as `_build_zones`'
multi-feeder warning rather than silently mis-modelling it.

**Verified on the bundled grid.** 80 buses, 79 in-service line branches,
connected, `edges == buses - 1` ⇒ it is a tree, so every root-incident branch is
a bridge and each zone really is islandable by opening one branch:

| code | head bus | buses | load buses | joining branches |
|---:|---:|---:|---:|---:|
| 1 | 2 | 29 | 11 | 1 |
| 2 | 31 | 30 | 10 | 1 |
| 3 | 61 | 17 | 4 | 1 |
| 4 | 78 | 3 | 1 | 1 |

This reproduces the GLM loader's partition exactly (§0), which is the strongest
available check: `grid_bus_network.glm` is a Pandapower-generated conversion of
this same grid, and whoever authored it placed `pcc_1..pcc_4` at precisely these
four points. Two independent derivations, same four zones.

**Measured after implementing** (`load_topology_spec("reference-grid:…")`):

```
zone_code counts {0: 1, 1: 29, 2: 30, 3: 17, 4: 3}
  zone 1 label=ref_pcc_2  pcc_bus=ref_lv_bus_2  pcc='Line_1'  islandable=True der=''
  zone 2 label=ref_pcc_31 pcc_bus=ref_lv_bus_31 pcc='Line_9'  islandable=True der=''
  zone 3 label=ref_pcc_61 pcc_bus=ref_lv_bus_61 pcc='Line_34' islandable=True der=''
  zone 4 label=ref_pcc_78 pcc_bus=ref_lv_bus_78 pcc='Line_54' islandable=True der=''
```

**One extra seam was required.** Stamping `GridBus.zone_code` is not enough for a
`reference-grid:` run, because that configuration builds its fleet through the
**meter registry**, not `generate_ieee_meters` — and `build_meter_configs`
(`meter_registry.py:170`) assembled `location_data` with no `zone_code`, so every
meter fell through to the round-robin regardless of what the topology derived. It
now carries `zone_code` from the pinned bus. `core/engine.py:102` (the
non-registry path) already read `bus.zone_code` and needed no change. Both paths
remain subject to the falsy-zero defect in §3.3: a zone-0 bus still round-robins.

### 3.3 Fix G1 — make zone 0 explicit, not falsy

Preferred: stop treating `0` as "absent".

- `core/engine.py:102` and `:261` — keep entries whose code is `0`
  (drop the truthiness filter; filter on "key present" instead).
- `transport/aggregator_bridge.py:302` — `if zone_code is not None:`.
- `meter_generator.py:479` — delete the `(meter_id % 10) + 1` fallback; its only
  purpose was to dodge the falsy zero.

The bridge already handles `"0"` correctly: `calculate_zone_index` parses the
suffix `0`, and `0 < num_zones`, so the substation routes to `zone_0`. That also
gives `zone_0` a real meaning instead of leaving it as the empty stream.

Alternative if a substation meter is not wanted at all: exclude the slack bus
from the fleet in step 3. Cheaper, but it drops a real measurement point (the
feeder-head totals) that per-zone loss calculations would want.

### 3.4 Fix G2 — no silent out-of-range

Whatever the fallback becomes, the bridge should stop hashing out-of-range codes
in silence. In `calculate_zone_index` (`crates/aggregator-logic/src/router.rs:452`),
when the suffix parses but `idx >= num_zones`:

- increment a counter (`aggregator_zone_code_out_of_range_total{code}`) alongside
  the existing `/metrics` gauges, and
- `warn!` once per distinct code (not per reading — this is an ingest hot path).

The hash fallback itself stays: it is the right behaviour for an unparseable or
absent code, and it is deterministic per serial.

### 3.5 Fix G5 — delete the duplicate

Remove `ZoneIngester::get_zone_index` and make `calculate_zone_index` the single
routing rule, or have the ingester delegate to it. It is dead code; deleting it
is the smaller change.

### 3.6 Optional — balanced sub-partition

If even stream load matters more than one-zone-per-feeder, the four feeder
subtrees can be recursively split: take the heaviest part, cut the edge whose
downstream load-bus count is closest to half of it, repeat until `N` parts. Zones
stay contiguous subtrees (still electrically meaningful — each is a feeder
segment), just smaller. Measured on this grid:

| N | load buses per zone | buses per zone |
|---:|---|---|
| 4 (feeders) | 11, 10, 4, 1 | 29, 30, 17, 3 |
| 6 | 5, 6, 5, 5, 4, 1 | 19, 10, 11, 19, 17, 3 |
| 10 | 3, 3, 3, 2, 3, 2, 2, 3, 4, 1 | 11, 5, 5, 8, 6, 5, 10, 9, 17, 3 |

Recommendation: **keep 4** and set `IOT_NUM_ZONES=5`. A zone is supposed to be an
islandable microgrid with a PCC and a DER slack — a sub-partition has neither, so
`ZoneController.island()` could not act on it and the extra codes would be
routing-only labels. Sub-partitioning is worth revisiting only if a single zone's
stream becomes a throughput bottleneck.

### 3.7 Persist the zone

- **Readings**: add `zone_id` to the `pg_readings` INSERT
  (`crates/aggregator-persistence/src/infra/pg_readings.rs:308`), parsed from
  `DeviceReading.zone_code`. Column already exists; nullable; no migration.
- **Meters**: `meters.zone_id` is meter-service's to write (this service is
  read-only on `meters`, per `CLAUDE.md`). Registration
  (`POST /api/v1/meters`) should accept a zone, and the fleet's bus→zone map is
  the source. Out of scope here beyond flagging the dependency — a bridge-side
  write would violate the ownership boundary.

---

## 4. Rollout

1. **Restart the simulator** — `docker restart gridtokenx-smartmeter-simulator`.
   Expect `GET :12010/api/v1/meters` to show `{1: 29, 2: 31, 3: 17, 4: 3}`
   (zone 2 inflated by one until G1 is fixed).
2. **Verify routing** — `zone_1..zone_4` grow, `zone_5..zone_9` go idle. The
   pre-restart backlog stays in the old streams; compare `XADD` rates, not `XLEN`.
3. **Land G1 + the fallback removal**, restart, confirm `{0: 1, 1: 29, 2: 30,
   3: 17, 4: 3}` and traffic on `zone_0`.
4. **Set `IOT_NUM_ZONES=5`** in the compose env for the bridge.
5. **Land G2/G5 in the bridge**, `cargo test -p aggregator-logic`.
6. **Land G4**, verify `SELECT zone_id, count(*) FROM meter_readings GROUP BY 1`.

Steps 1–2 need no code and are worth doing first — they turn the whole question
from "is it built?" into "is it correct?".

---

## 5. Open questions

1. **Does a meter belong on the slack bus at all?** §3.3 keeps it; the
   alternative removes a real feeder-head measurement.
2. **`IOT_NUM_ZONES` as a hard bound.** Today an over-range code degrades to
   hashing. Should it instead be rejected at ingest (fail-closed, consistent with
   this service's other invariants), or is silent-but-observable routing right for
   a partition key?
3. **Islanding and the wire format.** When `ZoneController.island()` opens a PCC,
   the zone's electrical identity changes but its `zone_code` does not. If
   per-zone analytics need to distinguish "zone 2, grid-connected" from "zone 2,
   islanded", that is a second field, not a second code.
