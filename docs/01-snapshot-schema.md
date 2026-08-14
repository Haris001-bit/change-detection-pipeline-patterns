# Deep dive: the append-only snapshot schema

Detail on [§4 of the README](../README.md#4-append-only-snapshots).

Schema is illustrative — a generalised version of the pattern, not any client's actual DDL.

---

## Three tables, two of them immutable

```sql
-- ─────────────────────────────────────────────────────────────
-- 1. ENTITIES — mutable current state. A materialised view.
--    Rebuildable from snapshots at any time.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE entities (
    entity_id           uuid PRIMARY KEY,

    -- composite identity (see README §8)
    primary_external_id text,
    secondary_external_id text,
    name_normalised     text NOT NULL,
    location_normalised text,
    contact_fingerprint text,

    tier                smallint NOT NULL DEFAULT 3,
    current_state       text NOT NULL,     -- present | absent_unconfirmed | absent_confirmed
    state_since         timestamptz,

    first_observed_at   timestamptz NOT NULL,
    last_observed_at    timestamptz NOT NULL,
    last_scanned_at     timestamptz NOT NULL,

    validity_status     text,              -- from authoritative registries
    validity_checked_at timestamptz,

    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ON entities (tier, last_scanned_at);   -- drives scan selection
CREATE INDEX ON entities (current_state) WHERE current_state <> 'present';
CREATE INDEX ON entities (name_normalised, location_normalised);  -- identity matching


-- ─────────────────────────────────────────────────────────────
-- 2. PRESENCE_SNAPSHOTS — append-only. One row per entity per scan.
--    Never updated. Never deleted (until retention rollup).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE presence_snapshots (
    snapshot_id     bigserial PRIMARY KEY,
    entity_id       uuid NOT NULL REFERENCES entities(entity_id),
    scan_run_id     uuid NOT NULL REFERENCES scan_runs(scan_run_id),

    observed_at     timestamptz NOT NULL,
    source          text NOT NULL,        -- which source produced this
    observation     text NOT NULL,        -- present | absent | error | not_applicable
    raw_response_hash text,               -- dedupe identical responses
    http_status     int,
    latency_ms      int,

    -- what the entity looked like, for change detection on attributes
    observed_attributes jsonb,

    tier_at_scan    smallint NOT NULL     -- tier when scanned, not current tier
);

CREATE INDEX ON presence_snapshots (entity_id, observed_at DESC);
CREATE INDEX ON presence_snapshots (scan_run_id);
CREATE INDEX ON presence_snapshots (observed_at);   -- partition key candidate


-- ─────────────────────────────────────────────────────────────
-- 3. STATE_CHANGE_EVENTS — append-only. Confirmed events only.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE state_change_events (
    event_id            uuid PRIMARY KEY,
    entity_id           uuid NOT NULL REFERENCES entities(entity_id),

    from_state          text NOT NULL,
    to_state            text NOT NULL,

    first_signal_at     timestamptz NOT NULL,   -- hypothesis raised
    confirmed_at        timestamptz NOT NULL,   -- promoted to fact
    hold_duration       interval GENERATED ALWAYS AS
                            (confirmed_at - first_signal_at) STORED,

    triggering_snapshot_id  bigint REFERENCES presence_snapshots(snapshot_id),
    confirming_snapshot_id  bigint REFERENCES presence_snapshots(snapshot_id),

    confirmation_path   text NOT NULL,   -- hold_elapsed | corroboration_bypass
    corroborating_sources text[],
    confidence_tier     smallint,

    -- idempotency: prevents double-emission on backfill/replay
    idempotency_key     text NOT NULL UNIQUE,

    created_at          timestamptz NOT NULL DEFAULT now()
);


-- ─────────────────────────────────────────────────────────────
-- 4. SCAN_RUNS — coverage receipts. Answers "did we actually check?"
-- ─────────────────────────────────────────────────────────────
CREATE TABLE scan_runs (
    scan_run_id     uuid PRIMARY KEY,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz,
    status          text NOT NULL,   -- running | completed | failed | halted_by_cap

    entities_targeted   int NOT NULL,
    entities_scanned    int,
    observations_absent int,
    errors              int,

    halt_reason     text
);
```

---

## Design decisions worth explaining

### `observation` is an enum, not a boolean

`present | absent | error | not_applicable`

Collapsing this to a boolean is the most common and most damaging simplification in this class of system. The four states have completely different meanings:

- `absent` — we checked, it wasn't there
- `error` — we couldn't determine anything
- `not_applicable` — this entity doesn't exist on this source and never did

Mapping `error` to `absent` manufactures false positives directly. Mapping `not_applicable` to `absent` poisons corroboration logic, because "not found on a platform it was never on" gets counted as evidence of change.

Once collapsed, the distinction is unrecoverable. This is a one-way door.

### `tier_at_scan` duplicates `entities.tier`

Deliberate denormalisation. Tier assignment changes over time; when analysing historical coverage you need the tier *as it was*, not as it is now. Otherwise "was this entity scanned at the correct frequency for its tier?" gives wrong answers for anything reclassified.

**General rule for append-only tables: denormalise any field whose current value would misrepresent history.**

### `raw_response_hash`

Two purposes. It detects identical consecutive responses cheaply (useful for spotting cached or stale upstream data), and when a platform layout change breaks extraction, hash clustering shows immediately that responses stopped varying — a strong signal that you're parsing a static error page rather than real data.

### `idempotency_key` on events

Format: `{entity_id}:{signal_type}:{detection_window}`, where `detection_window` is a truncated timestamp (e.g. the hour or day the signal was first raised).

Backfill and replay are normal operations on an append-only system. Without an idempotency key, re-processing a period emits duplicate events downstream. The unique constraint makes replay safe by construction rather than by careful operator behaviour.

### `scan_runs` as coverage receipts

The failure this prevents: a scan partially completes, half the entities are never checked, and their `last_observed_at` simply doesn't advance. Nothing errors. Downstream, unchecked entities are indistinguishable from unchanged ones.

Explicit scan receipts make coverage queryable:

```sql
-- entities overdue relative to their tier's target cadence
SELECT e.entity_id, e.tier, e.last_scanned_at,
       now() - e.last_scanned_at AS staleness
FROM   entities e
JOIN   tier_config t ON t.tier = e.tier
WHERE  now() - e.last_scanned_at > t.target_interval * 1.5
ORDER  BY staleness DESC;
```

Alert on this. It's the difference between "the pipeline is running" and "the pipeline is working."

---

## What append-only buys you

### Retrospective threshold tuning

Replay history against a candidate hold window and measure the outcome without waiting:

```sql
-- how many signals would a 12h hold have discarded as transient?
WITH signals AS (
    SELECT entity_id, observed_at AS signal_at,
           LEAD(observation) OVER w AS next_obs,
           LEAD(observed_at)  OVER w AS next_at
    FROM   presence_snapshots
    WHERE  observation = 'absent'
    WINDOW w AS (PARTITION BY entity_id ORDER BY observed_at)
)
SELECT
    count(*) FILTER (WHERE next_obs = 'present'
                       AND next_at - signal_at <= interval '12 hours')
        AS would_be_discarded,
    count(*) AS total_signals
FROM signals;
```

On a mutating schema this question is unanswerable. You would change the threshold, wait weeks, and argue about whether it helped.

### False positive rate as a measured quantity

Every signal that later resolved is a recorded false positive:

```sql
SELECT date_trunc('week', signal_at) AS week,
       count(*) FILTER (WHERE resolved) :: float / count(*) AS fp_rate
FROM   signal_outcomes
GROUP  BY 1 ORDER BY 1;
```

A precision number that moves when you change the detector is the difference between engineering and guessing.

### Attribute-level change detection, for free

Because `observed_attributes` is captured every scan, changes you weren't originally looking for are already in the data. Detecting a new class of change becomes a query over history rather than a new collection pipeline plus a wait for data to accumulate.

This has repeatedly turned "we'd need to build that and wait a month" into "here are the results, computed over the last year."

---

## Retention

Storage grows as `entities × scans_per_day × retention_days`. Manage it without losing the properties above:

**Partition `presence_snapshots` by `observed_at`** (monthly). Makes retention a partition drop rather than a mass delete, and keeps recent-data queries fast.

**Roll up rather than delete.** Beyond the hot window, compress consecutive identical observations into runs:

```
(entity, source, observation, first_at, last_at, count)
```

For stable entities — the large majority — this is a massive reduction while preserving the ability to answer "what state was this in on date X."

**Never roll up around events.** Retain full per-scan granularity for a window either side of any `state_change_event`. That's exactly the data you need when auditing why an event fired, and it's a tiny fraction of total volume.
