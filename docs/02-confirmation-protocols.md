# Deep dive: confirmation protocols

Detail on [§3 of the README](../README.md#3-two-stage-confirmation).

---

## The state machine

An entity's tracked state is not binary. Modelling it as `present | absent` is the root cause of most false positives in this class of system.

```
                    ┌──────────────────────────────────┐
                    │                                  │
                    ▼                                  │
              ┌──────────┐   signal    ┌──────────────────────┐
              │ PRESENT  │────────────▶│  ABSENT_UNCONFIRMED  │
              └──────────┘             └──────────────────────┘
                    ▲                       │            │
                    │                       │            │ corroboration
      signal clears │                       │ hold       │ available
      during hold   │                       │ elapsed    │
                    │                       ▼            │
                    │                  ┌─────────┐       │
                    └──────────────────│ RECHECK │       │
                       still present   └─────────┘       │
                                            │            │
                              still absent  │            │
                                            ▼            ▼
                                    ┌────────────────────────┐
                                    │   ABSENT_CONFIRMED     │
                                    │  → downstream release  │
                                    └────────────────────────┘
```

`ABSENT_UNCONFIRMED` is the important state, and it's the one naive implementations omit. It represents *"we have a hypothesis and are deliberately not acting on it yet."*

**Nothing leaves the system from `ABSENT_UNCONFIRMED`.** Not a provisional alert, not a "possible change" badge on a dashboard, not a row in a view someone might glance at. The moment unconfirmed signals become visible to an operator, they get acted on, and the hold window's entire purpose is defeated.

This is a discipline problem more than a technical one. There is always pressure to surface signals early — "just show it greyed out" — and it always erodes precision.

---

## Sizing the hold window

Derive it from measurement rather than intuition.

### Step 1 — instrument signal persistence

For every signal raised, record how long until it either resolves or is confirmed. Append-only snapshots give you this retrospectively:

```sql
WITH signal_windows AS (
    SELECT
        entity_id,
        observed_at AS signal_at,
        MIN(next_present_at) AS resolved_at
    FROM (
        SELECT s.entity_id, s.observed_at,
               MIN(p.observed_at) FILTER (WHERE p.observation = 'present')
                   AS next_present_at
        FROM   presence_snapshots s
        LEFT JOIN presence_snapshots p
               ON p.entity_id = s.entity_id
              AND p.observed_at > s.observed_at
        WHERE  s.observation = 'absent'
        GROUP  BY s.entity_id, s.observed_at
    ) x
    GROUP BY entity_id, observed_at, next_present_at
)
SELECT
    width_bucket(
        EXTRACT(epoch FROM resolved_at - signal_at)/3600, 0, 72, 24
    ) AS hour_bucket,
    count(*)
FROM signal_windows
WHERE resolved_at IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

### Step 2 — read the distribution

The distribution is typically **strongly bimodal**:

```
count
  │ ██
  │ ██
  │ ██▄
  │ ████▄▄
  │ ███████▄▄▄▄___________________________
  └────────────────────────────────────────▶ time to resolution
    ↑                                    ↑
    transients                    (real changes never resolve —
    resolve fast                   they're absent from this chart)
```

Transient failures resolve quickly and cluster near zero. Real state changes never resolve at all, so they don't appear in the resolved population.

**Set the hold above the bulk of the transient cluster.** Extending it further has sharply diminishing returns — you eliminate a shrinking tail of transients while adding latency to every real event.

### Step 3 — quantify the trade explicitly

| Hold | Transients caught | Latency added to real events |
|---|---|---|
| 1h | ~60% | 1h |
| 6h | ~85% | 6h |
| 24h | ~97% | 24h |
| 48h | ~98% | 48h |

*(Illustrative shape, not measured values — run the query above against your own data.)*

The knee is usually obvious once plotted. Going from 24h to 48h in this shape doubles latency for one additional percentage point.

### Step 4 — revisit after changes

Platform behaviour changes, your request patterns change, rate limits change. A hold tuned once and never revisited drifts out of calibration. Re-run the analysis periodically — cheap, because the data is already there.

---

## The corroboration bypass

If an independent source already confirms the change, the hold adds nothing — you have two observations, they just aren't both yours.

```python
def evaluate(signal, entity):
    corroboration = check_independent_sources(entity)

    if corroboration.confirms(signal):
        return confirm(signal, path="corroboration_bypass",
                       sources=corroboration.sources)

    return hold(signal, until=now() + HOLD_WINDOW)
```

### What counts as independent

The requirement is genuine independence of failure modes. Not independent:

- A second read of the same endpoint (same failure modes, same rate limiter)
- A different endpoint on the same platform (shared infrastructure, correlated outages)
- Two aggregators that both resell the same upstream feed

Independent:

- A structurally different platform with its own data pipeline
- A source with a different update mechanism and different operators

**Verify provenance before trusting independence.** Two sources that silently derive from the same upstream are one source, and agreement between them is worth nothing — but it *looks* like strong evidence, which makes it worse than no corroboration at all.

### Record which path was taken

`confirmation_path` on the event distinguishes `hold_elapsed` from `corroboration_bypass`. You need it to answer "how accurate is the bypass?" — if bypassed events have a higher error rate than held ones, the independence assumption is wrong and you'll want to know that before it compounds.

---

## Why time separation beats retry

Immediate retry catches only the narrowest transient class — a single dropped connection. The dominant failure modes persist across fast retries:

| Failure | Survives immediate retry? | Survives a hold? |
|---|---|---|
| Dropped connection | No | No |
| Rate limiting | **Yes** | No |
| Fingerprint-induced degradation | **Yes** | Usually not |
| Replica inconsistency | **Yes** | No |
| Platform-side incident | **Yes** | Usually not |
| Genuine state change | Yes | **Yes** |

Retrying within seconds against a rate limiter returns the same empty response, and you've now built *confidence* in a wrong answer — strictly worse than a single read, because you believe it more.

**Retry handles transport failures. Holds handle semantic failures.** They solve different problems and you want both — retry for the connection layer, holds for the interpretation layer.

---

## Blast radius caps

The cheapest catastrophic-failure protection available, and it belongs in the first version.

```python
MAX_EVENTS_ABSOLUTE  = 500
MAX_EVENTS_PROPORTION = 0.05   # 5% of tracked entities

def release(events, tracked_count):
    if len(events) > MAX_EVENTS_ABSOLUTE or \
       len(events) / tracked_count > MAX_EVENTS_PROPORTION:
        halt_pipeline(reason="event_cap_breached",
                      count=len(events))
        alert_operator()
        return                       # emit nothing
    emit(events)
```

### The reasoning

If 40% of tracked entities appear to change state simultaneously, the overwhelmingly likely explanation is that **your pipeline broke** — a layout change, an auth expiry, a rate-limit wall — not that the world changed.

Both caps are needed. Absolute catches the small-deployment case; proportional catches the large one. Either alone has a blind spot.

### Halt, don't degrade

On breach, stop and alert. Do not emit a subset, do not emit with a warning flag. A subset is arbitrary, and a warning flag gets ignored within a week.

The cost of a false halt is a delayed cycle and an operator glancing at a dashboard. The cost of not halting is thousands of wrong events reaching downstream systems, plus the unwinding — which is expensive, manual, and reputationally worse than the delay by a wide margin.

---

## Testing confirmation logic

State machines with time-dependent transitions are easy to get subtly wrong and hard to test naively.

**Inject the clock.** Never call `now()` inside the state machine. Pass time in, so tests can advance it freely without sleeping.

**Table-driven transition tests.** Enumerate `(current_state, observation, elapsed, corroboration) → expected_state` exhaustively. The state space is small; cover all of it.

**Replay real history.** Append-only snapshots are a free regression corpus. Replay a period of real observations through a modified state machine and diff the events produced. This catches behavioural changes that unit tests miss — particularly around ordering and boundaries.

**Test the boundaries explicitly.** Signal at exactly `hold_window`, corroboration arriving during the final second of a hold, entity flapping present/absent across cycle boundaries. These are where the bugs live.
