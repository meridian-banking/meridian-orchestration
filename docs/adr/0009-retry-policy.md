# ADR 0009: Retry policy encodes transient vs permanent failure

## Status
Accepted — 2026-07

## Context
Airflow retries any failed task the configured number of times. It has no way
to know whether a failure is worth retrying. Applying one retry count to every
task means either retrying things that can never succeed, or giving up on
things that would have worked on the second attempt.

## Decision
Two retry profiles, assigned by the NATURE of the likely failure.

**Transient** (`retries=3`, exponential backoff, max delay 30m):
infrastructure-touching tasks — data generation, ingestion, warehouse loads,
view refreshes. Their typical failure is a blipped connection, a 503, an
evicted worker. Nothing is wrong with the data or the code.

**Permanent** (`retries=0`): the data-quality gate. If the data violates a
rule, it will violate that rule on attempt four. Retrying changes nothing
except the timestamp on the failure.

## Rationale
Retrying a permanent failure is worse than not retrying, for a reason that is
easy to miss: it DELAYS THE ALERT. With `retries=3` and a 5-minute delay, a
human learns about broken data 15+ minutes later than they could have. During
an overnight batch window, that is a meaningful chunk of the runway available
to fix it before morning.

Exponential backoff on the transient profile matters for a similar
second-order reason: if a downstream service is struggling, every task retrying
every 5 minutes on the dot is precisely the traffic pattern that keeps it down.
Backoff (5, 10, 20 minutes) gives it room to recover.

Execution timeouts are part of the same policy. A hung task is worse than a
failed one — it holds a worker slot, blocks downstream work, and never fires a
failure callback. A hung task is an invisible outage.

## Consequences
+ Permanent failures alert immediately.
+ Transient failures usually self-heal without waking anyone.
+ The policy is testable, and is tested — `test_quality_gate_does_not_retry`
  fails if anyone adds retries to the gate.
- The classification is a judgement made in advance and can be wrong. A
  "transient" task failing for a permanent reason still burns three retries.
- Some failures are genuinely ambiguous (a timeout could be either). We default
  those to the transient profile, accepting some wasted retries in exchange for
  not giving up on recoverable work.
