# `max_row_group_bytes` in the parallel Parquet writer — design docs

Reference docs for teaching the parallel Parquet writer to honor
`max_row_group_bytes` (follow-up to apache/datafusion#22649). This branch is
documentation only and is **not intended to merge**.

- `issue.md` — the GitHub feature-request issue framing the problem and the two
  candidate approaches.
- `design-v1-barrier.md` — Design v1: synchronized per-batch barrier. Exact and
  deterministic, but serializes the parallel writer's batch pipeline while a byte
  limit is set.
- `design-v2-projection.md` — Design v2: passive projection. Barrier-free and
  best-effort; preserves pipelining. Includes the full v1-vs-v2 comparison.

Implementations live on the branches:

- `daipayan/parallel-max-row-group-bytes` — v1 (barrier)
- `daipayan/parallel-max-row-group-bytes-v2` — v2 (projection)
