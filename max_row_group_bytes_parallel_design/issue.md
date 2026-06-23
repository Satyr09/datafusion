<!--
DRAFT issue for apache/datafusion (feature request template). Local only — not filed.
Title: Honor `max_row_group_bytes` in the parallel Parquet writer
Labels: enhancement   Type: Feature
-->

# Honor `max_row_group_bytes` in the parallel Parquet writer

## Is your feature request related to a problem or challenge?

#22649 added the `max_row_group_bytes` Parquet writer option, but it is only
honored by the **single-threaded** writer (`allow_single_file_parallelism = false`).
Under the default (`allow_single_file_parallelism = true`), the parallel writer in
`datafusion/datasource-parquet/src/sink.rs` decides row-group boundaries from
`max_row_group_size` (row count) only and ignores the byte limit, so for most
users the option silently has no effect.

Why it happens: a row-group boundary must fall at the same row index in every
column, so the cut is decided centrally in the dispatcher
(`spawn_parquet_parallel_serialization_task`). But encoded sizes are only known
inside the detached per-column tasks, on the far side of the column channels. The
dispatcher knows how many *rows* it dispatched, never how many *bytes* they
encoded to, so it cannot apply a byte limit.

This matters because byte-bounded row groups are the point of the option
(bounding writer memory; matching `parquet.block.size` from parquet-mr), and the
parallel writer is the default write path.

## Describe the solution you'd like

Teach the parallel dispatcher to honor `max_row_group_bytes`, flushing on
whichever of the row or byte limit is reached first, mirroring `ArrowWriter`'s
best-effort, per-batch, predictive split (first batch of a row group written
whole; subsequent batches sized from the observed average encoded row size). When
no byte limit is set, the path stays byte-for-byte as it is today.

The only real design question is *how the dispatcher learns the per-column encoded
sizes across the channels*. Two approaches are viable (both implemented locally,
both pass the same end-to-end tests):

- **(A) Synchronized estimate (barrier).** After dispatching a batch, the
  dispatcher waits for every column task to finish encoding and report its byte
  count, then sums them for the row group's exact size before sizing the next
  batch. Boundaries are exact, deterministic, and identical to the single-threaded
  writer, which makes them trivial to test. The cost: while a byte limit is set
  the dispatcher pauses at every batch, so the parallel writer effectively
  processes one batch at a time and loses most of its pipelining (the per-stream
  buffer setting stops mattering, and fast columns idle waiting for the slowest
  one each batch).

- **(B) Passive projection (no barrier).** Each column task posts its latest
  progress (writes done and estimated bytes) to a `watch` channel that only keeps
  the most recent value. The dispatcher reads these without waiting, turns each
  column's own rows-and-bytes into an average bytes-per-row, and multiplies by the
  exact number of rows it has dispatched to estimate the current size. It blocks
  only once per row group, for the first report, so small limits stay predictable;
  after that it never waits, so the writer keeps streaming at full speed. The
  cost: boundaries are approximate (a group may end a batch early or late, with a
  small overshoot), so most tests assert ranges rather than exact counts.

Either way: `parquet_max_row_group_bytes.slt` runs the row-group-count assertions
under both the single-threaded and parallel writers, a `DataFrame` test checks the
written footer via the parallel path, and the "only honored when
`allow_single_file_parallelism` is `false`" doc caveat added in #22649 is removed.

Recommendation: lean toward (B), since preserving pipelining is the whole reason
the parallel writer exists and the byte limit is explicitly best-effort; (A) is
the conservative, simpler choice if exactness/determinism is preferred for a first
cut. A benchmark (below) would settle it.

## Describe alternatives you've considered

- **Per-column independent flushing** — not possible; Parquet row groups require
  every column to cut at the same row, so the decision must stay centralized.
- **Estimate from Arrow in-memory size** (`RecordBatch::get_array_memory_size`)
  instead of encoded size — needs no cross-task communication, but is wildly
  inaccurate across encodings/compression and would not match the single-threaded
  writer's semantics.
- **(A) barrier vs (B) projection** — the two candidates above; this is the main
  decision.
- **Longer term:** extract `ArrowWriter`'s split/flush decision into a reusable
  boundary planner in arrow-rs (`ArrowRowGroupWriterFactory` is already public for
  this kind of reuse) so DataFusion stops mirroring the algorithm by hand.

## Additional context

- Follow-up to #22649.
- Both approaches are implemented and green locally; I can open a PR for whichever
  the maintainers prefer.
- I can include a benchmark comparing (A) and (B) — uniform vs skewed-width
  columns, byte limit set vs unset — to quantify the pipelining difference and
  back the choice with data.
