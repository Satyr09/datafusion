# Design v1: `max_row_group_bytes` in the parallel Parquet writer (synchronized barrier)

Follow-up to apache/datafusion#22649, which added the option but could only honor
it when `allow_single_file_parallelism = false`. This is the **exact / barrier**
approach. Its counterpart, the barrier-free **projection** approach, is in
`design-v2-projection.md`; the two are compared in §5 of that document.

## 1. Why the parallel path ignores the byte limit today

All relevant code lives in `datafusion/datasource-parquet/src/sink.rs`
(`ParquetSink`), in the pipeline built by `output_single_parquet_file_parallelized`:

```
RecordBatch stream
      │
      ▼
spawn_parquet_parallel_serialization_task        ← "dispatcher": decides row-group
      │  slices batches at row-group boundaries     boundaries; today checks ONLY
      │                                             writer_props.max_row_group_size()
      ├──► per-leaf-column mpsc channels
      │    (capacity = maximum_buffered_record_batches_per_stream, default 2)
      │         │
      │         ▼
      │   column_serializer_task (one per leaf column)
      │     writer.write(&array) on an ArrowColumnWriter   ← only place encoded
      │                                                      sizes are known
      ▼
spawn_rg_join_and_finalize_task → concatenate_parallel_row_groups
```

A row-group boundary must fall at the same row index in every column, so the
flush decision must be made centrally in the dispatcher. But encoded sizes are
only observable inside the detached column tasks, on the far side of the mpsc
channels. The dispatcher knows how many rows it has dispatched, never how many
bytes they encoded to, so it cannot apply a byte limit.

## 2. The semantic contract we must match

arrow-rs's `ArrowWriter` (apache/arrow-rs#9357) implements `max_row_group_bytes`
as a best-effort, once-per-batch, predictive check: before writing each batch it
divides the current row group's encoded size (`in_progress_size()`) by its row
count to get an average row size, splits the incoming batch so the projected size
stays under the limit, always accepts the first batch of a row group whole, and
flushes on whichever of the row or byte limit is reached first.

## 3. Design: synchronize, then size exactly

### 3.1 Per-column progress reporting

Each leaf-column serializer task publishes `(writes_done, estimated_bytes)` on a
`tokio::sync::watch` channel after every write, where `estimated_bytes` is
`ArrowColumnWriter::get_estimated_total_bytes()`. Channels and progress state are
recreated per row group, so they reset at each boundary. When
`max_row_group_bytes` is unset, no progress channels are created and nothing is
published — the default path is unchanged.

### 3.2 Dispatcher: barrier on every batch, then sum exact sizes

Before sizing each batch (once the row group has at least one row), the dispatcher
**waits for every column to have applied all dispatched writes**, then sums their
exact reported sizes:

```rust
// wait_for each column to catch up to writes_dispatched, then sum
for rx in &mut col_progress_rxs {
    let p = rx.wait_for(|p| p.writes_done >= writes_dispatched).await?;
    total += p.estimated_bytes;
}
```

With the exact current size in hand, the dispatcher computes how many rows of the
next batch fit (`(max_bytes - total) / avg_row_bytes`) and flushes on
`min(row-count limit, byte budget)` — the same split `ArrowWriter` performs, but
with an exact, synchronized measurement.

### 3.3 Behavior notes

- First batch of a row group is accepted whole (no average yet), matching
  `ArrowWriter`; this is also the termination guarantee for the `n == 0` flush path.
- `max_row_group_bytes` smaller than one batch → one batch per row group, matching
  the single-threaded writer.
- Boundaries are **exact and batch-identical to the single-threaded writer**, and
  fully deterministic.
- Empty input batches are skipped (they carry no rows and would otherwise spin the
  flush loop); the demux forwards empty batches to the dispatcher.

## 4. Cost

The barrier serializes the parallel writer's batch pipeline whenever a byte limit
is active: the dispatcher cannot size batch *k+1* until every column has finished
batch *k*. Inter-batch lookahead drops to ~1 batch regardless of
`maximum_buffered_record_batches_per_stream` (which is effectively neutralized
while a limit is set), and per-batch column-time variance no longer averages out
across columns (wall time per cycle → slowest column + barrier hop). Column-level
parallelism within a batch is preserved; what is lost is pipelining across batches.

## 5. Tradeoff vs the projection approach

| Property | v1: barrier | v2: projection |
|---|---|---|
| Default config (limit unset) | unchanged | unchanged |
| Boundary parity with serial writer | exact, batch-identical | approximate; exact in tiny-limit cases |
| Determinism | full | tiny-limit cases deterministic; steady-state boundaries may shift ±1 batch |
| Inter-batch pipelining (limit set) | lost (lookahead ~1 batch) | preserved |
| New synchronization | N awaits per batch | N awaits per row group |
| Overshoot vs limit | none beyond serial writer's granularity | ≤ ratio drift over the lag window |
| Test style | exact group counts | range assertions; exact for tiny limits |

v1 is the conservative, simplest, exactly-correct choice; its drawback is turning
the parallel writer into a stop-and-go pipeline whenever a byte limit is set. See
`design-v2-projection.md` for the barrier-free alternative and the full comparison.
