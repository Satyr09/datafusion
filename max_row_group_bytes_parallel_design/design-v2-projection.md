# Design v2: `max_row_group_bytes` in the parallel Parquet writer

Follow-up to apache/datafusion#22649, which added the option but could only honor it
when `allow_single_file_parallelism = false`. Supersedes v1 (strict per-batch
barrier); the v1 design is retained in §6 as the rejected alternative, with the
comparison rationale.

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
      │     reservation.try_resize(writer.memory_size())      sizes are known
      │
      ├──► spawn_rg_join_and_finalize_task: joins column tasks, close() each
      │     ArrowColumnWriter → Vec<ArrowColumnChunk>
      ▼
concatenate_parallel_row_groups: in order, SerializedFileWriter::next_row_group(),
append each chunk, close row group. Column writers per row group come from
ArrowRowGroupWriterFactory::create_column_writers(rg_index).
```

A row-group boundary must fall at the same row index in every column, so the
flush decision must remain centralized in the dispatcher. But encoded sizes are only
observable inside the detached column tasks, on the far side of the mpsc channels.
That information gap is the entire problem: the dispatcher slices purely on
`max_row_group_size` (rows) and never learns how many bytes the current row group
has accumulated.

## 2. The semantic contract we must match

arrow-rs's `ArrowWriter` (apache/arrow-rs#9357) implements `max_row_group_bytes` as
a best-effort, once-per-batch, predictive check:

- Before writing each batch, compute the average encoded row size of the current
  row group (`in_progress_size() / in_progress_rows`, where `in_progress_size()` is
  the sum of `ArrowColumnWriter::get_estimated_total_bytes()`).
- Split/flush the incoming batch so the projected size stays under the limit.
- The first batch of a row group is always written whole (no average exists yet),
  unless the row-count limit also applies.
- "Whichever comes first" between the row limit and the byte limit.

Two consequences anchor this design:

- The contract is a target, not a guarantee. parquet-mr's `parquet.block.size`
  is a heuristic; arrow-rs explicitly frames its implementation as best-effort.
- The boundary decision needs only an average row size — a ratio. Ratios are
  robust to slightly stale inputs, which is what makes a barrier-free parallel
  implementation sound.

## 3. Design: passive observation + projection

### 3.1 Per-column progress reporting (passive, never awaited in steady state)

Each leaf-column serializer task publishes its progress after every write. A
`tokio::sync::watch` channel fits naturally (dispatcher only ever wants the latest
value); a packed `Arc<AtomicU64>` (rows in high bits / bytes in low bits, or two
atomics) is an equally valid lighter-weight choice.

```rust
#[derive(Clone, Copy, Default)]
struct ColSerializeProgress {
    /// rows written by this column in the current row group
    rows_written: usize,
    /// ArrowColumnWriter::get_estimated_total_bytes() after the last write
    estimated_bytes: usize,
}

// inside column_serializer_task, after each writer.write(&array)?:
let _ = progress_tx.send(ColSerializeProgress {
    rows_written: rows_written_so_far,
    estimated_bytes: writer.get_estimated_total_bytes(),
});
```

Channels, writers, and progress state are all recreated per row group, so estimates
reset naturally at each boundary. When `max_row_group_bytes` is unset, the progress
channels are not created and the send is compiled out of the hot path via a simple
`Option` — the default configuration is byte-for-byte the current behavior.

### 3.2 Dispatcher: project from dispatched rows, not encoded rows

The dispatcher knows exactly how many rows it has dispatched into the current
row group (`current_rg_rows`). The column tasks lag behind by at most the channel
capacity (default 2 batches). The trick is to never compare stale bytes against the
limit directly; instead, derive the average row size from the stale snapshot —
where rows and bytes are mutually consistent, because each column reports them
together — and project it onto the exact dispatched row count:

```rust
fn projected_rg_bytes(progress: &[watch::Receiver<ColSerializeProgress>],
                      current_rg_rows: usize) -> Option<usize> {
    // Sum the latest snapshot from every column. Each column's (rows, bytes)
    // pair is internally consistent; columns may be at different batches,
    // which only perturbs the average, not its validity.
    let (rows, bytes) = progress.iter()
        .map(|rx| { let p = *rx.borrow(); (p.rows_written, p.estimated_bytes) })
        .fold((0usize, 0usize), |(r, b), (pr, pb)| (r.max(pr), b + pb));
    if rows == 0 { return None; }          // nothing encoded yet
    let avg_row_bytes = (bytes / rows).max(1);
    Some(avg_row_bytes * current_rg_rows)  // project onto EXACT dispatched rows
}
```

Staleness only matters if the compression ratio drifts within the ≤2-batch lag
window. Against a realistic limit (e.g. 128 MB ≈ hundreds of batches), that error
is noise; the projection self-corrects every batch as fresher snapshots arrive.

The main loop unifies both limits, mirroring arrow-rs's split algorithm:

```rust
let max_rows  = writer_props.max_row_group_size();        // usize::MAX if unset
let max_bytes = writer_props.max_row_group_bytes();       // usize::MAX if unset
let bytes_limited = max_bytes != usize::MAX;

while let Some(mut rb) = data.recv().await {
    loop {
        let remaining_by_rows = max_rows - current_rg_rows;

        let remaining_by_bytes = if !bytes_limited || current_rg_rows == 0 {
            usize::MAX                     // first write of a rg: accept whole
        } else {
            // §3.3: once per row group, await the first snapshot
            ensure_first_snapshot(&mut progress, first_dispatch_done).await?;
            match projected_rg_bytes(&progress, current_rg_rows) {
                None => usize::MAX,
                Some(proj) if proj >= max_bytes => 0,
                Some(proj) => {
                    let avg = (proj / current_rg_rows).max(1);
                    (max_bytes - proj) / avg
                }
            }
        };

        let n = rb.num_rows().min(remaining_by_rows).min(remaining_by_bytes);

        if n == 0 {
            finalize_and_start_next_row_group(..).await?;  // never loops forever:
            continue;                                      // fresh rg accepts whole
        }

        let slice = rb.slice(0, n);
        send_arrays_to_col_writers(&col_array_channels, &slice, ..).await?;
        current_rg_rows += n;

        if n == rb.num_rows() {
            if current_rg_rows >= max_rows {
                finalize_and_start_next_row_group(..).await?;
            }
            break;
        }
        rb = rb.slice(n, rb.num_rows() - n);
        finalize_and_start_next_row_group(..).await?;
    }
}
// stream end: drop channels; if current_rg_rows > 0, finalize leftover rg
```

The duplicated "drop channels → spawn finalize task → respawn column writers" code
(currently inlined in the `else` branch and after the loop) is extracted into
`finalize_and_start_next_row_group`, now reachable from three places.

### 3.3 The one synchronization point: first snapshot per row group

Pure projection has one race: immediately after a row group starts, no measurement
may have landed when the dispatcher sizes batch 2. With tiny limits (the SLT tests
use bytes=1), whether you get one or two batches in the first row group becomes
scheduling-dependent.

Fix: once per row group, before the first byte-limit evaluation, await the
first progress report from each column:

```rust
// runs at most once per row group; cheap: each column has at most
// one batch in flight at this point
async fn ensure_first_snapshot(progress, done: &mut bool) -> Result<()> {
    if !*done {
        for rx in progress.iter_mut() {
            rx.wait_for(|p| p.rows_written > 0).await?;
        }
        *done = true;
    }
    Ok(())
}
```

After that single touch point, the dispatcher never awaits again within the row
group. Degenerate cases become deterministic; the steady state stays barrier-free.

### 3.4 Behavior notes / edge cases

- First batch of every row group accepted whole — arrow-rs parity, and the
  termination guarantee for the `n == 0` flush path.
- `max_row_group_bytes` smaller than one batch → one batch per row group,
  deterministic via §3.3, matching the serial writer.
- Nested types: progress reporting is per leaf column, same fan-out as the
  data channels (consistent with the #8923 nested-leaf fix). The rows reduction
  uses max across leaves (all leaves of a batch carry the same row count;
  max tolerates unequal lag).
- Memory accounting unchanged: column tasks keep resizing their
  `MemoryReservation` from `writer.memory_size()`.
- Encryption path unchanged: boundaries stay centralized; writers still come
  from `ArrowRowGroupWriterFactory::create_column_writers(rg_index)`.
- Overshoot bound: projection error ≤ compression-ratio drift over the lag
  window (≤ channel capacity batches). Worst-case overshoot ≈ a couple of batches
  of encoded data — comparable to the serial writer's own once-per-batch
  granularity, and documented as such.
- `Some(0)` already rejected at config validation (#22649).

## 4. Code changes, file by file

| File | Change |
|---|---|
| `datafusion/datasource-parquet/src/sink.rs` | `ColSerializeProgress` + optional watch channels; `column_serializer_task` publishes after each write; dispatcher loop per §3.2; `ensure_first_snapshot` (§3.3); extract `finalize_and_start_next_row_group` |
| `datafusion/common/src/config.rs` | Remove the "only honored when `allow_single_file_parallelism` is `false`" limitation sentence |
| `docs/source/user-guide/configs.md` | Regenerate |
| `datafusion/sqllogictest/test_files/parquet_max_row_group_bytes.slt` | Drop the `allow_single_file_parallelism = false` workaround; run under both modes. Assertions are range-style (≥ N row groups, every group's size ≤ limit + tolerance), not exact counts — the feature promises a target, not a contract. The tiny-limit case (one batch per group) may assert exactly, since §3.3 makes it deterministic |
| `datafusion/core/src/dataframe/parquet.rs` (tests) | Byte-limit sibling of `write_parquet_with_small_rg_size` with parallelism on: read footer metadata, assert `num_row_groups > 1` and per-group `total_byte_size` within tolerance; compare group-size distribution (not exact boundaries) against a serial-writer run |
| PR description | Benchmark table (§7) + rationale for rejecting the strict barrier (§6) |

## 5. Approach comparison: v1 (strict barrier) vs v2 (projection)

### 5.1 Control-flow difference

```
V1 — STRICT PER-BATCH BARRIER                V2 — PASSIVE PROJECTION
─────────────────────────────                ───────────────────────
dispatch batch k                             dispatch batch k
      │                                            │
      ▼                                            ▼
⏸ WAIT: every column reports                 read latest snapshots (no wait)
  writes_done == k                           project: avg_row_size × dispatched_rows
      │                                            │
      ▼                                            ▼
sum exact estimated bytes                    size batch k+1, dispatch immediately
      │
      ▼                                      (await happens ONCE per row group,
size batch k+1, dispatch                      for the first snapshot only — §3.3)
```

### 5.2 Sequence diagrams

Two columns, channel capacity 2. Wk = encode batch k; column B is slower.

v1 — strict barrier. The dispatcher cannot size batch k+1 until both columns
finish batch k. Fast column A idles every cycle; pipelining is capped at one batch
in flight regardless of channel capacity:

```
Dispatcher ──d1──────────⏸──────d2──────────⏸──────d3─ ─ ─
                         ▲                  ▲
Column A   ────W1────┐   │ idle  ────W2───┐ │ idle
                     ├───┤                ├─┤
Column B   ──────W1──┘   │ ────────W2─────┘ │
                       barrier            barrier
           (wall time per cycle = slowest column + barrier hop)
```

v2 — projection. Dispatcher sends as fast as bounded channels accept; columns
drain independently; snapshots flow back passively and are read, never awaited:

```
Dispatcher ──d1──d2──d3──────d4──────d5─ ─ ─        (paced only by channel
                ░ ░░  ░ ░░   ░ ░░                    backpressure, as today)
Column A   ────W1────W2────W3────W4─ ─ ─
Column B   ──────W1──────W2──────W3─ ─ ─
                ▲ stale-but-consistent snapshots (░) read at each sizing;
                  one-time wait_for at row-group start only
```

Row-group boundary, v1 vs v2. In v1 the barrier means channels are already
drained at flush time, so the previous group's tail cannot overlap the next group's
head. In v2 (as in today's code) the previous group's columns can still be encoding
queued batches while the dispatcher fills the next group's fresh writers, and
finalization overlaps via `serialize_tx`:

```
v1:   RG_n encode ▓▓▓▓▓│ flush │ RG_{n+1} encode ▓▓▓▓▓
                       └──────┘  (dead gap: drained channels + join)

v2:   RG_n encode ▓▓▓▓▓▓▓▓╗
                  flush ──╫── RG_n finalize ▓▓▓ (overlaps via serialize_tx)
      RG_{n+1} encode    ▓╝▓▓▓▓▓▓▓
```

### 5.3 Property table

| Property | v1: strict barrier | v2: projection + first-snapshot sync |
|---|---|---|
| Default config (`max_row_group_bytes` unset) | unchanged | unchanged |
| Boundary parity with serial writer | exact, batch-identical | approximate; identical in expectation, exact in tiny-limit cases |
| Determinism of boundaries | full | tiny-limit cases deterministic (§3.3); steady-state boundaries may shift ±1 batch run-to-run |
| Inter-batch pipelining (limit set) | lost (lookahead capped at 1 batch) | fully preserved |
| Per-batch variance absorption (rotating slow column) | lost — wall time → Σ max(cols) | preserved — wall time → max(Σ cols) |
| RG-boundary tail/head overlap | lost | preserved |
| `maximum_buffered_record_batches_per_stream` tuning | neutralized while limit set | honored |
| New synchronization | N awaits per batch | N awaits per row group |
| Overshoot vs limit | none beyond serial writer's own granularity | ≤ ratio drift over lag window (≈ a couple of batches, documented) |
| Test style | exact group counts | range assertions; exact only for tiny limits |
| Code footprint | barrier + writes_done bookkeeping + wait_for per batch | snapshot read + one wait_for per group; slightly smaller |

## 6. Rejected alternative: the strict barrier (v1)

See `design-v1-barrier.md`. The strict barrier is exact and deterministic but
serializes the parallel writer's batch pipeline whenever a byte limit is set,
neutralizing `maximum_buffered_record_batches_per_stream` and the per-batch
variance absorption that the parallel writer exists to provide. v2 keeps those
properties at the cost of approximate (best-effort, as the contract already is)
boundaries.

> Implementation note (as built): `ArrowLeafColumn`/`ArrowColumnWriter` expose no
> row count, so a column task cannot report `rows_written` directly. The
> implementation keeps the column reporting `writes_done` (as today) and maps a
> column's `writes_done` to encoded rows in the dispatcher via a small
> per-row-group cumulative-rows vector. Same passive-projection semantics, with the
> default (no byte limit) data path left untouched. The per-column averages are
> also summed individually (`Σ bytes_c / rows_c`) rather than `Σbytes / max(rows)`,
> which avoids a faster column skewing the estimate.
