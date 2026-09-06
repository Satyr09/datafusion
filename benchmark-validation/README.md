# Fork-only validation

This branch runs DataFusion's existing SQL benchmark runner against the shared
base and the two corrected writers. It is not an implementation PR branch.
The original Rust workflow is replaced here with a manually dispatched comparison;
the implementation branches retain DataFusion's normal CI workflows.

The SQL cases create input batches outside timing, then time single-file Parquet
COPY operations with byte limits enabled and disabled. A standard Parquet scan
case also runs. The same VM builds all variants before timing, and two rounds
reverse execution order. Each process runs seven iterations; the summary excludes
the first. Data and binaries are temporary; logs, source hashes, timings, memory
peaks, and footer layouts are uploaded.

To reuse compiled dependencies, only sink.rs changes between benchmark builds.
Configuration code is verified identical after removing Rustdoc comments; option
descriptions stay at base. Tests of each actual branch run in its normal CI.
Footer sizes are different from encoder estimates; tracked memory is not RSS.
Hosted-runner measurements are preliminary and do not establish universal speedups.
