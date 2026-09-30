# Fork-only review validation

This branch validates the changes proposed after review of PR #25041. It does
not update the branch used by the open Apache PR.

The standard SQL benchmark runner compares the original writer, the reviewed
v1 writer, and the revised writer on one Linux runner. The workloads and timing
method are unchanged from the earlier comparison. Results include source hashes,
readback checks, row-group layouts, timings and tracked memory peaks.

A second job runs the public Parquet integration tests, verifies that removing
the fixes reproduces the targeted failures, checks the two prerequisite stages,
and runs Clippy with all targets and features. Standard workspace CI runs on the
implementation branch separately.
