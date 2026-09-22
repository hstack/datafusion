# Skew-aware whole-file bin-packing file splitter

- Date: 2026-09-22
- Status: Approved (bounded change)
- Target file: `datafusion/datasource/src/file_groups.rs`
- Scope: one detection helper + one splitter + a builder flag + unit tests.
  Config-flag plumbing and integration testing are handled separately by the
  requester.

## Problem

`FileGroupPartitioner::repartition_evenly_by_size` balances partitions by
**bytes**. On large Delta tables with a bimodal file-size distribution (a few
hundred consolidated files of hundreds of MB, plus a few hundred tiny files of
single-digit MB), equalizing bytes forces the small files to concentrate: to
match the bytes of a partition holding one large file, another partition must
hold many small files. That partition then pays a huge per-file open cost and
becomes a long-tail straggler. The same happens when the groups are re-split
across many partitions for distributed execution.

### Evidence

Byte-balanced groups from a ~65 GB / ~1036-file table (12 partitions):
bytes were even to ~2% (~5.3-5.7 GB each) but file counts were
`46, 51, 55, 56, 57, 64, 66, 71, 73, 83, 110, 304` — median ~65, max **304**
(4.7x). The 304-file group is ~5.3 GB of *tiny* files and straggles.

Per-file open cost, measured on a 1240-file query:

- `files_opened = 1240`
- `metadata_load_time = 35.45 s`  → 35.45/1240 ≈ **28.6 ms/file**
- `time_elapsed_opening = 36.24 s` → 36.24/1240 ≈ **29 ms/file**
  (`metadata_load` is ~98% of opening → open cost is size-independent latency)
- vs `time_elapsed_processing = 3.31 s` — opens dominate ~10:1.

`1240 files x 30 ms ≈ 37 s`, matching the observed 36 s of opening. Balancing
opens across partitions is the entire win.

## Cost model

Estimated wall-clock time to read a file, in integer nanoseconds:

```
cost_ns(file) = FILE_OPEN_COST_NS + effective_size(file) * NS_PER_BYTE
```

- `FILE_OPEN_COST_NS = 30_000_000` (30 ms). Calibrated from measured
  `metadata_load_time / files_opened` (~29 ms/file). It is a fixed, per-file,
  size-independent latency (~1 effective footer roundtrip on blob storage).
- `NS_PER_BYTE = 100` (≈ 10 MB/s effective per-stream throughput). Deliberately
  conservative: each node opens 12-16 files in parallel against a shared
  object-store endpoint (~64 concurrent HTTP requests), so per-stream
  throughput is far below an uncontended link. Crossover
  (open cost == transfer cost) ≈ 300 KB. This is the least-certain constant;
  tune after real measurement.

Bin-packing on this single scalar balances *time*, so a would-be 304-tiny-file
partition is charged for all its opens and the packer spreads those files out.
A future refinement (not built now) is to add a per-column roundtrip term once
the projection is known at split time.

## Detection: `is_skewed(&self, file_groups) -> bool`

Bin-packing only helps when the table is both **large** (enough files that open
I/O dominates and small files can concentrate) and would suffer a **file-count
imbalance** under byte-balancing. Rather than infer the imbalance from a
size-distribution shape, predict it directly: the byte-range splitter gives every
partition an equal byte budget, so its most file-heavy ("fattest") partition is
the one filled with the *smallest* files. Estimate that partition's file count by
greedily accumulating the smallest files up to one budget, then compare it to the
average file count per partition. Measured over the flattened input file list:

```
num_files = count(files)
large   = num_files >= BIN_PACK_MIN_FILES_PER_PARTITION * target_partitions
budget  = total_size / target_partitions
fattest_count = number of smallest files whose sizes sum to >= budget
skewed  = total_size > 0 && large
          && fattest_count / (num_files / target_partitions)
             >= BIN_PACK_FILE_COUNT_SKEW_RATIO
```

- `BIN_PACK_MIN_FILES_PER_PARTITION = 3`
- `BIN_PACK_FILE_COUNT_SKEW_RATIO = 2.0` — "one partition would open at least
  twice the average number of files."

Needs one sort of sizes (O(n log n)); cheap for a few thousand files. Because it
predicts the outcome directly, this is **direction-agnostic**: it catches both
"a few huge files + many small" (small files a majority by count) and the more
common "mostly large files + a minority of small files" (median sits in the large
cluster, so a `mean/median` test would miss it — this was the real-world miss that
motivated the approach). Uniform tables give a ratio ~= 1 and stay on the existing
path. Few-file tables stay on the range-splitter, which can split a single giant
file — something bin-packing deliberately never does.

### Worked example (real distributed plan, DS2)

831 files, 55.2 GB, 12 partitions. `budget = 4.6 GB`; the ~221 smallest files are
~20 MB each (≈ 4.4 GB), so `fattest_count ≈ 230` vs an average of
`831 / 12 ≈ 69` → ratio ≈ **3.3 ≥ 2.0 → skewed**. This matches the observed
221-file straggler partition. A `mean/median` test fails here: mean ≈ 66 MB but
median ≈ 100 MB (73% of files are large), giving ~0.66.

## Splitter: `repartition_by_bin_packing(&self, file_groups) -> Option<Vec<FileGroup>>`

Longest-Processing-Time (LPT) greedy, whole files only, never touches ranges:

1. Flatten and clone all files.
2. Sort by `(cost_ns desc, path asc)` — deterministic; heaviest first.
3. Maintain a min-heap of `target_partitions` bins keyed `(current_load, index)`.
   For each file, pop the lightest bin, append the file, push the bin back with
   the updated load. Large files land one-per-bin first; small files then fill
   the lightest bins, so every partition ends up a mix of large + small.
4. Drop empty bins (only possible when `num_files < target_partitions`); return
   `Some(bins)`.

Returns `None` only on empty / zero-size input. Complexity O(n log k). Output is
fully deterministic given deterministic input order and index-tiebroken heap.

## Wiring / API

One new branch in `repartition_file_groups`, gated by a new builder flag that
defaults `false` so existing behavior is unchanged until explicitly enabled:

```rust
if self.preserve_order_within_groups {
    self.repartition_preserving_order(file_groups)
} else if self.bin_pack_skewed && self.is_skewed(file_groups) {
    self.repartition_by_bin_packing(file_groups)
} else {
    self.repartition_evenly_by_size(file_groups)
}
```

New field `bin_pack_skewed: bool` (default `false`) with
`with_bin_pack_skewed(bool)`. Thresholds and cost constants stay module-level
`const`s for now.

## Tests (unit, in the existing `mod test`)

- `is_skewed` fires on many mixed-size files; not on uniform-small; not on
  too-few files; not on all-empty.
- Packer reproduces the case in miniature: a few large files + a swarm of small
  ones → each bin gets >= 1 large file, small files spread, no bin gets all
  smalls; max-vs-min bin file-count and cost stay close.
- Whole-file guarantee: no output `PartitionedFile` has a `range` set.
- Determinism: same input → identical grouping across runs.
- Flag off → skewed input still takes the range-splitting path (ranges present),
  proving the feature is opt-in.

## Out of scope

- Config-flag plumbing into `FileScanConfig` / session settings.
- Per-column open-cost term.
- Integration / end-to-end tests.
