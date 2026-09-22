// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Logic for managing groups of [`PartitionedFile`]s in DataFusion

use crate::{FileRange, PartitionedFile};
use arrow::compute::SortOptions;
use datafusion_common::Statistics;
use datafusion_common::utils::compare_rows;
use itertools::Itertools;
use std::cmp::{Ordering, Reverse, min};
use std::collections::{BinaryHeap, HashMap};
use std::iter::repeat_with;
use std::mem;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::sync::{Arc, LazyLock};

/// Fixed, size-independent per-file open cost in nanoseconds used by the
/// skew-aware bin-packing splitter. Calibrated from measured
/// `metadata_load_time / files_opened` (~29 ms/file on blob storage): opening a
/// Parquet file costs roughly one metadata roundtrip regardless of its size.
const FILE_OPEN_COST_NS: u64 = 30_000_000;

/// Per-byte transfer + decode cost in nanoseconds used by the skew-aware
/// bin-packing splitter. `100 ns/byte` is ~10 MB/s of effective per-stream
/// throughput, deliberately conservative because many files are read
/// concurrently against a single shared object-store endpoint. This is the
/// least-certain constant and is expected to be tuned with real measurements.
const NS_PER_BYTE: u64 = 100;

/// The skew-aware splitter only engages when there are at least this many files
/// per target partition (enough that per-file open I/O dominates and small
/// files would otherwise concentrate onto a single partition).
const BIN_PACK_MIN_FILES_PER_PARTITION: usize = 3;

/// The skew-aware splitter only engages when the byte-range splitter would give
/// its most file-heavy partition at least this many times the average file
/// count. A ratio of `2.0` means one partition would open twice as many files
/// as the average, dominating query latency with per-file open overhead.
/// Uniform distributions (ratio ~= 1) are left on the byte-range splitter.
const BIN_PACK_FILE_COUNT_SKEW_RATIO: f64 = 2.0;

/// Environment variable that sets the default for whether
/// [`FileGroupPartitioner`] bin-packs skewed tables.
const BIN_PACK_SKEWED_ENV: &str = "DATAFUSION_FILE_GROUP_BIN_PACK_SKEWED";

static BIN_PACK_SKEWED_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    std::env::var(BIN_PACK_SKEWED_ENV)
        .map(|v| v == "true")
        .unwrap_or(true)
});

/// Repartition input files into `target_partitions` partitions, if total file size exceed
/// `repartition_file_min_size`
///
/// This partitions evenly by file byte range, and does not have any knowledge
/// of how data is laid out in specific files. The specific `FileOpener` are
/// responsible for the actual partitioning on specific data source type. (e.g.
/// the `CsvOpener` will read lines overlap with byte range as well as
/// handle boundaries to ensure all lines will be read exactly once)
///
/// # Example
///
/// For example, if there are two files `A` and `B` that we wish to read with 4
/// partitions (with 4 threads) they will be divided as follows:
///
/// ```text
///                                    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                                      ┌─────────────────┐
///                                    │ │                 │ │
///                                      │     File A      │
///                                    │ │  Range: 0-2MB   │ │
///                                      │                 │
///                                    │ └─────────────────┘ │
///                                     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ┌─────────────────┐                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │   Range 2-4MB   │ │
/// │                 │                  │                 │
/// │                 │                │ └─────────────────┘ │
/// │  File A (7MB)   │   ────────▶     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │  Range: 4-6MB   │ │
/// │                 │                  │                 │
/// │                 │                │ └─────────────────┘ │
/// └─────────────────┘                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ┌─────────────────┐                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │  File B (1MB)   │                  ┌─────────────────┐
/// │                 │                │ │     File A      │ │
/// └─────────────────┘                  │  Range: 6-7MB   │
///                                    │ └─────────────────┘ │
///                                      ┌─────────────────┐
///                                    │ │  File B (1MB)   │ │
///                                      │                 │
///                                    │ └─────────────────┘ │
///                                     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///
///                                    If target_partitions = 4,
///                                      divides into 4 groups
/// ```
///
/// # Maintaining Order
///
/// Within each group files are read sequentially. Thus, if the overall order of
/// tuples must be preserved, multiple files can not be mixed in the same group.
///
/// In this case, the code will split the largest files evenly into any
/// available empty groups, but the overall distribution may not be as even
/// as if the order did not need to be preserved.
///
/// ```text
///                                   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                                      ┌─────────────────┐
///                                    │ │                 │ │
///                                      │     File A      │
///                                    │ │  Range: 0-2MB   │ │
///                                      │                 │
/// ┌─────────────────┐                │ └─────────────────┘ │
/// │                 │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │   Range 2-4MB   │ │
/// │  File A (6MB)   │   ────────▶      │                 │
/// │    (ordered)    │                │ └─────────────────┘ │
/// │                 │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │  Range: 4-6MB   │ │
/// └─────────────────┘                  │                 │
/// ┌─────────────────┐                │ └─────────────────┘ │
/// │  File B (1MB)   │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │    (ordered)    │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// └─────────────────┘                  ┌─────────────────┐
///                                    │ │  File B (1MB)   │ │
///                                      │                 │
///                                    │ └─────────────────┘ │
///                                     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///
///                                    If target_partitions = 4,
///                                      divides into 4 groups
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FileGroupPartitioner {
    /// how many partitions should be created
    target_partitions: usize,
    /// the minimum size for a file to be repartitioned.
    repartition_file_min_size: usize,
    /// if the order when reading the files must be preserved
    preserve_order_within_groups: bool,
    /// Whether to bin-pack whole files (balancing estimated read time) for tables
    /// detected as "large and uneven", instead of the default byte-range splitter.
    /// Always gated by [`Self::is_skewed`], so enabling this never forces
    /// bin-packing on a uniform table. Defaults to the
    /// `DATAFUSION_FILE_GROUP_BIN_PACK_SKEWED` env var (or `true`).
    bin_pack_skewed: bool,
}

impl Default for FileGroupPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl FileGroupPartitioner {
    /// Creates a new [`FileGroupPartitioner`] with default values:
    /// 1. `target_partitions = 1`
    /// 2. `repartition_file_min_size = 10MB`
    /// 3. `preserve_order_within_groups = false`
    /// 4. `bin_pack_skewed` = `DATAFUSION_FILE_GROUP_BIN_PACK_SKEWED` env var
    ///    (default `true`)
    pub fn new() -> Self {
        Self {
            target_partitions: 1,
            repartition_file_min_size: 10 * 1024 * 1024,
            preserve_order_within_groups: false,
            bin_pack_skewed: *BIN_PACK_SKEWED_ENABLED,
        }
    }

    /// Set the target partitions
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    /// Set the minimum size at which to repartition a file
    pub fn with_repartition_file_min_size(
        mut self,
        repartition_file_min_size: usize,
    ) -> Self {
        self.repartition_file_min_size = repartition_file_min_size;
        self
    }

    /// Set whether the order of tuples within a file must be preserved
    pub fn with_preserve_order_within_groups(
        mut self,
        preserve_order_within_groups: bool,
    ) -> Self {
        self.preserve_order_within_groups = preserve_order_within_groups;
        self
    }

    /// Set whether to bin-pack whole files for tables detected as skewed. When
    /// `true`, [`Self::repartition_by_bin_packing`] is used instead of the
    /// byte-range splitter, but only for tables [`Self::is_skewed`] flags as
    /// "large and uneven" — never for uniform tables. Has no effect when order
    /// must be preserved.
    pub fn with_bin_pack_skewed(mut self, bin_pack_skewed: bool) -> Self {
        self.bin_pack_skewed = bin_pack_skewed;
        self
    }

    /// Whether these file groups should be bin-packed: only when enabled via
    /// [`Self::with_bin_pack_skewed`], the input is whole files (not already
    /// byte-range split by a prior repartitioning), *and* [`Self::is_skewed`]
    /// detects skew.
    fn should_bin_pack(&self, file_groups: &[FileGroup]) -> bool {
        self.bin_pack_skewed
            && !Self::has_range_split_files(file_groups)
            && self.is_skewed(file_groups)
    }

    /// Whether any input file is already a proper byte-range slice (its scanned
    /// range is smaller than the whole object). Bin-packing regroups whole files
    /// to cut file-open I/O; once a prior pass has split files by range those
    /// opens are already committed, so such inputs are left to the byte-range
    /// splitter. A full-file range (`0..size`) counts as a whole file.
    fn has_range_split_files(file_groups: &[FileGroup]) -> bool {
        file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .any(|f| f.effective_size() < f.object_meta.size)
    }

    /// Repartition input files according to the settings on this [`FileGroupPartitioner`].
    ///
    /// If no repartitioning is needed or possible, return `None`.
    pub fn repartition_file_groups(
        &self,
        file_groups: &[FileGroup],
    ) -> Option<Vec<FileGroup>> {
        if file_groups.is_empty() {
            return None;
        }

        //  special case when order must be preserved
        if self.preserve_order_within_groups {
            self.repartition_preserving_order(file_groups)
        } else if self.should_bin_pack(file_groups) {
            self.repartition_by_bin_packing(file_groups)
        } else {
            self.repartition_evenly_by_size(file_groups)
        }
    }

    /// Evenly repartition files across partitions by size, ignoring any
    /// existing grouping / ordering
    fn repartition_evenly_by_size(
        &self,
        file_groups: &[FileGroup],
    ) -> Option<Vec<FileGroup>> {
        let target_partitions = self.target_partitions;
        let repartition_file_min_size = self.repartition_file_min_size;
        let flattened_files = file_groups.iter().flat_map(FileGroup::iter).collect_vec();

        let total_size = flattened_files
            .iter()
            .map(|f| f.effective_size())
            .sum::<u64>();
        if (total_size < (repartition_file_min_size as u64)
            && target_partitions >= file_groups.len())
            || total_size == 0
        {
            return None;
        }

        let target_partition_size = total_size.div_ceil(target_partitions as u64);

        let current_partition_index: usize = 0;
        let current_partition_size: u64 = 0;

        // Partition byte range evenly for all `PartitionedFile`s
        let repartitioned_files = flattened_files
            .into_iter()
            .scan(
                (current_partition_index, current_partition_size),
                |(current_partition_index, current_partition_size), source_file| {
                    if source_file.object_meta.size > 0
                        && source_file.object_meta.size
                            < (repartition_file_min_size as u64)
                    {
                        *current_partition_size += source_file.object_meta.size;
                        if *current_partition_size > target_partition_size {
                            *current_partition_index += 1;
                            *current_partition_size = 0;
                        }
                        let small_file = (*current_partition_index, source_file.clone());
                        return Some(vec![small_file]);
                    }
                    let mut produced_files = vec![];
                    let (mut range_start, file_end) = source_file.range();
                    while range_start < file_end {
                        let range_end = min(
                            range_start
                                + (target_partition_size - *current_partition_size),
                            file_end,
                        );

                        let mut produced_file = source_file.clone();
                        produced_file.range = Some(FileRange {
                            start: range_start as i64,
                            end: range_end as i64,
                        });
                        produced_files.push((*current_partition_index, produced_file));

                        if *current_partition_size + (range_end - range_start)
                            >= target_partition_size
                        {
                            *current_partition_index += 1;
                            *current_partition_size = 0;
                        } else {
                            *current_partition_size += range_end - range_start;
                        }
                        range_start = range_end;
                    }
                    Some(produced_files)
                },
            )
            .flatten()
            .chunk_by(|(partition_idx, _)| *partition_idx)
            .into_iter()
            .map(|(_, group)| FileGroup::new(group.map(|(_, vals)| vals).collect_vec()))
            .collect_vec();

        Some(repartitioned_files)
    }

    /// Redistribute file groups across size preserving order
    fn repartition_preserving_order(
        &self,
        file_groups: &[FileGroup],
    ) -> Option<Vec<FileGroup>> {
        // Can't repartition and preserve order if there are more groups
        // than partitions
        if file_groups.len() >= self.target_partitions {
            return None;
        }
        let num_new_groups = self.target_partitions - file_groups.len();

        // If there is only a single file
        if file_groups.len() == 1 && file_groups[0].len() == 1 {
            return self.repartition_evenly_by_size(file_groups);
        }

        // Find which files could be split (single file groups)
        let mut heap: BinaryHeap<_> = file_groups
            .iter()
            .enumerate()
            .filter_map(|(group_index, group)| {
                // ignore groups that do not have exactly 1 file
                if group.len() == 1 {
                    Some(ToRepartition {
                        source_index: group_index,
                        file_size: group[0].effective_size(),
                        new_groups: vec![group_index],
                    })
                } else {
                    None
                }
            })
            .map(CompareByRangeSize)
            .collect();

        // No files can be redistributed
        if heap.is_empty() {
            return None;
        }

        // Add new empty groups to which we will redistribute ranges of existing files
        // Add new empty groups to which we will redistribute ranges of existing files
        let mut file_groups: Vec<_> = file_groups
            .iter()
            .cloned()
            .chain(repeat_with(|| FileGroup::new(Vec::new())).take(num_new_groups))
            .collect();

        // Divide up empty groups
        for (group_index, group) in file_groups.iter().enumerate() {
            if !group.is_empty() {
                continue;
            }
            // Pick the file that has the largest ranges to read so far
            let mut largest_group = heap.pop().unwrap();
            largest_group.new_groups.push(group_index);
            heap.push(largest_group);
        }

        // Distribute files to their newly assigned groups
        while let Some(to_repartition) = heap.pop() {
            let range_size = to_repartition.range_size();
            let ToRepartition {
                source_index,
                file_size: _,
                new_groups,
            } = to_repartition.into_inner();
            assert_eq!(file_groups[source_index].len(), 1);
            let original_file = file_groups[source_index].pop().unwrap();

            let last_group = new_groups.len() - 1;
            let (mut range_start, file_end) = original_file.range();
            let mut range_end = range_start + range_size;
            for (i, group_index) in new_groups.into_iter().enumerate() {
                let target_group = &mut file_groups[group_index];
                assert!(target_group.is_empty());

                // adjust last range to include the entire file
                if i == last_group {
                    range_end = file_end;
                }
                target_group.push(
                    original_file
                        .clone()
                        .with_range(range_start as i64, range_end as i64),
                );
                range_start = range_end;
                range_end += range_size;
            }
        }

        Some(file_groups)
    }

    /// Estimated wall-clock time in nanoseconds to open and read `file`,
    /// modeling blob-storage access as a fixed per-file open latency plus a
    /// per-byte transfer/decode cost. Used to bin-pack files by estimated time.
    fn file_cost_ns(file: &PartitionedFile) -> u64 {
        FILE_OPEN_COST_NS + file.effective_size().saturating_mul(NS_PER_BYTE)
    }

    /// Detect tables that are "large and uneven" enough that the default
    /// byte-range splitter would concentrate many small files onto a single
    /// partition, paying a large per-file open cost and creating a straggler.
    ///
    /// A table qualifies when it has both:
    /// - *many* files relative to the target partitions
    ///   ([`BIN_PACK_MIN_FILES_PER_PARTITION`] per partition), and
    /// - a predicted *file-count imbalance* of at least
    ///   [`BIN_PACK_FILE_COUNT_SKEW_RATIO`] (see below).
    ///
    /// The imbalance is predicted directly rather than inferred from a
    /// size-distribution shape: the byte-range splitter gives every partition an
    /// equal byte budget, so its most file-heavy partition is the one filled with
    /// the *smallest* files. We estimate that partition's file count by greedily
    /// accumulating the smallest files up to one budget, then compare it to the
    /// average file count per partition. This catches skew regardless of whether
    /// the small files are a minority or majority by count.
    ///
    /// Uniform distributions (ratio ~= 1) are intentionally left on the
    /// byte-range splitter, which already yields even file counts for them and
    /// can split a single oversized file (something bin-packing never does).
    fn is_skewed(&self, file_groups: &[FileGroup]) -> bool {
        let target_partitions = self.target_partitions;
        if target_partitions == 0 {
            return false;
        }

        let mut sizes: Vec<u64> = file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .map(|f| f.effective_size())
            .collect();

        let num_files = sizes.len();
        if num_files < BIN_PACK_MIN_FILES_PER_PARTITION * target_partitions {
            return false;
        }

        let total_size: u64 = sizes.iter().sum();
        if total_size == 0 {
            return false;
        }

        // Estimate the file count of the fattest byte-balanced partition by
        // filling one equal byte budget with the smallest files.
        let budget = total_size / target_partitions as u64;
        sizes.sort_unstable();
        let mut accumulated = 0u64;
        let mut fattest_count = 0usize;
        for &size in &sizes {
            accumulated += size;
            fattest_count += 1;
            if accumulated >= budget {
                break;
            }
        }

        let mean_count = num_files as f64 / target_partitions as f64;
        fattest_count as f64 / mean_count >= BIN_PACK_FILE_COUNT_SKEW_RATIO
    }

    /// Repartition by bin-packing whole files into `target_partitions` groups,
    /// balancing each group's estimated read time ([`Self::file_cost_ns`]).
    ///
    /// Unlike [`Self::repartition_evenly_by_size`], files are never split at byte
    /// boundaries. Using a Longest-Processing-Time greedy assignment, large files
    /// are placed first (one per group) and small files then fill the lightest
    /// groups, so every partition ends up with a mix of large and small files and
    /// a balanced number of file opens.
    ///
    /// Returns `None` only for empty input. Empty groups (possible only when
    /// there are fewer files than `target_partitions`) are dropped.
    fn repartition_by_bin_packing(
        &self,
        file_groups: &[FileGroup],
    ) -> Option<Vec<FileGroup>> {
        let target_partitions = self.target_partitions;
        if target_partitions == 0 {
            return None;
        }

        let mut files: Vec<PartitionedFile> = file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .cloned()
            .collect();
        if files.is_empty() {
            return None;
        }

        // Heaviest files first; break ties by path for deterministic output.
        files.sort_by(|a, b| {
            Self::file_cost_ns(b)
                .cmp(&Self::file_cost_ns(a))
                .then_with(|| a.path().cmp(b.path()))
        });

        // Min-heap of (current_load, group_index): always extend the lightest
        // group. The index in the key keeps ties deterministic.
        let mut bins: Vec<FileGroup> = (0..target_partitions)
            .map(|_| FileGroup::default())
            .collect();
        let mut heap: BinaryHeap<Reverse<(u64, usize)>> =
            (0..target_partitions).map(|i| Reverse((0, i))).collect();

        for file in files {
            let cost = Self::file_cost_ns(&file);
            let Reverse((load, index)) = heap.pop().expect("heap is non-empty");
            bins[index].push(file);
            heap.push(Reverse((load + cost, index)));
        }

        // Drop empty groups (possible only when files < target_partitions).
        Some(bins.into_iter().filter(|g| !g.is_empty()).collect())
    }
}

/// Represents a group of partitioned files that'll be processed by a single thread.
/// Maintains optional statistics across all files in the group.
///
/// # Statistics
///
/// The group-level [`FileGroup::file_statistics`] field contains merged statistics from all files
/// in the group for the **full table schema** (file columns + partition columns).
///
/// Partition column statistics are derived from the individual file partition values:
/// - `min` = minimum partition value across all files in the group
/// - `max` = maximum partition value across all files in the group
/// - `null_count` = 0 (partition values are never null)
///
/// This allows query optimizers to prune entire file groups based on partition bounds.
#[derive(Debug, Clone)]
pub struct FileGroup {
    /// The files in this group
    files: Vec<PartitionedFile>,
    /// Optional statistics for the data across all files in the group.
    ///
    /// These statistics cover the full table schema: file columns plus partition columns.
    /// Partition column statistics are merged from individual [`PartitionedFile::statistics`],
    /// which compute exact values from [`PartitionedFile::partition_values`].
    statistics: Option<Arc<Statistics>>,
}

impl FileGroup {
    /// Creates a new FileGroup from a vector of PartitionedFile objects
    pub fn new(files: Vec<PartitionedFile>) -> Self {
        Self {
            files,
            statistics: None,
        }
    }

    /// Returns the number of files in this group
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Set the statistics for this group
    pub fn with_statistics(mut self, statistics: Arc<Statistics>) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Returns a slice of the files in this group
    pub fn files(&self) -> &[PartitionedFile] {
        &self.files
    }

    pub fn iter(&self) -> impl Iterator<Item = &PartitionedFile> {
        self.files.iter()
    }

    pub fn into_inner(self) -> Vec<PartitionedFile> {
        self.files
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Removes the last element from the files vector and returns it, or None if empty
    pub fn pop(&mut self) -> Option<PartitionedFile> {
        self.files.pop()
    }

    /// Adds a file to the group
    pub fn push(&mut self, partitioned_file: PartitionedFile) {
        self.files.push(partitioned_file);
    }

    /// Get the specific file statistics for the given index
    /// If the index is None, return the `FileGroup` statistics
    pub fn file_statistics(&self, index: Option<usize>) -> Option<&Statistics> {
        if let Some(index) = index {
            self.files.get(index).and_then(|f| f.statistics.as_deref())
        } else {
            self.statistics.as_deref()
        }
    }

    /// Get the mutable reference to the statistics for this group
    pub fn statistics_mut(&mut self) -> Option<&mut Statistics> {
        self.statistics.as_mut().map(Arc::make_mut)
    }

    /// Partition the list of files into `n` groups
    pub fn split_files(mut self, n: usize) -> Vec<FileGroup> {
        if self.is_empty() {
            return vec![];
        }

        // ObjectStore::list does not guarantee any consistent order and for some
        // implementations such as LocalFileSystem, it may be inconsistent. Thus
        // Sort files by path to ensure consistent plans when run more than once.
        self.files.sort_by(|a, b| a.path().cmp(b.path()));

        // effectively this is div with rounding up instead of truncating
        let chunk_size = self.len().div_ceil(n);
        let mut chunks = Vec::with_capacity(n);
        let mut current_chunk = Vec::with_capacity(chunk_size);
        for file in self.files.drain(..) {
            current_chunk.push(file);
            if current_chunk.len() == chunk_size {
                let full_chunk = FileGroup::new(mem::replace(
                    &mut current_chunk,
                    Vec::with_capacity(chunk_size),
                ));
                chunks.push(full_chunk);
            }
        }

        if !current_chunk.is_empty() {
            chunks.push(FileGroup::new(current_chunk));
        }

        chunks
    }

    /// Groups files by their partition values, ensuring all files with same
    /// partition values are in the same group.
    ///
    /// Note: May return fewer groups than `max_target_partitions` when the
    /// number of unique partition values is less than the target.
    #[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
    pub fn group_by_partition_values(
        self,
        max_target_partitions: usize,
    ) -> Vec<FileGroup> {
        if self.is_empty() || max_target_partitions == 0 {
            return vec![];
        }

        let mut partition_groups: HashMap<
            Vec<datafusion_common::ScalarValue>,
            Vec<PartitionedFile>,
        > = HashMap::new();

        for file in self.files {
            partition_groups
                .entry(file.partition_values.clone())
                .or_default()
                .push(file);
        }

        let num_unique_partitions = partition_groups.len();

        // Sort for deterministic bucket assignment across query executions.
        let mut sorted_partitions: Vec<_> = partition_groups.into_iter().collect();
        let sort_options =
            vec![
                SortOptions::default();
                sorted_partitions.first().map(|(k, _)| k.len()).unwrap_or(0)
            ];
        sorted_partitions.sort_by(|a, b| {
            compare_rows(&a.0, &b.0, &sort_options).unwrap_or(Ordering::Equal)
        });

        if num_unique_partitions <= max_target_partitions {
            sorted_partitions
                .into_iter()
                .map(|(_, files)| FileGroup::new(files))
                .collect()
        } else {
            // Merge into max_target_partitions buckets using round-robin.
            // This maintains grouping by partition value as we are merging groups which already
            // contain all values for a partition key.
            let mut target_groups = vec![vec![]; max_target_partitions];

            for (idx, (_, files)) in sorted_partitions.into_iter().enumerate() {
                let bucket = idx % max_target_partitions;
                target_groups[bucket].extend(files);
            }

            target_groups.into_iter().map(FileGroup::new).collect()
        }
    }
}

impl Index<usize> for FileGroup {
    type Output = PartitionedFile;

    fn index(&self, index: usize) -> &Self::Output {
        &self.files[index]
    }
}

impl IndexMut<usize> for FileGroup {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.files[index]
    }
}

impl FromIterator<PartitionedFile> for FileGroup {
    fn from_iter<I: IntoIterator<Item = PartitionedFile>>(iter: I) -> Self {
        let files = iter.into_iter().collect();
        FileGroup::new(files)
    }
}

impl From<Vec<PartitionedFile>> for FileGroup {
    fn from(files: Vec<PartitionedFile>) -> Self {
        FileGroup::new(files)
    }
}

impl Default for FileGroup {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Tracks how a individual file will be repartitioned
#[derive(Debug, Clone)]
struct ToRepartition {
    /// the index from which the original file will be taken
    source_index: usize,
    /// the size of the original file
    file_size: u64,
    /// indexes of which group(s) will this be distributed to (including `source_index`)
    new_groups: Vec<usize>,
}

impl ToRepartition {
    /// How big will each file range be when this file is read in its new groups?
    fn range_size(&self) -> u64 {
        self.file_size / (self.new_groups.len() as u64)
    }
}

struct CompareByRangeSize(ToRepartition);
impl CompareByRangeSize {
    fn into_inner(self) -> ToRepartition {
        self.0
    }
}
impl Ord for CompareByRangeSize {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.range_size().cmp(&other.0.range_size())
    }
}
impl PartialOrd for CompareByRangeSize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for CompareByRangeSize {
    fn eq(&self, other: &Self) -> bool {
        // PartialEq must be consistent with PartialOrd
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for CompareByRangeSize {}
impl Deref for CompareByRangeSize {
    type Target = ToRepartition;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for CompareByRangeSize {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion_common::ScalarValue;

    /// Empty file won't get partitioned
    #[test]
    fn repartition_empty_file_only() {
        let partitioned_file_empty = pfile("empty", 0);
        let file_group = vec![FileGroup::new(vec![partitioned_file_empty])];

        let partitioned_files = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(0)
            .repartition_file_groups(&file_group);

        assert_partitioned_files(None, partitioned_files);
    }

    /// Repartition when there is a empty file in file groups
    #[test]
    fn repartition_empty_files() {
        let pfile_a = pfile("a", 10);
        let pfile_b = pfile("b", 10);
        let pfile_empty = pfile("empty", 0);

        let empty_first = vec![
            FileGroup::new(vec![pfile_empty.clone()]),
            FileGroup::new(vec![pfile_a.clone()]),
            FileGroup::new(vec![pfile_b.clone()]),
        ];
        let empty_middle = vec![
            FileGroup::new(vec![pfile_a.clone()]),
            FileGroup::new(vec![pfile_empty.clone()]),
            FileGroup::new(vec![pfile_b.clone()]),
        ];
        let empty_last = vec![
            FileGroup::new(vec![pfile_a]),
            FileGroup::new(vec![pfile_b]),
            FileGroup::new(vec![pfile_empty]),
        ];

        // Repartition file groups into x partitions
        let expected_2 = vec![
            FileGroup::new(vec![pfile("a", 10).with_range(0, 10)]),
            FileGroup::new(vec![pfile("b", 10).with_range(0, 10)]),
        ];
        let expected_3 = vec![
            FileGroup::new(vec![pfile("a", 10).with_range(0, 7)]),
            FileGroup::new(vec![
                pfile("a", 10).with_range(7, 10),
                pfile("b", 10).with_range(0, 4),
            ]),
            FileGroup::new(vec![pfile("b", 10).with_range(4, 10)]),
        ];

        let file_groups_tests = [empty_first, empty_middle, empty_last];

        for fg in file_groups_tests {
            let all_expected = [(2, expected_2.clone()), (3, expected_3.clone())];
            for (n_partition, expected) in all_expected {
                let actual = FileGroupPartitioner::new()
                    .with_target_partitions(n_partition)
                    .with_repartition_file_min_size(10)
                    .repartition_file_groups(&fg);

                assert_partitioned_files(Some(expected), actual);
            }
        }
    }

    #[test]
    fn repartition_single_file() {
        // Single file, single partition into multiple partitions
        let single_partition = vec![FileGroup::new(vec![pfile("a", 123)])];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 123).with_range(0, 31)]),
            FileGroup::new(vec![pfile("a", 123).with_range(31, 62)]),
            FileGroup::new(vec![pfile("a", 123).with_range(62, 93)]),
            FileGroup::new(vec![pfile("a", 123).with_range(93, 123)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_single_file_with_range() {
        // Single file, single partition into multiple partitions
        let single_partition =
            vec![FileGroup::new(vec![pfile("a", 123).with_range(0, 123)])];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 123).with_range(0, 31)]),
            FileGroup::new(vec![pfile("a", 123).with_range(31, 62)]),
            FileGroup::new(vec![pfile("a", 123).with_range(62, 93)]),
            FileGroup::new(vec![pfile("a", 123).with_range(93, 123)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_single_file_with_incomplete_range() {
        // Single file, single partition into multiple partitions
        let single_partition =
            vec![FileGroup::new(vec![pfile("a", 123).with_range(10, 100)])];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 123).with_range(10, 33)]),
            FileGroup::new(vec![pfile("a", 123).with_range(33, 56)]),
            FileGroup::new(vec![pfile("a", 123).with_range(56, 79)]),
            FileGroup::new(vec![pfile("a", 123).with_range(79, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_single_file_duplicated_with_range() {
        // Single file, two partitions into multiple partitions
        let single_partition = vec![FileGroup::new(vec![
            pfile("a", 100).with_range(0, 50),
            pfile("a", 100).with_range(50, 100),
        ])];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 100).with_range(0, 25)]),
            FileGroup::new(vec![pfile("a", 100).with_range(25, 50)]),
            FileGroup::new(vec![pfile("a", 100).with_range(50, 75)]),
            FileGroup::new(vec![pfile("a", 100).with_range(75, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_too_much_partitions() {
        // Single file, single partition into 96 partitions
        let partitioned_file = pfile("a", 8);
        let single_partition = vec![FileGroup::new(vec![partitioned_file])];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(96)
            .with_repartition_file_min_size(5)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 8).with_range(0, 1)]),
            FileGroup::new(vec![pfile("a", 8).with_range(1, 2)]),
            FileGroup::new(vec![pfile("a", 8).with_range(2, 3)]),
            FileGroup::new(vec![pfile("a", 8).with_range(3, 4)]),
            FileGroup::new(vec![pfile("a", 8).with_range(4, 5)]),
            FileGroup::new(vec![pfile("a", 8).with_range(5, 6)]),
            FileGroup::new(vec![pfile("a", 8).with_range(6, 7)]),
            FileGroup::new(vec![pfile("a", 8).with_range(7, 8)]),
        ]);

        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_multiple_partitions() {
        // Multiple files in single partition after redistribution
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 40)]),
            FileGroup::new(vec![pfile("b", 60)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 40).with_range(0, 34)]),
            FileGroup::new(vec![
                pfile("a", 40).with_range(34, 40),
                pfile("b", 60).with_range(0, 28),
            ]),
            FileGroup::new(vec![pfile("b", 60).with_range(28, 60)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_same_num_partitions() {
        // "Rebalance" files across partitions
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 40)]),
            FileGroup::new(vec![pfile("b", 60)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            FileGroup::new(vec![
                pfile("a", 40).with_range(0, 40),
                pfile("b", 60).with_range(0, 10),
            ]),
            FileGroup::new(vec![pfile("b", 60).with_range(10, 60)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_no_action_min_size() {
        // No action due to target_partition_size
        let single_partition = vec![FileGroup::new(vec![pfile("a", 123)])];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(500)
            .repartition_file_groups(&single_partition);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_no_action_zero_files() {
        // No action due to no files
        let empty_partition = vec![];

        let partitioner = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(500);

        assert_partitioned_files(None, repartition_test(partitioner, empty_partition))
    }

    #[test]
    fn repartition_ordered_no_action_too_few_partitions() {
        // No action as there are no new groups to redistribute to
        let input_partitions = vec![
            FileGroup::new(vec![pfile("a", 100)]),
            FileGroup::new(vec![pfile("b", 200)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(2)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&input_partitions);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_ordered_no_action_file_too_small() {
        // No action as there are no new groups to redistribute to
        let single_partition = vec![FileGroup::new(vec![pfile("a", 100)])];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(2)
            // file is too small to repartition
            .with_repartition_file_min_size(1000)
            .repartition_file_groups(&single_partition);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_ordered_one_large_file() {
        // "Rebalance" the single large file across partitions
        let source_partitions = vec![FileGroup::new(vec![pfile("a", 100)])];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 100).with_range(0, 34)]),
            FileGroup::new(vec![pfile("a", 100).with_range(34, 68)]),
            FileGroup::new(vec![pfile("a", 100).with_range(68, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_file_with_range() {
        // "Rebalance" the single large file across partitions
        let source_partitions =
            vec![FileGroup::new(vec![pfile("a", 100).with_range(0, 100)])];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 100).with_range(0, 34)]),
            FileGroup::new(vec![pfile("a", 100).with_range(34, 68)]),
            FileGroup::new(vec![pfile("a", 100).with_range(68, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_file() {
        // "Rebalance" the single large file across empty partitions, but can't split
        // small file
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100)]),
            FileGroup::new(vec![pfile("b", 30)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(0, 33)]),
            // only b in this group (can't do this)
            FileGroup::new(vec![pfile("b", 30).with_range(0, 30)]),
            // second third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(33, 66)]),
            // final third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(66, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_file_with_full_range() {
        // "Rebalance" the single large file across empty partitions, but can't split
        // small file
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100).with_range(0, 100)]),
            FileGroup::new(vec![pfile("b", 30)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(0, 33)]),
            // only b in this group (can't do this)
            FileGroup::new(vec![pfile("b", 30).with_range(0, 30)]),
            // second third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(33, 66)]),
            // final third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(66, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_file_with_split_range() {
        // "Rebalance" the single large file across empty partitions, but can't split
        // small file
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100).with_range(0, 50)]),
            FileGroup::new(vec![pfile("a", 100).with_range(50, 100)]),
            FileGroup::new(vec![pfile("b", 30)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of first "a"
            FileGroup::new(vec![pfile("a", 100).with_range(0, 25)]),
            // second "a" fully (not split)
            FileGroup::new(vec![pfile("a", 100).with_range(50, 100)]),
            // only b in this group (can't do this)
            FileGroup::new(vec![pfile("b", 30).with_range(0, 30)]),
            // second half of first "a"
            FileGroup::new(vec![pfile("a", 100).with_range(25, 50)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_file_with_non_full_range() {
        // "Rebalance" the single large file across empty partitions, but can't split
        // small file
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100).with_range(20, 80)]),
            FileGroup::new(vec![pfile("b", 30).with_range(5, 25)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(20, 40)]),
            // only b in this group (can't split this)
            FileGroup::new(vec![pfile("b", 30).with_range(5, 25)]),
            // second third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(40, 60)]),
            // final third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(60, 80)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_two_large_files() {
        // "Rebalance" two large files across empty partitions, but can't mix them
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100)]),
            FileGroup::new(vec![pfile("b", 100)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(0, 50)]),
            // scan first half of "b"
            FileGroup::new(vec![pfile("b", 100).with_range(0, 50)]),
            // second half of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(50, 100)]),
            // second half of "b"
            FileGroup::new(vec![pfile("b", 100).with_range(50, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_two_large_one_small_files() {
        // "Rebalance" two large files and one small file across empty partitions
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100)]),
            FileGroup::new(vec![pfile("b", 100)]),
            FileGroup::new(vec![pfile("c", 30)]),
        ];

        let partitioner = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_repartition_file_min_size(10);

        // with 4 partitions, can only split the first large file "a"
        let actual = partitioner
            .with_target_partitions(4)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(0, 50)]),
            // All of "b"
            FileGroup::new(vec![pfile("b", 100).with_range(0, 100)]),
            // All of "c"
            FileGroup::new(vec![pfile("c", 30).with_range(0, 30)]),
            // second half of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(50, 100)]),
        ]);
        assert_partitioned_files(expected, actual);

        // With 5 partitions, we can split both "a" and "b", but they can't be intermixed
        let actual = partitioner
            .with_target_partitions(5)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(0, 50)]),
            // scan first half of "b"
            FileGroup::new(vec![pfile("b", 100).with_range(0, 50)]),
            // All of "c"
            FileGroup::new(vec![pfile("c", 30).with_range(0, 30)]),
            // second half of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(50, 100)]),
            // second half of "b"
            FileGroup::new(vec![pfile("b", 100).with_range(50, 100)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_existing_empty() {
        // "Rebalance" files using existing empty partition
        let source_partitions = vec![
            FileGroup::new(vec![pfile("a", 100)]),
            FileGroup::default(),
            FileGroup::new(vec![pfile("b", 40)]),
            FileGroup::default(),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(5)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        // Of the three available groups (2 original empty and 1 new from the
        // target partitions), assign two to "a" and one to "b"
        let expected = Some(vec![
            // Scan of "a" across three groups
            FileGroup::new(vec![pfile("a", 100).with_range(0, 33)]),
            FileGroup::new(vec![pfile("a", 100).with_range(33, 66)]),
            // scan first half of "b"
            FileGroup::new(vec![pfile("b", 40).with_range(0, 20)]),
            // final third of "a"
            FileGroup::new(vec![pfile("a", 100).with_range(66, 100)]),
            // second half of "b"
            FileGroup::new(vec![pfile("b", 40).with_range(20, 40)]),
        ]);
        assert_partitioned_files(expected, actual);
    }
    #[test]
    fn repartition_ordered_existing_group_multiple_files() {
        // groups with multiple files in a group can not be changed, but can divide others
        let source_partitions = vec![
            // two files in an existing partition
            FileGroup::new(vec![pfile("a", 100), pfile("b", 100)]),
            FileGroup::new(vec![pfile("c", 40)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        // Of the three available groups (2 original empty and 1 new from the
        // target partitions), assign two to "a" and one to "b"
        let expected = Some(vec![
            // don't try and rearrange files in the existing partition
            // assuming that the caller had a good reason to put them that way.
            // (it is technically possible to split off ranges from the files if desired)
            FileGroup::new(vec![pfile("a", 100), pfile("b", 100)]),
            // first half of "c"
            FileGroup::new(vec![pfile("c", 40).with_range(0, 20)]),
            // second half of "c"
            FileGroup::new(vec![pfile("c", 40).with_range(20, 40)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    /// Asserts that the two groups of [`PartitionedFile`] are the same
    /// (PartitionedFile doesn't implement PartialEq)
    fn assert_partitioned_files(
        expected: Option<Vec<FileGroup>>,
        actual: Option<Vec<FileGroup>>,
    ) {
        match (expected, actual) {
            (None, None) => {}
            (Some(_), None) => panic!("Expected Some, got None"),
            (None, Some(_)) => panic!("Expected None, got Some"),
            (Some(expected), Some(actual)) => {
                let expected_string = format!("{expected:#?}");
                let actual_string = format!("{actual:#?}");
                assert_eq!(expected_string, actual_string);
            }
        }
    }

    /// returns a partitioned file with the specified path and size
    fn pfile(path: impl Into<String>, file_size: u64) -> PartitionedFile {
        PartitionedFile::new(path, file_size)
    }

    /// Creates a file with partition value with a static size of 10.
    fn pfile_with_pv(path: &str, pv: &str) -> PartitionedFile {
        let mut file = pfile(path, 10);
        file.partition_values = vec![ScalarValue::from(pv)];
        file
    }

    /// repartition the file groups both with and without preserving order
    /// asserting they return the same value and returns that value
    fn repartition_test(
        partitioner: FileGroupPartitioner,
        file_groups: Vec<FileGroup>,
    ) -> Option<Vec<FileGroup>> {
        let repartitioned = partitioner.repartition_file_groups(&file_groups);

        let repartitioned_preserving_sort = partitioner
            .with_preserve_order_within_groups(true)
            .repartition_file_groups(&file_groups);

        assert_partitioned_files(repartitioned.clone(), repartitioned_preserving_sort);
        repartitioned
    }

    // --- [HSTACK] small-file consolidation tests ---

    /// Many small files (one group each) with total size below `repartition_file_min_size`
    /// but more groups than `target_partitions` → consolidate instead of bailing out.
    ///
    /// Setup: 5 groups of 1 file each (size=10), min_size=100, target_partitions=2.
    /// total_size=50 < 100, but len(5) > target(2), so the early-return is skipped.
    /// target_partition_size = 50/2 = 25.
    /// Files are bin-packed: a+b → partition 0 (cumulative 20 ≤ 25),
    /// c overflows (30 > 25) → bumps to partition 1; d+e → partition 1.
    #[test]
    fn repartition_consolidates_small_files_many_groups() {
        let input = vec![
            FileGroup::new(vec![pfile("a", 10)]),
            FileGroup::new(vec![pfile("b", 10)]),
            FileGroup::new(vec![pfile("c", 10)]),
            FileGroup::new(vec![pfile("d", 10)]),
            FileGroup::new(vec![pfile("e", 10)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(100)
            .repartition_file_groups(&input);

        let expected = Some(vec![
            FileGroup::new(vec![pfile("a", 10), pfile("b", 10)]),
            FileGroup::new(vec![pfile("c", 10), pfile("d", 10), pfile("e", 10)]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    /// Small files are grouped without byte-range splitting; large files in the same
    /// dataset are still split by range as before.
    #[test]
    fn repartition_small_files_not_range_split() {
        // One large file (size=80) and four small files (size=5 each), total=100.
        // min_size=20, target=2, target_partition_size=50.
        // Large file exceeds min_size → range-split: [0,50) and [50,80).
        // Small files (size=5 < 20) are bin-packed into whichever partition
        // the range-splitter left current_partition_index on after splitting the large file.
        let input = vec![
            FileGroup::new(vec![pfile("large", 80)]),
            FileGroup::new(vec![pfile("s1", 5)]),
            FileGroup::new(vec![pfile("s2", 5)]),
            FileGroup::new(vec![pfile("s3", 5)]),
            FileGroup::new(vec![pfile("s4", 5)]),
        ];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(20)
            .repartition_file_groups(&input);

        // Large file is split into [0,50) on partition 0 and [50,80) on partition 1.
        // After the split, current_partition_index=1, current_partition_size=30.
        // s1: 30+5=35 ≤ 50 → partition 1
        // s2: 35+5=40 ≤ 50 → partition 1
        // s3: 40+5=45 ≤ 50 → partition 1
        // s4: 45+5=50 ≤ 50 → partition 1
        let expected = Some(vec![
            FileGroup::new(vec![pfile("large", 80).with_range(0, 50)]),
            FileGroup::new(vec![
                pfile("large", 80).with_range(50, 80),
                pfile("s1", 5),
                pfile("s2", 5),
                pfile("s3", 5),
                pfile("s4", 5),
            ]),
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn test_group_by_partition_values_edge_cases() {
        // Edge cases: empty and zero target
        assert!(FileGroup::default().group_by_partition_values(4).is_empty());
        assert!(
            FileGroup::new(vec![pfile("a", 100)])
                .group_by_partition_values(0)
                .is_empty()
        );
    }

    #[test]
    fn test_group_by_partition_values_less_groups_than_target() {
        // File a and b have partition value p1.
        // File c has partition value p2.
        // Grouping by partition value should not redistribute any files since the number of partition
        // values <= max_target_partitions.
        let fg = FileGroup::new(vec![
            pfile_with_pv("a", "p1"),
            pfile_with_pv("b", "p1"),
            pfile_with_pv("c", "p2"),
        ]);
        let groups = fg.group_by_partition_values(4);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 1);
    }

    #[test]
    fn test_group_by_partition_values_more_groups_than_target() {
        // Each file has a single partition value. The number of partition values > max_target_partitions, so
        // they should be round-robin distributed into groups.
        let fg = FileGroup::new(vec![
            pfile_with_pv("a", "p1"),
            pfile_with_pv("b", "p2"),
            pfile_with_pv("c", "p3"),
            pfile_with_pv("d", "p4"),
            pfile_with_pv("e", "p5"),
        ]);
        let groups = fg.group_by_partition_values(3);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 2);
        assert_eq!(groups[2].len(), 1);
    }

    // --- skew-aware bin-packing splitter tests ---

    /// True if any file in any group carries a byte range (i.e. was split).
    fn has_any_range(groups: &[FileGroup]) -> bool {
        groups
            .iter()
            .flat_map(FileGroup::iter)
            .any(|f| f.range.is_some())
    }

    /// Build `n` groups of one file each with the given size, named `<prefix><i>`.
    fn single_file_groups(prefix: &str, size: u64, n: usize) -> Vec<FileGroup> {
        (0..n)
            .map(|i| FileGroup::new(vec![pfile(format!("{prefix}{i}"), size)]))
            .collect()
    }

    #[test]
    fn is_skewed_detects_large_and_uneven() {
        // 4 large (400MB) + 16 small (1MB) = 20 files across 4 partitions.
        // 20 >= 3*4 files and mean/median ~= 80 >= 3.0 -> skewed.
        let mut groups = single_file_groups("big", 400_000_000, 4);
        groups.extend(single_file_groups("small", 1_000_000, 16));

        let partitioner = FileGroupPartitioner::new().with_target_partitions(4);
        assert!(partitioner.is_skewed(&groups));
    }

    #[test]
    fn is_skewed_false_on_uniform_sizes() {
        // 20 identically sized files: mean == median -> not uneven.
        let groups = single_file_groups("u", 10_000_000, 20);

        let partitioner = FileGroupPartitioner::new().with_target_partitions(4);
        assert!(!partitioner.is_skewed(&groups));
    }

    #[test]
    fn is_skewed_false_on_too_few_files() {
        // Uneven sizes but only 8 files for 4 partitions (< 3*4) -> not large.
        let mut groups = single_file_groups("big", 400_000_000, 4);
        groups.extend(single_file_groups("small", 1_000_000, 4));

        let partitioner = FileGroupPartitioner::new().with_target_partitions(4);
        assert!(!partitioner.is_skewed(&groups));
    }

    #[test]
    fn is_skewed_false_on_all_empty_files() {
        let groups = single_file_groups("e", 0, 20);

        let partitioner = FileGroupPartitioner::new().with_target_partitions(4);
        assert!(!partitioner.is_skewed(&groups));
    }

    #[test]
    fn is_skewed_detects_minority_small_file_tail() {
        // Real-world shape: most files are large (~100MB) and a *minority by
        // count* are small (~20MB). The median sits in the large cluster, so a
        // mean/median test would miss this, but byte-balancing still concentrates
        // the many small files onto one partition (fattest ~= 3x the average
        // file count), which is exactly the straggler we want to detect.
        let mut groups = single_file_groups("big", 100_000_000, 73);
        groups.extend(single_file_groups("small", 20_000_000, 27));

        let partitioner = FileGroupPartitioner::new().with_target_partitions(10);
        assert!(partitioner.is_skewed(&groups));
    }

    #[test]
    fn bin_packing_spreads_large_and_small_files() {
        // 4 large + 16 small into 4 partitions: each partition should get
        // exactly one large file plus its share of the small files, and no file
        // should be byte-range split.
        let mut groups = single_file_groups("big", 400_000_000, 4);
        groups.extend(single_file_groups("small", 1_000_000, 16));

        let partitioner = FileGroupPartitioner::new().with_target_partitions(4);
        let result = partitioner
            .repartition_by_bin_packing(&groups)
            .expect("skewed input should repartition");

        assert_eq!(result.len(), 4);
        for group in &result {
            assert_eq!(group.len(), 5, "each partition gets 1 large + 4 small");
            let large = group
                .iter()
                .filter(|f| f.effective_size() == 400_000_000)
                .count();
            assert_eq!(large, 1, "each partition gets exactly one large file");
        }
        assert!(!has_any_range(&result), "bin-packing must not split files");
    }

    #[test]
    fn bin_packing_is_deterministic() {
        let mut groups = single_file_groups("big", 400_000_000, 3);
        groups.extend(single_file_groups("small", 1_000_000, 30));

        let partitioner = FileGroupPartitioner::new().with_target_partitions(5);
        let a = partitioner.repartition_by_bin_packing(&groups);
        let b = partitioner.repartition_by_bin_packing(&groups);
        assert_partitioned_files(a, b);
    }

    #[test]
    fn bin_pack_flag_off_keeps_range_splitting() {
        // Skewed input: with the flag explicitly OFF the default byte-range
        // splitter runs and produces ranges (regardless of the shipping default).
        let mut groups = single_file_groups("big", 400_000_000, 4);
        groups.extend(single_file_groups("small", 1_000_000, 16));

        let result = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(100)
            .with_bin_pack_skewed(false)
            .repartition_file_groups(&groups)
            .expect("should repartition");

        assert!(
            has_any_range(&result),
            "flag-off path splits files by range"
        );
    }

    #[test]
    fn bin_pack_flag_on_avoids_range_splitting() {
        // Same skewed input: with the flag ON whole files are bin-packed and no
        // ranges are produced.
        let mut groups = single_file_groups("big", 400_000_000, 4);
        groups.extend(single_file_groups("small", 1_000_000, 16));

        let result = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(100)
            .with_bin_pack_skewed(true)
            .repartition_file_groups(&groups)
            .expect("should repartition");

        assert_eq!(result.len(), 4);
        assert!(!has_any_range(&result), "bin-packing must not split files");
    }

    #[test]
    fn bin_pack_enabled_only_when_skewed() {
        // Enabled: a skewed table is bin-packed (whole files, no ranges) ...
        let mut skewed = single_file_groups("big", 400_000_000, 4);
        skewed.extend(single_file_groups("small", 1_000_000, 16));
        let out = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(100)
            .with_bin_pack_skewed(true)
            .repartition_file_groups(&skewed)
            .expect("should repartition");
        assert!(!has_any_range(&out), "auto + skewed: bin-packed, no ranges");

        // ... but a uniform table stays on the byte-range splitter.
        let uniform = single_file_groups("u", 100_000_000, 20);
        let out = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(100)
            .with_bin_pack_skewed(true)
            .repartition_file_groups(&uniform)
            .expect("should repartition");
        assert!(has_any_range(&out), "auto + uniform: byte-range split");
    }

    #[test]
    fn bin_pack_skips_range_split_inputs() {
        let partitioner = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_bin_pack_skewed(true);

        // Whole-file skewed input -> bin-pack.
        let mut whole = single_file_groups("big", 400_000_000, 4);
        whole.extend(single_file_groups("small", 1_000_000, 16));
        assert!(
            partitioner.should_bin_pack(&whole),
            "whole-file skew is bin-packed"
        );

        // Same size distribution, but the large files are already byte-range
        // slices (a prior split): leave them to the byte-range splitter.
        let mut ranged: Vec<FileGroup> = (0..4)
            .map(|i| {
                FileGroup::new(vec![
                    pfile(format!("big{i}"), 400_000_000).with_range(0, 100_000_000),
                ])
            })
            .collect();
        ranged.extend(single_file_groups("small", 1_000_000, 16));
        assert!(
            !partitioner.should_bin_pack(&ranged),
            "range-split input is left to the byte-range splitter"
        );
    }
}
