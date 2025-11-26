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
use datafusion_common::Statistics;
use itertools::Itertools;
use std::cmp::{min, Ordering};
use std::collections::BinaryHeap;
use std::iter::repeat_with;
use std::mem;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::sync::Arc;

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
    pub fn new() -> Self {
        Self {
            target_partitions: 1,
            repartition_file_min_size: 10 * 1024 * 1024,
            preserve_order_within_groups: false,
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
            .map(|f| {
                if let Some(range) = &f.range {
                    range.end - range.start
                } else {
                    f.object_meta.size as i64
                }
            })
            .sum::<i64>();
        if total_size < (repartition_file_min_size as i64) || total_size == 0 {
            return None;
        }

        let target_partition_size =
            (total_size as u64).div_ceil(target_partitions as u64);
        println!("total_size={total_size}, target_partitions={target_partitions}, target_partition_size={target_partition_size}");

        let current_partition_index: usize = 0;
        let current_partition_size: u64 = 0;

        // Partition byte range evenly for all `PartitionedFile`s
        let repartitioned_files = flattened_files
            .into_iter()
            .scan(
                (current_partition_index, current_partition_size),
                |(current_partition_index, current_partition_size), source_file| {
                    let mut produced_files = vec![];
                    let (mut range_start, file_end) =
                        if let Some(range) = &source_file.range {
                            (range.start as u64, range.end as u64)
                        } else {
                            (0, source_file.object_meta.size)
                        };
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
                        file_size: group[0].object_meta.size,
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
            let range_size = to_repartition.range_size() as i64;
            let ToRepartition {
                source_index,
                file_size,
                new_groups,
            } = to_repartition.into_inner();
            assert_eq!(file_groups[source_index].len(), 1);
            let original_file = file_groups[source_index].pop().unwrap();

            let last_group = new_groups.len() - 1;
            let mut range_start: i64 = 0;
            let mut range_end: i64 = range_size;
            for (i, group_index) in new_groups.into_iter().enumerate() {
                let target_group = &mut file_groups[group_index];
                assert!(target_group.is_empty());

                // adjust last range to include the entire file
                if i == last_group {
                    range_end = file_size as i64;
                }
                target_group
                    .push(original_file.clone().with_range(range_start, range_end));
                range_start = range_end;
                range_end += range_size;
            }
        }

        Some(file_groups)
    }
}

/// Represents a group of partitioned files that'll be processed by a single thread.
/// Maintains optional statistics across all files in the group.
#[derive(Debug, Clone)]
pub struct FileGroup {
    /// The files in this group
    files: Vec<PartitionedFile>,
    /// Optional statistics for the data across all files in the group
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
            chunks.push(FileGroup::new(current_chunk))
        }

        chunks
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
}
