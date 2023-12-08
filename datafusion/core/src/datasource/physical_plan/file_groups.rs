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

//! Logic for managing groups of [`PartitionFile`]s in DataFusion

use crate::datasource::listing::{FileRange, PartitionedFile};
use itertools::Itertools;
use std::cmp::min;

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
#[derive(Debug, Clone)]
pub struct FileGroupPartitioner {
    /// how many partitions should be created
    target_partitions: usize,
    /// the minimum size for a file to be repartitioned.
    repartition_file_min_size: usize,
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
    pub fn new() -> Self {
        Self {
            target_partitions: 1,
            repartition_file_min_size: 10 * 1024 * 1024,
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

    /// Repartition input files according to the settings on this [`FileGroupPartitioner`].
    ///
    /// If no repartitioning is needed or possible, return `None`.
    pub fn repartition_file_groups(
        &self,
        file_groups: &[Vec<PartitionedFile>],
    ) -> Option<Vec<Vec<PartitionedFile>>> {
        let target_partitions = self.target_partitions;
        let repartition_file_min_size = self.repartition_file_min_size;
        let flattened_files = file_groups.iter().flatten().collect::<Vec<_>>();

        // Perform redistribution only in case all files should be read from beginning to end
        let has_ranges = flattened_files.iter().any(|f| f.range.is_some());
        if has_ranges {
            return None;
        }

        let total_size = flattened_files
            .iter()
            .map(|f| f.object_meta.size as i64)
            .sum::<i64>();
        if total_size < (repartition_file_min_size as i64) || total_size == 0 {
            return None;
        }

        let target_partition_size =
            (total_size as usize + (target_partitions) - 1) / (target_partitions);

        let current_partition_index: usize = 0;
        let current_partition_size: usize = 0;

        // Partition byte range evenly for all `PartitionedFile`s
        let repartitioned_files = flattened_files
            .into_iter()
            .scan(
                (current_partition_index, current_partition_size),
                |state, source_file| {
                    let mut produced_files = vec![];
                    let mut range_start = 0;
                    while range_start < source_file.object_meta.size {
                        let range_end = min(
                            range_start + (target_partition_size - state.1),
                            source_file.object_meta.size,
                        );

                        let mut produced_file = source_file.clone();
                        produced_file.range = Some(FileRange {
                            start: range_start as i64,
                            end: range_end as i64,
                        });
                        produced_files.push((state.0, produced_file));

                        if state.1 + (range_end - range_start) >= target_partition_size {
                            state.0 += 1;
                            state.1 = 0;
                        } else {
                            state.1 += range_end - range_start;
                        }
                        range_start = range_end;
                    }
                    Some(produced_files)
                },
            )
            .flatten()
            .group_by(|(partition_idx, _)| *partition_idx)
            .into_iter()
            .map(|(_, group)| group.map(|(_, vals)| vals).collect_vec())
            .collect_vec();

        Some(repartitioned_files)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Empty file won't get partitioned
    #[test]
    fn repartition_empty_file_only() {
        let partitioned_file_empty = pfile("empty", 0);
        let file_group = vec![vec![partitioned_file_empty.clone()]];

        let partitioned_files = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(0)
            .repartition_file_groups(&file_group);

        assert!(partitioned_files.is_none());
    }

    /// Repartition when there is a empty file in file groups
    #[test]
    fn repartition_empty_files() {
        let pfile_a = pfile("a", 10);
        let pfile_b = pfile("b", 10);
        let pfile_empty = pfile("empty", 0);

        let empty_first = vec![
            vec![pfile_empty.clone()],
            vec![pfile_a.clone()],
            vec![pfile_b.clone()],
        ];
        let empty_middle = vec![
            vec![pfile_a.clone()],
            vec![pfile_empty.clone()],
            vec![pfile_b.clone()],
        ];
        let empty_last = vec![vec![pfile_a], vec![pfile_b], vec![pfile_empty]];

        // Repartition file groups into x partitions
        let expected_2 = vec![
            vec![pfile("a", 10).with_range(0, 10)],
            vec![pfile("b", 10).with_range(0, 10)],
        ];
        let expected_3 = vec![
            vec![pfile("a", 10).with_range(0, 7)],
            vec![
                pfile("a", 10).with_range(7, 10),
                pfile("b", 10).with_range(0, 4),
            ],
            vec![pfile("b", 10).with_range(4, 10)],
        ];

        let file_groups_tests = [empty_first, empty_middle, empty_last];

        for fg in file_groups_tests {
            for (n_partition, expected) in [(2, &expected_2), (3, &expected_3)] {
                let actual = FileGroupPartitioner::new()
                    .with_target_partitions(n_partition)
                    .with_repartition_file_min_size(10)
                    .repartition_file_groups(&fg)
                    .unwrap();

                assert_partitioned_files(expected, &actual);
            }
        }
    }

    #[test]
    fn repartition_single_file() {
        // Single file, single partition into multiple partitions
        let single_partition = vec![vec![pfile("a", 123)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&single_partition)
            .unwrap();

        let expected = vec![
            vec![pfile("a", 123).with_range(0, 31)],
            vec![pfile("a", 123).with_range(31, 62)],
            vec![pfile("a", 123).with_range(62, 93)],
            vec![pfile("a", 123).with_range(93, 123)],
        ];
        assert_partitioned_files(&expected, &actual);
    }

    #[test]
    fn repartition_too_much_partitions() {
        // Single file, single partition into 96 partitions
        let partitioned_file = pfile("a", 8);
        let single_partition = vec![vec![partitioned_file]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(96)
            .with_repartition_file_min_size(5)
            .repartition_file_groups(&single_partition)
            .unwrap();

        let expected = vec![
            vec![pfile("a", 8).with_range(0, 1)],
            vec![pfile("a", 8).with_range(1, 2)],
            vec![pfile("a", 8).with_range(2, 3)],
            vec![pfile("a", 8).with_range(3, 4)],
            vec![pfile("a", 8).with_range(4, 5)],
            vec![pfile("a", 8).with_range(5, 6)],
            vec![pfile("a", 8).with_range(6, 7)],
            vec![pfile("a", 8).with_range(7, 8)],
        ];

        assert_partitioned_files(&expected, &actual);
    }

    #[test]
    fn repartition_multiple_partitions() {
        // Multiple files in single partition after redistribution
        let source_partitions = vec![vec![pfile("a", 40)], vec![pfile("b", 60)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions)
            .unwrap();

        let expected = vec![
            vec![pfile("a", 40).with_range(0, 34)],
            vec![
                pfile("a", 40).with_range(34, 40),
                pfile("b", 60).with_range(0, 28),
            ],
            vec![pfile("b", 60).with_range(28, 60)],
        ];
        assert_partitioned_files(&expected, &actual);
    }

    #[test]
    fn repartition_same_num_partitions() {
        // "Rebalance" files across partitions
        let source_partitions = vec![vec![pfile("a", 40)], vec![pfile("b", 60)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions)
            .unwrap();

        let expected = vec![
            vec![
                pfile("a", 40).with_range(0, 40),
                pfile("b", 60).with_range(0, 10),
            ],
            vec![pfile("b", 60).with_range(10, 60)],
        ];
        assert_partitioned_files(&expected, &actual);
    }

    #[test]
    fn repartition_no_action_ranges() {
        // No action due to Some(range) in second file
        let source_partitions = vec![
            vec![pfile("a", 123)],
            vec![pfile("b", 144).with_range(1, 50)],
        ];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        assert!(actual.is_none());
    }

    #[test]
    fn repartition_no_action_min_size() {
        // No action due to target_partition_size
        let single_partition = vec![vec![pfile("a", 123)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(500)
            .repartition_file_groups(&single_partition);

        assert!(actual.is_none());
    }

    /// Asserts that the two groups of `ParititonedFile` are the same
    /// (PartitionedFile doesn't implement PartialEq)
    fn assert_partitioned_files(
        expected: &[Vec<PartitionedFile>],
        actual: &[Vec<PartitionedFile>],
    ) {
        let expected_string = format!("{:#?}", expected);
        let actual_string = format!("{:#?}", actual);
        assert_eq!(expected_string, actual_string);
    }

    /// returns a partitioned file with the specified path and size
    fn pfile(path: impl Into<String>, file_size: u64) -> PartitionedFile {
        PartitionedFile::new(path, file_size)
    }
}
