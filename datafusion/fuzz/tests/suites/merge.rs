// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Checks sorted-stream merging against sorting the input values, with seeded
//! input layouts, batch boundaries, and empty batches.

use std::future::Future;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::expressions::{PhysicalSortExpr, col};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_utils::fuzz::FuzzTest;
use test_utils::{batches_to_vec, partitions_to_sorted_vec, stagger_batch_with_seed};

type MergeLayout = (&'static str, &'static [&'static [(i32, i32)]]);

// (name, partitions), where each partition lists [start, end) value ranges.
static MERGE_LAYOUTS: &[MergeLayout] = &[
    ("two", &[&[(0, 100)], &[(0, 100)]]),
    ("two_no_overlap", &[&[(0, 20)], &[(20, 40)]]),
    ("three", &[&[(0, 100)], &[(0, 100)], &[(0, 51)]]),
    (
        "three_gaps",
        &[&[(0, 50), (50, 100)], &[(0, 33), (50, 123)], &[(0, 51)]],
    ),
];

pub(crate) struct MergeFuzzer;

pub(crate) struct MergeDataset {
    name: &'static str,
    input: Vec<Vec<RecordBatch>>,
    expected: Vec<Option<i32>>,
}

impl FuzzTest for MergeFuzzer {
    type Dataset = MergeDataset;

    fn name(&self) -> &str {
        "merge"
    }

    fn prepare_dataset(&self, seed: u64) -> impl Future<Output = Result<Self::Dataset>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let (name, ranges) = MERGE_LAYOUTS[rng.random_range(0..MERGE_LAYOUTS.len())];
        let input: Vec<Vec<RecordBatch>> = ranges
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .flat_map(|&(low, high)| {
                        let array: ArrayRef =
                            Arc::new(Int32Array::from_iter_values(low..high));
                        let batch = RecordBatch::try_from_iter([("x", array)]).unwrap();
                        stagger_batch_with_seed(batch, rng.random())
                    })
                    .collect()
            })
            .collect();
        let expected = partitions_to_sorted_vec(&input);
        std::future::ready(Ok(MergeDataset {
            name,
            input,
            expected,
        }))
    }

    async fn run_case(&self, dataset: &Self::Dataset, seed: u64) -> Result<()> {
        // Seven consecutive case seeds cover every output batch size on one dataset.
        let batch_size = [1, 2, 7, 49, 50, 51, 100][(seed % 7) as usize];
        let name = dataset.name;
        let schema = dataset.input[0][0].schema();
        let sort = [PhysicalSortExpr::new(
            col("x", &schema)?,
            SortOptions::default(),
        )]
        .into();
        let input = MemorySourceConfig::try_new_exec(&dataset.input, schema, None)?;
        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(batch_size)),
        );
        let collected = collect(merge, task_ctx).await?;

        assert_eq!(
            dataset.expected,
            batches_to_vec(&collected),
            "{name}, batch size {batch_size}"
        );
        if let Some((_, full_batches)) = collected.split_last() {
            for batch in full_batches {
                assert_eq!(
                    batch.num_rows(),
                    batch_size,
                    "{name}, batch size {batch_size}"
                );
            }
        }
        Ok(())
    }
}
