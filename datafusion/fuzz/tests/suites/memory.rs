// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Checks incremental memory accounting against recounting the retained arrays
//! after each operation, including arrays that share buffers.

use std::future::Future;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, DictionaryArray, Int32Array, StringArray, StringViewArray,
};
use arrow::datatypes::Int32Type;
use datafusion_common::Result;
use datafusion_common::utils::memory::{
    RecordBatchMemoryCounter, RecordBatchMemoryTracker,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_utils::fuzz::FuzzTest;

pub(crate) struct RecordBatchMemoryFuzzer;

impl FuzzTest for RecordBatchMemoryFuzzer {
    type Dataset = Vec<ArrayRef>;

    fn name(&self) -> &str {
        "record_batch_memory_tracker"
    }

    fn prepare_dataset(&self, _seed: u64) -> impl Future<Output = Result<Self::Dataset>> {
        let int32: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let string: ArrayRef = Arc::new(StringArray::from(vec!["a", "bc", "def"]));
        let string_view: ArrayRef = Arc::new(StringViewArray::from_iter_values([
            "short",
            "another long string that also spills to data buffers for sharing",
        ]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1, 2]),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            )
            .unwrap(),
        );

        std::future::ready(Ok(vec![
            int32.slice(0, 2),
            int32.slice(1, 2),
            Arc::clone(&string),
            string.slice(1, 2),
            string_view.slice(0, 1),
            string_view.slice(1, 1),
            dictionary.slice(0, 2),
            dictionary,
            Arc::new(BooleanArray::from(vec![true, false])),
        ]))
    }

    fn run_case(
        &self,
        arrays: &Self::Dataset,
        seed: u64,
    ) -> impl Future<Output = Result<()>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut tracker = RecordBatchMemoryTracker::new();
        let mut held = vec![0usize; arrays.len()];
        for op in 0..300 {
            let idx = rng.random_range(0..arrays.len());
            if held[idx] > 0 && rng.random_bool(0.5) {
                tracker.uncount_array(arrays[idx].as_ref());
                held[idx] -= 1;
            } else {
                tracker.count_array(arrays[idx].as_ref());
                held[idx] += 1;
            }

            let mut expected = RecordBatchMemoryCounter::new();
            for (array, &count) in arrays.iter().zip(&held) {
                if count > 0 {
                    expected.count_array(array.as_ref());
                }
            }
            assert_eq!(tracker.memory_usage(), expected.memory_usage(), "op {op}");
        }

        std::future::ready(Ok(()))
    }
}
