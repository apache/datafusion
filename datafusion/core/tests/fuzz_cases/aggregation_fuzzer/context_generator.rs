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

use std::{cmp, sync::Arc, usize};

use datafusion::{
    datasource::MemTable,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::ScalarValue;
use datafusion_expr::col;
use rand::{thread_rng, Rng};

use crate::fuzz_cases::aggregation_fuzzer::data_generator::Dataset;

/// SessionContext generator
///
/// It will generate one random `SessionContext` when `generate` function is called.
///
/// Following parameters of [`SessionContext`] used in query running will be generated randomly:
///   - `batch_size`
///   - `target_partitions`
///   - `skip_partial parameters`
///   - push down `sorted information`` or not
///   - `spilling` or not (TODO, I think a special `MemoryPool` may be needed
///      to support this)
///
/// Then we will use [`SessionContext`] to accept `sql`, and generate [`DataFrame`] to run related query.
///
/// [`DataFrame`]: datafusion::prelude::DataFrame
pub struct SessionContextGenerator {
    /// Current testing dataset
    dataset: Dataset,

    /// Used in generate the random `batch_size`
    ///
    /// The generated `batch_size` is between (0, total_rows_num]
    max_batch_size: usize,

    /// Candidate `SkipPartialParams` which will be picked randomly
    candidate_skip_partial_params: Vec<SkipPartialParams>,

    /// The upper bound of the randomly generated target partitions,
    /// and the lower bound will be 1
    max_target_partitions: usize,
}

impl SessionContextGenerator {
    fn new(dataset: Dataset) -> Self {
        let candidate_skip_partial_params = vec![
            SkipPartialParams::ensure_trigger(),
            SkipPartialParams::ensure_not_trigger(),
        ];

        let max_batch_size = cmp::max(1, dataset.total_rows_num);
        let max_target_partitions = num_cpus::get();

        Self {
            dataset,
            max_batch_size,
            candidate_skip_partial_params,
            max_target_partitions,
        }
    }
}

impl SessionContextGenerator {
    /// Generate the `SessionContext` for the baseline run
    pub fn generate_baseline(&self) -> SessionContext {
        let schema = self.dataset.batches[0].schema();
        let batches = self.dataset.batches.clone();
        let provider = MemTable::try_new(schema, vec![batches]).unwrap();

        let batch_size = self.max_batch_size;
        let target_partitions = 1;
        let skip_partial_params = SkipPartialParams::ensure_not_trigger();

        // Generate session context
        let mut session_config = SessionConfig::default();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &ScalarValue::UInt64(Some(batch_size as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.target_partitions",
            &ScalarValue::UInt64(Some(target_partitions as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::UInt64(Some(skip_partial_params.rows_threshold as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(skip_partial_params.ratio_threshold)),
        );

        let ctx = SessionContext::new_with_config(session_config);
        ctx.register_table("fuzz_table", Arc::new(provider))
            .unwrap();

        ctx
    }

    pub fn generate(&self) -> SessionContext {
        let mut rng = thread_rng();
        let schema = self.dataset.batches[0].schema();
        let batches = self.dataset.batches.clone();
        let provider = MemTable::try_new(schema, vec![batches]).unwrap();

        // We will randomly generate following options:
        //   - `batch_size`, from range: [1, `total_rows_num`]
        //   - `target_partitions`, from range: [1, cpu_num]
        //   - `skip_partial`, trigger or not trigger currently for simplicity
        //   - `sorted`, if found a sorted dataset, will or will not push down this information
        //   - `spilling`, still not supported now, I think a special `MemoryPool` may be needed
        //      to support this
        let batch_size = rng.gen_range(1..=self.max_batch_size);

        let target_partitions = rng.gen_range(1..=self.max_target_partitions);

        let skip_partial_params_idx =
            rng.gen_range(0..self.candidate_skip_partial_params.len());
        let skip_partial_params =
            self.candidate_skip_partial_params[skip_partial_params_idx];

        let provider = if rng.gen_bool(0.5) && !self.dataset.sort_keys.is_empty() {
            // Sort keys exist and random to push down
            let sort_exprs = self
                .dataset
                .sort_keys
                .iter()
                .map(|key| col(key).sort(true, true))
                .collect::<Vec<_>>();
            provider.with_sort_order(vec![sort_exprs])
        } else {
            provider
        };

        // Generate session context
        let mut session_config = SessionConfig::default();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &ScalarValue::UInt64(Some(batch_size as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.target_partitions",
            &ScalarValue::UInt64(Some(target_partitions as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::UInt64(Some(skip_partial_params.rows_threshold as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(skip_partial_params.ratio_threshold)),
        );

        let ctx = SessionContext::new_with_config(session_config);
        ctx.register_table("fuzz_table", Arc::new(provider))
            .unwrap();

        ctx
    }
}

/// Partial skipping parameters
#[derive(Debug, Clone, Copy)]
struct SkipPartialParams {
    /// Related to `skip_partial_aggregation_probe_ratio_threshold` in `ExecutionOptions`
    pub ratio_threshold: f64,

    /// Related to `skip_partial_aggregation_probe_rows_threshold` in `ExecutionOptions`
    pub rows_threshold: usize,
}

impl SkipPartialParams {
    /// Generate `SkipPartialParams` ensuring to trigger partial skipping
    pub fn ensure_trigger() -> Self {
        Self {
            ratio_threshold: 0.0,
            rows_threshold: 0,
        }
    }

    /// Generate `SkipPartialParams` ensuring not to trigger partial skipping
    pub fn ensure_not_trigger() -> Self {
        Self {
            ratio_threshold: 1.0,
            rows_threshold: usize::MAX,
        }
    }
}
