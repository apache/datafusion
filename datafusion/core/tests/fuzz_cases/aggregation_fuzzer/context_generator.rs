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

use std::{cmp, sync::Arc};

use datafusion::{
    datasource::MemTable,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_catalog::TableProvider;
use datafusion_common::error::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::col;
use rand::{thread_rng, Rng};

use crate::fuzz_cases::aggregation_fuzzer::data_generator::Dataset;

/// SessionContext generator
///
/// During testing, `generate_baseline` will be called firstly to generate a standard [`SessionContext`],
/// and we will run `sql` on it to get the `expected result`. Then `generate` will be called some times to
/// generate some random [`SessionContext`]s, and we will run the same `sql` on them to get `actual results`.
/// Finally, we compare the `actual results` with `expected result`, the test only success while all they are
/// same with the expected.
///
/// Following parameters of [`SessionContext`] used in query running will be generated randomly:
///   - `batch_size`
///   - `target_partitions`
///   - `skip_partial parameters`
///   - hint `sorted` or not
///   - `spilling` or not (TODO, I think a special `MemoryPool` may be needed
///      to support this)
///
pub struct SessionContextGenerator {
    /// Current testing dataset
    dataset: Arc<Dataset>,

    /// Table name of the test table
    table_name: String,

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
    pub fn new(dataset_ref: Arc<Dataset>, table_name: &str) -> Self {
        let candidate_skip_partial_params = vec![
            SkipPartialParams::ensure_trigger(),
            SkipPartialParams::ensure_not_trigger(),
        ];

        let max_batch_size = cmp::max(1, dataset_ref.total_rows_num);
        let max_target_partitions = num_cpus::get();

        Self {
            dataset: dataset_ref,
            table_name: table_name.to_string(),
            max_batch_size,
            candidate_skip_partial_params,
            max_target_partitions,
        }
    }
}

impl SessionContextGenerator {
    /// Generate the `SessionContext` for the baseline run
    pub fn generate_baseline(&self) -> Result<SessionContextWithParams> {
        let schema = self.dataset.batches[0].schema();
        let batches = self.dataset.batches.clone();
        let provider = MemTable::try_new(schema, vec![batches])?;

        // The baseline context should try best to disable all optimizations,
        // and pursuing the rightness.
        let batch_size = self.max_batch_size;
        let target_partitions = 1;
        let skip_partial_params = SkipPartialParams::ensure_not_trigger();

        let builder = GeneratedSessionContextBuilder {
            batch_size,
            target_partitions,
            skip_partial_params,
            sort_hint: false,
            table_name: self.table_name.clone(),
            table_provider: Arc::new(provider),
        };

        builder.build()
    }

    /// Randomly generate session context
    pub fn generate(&self) -> Result<SessionContextWithParams> {
        let mut rng = thread_rng();
        let schema = self.dataset.batches[0].schema();
        let batches = self.dataset.batches.clone();
        let provider = MemTable::try_new(schema, vec![batches])?;

        // We will randomly generate following options:
        //   - `batch_size`, from range: [1, `total_rows_num`]
        //   - `target_partitions`, from range: [1, cpu_num]
        //   - `skip_partial`, trigger or not trigger currently for simplicity
        //   - `sorted`, if found a sorted dataset, will or will not push down this information
        //   - `spilling`(TODO)
        let batch_size = rng.gen_range(1..=self.max_batch_size);

        let target_partitions = rng.gen_range(1..=self.max_target_partitions);

        let skip_partial_params_idx =
            rng.gen_range(0..self.candidate_skip_partial_params.len());
        let skip_partial_params =
            self.candidate_skip_partial_params[skip_partial_params_idx];

        let (provider, sort_hint) =
            if rng.gen_bool(0.5) && !self.dataset.sort_keys.is_empty() {
                // Sort keys exist and random to push down
                let sort_exprs = self
                    .dataset
                    .sort_keys
                    .iter()
                    .map(|key| col(key).sort(true, true))
                    .collect::<Vec<_>>();
                (provider.with_sort_order(vec![sort_exprs]), true)
            } else {
                (provider, false)
            };

        let builder = GeneratedSessionContextBuilder {
            batch_size,
            target_partitions,
            sort_hint,
            skip_partial_params,
            table_name: self.table_name.clone(),
            table_provider: Arc::new(provider),
        };

        builder.build()
    }
}

/// The generated [`SessionContext`] with its params
///
/// Storing the generated `params` is necessary for
/// reporting the broken test case.
pub struct SessionContextWithParams {
    pub ctx: SessionContext,
    pub params: SessionContextParams,
}

/// Collect the generated params, and build the [`SessionContext`]
struct GeneratedSessionContextBuilder {
    batch_size: usize,
    target_partitions: usize,
    sort_hint: bool,
    skip_partial_params: SkipPartialParams,
    table_name: String,
    table_provider: Arc<dyn TableProvider>,
}

impl GeneratedSessionContextBuilder {
    fn build(self) -> Result<SessionContextWithParams> {
        // Build session context
        let mut session_config = SessionConfig::default();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &ScalarValue::UInt64(Some(self.batch_size as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.target_partitions",
            &ScalarValue::UInt64(Some(self.target_partitions as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &ScalarValue::UInt64(Some(self.skip_partial_params.rows_threshold as u64)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &ScalarValue::Float64(Some(self.skip_partial_params.ratio_threshold)),
        );

        let ctx = SessionContext::new_with_config(session_config);
        ctx.register_table(self.table_name, self.table_provider)?;

        let params = SessionContextParams {
            batch_size: self.batch_size,
            target_partitions: self.target_partitions,
            sort_hint: self.sort_hint,
            skip_partial_params: self.skip_partial_params,
        };

        Ok(SessionContextWithParams { ctx, params })
    }
}

/// The generated params for [`SessionContext`]
#[derive(Debug)]
#[allow(dead_code)]
pub struct SessionContextParams {
    batch_size: usize,
    target_partitions: usize,
    sort_hint: bool,
    skip_partial_params: SkipPartialParams,
}

/// Partial skipping parameters
#[derive(Debug, Clone, Copy)]
pub struct SkipPartialParams {
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

#[cfg(test)]
mod test {
    use arrow_array::{RecordBatch, StringArray, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};

    use crate::fuzz_cases::aggregation_fuzzer::check_equality_of_batches;

    use super::*;

    #[tokio::test]
    async fn test_generated_context() {
        // 1. Define a test dataset firstly
        let a_col: StringArray = [
            Some("rust"),
            Some("java"),
            Some("cpp"),
            Some("go"),
            Some("go1"),
            Some("python"),
            Some("python1"),
            Some("python2"),
        ]
        .into_iter()
        .collect();
        // Sort by "b"
        let b_col: UInt32Array = [
            Some(1),
            Some(2),
            Some(4),
            Some(8),
            Some(8),
            Some(16),
            Some(16),
            Some(16),
        ]
        .into_iter()
        .collect();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::UInt32, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a_col), Arc::new(b_col)],
        )
        .unwrap();

        // One row a group to create batches
        let mut batches = Vec::with_capacity(batch.num_rows());
        for start in 0..batch.num_rows() {
            let sub_batch = batch.slice(start, 1);
            batches.push(sub_batch);
        }

        let dataset = Dataset::new(batches, vec!["b".to_string()]);

        // 2. Generate baseline context, and some randomly session contexts.
        // Run the same query on them, and all randoms' results should equal to baseline's
        let ctx_generator = SessionContextGenerator::new(Arc::new(dataset), "fuzz_table");

        let query = "select b, count(a) from fuzz_table group by b";
        let baseline_wrapped_ctx = ctx_generator.generate_baseline().unwrap();
        let mut random_wrapped_ctxs = Vec::with_capacity(8);
        for _ in 0..8 {
            let ctx = ctx_generator.generate().unwrap();
            random_wrapped_ctxs.push(ctx);
        }

        let base_result = baseline_wrapped_ctx
            .ctx
            .sql(query)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        for wrapped_ctx in random_wrapped_ctxs {
            let random_result = wrapped_ctx
                .ctx
                .sql(query)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            check_equality_of_batches(&base_result, &random_result).unwrap();
        }
    }
}
