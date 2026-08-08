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
use datafusion_common::ScalarValue;
use datafusion_common::{error::Result, utils::get_available_parallelism};
use datafusion_execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::col;
use rand::{Rng, rng};

use crate::fuzz_cases::aggregation_fuzzer::data_generator::Dataset;

/// Bounded pool sizes, each a fraction of the aggregate's peak reservation:
/// 2x, 1/2, 2/5, and 1/3 of the peak. 2x is generous and should not spill (the
/// light end); the smaller ones force a spill, deeper as the pool shrinks. Each
/// entry is `(numerator, denominator)`.
const SPILL_POOL_PEAK_FRACTIONS: [(usize, usize); 4] = [(2, 1), (1, 2), (2, 5), (1, 3)];

/// Batch size cap for spilling contexts, so the spill's sort reservation stays
/// small enough to fit under the pool.
const SPILL_BATCH_SIZE_CAP: usize = 256;

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
///   - `enable_migration_aggregate`
///   - hint `sorted` or not
///   - memory limit (bounded pool to force spilling, or unbounded)
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
        let max_target_partitions = get_available_parallelism();

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
    /// Generate the `SessionContext` for the baseline run.
    ///
    /// Runs under the default unbounded pool, so it never spills (it is the
    /// oracle). The caller reads the aggregate's peak from the plan metrics of
    /// this run (see `run_sql_capturing_peak`) to size the spilling pools.
    pub fn generate_baseline(&self) -> Result<SessionContextWithParams> {
        let schema = self.dataset.batches[0].schema();
        let batches = self.dataset.batches.clone();
        let provider = MemTable::try_new(schema, vec![batches])?;

        // The baseline context should try best to disable all optimizations,
        // and pursuing the rightness.
        let batch_size = self.max_batch_size;
        let target_partitions = 1;
        let skip_partial_params = SkipPartialParams::ensure_not_trigger();
        let enable_migration_aggregate = false;

        let builder = GeneratedSessionContextBuilder {
            batch_size,
            target_partitions,
            skip_partial_params,
            enable_migration_aggregate,
            sort_hint: false,
            memory_pool: None,
            memory_limit: None,
            table_name: self.table_name.clone(),
            table_provider: Arc::new(provider),
        };

        builder.build()
    }

    /// Randomly generate a session context.
    ///
    /// `agg_peak` is the aggregate's peak reservation from the baseline, used to
    /// size the spilling pool.
    pub fn generate(&self, agg_peak: usize) -> Result<SessionContextWithParams> {
        let mut rng = rng();
        let schema = self.dataset.batches[0].schema();
        let batches = self.dataset.batches.clone();
        let provider = MemTable::try_new(schema, vec![batches])?;

        // We will randomly generate following options:
        //   - `batch_size`, from range: [1, `total_rows_num`]
        //   - `target_partitions`, from range: [1, cpu_num]
        //   - `skip_partial`, trigger or not trigger currently for simplicity
        //   - `enable_migration_aggregate`, true or false
        //   - `sorted`, if found a sorted dataset, will or will not push down this information
        //   - memory limit, `None` (unbounded) or a bounded pool that forces spilling

        // Decide spilling first; it constrains batch_size and partitions below.
        let spilling = agg_peak > 0 && rng.random_bool(0.5);

        // Cap batch_size when spilling. The spill reserves ~2x the emitted batch to sort it, so a large batch
        // needs a pool bigger than the aggregate's peak and never spills. Large batches stay covered by the unbounded rounds.
        let batch_size = if spilling {
            rng.random_range(1..=cmp::min(self.max_batch_size, SPILL_BATCH_SIZE_CAP))
        } else {
            rng.random_range(1..=self.max_batch_size)
        };

        // Single partition when spilling. `FairSpillPool` splits across the  per-partition aggregate consumers,
        // so with many partitions each share is too small and hits `ResourcesExhausted` before it can spill. Multi
        // partition stays covered by the unbounded rounds.
        // Single partition when spilling. `FairSpillPool` splits across the  per-partition aggregate consumers,
        // so with many partitions each share is too small and hits `ResourcesExhausted` before it can spill. Multi
        // partition stays covered by the unbounded rounds.
        let target_partitions = if spilling {
            1
        } else {
            rng.random_range(1..=self.max_target_partitions)
        };

        let skip_partial_params_idx =
            rng.random_range(0..self.candidate_skip_partial_params.len());
        let skip_partial_params =
            self.candidate_skip_partial_params[skip_partial_params_idx];

        let enable_migration_aggregate = rng.random_bool(0.5);

        // Pool at a randomly picked fraction of the measured peak, so pressure
        // ranges from light (above peak, no spill needed) to heavy across
        // contexts.
        let memory_limit = if spilling {
            let (num, den) = SPILL_POOL_PEAK_FRACTIONS
                [rng.random_range(0..SPILL_POOL_PEAK_FRACTIONS.len())];
            Some(cmp::max(1, agg_peak.saturating_mul(num) / den))
        } else {
            None
        };
        let memory_pool = memory_limit
            .map(|bytes| Arc::new(FairSpillPool::new(bytes)) as Arc<dyn MemoryPool>);

        let (provider, sort_hint) =
            if rng.random_bool(0.5) && !self.dataset.sort_keys.is_empty() {
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
            enable_migration_aggregate,
            memory_pool,
            memory_limit,
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
    enable_migration_aggregate: bool,
    /// Pool to install, or `None` for the default unbounded pool.
    memory_pool: Option<Arc<dyn MemoryPool>>,
    /// Bounded pool size in bytes, recorded in the failure report; `None` if unbounded.
    memory_limit: Option<usize>,
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
        session_config = session_config.set_bool(
            "datafusion.execution.enable_migration_aggregate",
            self.enable_migration_aggregate,
        );

        // Bounded pool forces the spill path; `None` keeps the default unbounded
        // pool (behavior-preserving).
        let ctx = match self.memory_pool {
            Some(pool) => {
                let runtime = RuntimeEnvBuilder::new()
                    .with_memory_pool(pool)
                    .build_arc()?;
                SessionContext::new_with_config_rt(session_config, runtime)
            }
            None => SessionContext::new_with_config(session_config),
        };
        ctx.register_table(self.table_name, self.table_provider)?;

        let params = SessionContextParams {
            batch_size: self.batch_size,
            target_partitions: self.target_partitions,
            sort_hint: self.sort_hint,
            skip_partial_params: self.skip_partial_params,
            enable_migration_aggregate: self.enable_migration_aggregate,
            memory_limit: self.memory_limit,
        };

        Ok(SessionContextWithParams { ctx, params })
    }
}

/// The generated params for [`SessionContext`]
#[derive(Debug)]
#[expect(dead_code)]
pub struct SessionContextParams {
    batch_size: usize,
    target_partitions: usize,
    sort_hint: bool,
    skip_partial_params: SkipPartialParams,
    enable_migration_aggregate: bool,
    memory_limit: Option<usize>, // Bounded pool size in bytes for this run, or `None` for unbounded.
}

impl SessionContextParams {
    /// Bounded pool size for this run, or `None` if the pool was unbounded.
    pub(crate) fn memory_limit(&self) -> Option<usize> {
        self.memory_limit
    }
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
    use arrow::array::{RecordBatch, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion_common::DataFusionError;

    use crate::fuzz_cases::aggregation_fuzzer::{
        check_equality_of_batches, run_sql_capturing_peak,
    };

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

        // Run the baseline first to capture the peak, then size the spilling
        // contexts from it.
        let (base_result, agg_peak) =
            run_sql_capturing_peak(query, &baseline_wrapped_ctx.ctx)
                .await
                .unwrap();

        let mut random_wrapped_ctxs = Vec::with_capacity(8);
        for _ in 0..8 {
            let ctx = ctx_generator.generate(agg_peak).unwrap();
            random_wrapped_ctxs.push(ctx);
        }

        for wrapped_ctx in random_wrapped_ctxs {
            let memory_limit = wrapped_ctx.params.memory_limit();
            match wrapped_ctx.ctx.sql(query).await.unwrap().collect().await {
                Ok(random_result) => {
                    check_equality_of_batches(&base_result, &random_result).unwrap();
                }
                // A bounded pool may be too tight to spill on this tiny dataset;
                // that is an acceptable skip, matching the fuzzer's behavior.
                Err(e)
                    if memory_limit.is_some()
                        && matches!(
                            e.find_root(),
                            DataFusionError::ResourcesExhausted(_)
                        ) => {}
                Err(e) => panic!("unexpected error running generated context: {e}"),
            }
        }
    }

    /// Guards against the spill knob silently becoming a no-op: a bounded context
    /// on high-cardinality data must actually reach the spill path. Without this,
    /// a regression that stopped forcing spills would still pass every other test.
    #[tokio::test]
    async fn test_generated_context_spills() {
        // High cardinality: every row is its own group, so the aggregate builds a
        // large state and a fraction-of-peak pool is forced to spill. No sort keys,
        // so this stays on the hash aggregate (spilling) path.
        let num_rows = 20_000;
        let k: UInt64Array = (0..num_rows as u64).map(Some).collect();
        let schema = Schema::new(vec![Field::new("k", DataType::UInt64, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(k)]).unwrap();

        let mut batches = Vec::new();
        for start in (0..batch.num_rows()).step_by(1024) {
            let len = cmp::min(1024, batch.num_rows() - start);
            batches.push(batch.slice(start, len));
        }

        let dataset = Dataset::new(batches, vec![]);
        let ctx_generator = SessionContextGenerator::new(Arc::new(dataset), "fuzz_table");
        let query = "select k, count(*) from fuzz_table group by k";

        // Run the baseline to capture the peak that sizes the spill pools.
        let baseline = ctx_generator.generate_baseline().unwrap();
        let (_baseline_result, agg_peak) =
            run_sql_capturing_peak(query, &baseline.ctx).await.unwrap();
        assert!(agg_peak > 0, "baseline should have reserved memory");

        // Generate contexts until a bounded one actually spills. Spilling is a
        // random knob, so loop; on this data a bounded context spills nearly every
        // time, and exhausted rounds are skipped just like the fuzzer does.
        let mut spilled = false;
        for _ in 0..50 {
            let wrapped = ctx_generator.generate(agg_peak).unwrap();
            if wrapped.params.memory_limit().is_none() {
                continue;
            }
            let explain = format!("EXPLAIN ANALYZE {query}");
            let Ok(rows) = wrapped.ctx.sql(&explain).await.unwrap().collect().await
            else {
                continue; // pool too tight this round, skip like the fuzzer
            };
            let plan = pretty_format_batches(&rows).unwrap().to_string();
            if plan.contains("spill_count=") && !plan.contains("spill_count=0,") {
                spilled = true;
                break;
            }
        }
        assert!(spilled, "a bounded context should have spilled to disk");
    }
}
