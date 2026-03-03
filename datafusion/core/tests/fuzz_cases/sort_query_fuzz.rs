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

//! Fuzz Test for various corner cases sorting RecordBatches exceeds available memory and should spill

use std::cmp::min;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Result, human_readable_size, instant::Instant};
use datafusion_execution::disk_manager::DiskManagerBuilder;
use datafusion_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion_expr::display_schema;
use datafusion_physical_plan::spill::get_record_batch_memory_size;
use std::time::Duration;

use datafusion_execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder};
use rand::Rng;
use rand::prelude::IndexedRandom;
use rand::{SeedableRng, rngs::StdRng};

use crate::fuzz_cases::aggregation_fuzzer::check_equality_of_batches;

use super::aggregation_fuzzer::ColumnDescr;
use super::record_batch_generator::{RecordBatchGenerator, get_supported_types_columns};

/// Entry point for executing the sort query fuzzer.
///
/// Now memory limiting is disabled by default. See TODOs in `SortQueryFuzzer`.
#[tokio::test(flavor = "multi_thread")]
async fn sort_query_fuzzer_runner() {
    let random_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let test_generator = SortFuzzerTestGenerator::new(
        2000,
        3,
        "sort_fuzz_table".to_string(),
        get_supported_types_columns(random_seed),
        false,
        random_seed,
    );
    let mut fuzzer = SortQueryFuzzer::new(random_seed)
        // Configs for how many random query to test
        .with_max_rounds(Some(5))
        .with_queries_per_round(4)
        .with_config_variations_per_query(5)
        // Will stop early if the time limit is reached
        .with_time_limit(Duration::from_secs(5))
        .with_test_generator(test_generator);

    fuzzer.run().await.unwrap();
}

/// SortQueryFuzzer holds the runner configuration for executing sort query fuzz tests. The fuzzing details are managed inside `SortFuzzerTestGenerator`.
///
/// It defines:
/// - `max_rounds`: Maximum number of rounds to run (or None to run until `time_limit`).
/// - `queries_per_round`: Number of different queries to run in each round.
/// - `config_variations_per_query`: Number of different configurations to test per query.
/// - `time_limit`: Time limit for the entire fuzzer execution.
///
/// TODO: The following improvements are blocked on https://github.com/apache/datafusion/issues/14748:
/// 1. Support generating queries with arbitrary number of ORDER BY clauses
///    Currently limited to be smaller than number of projected columns
/// 2. Enable special type columns like utf8_low to be used in ORDER BY clauses
/// 3. Enable memory limiting functionality in the fuzzer runner
pub struct SortQueryFuzzer {
    test_gen: SortFuzzerTestGenerator,
    /// Random number generator for the runner, used to generate seeds for inner components.
    /// Seeds for each choice (query, config, etc.) are printed out for reproducibility.
    runner_rng: StdRng,

    // ========================================================================
    // Runner configurations
    // ========================================================================
    /// For each round, a new dataset is generated. If `None`, keep running until
    /// the time limit is reached
    max_rounds: Option<usize>,
    /// How many different queries to run in each round
    queries_per_round: usize,
    /// For each query, how many different configurations to try and make sure their
    /// results are consistent
    config_variations_per_query: usize,
    /// The time limit for the entire sort query fuzzer execution.
    time_limit: Option<Duration>,
}

impl SortQueryFuzzer {
    pub fn new(seed: u64) -> Self {
        let max_rounds = Some(2);
        let queries_per_round = 3;
        let config_variations_per_query = 5;
        let time_limit = None;

        // Filtered out one column due to a known bug https://github.com/apache/datafusion/issues/14748
        // TODO: Remove this once the bug is fixed
        let candidate_columns = get_supported_types_columns(seed)
            .into_iter()
            .filter(|col| {
                col.name != "utf8_low"
                    && col.name != "utf8view"
                    && col.name != "binaryview"
            })
            .collect::<Vec<_>>();

        let test_gen = SortFuzzerTestGenerator::new(
            10000,
            4,
            "sort_fuzz_table".to_string(),
            candidate_columns,
            false,
            seed,
        );

        Self {
            max_rounds,
            queries_per_round,
            config_variations_per_query,
            time_limit,
            test_gen,
            runner_rng: StdRng::seed_from_u64(seed),
        }
    }

    pub fn with_test_generator(mut self, test_gen: SortFuzzerTestGenerator) -> Self {
        self.test_gen = test_gen;
        self
    }

    pub fn with_max_rounds(mut self, max_rounds: Option<usize>) -> Self {
        self.max_rounds = max_rounds;
        self
    }

    pub fn with_queries_per_round(mut self, queries_per_round: usize) -> Self {
        self.queries_per_round = queries_per_round;
        self
    }

    pub fn with_config_variations_per_query(
        mut self,
        config_variations_per_query: usize,
    ) -> Self {
        self.config_variations_per_query = config_variations_per_query;
        self
    }

    pub fn with_time_limit(mut self, time_limit: Duration) -> Self {
        self.time_limit = Some(time_limit);
        self
    }

    fn should_stop_due_to_time_limit(
        &self,
        start_time: Instant,
        n_round: usize,
        n_query: usize,
    ) -> bool {
        if let Some(time_limit) = self.time_limit
            && Instant::now().duration_since(start_time) > time_limit
        {
            println!(
                "[SortQueryFuzzer] Time limit reached: {} queries ({} random configs each) in {} rounds",
                n_round * self.queries_per_round + n_query,
                self.config_variations_per_query,
                n_round
            );
            return true;
        }
        false
    }

    pub async fn run(&mut self) -> Result<()> {
        let start_time = Instant::now();

        // Execute until either`max_rounds` or `time_limit` is reached
        let max_rounds = self.max_rounds.unwrap_or(usize::MAX);
        for round in 0..max_rounds {
            let init_seed = self.runner_rng.random();
            for query_i in 0..self.queries_per_round {
                let query_seed = self.runner_rng.random();
                let mut expected_results: Option<Vec<RecordBatch>> = None; // use first config's result as the expected result
                for config_i in 0..self.config_variations_per_query {
                    if self.should_stop_due_to_time_limit(start_time, round, query_i) {
                        return Ok(());
                    }

                    let config_seed = self.runner_rng.random();

                    println!(
                        "[SortQueryFuzzer] Round {round}, Query {query_i} (Config {config_i})"
                    );
                    println!("  Seeds:");
                    println!("    init_seed   = {init_seed}");
                    println!("    query_seed  = {query_seed}");
                    println!("    config_seed = {config_seed}");

                    let results = self
                        .test_gen
                        .fuzzer_run(init_seed, query_seed, config_seed)
                        .await?;
                    println!("\n"); // Separator between tested runs

                    if expected_results.is_none() {
                        expected_results = Some(results);
                    } else if let Some(ref expected) = expected_results {
                        // `fuzzer_run` might append `LIMIT k` to either the
                        // expected or actual query. The number of results is
                        // checked inside `fuzzer_run()`. Here we only check
                        // that the first k rows of each result are consistent.
                        check_equality_of_batches(expected, &results).unwrap();
                    } else {
                        unreachable!();
                    }
                }
            }
        }
        Ok(())
    }
}

/// Struct to generate and manage a random dataset for fuzz testing.
/// It is able to re-run the failed test cases by setting the same seed printed out.
/// See the unit tests for examples.
///
/// To use this struct:
/// 1. Call `init_partitioned_staggered_batches` to generate a random dataset.
/// 2. Use `generate_random_query` to create a random SQL query.
/// 3. Use `generate_random_config` to create a random configuration.
/// 4. Run the fuzzer check with the generated query and configuration.
pub struct SortFuzzerTestGenerator {
    /// The total number of rows for the registered table
    num_rows: usize,
    /// Max number of partitions for the registered table
    max_partitions: usize,
    /// The name of the registered table
    table_name: String,
    /// The selected columns from all available candidate columns to be used for
    ///  this dataset
    selected_columns: Vec<ColumnDescr>,
    /// If true, will randomly generate a memory limit for the query. Otherwise
    /// the query will run under the context with unlimited memory.
    set_memory_limit: bool,

    /// States related to the randomly generated dataset. `None` if not initialized
    /// by calling `init_partitioned_staggered_batches()`
    dataset_state: Option<DatasetState>,
}

/// Struct to hold states related to the randomly generated dataset
pub struct DatasetState {
    /// Dataset to construct the partitioned memory table. Outer vector is the
    /// partitions, inner vector is staggered batches within the same partition.
    partitioned_staggered_batches: Vec<Vec<RecordBatch>>,
    /// Number of rows in the whole dataset
    dataset_size: usize,
    /// The approximate number of rows of a batch (staggered batches will be generated
    /// with random number of rows between 1 and `approx_batch_size`)
    approx_batch_num_rows: usize,
    /// The schema of the dataset
    schema: SchemaRef,
    /// The memory size of the whole dataset
    mem_size: usize,
}

impl SortFuzzerTestGenerator {
    /// Randomly pick a subset of `candidate_columns` to be used for this dataset
    pub fn new(
        num_rows: usize,
        max_partitions: usize,
        table_name: String,
        candidate_columns: Vec<ColumnDescr>,
        set_memory_limit: bool,
        rng_seed: u64,
    ) -> Self {
        let mut rng = StdRng::seed_from_u64(rng_seed);
        let min_ncol = min(candidate_columns.len(), 5);
        let max_ncol = min(candidate_columns.len(), 10);
        let amount = rng.random_range(min_ncol..=max_ncol);
        let selected_columns = candidate_columns
            .choose_multiple(&mut rng, amount)
            .cloned()
            .collect();

        Self {
            num_rows,
            max_partitions,
            table_name,
            selected_columns,
            set_memory_limit,
            dataset_state: None,
        }
    }

    /// The outer vector is the partitions, the inner vector is the chunked batches
    /// within each partition.
    /// The partition number is determined by `self.max_partitions`.
    /// The chunked batch length is a random number between 1 and `self.num_rows` /
    /// 100 (make sure a single batch won't exceed memory budget for external sort
    /// executions)
    ///
    /// Hack: If we want the query to run under certain degree of parallelism, the
    /// memory table should be generated with more partitions, due to https://github.com/apache/datafusion/issues/15088
    fn init_partitioned_staggered_batches(&mut self, rng_seed: u64) {
        let mut rng = StdRng::seed_from_u64(rng_seed);
        let num_partitions = rng.random_range(1..=self.max_partitions);

        let max_batch_size = self.num_rows / num_partitions / 50;
        let target_partition_size = self.num_rows / num_partitions;

        let mut partitions = Vec::new();
        let mut schema = None;
        for _ in 0..num_partitions {
            let mut partition = Vec::new();
            let mut num_rows = 0;

            // For each partition, generate random batches until there is about enough
            // rows for the specified total number of rows
            while num_rows < target_partition_size {
                // Generate a random batch of size between 1 and max_batch_size

                // Let edge case (1-row batch) more common
                let (min_nrow, max_nrow) = if rng.random_bool(0.1) {
                    (1, 3)
                } else {
                    (1, max_batch_size)
                };

                let mut record_batch_generator = RecordBatchGenerator::new(
                    min_nrow,
                    max_nrow,
                    self.selected_columns.clone(),
                )
                .with_seed(rng.random());

                let record_batch = record_batch_generator.generate().unwrap();
                num_rows += record_batch.num_rows();

                if schema.is_none() {
                    schema = Some(record_batch.schema());
                    println!("  Dataset schema:");
                    println!("    {}", display_schema(schema.as_ref().unwrap()));
                }

                partition.push(record_batch);
            }

            partitions.push(partition);
        }

        // After all partitions are created, optionally make one partition have 0/1 batch
        if num_partitions > 2 && rng.random_bool(0.1) {
            let partition_index = rng.random_range(0..num_partitions);
            if rng.random_bool(0.5) {
                // 0 batch
                partitions[partition_index] = Vec::new();
            } else {
                // 1 batch, keep the old first batch
                let first_batch = partitions[partition_index].first().cloned();
                if let Some(batch) = first_batch {
                    partitions[partition_index] = vec![batch];
                }
            }
        }

        // Init self fields
        let mem_size: usize = partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(get_record_batch_memory_size)
                    .sum::<usize>()
            })
            .sum();

        let dataset_size = partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
            })
            .sum::<usize>();

        let approx_batch_num_rows = max_batch_size;

        self.dataset_state = Some(DatasetState {
            partitioned_staggered_batches: partitions,
            dataset_size,
            approx_batch_num_rows,
            schema: schema.unwrap(),
            mem_size,
        });
    }

    /// Generates a random SQL query string and an optional limit value.
    /// Returns a tuple containing the query string and an optional limit.
    pub fn generate_random_query(&self, rng_seed: u64) -> (String, Option<usize>) {
        let mut rng = StdRng::seed_from_u64(rng_seed);

        let num_columns = rng.random_range(1..=3).min(self.selected_columns.len());
        let selected_columns: Vec<_> = self
            .selected_columns
            .choose_multiple(&mut rng, num_columns)
            .collect();

        let mut order_by_clauses = Vec::new();
        for col in &selected_columns {
            let mut clause = col.name.clone();
            if rng.random_bool(0.5) {
                let order = if rng.random_bool(0.5) { "ASC" } else { "DESC" };
                clause.push_str(&format!(" {order}"));
            }
            if rng.random_bool(0.5) {
                let nulls = if rng.random_bool(0.5) {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                clause.push_str(&format!(" {nulls}"));
            }
            order_by_clauses.push(clause);
        }

        let dataset_size = self.dataset_state.as_ref().unwrap().dataset_size;

        let limit = if rng.random_bool(0.2) {
            // Prefer edge cases for k like 1, dataset_size, etc.
            Some(if rng.random_bool(0.5) {
                let edge_cases =
                    [1, 2, 3, dataset_size - 1, dataset_size, dataset_size + 1];
                *edge_cases.choose(&mut rng).unwrap()
            } else {
                rng.random_range(1..=dataset_size)
            })
        } else {
            None
        };

        let limit_clause = limit.map_or(String::new(), |l| format!(" LIMIT {l}"));

        let query = format!(
            "SELECT {} FROM {} ORDER BY {}{}",
            selected_columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            self.table_name,
            order_by_clauses.join(", "),
            limit_clause
        );

        (query, limit)
    }

    pub fn generate_random_config(
        &self,
        rng_seed: u64,
        with_memory_limit: bool,
    ) -> Result<SessionContext> {
        let mut rng = StdRng::seed_from_u64(rng_seed);
        let init_state = self.dataset_state.as_ref().unwrap();
        let dataset_size = init_state.mem_size;
        let num_partitions = init_state.partitioned_staggered_batches.len();

        // 30% to 200% of the dataset size (if `with_memory_limit` is false, config
        // will use the default unbounded pool to override it later)
        let memory_limit = rng.random_range(
            (dataset_size as f64 * 0.5) as usize..=(dataset_size as f64 * 2.0) as usize,
        );
        // 10% to 20% of the per-partition memory limit size
        let per_partition_mem_limit = memory_limit / num_partitions;
        let sort_spill_reservation_bytes = rng.random_range(
            (per_partition_mem_limit as f64 * 0.2) as usize
                ..=(per_partition_mem_limit as f64 * 0.3) as usize,
        );

        // 1 to 3 times of the approx batch size. Setting this to a very large nvalue
        // will cause external sort to fail.
        let sort_in_place_threshold_bytes = if with_memory_limit {
            // For memory-limited query, setting `sort_in_place_threshold_bytes` too
            // large will cause failure.
            0
        } else {
            let dataset_size = self.dataset_state.as_ref().unwrap().dataset_size;
            rng.random_range(0..=dataset_size * 2_usize)
        };

        // Set up strings for printing
        let memory_limit_str = if with_memory_limit {
            human_readable_size(memory_limit)
        } else {
            "Unbounded".to_string()
        };
        let per_partition_limit_str = if with_memory_limit {
            human_readable_size(per_partition_mem_limit)
        } else {
            "Unbounded".to_string()
        };

        println!("  Config: ");
        println!("    Dataset size: {}", human_readable_size(dataset_size));
        println!("    Number of partitions: {num_partitions}");
        println!("    Batch size: {}", init_state.approx_batch_num_rows / 2);
        println!("    Memory limit: {memory_limit_str}");
        println!("    Per partition memory limit: {per_partition_limit_str}");
        println!(
            "    Sort spill reservation bytes: {}",
            human_readable_size(sort_spill_reservation_bytes)
        );
        println!(
            "    Sort in place threshold bytes: {}",
            human_readable_size(sort_in_place_threshold_bytes)
        );

        let config = SessionConfig::new()
            .with_target_partitions(num_partitions)
            .with_batch_size(init_state.approx_batch_num_rows / 2)
            .with_sort_spill_reservation_bytes(sort_spill_reservation_bytes)
            .with_sort_in_place_threshold_bytes(sort_in_place_threshold_bytes);

        let memory_pool: Arc<dyn MemoryPool> = if with_memory_limit {
            Arc::new(FairSpillPool::new(memory_limit))
        } else {
            Arc::new(UnboundedMemoryPool::default())
        };

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .with_disk_manager_builder(DiskManagerBuilder::default())
            .build_arc()?;

        let ctx = SessionContext::new_with_config_rt(config, runtime);

        let dataset = &init_state.partitioned_staggered_batches;
        let schema = &init_state.schema;

        let provider = MemTable::try_new(schema.clone(), dataset.clone())?;
        ctx.register_table("sort_fuzz_table", Arc::new(provider))?;

        Ok(ctx)
    }

    async fn fuzzer_run(
        &mut self,
        dataset_seed: u64,
        query_seed: u64,
        config_seed: u64,
    ) -> Result<Vec<RecordBatch>> {
        self.init_partitioned_staggered_batches(dataset_seed);
        let (query_str, limit) = self.generate_random_query(query_seed);
        println!("  Query:");
        println!("    {query_str}");

        // ==== Execute the query ====

        // Only enable memory limits if:
        // 1. Query does not contain LIMIT (since topK does not support external execution)
        // 2. Memory limiting is enabled in the test generator config
        let with_mem_limit = !query_str.contains("LIMIT") && self.set_memory_limit;

        let ctx = self.generate_random_config(config_seed, with_mem_limit)?;
        let df = ctx.sql(&query_str).await.unwrap();
        let results = df.collect().await.unwrap();

        // ==== Check the result size is consistent with the limit ====
        let result_num_rows = results.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let dataset_size = self.dataset_state.as_ref().unwrap().dataset_size;

        if let Some(limit) = limit {
            let expected_num_rows = min(limit, dataset_size);
            assert_eq!(result_num_rows, expected_num_rows);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Given the same seed, the result should be the same
    #[tokio::test]
    async fn test_sort_query_fuzzer_deterministic() {
        let gen_seed = 310104;
        let mut test_generator = SortFuzzerTestGenerator::new(
            2000,
            3,
            "sort_fuzz_table".to_string(),
            get_supported_types_columns(gen_seed),
            false,
            gen_seed,
        );

        let res1 = test_generator.fuzzer_run(1, 2, 3).await.unwrap();
        let res2 = test_generator.fuzzer_run(1, 2, 3).await.unwrap();
        check_equality_of_batches(&res1, &res2).unwrap();
    }
}
