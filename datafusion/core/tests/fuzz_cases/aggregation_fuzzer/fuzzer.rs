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

use std::collections::HashSet;
use std::sync::Arc;

use crate::fuzz_cases::aggregation_fuzzer::{
    check_equality_of_batches,
    context_generator::{SessionContextGenerator, SessionContextWithParams},
    data_generator::{Dataset, DatasetGenerator, DatasetGeneratorConfig},
    run_sql,
};
use arrow::util::pretty::pretty_format_batches;
use arrow_array::RecordBatch;
use rand::{thread_rng, Rng};
use tokio::task::JoinSet;

/// Rounds to call `generate` of [`SessionContextGenerator`]
/// in [`AggregationFuzzer`], `ctx_gen_rounds` random [`SessionContext`]
/// will generated for each dataset for testing.
const CTX_GEN_ROUNDS: usize = 16;

/// Aggregation fuzzer's builder
pub struct AggregationFuzzerBuilder {
    /// See `candidate_sqls` in [`AggregationFuzzer`], no default, and required to set
    candidate_sqls: Vec<Arc<str>>,

    /// See `table_name` in [`AggregationFuzzer`], no default, and required to set
    table_name: Option<Arc<str>>,

    /// Used to generate `dataset_generator` in [`AggregationFuzzer`],
    /// no default, and required to set
    data_gen_config: Option<DatasetGeneratorConfig>,

    /// See `data_gen_rounds` in [`AggregationFuzzer`], default 16
    data_gen_rounds: usize,
}

impl AggregationFuzzerBuilder {
    fn new() -> Self {
        Self {
            candidate_sqls: Vec::new(),
            table_name: None,
            data_gen_config: None,
            data_gen_rounds: 16,
        }
    }

    /// Adds n random SQL queries to the fuzzer
    pub fn add_sql_from_builder(mut self, query_builder: QueryBuilder, n: usize) -> Self {
        for _ in 0..n {
            self = self.add_sql(&query_builder.generate_query());
        }
        self
    }

    pub fn add_sql(mut self, sql: &str) -> Self {
        self.candidate_sqls.push(Arc::from(sql));
        self
    }

    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = Some(Arc::from(table_name));
        self
    }

    pub fn data_gen_config(mut self, data_gen_config: DatasetGeneratorConfig) -> Self {
        self.data_gen_config = Some(data_gen_config);
        self
    }

    pub fn data_gen_rounds(mut self, data_gen_rounds: usize) -> Self {
        self.data_gen_rounds = data_gen_rounds;
        self
    }

    pub fn build(self) -> AggregationFuzzer {
        assert!(!self.candidate_sqls.is_empty());
        let candidate_sqls = self.candidate_sqls;
        let table_name = self.table_name.expect("table_name is required");
        let data_gen_config = self.data_gen_config.expect("data_gen_config is required");
        let data_gen_rounds = self.data_gen_rounds;

        let dataset_generator = DatasetGenerator::new(data_gen_config);

        AggregationFuzzer {
            candidate_sqls,
            table_name,
            dataset_generator,
            data_gen_rounds,
        }
    }
}

impl std::default::Default for AggregationFuzzerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// AggregationFuzzer randomly generating multiple [`AggregationFuzzTestTask`],
/// and running them to check the correctness of the optimizations
/// (e.g. sorted, partial skipping, spilling...)
pub struct AggregationFuzzer {
    /// Candidate test queries represented by sqls
    candidate_sqls: Vec<Arc<str>>,

    /// The queried table name
    table_name: Arc<str>,

    /// Dataset generator used to randomly generate datasets
    dataset_generator: DatasetGenerator,

    /// Rounds to call `generate` of [`DatasetGenerator`],
    /// len(sort_keys_set) + 1` datasets will be generated for testing.
    ///
    /// It is suggested to set value 2x or more bigger than num of
    /// `candidate_sqls` for better test coverage.
    data_gen_rounds: usize,
}

/// Query group including the tested dataset and its sql query
struct QueryGroup {
    dataset: Dataset,
    sql: Arc<str>,
}

impl AggregationFuzzer {
    pub async fn run(&self) {
        let mut join_set = JoinSet::new();
        let mut rng = thread_rng();

        // Loop to generate datasets and its query
        for _ in 0..self.data_gen_rounds {
            // Generate datasets first
            let datasets = self
                .dataset_generator
                .generate()
                .expect("should success to generate dataset");

            // Then for each of them, we random select a test sql for it
            let query_groups = datasets
                .into_iter()
                .map(|dataset| {
                    let sql_idx = rng.gen_range(0..self.candidate_sqls.len());
                    let sql = self.candidate_sqls[sql_idx].clone();

                    QueryGroup { dataset, sql }
                })
                .collect::<Vec<_>>();

            let tasks = self.generate_fuzz_tasks(query_groups).await;
            for task in tasks {
                join_set.spawn(async move {
                    task.run().await;
                });
            }
        }

        while let Some(join_handle) = join_set.join_next().await {
            // propagate errors
            join_handle.unwrap();
        }
    }

    async fn generate_fuzz_tasks(
        &self,
        query_groups: Vec<QueryGroup>,
    ) -> Vec<AggregationFuzzTestTask> {
        let mut tasks = Vec::with_capacity(query_groups.len() * CTX_GEN_ROUNDS);
        for QueryGroup { dataset, sql } in query_groups {
            let dataset_ref = Arc::new(dataset);
            let ctx_generator =
                SessionContextGenerator::new(dataset_ref.clone(), &self.table_name);

            // Generate the baseline context, and get the baseline result firstly
            let baseline_ctx_with_params = ctx_generator
                .generate_baseline()
                .expect("should success to generate baseline session context");
            let baseline_result = run_sql(&sql, &baseline_ctx_with_params.ctx)
                .await
                .expect("should success to run baseline sql");
            let baseline_result = Arc::new(baseline_result);
            // Generate test tasks
            for _ in 0..CTX_GEN_ROUNDS {
                let ctx_with_params = ctx_generator
                    .generate()
                    .expect("should success to generate session context");
                let task = AggregationFuzzTestTask {
                    dataset_ref: dataset_ref.clone(),
                    expected_result: baseline_result.clone(),
                    sql: sql.clone(),
                    ctx_with_params,
                };

                tasks.push(task);
            }
        }
        tasks
    }
}

/// One test task generated by [`AggregationFuzzer`]
///
/// It includes:
///   - `expected_result`, the expected result generated by baseline [`SessionContext`]
///     (disable all possible optimizations for ensuring correctness).
///    
///   - `ctx`, a randomly generated [`SessionContext`], `sql` will be run
///     on it after, and check if the result is equal to expected.
///   
///   - `sql`, the selected test sql
///
///   - `dataset_ref`, the input dataset, store it for error reported when found
///      the inconsistency between the one for `ctx` and `expected results`.
///
struct AggregationFuzzTestTask {
    /// Generated session context in current test case
    ctx_with_params: SessionContextWithParams,

    /// Expected result in current test case
    /// It is generate from `query` + `baseline session context`
    expected_result: Arc<Vec<RecordBatch>>,

    /// The test query
    /// Use sql to represent it currently.
    sql: Arc<str>,

    /// The test dataset for error reporting
    dataset_ref: Arc<Dataset>,
}

impl AggregationFuzzTestTask {
    async fn run(&self) {
        let task_result = run_sql(&self.sql, &self.ctx_with_params.ctx)
            .await
            .expect("should success to run sql");
        self.check_result(&task_result, &self.expected_result);
    }

    // TODO: maybe we should persist the `expected_result` and `task_result`,
    // because the readability is not so good if we just print it.
    fn check_result(&self, task_result: &[RecordBatch], expected_result: &[RecordBatch]) {
        let result = check_equality_of_batches(task_result, expected_result);
        if let Err(e) = result {
            // If we found inconsistent result, we print the test details for reproducing at first
            println!(
                "##### AggregationFuzzer error report #####
                 ### Sql:\n{}\n\
                 ### Schema:\n{}\n\
                 ### Session context params:\n{:?}\n\
                 ### Inconsistent row:\n\
                 - row_idx:{}\n\
                 - task_row:{}\n\
                 - expected_row:{}\n\
                 ### Task total result:\n{}\n\
                 ### Expected total result:\n{}\n\
                 ### Input:\n{}\n\
                 ",
                self.sql,
                self.dataset_ref.batches[0].schema_ref(),
                self.ctx_with_params.params,
                e.row_idx,
                e.lhs_row,
                e.rhs_row,
                pretty_format_batches(task_result).unwrap(),
                pretty_format_batches(expected_result).unwrap(),
                pretty_format_batches(&self.dataset_ref.batches).unwrap(),
            );

            // Then we just panic
            panic!();
        }
    }
}

/// Random aggregate query builder
#[derive(Debug, Default)]
pub struct QueryBuilder {
    table_name: String,
    /// Aggregate functions to be used in the query
    /// (function_name, is_distinct)
    aggregate_functions: Vec<(String, bool)>,
    /// Columns to be used in group by
    group_by_columns: Vec<String>,
    /// Columns to be used as arguments in the aggregate functions
    arguments: Vec<String>,
}
impl QueryBuilder {
    pub fn new() -> Self {
        std::default::Default::default()
    }

    /// Set the table name for the query
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Add a new aggregate function to the query
    pub fn with_aggregate_function(
        mut self,
        aggregate_function: impl Into<String>,
    ) -> Self {
        self.aggregate_functions
            .push((aggregate_function.into(), false));
        self
    }

    /// Add a new distinct aggregate function to the query
    pub fn with_distinct_aggregate_function(
        mut self,
        aggregate_function: impl Into<String>,
    ) -> Self {
        self.aggregate_functions
            .push((aggregate_function.into(), true));
        self
    }

    /// Add a column to be used in the group bys
    pub fn with_group_by_column(mut self, group_by: impl Into<String>) -> Self {
        self.group_by_columns.push(group_by.into());
        self
    }

    /// Add a column to be used as an argument in the aggregate functions
    pub fn with_aggregate_argument(mut self, argument: impl Into<String>) -> Self {
        self.arguments.push(argument.into());
        self
    }

    pub fn generate_query(&self) -> String {
        let group_by = self.random_group_by();
        let mut query = format!("SELECT ");
        query.push_str(&self.random_aggregate_functions().join(", "));
        query.push_str(" FROM ");
        query.push_str(&self.table_name);
        if !group_by.is_empty() {
            query.push_str(" GROUP BY ");
            query.push_str(&group_by.join(", "));
        }
        query
    }

    /// Generate a random number of aggregate functions (potentially repeating)
    /// to use in the query between 1 and 5
    fn random_aggregate_functions(&self) -> Vec<String> {
        let mut rng = thread_rng();
        let num_aggregate_functions = rng.gen_range(1..5);

        let mut alias_gen = 1;

        let mut aggregate_functions = vec![];
        while aggregate_functions.len() < num_aggregate_functions {
            let idx = rng.gen_range(0..self.aggregate_functions.len());
            let (function_name, is_distinct) = &self.aggregate_functions[idx];
            let argument = self.random_argument();
            let alias = format!("col{}", alias_gen);
            let distinct = if *is_distinct { "DISTINCT " } else { "" };
            alias_gen += 1;
            let function = format!("{function_name}({distinct}{argument}) as {alias}");
            aggregate_functions.push(function);
        }
        aggregate_functions
    }

    /// Pick a random aggregate function argument
    fn random_argument(&self) -> String {
        let mut rng = thread_rng();
        let idx = rng.gen_range(0..self.arguments.len());
        self.arguments[idx].clone()
    }

    /// Pick a random number of fields to group by (non repeating)
    fn random_group_by(&self) -> Vec<String> {
        let mut rng = thread_rng();
        let num_group_by = rng.gen_range(1..self.group_by_columns.len());

        let mut already_used = HashSet::new();
        let mut group_by = vec![];
        while group_by.len() < num_group_by {
            let idx = rng.gen_range(0..self.group_by_columns.len());
            if already_used.insert(idx) {
                group_by.push(self.group_by_columns[idx].clone());
            }
        }
        group_by
    }
}
