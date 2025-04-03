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

use std::sync::Arc;
use std::{collections::HashSet, str::FromStr};

use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion_common::{DataFusionError, Result};
use datafusion_common_runtime::JoinSet;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

use crate::fuzz_cases::aggregation_fuzzer::{
    check_equality_of_batches,
    context_generator::{SessionContextGenerator, SessionContextWithParams},
    data_generator::{Dataset, DatasetGenerator, DatasetGeneratorConfig},
    run_sql,
};

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

    /// Adds random SQL queries to the fuzzer along with the table name
    ///
    /// Adds
    /// - 3 random queries
    /// - 3 random queries for each group by selected from the sort keys
    /// - 1 random query with no grouping
    pub fn add_query_builder(mut self, mut query_builder: QueryBuilder) -> Self {
        const NUM_QUERIES: usize = 3;
        for _ in 0..NUM_QUERIES {
            let sql = query_builder.generate_query();
            self.candidate_sqls.push(Arc::from(sql));
        }
        // also add several queries limited to grouping on the group by columns only, if any
        // So if the data is sorted on `a,b` only group by `a,b` or`a` or `b`
        if let Some(data_gen_config) = &self.data_gen_config {
            for sort_keys in &data_gen_config.sort_keys_set {
                let group_by_columns = sort_keys.iter().map(|s| s.as_str());
                query_builder = query_builder.set_group_by_columns(group_by_columns);
                for _ in 0..NUM_QUERIES {
                    let sql = query_builder.generate_query();
                    self.candidate_sqls.push(Arc::from(sql));
                }
            }
        }
        // also add a query with no grouping
        query_builder = query_builder.set_group_by_columns(vec![]);
        let sql = query_builder.generate_query();
        self.candidate_sqls.push(Arc::from(sql));

        self.table_name(query_builder.table_name())
    }

    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = Some(Arc::from(table_name));
        self
    }

    pub fn data_gen_config(mut self, data_gen_config: DatasetGeneratorConfig) -> Self {
        self.data_gen_config = Some(data_gen_config);
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

impl Default for AggregationFuzzerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<DatasetGeneratorConfig> for AggregationFuzzerBuilder {
    fn from(value: DatasetGeneratorConfig) -> Self {
        Self::default().data_gen_config(value)
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
    /// Run the fuzzer, printing an error and panicking if any of the tasks fail
    pub async fn run(&self) {
        let res = self.run_inner().await;

        if let Err(e) = res {
            // Print the error via `Display` so that it displays nicely (the default `unwrap()`
            // prints using `Debug` which escapes newlines, and makes multi-line messages
            // hard to read
            println!("{e}");
            panic!("Error!");
        }
    }

    async fn run_inner(&self) -> Result<()> {
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

            for q in &query_groups {
                println!(" Testing with query {}", q.sql);
            }

            let tasks = self.generate_fuzz_tasks(query_groups).await;
            for task in tasks {
                join_set.spawn(async move { task.run().await });
            }
        }

        while let Some(join_handle) = join_set.join_next().await {
            // propagate errors
            join_handle.map_err(|e| {
                DataFusionError::Internal(format!(
                    "AggregationFuzzer task error: {:?}",
                    e
                ))
            })??;
        }
        Ok(())
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
    async fn run(&self) -> Result<()> {
        let task_result = run_sql(&self.sql, &self.ctx_with_params.ctx)
            .await
            .map_err(|e| e.context(self.context_error_report()))?;
        self.check_result(&task_result, &self.expected_result)
    }

    fn check_result(
        &self,
        task_result: &[RecordBatch],
        expected_result: &[RecordBatch],
    ) -> Result<()> {
        check_equality_of_batches(task_result, expected_result).map_err(|e| {
            // If we found inconsistent result, we print the test details for reproducing at first
            let message = format!(
                "##### AggregationFuzzer error report #####\n\
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
                format_batches_with_limit(task_result),
                format_batches_with_limit(expected_result),
                format_batches_with_limit(&self.dataset_ref.batches),
            );
            DataFusionError::Internal(message)
        })
    }

    /// Returns a formatted error message
    fn context_error_report(&self) -> String {
        format!(
            "##### AggregationFuzzer error report #####\n\
               ### Sql:\n{}\n\
               ### Schema:\n{}\n\
               ### Session context params:\n{:?}\n\
               ### Input:\n{}\n\
                     ",
            self.sql,
            self.dataset_ref.batches[0].schema_ref(),
            self.ctx_with_params.params,
            pretty_format_batches(&self.dataset_ref.batches).unwrap(),
        )
    }
}

/// Pretty prints the `RecordBatch`es, limited to the first 100 rows
fn format_batches_with_limit(batches: &[RecordBatch]) -> impl std::fmt::Display {
    const MAX_ROWS: usize = 100;
    let mut row_count = 0;
    let to_print = batches
        .iter()
        .filter_map(|b| {
            if row_count >= MAX_ROWS {
                None
            } else if row_count + b.num_rows() > MAX_ROWS {
                // output last rows before limit
                let slice_len = MAX_ROWS - row_count;
                let b = b.slice(0, slice_len);
                row_count += slice_len;
                Some(b)
            } else {
                row_count += b.num_rows();
                Some(b.clone())
            }
        })
        .collect::<Vec<_>>();

    pretty_format_batches(&to_print).unwrap()
}

/// Random aggregate query builder
///
/// Creates queries like
/// ```sql
/// SELECT AGG(..) FROM table_name GROUP BY <group_by_columns>
///```
#[derive(Debug, Default, Clone)]
pub struct QueryBuilder {
    /// The name of the table to query
    table_name: String,
    /// Aggregate functions to be used in the query
    /// (function_name, is_distinct)
    aggregate_functions: Vec<(String, bool)>,
    /// Columns to be used in group by
    group_by_columns: Vec<String>,
    /// Possible columns for arguments in the aggregate functions
    ///
    /// Assumes each
    arguments: Vec<String>,
}
impl QueryBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// return the table name if any
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Set the table name for the query builder
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Add a new possible aggregate function to the query builder
    pub fn with_aggregate_function(
        mut self,
        aggregate_function: impl Into<String>,
    ) -> Self {
        self.aggregate_functions
            .push((aggregate_function.into(), false));
        self
    }

    /// Add a new possible `DISTINCT` aggregate function to the query
    ///
    /// This is different than `with_aggregate_function` because only certain
    /// aggregates support `DISTINCT`
    pub fn with_distinct_aggregate_function(
        mut self,
        aggregate_function: impl Into<String>,
    ) -> Self {
        self.aggregate_functions
            .push((aggregate_function.into(), true));
        self
    }

    /// Set the columns to be used in the group bys clauses
    pub fn set_group_by_columns<'a>(
        mut self,
        group_by: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        self.group_by_columns = group_by.into_iter().map(String::from).collect();
        self
    }

    /// Add one or more columns to be used as an argument in the aggregate functions
    pub fn with_aggregate_arguments<'a>(
        mut self,
        arguments: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let arguments = arguments.into_iter().map(String::from);
        self.arguments.extend(arguments);
        self
    }

    pub fn generate_query(&self) -> String {
        let group_by = self.random_group_by();
        let mut query = String::from("SELECT ");
        query.push_str(&group_by.join(", "));
        if !group_by.is_empty() {
            query.push_str(", ");
        }
        query.push_str(&self.random_aggregate_functions(&group_by).join(", "));
        query.push_str(" FROM ");
        query.push_str(&self.table_name);
        if !group_by.is_empty() {
            query.push_str(" GROUP BY ");
            query.push_str(&group_by.join(", "));
        }
        query
    }

    /// Generate a some random aggregate function invocations (potentially repeating).
    ///
    /// Each aggregate function invocation is of the form
    ///
    /// ```sql
    /// function_name(<DISTINCT> argument) as alias
    /// ```
    ///
    /// where
    /// * `function_names` are randomly selected from [`Self::aggregate_functions`]
    /// * `<DISTINCT> argument` is randomly selected from [`Self::arguments`]
    /// * `alias` is a unique alias `colN` for the column (to avoid duplicate column names)
    fn random_aggregate_functions(&self, group_by_cols: &[String]) -> Vec<String> {
        const MAX_NUM_FUNCTIONS: usize = 5;
        let mut rng = thread_rng();
        let num_aggregate_functions = rng.gen_range(1..MAX_NUM_FUNCTIONS);

        let mut alias_gen = 1;

        let mut aggregate_functions = vec![];

        let mut order_by_black_list: HashSet<String> =
            group_by_cols.iter().cloned().collect();
        // remove one random col
        if let Some(first) = order_by_black_list.iter().next().cloned() {
            order_by_black_list.remove(&first);
        }

        while aggregate_functions.len() < num_aggregate_functions {
            let idx = rng.gen_range(0..self.aggregate_functions.len());
            let (function_name, is_distinct) = &self.aggregate_functions[idx];
            let argument = self.random_argument();
            let alias = format!("col{}", alias_gen);
            let distinct = if *is_distinct { "DISTINCT " } else { "" };
            alias_gen += 1;

            let (order_by, null_opt) = if function_name.eq("first_value") {
                (
                    self.order_by(&order_by_black_list), /* Among the order by columns, at most one group by column can be included to avoid all order by column values being identical */
                    self.null_opt(),
                )
            } else {
                ("".to_string(), "".to_string())
            };

            let function = format!(
                "{function_name}({distinct}{argument}{order_by}) {null_opt} as {alias}"
            );
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

    fn order_by(&self, black_list: &HashSet<String>) -> String {
        let mut available_columns: Vec<String> = self
            .arguments
            .iter()
            .filter(|col| !black_list.contains(*col))
            .cloned()
            .collect();

        available_columns.shuffle(&mut thread_rng());

        let num_of_order_by_col = 12;
        let column_count = std::cmp::min(num_of_order_by_col, available_columns.len());

        let selected_columns = &available_columns[0..column_count];

        let mut rng = thread_rng();
        let mut result = String::from_str(" order by ").unwrap();
        for col in selected_columns {
            let order = if rng.gen_bool(0.5) { "ASC" } else { "DESC" };
            result.push_str(&format!("{} {},", col, order));
        }

        result.strip_suffix(",").unwrap().to_string()
    }

    fn null_opt(&self) -> String {
        if thread_rng().gen_bool(0.5) {
            "RESPECT NULLS".to_string()
        } else {
            "IGNORE NULLS".to_string()
        }
    }

    /// Pick a random number of fields to group by (non-repeating)
    ///
    /// Limited to 3 group by columns to ensure coverage for large groups. With
    /// larger numbers of columns, each group has many fewer values.
    fn random_group_by(&self) -> Vec<String> {
        let mut rng = thread_rng();
        const MAX_GROUPS: usize = 3;
        let max_groups = self.group_by_columns.len().max(MAX_GROUPS);
        let num_group_by = rng.gen_range(1..max_groups);

        let mut already_used = HashSet::new();
        let mut group_by = vec![];
        while group_by.len() < num_group_by
            && already_used.len() != self.group_by_columns.len()
        {
            let idx = rng.gen_range(0..self.group_by_columns.len());
            if already_used.insert(idx) {
                group_by.push(self.group_by_columns[idx].clone());
            }
        }
        group_by
    }
}
