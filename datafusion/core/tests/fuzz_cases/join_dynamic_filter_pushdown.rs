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

use std::sync::{Arc, LazyLock};

use arrow::array::{Int32Array, StringArray, StringDictionaryBuilder};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::ParquetFormat;
use datafusion_execution::object_store::ObjectStoreUrl;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Clone)]
struct TestDataSet {
    build_store: Arc<dyn ObjectStore>,
    build_schema: Arc<Schema>,
    probe_store: Arc<dyn ObjectStore>,
    probe_schema: Arc<Schema>,
}

/// List of in memory parquet files with UTF8 data for join testing
// Use a mutex rather than LazyLock to allow for async initialization
static TESTFILES: LazyLock<Mutex<Vec<TestDataSet>>> =
    LazyLock::new(|| Mutex::new(vec![]));

async fn test_files() -> Vec<TestDataSet> {
    let files_mutex = &TESTFILES;
    let mut files = files_mutex.lock().await;
    if !files.is_empty() {
        return (*files).clone();
    }

    let mut rng = StdRng::seed_from_u64(0);

    for nulls_in_ids in [false, true] {
        for nulls_in_names in [false, true] {
            for nulls_in_departments in [false, true] {
                let build_store = Arc::new(InMemory::new());
                let probe_store = Arc::new(InMemory::new());

                let build_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, nulls_in_ids),
                    Field::new("name", DataType::Utf8, nulls_in_names),
                    Field::new(
                        "department",
                        DataType::Dictionary(
                            Box::new(DataType::Int32),
                            Box::new(DataType::Utf8),
                        ),
                        nulls_in_departments,
                    ),
                    Field::new("build_extra", DataType::Int32, false),
                ]));

                let probe_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, nulls_in_ids),
                    Field::new("name", DataType::Utf8, nulls_in_names),
                    Field::new(
                        "department",
                        DataType::Dictionary(
                            Box::new(DataType::Int32),
                            Box::new(DataType::Utf8),
                        ),
                        nulls_in_departments,
                    ),
                    Field::new("probe_extra", DataType::Int32, false),
                ]));

                let name_choices = if nulls_in_names {
                    [Some("Alice"), Some("Bob"), None, Some("David"), None]
                } else {
                    [
                        Some("Alice"),
                        Some("Bob"),
                        Some("Charlie"),
                        Some("David"),
                        Some("Eve"),
                    ]
                };

                let department_choices = if nulls_in_departments {
                    [
                        Some("Theater"),
                        Some("Engineering"),
                        None,
                        Some("Arts"),
                        None,
                    ]
                } else {
                    [
                        Some("Theater"),
                        Some("Engineering"),
                        Some("Healthcare"),
                        Some("Arts"),
                        Some("Music"),
                    ]
                };

                // Generate build side files (fewer unique values for better filtering)
                for i in 0..3 {
                    let num_batches = rng.random_range(1..3);
                    let mut batches = Vec::with_capacity(num_batches);
                    for _ in 0..num_batches {
                        let num_rows = 10;
                        let ids = Int32Array::from_iter((0..num_rows).map(|_| {
                            if nulls_in_ids {
                                if rng.random_bool(1.0 / 10.0) {
                                    None
                                } else {
                                    Some(rng.random_range(0..3)) // Limited range for build side
                                }
                            } else {
                                Some(rng.random_range(0..3)) // Limited range for build side
                            }
                        }));
                        let names = StringArray::from_iter((0..num_rows).map(|_| {
                            let idx = rng.random_range(0..name_choices.len());
                            name_choices[idx].map(|s| s.to_string())
                        }));
                        let mut departments = StringDictionaryBuilder::<Int32Type>::new();
                        for _ in 0..num_rows {
                            let idx = rng.random_range(0..department_choices.len());
                            departments.append_option(department_choices[idx].as_ref());
                        }
                        let build_extra = Int32Array::from_iter(
                            (0..num_rows).map(|_| Some(rng.random_range(100..200))),
                        );

                        let batch = RecordBatch::try_new(
                            build_schema.clone(),
                            vec![
                                Arc::new(ids),
                                Arc::new(names),
                                Arc::new(departments.finish()),
                                Arc::new(build_extra),
                            ],
                        )
                        .unwrap();
                        batches.push(batch);
                    }
                    let mut buf = vec![];
                    {
                        let mut writer =
                            ArrowWriter::try_new(&mut buf, build_schema.clone(), None)
                                .unwrap();
                        for batch in batches {
                            writer.write(&batch).unwrap();
                            writer.flush().unwrap();
                        }
                        writer.flush().unwrap();
                        writer.finish().unwrap();
                    }
                    let payload = PutPayload::from(buf);
                    let path = Path::from(format!("build_file_{i}.parquet"));
                    build_store.put(&path, payload).await.unwrap();
                }

                // Generate probe side files (more unique values)
                for i in 0..5 {
                    let num_batches = rng.random_range(1..3);
                    let mut batches = Vec::with_capacity(num_batches);
                    for _ in 0..num_batches {
                        let num_rows = 25;
                        let ids = Int32Array::from_iter((0..num_rows).map(|_| {
                            if nulls_in_ids {
                                if rng.random_bool(1.0 / 10.0) {
                                    None
                                } else {
                                    Some(rng.random_range(0..10)) // Wider range for probe side
                                }
                            } else {
                                Some(rng.random_range(0..10)) // Wider range for probe side
                            }
                        }));
                        let names = StringArray::from_iter((0..num_rows).map(|_| {
                            let idx = rng.random_range(0..name_choices.len());
                            name_choices[idx].map(|s| s.to_string())
                        }));
                        let mut departments = StringDictionaryBuilder::<Int32Type>::new();
                        for _ in 0..num_rows {
                            let idx = rng.random_range(0..department_choices.len());
                            departments.append_option(department_choices[idx].as_ref());
                        }
                        let probe_extra = Int32Array::from_iter(
                            (0..num_rows).map(|_| Some(rng.random_range(200..300))),
                        );

                        let batch = RecordBatch::try_new(
                            probe_schema.clone(),
                            vec![
                                Arc::new(ids),
                                Arc::new(names),
                                Arc::new(departments.finish()),
                                Arc::new(probe_extra),
                            ],
                        )
                        .unwrap();
                        batches.push(batch);
                    }
                    let mut buf = vec![];
                    {
                        let mut writer =
                            ArrowWriter::try_new(&mut buf, probe_schema.clone(), None)
                                .unwrap();
                        for batch in batches {
                            writer.write(&batch).unwrap();
                            writer.flush().unwrap();
                        }
                        writer.flush().unwrap();
                        writer.finish().unwrap();
                    }
                    let payload = PutPayload::from(buf);
                    let path = Path::from(format!("probe_file_{i}.parquet"));
                    probe_store.put(&path, payload).await.unwrap();
                }
                files.push(TestDataSet {
                    build_store,
                    build_schema,
                    probe_store,
                    probe_schema,
                });
            }
        }
    }
    (*files).clone()
}

struct RunResult {
    results: Vec<RecordBatch>,
    explain_plan: String,
}

async fn run_query_with_config(
    query: &str,
    config: SessionConfig,
    dataset: TestDataSet,
) -> RunResult {
    let build_store = dataset.build_store;
    let build_schema = dataset.build_schema;
    let probe_store = dataset.probe_store;
    let probe_schema = dataset.probe_schema;

    let ctx = SessionContext::new_with_config(config);

    // Register build side table
    let build_url = ObjectStoreUrl::parse("memory://build/").unwrap();
    ctx.register_object_store(build_url.as_ref(), build_store.clone());
    let format = Arc::new(
        ParquetFormat::default()
            .with_options(ctx.state().table_options().parquet.clone()),
    );
    let options = ListingOptions::new(format.clone());
    let table_path = ListingTableUrl::parse("memory://build/").unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(build_schema);
    let table = Arc::new(ListingTable::try_new(config).unwrap());
    ctx.register_table("build_table", table).unwrap();

    // Register probe side table
    let probe_url = ObjectStoreUrl::parse("memory://probe/").unwrap();
    ctx.register_object_store(probe_url.as_ref(), probe_store.clone());
    let options = ListingOptions::new(format);
    let table_path = ListingTableUrl::parse("memory://probe/").unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(probe_schema);
    let table = Arc::new(ListingTable::try_new(config).unwrap());
    ctx.register_table("probe_table", table).unwrap();

    let results = ctx.sql(query).await.unwrap().collect().await.unwrap();
    let explain_batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {query}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let explain_plan = pretty_format_batches(&explain_batches).unwrap().to_string();
    RunResult {
        results,
        explain_plan,
    }
}

#[derive(Debug)]
struct RunQueryResult {
    query: String,
    result: Vec<RecordBatch>,
    expected: Vec<RecordBatch>,
}

impl RunQueryResult {
    fn expected_formated(&self) -> String {
        format!("{}", pretty_format_batches(&self.expected).unwrap())
    }

    fn result_formated(&self) -> String {
        format!("{}", pretty_format_batches(&self.result).unwrap())
    }

    fn is_ok(&self) -> bool {
        // For comparison, sort the lines since order may vary between runs
        let expected_formatted = self.expected_formated();
        let result_formatted = self.result_formated();
        let mut expected_lines: Vec<&str> = expected_formatted.trim().lines().collect();
        let mut result_lines: Vec<&str> = result_formatted.trim().lines().collect();
        expected_lines.sort_unstable();
        result_lines.sort_unstable();
        expected_lines == result_lines
    }
}

/// Iterate over each line in the plan and check that one of them has `DataSourceExec` and `DynamicFilterPhysicalExpr` in the same line.
fn has_dynamic_filter_expr_pushdown(plan: &str) -> bool {
    for line in plan.lines() {
        if line.contains("DataSourceExec") && line.contains("DynamicFilterPhysicalExpr") {
            return true;
        }
    }
    false
}

async fn run_query(
    query: String,
    cfg: SessionConfig,
    dataset: TestDataSet,
) -> RunQueryResult {
    let cfg_with_dynamic_filters = cfg
        .clone()
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true);
    let cfg_without_dynamic_filters = cfg
        .clone()
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", false);

    let expected_result =
        run_query_with_config(&query, cfg_without_dynamic_filters, dataset.clone()).await;
    let result =
        run_query_with_config(&query, cfg_with_dynamic_filters, dataset.clone()).await;

    // Check that dynamic filters were actually pushed down for hash joins (only for INNER JOINs)
    if query.contains("INNER JOIN")
        && !has_dynamic_filter_expr_pushdown(&result.explain_plan)
    {
        panic!(
            "Dynamic filter was not pushed down in query: {query}\n\n{}",
            result.explain_plan
        );
    }

    RunQueryResult {
        query: query.to_string(),
        result: result.results,
        expected: expected_result.results,
    }
}

struct TestCase {
    query: String,
    cfg: SessionConfig,
    dataset: TestDataSet,
}

#[tokio::test]
async fn test_join_dynamic_filter_pushdown_smoke() {
    // Simple smoke test with one dataset and one query
    let datasets = test_files().await;
    let dataset = &datasets[0];

    let query = "SELECT build_table.id, build_table.name, probe_table.probe_extra \
                FROM build_table INNER JOIN probe_table ON build_table.id = probe_table.id";
    let cfg = SessionConfig::new();

    let result = run_query(query.to_string(), cfg, dataset.clone()).await;

    if !result.is_ok() {
        println!("Query:\n{}", result.query);
        println!("\nExpected:\n{}", result.expected_formated());
        println!("\nResult:\n{}", result.result_formated());
        panic!("Smoke test failed");
    }

    println!("Smoke test passed!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuzz_join_dynamic_filter_pushdown() {
    let join_conditions = ["ON build_table.id = probe_table.id"];

    let start = datafusion_common::instant::Instant::now();
    let mut queries = vec![];

    for join_type in ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN"] {
        for condition in &join_conditions {
            // Basic join query with ORDER BY to ensure deterministic results
            let query = format!(
                "SELECT build_table.id, probe_table.probe_extra \
                FROM build_table {join_type} probe_table {condition} \
                ORDER BY id, probe_table.probe_extra",
            );
            queries.push(query);

            // Aggregation query (should be deterministic)
            let query = format!(
                "SELECT COUNT(*) as cnt \
                FROM build_table {join_type} probe_table {condition}",
            );
            queries.push(query);
        }
    }
    // Left semi join
    for condition in &join_conditions {
        let query = format!(
            "SELECT build_table.id, build_table.name \
            FROM build_table LEFT SEMI JOIN probe_table {condition} \
            ORDER BY build_table.id, build_table.name",
        );
        queries.push(query);
    }
    // Right semi join
    for condition in &join_conditions {
        let query = format!(
            "SELECT probe_table.id, probe_table.name \
            FROM build_table RIGHT SEMI JOIN probe_table {condition} \
            ORDER BY probe_table.id, probe_table.name",
        );
        queries.push(query);
    }
    // Left anti join
    for condition in &join_conditions {
        let query = format!(
            "SELECT build_table.id, build_table.name \
            FROM build_table LEFT ANTI JOIN probe_table {condition} \
            ORDER BY build_table.id, build_table.name",
        );
        queries.push(query);
    }
    // Right anti join
    for condition in &join_conditions {
        let query = format!(
            "SELECT probe_table.id, probe_table.name \
            FROM build_table RIGHT ANTI JOIN probe_table {condition} \
            ORDER BY probe_table.id, probe_table.name",
        );
        queries.push(query);
    }
    // Full outer join
    for condition in &join_conditions {
        let query = format!(
            "SELECT build_table.id, probe_table.id, build_table.name, probe_table.probe_extra \
            FROM build_table FULL OUTER JOIN probe_table {condition} \
            ORDER BY build_table.id, probe_table.id, build_table.name, probe_table.probe_extra",
        );
        queries.push(query);
    }

    queries.sort_unstable();
    queries.dedup(); // Remove duplicates

    println!(
        "Generated {} unique join queries in {:?}",
        queries.len(),
        start.elapsed()
    );

    let start = datafusion_common::instant::Instant::now();
    let datasets = test_files().await;
    println!("Generated test files in {:?}", start.elapsed());

    let mut test_cases = vec![];
    for query in &queries {
        for dataset in &datasets {
            let cfg = SessionConfig::new();
            test_cases.push(TestCase {
                query: query.to_string(),
                cfg,
                dataset: dataset.clone(),
            });
        }
    }

    let start = datafusion_common::instant::Instant::now();
    let mut join_set = JoinSet::new();
    for tc in test_cases {
        join_set.spawn(run_query(tc.query, tc.cfg, tc.dataset));
    }
    let mut results = join_set.join_all().await;
    results.sort_unstable_by(|a, b| a.query.cmp(&b.query));
    println!("Ran {} test cases in {:?}", results.len(), start.elapsed());

    let failures = results
        .iter()
        .filter(|result| !result.is_ok())
        .collect::<Vec<_>>();

    for failure in &failures {
        println!("Failure:");
        println!("Query:\n{}", failure.query);
        println!("\nExpected:\n{}", failure.expected_formated());
        println!("\nResult:\n{}", failure.result_formated());
        println!("\n\n");
    }

    if !failures.is_empty() {
        panic!("Some test cases failed");
    } else {
        println!("All test cases passed");
    }
}
