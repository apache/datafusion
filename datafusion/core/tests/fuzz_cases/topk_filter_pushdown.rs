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

use std::collections::HashMap;
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
use itertools::Itertools;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Clone)]
struct TestDataSet {
    store: Arc<dyn ObjectStore>,
    schema: Arc<Schema>,
}

/// List of in memory parquet files with UTF8 data
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
                let store = Arc::new(InMemory::new());

                let schema = Arc::new(Schema::new(vec![
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

                // Generate 5 files, some with overlapping or repeated ids some without
                for i in 0..5 {
                    let num_batches = rng.random_range(1..3);
                    let mut batches = Vec::with_capacity(num_batches);
                    for _ in 0..num_batches {
                        let num_rows = 25;
                        let ids = Int32Array::from_iter((0..num_rows).map(|file| {
                            if nulls_in_ids {
                                if rng.random_bool(1.0 / 10.0) {
                                    None
                                } else {
                                    Some(rng.random_range(file..file + 5))
                                }
                            } else {
                                Some(rng.random_range(file..file + 5))
                            }
                        }));
                        let names = StringArray::from_iter((0..num_rows).map(|_| {
                            // randomly select a name
                            let idx = rng.random_range(0..name_choices.len());
                            name_choices[idx].map(|s| s.to_string())
                        }));
                        let mut departments = StringDictionaryBuilder::<Int32Type>::new();
                        for _ in 0..num_rows {
                            // randomly select a department
                            let idx = rng.random_range(0..department_choices.len());
                            departments.append_option(department_choices[idx].as_ref());
                        }
                        let batch = RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                Arc::new(ids),
                                Arc::new(names),
                                Arc::new(departments.finish()),
                            ],
                        )
                        .unwrap();
                        batches.push(batch);
                    }
                    let mut buf = vec![];
                    {
                        let mut writer =
                            ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
                        for batch in batches {
                            writer.write(&batch).unwrap();
                            writer.flush().unwrap();
                        }
                        writer.flush().unwrap();
                        writer.finish().unwrap();
                    }
                    let payload = PutPayload::from(buf);
                    let path = Path::from(format!("file_{i}.parquet"));
                    store.put(&path, payload).await.unwrap();
                }
                files.push(TestDataSet { store, schema });
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
    let store = dataset.store;
    let schema = dataset.schema;
    let ctx = SessionContext::new_with_config(config);
    let url = ObjectStoreUrl::parse("memory://").unwrap();
    ctx.register_object_store(url.as_ref(), store.clone());

    let format = Arc::new(
        ParquetFormat::default()
            .with_options(ctx.state().table_options().parquet.clone()),
    );
    let options = ListingOptions::new(format);
    let table_path = ListingTableUrl::parse("memory:///").unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema);
    let table = Arc::new(ListingTable::try_new(config).unwrap());

    ctx.register_table("test_table", table).unwrap();

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
    fn expected_formatted(&self) -> String {
        format!("{}", pretty_format_batches(&self.expected).unwrap())
    }

    fn result_formatted(&self) -> String {
        format!("{}", pretty_format_batches(&self.result).unwrap())
    }

    fn is_ok(&self) -> bool {
        self.expected_formatted() == self.result_formatted()
    }
}

/// Iterate over each line in the plan and check that one of them has `DataSourceExec` and `DynamicFilter` in the same line.
fn has_dynamic_filter_expr_pushdown(plan: &str) -> bool {
    for line in plan.lines() {
        if line.contains("DataSourceExec") && line.contains("DynamicFilter") {
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
    // Check that dynamic filters were actually pushed down
    if !has_dynamic_filter_expr_pushdown(&result.explain_plan) {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_fuzz_topk_filter_pushdown() {
    let order_columns = ["id", "name", "department"];
    let order_directions = ["ASC", "DESC"];
    let null_orders = ["NULLS FIRST", "NULLS LAST"];

    let start = datafusion_common::instant::Instant::now();
    let mut orders: HashMap<String, Vec<String>> = HashMap::new();
    for order_column in &order_columns {
        for order_direction in &order_directions {
            for null_order in &null_orders {
                // if there is a vec for this column insert the order, otherwise create a new vec
                let ordering = format!("{order_column} {order_direction} {null_order}");
                match orders.get_mut(*order_column) {
                    Some(order_vec) => {
                        order_vec.push(ordering);
                    }
                    None => {
                        orders.insert((*order_column).to_string(), vec![ordering]);
                    }
                }
            }
        }
    }

    let mut queries = vec![];

    for limit in [1, 10] {
        for num_order_by_columns in [1, 2, 3] {
            for order_columns in ["id", "name", "department"]
                .iter()
                .combinations(num_order_by_columns)
            {
                for orderings in order_columns
                    .iter()
                    .map(|col| orders.get(**col).unwrap())
                    .multi_cartesian_product()
                {
                    let query = format!(
                        "SELECT * FROM test_table ORDER BY {} LIMIT {}",
                        orderings.into_iter().join(", "),
                        limit
                    );
                    queries.push(query);
                }
            }
        }
    }

    queries.sort_unstable();
    println!(
        "Generated {} queries in {:?}",
        queries.len(),
        start.elapsed()
    );

    let start = datafusion_common::instant::Instant::now();
    let datasets = test_files().await;
    println!("Generated test files in {:?}", start.elapsed());

    let mut test_cases = vec![];
    for enable_filter_pushdown in [true, false] {
        for query in &queries {
            for dataset in &datasets {
                let mut cfg = SessionConfig::new();
                cfg = cfg.set_bool(
                    "datafusion.optimizer.enable_dynamic_filter_pushdown",
                    enable_filter_pushdown,
                );
                test_cases.push(TestCase {
                    query: query.to_string(),
                    cfg,
                    dataset: dataset.clone(),
                });
            }
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
        println!("\nExpected:\n{}", failure.expected_formatted());
        println!("\nResult:\n{}", failure.result_formatted());
        println!("\n\n");
    }

    if !failures.is_empty() {
        panic!("Some test cases failed");
    } else {
        println!("All test cases passed");
    }
}
