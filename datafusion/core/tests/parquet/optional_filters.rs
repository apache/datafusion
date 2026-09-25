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

//! End-to-end tests: the dynamic filters of the hash join, TopK and
//! aggregate operators arrive at the Parquet scan as *optional* conjuncts of
//! its [`PhysicalFilter`], and the query results do not change.
//!
//! [`PhysicalFilter`]: datafusion_physical_expr::filter::PhysicalFilter

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_datasource::file_scan_config::FileScanConfig;
use insta::assert_snapshot;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

/// Write `columns` as a single-file Parquet table `name` and register it.
async fn register_table(
    ctx: &SessionContext,
    dir: &TempDir,
    name: &str,
    columns: Vec<(&str, Vec<i64>)>,
) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays = columns
        .into_iter()
        .map(|(_, values)| Arc::new(Int64Array::from(values)) as _)
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
    let path = dir.path().join(format!("{name}.parquet"));
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    ctx.register_parquet(name, path.to_str().unwrap(), ParquetReadOptions::default())
        .await
        .unwrap();
}

/// A context with a small `build` table (`k` in 1..=3) and a larger `probe`
/// table (`k` in 0..100, `v = 10 * k`).
async fn context() -> (SessionContext, TempDir) {
    let config = SessionConfig::new()
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .with_target_partitions(2);
    let ctx = SessionContext::new_with_config(config);
    let dir = TempDir::new().unwrap();
    register_table(&ctx, &dir, "build", vec![("k", vec![1, 2, 3])]).await;
    let k: Vec<i64> = (0..100).collect();
    let v = k.iter().map(|k| k * 10).collect();
    register_table(&ctx, &dir, "probe", vec![("k", k), ("v", v)]).await;
    (ctx, dir)
}

/// The conjuncts of the filter of each Parquet scan in `plan`, with their
/// optional flag.
fn scan_conjuncts(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<(String, bool)>> {
    let mut scans = vec![];
    plan.apply(|node| {
        if let Some(source) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
            .and_then(|config| config.file_source().downcast_ref::<ParquetSource>())
        {
            scans.push(
                source
                    .physical_filter()
                    .conjuncts()
                    .iter()
                    .map(|c| (c.to_string(), c.is_optional()))
                    .collect(),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    scans
}

/// Plan and run `sql`. Return the conjuncts of each Parquet scan and the
/// formatted result.
async fn run(sql: &str) -> (Vec<Vec<(String, bool)>>, String) {
    let (ctx, _dir) = context().await;
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let conjuncts = scan_conjuncts(&plan);
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    (
        conjuncts,
        pretty_format_batches(&batches).unwrap().to_string(),
    )
}

fn is_dynamic_filter(conjunct: &(String, bool)) -> bool {
    conjunct.0.starts_with("DynamicFilter")
}

#[tokio::test]
async fn hash_join_dynamic_filter_arrives_optional() {
    let (scans, result) = run("SELECT p.k, p.v FROM build b JOIN probe p ON b.k = p.k \
         WHERE p.v > 10 ORDER BY p.k")
    .await;
    // The build side scan has no filter. The probe side scan has the
    // required user filter and the optional dynamic filter of the join.
    let probe = scans
        .iter()
        .find(|conjuncts| conjuncts.iter().any(is_dynamic_filter))
        .expect("a scan with the dynamic filter of the join");
    assert!(
        probe
            .iter()
            .all(|conjunct| conjunct.1 == is_dynamic_filter(conjunct)),
        "only the dynamic filter is optional: {probe:?}"
    );
    assert!(
        probe
            .iter()
            .any(|(expr, optional)| expr == "v@1 > 10" && !optional)
    );
    assert_snapshot!(result, @r"
    +---+----+
    | k | v  |
    +---+----+
    | 2 | 20 |
    | 3 | 30 |
    +---+----+
    ");
}

#[tokio::test]
async fn topk_dynamic_filter_arrives_optional() {
    let (scans, result) = run("SELECT v FROM probe ORDER BY v DESC LIMIT 2").await;
    assert_eq!(scans.len(), 1);
    let [conjunct] = scans[0].as_slice() else {
        panic!("expected one conjunct: {scans:?}");
    };
    assert!(is_dynamic_filter(conjunct) && conjunct.1, "{conjunct:?}");
    assert_snapshot!(result, @r"
    +-----+
    | v   |
    +-----+
    | 990 |
    | 980 |
    +-----+
    ");
}

#[tokio::test]
async fn aggregate_dynamic_filter_arrives_optional() {
    // The `WHERE` clause prevents an answer from the file statistics.
    let (scans, result) = run("SELECT max(v) FROM probe WHERE k > 5").await;
    assert_eq!(scans.len(), 1);
    let [required, optional] = scans[0].as_slice() else {
        panic!("expected two conjuncts: {scans:?}");
    };
    assert_eq!(required, &("k@0 > 5".to_string(), false));
    assert!(is_dynamic_filter(optional) && optional.1, "{optional:?}");
    assert_snapshot!(result, @r"
    +--------------+
    | max(probe.v) |
    +--------------+
    | 990          |
    +--------------+
    ");
}
