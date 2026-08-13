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

//! Benchmarks Parquet RowFilter evaluation when row-group statistics prove
//! that almost every surviving row group fully matches the predicate.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ROW_GROUP_ROWS: usize = 65_536;
const ROW_GROUPS: usize = 40;
const TOTAL_ROWS: usize = ROW_GROUP_ROWS * ROW_GROUPS;
const LOWER_ITEM_SK: i32 = (ROW_GROUP_ROWS / 2) as i32;
const UPPER_ITEM_SK: i32 = (TOTAL_ROWS - ROW_GROUP_ROWS / 2) as i32;
const EXPECTED_ROWS: usize = TOTAL_ROWS - ROW_GROUP_ROWS;
const EXPECTED_ROW_FILTER_MATCHES: usize = ROW_GROUP_ROWS;

struct BenchmarkDataset {
    _tempdir: TempDir,
    file_path: PathBuf,
}

impl BenchmarkDataset {
    fn path(&self) -> &Path {
        &self.file_path
    }
}

static DATASET: LazyLock<BenchmarkDataset> = LazyLock::new(|| {
    create_dataset().expect("failed to prepare fully-matched benchmark dataset")
});

fn sql() -> String {
    format!(
        "SELECT SUM(payload) FROM t \
         WHERE item_sk >= {LOWER_ITEM_SK} AND item_sk < {UPPER_ITEM_SK} \
           AND warehouse_sk >= 1 AND warehouse_sk <= 10 \
           AND sold_date_sk >= 2450905 AND sold_date_sk <= 2450934"
    )
}

fn create_context(path: &Path, rt: &Runtime) -> SessionContext {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.reorder_filters = true;
    config.options_mut().execution.parquet.pruning = true;

    let ctx = SessionContext::new_with_config(config);
    rt.block_on(ctx.register_parquet(
        "t",
        path.to_str().expect("UTF-8 benchmark path"),
        ParquetReadOptions::default(),
    ))
    .expect("register parquet benchmark table");
    ctx
}

fn metric_sum(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let own = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map(|value| value.as_usize())
        .unwrap_or(0);
    own + plan
        .children()
        .into_iter()
        .map(|child| metric_sum(&child, name))
        .sum::<usize>()
}

fn assert_fully_matched_groups_bypass_row_filter(
    ctx: &SessionContext,
    rt: &Runtime,
    query: &str,
) {
    let (sum, row_filter_matches) = rt
        .block_on(async {
            let plan = ctx.sql(query).await?.create_physical_plan().await?;
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            let sum = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("SUM(Int64) result")
                .value(0);
            Ok::<_, datafusion_common::DataFusionError>((
                sum,
                metric_sum(&plan, "pushdown_rows_matched"),
            ))
        })
        .expect("execute fully-matched benchmark probe");

    assert_eq!(sum, EXPECTED_ROWS as i64);
    assert_eq!(
        row_filter_matches, EXPECTED_ROW_FILTER_MATCHES,
        "38 fully-matched row groups should bypass RowFilter; only the two \
         half-matched boundary groups should contribute matched-row metrics"
    );
}

fn run_query(ctx: &SessionContext, rt: &Runtime, query: &str) {
    let df = rt.block_on(ctx.sql(query)).expect("plan benchmark query");
    black_box(rt.block_on(df.collect()).expect("execute benchmark query"));
}

fn parquet_fully_matched_row_filter(c: &mut Criterion) {
    let rt = Runtime::new().expect("create benchmark runtime");
    let ctx = create_context(DATASET.path(), &rt);
    let query = sql();

    assert_fully_matched_groups_bypass_row_filter(&ctx, &rt, &query);

    let mut group = c.benchmark_group("parquet_fully_matched_row_filter");
    group.throughput(Throughput::Elements(TOTAL_ROWS as u64));
    group.bench_function("q39_like_three_integer_bounds", |b| {
        b.iter(|| run_query(&ctx, &rt, &query));
    });
    group.finish();
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("fully_matched_row_filter.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("item_sk", DataType::Int32, false),
        Field::new("warehouse_sk", DataType::Int32, false),
        Field::new("sold_date_sk", DataType::Int32, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file_path)?,
        Arc::clone(&schema),
        Some(properties),
    )?;

    for row_group in 0..ROW_GROUPS {
        let start = row_group * ROW_GROUP_ROWS;
        let item_sk = Int32Array::from_iter_values(
            (start..start + ROW_GROUP_ROWS).map(|value| value as i32),
        );
        let warehouse_sk = Int32Array::from_iter_values(
            (0..ROW_GROUP_ROWS).map(|value| (value % 10 + 1) as i32),
        );
        let sold_date_sk = Int32Array::from_iter_values(
            (0..ROW_GROUP_ROWS).map(|value| 2_450_905 + (value % 30) as i32),
        );
        let payload = Int64Array::from_value(1, ROW_GROUP_ROWS);
        writer.write(&RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(item_sk),
                Arc::new(warehouse_sk),
                Arc::new(sold_date_sk),
                Arc::new(payload),
            ],
        )?)?;
        writer.flush()?;
    }

    let metadata = writer.close()?;
    assert_eq!(metadata.row_groups().len(), ROW_GROUPS);

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
    })
}

criterion_group!(benches, parquet_fully_matched_row_filter);
criterion_main!(benches);
