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

//! Joins between two sources with identical, source-declared range partitions.
//!
//! Run with `cargo bench -p datafusion --bench co_partitioned_join`.
//! Data generation is outside the timed region. Each iteration plans and executes
//! a SQL join, including building the dynamic filter and scanning both sources.
//! Vary partition count (CASE routing cost) and probe selectivity. One partition
//! is a control: the existing lowering already elides CASE for that workload.
//! Prints an executed plan with metrics and separately reports median predicate
//! evaluation time, summed across scan partitions, before timing query latency.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::catalog::{Session, TableProvider};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Result, ScalarValue};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{
    Partitioning, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion_physical_plan::display::DisplayableExecutionPlan;
use datafusion_physical_plan::{ExecutionPlan, displayable};
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

const PROBE_ROWS: usize = 1 << 20;

#[derive(Debug)]
struct RangeTable {
    schema: SchemaRef,
    scan: FileScanConfig,
}

#[async_trait]
impl TableProvider for RangeTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The single key is required by the join, even for COUNT(*).
        assert!(projection.is_none_or(|p| p == [0]));
        let mut scan = self.scan.clone();
        // Each execution needs fresh metrics; cloning the template source would
        // accumulate counters and metric registrations across benchmark iterations.
        scan.file_source = Arc::new(
            ParquetSource::new(Arc::clone(&self.schema)).with_pushdown_filters(true),
        );
        Ok(DataSourceExec::from_data_source(scan))
    }
}

fn table(dir: &TempDir, name: &str, partitions: usize, stride: usize) -> RangeTable {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let rows_per_partition = PROBE_ROWS / partitions;
    let mut groups = Vec::new();
    for partition in 0..partitions {
        let path = dir.path().join(format!("{name}-{partition}.parquet"));
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            Arc::clone(&schema),
            Some(
                WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )
        .unwrap();
        // Permute keys within each range so bounds cannot prune entire row groups.
        let keys = (0..rows_per_partition)
            .map(|i| (i * 8191) % rows_per_partition)
            .filter(|key| key % stride == 0)
            .map(|key| (partition * rows_per_partition + key) as i64)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(keys))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut file = PartitionedFile::new("", std::fs::metadata(&path).unwrap().len());
        file.object_meta.location = ObjectPath::from_absolute_path(&path).unwrap();
        groups.push(FileGroup::new(vec![file]));
    }
    let partitioning = Partitioning::Range(
        RangePartitioning::try_new(
            [PhysicalSortExpr::new_default(col("k", &schema).unwrap())].into(),
            (1..partitions)
                .map(|p| {
                    SplitPoint::new(vec![ScalarValue::Int64(Some(
                        (p * rows_per_partition) as i64,
                    ))])
                })
                .collect(),
        )
        .unwrap(),
    );
    let source = ParquetSource::new(Arc::clone(&schema)).with_pushdown_filters(true);
    let scan =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file_groups(groups)
            .with_output_partitioning(Some(partitioning))
            .build();
    RangeTable { schema, scan }
}

fn benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("co_partitioned_join");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(PROBE_ROWS as u64));
    for (partitions, stride) in [(1, 100), (4, 100), (16, 100), (16, 10), (16, 1)] {
        let dir = TempDir::new().unwrap();
        let mut config = SessionConfig::new()
            .with_target_partitions(partitions)
            .with_batch_size(8192);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("build", Arc::new(table(&dir, "build", partitions, stride)))
            .unwrap();
        ctx.register_table("probe", Arc::new(table(&dir, "probe", partitions, 1)))
            .unwrap();
        let sql = "SELECT count(*) FROM build JOIN probe USING (k)";
        let expected = (PROBE_ROWS / partitions).div_ceil(stride) * partitions;
        runtime.block_on(async {
            let plan = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let display = displayable(plan.as_ref()).indent(true).to_string();
            let mode = if partitions == 1 {
                "CollectLeft"
            } else {
                "Partitioned"
            };
            assert!(
                display.contains(&format!("HashJoinExec: mode={mode}")),
                "{display}"
            );
            assert!(!display.contains("RepartitionExec"), "{display}");
            assert!(display.contains("predicate=DynamicFilter"), "{display}");
            let batches =
                datafusion_physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
                    .await
                    .unwrap();
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(count as usize, expected);
            assert_eq!(metric(&plan, "pushdown_rows_pruned"), PROBE_ROWS - expected);
            eprintln!(
                "{partitions} partitions, 1/{stride} selectivity:\n{}",
                DisplayableExecutionPlan::with_metrics(plan.as_ref()).indent(true)
            );
        });
        // Measure expression evaluation separately from latency, on fresh plans
        // over the same warm files. Report medians to limit scheduling noise.
        let mut eval_times = Vec::new();
        for _ in 0..11 {
            runtime.block_on(async {
                let plan = ctx
                    .sql(sql)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap();
                datafusion_physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(metric(&plan, "pushdown_rows_pruned"), PROBE_ROWS - expected);
                eval_times.push(metric(&plan, "row_pushdown_eval_time"));
            });
        }
        eval_times.sort_unstable();
        eprintln!(
            "{partitions} partitions, 1/{stride} selectivity: median row_pushdown_eval_time = {:.3} ms (sum across partitions)",
            eval_times[eval_times.len() / 2] as f64 / 1_000_000.0
        );
        group.bench_with_input(
            BenchmarkId::new(format!("1_in_{stride}"), partitions),
            &ctx,
            |b, ctx| {
                b.iter(|| {
                    runtime.block_on(async {
                        let result = ctx.sql(sql).await.unwrap().collect().await.unwrap();
                        std::hint::black_box(result);
                    })
                });
            },
        );
    }
    group.finish();
}

fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map(|value| value.as_usize())
        .unwrap_or(0)
        + plan
            .children()
            .into_iter()
            .map(|child| metric(child, name))
            .sum::<usize>()
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
