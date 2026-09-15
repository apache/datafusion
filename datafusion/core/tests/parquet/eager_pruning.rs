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

//! Tests for eager parquet pruning (`datafusion.execution.parquet.eager_pruning`)

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::{FileExtensions, PartitionedFile};
use datafusion_datasource_parquet::{
    EagerPruningStats, EagerPruningSummary, ParquetAccessPlan,
};
use datafusion_physical_plan::{ExecutionPlan, collect, displayable};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use tempfile::TempDir;

/// Rows in each test file
const ROWS_PER_FILE: i64 = 1000;

/// Writes a parquet file with 10 row groups of 100 rows, each with data pages
/// of 10 rows and a bloom filter on column `s`.
///
/// Column `a` is sorted and holds `offset..offset + ROWS_PER_FILE`. Column `s`
/// holds values `s000` to `s999` scattered across all row groups, so that its
/// row group statistics cannot prune, but its bloom filters can. Column `c`
/// holds the index of the row group, so it has a single value in each row
/// group but not in the file.
fn write_file(path: &Path, offset: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let a = Int64Array::from_iter_values(offset..offset + ROWS_PER_FILE);
    let s = StringArray::from_iter_values(
        (0..ROWS_PER_FILE).map(|i| format!("s{:03}", (i * 37) % 1000)),
    );
    let c = Int64Array::from_iter_values((0..ROWS_PER_FILE).map(|i| i / 100));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(a), Arc::new(s), Arc::new(c)],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_column_bloom_filter_enabled(ColumnPath::from("s"), true)
        .set_column_bloom_filter_fpp(ColumnPath::from("s"), 0.0001)
        .build();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// A session with a table `t` of `num_files` test files, whose column `a`
/// holds `0..num_files * ROWS_PER_FILE`
struct TestTable {
    ctx: SessionContext,
    dir: TempDir,
}

impl TestTable {
    async fn new(config: &[(&str, &str)], num_files: usize) -> Self {
        let mut session_config = SessionConfig::new().with_target_partitions(2);
        for (key, value) in config {
            session_config = session_config.set_str(key, value);
        }
        let ctx = SessionContext::new_with_config(session_config);
        let dir = TempDir::new().unwrap();
        for i in 0..num_files {
            write_file(
                &dir.path().join(format!("{i}.parquet")),
                i as i64 * ROWS_PER_FILE,
            );
        }
        ctx.register_parquet(
            "t",
            dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
        Self { ctx, dir }
    }

    /// A table with eager pruning at `level` and otherwise default settings
    async fn with_level(level: &str, num_files: usize) -> Self {
        Self::new(
            &[("datafusion.execution.parquet.eager_pruning", level)],
            num_files,
        )
        .await
    }

    async fn scan(&self, sql: &str) -> Scan {
        Scan::plan(&self.ctx, sql).await
    }

    async fn results(&self, sql: &str) -> String {
        let batches = self.ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }
}

/// The parquet scan of a planned query
struct Scan {
    conf: FileScanConfig,
    source: ParquetSource,
    plan: String,
}

impl Scan {
    async fn plan(ctx: &SessionContext, sql: &str) -> Self {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let (conf, source) = find_parquet_scan(plan.as_ref())
            .unwrap_or_else(|| panic!("no parquet scan in plan for {sql}"));
        Self {
            conf,
            source,
            plan: displayable(plan.as_ref()).indent(true).to_string(),
        }
    }

    fn num_rows(&self) -> Precision<usize> {
        self.conf.statistics().num_rows
    }

    fn num_files(&self) -> usize {
        self.conf.file_groups.iter().map(|group| group.len()).sum()
    }

    fn num_files_with_access_plan(&self) -> usize {
        self.conf
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .filter(|file| file.extensions.contains::<ParquetAccessPlan>())
            .count()
    }

    fn summary(&self) -> Option<&EagerPruningSummary> {
        self.source.eager_pruning_summary()
    }

    fn pruned_stats(&self) -> &EagerPruningStats {
        match self.summary() {
            Some(EagerPruningSummary::Pruned(stats)) => stats,
            other => panic!("expected eager pruning, got {other:?}\n{}", self.plan),
        }
    }
}

fn find_parquet_scan(
    plan: &dyn ExecutionPlan,
) -> Option<(FileScanConfig, ParquetSource)> {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some((conf, source)) = exec.downcast_to_file_source::<ParquetSource>()
    {
        return Some((conf.clone(), source.clone()));
    }
    plan.children()
        .into_iter()
        .find_map(|child| find_parquet_scan(child.as_ref()))
}

#[tokio::test]
async fn disabled_by_default() {
    let table = TestTable::new(&[], 1).await;
    let scan = table.scan("SELECT * FROM t WHERE a < 150").await;

    assert_eq!(scan.summary(), None);
    assert_eq!(scan.num_rows(), Precision::Inexact(1000));
    assert_eq!(scan.num_files_with_access_plan(), 0);
    assert!(!scan.plan.contains("eager_pruning"), "{}", scan.plan);
}

#[tokio::test]
async fn row_groups() {
    let table = TestTable::with_level("row_groups", 1).await;
    let scan = table.scan("SELECT * FROM t WHERE a < 150").await;

    assert_eq!(
        scan.pruned_stats(),
        &EagerPruningStats {
            level: datafusion_common::config::EagerParquetPruning::RowGroups,
            files: 1,
            files_not_evaluated: 0,
            files_pruned: 0,
            row_groups: 10,
            row_groups_pruned: 8,
            rows: 1000,
            rows_pruned: 800,
        }
    );
    assert_eq!(scan.num_rows(), Precision::Inexact(200));
    assert_eq!(scan.num_files_with_access_plan(), 1);
    // Column statistics describe the rows that are read, so that estimates
    // derived from them do not account for the pruning twice
    let statistics = scan.conf.statistics();
    assert_eq!(
        statistics.column_statistics[0].max_value,
        Precision::Inexact(ScalarValue::Int64(Some(199)))
    );
    assert!(
        scan.plan.contains(
            "eager_pruning=[level=row_groups, files_pruned=0/1, \
             row_groups_pruned=8/10, rows_pruned=800/1000]"
        ),
        "{}",
        scan.plan
    );
}

#[tokio::test]
async fn page_index() {
    let sql = "SELECT * FROM t WHERE a = 123";

    let table = TestTable::with_level("row_groups", 1).await;
    assert_eq!(table.scan(sql).await.num_rows(), Precision::Inexact(100));

    let table = TestTable::with_level("page_index", 1).await;
    let scan = table.scan(sql).await;
    assert_eq!(scan.num_rows(), Precision::Inexact(10));
    assert_eq!(scan.pruned_stats().rows_pruned, 990);
}

#[tokio::test]
async fn page_index_respects_enable_page_index() {
    let table = TestTable::new(
        &[
            ("datafusion.execution.parquet.eager_pruning", "page_index"),
            ("datafusion.execution.parquet.enable_page_index", "false"),
        ],
        1,
    )
    .await;
    let scan = table.scan("SELECT * FROM t WHERE a = 123").await;
    assert_eq!(scan.num_rows(), Precision::Inexact(100));
}

#[tokio::test]
async fn bloom_filters() {
    // 's5005' is within the min/max range of every row group, but absent
    let sql = "SELECT * FROM t WHERE s = 's5005'";

    let table = TestTable::with_level("row_groups", 1).await;
    let scan = table.scan(sql).await;
    assert_eq!(scan.pruned_stats().row_groups_pruned, 0);
    assert_eq!(scan.num_files(), 1);
    // Nothing was pruned, so the file is left unchanged
    assert_eq!(scan.num_files_with_access_plan(), 0);

    let table = TestTable::with_level("bloom_filters", 1).await;
    let scan = table.scan(sql).await;
    let stats = scan.pruned_stats();
    assert_eq!(stats.files_pruned, 1);
    assert_eq!(stats.row_groups_pruned, 10);
    assert_eq!(scan.num_files(), 0);
    assert_eq!(scan.num_rows(), Precision::Inexact(0));
}

#[tokio::test]
async fn prunes_files() {
    let table = TestTable::with_level("row_groups", 3).await;
    let scan = table.scan("SELECT * FROM t WHERE a < 150").await;

    let stats = scan.pruned_stats();
    assert_eq!(stats.files, 3);
    assert_eq!(stats.files_pruned, 2);
    assert_eq!(stats.row_groups_pruned, 28);
    assert_eq!(scan.num_files(), 1);
    assert_eq!(scan.num_rows(), Precision::Inexact(200));
}

#[tokio::test]
async fn file_limit() {
    let table = TestTable::new(
        &[
            ("datafusion.execution.parquet.eager_pruning", "row_groups"),
            ("datafusion.execution.parquet.eager_pruning_file_limit", "2"),
        ],
        3,
    )
    .await;
    let scan = table.scan("SELECT * FROM t WHERE a < 150").await;

    assert_eq!(
        scan.summary(),
        Some(&EagerPruningSummary::Skipped {
            level: datafusion_common::config::EagerParquetPruning::RowGroups,
            reason: "3 files exceed eager_pruning_file_limit 2".to_string(),
        })
    );
    assert_eq!(scan.num_files(), 3);
    assert_eq!(scan.num_rows(), Precision::Inexact(3000));
}

#[tokio::test]
async fn no_filters() {
    let table = TestTable::with_level("bloom_filters", 1).await;
    let scan = table.scan("SELECT * FROM t").await;

    assert_eq!(scan.summary(), None);
    assert_eq!(scan.num_rows(), Precision::Exact(1000));
}

#[tokio::test]
async fn nothing_to_prune_keeps_exact_file_statistics() {
    let table = TestTable::with_level("row_groups", 1).await;
    let scan = table.scan("SELECT * FROM t WHERE a >= 0").await;

    assert_eq!(scan.pruned_stats().rows_pruned, 0);
    assert_eq!(scan.num_files_with_access_plan(), 0);
    let file = &scan.conf.file_groups[0].files()[0];
    assert_eq!(
        file.statistics.as_ref().unwrap().num_rows,
        Precision::Exact(1000)
    );
}

#[tokio::test]
async fn without_collected_statistics() {
    let table = TestTable::new(
        &[
            ("datafusion.execution.parquet.eager_pruning", "row_groups"),
            ("datafusion.execution.collect_statistics", "false"),
        ],
        1,
    )
    .await;
    let scan = table.scan("SELECT * FROM t WHERE a < 150").await;

    assert_eq!(scan.num_rows(), Precision::Inexact(200));
}

#[tokio::test]
async fn without_collected_statistics_and_unpruned_files() {
    let table = TestTable::new(
        &[
            ("datafusion.execution.parquet.eager_pruning", "row_groups"),
            ("datafusion.execution.collect_statistics", "false"),
        ],
        2,
    )
    .await;
    // The second file is read entirely, but its row count is known from the
    // metadata eager pruning reads, so the row count of the scan is known too
    let scan = table
        .scan("SELECT * FROM t WHERE a < 150 OR a >= 1000")
        .await;

    assert_eq!(scan.pruned_stats().rows_pruned, 800);
    assert_eq!(scan.num_rows(), Precision::Inexact(1200));
}

#[tokio::test]
async fn page_index_without_pruned_rows() {
    // Row group 0 is not fully matched by `a <> 5`, but no page can be pruned
    let table = TestTable::with_level("page_index", 1).await;
    let scan = table.scan("SELECT * FROM t WHERE a <> 5").await;

    assert_eq!(scan.pruned_stats().rows_pruned, 0);
    // Nothing was pruned, so the file is left unchanged
    assert_eq!(scan.num_files_with_access_plan(), 0);
    let file = &scan.conf.file_groups[0].files()[0];
    assert_eq!(
        file.statistics.as_ref().unwrap().num_rows,
        Precision::Exact(1000)
    );
}

/// Removes the access plans attached to the files of parquet scans, like
/// serializing a plan does (file statistics are serialized, extensions are not)
fn without_access_plans(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    plan.transform_up(|node| {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some((conf, _)) = exec.downcast_to_file_source::<ParquetSource>() else {
            return Ok(Transformed::no(node));
        };
        let file_groups = conf
            .file_groups
            .iter()
            .map(|group| {
                FileGroup::new(
                    group
                        .iter()
                        .map(|file| PartitionedFile {
                            extensions: FileExtensions::new(),
                            ..file.clone()
                        })
                        .collect(),
                )
            })
            .collect();
        let conf = FileScanConfigBuilder::from(conf.clone())
            .with_file_groups(file_groups)
            .build();
        Ok(Transformed::yes(
            DataSourceExec::from_data_source(conf) as Arc<dyn ExecutionPlan>
        ))
    })
    .unwrap()
    .data
}

#[tokio::test]
async fn correct_without_access_plans() {
    // Eager pruning reads only row group 0, in which `c` has a single value.
    // The statistics of the file must still be correct for the entire file
    // when the attached access plan is lost, otherwise the scan could replace
    // `c` with that value in the row groups it now reads.
    let sql = "SELECT c, count(*) FROM t WHERE c = 0 GROUP BY c";
    for level in ["row_groups", "page_index"] {
        for pushdown_filters in ["false", "true"] {
            let table = TestTable::new(
                &[
                    ("datafusion.execution.parquet.eager_pruning", level),
                    (
                        "datafusion.execution.parquet.pushdown_filters",
                        pushdown_filters,
                    ),
                ],
                1,
            )
            .await;
            let plan = table
                .ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let plan = without_access_plans(plan);
            let batches = collect(plan, table.ctx.task_ctx()).await.unwrap();
            assert_eq!(
                pretty_format_batches(&batches).unwrap().to_string(),
                "+---+----------+\n\
                 | c | count(*) |\n\
                 +---+----------+\n\
                 | 0 | 100      |\n\
                 +---+----------+",
                "level={level}, pushdown_filters={pushdown_filters}"
            );
        }
    }
}

#[tokio::test]
async fn table_option() {
    let table = TestTable::new(&[], 1).await;
    let location = table.dir.path().to_str().unwrap();
    table
        .ctx
        .sql(&format!(
            "CREATE EXTERNAL TABLE t_eager STORED AS PARQUET LOCATION '{location}' \
             OPTIONS ('format.eager_pruning' 'row_groups')"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let scan = table.scan("SELECT * FROM t_eager WHERE a < 150").await;
    assert_eq!(scan.num_rows(), Precision::Inexact(200));

    // The session default is unchanged
    let scan = table.scan("SELECT * FROM t WHERE a < 150").await;
    assert_eq!(scan.summary(), None);
}

#[tokio::test]
async fn partition_columns() {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .set_str("datafusion.execution.parquet.eager_pruning", "row_groups"),
    );
    let dir = TempDir::new().unwrap();
    write_file(&dir.path().join("part=1").join("0.parquet"), 0);
    write_file(&dir.path().join("part=2").join("0.parquet"), 0);
    ctx.register_parquet(
        "t",
        dir.path().to_str().unwrap(),
        ParquetReadOptions::default()
            .table_partition_cols(vec![("part".to_string(), DataType::Int32)]),
    )
    .await
    .unwrap();

    // The filter cannot be evaluated using partition values alone, but after
    // substituting them it prunes all rows with part = 1 and a >= 150
    let sql = "SELECT count(*) FROM t WHERE a < 150 OR part = 2";
    let scan = Scan::plan(&ctx, sql).await;
    assert_eq!(scan.num_rows(), Precision::Inexact(1200));

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(
        pretty_format_batches(&batches).unwrap().to_string(),
        "+----------+\n\
         | count(*) |\n\
         +----------+\n\
         | 1150     |\n\
         +----------+"
    );
}

#[tokio::test]
async fn results_match_disabled() {
    let queries = [
        "SELECT count(*) FROM t WHERE a < 150",
        "SELECT a, s FROM t WHERE a = 123",
        "SELECT count(*) FROM t WHERE s = 's5005'",
        "SELECT count(*) FROM t WHERE s = 's500'",
        "SELECT a FROM t WHERE a >= 95 AND a < 105 ORDER BY a",
        "SELECT count(*), min(a), max(a) FROM t WHERE a < 450 AND s > 's500'",
        "SELECT a FROM t WHERE a < 2500 ORDER BY a DESC LIMIT 3",
        "SELECT t.a FROM t JOIN (VALUES (5), (250), (1500)) AS v(x) ON t.a = v.x \
         WHERE t.a < 500 ORDER BY t.a",
        "SELECT count(*) FROM t WHERE a > 5000",
    ];

    let expected = {
        let table = TestTable::new(&[], 3).await;
        let mut expected = Vec::with_capacity(queries.len());
        for sql in queries {
            expected.push(table.results(sql).await);
        }
        expected
    };

    for level in ["row_groups", "page_index", "bloom_filters"] {
        for pushdown_filters in ["false", "true"] {
            let table = TestTable::new(
                &[
                    ("datafusion.execution.parquet.eager_pruning", level),
                    (
                        "datafusion.execution.parquet.pushdown_filters",
                        pushdown_filters,
                    ),
                ],
                3,
            )
            .await;
            for (sql, expected) in queries.iter().zip(&expected) {
                assert_eq!(
                    &table.results(sql).await,
                    expected,
                    "level={level}, pushdown_filters={pushdown_filters}: {sql}"
                );
            }
        }
    }
}
