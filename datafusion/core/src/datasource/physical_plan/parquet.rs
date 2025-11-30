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

//! Reexports the [`datafusion_datasource_parquet`] crate, containing Parquet based [`FileSource`].
//!
//! [`FileSource`]: datafusion_datasource::file::FileSource

pub use datafusion_datasource_parquet::*;

#[cfg(test)]
mod tests {
    // See also `parquet_exec` integration test
    use std::fs::{self, File};
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex;

    use crate::dataframe::DataFrameWriteOptions;
    use crate::datasource::file_format::options::CsvReadOptions;
    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::datasource::file_format::test_util::scan_format;
    use crate::datasource::listing::ListingOptions;
    use crate::execution::context::SessionState;
    use crate::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;
    use arrow::array::{
        ArrayRef, AsArray, Date64Array, Int32Array, Int64Array, Int8Array, StringArray,
        StringViewArray, StructArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaBuilder};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::{SchemaRef, TimeUnit};
    use bytes::{BufMut, BytesMut};
    use datafusion_common::config::TableParquetOptions;
    use datafusion_common::test_util::{batches_to_sort_string, batches_to_string};
    use datafusion_common::{assert_contains, Result, ScalarValue};
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::source::DataSourceExec;

    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::{FileRange, PartitionedFile, TableSchema};
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_datasource_parquet::{
        DefaultParquetFileReaderFactory, ParquetFileReaderFactory, ParquetFormat,
    };
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::{col, lit, when, Expr};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::analyze::AnalyzeExec;
    use datafusion_physical_plan::collect;
    use datafusion_physical_plan::metrics::{
        ExecutionPlanMetricsSet, MetricType, MetricValue, MetricsSet,
    };
    use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

    use chrono::{TimeZone, Utc};
    use datafusion_datasource::file_groups::FileGroup;
    use futures::StreamExt;
    use insta;
    use insta::assert_snapshot;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{ObjectMeta, ObjectStore};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use url::Url;

    struct RoundTripResult {
        /// Data that was read back from ParquetFiles
        batches: Result<Vec<RecordBatch>>,
        /// The EXPLAIN ANALYZE output
        explain: Result<String>,
        /// The physical plan that was created (that has statistics, etc)
        parquet_exec: Arc<DataSourceExec>,
    }

    /// round-trip record batches by writing each individual RecordBatch to
    /// a parquet file and then reading that parquet file with the specified
    /// options.
    #[derive(Debug, Default)]
    struct RoundTrip {
        projection: Option<Vec<usize>>,
        /// Optional logical table schema to use when reading the parquet files
        ///
        /// If None, the logical schema to use will be inferred from the
        /// original data via [`Schema::try_merge`]
        table_schema: Option<SchemaRef>,
        predicate: Option<Expr>,
        pushdown_predicate: bool,
        page_index_predicate: bool,
        bloom_filters: bool,
    }

    impl RoundTrip {
        fn new() -> Self {
            Default::default()
        }

        fn with_projection(mut self, projection: Vec<usize>) -> Self {
            self.projection = Some(projection);
            self
        }

        /// Specify table schema.
        ///
        ///See  [`Self::table_schema`] for more details
        fn with_table_schema(mut self, schema: SchemaRef) -> Self {
            self.table_schema = Some(schema);
            self
        }

        fn with_predicate(mut self, predicate: Expr) -> Self {
            self.predicate = Some(predicate);
            self
        }

        fn with_pushdown_predicate(mut self) -> Self {
            self.pushdown_predicate = true;
            self
        }

        fn with_page_index_predicate(mut self) -> Self {
            self.page_index_predicate = true;
            self
        }

        fn with_bloom_filters(mut self) -> Self {
            self.bloom_filters = true;
            self
        }

        /// run the test, returning only the resulting RecordBatches
        async fn round_trip_to_batches(
            self,
            batches: Vec<RecordBatch>,
        ) -> Result<Vec<RecordBatch>> {
            self.round_trip(batches).await.batches
        }

        fn build_file_source(&self, table_schema: SchemaRef) -> Arc<dyn FileSource> {
            // set up predicate (this is normally done by a layer higher up)
            let predicate = self
                .predicate
                .as_ref()
                .map(|p| logical2physical(p, &table_schema));

            let mut source = ParquetSource::new(table_schema);
            if let Some(predicate) = predicate {
                source = source.with_predicate(predicate);
            }

            if self.pushdown_predicate {
                source = source
                    .with_pushdown_filters(true)
                    .with_reorder_filters(true);
            } else {
                source = source.with_pushdown_filters(false);
            }

            if self.page_index_predicate {
                source = source.with_enable_page_index(true);
            } else {
                source = source.with_enable_page_index(false);
            }

            if self.bloom_filters {
                source = source.with_bloom_filter_on_read(true);
            } else {
                source = source.with_bloom_filter_on_read(false);
            }

            Arc::new(source)
        }

        fn build_parquet_exec(
            &self,
            file_group: FileGroup,
            source: Arc<dyn FileSource>,
        ) -> Arc<DataSourceExec> {
            let base_config =
                FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
                    .with_file_group(file_group)
                    .with_projection_indices(self.projection.clone())
                    .unwrap()
                    .build();
            DataSourceExec::from_data_source(base_config)
        }

        /// run the test, returning the `RoundTripResult`
        ///
        /// Each input batch is written into one or more parquet files (and thus
        /// they could potentially have different schemas). The resulting
        /// parquet files are then read back and filters are applied to the
        async fn round_trip(&self, batches: Vec<RecordBatch>) -> RoundTripResult {
            // If table_schema is not set, we need to merge the schema of the
            // input batches to get a unified schema.
            let table_schema = match &self.table_schema {
                Some(schema) => schema,
                None => &Arc::new(
                    Schema::try_merge(
                        batches.iter().map(|b| b.schema().as_ref().clone()),
                    )
                    .unwrap(),
                ),
            };
            // If testing with page_index_predicate, write parquet
            // files with multiple pages
            let multi_page = self.page_index_predicate;
            let (meta, _files) = store_parquet(batches, multi_page).await.unwrap();
            let file_group: FileGroup = meta.into_iter().map(Into::into).collect();

            // build a ParquetExec to return the results
            let parquet_source = self.build_file_source(Arc::clone(table_schema));
            let parquet_exec =
                self.build_parquet_exec(file_group.clone(), Arc::clone(&parquet_source));

            let analyze_exec = Arc::new(AnalyzeExec::new(
                false,
                false,
                vec![MetricType::SUMMARY, MetricType::DEV],
                // use a new ParquetSource to avoid sharing execution metrics
                self.build_parquet_exec(
                    file_group.clone(),
                    self.build_file_source(Arc::clone(table_schema)),
                ),
                Arc::new(Schema::new(vec![
                    Field::new("plan_type", DataType::Utf8, true),
                    Field::new("plan", DataType::Utf8, true),
                ])),
            ));

            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();

            let batches = collect(
                Arc::clone(&parquet_exec) as Arc<dyn ExecutionPlan>,
                task_ctx.clone(),
            )
            .await;

            let explain = collect(analyze_exec, task_ctx.clone())
                .await
                .map(|batches| {
                    let batches = pretty_format_batches(&batches).unwrap();
                    format!("{batches}")
                });

            RoundTripResult {
                batches,
                explain,
                parquet_exec,
            }
        }
    }

    // Add a new column with the specified field name to the RecordBatch
    fn add_to_batch(
        batch: &RecordBatch,
        field_name: &str,
        array: ArrayRef,
    ) -> RecordBatch {
        let mut fields = SchemaBuilder::from(batch.schema().fields());
        fields.push(Field::new(field_name, array.data_type().clone(), true));
        let schema = Arc::new(fields.finish());

        let mut columns = batch.columns().to_vec();
        columns.push(array);
        RecordBatch::try_new(schema, columns).expect("error; creating record batch")
    }

    fn create_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        columns.into_iter().fold(
            RecordBatch::new_empty(Arc::new(Schema::empty())),
            |batch, (field_name, arr)| add_to_batch(&batch, field_name, arr.clone()),
        )
    }

    #[tokio::test]
    async fn test_pushdown_with_missing_column_in_file() {
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));

        let file_schema =
            Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(file_schema.clone(), vec![c1]).unwrap();

        // Since c2 is missing from the file and we didn't supply a custom `SchemaAdapterFactory`,
        // the default behavior is to fill in missing columns with nulls.
        // Thus this predicate will come back as false.
        let filter = col("c2").eq(lit(1_i32));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let total_rows = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(total_rows, 0, "Expected no rows to match the predicate");
        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 3, "Expected all rows to be pruned");

        // If we explicitly allow nulls the rest of the predicate should work
        let filter = col("c2").is_null().and(col("c1").eq(lit(1_i32)));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let batches = rt.batches.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&batches),@r###"
        +----+----+
        | c1 | c2 |
        +----+----+
        | 1  |    |
        +----+----+
        "###);

        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 2, "Expected all rows to be pruned");
    }

    #[tokio::test]
    async fn test_pushdown_with_missing_column_in_file_multiple_types() {
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));

        let file_schema =
            Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(file_schema.clone(), vec![c1]).unwrap();

        // Since c2 is missing from the file and we didn't supply a custom `SchemaAdapterFactory`,
        // the default behavior is to fill in missing columns with nulls.
        // Thus this predicate will come back as false.
        let filter = col("c2").eq(lit("abc"));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let total_rows = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(total_rows, 0, "Expected no rows to match the predicate");
        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 3, "Expected all rows to be pruned");

        // If we explicitly allow nulls the rest of the predicate should work
        let filter = col("c2").is_null().and(col("c1").eq(lit(1_i32)));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let batches = rt.batches.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&batches),@r###"
        +----+----+
        | c1 | c2 |
        +----+----+
        | 1  |    |
        +----+----+
        "###);

        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 2, "Expected all rows to be pruned");
    }

    #[tokio::test]
    async fn test_pushdown_with_missing_middle_column() {
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let c3 = Arc::new(Int32Array::from(vec![7, 8, 9]));

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(file_schema.clone(), vec![c1, c3]).unwrap();

        // Since c2 is missing from the file and we didn't supply a custom `SchemaAdapterFactory`,
        // the default behavior is to fill in missing columns with nulls.
        // Thus this predicate will come back as false.
        let filter = col("c2").eq(lit("abc"));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let total_rows = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(total_rows, 0, "Expected no rows to match the predicate");
        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 3, "Expected all rows to be pruned");

        // If we explicitly allow nulls the rest of the predicate should work
        let filter = col("c2").is_null().and(col("c1").eq(lit(1_i32)));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let batches = rt.batches.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&batches),@r###"
        +----+----+----+
        | c1 | c2 | c3 |
        +----+----+----+
        | 1  |    | 7  |
        +----+----+----+
        "###);

        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 2, "Expected all rows to be pruned");
    }

    #[tokio::test]
    async fn test_pushdown_with_file_column_order_mismatch() {
        let c3 = Arc::new(Int32Array::from(vec![7, 8, 9]));

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("c3", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let batch =
            RecordBatch::try_new(file_schema.clone(), vec![c3.clone(), c3]).unwrap();

        // Since c2 is missing from the file and we didn't supply a custom `SchemaAdapterFactory`,
        // the default behavior is to fill in missing columns with nulls.
        // Thus this predicate will come back as false.
        let filter = col("c2").eq(lit("abc"));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let total_rows = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(total_rows, 0, "Expected no rows to match the predicate");
        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 3, "Expected all rows to be pruned");

        // If we explicitly allow nulls the rest of the predicate should work
        let filter = col("c2").is_null().and(col("c3").eq(lit(7_i32)));
        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;
        let batches = rt.batches.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&batches),@r###"
        +----+----+----+
        | c1 | c2 | c3 |
        +----+----+----+
        |    |    | 7  |
        +----+----+----+
        "###);

        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 2, "Expected all rows to be pruned");
    }

    #[tokio::test]
    async fn test_pushdown_with_missing_column_nested_conditions() {
        // Create test data with c1 and c3 columns
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let c3: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]));

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(file_schema.clone(), vec![c1, c3]).unwrap();

        // Test with complex nested AND/OR:
        // (c1 = 1 OR c2 = 5) AND (c3 = 10 OR c2 IS NULL)
        // Should return 1 row where c1=1 AND c3=10 (since c2 IS NULL is always true)
        let filter = col("c1")
            .eq(lit(1_i32))
            .or(col("c2").eq(lit(5_i32)))
            .and(col("c3").eq(lit(10_i32)).or(col("c2").is_null()));

        let rt = RoundTrip::new()
            .with_table_schema(table_schema.clone())
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch.clone()])
            .await;

        let batches = rt.batches.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&batches),@r###"
        +----+----+----+
        | c1 | c2 | c3 |
        +----+----+----+
        | 1  |    | 10 |
        +----+----+----+
        "###);

        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 4, "Expected 4 rows to be pruned");

        // Test a more complex nested condition:
        // (c1 < 3 AND c2 IS NOT NULL) OR (c3 > 20 AND c2 IS NULL)
        // First part should return 0 rows (c2 IS NOT NULL is always false)
        // Second part should return rows where c3 > 20 (3 rows: where c3 is 30, 40, 50)
        let filter = col("c1")
            .lt(lit(3_i32))
            .and(col("c2").is_not_null())
            .or(col("c3").gt(lit(20_i32)).and(col("c2").is_null()));

        let rt = RoundTrip::new()
            .with_table_schema(table_schema)
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch])
            .await;

        let batches = rt.batches.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&batches),@r###"
        +----+----+----+
        | c1 | c2 | c3 |
        +----+----+----+
        | 3  |    | 30 |
        | 4  |    | 40 |
        | 5  |    | 50 |
        +----+----+----+
        "###);

        let metrics = rt.parquet_exec.metrics().unwrap();
        let metric = get_value(&metrics, "pushdown_rows_pruned");
        assert_eq!(metric, 2, "Expected 2 rows to be pruned");
    }

    #[tokio::test]
    async fn evolved_schema() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        // batch1: c1(string)
        let batch1 =
            add_to_batch(&RecordBatch::new_empty(Arc::new(Schema::empty())), "c1", c1);

        // batch2: c1(string) and c2(int64)
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch2 = add_to_batch(&batch1, "c2", c2);

        // batch3: c1(string) and c3(int8)
        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));
        let batch3 = add_to_batch(&batch1, "c3", c3);

        // read/write them files:
        let read = RoundTrip::new()
            .round_trip_to_batches(vec![batch1, batch2, batch3])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read), @r###"
        +-----+----+----+
        | c1  | c2 | c3 |
        +-----+----+----+
        |     |    |    |
        |     |    | 20 |
        |     | 2  |    |
        | Foo |    |    |
        | Foo |    | 10 |
        | Foo | 1  |    |
        | bar |    |    |
        | bar |    |    |
        | bar |    |    |
        +-----+----+----+
        "###);
    }

    #[tokio::test]
    async fn evolved_schema_inconsistent_order() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        // read/write them files:
        let read = RoundTrip::new()
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read),@r"
        +-----+----+----+
        | c1  | c2 | c3 |
        +-----+----+----+
        |     | 2  | 20 |
        |     | 2  | 20 |
        | Foo | 1  | 10 |
        | Foo | 1  | 10 |
        | bar |    |    |
        | bar |    |    |
        +-----+----+----+
        ");
    }

    #[tokio::test]
    async fn evolved_schema_intersection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        // read/write them files:
        let read = RoundTrip::new()
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read),@r"
        +-----+----+----+
        | c1  | c3 | c2 |
        +-----+----+----+
        |     |    |    |
        |     | 10 | 1  |
        |     | 20 |    |
        |     | 20 | 2  |
        | Foo | 10 |    |
        | bar |    |    |
        +-----+----+----+
        ");
    }

    #[tokio::test]
    async fn evolved_schema_intersection_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        let filter = col("c2").eq(lit(2_i64));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read),@r###"
            +-----+----+----+
            | c1  | c3 | c2 |
            +-----+----+----+
            |     |    |    |
            |     | 10 | 1  |
            |     | 20 |    |
            |     | 20 | 2  |
            | Foo | 10 |    |
            | bar |    |    |
            +-----+----+----+
        "###);
    }

    #[tokio::test]
    async fn evolved_schema_intersection_filter_with_filter_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));
        // batch1: c1(string), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);
        // batch2: c3(int8), c2(int64)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);
        let filter = col("c2").eq(lit(2_i64)).or(col("c2").eq(lit(1_i64)));
        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1, batch2])
            .await;

        insta::assert_snapshot!(batches_to_sort_string(&rt.batches.unwrap()), @r###"
        +----+----+----+
        | c1 | c3 | c2 |
        +----+----+----+
        |    | 10 | 1  |
        |    | 20 | 2  |
        +----+----+----+
        "###);
        let metrics = rt.parquet_exec.metrics().unwrap();
        // Note there are were 6 rows in total (across three batches)
        assert_eq!(get_value(&metrics, "pushdown_rows_pruned"), 4);
        assert_eq!(get_value(&metrics, "pushdown_rows_matched"), 2);
    }

    #[tokio::test]
    async fn evolved_schema_projection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef =
            Arc::new(StringArray::from(vec![Some("baz"), Some("boo"), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1), ("c4", c4)]);

        // read/write them files:
        let read = RoundTrip::new()
            .with_projection(vec![0, 3])
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read), @r###"
        +-----+-----+
        | c1  | c4  |
        +-----+-----+
        |     |     |
        |     | boo |
        | Foo |     |
        | Foo | baz |
        | bar |     |
        | bar |     |
        +-----+-----+
        "###);
    }

    #[tokio::test]
    async fn evolved_schema_column_order_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        let filter = col("c3").eq(lit(0_i8));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        // Predicate should prune all row groups
        assert_eq!(read.len(), 0);
    }

    #[tokio::test]
    async fn evolved_schema_column_type_filter_strings() {
        // The table and filter have a common data type, but the file schema differs
        let c1: ArrayRef =
            Arc::new(StringViewArray::from(vec![Some("foo"), Some("bar")]));
        let batch = create_batch(vec![("c1", c1.clone())]);

        // Table schema is Utf8 but file schema is StringView
        let table_schema =
            Arc::new(Schema::new(vec![Field::new("c1", DataType::Utf8, false)]));

        // Predicate should prune all row groups
        let filter = col("c1").eq(lit(ScalarValue::Utf8(Some("aaa".to_string()))));
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_table_schema(table_schema.clone())
            .round_trip(vec![batch.clone()])
            .await;
        // There should be no predicate evaluation errors
        let metrics = rt.parquet_exec.metrics().unwrap();
        assert_eq!(get_value(&metrics, "predicate_evaluation_errors"), 0);
        assert_eq!(get_value(&metrics, "pushdown_rows_matched"), 0);
        assert_eq!(rt.batches.unwrap().len(), 0);

        // Predicate should prune no row groups
        let filter = col("c1").eq(lit(ScalarValue::Utf8(Some("foo".to_string()))));
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_table_schema(table_schema)
            .round_trip(vec![batch])
            .await;
        // There should be no predicate evaluation errors
        let metrics = rt.parquet_exec.metrics().unwrap();
        assert_eq!(get_value(&metrics, "predicate_evaluation_errors"), 0);
        assert_eq!(get_value(&metrics, "pushdown_rows_matched"), 0);
        let read = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(read, 2, "Expected 2 rows to match the predicate");
    }

    #[tokio::test]
    async fn evolved_schema_column_type_filter_ints() {
        // The table and filter have a common data type, but the file schema differs
        let c1: ArrayRef = Arc::new(Int8Array::from(vec![Some(1), Some(2)]));
        let batch = create_batch(vec![("c1", c1.clone())]);

        let table_schema =
            Arc::new(Schema::new(vec![Field::new("c1", DataType::UInt64, false)]));

        // Predicate should prune all row groups
        let filter = col("c1").eq(lit(ScalarValue::UInt64(Some(5))));
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_table_schema(table_schema.clone())
            .round_trip(vec![batch.clone()])
            .await;
        // There should be no predicate evaluation errors
        let metrics = rt.parquet_exec.metrics().unwrap();
        assert_eq!(get_value(&metrics, "predicate_evaluation_errors"), 0);
        assert_eq!(rt.batches.unwrap().len(), 0);

        // Predicate should prune no row groups
        let filter = col("c1").eq(lit(ScalarValue::UInt64(Some(1))));
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_table_schema(table_schema)
            .round_trip(vec![batch])
            .await;
        // There should be no predicate evaluation errors
        let metrics = rt.parquet_exec.metrics().unwrap();
        assert_eq!(get_value(&metrics, "predicate_evaluation_errors"), 0);
        let read = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(read, 2, "Expected 2 rows to match the predicate");
    }

    #[tokio::test]
    async fn evolved_schema_column_type_filter_timestamp_units() {
        // The table and filter have a common data type
        // The table schema is in milliseconds, but the file schema is in nanoseconds
        let c1: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_000_000_000), // 1970-01-01T00:00:01Z
            Some(2_000_000_000), // 1970-01-01T00:00:02Z
            Some(3_000_000_000), // 1970-01-01T00:00:03Z
            Some(4_000_000_000), // 1970-01-01T00:00:04Z
        ]));
        let batch = create_batch(vec![("c1", c1.clone())]);
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )]));
        // One row should match, 2 pruned via page index, 1 pruned via filter pushdown
        let filter = col("c1").eq(lit(ScalarValue::TimestampMillisecond(
            Some(1_000),
            Some("UTC".into()),
        )));
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .with_page_index_predicate() // produces pages with 2 rows each (2 pages total for our data)
            .with_table_schema(table_schema.clone())
            .round_trip(vec![batch.clone()])
            .await;
        // There should be no predicate evaluation errors and we keep 1 row
        let metrics = rt.parquet_exec.metrics().unwrap();
        assert_eq!(get_value(&metrics, "predicate_evaluation_errors"), 0);
        let read = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(read, 1, "Expected 1 rows to match the predicate");
        assert_eq!(get_value(&metrics, "row_groups_pruned_statistics"), 0);
        assert_eq!(get_value(&metrics, "page_index_rows_pruned"), 2);
        assert_eq!(get_value(&metrics, "pushdown_rows_pruned"), 1);
        // If we filter with a value that is completely out of the range of the data
        // we prune at the row group level.
        let filter = col("c1").eq(lit(ScalarValue::TimestampMillisecond(
            Some(5_000),
            Some("UTC".into()),
        )));
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .with_table_schema(table_schema)
            .round_trip(vec![batch])
            .await;
        // There should be no predicate evaluation errors and we keep 0 rows
        let metrics = rt.parquet_exec.metrics().unwrap();
        assert_eq!(get_value(&metrics, "predicate_evaluation_errors"), 0);
        let read = rt
            .batches
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(read, 0, "Expected 0 rows to match the predicate");
        assert_eq!(get_value(&metrics, "row_groups_pruned_statistics"), 1);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_filter() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .round_trip_to_batches(vec![batch1, batch2])
            .await
            .unwrap();

        // This does not look correct since the "c2" values in the result do not in fact match the predicate `c2 == 0`
        // but parquet pruning is not exact. If the min/max values are not defined (which they are not in this case since the it is
        // a null array, then the pruning predicate (currently) can not be applied.
        // In a real query where this predicate was pushed down from a filter stage instead of created directly in the `DataSourceExec`,
        // the filter stage would be preserved as a separate execution plan stage so the actual query results would be as expected.

        insta::assert_snapshot!(batches_to_sort_string(&read),@r###"
            +-----+----+
            | c1  | c2 |
            +-----+----+
            |     |    |
            |     |    |
            |     | 1  |
            |     | 2  |
            | Foo |    |
            | bar |    |
            +-----+----+
        "###);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_with_filter_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1, batch2])
            .await;

        insta::assert_snapshot!(batches_to_sort_string(&rt.batches.unwrap()), @r###"
        +----+----+
        | c1 | c2 |
        +----+----+
        |    | 1  |
        +----+----+
        "###);
        let metrics = rt.parquet_exec.metrics().unwrap();
        // Note there are were 6 rows in total (across three batches)
        assert_eq!(get_value(&metrics, "pushdown_rows_pruned"), 5);
        assert_eq!(get_value(&metrics, "pushdown_rows_matched"), 1);
    }

    #[tokio::test]
    async fn evolved_schema_disjoint_schema_with_page_index_pushdown() {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            // Page 1
            Some("Foo"),
            Some("Bar"),
            // Page 2
            Some("Foo2"),
            Some("Bar2"),
            // Page 3
            Some("Foo3"),
            Some("Bar3"),
        ]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![
            // Page 1:
            Some(1),
            Some(2),
            // Page 2: (pruned)
            Some(3),
            Some(4),
            // Page 3: (pruned)
            Some(5),
            None,
        ]));

        // batch1: c1(string)
        let batch1 = create_batch(vec![("c1", c1.clone())]);

        // batch2: c2(int64)
        let batch2 = create_batch(vec![("c2", c2.clone())]);

        // batch3 (has c2, c1) -- both columns, should still prune
        let batch3 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // batch4 (has c2, c1) -- different column order, should still prune
        let batch4 = create_batch(vec![("c2", c2), ("c1", c1)]);

        let filter = col("c2").eq(lit(1_i64));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_page_index_predicate()
            .round_trip(vec![batch1, batch2, batch3, batch4])
            .await;

        insta::assert_snapshot!(batches_to_sort_string(&rt.batches.unwrap()), @r###"
        +------+----+
        | c1   | c2 |
        +------+----+
        |      | 1  |
        |      | 2  |
        | Bar  |    |
        | Bar  | 2  |
        | Bar  | 2  |
        | Bar2 |    |
        | Bar3 |    |
        | Foo  |    |
        | Foo  | 1  |
        | Foo  | 1  |
        | Foo2 |    |
        | Foo3 |    |
        +------+----+
        "###);
        let metrics = rt.parquet_exec.metrics().unwrap();

        // There are 4 rows pruned in each of batch2, batch3, and
        // batch4 for a total of 12. batch1 had no pruning as c2 was
        // filled in as null
        let (page_index_pruned, page_index_matched) =
            get_pruning_metric(&metrics, "page_index_rows_pruned");
        assert_eq!(page_index_pruned, 12);
        assert_eq!(page_index_matched, 6);
    }

    #[tokio::test]
    async fn multi_column_predicate_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // Columns in different order to schema
        let filter = col("c2").eq(lit(1_i64)).or(col("c1").eq(lit("bar")));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip_to_batches(vec![batch1])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read),@r###"
            +-----+----+
            | c1  | c2 |
            +-----+----+
            | Foo | 1  |
            | bar |    |
            +-----+----+
        "###);
    }

    #[tokio::test]
    async fn multi_column_predicate_pushdown_page_index_pushdown() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = create_batch(vec![("c1", c1.clone()), ("c2", c2.clone())]);

        // Columns in different order to schema
        let filter = col("c2").eq(lit(1_i64)).or(col("c1").eq(lit("bar")));

        // read/write them files:
        let read = RoundTrip::new()
            .with_predicate(filter)
            .with_page_index_predicate()
            .round_trip_to_batches(vec![batch1])
            .await
            .unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read),@r###"
            +-----+----+
            | c1  | c2 |
            +-----+----+
            |     | 2  |
            | Foo | 1  |
            | bar |    |
            +-----+----+
        "###);
    }

    #[tokio::test]
    async fn evolved_schema_incompatible_types() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef = Arc::new(Date64Array::from(vec![
            Some(86400000),
            None,
            Some(259200000),
        ]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c4), ("c2", c2), ("c1", c1)]);

        let table_schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int8, true),
        ]);

        // read/write them files:
        let read = RoundTrip::new()
            .with_table_schema(Arc::new(table_schema))
            .round_trip_to_batches(vec![batch1, batch2])
            .await;
        assert_contains!(read.unwrap_err().to_string(),
            "Cannot cast file schema field c3 of type Date64 to table schema field of type Int8");
    }

    #[tokio::test]
    async fn parquet_exec_with_projection() -> Result<()> {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let filename = "alltypes_plain.parquet";
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let parquet_exec = scan_format(
            &state,
            &ParquetFormat::default(),
            None,
            &testdata,
            filename,
            Some(vec![0, 1, 2]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_int96_from_spark() -> Result<()> {
        // arrow-rs relies on the chrono library to convert between timestamps and strings, so
        // instead compare as Int64. The underlying type should be a PrimitiveArray of Int64
        // anyway, so this should be a zero-copy non-modifying cast at the SchemaAdapter.

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let testdata = datafusion_common::test_util::parquet_test_data();
        let filename = "int96_from_spark.parquet";
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();

        let time_units_and_expected = vec![
            (
                None, // Same as "ns" time_unit
                Arc::new(Int64Array::from(vec![
                    Some(1704141296123456000), // Reads as nanosecond fine (note 3 extra 0s)
                    Some(1704070800000000000), // Reads as nanosecond fine (note 3 extra 0s)
                    Some(-4852191831933722624), // Cannot be represented with nanos timestamp (year 9999)
                    Some(1735599600000000000), // Reads as nanosecond fine (note 3 extra 0s)
                    None,
                    Some(-4864435138808946688), // Cannot be represented with nanos timestamp (year 290000)
                ])),
            ),
            (
                Some("ns".to_string()),
                Arc::new(Int64Array::from(vec![
                    Some(1704141296123456000),
                    Some(1704070800000000000),
                    Some(-4852191831933722624),
                    Some(1735599600000000000),
                    None,
                    Some(-4864435138808946688),
                ])),
            ),
            (
                Some("us".to_string()),
                Arc::new(Int64Array::from(vec![
                    Some(1704141296123456),
                    Some(1704070800000000),
                    Some(253402225200000000),
                    Some(1735599600000000),
                    None,
                    Some(9089380393200000000),
                ])),
            ),
        ];

        for (time_unit, expected) in time_units_and_expected {
            let parquet_exec = scan_format(
                &state,
                &ParquetFormat::default().with_coerce_int96(time_unit.clone()),
                Some(schema.clone()),
                &testdata,
                filename,
                Some(vec![0]),
                None,
            )
            .await
            .unwrap();
            assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

            let mut results = parquet_exec.execute(0, task_ctx.clone())?;
            let batch = results.next().await.unwrap()?;

            assert_eq!(6, batch.num_rows());
            assert_eq!(1, batch.num_columns());

            assert_eq!(batch.num_columns(), 1);
            let column = batch.column(0);

            assert_eq!(column.len(), expected.len());

            column
                .as_primitive::<arrow::datatypes::Int64Type>()
                .iter()
                .zip(expected.iter())
                .for_each(|(lhs, rhs)| {
                    assert_eq!(lhs, rhs);
                });
        }

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_int96_nested() -> Result<()> {
        // This test ensures that we maintain compatibility with coercing int96 to the desired
        // resolution when they're within a nested type (e.g., struct, map, list). This file
        // originates from a modified CometFuzzTestSuite ParquetGenerator to generate combinations
        // of primitive and complex columns using int96. Other tests cover reading the data
        // correctly with this coercion. Here we're only checking the coerced schema is correct.
        let testdata = "../../datafusion/core/tests/data";
        let filename = "int96_nested.parquet";
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();

        let parquet_exec = scan_format(
            &state,
            &ParquetFormat::default().with_coerce_int96(Some("us".to_string())),
            None,
            testdata,
            filename,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0, task_ctx.clone())?;
        let batch = results.next().await.unwrap()?;

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new_struct(
                "c1",
                vec![Field::new(
                    "c0",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )],
                true,
            ),
            Field::new_struct(
                "c2",
                vec![Field::new_list(
                    "c0",
                    Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    true,
                )],
                true,
            ),
            Field::new_map(
                "c3",
                "key_value",
                Field::new(
                    "key",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                false,
                true,
            ),
            Field::new_list(
                "c4",
                Field::new(
                    "element",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                true,
            ),
            Field::new_list(
                "c5",
                Field::new_struct(
                    "element",
                    vec![Field::new(
                        "c0",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    )],
                    true,
                ),
                true,
            ),
            Field::new_list(
                "c6",
                Field::new_map(
                    "element",
                    "key_value",
                    Field::new(
                        "key",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        false,
                    ),
                    Field::new(
                        "value",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    false,
                    true,
                ),
                true,
            ),
        ]));

        assert_eq!(batch.schema(), expected_schema);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_range() -> Result<()> {
        fn file_range(meta: &ObjectMeta, start: i64, end: i64) -> PartitionedFile {
            PartitionedFile {
                object_meta: meta.clone(),
                partition_values: vec![],
                range: Some(FileRange { start, end }),
                statistics: None,
                extensions: None,
                metadata_size_hint: None,
            }
        }

        async fn assert_parquet_read(
            state: &SessionState,
            file_groups: Vec<FileGroup>,
            expected_row_num: Option<usize>,
            file_schema: SchemaRef,
        ) -> Result<()> {
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(ParquetSource::new(file_schema)),
            )
            .with_file_groups(file_groups)
            .build();

            let parquet_exec = DataSourceExec::from_data_source(config);
            assert_eq!(
                parquet_exec
                    .properties()
                    .output_partitioning()
                    .partition_count(),
                1
            );
            let results = parquet_exec.execute(0, state.task_ctx())?.next().await;

            if let Some(expected_row_num) = expected_row_num {
                let batch = results.unwrap()?;
                assert_eq!(expected_row_num, batch.num_rows());
            } else {
                assert!(results.is_none());
            }

            Ok(())
        }

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = datafusion_common::test_util::parquet_test_data();
        let filename = format!("{testdata}/alltypes_plain.parquet");

        let meta = local_unpartitioned_file(filename);

        let store = Arc::new(LocalFileSystem::new()) as _;
        let file_schema = ParquetFormat::default()
            .infer_schema(&state, &store, std::slice::from_ref(&meta))
            .await?;

        let group_empty = vec![FileGroup::new(vec![file_range(&meta, 0, 2)])];
        let group_contain = vec![FileGroup::new(vec![file_range(&meta, 2, i64::MAX)])];
        let group_all = vec![FileGroup::new(vec![
            file_range(&meta, 0, 2),
            file_range(&meta, 2, i64::MAX),
        ])];

        assert_parquet_read(&state, group_empty, None, file_schema.clone()).await?;
        assert_parquet_read(&state, group_contain, Some(8), file_schema.clone()).await?;
        assert_parquet_read(&state, group_all, Some(8), file_schema).await?;

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&object_store_url).unwrap();

        let testdata = datafusion_common::test_util::parquet_test_data();
        let filename = format!("{testdata}/alltypes_plain.parquet");

        let meta = local_unpartitioned_file(filename);

        let schema = ParquetFormat::default()
            .infer_schema(&state, &store, std::slice::from_ref(&meta))
            .await
            .unwrap();

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![
                ScalarValue::from("2021"),
                ScalarValue::UInt8(Some(10)),
                ScalarValue::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(ScalarValue::from("26")),
                ),
            ],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let expected_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("tinyint_col", DataType::Int32, true),
            Field::new("month", DataType::UInt8, false),
            Field::new(
                "day",
                DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                ),
                false,
            ),
        ]);

        let table_schema = TableSchema::new(
            Arc::clone(&schema),
            vec![
                Arc::new(Field::new("year", DataType::Utf8, false)),
                Arc::new(Field::new("month", DataType::UInt8, false)),
                Arc::new(Field::new(
                    "day",
                    DataType::Dictionary(
                        Box::new(DataType::UInt16),
                        Box::new(DataType::Utf8),
                    ),
                    false,
                )),
            ],
        );
        let source = Arc::new(ParquetSource::new(table_schema.clone()));
        let config = FileScanConfigBuilder::new(object_store_url, source)
            .with_file(partitioned_file)
            // file has 10 cols so index 12 should be month and 13 should be day
            .with_projection_indices(Some(vec![0, 1, 2, 12, 13]))
            .unwrap()
            .build();

        let parquet_exec = DataSourceExec::from_data_source(config);
        let partition_count = parquet_exec
            .data_source()
            .output_partitioning()
            .partition_count();
        assert_eq!(partition_count, 1);
        assert_eq!(parquet_exec.schema().as_ref(), &expected_schema);

        let mut results = parquet_exec.execute(0, task_ctx)?;
        let batch = results.next().await.unwrap()?;
        assert_eq!(batch.schema().as_ref(), &expected_schema);

        assert_snapshot!(batches_to_string(&[batch]),@r###"
            +----+----------+-------------+-------+-----+
            | id | bool_col | tinyint_col | month | day |
            +----+----------+-------------+-------+-----+
            | 4  | true     | 0           | 10    | 26  |
            | 5  | false    | 1           | 10    | 26  |
            | 6  | true     | 0           | 10    | 26  |
            | 7  | false    | 1           | 10    | 26  |
            | 2  | true     | 0           | 10    | 26  |
            | 3  | false    | 1           | 10    | 26  |
            | 0  | true     | 0           | 10    | 26  |
            | 1  | false    | 1           | 10    | 26  |
            +----+----------+-------------+-------+-----+
        "###);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_error() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let location = Path::from_filesystem_path(".")
            .unwrap()
            .child("invalid.parquet");

        let partitioned_file = PartitionedFile {
            object_meta: ObjectMeta {
                location,
                last_modified: Utc.timestamp_nanos(0),
                size: 1337,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let file_schema = Arc::new(Schema::empty());
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(file_schema)),
        )
        .with_file(partitioned_file)
        .build();

        let parquet_exec = DataSourceExec::from_data_source(config);

        let mut results = parquet_exec.execute(0, state.task_ctx())?;
        let batch = results.next().await.unwrap();
        // invalid file should produce an error to that effect
        assert_contains!(batch.unwrap_err().to_string(), "invalid.parquet not found");
        assert!(results.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_page_index_exec_metrics() {
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));
        let batch1 = create_batch(vec![("int", c1.clone())]);

        let filter = col("int").eq(lit(4_i32));

        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_page_index_predicate()
            .round_trip(vec![batch1])
            .await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        assert_snapshot!(batches_to_sort_string(&rt.batches.unwrap()),@r###"
            +-----+
            | int |
            +-----+
            | 4   |
            | 5   |
            +-----+
        "###);
        let (page_index_pruned, page_index_matched) =
            get_pruning_metric(&metrics, "page_index_rows_pruned");
        assert_eq!(page_index_pruned, 4);
        assert_eq!(page_index_matched, 2);
        assert!(
            get_value(&metrics, "page_index_eval_time") > 0,
            "no eval time in metrics: {metrics:#?}"
        );
    }

    /// Returns a string array with contents:
    /// "[Foo, null, bar, bar, bar, bar, zzz]"
    fn string_batch() -> RecordBatch {
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Foo"),
            None,
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("bar"),
            Some("zzz"),
        ]));

        // batch1: c1(string)
        create_batch(vec![("c1", c1.clone())])
    }

    #[tokio::test]
    async fn parquet_exec_metrics() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // c1 != 'bar'
        let filter = col("c1").not_eq(lit("bar"));

        // read/write them files:
        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // assert the batches and some metrics
        assert_snapshot!(batches_to_string(&rt.batches.unwrap()),@r###"
            +-----+
            | c1  |
            +-----+
            | Foo |
            | zzz |
            +-----+
        "###);

        // pushdown predicates have eliminated all 4 bar rows and the
        // null row for 5 rows total
        assert_eq!(get_value(&metrics, "pushdown_rows_pruned"), 5);
        assert_eq!(get_value(&metrics, "pushdown_rows_matched"), 2);
        assert!(
            get_value(&metrics, "row_pushdown_eval_time") > 0,
            "no pushdown eval time in metrics: {metrics:#?}"
        );
        assert!(
            get_value(&metrics, "statistics_eval_time") > 0,
            "no statistics eval time in metrics: {metrics:#?}"
        );
        assert!(
            get_value(&metrics, "bloom_filter_eval_time") > 0,
            "no Bloom Filter eval time in metrics: {metrics:#?}"
        );
    }

    #[tokio::test]
    async fn parquet_exec_display() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // c1 != 'bar'
        let filter = col("c1").not_eq(lit("bar"));

        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        let explain = rt.explain.unwrap();

        // check that there was a pruning predicate -> row groups got pruned
        assert_contains!(&explain, "predicate=c1@0 != bar");

        // there's a single row group, but we can check that it matched
        assert_contains!(
            &explain,
            "row_groups_pruned_statistics=1 total \u{2192} 1 matched"
        );

        // check the projection
        assert_contains!(&explain, "projection=[c1]");
    }

    #[tokio::test]
    async fn parquet_exec_metrics_with_multiple_predicates() {
        // Test that metrics are correctly calculated when multiple predicates
        // are pushed down (connected with AND). This ensures we don't double-count
        // rows when multiple predicates filter the data sequentially.

        // Create a batch with two columns: c1 (string) and c2 (int32)
        // Total: 10 rows
        let c1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("foo"), // 0 - passes c1 filter, fails c2 filter (5 <= 10)
            Some("bar"), // 1 - fails c1 filter
            Some("bar"), // 2 - fails c1 filter
            Some("baz"), // 3 - passes both filters (20 > 10)
            Some("foo"), // 4 - passes both filters (12 > 10)
            Some("bar"), // 5 - fails c1 filter
            Some("baz"), // 6 - passes both filters (25 > 10)
            Some("foo"), // 7 - passes c1 filter, fails c2 filter (7 <= 10)
            Some("bar"), // 8 - fails c1 filter
            Some("qux"), // 9 - passes both filters (30 > 10)
        ]));

        let c2: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(5),
            Some(15),
            Some(8),
            Some(20),
            Some(12),
            Some(9),
            Some(25),
            Some(7),
            Some(18),
            Some(30),
        ]));

        let batch = create_batch(vec![("c1", c1), ("c2", c2)]);

        // Create filter: c1 != 'bar' AND c2 > 10
        //
        // First predicate (c1 != 'bar'):
        //   - Rows passing: 0, 3, 4, 6, 7, 9 (6 rows)
        //   - Rows pruned: 1, 2, 5, 8 (4 rows)
        //
        // Second predicate (c2 > 10) on remaining 6 rows:
        //   - Rows passing: 3, 4, 6, 9 (4 rows with c2 = 20, 12, 25, 30)
        //   - Rows pruned: 0, 7 (2 rows with c2 = 5, 7)
        //
        // Expected final metrics:
        //   - pushdown_rows_matched: 4 (final result)
        //   - pushdown_rows_pruned: 4 + 2 = 6 (cumulative)
        //   - Total: 4 + 6 = 10

        let filter = col("c1").not_eq(lit("bar")).and(col("c2").gt(lit(10)));

        let rt = RoundTrip::new()
            .with_predicate(filter)
            .with_pushdown_predicate()
            .round_trip(vec![batch])
            .await;

        let metrics = rt.parquet_exec.metrics().unwrap();

        // Verify the result rows
        assert_snapshot!(batches_to_string(&rt.batches.unwrap()),@r###"
            +-----+----+
            | c1  | c2 |
            +-----+----+
            | baz | 20 |
            | foo | 12 |
            | baz | 25 |
            | qux | 30 |
            +-----+----+
        "###);

        // Verify metrics - this is the key test
        let pushdown_rows_matched = get_value(&metrics, "pushdown_rows_matched");
        let pushdown_rows_pruned = get_value(&metrics, "pushdown_rows_pruned");

        assert_eq!(
            pushdown_rows_matched, 4,
            "Expected 4 rows to pass both predicates"
        );
        assert_eq!(
            pushdown_rows_pruned, 6,
            "Expected 6 rows to be pruned (4 by first predicate + 2 by second predicate)"
        );

        // The sum should equal the total number of rows
        assert_eq!(
            pushdown_rows_matched + pushdown_rows_pruned,
            10,
            "matched + pruned should equal total rows"
        );
    }

    #[tokio::test]
    async fn parquet_exec_has_no_pruning_predicate_if_can_not_prune() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // filter is too complicated for pruning (PruningPredicate code does not
        // handle case expressions), so the pruning predicate will always be
        // "true"

        // WHEN c1 != bar THEN true ELSE false END
        let filter = when(col("c1").not_eq(lit("bar")), lit(true))
            .otherwise(lit(false))
            .unwrap();

        let rt = RoundTrip::new()
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .round_trip(vec![batch1])
            .await;

        // Should not contain a pruning predicate (since nothing can be pruned)
        let explain = rt.explain.unwrap();

        // When both matched and pruned are 0, it means that the pruning predicate
        // was not used at all.
        assert_contains!(
            &explain,
            "row_groups_pruned_statistics=1 total \u{2192} 1 matched"
        );

        // But pushdown predicate should be present
        assert_contains!(
            &explain,
            "predicate=CASE WHEN c1@0 != bar THEN true ELSE false END"
        );
        assert_contains!(&explain, "pushdown_rows_pruned=5");
    }

    #[tokio::test]
    async fn parquet_exec_has_pruning_predicate_for_guarantees() {
        // batch1: c1(string)
        let batch1 = string_batch();

        // part of the filter is too complicated for pruning (PruningPredicate code does not
        // handle case expressions), but part (c1 = 'foo') can be used for bloom filtering, so
        // should still have the pruning predicate.

        // c1 = 'foo' AND (WHEN c1 != bar THEN true ELSE false END)
        let filter = col("c1").eq(lit("foo")).and(
            when(col("c1").not_eq(lit("bar")), lit(true))
                .otherwise(lit(false))
                .unwrap(),
        );

        let rt = RoundTrip::new()
            .with_predicate(filter.clone())
            .with_pushdown_predicate()
            .with_bloom_filters()
            .round_trip(vec![batch1])
            .await;

        // Should have a pruning predicate
        let explain = rt.explain.unwrap();
        assert_contains!(
            &explain,
            "predicate=c1@0 = foo AND CASE WHEN c1@0 != bar THEN true ELSE false END"
        );

        // And bloom filters should have been evaluated
        assert_contains!(&explain, "row_groups_pruned_bloom_filter=1");
    }

    /// Returns the sum of all the metrics with the specified name
    /// the returned set.
    ///
    /// Count: returns value
    /// Time: returns elapsed nanoseconds
    ///
    /// Panics if no such metric.
    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => match v {
                MetricValue::PruningMetrics {
                    pruning_metrics, ..
                } => pruning_metrics.pruned(),
                _ => v.as_usize(),
            },
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }

    fn get_pruning_metric(metrics: &MetricsSet, metric_name: &str) -> (usize, usize) {
        match metrics.sum_by_name(metric_name) {
            Some(MetricValue::PruningMetrics {
                pruning_metrics, ..
            }) => (pruning_metrics.pruned(), pruning_metrics.matched()),
            Some(_) => panic!(
                "Metric '{metric_name}' is not a pruning metric in\n\n{metrics:#?}"
            ),
            None => panic!(
                "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
            ),
        }
    }

    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{partition}.{file_extension}");
            let file_path = tmp_dir.path().join(filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    #[tokio::test]
    async fn write_table_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        // let mut ctx = create_ctx(&tmp_dir, 4).await?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(8),
        );
        let schema = populate_csv_partitions(&tmp_dir, 4, ".csv")?;
        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // register a local file system object store for /tmp directory
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);

        // Configure listing options
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(ParquetFormat::default().get_ext());

        // execute a simple query and write the results to parquet
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        fs::create_dir(&out_dir).unwrap();
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        let schema = Arc::clone(df.schema().inner());
        // Register a listing table - this will use all files in the directory as data sources
        // for the query
        ctx.register_listing_table(
            "my_table",
            &out_dir,
            listing_options,
            Some(schema),
            None,
        )
        .await
        .unwrap();
        df.write_table("my_table", DataFrameWriteOptions::new())
            .await?;

        // create a new context and verify that the results were saved to a partitioned parquet file
        let ctx = SessionContext::new();

        // get write_id
        let mut paths = fs::read_dir(&out_dir).unwrap();
        let path = paths.next();
        let name = path
            .unwrap()?
            .path()
            .file_name()
            .expect("Should be a file name")
            .to_str()
            .expect("Should be a str")
            .to_owned();
        let (parsed_id, _) = name.split_once('_').expect("File should contain _ !");
        let write_id = parsed_id.to_owned();

        // register each partition as well as the top level dir
        ctx.register_parquet(
            "part0",
            &format!("{out_dir}/{write_id}_0.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_parquet("allparts", &out_dir, ParquetReadOptions::default())
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 40);

        Ok(())
    }

    #[tokio::test]
    async fn test_struct_filter_parquet() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let path = tmp_dir.path().to_str().unwrap().to_string() + "/test.parquet";
        write_file(&path);
        let ctx = SessionContext::new();
        let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));
        ctx.register_listing_table("base_table", path, opt, None, None)
            .await
            .unwrap();
        let sql = "select * from base_table where name='test02'";
        let batch = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batch.len(), 1);
        insta::assert_snapshot!(batches_to_string(&batch),@r###"
            +---------------------+----+--------+
            | struct              | id | name   |
            +---------------------+----+--------+
            | {id: 4, name: aaa2} | 2  | test02 |
            +---------------------+----+--------+
        "###);
        Ok(())
    }

    #[tokio::test]
    async fn test_struct_filter_parquet_with_view_types() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().to_str().unwrap().to_string() + "/test.parquet";
        write_file(&path);

        let ctx = SessionContext::new();

        let mut options = TableParquetOptions::default();
        options.global.schema_force_view_types = true;
        let opt =
            ListingOptions::new(Arc::new(ParquetFormat::default().with_options(options)));

        ctx.register_listing_table("base_table", path, opt, None, None)
            .await
            .unwrap();
        let sql = "select * from base_table where name='test02'";
        let batch = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batch.len(), 1);
        insta::assert_snapshot!(batches_to_string(&batch),@r###"
            +---------------------+----+--------+
            | struct              | id | name   |
            +---------------------+----+--------+
            | {id: 4, name: aaa2} | 2  | test02 |
            +---------------------+----+--------+
        "###);
        Ok(())
    }

    fn write_file(file: &String) {
        let struct_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![
            Field::new("struct", DataType::Struct(struct_fields.clone()), false),
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let id_array = Int64Array::from(vec![Some(1), Some(2)]);
        let columns = vec![
            Arc::new(Int64Array::from(vec![3, 4])) as _,
            Arc::new(StringArray::from(vec!["aaa1", "aaa2"])) as _,
        ];
        let struct_array = StructArray::new(struct_fields, columns, None);

        let name_array = StringArray::from(vec![Some("test01"), Some("test02")]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(struct_array),
                Arc::new(id_array),
                Arc::new(name_array),
            ],
        )
        .unwrap();
        let file = File::create(file).unwrap();
        let w_opt = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(w_opt)).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();
    }

    /// Write out a batch to a parquet file and return the total size of the file
    async fn write_batch(
        path: &str,
        store: Arc<dyn ObjectStore>,
        batch: RecordBatch,
    ) -> u64 {
        let mut writer =
            ArrowWriter::try_new(BytesMut::new().writer(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        let bytes = writer.into_inner().unwrap().into_inner().freeze();
        let total_size = bytes.len() as u64;
        let path = Path::from(path);
        let payload = object_store::PutPayload::from_bytes(bytes);
        store
            .put_opts(&path, payload, object_store::PutOptions::default())
            .await
            .unwrap();
        total_size
    }

    /// A ParquetFileReaderFactory that tracks the metadata_size_hint passed to it
    #[derive(Debug, Clone)]
    struct TrackingParquetFileReaderFactory {
        inner: Arc<dyn ParquetFileReaderFactory>,
        metadata_size_hint_calls: Arc<Mutex<Vec<Option<usize>>>>,
    }

    impl TrackingParquetFileReaderFactory {
        fn new(store: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner: Arc::new(DefaultParquetFileReaderFactory::new(store)) as _,
                metadata_size_hint_calls: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl ParquetFileReaderFactory for TrackingParquetFileReaderFactory {
        fn create_reader(
            &self,
            partition_index: usize,
            partitioned_file: PartitionedFile,
            metadata_size_hint: Option<usize>,
            metrics: &ExecutionPlanMetricsSet,
        ) -> Result<Box<dyn parquet::arrow::async_reader::AsyncFileReader + Send>>
        {
            self.metadata_size_hint_calls
                .lock()
                .unwrap()
                .push(metadata_size_hint);
            self.inner.create_reader(
                partition_index,
                partitioned_file,
                metadata_size_hint,
                metrics,
            )
        }
    }

    /// Test passing `metadata_size_hint` to either a single file or the whole exec
    #[tokio::test]
    async fn test_metadata_size_hint() {
        let store =
            Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;
        let store_url = ObjectStoreUrl::parse("memory://test").unwrap();

        let ctx = SessionContext::new();
        ctx.register_object_store(store_url.as_ref(), store.clone());

        // write some data out, it doesn't matter what it is
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let batch = create_batch(vec![("c1", c1)]);
        let schema = batch.schema();
        let name_1 = "test1.parquet";
        let name_2 = "test2.parquet";
        let total_size_1 = write_batch(name_1, store.clone(), batch.clone()).await;
        let total_size_2 = write_batch(name_2, store.clone(), batch.clone()).await;

        let reader_factory =
            Arc::new(TrackingParquetFileReaderFactory::new(store.clone()));

        let size_hint_calls = reader_factory.metadata_size_hint_calls.clone();

        let source = Arc::new(
            ParquetSource::new(Arc::clone(&schema))
                .with_parquet_file_reader_factory(reader_factory)
                .with_metadata_size_hint(456),
        );
        let config = FileScanConfigBuilder::new(store_url, source)
            .with_file(
                PartitionedFile {
                    object_meta: ObjectMeta {
                        location: Path::from(name_1),
                        last_modified: Utc::now(),
                        size: total_size_1,
                        e_tag: None,
                        version: None,
                    },
                    partition_values: vec![],
                    range: None,
                    statistics: None,
                    extensions: None,
                    metadata_size_hint: None,
                }
                .with_metadata_size_hint(123),
            )
            .with_file(PartitionedFile {
                object_meta: ObjectMeta {
                    location: Path::from(name_2),
                    last_modified: Utc::now(),
                    size: total_size_2,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![],
                range: None,
                statistics: None,
                extensions: None,
                metadata_size_hint: None,
            })
            .build();

        let exec = DataSourceExec::from_data_source(config);

        let res = collect(exec, ctx.task_ctx()).await.unwrap();
        assert_eq!(res.len(), 2);

        let calls = size_hint_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls, vec![Some(123), Some(456)]);
    }
}
