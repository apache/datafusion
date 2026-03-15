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

use crate::execution::SessionState;
use async_trait::async_trait;
use datafusion_catalog_listing::{ListingOptions, ListingTableConfig};
use datafusion_common::{config_datafusion_err, internal_datafusion_err};
use datafusion_session::Session;
use futures::StreamExt;
use std::collections::HashMap;

/// Extension trait for [`ListingTableConfig`] that supports inferring schemas
///
/// This trait exists because the following inference methods only
/// work for [`SessionState`] implementations of [`Session`].
/// See [`ListingTableConfig`] for the remaining inference methods.
#[async_trait]
pub trait ListingTableConfigExt {
    /// Infer `ListingOptions` based on `table_path` and file suffix.
    ///
    /// The format is inferred based on the first `table_path`.
    async fn infer_options(
        self,
        state: &dyn Session,
    ) -> datafusion_common::Result<ListingTableConfig>;

    /// Convenience method to call both [`Self::infer_options`] and [`ListingTableConfig::infer_schema`]
    async fn infer(
        self,
        state: &dyn Session,
    ) -> datafusion_common::Result<ListingTableConfig>;
}

#[async_trait]
impl ListingTableConfigExt for ListingTableConfig {
    async fn infer_options(
        self,
        state: &dyn Session,
    ) -> datafusion_common::Result<ListingTableConfig> {
        let store = if let Some(url) = self.table_paths.first() {
            state.runtime_env().object_store(url)?
        } else {
            return Ok(self);
        };

        let file = self
            .table_paths
            .first()
            .unwrap()
            .list_all_files(state, store.as_ref(), "")
            .await?
            .next()
            .await
            .ok_or_else(|| internal_datafusion_err!("No files for table"))??;

        let (file_extension, maybe_compression_type) =
            ListingTableConfig::infer_file_extension_and_compression_type(
                file.location.as_ref(),
            )?;

        let mut format_options = HashMap::new();
        if let Some(ref compression_type) = maybe_compression_type {
            format_options
                .insert("format.compression".to_string(), compression_type.clone());
        }
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let file_format = state
            .get_file_format_factory(&file_extension)
            .ok_or(config_datafusion_err!(
                "No file_format found with extension {file_extension}"
            ))?
            .create(state, &format_options)?;

        let listing_file_extension =
            if let Some(compression_type) = maybe_compression_type {
                format!("{}.{}", &file_extension, &compression_type)
            } else {
                file_extension
            };

        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(listing_file_extension)
            .with_target_partitions(state.config().target_partitions())
            .with_collect_stat(state.config().collect_statistics());

        Ok(self.with_listing_options(listing_options))
    }

    async fn infer(self, state: &dyn Session) -> datafusion_common::Result<Self> {
        self.infer_options(state).await?.infer_schema(state).await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "parquet")]
    use crate::datasource::file_format::parquet::ParquetFormat;
    use crate::datasource::listing::table::ListingTableConfigExt;
    use crate::execution::options::JsonReadOptions;
    use crate::prelude::*;
    use crate::{
        datasource::{
            DefaultTableSource, MemTable, file_format::csv::CsvFormat,
            file_format::json::JsonFormat, provider_as_source,
        },
        execution::options::ArrowReadOptions,
        test::{
            columns, object_store::ensure_head_concurrency,
            object_store::make_test_store_and_state, object_store::register_test_store,
        },
    };
    use arrow::{compute::SortOptions, record_batch::RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion_catalog::TableProvider;
    use datafusion_catalog_listing::{
        ListingOptions, ListingTable, ListingTableConfig, SchemaSource,
    };
    use datafusion_common::{
        DataFusionError, Result, ScalarValue, assert_contains,
        stats::Precision,
        test_util::{batches_to_string, datafusion_test_data},
    };
    use datafusion_datasource::ListingTableUrl;
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_expr::dml::InsertOp;
    use datafusion_expr::{BinaryExpr, LogicalPlanBuilder, Operator};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::binary;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::{ExecutionPlanProperties, collect};
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;
    use url::Url;

    /// Creates a test schema with standard field types used in tests
    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Float32, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Boolean, true),
            Field::new("c4", DataType::Utf8, true),
        ]))
    }

    /// Helper function to generate test file paths with given prefix, count, and optional start index
    fn generate_test_files(prefix: &str, count: usize) -> Vec<String> {
        generate_test_files_with_start(prefix, count, 0)
    }

    /// Helper function to generate test file paths with given prefix, count, and start index
    fn generate_test_files_with_start(
        prefix: &str,
        count: usize,
        start_index: usize,
    ) -> Vec<String> {
        (start_index..start_index + count)
            .map(|i| format!("{prefix}/file{i}"))
            .collect()
    }

    #[tokio::test]
    async fn test_schema_source_tracking_comprehensive() -> Result<()> {
        let ctx = SessionContext::new();
        let testdata = datafusion_test_data();
        let filename = format!("{testdata}/aggregate_simple.csv");
        let table_path = ListingTableUrl::parse(filename)?;

        // Test default schema source
        let format = CsvFormat::default();
        let options = ListingOptions::new(Arc::new(format));
        let config =
            ListingTableConfig::new(table_path.clone()).with_listing_options(options);
        assert_eq!(config.schema_source(), SchemaSource::Unset);

        // Test schema source after setting a schema explicitly
        let provided_schema = create_test_schema();
        let config_with_schema = config.clone().with_schema(provided_schema.clone());
        assert_eq!(config_with_schema.schema_source(), SchemaSource::Specified);

        // Test schema source after inferring schema
        assert_eq!(config.schema_source(), SchemaSource::Unset);

        let config_with_inferred = config.infer_schema(&ctx.state()).await?;
        assert_eq!(config_with_inferred.schema_source(), SchemaSource::Inferred);

        // Test schema preservation through operations
        let config_with_schema_and_options = config_with_schema.clone();
        assert_eq!(
            config_with_schema_and_options.schema_source(),
            SchemaSource::Specified
        );

        // Make sure inferred schema doesn't override specified schema
        let config_with_schema_and_infer = config_with_schema_and_options
            .clone()
            .infer(&ctx.state())
            .await?;
        assert_eq!(
            config_with_schema_and_infer.schema_source(),
            SchemaSource::Specified
        );

        // Verify sources in actual ListingTable objects
        let table_specified = ListingTable::try_new(config_with_schema_and_options)?;
        assert_eq!(table_specified.schema_source(), SchemaSource::Specified);

        let table_inferred = ListingTable::try_new(config_with_inferred)?;
        assert_eq!(table_inferred.schema_source(), SchemaSource::Inferred);

        Ok(())
    }

    #[tokio::test]
    async fn read_single_file() -> Result<()> {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_collect_statistics(true),
        );

        let table = load_table(&ctx, "alltypes_plain.parquet").await?;
        let projection = None;
        let exec = table
            .scan(&ctx.state(), projection, &[], None)
            .await
            .expect("Scan table");

        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.output_partitioning().partition_count(), 1);

        // test metadata
        assert_eq!(
            exec.partition_statistics(None)?.num_rows,
            Precision::Exact(8)
        );
        assert_eq!(
            exec.partition_statistics(None)?.total_byte_size,
            Precision::Absent,
        );

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn test_try_create_output_ordering() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema = options.infer_schema(&state, &table_path).await.unwrap();

        use crate::datasource::file_format::parquet::ParquetFormat;
        use datafusion_physical_plan::expressions::col as physical_col;
        use datafusion_physical_plan::expressions::lit as physical_lit;
        use std::ops::Add;

        // (file_sort_order, expected_result)
        let cases = vec![
            (
                vec![],
                Ok::<Vec<LexOrdering>, DataFusionError>(Vec::<LexOrdering>::new()),
            ),
            // sort expr, but non column
            (
                vec![vec![col("int_col").add(lit(1)).sort(true, true)]],
                Ok(vec![
                    [PhysicalSortExpr {
                        expr: binary(
                            physical_col("int_col", &schema).unwrap(),
                            Operator::Plus,
                            physical_lit(1),
                            &schema,
                        )
                        .unwrap(),
                        options: SortOptions {
                            descending: false,
                            nulls_first: true,
                        },
                    }]
                    .into(),
                ]),
            ),
            // ok with one column
            (
                vec![vec![col("string_col").sort(true, false)]],
                Ok(vec![
                    [PhysicalSortExpr {
                        expr: physical_col("string_col", &schema).unwrap(),
                        options: SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    }]
                    .into(),
                ]),
            ),
            // ok with two columns, different options
            (
                vec![vec![
                    col("string_col").sort(true, false),
                    col("int_col").sort(false, true),
                ]],
                Ok(vec![
                    [
                        PhysicalSortExpr::new_default(
                            physical_col("string_col", &schema).unwrap(),
                        )
                        .asc()
                        .nulls_last(),
                        PhysicalSortExpr::new_default(
                            physical_col("int_col", &schema).unwrap(),
                        )
                        .desc()
                        .nulls_first(),
                    ]
                    .into(),
                ]),
            ),
        ];

        for (file_sort_order, expected_result) in cases {
            let options = options.clone().with_file_sort_order(file_sort_order);

            let config = ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(schema.clone());

            let table =
                ListingTable::try_new(config.clone()).expect("Creating the table");
            let ordering_result =
                table.try_create_output_ordering(state.execution_props(), &[]);

            match (expected_result, ordering_result) {
                (Ok(expected), Ok(result)) => {
                    assert_eq!(expected, result);
                }
                (Err(expected), Err(result)) => {
                    // can't compare the DataFusionError directly
                    let result = result.to_string();
                    let expected = expected.to_string();
                    assert_contains!(result.to_string(), expected);
                }
                (expected_result, ordering_result) => {
                    panic!(
                        "expected: {expected_result:#?}\n\nactual:{ordering_result:#?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn read_empty_table() -> Result<()> {
        let ctx = SessionContext::new();
        let path = String::from("table/p1=v1/file.json");
        register_test_store(&ctx, &[(&path, 100)]);

        let format = JsonFormat::default();
        let ext = format.get_ext();

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension(ext)
            .with_table_partition_cols(vec![(String::from("p1"), DataType::Utf8)])
            .with_target_partitions(4);

        let table_path = ListingTableUrl::parse("test:///table/")?;
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(file_schema);
        let table = ListingTable::try_new(config)?;

        assert_eq!(
            columns(&table.schema()),
            vec!["a".to_owned(), "p1".to_owned()]
        );

        // this will filter out the only file in the store
        let filter = Expr::not_eq(col("p1"), lit("v1"));

        let scan = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("Empty execution plan");

        assert!(scan.as_any().is::<EmptyExec>());
        assert_eq!(
            columns(&scan.schema()),
            vec!["a".to_owned(), "p1".to_owned()]
        );

        Ok(())
    }

    async fn load_table(
        ctx: &SessionContext,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{testdata}/{name}");
        let table_path = ListingTableUrl::parse(filename)?;

        let config = ListingTableConfig::new(table_path)
            .infer(&ctx.state())
            .await?;
        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`.
    async fn assert_list_files_for_scan_grouping(
        files: &[&str],
        table_prefix: &str,
        target_partitions: usize,
        output_partitioning: usize,
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let opt = ListingOptions::new(Arc::new(JsonFormat::default()))
            .with_file_extension_opt(file_ext)
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_path = ListingTableUrl::parse(table_prefix)?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let result = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(result.file_groups.len(), output_partitioning);

        Ok(())
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`.
    async fn assert_list_files_for_multi_paths(
        files: &[&str],
        table_prefix: &[&str],
        target_partitions: usize,
        output_partitioning: usize,
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let opt = ListingOptions::new(Arc::new(JsonFormat::default()))
            .with_file_extension_opt(file_ext)
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_paths = table_prefix
            .iter()
            .map(|t| ListingTableUrl::parse(t).unwrap())
            .collect();
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let result = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(result.file_groups.len(), output_partitioning);

        Ok(())
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`, and validate that file metadata is fetched
    /// concurrently
    async fn assert_list_files_for_exact_paths(
        files: &[&str],
        target_partitions: usize,
        output_partitioning: usize,
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let (store, _) = make_test_store_and_state(
            &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>(),
        );

        let meta_fetch_concurrency = ctx
            .state()
            .config_options()
            .execution
            .meta_fetch_concurrency;
        let expected_concurrency = files.len().min(meta_fetch_concurrency);
        let head_concurrency_store = ensure_head_concurrency(store, expected_concurrency);

        let url = Url::parse("test://").unwrap();
        ctx.register_object_store(&url, head_concurrency_store.clone());

        let format = JsonFormat::default();

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension_opt(file_ext)
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_paths = files
            .iter()
            .map(|t| ListingTableUrl::parse(format!("test:///{t}")).unwrap())
            .collect();
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let result = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(result.file_groups.len(), output_partitioning);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults() -> Result<()> {
        helper_test_insert_into_sql("csv", FileCompressionType::UNCOMPRESSED, "", None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults_header_row() -> Result<()> {
        helper_test_insert_into_sql(
            "csv",
            FileCompressionType::UNCOMPRESSED,
            "",
            Some(HashMap::from([("has_header".into(), "true".into())])),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_json_defaults() -> Result<()> {
        helper_test_insert_into_sql("json", FileCompressionType::UNCOMPRESSED, "", None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_parquet_defaults() -> Result<()> {
        helper_test_insert_into_sql(
            "parquet",
            FileCompressionType::UNCOMPRESSED,
            "",
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_parquet_session_overrides() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd(5)".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_enabled".into(),
            "false".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_page_size_limit".into(),
            "100".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.statistics_enabled".into(),
            "none".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_statistics_size".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_row_group_size".into(),
            "5".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.created_by".into(),
            "datafusion test".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.column_index_truncate_length".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.data_page_row_count_limit".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_on_write".into(),
            "true".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_fpp".into(),
            "0.01".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_ndv".into(),
            "1000".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.writer_version".into(),
            "2.0".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.write_batch_size".into(),
            "5".into(),
        );
        helper_test_insert_into_sql(
            "parquet",
            FileCompressionType::UNCOMPRESSED,
            "",
            Some(config_map),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_session_overrides() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd(5)".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_enabled".into(),
            "false".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_page_size_limit".into(),
            "100".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.statistics_enabled".into(),
            "none".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_statistics_size".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_row_group_size".into(),
            "5".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.created_by".into(),
            "datafusion test".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.column_index_truncate_length".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.data_page_row_count_limit".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.encoding".into(),
            "delta_binary_packed".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_on_write".into(),
            "true".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_fpp".into(),
            "0.01".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_ndv".into(),
            "1000".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.writer_version".into(),
            "2.0".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.write_batch_size".into(),
            "5".into(),
        );
        config_map.insert("datafusion.execution.batch_size".into(), "10".into());
        helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_invalid_session_fails()
    -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd".into(),
        );
        let e = helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await
        .expect_err("Example should fail!");
        assert_eq!(
            e.strip_backtrace(),
            "Invalid or Unsupported Configuration: zstd compression requires specifying a level such as zstd(4)"
        );

        Ok(())
    }

    async fn helper_test_append_new_files_to_table(
        file_type_ext: String,
        file_compression_type: FileCompressionType,
        session_config_map: Option<HashMap<String, String>>,
        expected_n_files_per_insert: usize,
    ) -> Result<()> {
        // Create the initial context, schema, and batch.
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(&cfg)?;
                SessionContext::new_with_config(config)
            }
            None => SessionContext::new(),
        };

        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new(
            "column1",
            DataType::Int32,
            false,
        )]));

        let filter_predicate = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column("column1".into())),
            Operator::GtEq,
            Box::new(Expr::Literal(ScalarValue::Int32(Some(0)), None)),
        ));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            ]))],
        )?;

        // Register appropriate table depending on file_type we want to test
        let tmp_dir = TempDir::new()?;
        match file_type_ext.as_str() {
            "csv" => {
                session_ctx
                    .register_csv(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        CsvReadOptions::new()
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            "json" => {
                session_ctx
                    .register_json(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        JsonReadOptions::default()
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            #[cfg(feature = "parquet")]
            "parquet" => {
                session_ctx
                    .register_parquet(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ParquetReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            #[cfg(feature = "avro")]
            "avro" => {
                session_ctx
                    .register_avro(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        AvroReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            "arrow" => {
                session_ctx
                    .register_arrow(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ArrowReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            _ => panic!("Unrecognized file extension {file_type_ext}"),
        }

        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        let target = session_ctx.table_provider("t").await?;
        let target = Arc::new(DefaultTableSource::new(target));
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?
            .filter(filter_predicate)?
            .build()?;
        // Since logical plan contains a filter, increasing parallelism is helpful.
        // Therefore, we will have 8 partitions in the final plan.
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", target, InsertOp::Append)?
                .build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;
        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        // Insert returns the number of rows written, in our case this would be 6.

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&res),@r"
        +-------+
        | count |
        +-------+
        | 20    |
        +-------+
        ");}

        // Read the records in the table
        let batches = session_ctx
            .sql("select count(*) as count from t")
            .await?
            .collect()
            .await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&batches),@r"
        +-------+
        | count |
        +-------+
        | 20    |
        +-------+
        ");}

        // Assert that `target_partition_number` many files were added to the table.
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, expected_n_files_per_insert);

        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Again, execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&res),@r"
        +-------+
        | count |
        +-------+
        | 20    |
        +-------+
        ");}

        // Read the contents of the table
        let batches = session_ctx
            .sql("select count(*) AS count from t")
            .await?
            .collect()
            .await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&batches),@r"
        +-------+
        | count |
        +-------+
        | 40    |
        +-------+
        ");}

        // Assert that another `target_partition_number` many files were added to the table.
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, expected_n_files_per_insert * 2);

        // Return Ok if the function
        Ok(())
    }

    /// tests insert into with end to end sql
    /// create external table + insert into statements
    async fn helper_test_insert_into_sql(
        file_type: &str,
        // TODO test with create statement options such as compression
        _file_compression_type: FileCompressionType,
        external_table_options: &str,
        session_config_map: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // Create the initial context
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(&cfg)?;
                SessionContext::new_with_config(config)
            }
            None => SessionContext::new(),
        };

        // create table
        let tmp_dir = TempDir::new()?;
        let str_path = tmp_dir
            .path()
            .to_str()
            .expect("Temp path should convert to &str");
        session_ctx
            .sql(&format!(
                "create external table foo(a varchar, b varchar, c int) \
                        stored as {file_type} \
                        location '{str_path}' \
                        {external_table_options}"
            ))
            .await?
            .collect()
            .await?;

        // insert data
        session_ctx.sql("insert into foo values ('foo', 'bar', 1),('foo', 'bar', 2), ('foo', 'bar', 3)")
            .await?
            .collect()
            .await?;

        // check count
        let batches = session_ctx
            .sql("select * from foo")
            .await?
            .collect()
            .await?;

        insta::allow_duplicates! {insta::assert_snapshot!(batches_to_string(&batches),@r"
        +-----+-----+---+
        | a   | b   | c |
        +-----+-----+---+
        | foo | bar | 1 |
        | foo | bar | 2 |
        | foo | bar | 3 |
        +-----+-----+---+
        ");}

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_options_compressed_csv() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/csv/aggregate_test_100.csv.gz");
        let table_path = ListingTableUrl::parse(filename)?;

        let ctx = SessionContext::new();

        let config = ListingTableConfig::new(table_path);
        let config_with_opts = config.infer_options(&ctx.state()).await?;
        let config_with_schema = config_with_opts.infer_schema(&ctx.state()).await?;

        let schema = config_with_schema.file_schema.unwrap();

        assert_eq!(schema.fields.len(), 13);

        Ok(())
    }

    #[tokio::test]
    async fn infer_preserves_provided_schema() -> Result<()> {
        let ctx = SessionContext::new();

        let testdata = datafusion_test_data();
        let filename = format!("{testdata}/aggregate_simple.csv");
        let table_path = ListingTableUrl::parse(filename)?;

        let provided_schema = create_test_schema();

        let format = CsvFormat::default();
        let options = ListingOptions::new(Arc::new(format));
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(Arc::clone(&provided_schema));

        let config = config.infer(&ctx.state()).await?;

        assert_eq!(*config.file_schema.unwrap(), *provided_schema);

        Ok(())
    }

    #[tokio::test]
    async fn test_listing_table_config_with_multiple_files_comprehensive() -> Result<()> {
        let ctx = SessionContext::new();

        // Create test files with different schemas
        let tmp_dir = TempDir::new()?;
        let file_path1 = tmp_dir.path().join("file1.csv");
        let file_path2 = tmp_dir.path().join("file2.csv");

        // File 1: c1,c2,c3
        let mut file1 = std::fs::File::create(&file_path1)?;
        writeln!(file1, "c1,c2,c3")?;
        writeln!(file1, "1,2,3")?;
        writeln!(file1, "4,5,6")?;

        // File 2: c1,c2,c3,c4
        let mut file2 = std::fs::File::create(&file_path2)?;
        writeln!(file2, "c1,c2,c3,c4")?;
        writeln!(file2, "7,8,9,10")?;
        writeln!(file2, "11,12,13,14")?;

        // Parse paths
        let table_path1 = ListingTableUrl::parse(file_path1.to_str().unwrap())?;
        let table_path2 = ListingTableUrl::parse(file_path2.to_str().unwrap())?;

        // Create format and options
        let format = CsvFormat::default().with_has_header(true);
        let options = ListingOptions::new(Arc::new(format));

        // Test case 1: Infer schema using first file's schema
        let config1 = ListingTableConfig::new_with_multi_paths(vec![
            table_path1.clone(),
            table_path2.clone(),
        ])
        .with_listing_options(options.clone());
        let config1 = config1.infer_schema(&ctx.state()).await?;
        assert_eq!(config1.schema_source(), SchemaSource::Inferred);

        // Verify schema matches first file
        let schema1 = config1.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema1.fields().len(), 3);
        assert_eq!(schema1.field(0).name(), "c1");
        assert_eq!(schema1.field(1).name(), "c2");
        assert_eq!(schema1.field(2).name(), "c3");

        // Test case 2: Use specified schema with 3 columns
        let schema_3cols = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Utf8, true),
        ]));

        let config2 = ListingTableConfig::new_with_multi_paths(vec![
            table_path1.clone(),
            table_path2.clone(),
        ])
        .with_listing_options(options.clone())
        .with_schema(schema_3cols);
        let config2 = config2.infer_schema(&ctx.state()).await?;
        assert_eq!(config2.schema_source(), SchemaSource::Specified);

        // Verify that the schema is still the one we specified (3 columns)
        let schema2 = config2.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema2.fields().len(), 3);
        assert_eq!(schema2.field(0).name(), "c1");
        assert_eq!(schema2.field(1).name(), "c2");
        assert_eq!(schema2.field(2).name(), "c3");

        // Test case 3: Use specified schema with 4 columns
        let schema_4cols = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c4", DataType::Utf8, true),
        ]));

        let config3 = ListingTableConfig::new_with_multi_paths(vec![
            table_path1.clone(),
            table_path2.clone(),
        ])
        .with_listing_options(options.clone())
        .with_schema(schema_4cols);
        let config3 = config3.infer_schema(&ctx.state()).await?;
        assert_eq!(config3.schema_source(), SchemaSource::Specified);

        // Verify that the schema is still the one we specified (4 columns)
        let schema3 = config3.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema3.fields().len(), 4);
        assert_eq!(schema3.field(0).name(), "c1");
        assert_eq!(schema3.field(1).name(), "c2");
        assert_eq!(schema3.field(2).name(), "c3");
        assert_eq!(schema3.field(3).name(), "c4");

        // Test case 4: Verify order matters when inferring schema
        let config4 = ListingTableConfig::new_with_multi_paths(vec![
            table_path2.clone(),
            table_path1.clone(),
        ])
        .with_listing_options(options);
        let config4 = config4.infer_schema(&ctx.state()).await?;

        // Should use first file's schema, which now has 4 columns
        let schema4 = config4.file_schema.as_ref().unwrap().clone();
        assert_eq!(schema4.fields().len(), 4);
        assert_eq!(schema4.field(0).name(), "c1");
        assert_eq!(schema4.field(1).name(), "c2");
        assert_eq!(schema4.field(2).name(), "c3");
        assert_eq!(schema4.field(3).name(), "c4");

        Ok(())
    }

    #[tokio::test]
    async fn test_list_files_configurations() -> Result<()> {
        // Define common test cases as (description, files, paths, target_partitions, expected_partitions, file_ext)
        let test_cases = vec![
            // Single path cases
            (
                "Single path, more partitions than files",
                generate_test_files("bucket/key-prefix", 5),
                vec!["test:///bucket/key-prefix/"],
                12,
                5,
                Some(""),
            ),
            (
                "Single path, equal partitions and files",
                generate_test_files("bucket/key-prefix", 4),
                vec!["test:///bucket/key-prefix/"],
                4,
                4,
                Some(""),
            ),
            (
                "Single path, more files than partitions",
                generate_test_files("bucket/key-prefix", 5),
                vec!["test:///bucket/key-prefix/"],
                2,
                2,
                Some(""),
            ),
            // Multi path cases
            (
                "Multi path, more partitions than files",
                {
                    let mut files = generate_test_files("bucket/key1", 3);
                    files.extend(generate_test_files_with_start("bucket/key2", 2, 3));
                    files.extend(generate_test_files_with_start("bucket/key3", 1, 5));
                    files
                },
                vec!["test:///bucket/key1/", "test:///bucket/key2/"],
                12,
                5,
                Some(""),
            ),
            // No files case
            (
                "No files",
                vec![],
                vec!["test:///bucket/key-prefix/"],
                2,
                0,
                Some(""),
            ),
            // Exact path cases
            (
                "Exact paths test",
                {
                    let mut files = generate_test_files("bucket/key1", 3);
                    files.extend(generate_test_files_with_start("bucket/key2", 2, 3));
                    files
                },
                vec![
                    "test:///bucket/key1/file0",
                    "test:///bucket/key1/file1",
                    "test:///bucket/key1/file2",
                    "test:///bucket/key2/file3",
                    "test:///bucket/key2/file4",
                ],
                12,
                5,
                Some(""),
            ),
        ];

        // Run each test case
        for (test_name, files, paths, target_partitions, expected_partitions, file_ext) in
            test_cases
        {
            println!("Running test: {test_name}");

            if files.is_empty() {
                // Test empty files case
                assert_list_files_for_multi_paths(
                    &[],
                    &paths,
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            } else if paths.len() == 1 {
                // Test using single path API
                let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
                assert_list_files_for_scan_grouping(
                    &file_refs,
                    paths[0],
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            } else if paths[0].contains("test:///bucket/key") {
                // Test using multi path API
                let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
                assert_list_files_for_multi_paths(
                    &file_refs,
                    &paths,
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            } else {
                // Test using exact path API for specific cases
                let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();
                assert_list_files_for_exact_paths(
                    &file_refs,
                    target_partitions,
                    expected_partitions,
                    file_ext,
                )
                .await?;
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_listing_table_prunes_extra_files_in_hive() -> Result<()> {
        let files = [
            "bucket/test/pid=1/file1",
            "bucket/test/pid=1/file2",
            "bucket/test/pid=2/file3",
            "bucket/test/pid=2/file4",
            "bucket/test/other/file5",
        ];

        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let opt = ListingOptions::new(Arc::new(JsonFormat::default()))
            .with_file_extension_opt(Some(""))
            .with_table_partition_cols(vec![("pid".to_string(), DataType::Int32)]);

        let table_path = ListingTableUrl::parse("test:///bucket/test/").unwrap();
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let result = table.list_files_for_scan(&ctx.state(), &[], None).await?;
        assert_eq!(result.file_groups.len(), 1);

        let files = result.file_groups[0].clone();

        assert_eq!(
            files
                .iter()
                .map(|f| f.path().to_string())
                .collect::<Vec<_>>(),
            vec![
                "bucket/test/pid=1/file1",
                "bucket/test/pid=1/file2",
                "bucket/test/pid=2/file3",
                "bucket/test/pid=2/file4",
            ]
        );

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn test_table_stats_behaviors() -> Result<()> {
        use crate::datasource::file_format::parquet::ParquetFormat;

        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename)?;

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Test 1: Default behavior - stats not collected
        let opt_default = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema_default = opt_default.infer_schema(&state, &table_path).await?;
        let config_default = ListingTableConfig::new(table_path.clone())
            .with_listing_options(opt_default)
            .with_schema(schema_default);

        let table_default = ListingTable::try_new(config_default)?;

        let exec_default = table_default.scan(&state, None, &[], None).await?;
        assert_eq!(
            exec_default.partition_statistics(None)?.num_rows,
            Precision::Absent
        );

        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        assert_eq!(
            exec_default.partition_statistics(None)?.total_byte_size,
            Precision::Absent
        );

        // Test 2: Explicitly disable stats
        let opt_disabled = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_collect_stat(false);
        let schema_disabled = opt_disabled.infer_schema(&state, &table_path).await?;
        let config_disabled = ListingTableConfig::new(table_path.clone())
            .with_listing_options(opt_disabled)
            .with_schema(schema_disabled);
        let table_disabled = ListingTable::try_new(config_disabled)?;

        let exec_disabled = table_disabled.scan(&state, None, &[], None).await?;
        assert_eq!(
            exec_disabled.partition_statistics(None)?.num_rows,
            Precision::Absent
        );
        assert_eq!(
            exec_disabled.partition_statistics(None)?.total_byte_size,
            Precision::Absent
        );

        // Test 3: Explicitly enable stats
        let opt_enabled = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_collect_stat(true);
        let schema_enabled = opt_enabled.infer_schema(&state, &table_path).await?;
        let config_enabled = ListingTableConfig::new(table_path)
            .with_listing_options(opt_enabled)
            .with_schema(schema_enabled);
        let table_enabled = ListingTable::try_new(config_enabled)?;

        let exec_enabled = table_enabled.scan(&state, None, &[], None).await?;
        assert_eq!(
            exec_enabled.partition_statistics(None)?.num_rows,
            Precision::Exact(8)
        );
        // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
        assert_eq!(
            exec_enabled.partition_statistics(None)?.total_byte_size,
            Precision::Absent,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_parameterized() -> Result<()> {
        let test_cases = vec![
            // (file_format, batch_size, soft_max_rows, expected_files)
            ("json", 10, 10, 2),
            ("csv", 10, 10, 2),
            #[cfg(feature = "parquet")]
            ("parquet", 10, 10, 2),
            #[cfg(feature = "parquet")]
            ("parquet", 20, 20, 1),
        ];

        for (format, batch_size, soft_max_rows, expected_files) in test_cases {
            println!(
                "Testing insert with format: {format}, batch_size: {batch_size}, expected files: {expected_files}"
            );

            let mut config_map = HashMap::new();
            config_map.insert(
                "datafusion.execution.batch_size".into(),
                batch_size.to_string(),
            );
            config_map.insert(
                "datafusion.execution.soft_max_rows_per_output_file".into(),
                soft_max_rows.to_string(),
            );

            let file_extension = match format {
                "json" => JsonFormat::default().get_ext(),
                "csv" => CsvFormat::default().get_ext(),
                #[cfg(feature = "parquet")]
                "parquet" => ParquetFormat::default().get_ext(),
                _ => unreachable!("Unsupported format"),
            };

            helper_test_append_new_files_to_table(
                file_extension,
                FileCompressionType::UNCOMPRESSED,
                Some(config_map),
                expected_files,
            )
            .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_table_scan() -> Result<()> {
        let ctx = SessionContext::new();

        // Test basic table creation and scanning
        let path = "table/file.json";
        register_test_store(&ctx, &[(path, 10)]);

        let format = JsonFormat::default();
        let opt = ListingOptions::new(Arc::new(format)).with_collect_stat(false);
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let table_path = ListingTableUrl::parse("test:///table/")?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        // The scan should work correctly
        let scan_result = table.scan(&ctx.state(), None, &[], None).await;
        assert!(scan_result.is_ok(), "Scan should succeed");

        // Verify file listing works
        let result = table.list_files_for_scan(&ctx.state(), &[], None).await?;
        assert!(
            !result.file_groups.is_empty(),
            "Should list files successfully"
        );

        Ok(())
    }
}
