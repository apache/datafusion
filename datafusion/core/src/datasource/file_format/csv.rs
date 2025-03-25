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

//! Re-exports the [`datafusion_datasource_csv::file_format`] module, and contains tests for it.
pub use datafusion_datasource_csv::file_format::*;

#[cfg(test)]
mod tests {
    use std::fmt::{self, Display};
    use std::ops::Range;
    use std::sync::{Arc, Mutex};

    use super::*;

    use crate::datasource::file_format::test_util::scan_format;
    use crate::datasource::listing::ListingOptions;
    use crate::execution::session_state::SessionStateBuilder;
    use crate::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion_catalog::Session;
    use datafusion_common::cast::as_string_array;
    use datafusion_common::internal_err;
    use datafusion_common::stats::Precision;
    use datafusion_common::test_util::{arrow_test_data, batches_to_string};
    use datafusion_common::Result;
    use datafusion_datasource::decoder::{
        BatchDeserializer, DecoderDeserializer, DeserializerOutput,
    };
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_datasource::write::BatchSerializer;
    use datafusion_expr::{col, lit};
    use datafusion_physical_plan::{collect, ExecutionPlan};

    use arrow::array::{
        BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray,
    };
    use arrow::compute::concat_batches;
    use arrow::csv::ReaderBuilder;
    use arrow::util::pretty::pretty_format_batches;
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::DateTime;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use insta::assert_snapshot;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{
        Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };
    use regex::Regex;
    use rstest::*;

    /// Mock ObjectStore to provide an variable stream of bytes on get
    /// Able to keep track of how many iterations of the provided bytes were repeated
    #[derive(Debug)]
    struct VariableStream {
        bytes_to_repeat: Bytes,
        max_iterations: usize,
        iterations_detected: Arc<Mutex<usize>>,
    }

    impl Display for VariableStream {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "VariableStream")
        }
    }

    #[async_trait]
    impl ObjectStore for VariableStream {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
            let bytes = self.bytes_to_repeat.clone();
            let range = 0..bytes.len() * self.max_iterations;
            let arc = self.iterations_detected.clone();
            let stream = futures::stream::repeat_with(move || {
                let arc_inner = arc.clone();
                *arc_inner.lock().unwrap() += 1;
                Ok(bytes.clone())
            })
            .take(self.max_iterations)
            .boxed();

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: Default::default(),
                    size: range.end,
                    e_tag: None,
                    version: None,
                },
                range: Default::default(),
                attributes: Attributes::default(),
            })
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _opts: GetOptions,
        ) -> object_store::Result<GetResult> {
            unimplemented!()
        }

        async fn get_ranges(
            &self,
            _location: &Path,
            _ranges: &[Range<usize>],
        ) -> object_store::Result<Vec<Bytes>> {
            unimplemented!()
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(
            &self,
            _from: &Path,
            _to: &Path,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    impl VariableStream {
        pub fn new(bytes_to_repeat: Bytes, max_iterations: usize) -> Self {
            Self {
                bytes_to_repeat,
                max_iterations,
                iterations_detected: Arc::new(Mutex::new(0)),
            }
        }

        pub fn get_iterations_detected(&self) -> usize {
            *self.iterations_detected.lock().unwrap()
        }
    }

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        // skip column 9 that overflows the automatically discovered column type of i64 (u64 would work)
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, None, true).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(12, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 50 /* 100/2 */);

        // test metadata
        assert_eq!(exec.statistics()?.num_rows, Precision::Absent);
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0, 1, 2, 3]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, Some(1), true).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let projection = None;
        let root = "./tests/data/csv";
        let format = CsvFormat::default().with_has_header(true);
        let exec = scan_format(
            &state,
            &format,
            root,
            "aggregate_test_100_with_nulls.csv",
            projection,
            None,
        )
        .await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Utf8",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8",
                "c14: Null",
                "c15: Utf8"
            ],
            x
        );

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema_with_null_regex() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let projection = None;
        let root = "./tests/data/csv";
        let format = CsvFormat::default()
            .with_has_header(true)
            .with_null_regex(Some("^NULL$|^$".to_string()));
        let exec = scan_format(
            &state,
            &format,
            root,
            "aggregate_test_100_with_nulls.csv",
            projection,
            None,
        )
        .await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Utf8",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8",
                "c14: Null",
                "c15: Null"
            ],
            x
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_char_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, None, true).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(100, batches[0].num_rows());

        let array = as_string_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..5 {
            values.push(array.value(i));
        }

        assert_eq!(vec!["c", "d", "b", "a", "b"], values);

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_stream() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let variable_object_store =
            Arc::new(VariableStream::new(Bytes::from("1,2,3,4,5\n"), 200));
        let object_meta = ObjectMeta {
            location: Path::parse("/")?,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
            version: None,
        };

        let num_rows_to_read = 100;
        let csv_format = CsvFormat::default()
            .with_has_header(false)
            .with_schema_infer_max_rec(num_rows_to_read);
        let inferred_schema = csv_format
            .infer_schema(
                &state,
                &(variable_object_store.clone() as Arc<dyn ObjectStore>),
                &[object_meta],
            )
            .await?;

        let actual_fields: Vec<_> = inferred_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "column_1: Int64",
                "column_2: Int64",
                "column_3: Int64",
                "column_4: Int64",
                "column_5: Int64"
            ],
            actual_fields
        );
        // ensuring on csv infer that it won't try to read entire file
        // should only read as many rows as was configured in the CsvFormat
        assert_eq!(
            num_rows_to_read,
            variable_object_store.get_iterations_detected()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_escape_chars() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let variable_object_store = Arc::new(VariableStream::new(
            Bytes::from(
                r#"c1,c2,c3,c4
0.3,"Here, is a comma\"",third,3
0.31,"double quotes are ok, "" quote",third again,9
0.314,abc,xyz,27"#,
            ),
            1,
        ));
        let object_meta = ObjectMeta {
            location: Path::parse("/")?,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
            version: None,
        };

        let num_rows_to_read = 3;
        let csv_format = CsvFormat::default()
            .with_has_header(true)
            .with_schema_infer_max_rec(num_rows_to_read)
            .with_quote(b'"')
            .with_escape(Some(b'\\'));

        let inferred_schema = csv_format
            .infer_schema(
                &state,
                &(variable_object_store.clone() as Arc<dyn ObjectStore>),
                &[object_meta],
            )
            .await?;

        let actual_fields: Vec<_> = inferred_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();

        assert_eq!(
            vec!["c1: Float64", "c2: Utf8", "c3: Utf8", "c4: Int64",],
            actual_fields
        );
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn query_compress_data(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use arrow_schema::{DataType, Field, Schema};
        use datafusion_common::DataFusionError;
        use datafusion_datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
        use futures::TryStreamExt;

        let mut cfg = SessionConfig::new();
        cfg.options_mut().catalog.has_header = true;
        let session_state = SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .build();
        let integration = LocalFileSystem::new_with_prefix(arrow_test_data()).unwrap();
        let path = Path::from("csv/aggregate_test_100.csv");
        let csv = CsvFormat::default().with_has_header(true);
        let records_to_read = csv
            .options()
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);
        let store = Arc::new(integration) as Arc<dyn ObjectStore>;
        let original_stream = store.get(&path).await?;

        //convert original_stream to compressed_stream for next step
        let compressed_stream =
            file_compression_type.to_owned().convert_to_compress_stream(
                original_stream
                    .into_stream()
                    .map_err(DataFusionError::from)
                    .boxed(),
            );

        //prepare expected schema for assert_eq
        let expected = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
            Field::new("c4", DataType::Int64, true),
            Field::new("c5", DataType::Int64, true),
            Field::new("c6", DataType::Int64, true),
            Field::new("c7", DataType::Int64, true),
            Field::new("c8", DataType::Int64, true),
            Field::new("c9", DataType::Int64, true),
            Field::new("c10", DataType::Utf8, true),
            Field::new("c11", DataType::Float64, true),
            Field::new("c12", DataType::Float64, true),
            Field::new("c13", DataType::Utf8, true),
        ]);

        let compressed_csv = csv.with_file_compression_type(file_compression_type);

        //convert compressed_stream to decoded_stream
        let decoded_stream = compressed_csv
            .read_to_delimited_chunks_from_stream(compressed_stream.unwrap())
            .await;
        let (schema, records_read) = compressed_csv
            .infer_schema_from_stream(&session_state, records_to_read, decoded_stream)
            .await?;

        assert_eq!(expected, schema);
        assert_eq!(100, records_read);
        Ok(())
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn query_compress_csv() -> Result<()> {
        let ctx = SessionContext::new();

        let csv_options = CsvReadOptions::default()
            .has_header(true)
            .file_compression_type(FileCompressionType::GZIP)
            .file_extension("csv.gz");
        let df = ctx
            .read_csv(
                &format!("{}/csv/aggregate_test_100.csv.gz", arrow_test_data()),
                csv_options,
            )
            .await?;

        let record_batch = df
            .filter(col("c1").eq(lit("a")).and(col("c2").gt(lit("4"))))?
            .select_columns(&["c2", "c3"])?
            .collect()
            .await?;

        assert_snapshot!(batches_to_string(&record_batch), @r###"
            +----+------+
            | c2 | c3   |
            +----+------+
            | 5  | 36   |
            | 5  | -31  |
            | 5  | -101 |
            +----+------+
        "###);

        Ok(())
    }

    async fn get_exec(
        state: &dyn Session,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        has_header: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let root = format!("{}/csv", arrow_test_data());
        let format = CsvFormat::default().with_has_header(has_header);
        scan_format(state, &format, &root, file_name, projection, limit).await
    }

    #[tokio::test]
    async fn test_csv_serializer() -> Result<()> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv(
                &format!("{}/csv/aggregate_test_100.csv", arrow_test_data()),
                CsvReadOptions::default().has_header(true),
            )
            .await?;
        let batches = df
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(10))?
            .collect()
            .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        let serializer = CsvSerializer::new();
        let bytes = serializer.serialize(batch, true)?;
        assert_eq!(
            "c2,c3\n2,1\n5,-40\n1,29\n1,-85\n5,-82\n4,-111\n3,104\n3,13\n1,38\n4,-38\n",
            String::from_utf8(bytes.into()).unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_csv_serializer_no_header() -> Result<()> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv(
                &format!("{}/csv/aggregate_test_100.csv", arrow_test_data()),
                CsvReadOptions::default().has_header(true),
            )
            .await?;
        let batches = df
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(10))?
            .collect()
            .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        let serializer = CsvSerializer::new().with_header(false);
        let bytes = serializer.serialize(batch, true)?;
        assert_eq!(
            "2,1\n5,-40\n1,29\n1,-85\n5,-82\n4,-111\n3,104\n3,13\n1,38\n4,-38\n",
            String::from_utf8(bytes.into()).unwrap()
        );
        Ok(())
    }

    /// Explain the `sql` query under `ctx` to make sure the underlying csv scan is parallelized
    /// e.g. "DataSourceExec: file_groups={2 groups:" in plan means 2 DataSourceExec runs concurrently
    async fn count_query_csv_partitions(
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<usize> {
        let df = ctx.sql(&format!("EXPLAIN {sql}")).await?;
        let result = df.collect().await?;
        let plan = format!("{}", &pretty_format_batches(&result)?);

        let re = Regex::new(r"DataSourceExec: file_groups=\{(\d+) group").unwrap();

        if let Some(captures) = re.captures(&plan) {
            if let Some(match_) = captures.get(1) {
                let n_partitions = match_.as_str().parse::<usize>().unwrap();
                return Ok(n_partitions);
            }
        }

        internal_err!("query contains no DataSourceExec")
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn test_csv_parallel_basic(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);
        let testdata = arrow_test_data();
        ctx.register_csv(
            "aggr",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        let query = "select sum(c2) from aggr;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&query_result),@r###"
        +--------------+
        | sum(aggr.c2) |
        +--------------+
        | 285          |
        +--------------+
        "###);
        }

        assert_eq!(n_partitions, actual_partitions);

        Ok(())
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_csv_parallel_compressed(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let csv_options = CsvReadOptions::default()
            .has_header(true)
            .file_compression_type(FileCompressionType::GZIP)
            .file_extension("csv.gz");
        let ctx = SessionContext::new_with_config(config);
        let testdata = arrow_test_data();
        ctx.register_csv(
            "aggr",
            &format!("{testdata}/csv/aggregate_test_100.csv.gz"),
            csv_options,
        )
        .await?;

        let query = "select sum(c3) from aggr;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&query_result),@r###"
        +--------------+
        | sum(aggr.c3) |
        +--------------+
        | 781          |
        +--------------+
        "###);
        }

        assert_eq!(1, actual_partitions); // Compressed csv won't be scanned in parallel

        Ok(())
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn test_csv_parallel_newlines_in_values(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let csv_options = CsvReadOptions::default()
            .has_header(true)
            .newlines_in_values(true);
        let ctx = SessionContext::new_with_config(config);
        let testdata = arrow_test_data();
        ctx.register_csv(
            "aggr",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            csv_options,
        )
        .await?;

        let query = "select sum(c3) from aggr;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&query_result),@r###"
        +--------------+
        | sum(aggr.c3) |
        +--------------+
        | 781          |
        +--------------+
        "###);
        }

        assert_eq!(1, actual_partitions); // csv won't be scanned in parallel when newlines_in_values is set

        Ok(())
    }

    /// Read a single empty csv file
    ///
    /// empty_0_byte.csv:
    /// (file is empty)
    #[tokio::test]
    async fn test_csv_empty_file() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "empty",
            "tests/data/empty_0_byte.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await?;

        let query = "select * from empty where random() > 0.5;";
        let query_result = ctx.sql(query).await?.collect().await?;

        assert_snapshot!(batches_to_string(&query_result),@r###"
            ++
            ++
        "###);

        Ok(())
    }

    /// Read a single empty csv file with header
    ///
    /// empty.csv:
    /// c1,c2,c3
    #[tokio::test]
    async fn test_csv_empty_with_header() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "empty",
            "tests/data/empty.csv",
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        let query = "select * from empty where random() > 0.5;";
        let query_result = ctx.sql(query).await?.collect().await?;

        assert_snapshot!(batches_to_string(&query_result),@r###"
            ++
            ++
        "###);

        Ok(())
    }

    /// Read multiple empty csv files
    ///
    /// all_empty
    /// ├── empty0.csv
    /// ├── empty1.csv
    /// └── empty2.csv
    ///
    /// empty0.csv/empty1.csv/empty2.csv:
    /// (file is empty)
    #[tokio::test]
    async fn test_csv_multiple_empty_files() -> Result<()> {
        // Testing that partitioning doesn't break with empty files
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        let file_format = Arc::new(CsvFormat::default().with_has_header(false));
        let listing_options = ListingOptions::new(file_format.clone())
            .with_file_extension(file_format.get_ext());
        ctx.register_listing_table(
            "empty",
            "tests/data/empty_files/all_empty/",
            listing_options,
            None,
            None,
        )
        .await
        .unwrap();

        // Require a predicate to enable repartition for the optimizer
        let query = "select * from empty where random() > 0.5;";
        let query_result = ctx.sql(query).await?.collect().await?;

        assert_snapshot!(batches_to_string(&query_result),@r###"
            ++
            ++
        "###);

        Ok(())
    }

    /// Read multiple csv files (some are empty) in parallel
    ///
    /// some_empty
    /// ├── a_empty.csv
    /// ├── b.csv
    /// ├── c_empty.csv
    /// ├── d.csv
    /// └── e_empty.csv
    ///
    /// a_empty.csv/c_empty.csv/e_empty.csv:
    /// (file is empty)
    ///
    /// b.csv/d.csv:
    /// 1\n
    /// 1\n
    /// 1\n
    /// 1\n
    /// 1\n
    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn test_csv_parallel_some_file_empty(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);
        let file_format = Arc::new(CsvFormat::default().with_has_header(false));
        let listing_options = ListingOptions::new(file_format.clone())
            .with_file_extension(file_format.get_ext());
        ctx.register_listing_table(
            "empty",
            "tests/data/empty_files/some_empty",
            listing_options,
            None,
            None,
        )
        .await
        .unwrap();

        // Require a predicate to enable repartition for the optimizer
        let query = "select sum(column_1) from empty where column_1 > 0;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&query_result),@r###"
            +---------------------+
            | sum(empty.column_1) |
            +---------------------+
            | 10                  |
            +---------------------+
        "###);}

        assert_eq!(n_partitions, actual_partitions); // Won't get partitioned if all files are empty

        Ok(())
    }

    /// Parallel scan on a csv file with only 1 byte in each line
    /// Testing partition byte range land on line boundaries
    ///
    /// one_col.csv:
    /// 5\n
    /// 5\n
    /// (...10 rows total)
    #[rstest(n_partitions, case(1), case(2), case(3), case(5), case(10), case(32))]
    #[tokio::test]
    async fn test_csv_parallel_one_col(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);

        ctx.register_csv(
            "one_col",
            "tests/data/one_col.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await?;

        let query = "select sum(column_1) from one_col where column_1 > 0;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        let file_size = std::fs::metadata("tests/data/one_col.csv")?.len() as usize;
        // A 20-Byte file at most get partitioned into 20 chunks
        let expected_partitions = if n_partitions <= file_size {
            n_partitions
        } else {
            file_size
        };

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&query_result),@r###"
        +-----------------------+
        | sum(one_col.column_1) |
        +-----------------------+
        | 50                    |
        +-----------------------+
        "###);
        }

        assert_eq!(expected_partitions, actual_partitions);

        Ok(())
    }

    /// Parallel scan on a csv file with 2 wide rows
    /// The byte range of a partition might be within some line
    ///
    /// wode_rows.csv:
    /// 1, 1, ..., 1\n (100 columns total)
    /// 2, 2, ..., 2\n
    #[rstest(n_partitions, case(1), case(2), case(10), case(16))]
    #[tokio::test]
    async fn test_csv_parallel_wide_rows(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_csv(
            "wide_rows",
            "tests/data/wide_rows.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await?;

        let query = "select sum(column_1) + sum(column_33) + sum(column_50) + sum(column_77) + sum(column_100) as sum_of_5_cols from wide_rows where column_1 > 0;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&query_result),@r###"
            +---------------+
            | sum_of_5_cols |
            +---------------+
            | 15            |
            +---------------+
        "###);}

        assert_eq!(n_partitions, actual_partitions);

        Ok(())
    }

    #[rstest]
    fn test_csv_deserializer_with_finish(
        #[values(1, 5, 17)] batch_size: usize,
        #[values(0, 5, 93)] line_count: usize,
    ) -> Result<()> {
        let schema = csv_schema();
        let generator = CsvBatchGenerator::new(batch_size, line_count);
        let mut deserializer = csv_deserializer(batch_size, &schema);

        for data in generator {
            deserializer.digest(data);
        }
        deserializer.finish();

        let batch_count = line_count.div_ceil(batch_size);

        let mut all_batches = RecordBatch::new_empty(schema.clone());
        for _ in 0..batch_count {
            let output = deserializer.next()?;
            let DeserializerOutput::RecordBatch(batch) = output else {
                panic!("Expected RecordBatch, got {:?}", output);
            };
            all_batches = concat_batches(&schema, &[all_batches, batch])?;
        }
        assert_eq!(deserializer.next()?, DeserializerOutput::InputExhausted);

        let expected = csv_expected_batch(schema, line_count)?;

        assert_eq!(
            expected.clone(),
            all_batches.clone(),
            "Expected:\n{}\nActual:\n{}",
            pretty_format_batches(&[expected])?,
            pretty_format_batches(&[all_batches])?,
        );

        Ok(())
    }

    #[rstest]
    fn test_csv_deserializer_without_finish(
        #[values(1, 5, 17)] batch_size: usize,
        #[values(0, 5, 93)] line_count: usize,
    ) -> Result<()> {
        let schema = csv_schema();
        let generator = CsvBatchGenerator::new(batch_size, line_count);
        let mut deserializer = csv_deserializer(batch_size, &schema);

        for data in generator {
            deserializer.digest(data);
        }

        let batch_count = line_count / batch_size;

        let mut all_batches = RecordBatch::new_empty(schema.clone());
        for _ in 0..batch_count {
            let output = deserializer.next()?;
            let DeserializerOutput::RecordBatch(batch) = output else {
                panic!("Expected RecordBatch, got {:?}", output);
            };
            all_batches = concat_batches(&schema, &[all_batches, batch])?;
        }
        assert_eq!(deserializer.next()?, DeserializerOutput::RequiresMoreData);

        let expected = csv_expected_batch(schema, batch_count * batch_size)?;

        assert_eq!(
            expected.clone(),
            all_batches.clone(),
            "Expected:\n{}\nActual:\n{}",
            pretty_format_batches(&[expected])?,
            pretty_format_batches(&[all_batches])?,
        );

        Ok(())
    }

    struct CsvBatchGenerator {
        batch_size: usize,
        line_count: usize,
        offset: usize,
    }

    impl CsvBatchGenerator {
        fn new(batch_size: usize, line_count: usize) -> Self {
            Self {
                batch_size,
                line_count,
                offset: 0,
            }
        }
    }

    impl Iterator for CsvBatchGenerator {
        type Item = Bytes;

        fn next(&mut self) -> Option<Self::Item> {
            // Return `batch_size` rows per batch:
            let mut buffer = Vec::new();
            for _ in 0..self.batch_size {
                if self.offset >= self.line_count {
                    break;
                }
                buffer.extend_from_slice(&csv_line(self.offset));
                self.offset += 1;
            }

            (!buffer.is_empty()).then(|| buffer.into())
        }
    }

    fn csv_expected_batch(schema: SchemaRef, line_count: usize) -> Result<RecordBatch> {
        let mut c1 = Vec::with_capacity(line_count);
        let mut c2 = Vec::with_capacity(line_count);
        let mut c3 = Vec::with_capacity(line_count);
        let mut c4 = Vec::with_capacity(line_count);

        for i in 0..line_count {
            let (int_value, float_value, bool_value, char_value) = csv_values(i);
            c1.push(int_value);
            c2.push(float_value);
            c3.push(bool_value);
            c4.push(char_value);
        }

        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(c1)),
                Arc::new(Float64Array::from(c2)),
                Arc::new(BooleanArray::from(c3)),
                Arc::new(StringArray::from(c4)),
            ],
        )?;
        Ok(expected)
    }

    fn csv_line(line_number: usize) -> Bytes {
        let (int_value, float_value, bool_value, char_value) = csv_values(line_number);
        format!(
            "{},{},{},{}\n",
            int_value, float_value, bool_value, char_value
        )
        .into()
    }

    fn csv_values(line_number: usize) -> (i32, f64, bool, String) {
        let int_value = line_number as i32;
        let float_value = line_number as f64;
        let bool_value = line_number % 2 == 0;
        let char_value = format!("{}-string", line_number);
        (int_value, float_value, bool_value, char_value)
    }

    fn csv_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Boolean, true),
            Field::new("c4", DataType::Utf8, true),
        ]))
    }

    fn csv_deserializer(
        batch_size: usize,
        schema: &Arc<Schema>,
    ) -> impl BatchDeserializer<Bytes> {
        let decoder = ReaderBuilder::new(schema.clone())
            .with_batch_size(batch_size)
            .build_decoder();
        DecoderDeserializer::new(CsvDecoder::new(decoder))
    }
}
