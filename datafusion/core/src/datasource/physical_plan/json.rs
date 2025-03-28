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

//! Reexports the [`datafusion_datasource_json::source`] module, containing JSON based [`FileSource`].
//!
//! [`FileSource`]: datafusion_datasource::file::FileSource

#[allow(deprecated)]
pub use datafusion_datasource_json::source::*;

#[cfg(test)]
mod tests {

    use super::*;

    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use crate::dataframe::DataFrameWriteOptions;
    use crate::execution::SessionState;
    use crate::prelude::{CsvReadOptions, NdJsonReadOptions, SessionContext};
    use crate::test::partitioned_file_groups;
    use datafusion_common::cast::{as_int32_array, as_int64_array, as_string_array};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::Result;
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_datasource_json::JsonFormat;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::ExecutionPlan;

    use arrow::array::Array;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::{Field, SchemaBuilder};
    use datafusion_datasource::file_groups::FileGroup;
    use insta::assert_snapshot;
    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use rstest::*;
    use tempfile::TempDir;
    use url::Url;

    const TEST_DATA_BASE: &str = "tests/data";

    async fn prepare_store(
        state: &SessionState,
        file_compression_type: FileCompressionType,
        work_dir: &Path,
    ) -> (ObjectStoreUrl, Vec<FileGroup>, SchemaRef) {
        let store_url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&store_url).unwrap();

        let filename = "1.json";
        let file_groups = partitioned_file_groups(
            TEST_DATA_BASE,
            filename,
            1,
            Arc::new(JsonFormat::default()),
            file_compression_type.to_owned(),
            work_dir,
        )
        .unwrap();
        let meta = file_groups
            .first()
            .unwrap()
            .files()
            .first()
            .unwrap()
            .clone()
            .object_meta;
        let schema = JsonFormat::default()
            .with_file_compression_type(file_compression_type.to_owned())
            .infer_schema(state, &store, std::slice::from_ref(&meta))
            .await
            .unwrap();

        (store_url, file_groups, schema)
    }

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.register_object_store(&url, store.clone());
        let filename = "1.json";
        let tmp_dir = TempDir::new()?;
        let file_groups = partitioned_file_groups(
            TEST_DATA_BASE,
            filename,
            1,
            Arc::new(JsonFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )
        .unwrap();
        let path = file_groups
            .first()
            .unwrap()
            .files()
            .first()
            .unwrap()
            .object_meta
            .location
            .as_ref();

        let store_url = ObjectStoreUrl::local_filesystem();
        let url: &Url = store_url.as_ref();
        let path_buf = Path::new(url.path()).join(path);
        let path = path_buf.to_str().unwrap();

        let ext = JsonFormat::default()
            .get_ext_with_compression(&file_compression_type)
            .unwrap();

        let read_options = NdJsonReadOptions::default()
            .file_extension(ext.as_str())
            .file_compression_type(file_compression_type.to_owned());
        let frame = ctx.read_json(path, read_options).await.unwrap();
        let results = frame.collect().await.unwrap();

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&results), @r###"
            +-----+------------------+---------------+------+
            | a   | b                | c             | d    |
            +-----+------------------+---------------+------+
            | 1   | [2.0, 1.3, -6.1] | [false, true] | 4    |
            | -10 | [2.0, 1.3, -6.1] | [true, true]  | 4    |
            | 2   | [2.0, , -6.1]    | [false, ]     | text |
            |     |                  |               |      |
            +-----+------------------+---------------+------+
        "###);}

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
    async fn nd_json_exec_file_without_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        use arrow::datatypes::DataType;
        use datafusion_datasource::file_scan_config::FileScanConfig;
        use futures::StreamExt;

        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_limit(Some(3))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.build();

        // TODO: this is not where schema inference should be tested

        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 4);

        // a,b,c,d should be inferred
        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap();

        assert_eq!(
            inferred_schema.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(matches!(
            inferred_schema.field_with_name("b").unwrap().data_type(),
            DataType::List(_)
        ));
        assert_eq!(
            inferred_schema.field_with_name("d").unwrap().data_type(),
            &DataType::Utf8
        );

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 3);
        let values = as_int64_array(batch.column(0))?;
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);

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
    async fn nd_json_exec_file_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use arrow::datatypes::DataType;
        use datafusion_datasource::file_scan_config::FileScanConfig;
        use futures::StreamExt;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();

        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, actual_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let mut builder = SchemaBuilder::from(actual_schema.fields());
        builder.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(builder.finish());
        let missing_field_idx = file_schema.fields.len() - 1;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_limit(Some(3))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.build();

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 3);
        let values = as_int32_array(batch.column(missing_field_idx))?;
        assert_eq!(values.len(), 3);
        assert!(values.is_null(0));
        assert!(values.is_null(1));
        assert!(values.is_null(2));

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
    async fn nd_json_exec_file_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use datafusion_datasource::file_scan_config::FileScanConfig;
        use futures::StreamExt;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_projection(Some(vec![0, 2]))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.build();
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 2);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap_err();

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 4);
        let values = as_int64_array(batch.column(0))?;
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);
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
    async fn nd_json_exec_file_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use datafusion_datasource::file_scan_config::FileScanConfig;
        use futures::StreamExt;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let tmp_dir = TempDir::new()?;
        let (object_store_url, file_groups, file_schema) =
            prepare_store(&state, file_compression_type.to_owned(), tmp_dir.path()).await;

        let source = Arc::new(JsonSource::new());
        let conf = FileScanConfig::new(object_store_url, file_schema, source)
            .with_file_groups(file_groups)
            .with_projection(Some(vec![3, 0, 2]))
            .with_file_compression_type(file_compression_type.to_owned());
        let exec = conf.build();
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 3);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap();

        let mut it = exec.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 4);

        let values = as_string_array(batch.column(0))?;
        assert_eq!(values.value(0), "4");
        assert_eq!(values.value(1), "4");
        assert_eq!(values.value(2), "text");

        let values = as_int64_array(batch.column(1))?;
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);
        Ok(())
    }

    #[tokio::test]
    async fn write_json_results() -> Result<()> {
        // create partitioned input file and context
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(8),
        );

        let path = format!("{TEST_DATA_BASE}/1.json");

        // register json file with the execution context
        ctx.register_json("test", path.as_str(), NdJsonReadOptions::default())
            .await?;

        // register a local file system object store for /tmp directory
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
        let out_dir_url = "file://local/out/";
        let df = ctx.sql("SELECT a, b FROM test").await?;
        df.write_json(out_dir_url, DataFrameWriteOptions::new(), None)
            .await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new();

        // get name of first part
        let paths = fs::read_dir(&out_dir).unwrap();
        let mut part_0_name: String = "".to_owned();
        for path in paths {
            let name = path
                .unwrap()
                .path()
                .file_name()
                .expect("Should be a file name")
                .to_str()
                .expect("Should be a str")
                .to_owned();
            if name.ends_with("_0.json") {
                part_0_name = name;
                break;
            }
        }

        if part_0_name.is_empty() {
            panic!("Did not find part_0 in json output files!")
        }

        // register each partition as well as the top level dir
        let json_read_option = NdJsonReadOptions::default();
        ctx.register_json(
            "part0",
            &format!("{out_dir}/{part_0_name}"),
            json_read_option.clone(),
        )
        .await?;
        ctx.register_json("allparts", &out_dir, json_read_option)
            .await?;

        let part0 = ctx.sql("SELECT a, b FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT a, b FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 4);

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
    async fn test_chunked_json(
        file_compression_type: FileCompressionType,
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_additional_stores(
            file_compression_type,
            Arc::new(ChunkedStore::new(
                Arc::new(LocalFileSystem::new()),
                chunk_size,
            )),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn write_json_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        // register a local file system object store for /tmp directory
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;
        let out_dir_url = "file://local/out";
        let e = df
            .write_json(out_dir_url, DataFrameWriteOptions::new(), None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value d for column 0 at line 4");
        Ok(())
    }

    #[tokio::test]
    async fn ndjson_schema_infer_max_records() -> Result<()> {
        async fn read_test_data(schema_infer_max_records: usize) -> Result<SchemaRef> {
            let ctx = SessionContext::new();

            let options = NdJsonReadOptions {
                schema_infer_max_records,
                ..Default::default()
            };

            let batches = ctx
                .read_json("tests/data/4.json", options)
                .await?
                .collect()
                .await?;

            Ok(batches[0].schema())
        }

        // Use only the first 2 rows to infer the schema, those have 2 fields.
        let schema = read_test_data(2).await?;
        assert_eq!(schema.fields().len(), 2);

        // Use all rows to infer the schema, those have 5 fields.
        let schema = read_test_data(10).await?;
        assert_eq!(schema.fields().len(), 5);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case::uncompressed(FileCompressionType::UNCOMPRESSED),
        case::gzip(FileCompressionType::GZIP),
        case::bzip2(FileCompressionType::BZIP2),
        case::xz(FileCompressionType::XZ),
        case::zstd(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_json_with_repartitioning(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use datafusion_common::assert_batches_sorted_eq;
        use datafusion_execution::config::SessionConfig;

        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);

        let tmp_dir = TempDir::new()?;
        let (store_url, file_groups, _) =
            prepare_store(&ctx.state(), file_compression_type, tmp_dir.path()).await;

        // It's important to have less than `target_partitions` amount of file groups, to
        // trigger repartitioning.
        assert_eq!(
            file_groups.len(),
            1,
            "Expected prepared store with single file group"
        );

        let path = file_groups
            .first()
            .unwrap()
            .files()
            .first()
            .unwrap()
            .object_meta
            .location
            .as_ref();

        let url: &Url = store_url.as_ref();
        let path_buf = Path::new(url.path()).join(path);
        let path = path_buf.to_str().unwrap();
        let ext = JsonFormat::default()
            .get_ext_with_compression(&file_compression_type)
            .unwrap();

        let read_option = NdJsonReadOptions::default()
            .file_compression_type(file_compression_type)
            .file_extension(ext.as_str());

        let df = ctx.read_json(path, read_option).await?;
        let res = df.collect().await;

        // Output sort order is nondeterministic due to multiple
        // target partitions. To handle it, assert compares sorted
        // result.
        assert_batches_sorted_eq!(
            &[
                "+-----+------------------+---------------+------+",
                "| a   | b                | c             | d    |",
                "+-----+------------------+---------------+------+",
                "| 1   | [2.0, 1.3, -6.1] | [false, true] | 4    |",
                "| -10 | [2.0, 1.3, -6.1] | [true, true]  | 4    |",
                "| 2   | [2.0, , -6.1]    | [false, ]     | text |",
                "|     |                  |               |      |",
                "+-----+------------------+---------------+------+",
            ],
            &res?
        );
        Ok(())
    }
}
