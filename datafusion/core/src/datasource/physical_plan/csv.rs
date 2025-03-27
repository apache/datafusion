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

//! Reexports the [`datafusion_datasource_json::source`] module, containing CSV based [`FileSource`].
//!
//! [`FileSource`]: datafusion_datasource::file::FileSource

pub use datafusion_datasource_csv::source::*;

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::io::Write;
    use std::sync::Arc;

    use datafusion_datasource_csv::CsvFormat;
    use object_store::ObjectStore;

    use crate::prelude::CsvReadOptions;
    use crate::prelude::SessionContext;
    use crate::test::partitioned_file_groups;
    use datafusion_common::test_util::arrow_test_data;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{assert_batches_eq, Result};
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_plan::metrics::MetricsSet;
    use datafusion_physical_plan::ExecutionPlan;

    #[cfg(feature = "compression")]
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource_csv::partitioned_csv_config;
    use datafusion_datasource_csv::source::CsvSource;
    use futures::{StreamExt, TryStreamExt};

    use arrow::datatypes::*;
    use bytes::Bytes;
    use insta::assert_snapshot;
    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use rstest::*;
    use tempfile::TempDir;
    use url::Url;

    fn aggr_test_schema() -> SchemaRef {
        let mut f1 = Field::new("c1", DataType::Utf8, false);
        f1.set_metadata(HashMap::from_iter(vec![("testing".into(), "test".into())]));
        let schema = Schema::new(vec![
            f1,
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c4", DataType::Int16, false),
            Field::new("c5", DataType::Int32, false),
            Field::new("c6", DataType::Int64, false),
            Field::new("c7", DataType::UInt8, false),
            Field::new("c8", DataType::UInt16, false),
            Field::new("c9", DataType::UInt32, false),
            Field::new("c10", DataType::UInt64, false),
            Field::new("c11", DataType::Float32, false),
            Field::new("c12", DataType::Float64, false),
            Field::new("c13", DataType::Utf8, false),
        ]);

        Arc::new(schema)
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
    async fn csv_exec_with_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_file_compression_type(file_compression_type)
            .with_newlines_in_values(false)
            .with_projection(Some(vec![0, 2, 4]));

        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();

        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch.slice(0, 5)]), @r###"
            +----+-----+------------+
            | c1 | c3  | c5         |
            +----+-----+------------+
            | c  | 1   | 2033001162 |
            | d  | -40 | 706441268  |
            | b  | 29  | 994303988  |
            | a  | -85 | 1171968280 |
            | b  | -82 | 1824882165 |
            +----+-----+------------+
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
    async fn csv_exec_with_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
        let session_ctx = SessionContext::new_with_config(cfg);
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned())
            .with_projection(Some(vec![4, 0, 2]));
        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch.slice(0, 5)]), @r###"
            +------------+----+-----+
            | c5         | c1 | c3  |
            +------------+----+-----+
            | 2033001162 | c  | 1   |
            | 706441268  | d  | -40 |
            | 994303988  | b  | 29  |
            | 1171968280 | a  | -85 |
            | 1824882165 | b  | -82 |
            +------------+----+-----+
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
    async fn csv_exec_with_limit(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use futures::StreamExt;

        let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
        let session_ctx = SessionContext::new_with_config(cfg);
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned())
            .with_limit(Some(5));
        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(13, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch]), @r###"
            +----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+
            | c1 | c2 | c3  | c4     | c5         | c6                   | c7  | c8    | c9         | c10                  | c11         | c12                 | c13                            |
            +----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+
            | c  | 2  | 1   | 18109  | 2033001162 | -6513304855495910254 | 25  | 43062 | 1491205016 | 5863949479783605708  | 0.110830784 | 0.9294097332465232  | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |
            | d  | 5  | -40 | 22614  | 706441268  | -7542719935673075327 | 155 | 14337 | 3373581039 | 11720144131976083864 | 0.69632107  | 0.3114712539863804  | C2GT5KVyOPZpgKVl110TyZO0NcJ434 |
            | b  | 1  | 29  | -18218 | 994303988  | 5983957848665088916  | 204 | 9489  | 3275293996 | 14857091259186476033 | 0.53840446  | 0.17909035118828576 | AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz |
            | a  | 1  | -85 | -15154 | 1171968280 | 1919439543497968449  | 77  | 52286 | 774637006  | 12101411955859039553 | 0.12285209  | 0.6864391962767343  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB |
            | b  | 5  | -82 | 22080  | 1824882165 | 7373730676428214987  | 208 | 34331 | 3342719438 | 3330177516592499461  | 0.82634634  | 0.40975383525297016 | Ig1QcuKsjHXkproePdERo2w0mYzIqd |
            +----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+
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
    async fn csv_exec_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema_with_missing_col();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned())
            .with_limit(Some(5));
        assert_eq!(14, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(14, csv.schema().fields().len());

        // errors due to https://github.com/apache/datafusion/issues/4918
        let mut it = csv.execute(0, task_ctx)?;
        let err = it.next().await.unwrap().unwrap_err().strip_backtrace();
        assert_eq!(
            err,
            "Arrow error: Csv error: incorrect number of fields for line 1, expected 14 got 13"
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
    async fn csv_exec_with_partition(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use datafusion_common::ScalarValue;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let mut config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned());

        // Add partition columns
        config.table_partition_cols = vec![Field::new("date", DataType::Utf8, false)];
        config.file_groups[0][0].partition_values = vec![ScalarValue::from("2021-10-26")];

        // We should be able to project on the partition column
        // Which is supposed to be after the file fields
        config.projection = Some(vec![0, config.file_schema.fields().len()]);

        // we don't have `/date=xx/` in the path but that is ok because
        // partitions are resolved during scan anyway

        assert_eq!(13, config.file_schema.fields().len());
        let csv = config.build();
        assert_eq!(2, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch.slice(0, 5)]), @r###"
            +----+------------+
            | c1 | date       |
            +----+------------+
            | c  | 2021-10-26 |
            | d  | 2021-10-26 |
            | b  | 2021-10-26 |
            | a  | 2021-10-26 |
            | b  | 2021-10-26 |
            +----+------------+
        "###);}

        let metrics = csv.metrics().expect("doesn't found metrics");
        let time_elapsed_processing = get_value(&metrics, "time_elapsed_processing");
        assert!(
            time_elapsed_processing > 0,
            "Expected time_elapsed_processing greater than 0",
        );
        Ok(())
    }

    /// Generate CSV partitions within the supplied directory
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

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.register_object_store(&url, store.clone());

        let task_ctx = ctx.task_ctx();

        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )
        .unwrap();

        let source = Arc::new(CsvSource::new(true, b',', b'"'));
        let config = partitioned_csv_config(file_schema, file_groups, source)
            .with_newlines_in_values(false)
            .with_file_compression_type(file_compression_type.to_owned());
        let csv = config.build();

        let it = csv.execute(0, task_ctx).unwrap();
        let batches: Vec<_> = it.try_collect().await.unwrap();

        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        assert_eq!(total_rows, 100);
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
    async fn test_chunked_csv(
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
    async fn test_no_trailing_delimiter() {
        let session_ctx = SessionContext::new();
        let store = object_store::memory::InMemory::new();

        let data = Bytes::from("a,b\n1,2\n3,4");
        let path = object_store::path::Path::from("a.csv");
        store.put(&path, data.into()).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx.register_object_store(&url, Arc::new(store));

        let df = session_ctx
            .read_csv("memory:///", CsvReadOptions::new())
            .await
            .unwrap();

        let result = df.collect().await.unwrap();

        assert_snapshot!(batches_to_string(&result), @r###"
            +---+---+
            | a | b |
            +---+---+
            | 1 | 2 |
            | 3 | 4 |
            +---+---+
        "###);
    }

    #[tokio::test]
    async fn test_terminator() {
        let session_ctx = SessionContext::new();
        let store = object_store::memory::InMemory::new();

        let data = Bytes::from("a,b\r1,2\r3,4");
        let path = object_store::path::Path::from("a.csv");
        store.put(&path, data.into()).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx.register_object_store(&url, Arc::new(store));

        let df = session_ctx
            .read_csv("memory:///", CsvReadOptions::new().terminator(Some(b'\r')))
            .await
            .unwrap();

        let result = df.collect().await.unwrap();

        assert_snapshot!(batches_to_string(&result),@r###"
            +---+---+
            | a | b |
            +---+---+
            | 1 | 2 |
            | 3 | 4 |
            +---+---+
        "###);

        let e = session_ctx
            .read_csv("memory:///", CsvReadOptions::new().terminator(Some(b'\n')))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert_eq!(e.strip_backtrace(), "Arrow error: Csv error: incorrect number of fields for line 1, expected 2 got more than 2")
    }

    #[tokio::test]
    async fn test_create_external_table_with_terminator() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.sql(
            r#"
            CREATE EXTERNAL TABLE t1 (
            col1 TEXT,
            col2 TEXT
            ) STORED AS CSV
            LOCATION 'tests/data/cr_terminator.csv'
            OPTIONS ('format.terminator' E'\r', 'format.has_header' 'true');
    "#,
        )
        .await?
        .collect()
        .await?;

        let df = ctx.sql(r#"select * from t1"#).await?.collect().await?;
        assert_snapshot!(batches_to_string(&df),@r###"
            +------+--------+
            | col1 | col2   |
            +------+--------+
            | id0  | value0 |
            | id1  | value1 |
            | id2  | value2 |
            | id3  | value3 |
            +------+--------+
        "###);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_external_table_with_terminator_with_newlines_in_values(
    ) -> Result<()> {
        let ctx = SessionContext::new();
        ctx.sql(r#"
            CREATE EXTERNAL TABLE t1 (
            col1 TEXT,
            col2 TEXT
            ) STORED AS CSV
            LOCATION 'tests/data/newlines_in_values_cr_terminator.csv'
            OPTIONS ('format.terminator' E'\r', 'format.has_header' 'true', 'format.newlines_in_values' 'true');
    "#).await?.collect().await?;

        let df = ctx.sql(r#"select * from t1"#).await?.collect().await?;
        let expected = [
            "+-------+-----------------------------+",
            "| col1  | col2                        |",
            "+-------+-----------------------------+",
            "| 1     | hello\rworld                 |",
            "| 2     | something\relse              |",
            "| 3     | \rmany\rlines\rmake\rgood test\r |",
            "| 4     | unquoted                    |",
            "| value | end                         |",
            "+-------+-----------------------------+",
        ];
        assert_batches_eq!(expected, &df);
        Ok(())
    }

    #[tokio::test]
    async fn write_csv_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();

        // register a local file system object store
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
            .write_csv(
                out_dir_url,
                crate::dataframe::DataFrameWriteOptions::new(),
                None,
            )
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value d for column 0 at line 4");
        Ok(())
    }

    #[tokio::test]
    async fn write_csv_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(8)
                .set_str("datafusion.catalog.has_header", "false"),
        );

        let schema = populate_csv_partitions(&tmp_dir, 8, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // register a local file system object store
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();

        ctx.register_object_store(&local_url, local);

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
        let out_dir_url = "file://local/out/";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        df.write_csv(
            out_dir_url,
            crate::dataframe::DataFrameWriteOptions::new(),
            None,
        )
        .await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().set_str("datafusion.catalog.has_header", "false"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // get name of first part
        let paths = fs::read_dir(&out_dir).unwrap();
        let mut part_0_name: String = "".to_owned();
        for path in paths {
            let path = path.unwrap();
            let name = path
                .path()
                .file_name()
                .expect("Should be a file name")
                .to_str()
                .expect("Should be a str")
                .to_owned();
            if name.ends_with("_0.csv") {
                part_0_name = name;
                break;
            }
        }

        if part_0_name.is_empty() {
            panic!("Did not find part_0 in csv output files!")
        }
        // register each partition as well as the top level dir
        let csv_read_option = CsvReadOptions::new().schema(&schema).has_header(false);
        ctx.register_csv(
            "part0",
            &format!("{out_dir}/{part_0_name}"),
            csv_read_option.clone(),
        )
        .await?;
        ctx.register_csv("allparts", &out_dir, csv_read_option)
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 80);

        Ok(())
    }

    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }

    /// Get the schema for the aggregate_test_* csv files with an additional filed not present in the files.
    fn aggr_test_schema_with_missing_col() -> SchemaRef {
        let fields =
            Fields::from_iter(aggr_test_schema().fields().iter().cloned().chain(
                std::iter::once(Arc::new(Field::new(
                    "missing_col",
                    DataType::Int64,
                    true,
                ))),
            ));

        let schema = Schema::new(fields);

        Arc::new(schema)
    }
}
