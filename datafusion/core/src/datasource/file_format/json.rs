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

//! Re-exports the [`datafusion_datasource_json::file_format`] module, and contains tests for it.
pub use datafusion_datasource_json::file_format::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::datasource::file_format::test_util::scan_format;
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;
    use arrow::array::RecordBatch;
    use arrow_schema::Schema;
    use bytes::Bytes;
    use datafusion_catalog::Session;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_datasource::decoder::{
        BatchDeserializer, DecoderDeserializer, DeserializerOutput,
    };
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_physical_plan::{ExecutionPlan, collect};

    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field};
    use arrow::json::ReaderBuilder;
    use arrow::util::pretty;
    use datafusion_common::cast::as_int64_array;
    use datafusion_common::internal_err;
    use datafusion_common::stats::Precision;

    use crate::execution::options::JsonReadOptions;
    use datafusion_common::Result;
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use futures::StreamExt;
    use insta::assert_snapshot;
    use object_store::local::LocalFileSystem;
    use regex::Regex;
    use rstest::rstest;
    // ==================== Test Helpers ====================

    /// Create a temporary JSON file and return (TempDir, path)
    fn create_temp_json(content: &str) -> (tempfile::TempDir, String) {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let path = format!("{}/test.json", tmp_dir.path().to_string_lossy());
        std::fs::write(&path, content).unwrap();
        (tmp_dir, path)
    }

    /// Infer schema from JSON array format file
    async fn infer_json_array_schema(
        content: &str,
    ) -> Result<arrow::datatypes::SchemaRef> {
        let (_tmp_dir, path) = create_temp_json(content);
        let session = SessionContext::new();
        let ctx = session.state();
        let store = Arc::new(LocalFileSystem::new()) as _;
        let format = JsonFormat::default().with_newline_delimited(false);
        format
            .infer_schema(&ctx, &store, &[local_unpartitioned_file(&path)])
            .await
    }

    /// Register a JSON array table and run a query
    async fn query_json_array(content: &str, query: &str) -> Result<Vec<RecordBatch>> {
        let (_tmp_dir, path) = create_temp_json(content);
        let ctx = SessionContext::new();
        let options = JsonReadOptions::default().newline_delimited(false);
        ctx.register_json("test_table", &path, options).await?;
        ctx.sql(query).await?.collect().await
    }

    /// Register a JSON array table and run a query, return formatted string
    async fn query_json_array_str(content: &str, query: &str) -> Result<String> {
        let result = query_json_array(content, query).await?;
        Ok(batches_to_string(&result))
    }

    // ==================== Existing Tests ====================

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(4, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 6 /* 12/2 */);

        // test metadata
        assert_eq!(exec.partition_statistics(None)?.num_rows, Precision::Absent);
        assert_eq!(
            exec.partition_statistics(None)?.total_byte_size,
            Precision::Absent
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let projection = None;
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let exec = get_exec(&state, projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean", "d: Utf8",], x);

        Ok(())
    }

    #[tokio::test]
    async fn read_int_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, projection, None).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(12, batches[0].num_rows());

        let array = as_int64_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            vec![1, -10, 2, 1, 7, 1, 1, 5, 1, 1, 1, 100000000000000],
            values
        );

        Ok(())
    }

    async fn get_exec(
        state: &dyn Session,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = "tests/data/2.json";
        let format = JsonFormat::default();
        scan_format(state, &format, None, ".", filename, projection, limit).await
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let session = SessionContext::new();
        let ctx = session.state();
        let store = Arc::new(LocalFileSystem::new()) as _;
        let filename = "tests/data/schema_infer_limit.json";
        let format = JsonFormat::default().with_schema_infer_max_rec(3);

        let file_schema = format
            .infer_schema(&ctx, &store, &[local_unpartitioned_file(filename)])
            .await
            .expect("Schema inference");

        let fields = file_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect::<Vec<_>>();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean"], fields);
    }

    async fn count_num_partitions(ctx: &SessionContext, query: &str) -> Result<usize> {
        let result = ctx
            .sql(&format!("EXPLAIN {query}"))
            .await?
            .collect()
            .await?;

        let plan = format!("{}", &pretty::pretty_format_batches(&result)?);

        let re = Regex::new(r"file_groups=\{(\d+) group").unwrap();

        if let Some(captures) = re.captures(&plan)
            && let Some(match_) = captures.get(1)
        {
            let count = match_.as_str().parse::<usize>().unwrap();
            return Ok(count);
        }

        internal_err!("Query contains no Exec: file_groups")
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn it_can_read_ndjson_in_parallel(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);

        let ctx = SessionContext::new_with_config(config);

        let table_path = "tests/data/1.json";
        let options = JsonReadOptions::default();

        ctx.register_json("json_parallel", table_path, options)
            .await?;

        let query = "SELECT sum(a) FROM json_parallel;";

        let result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_num_partitions(&ctx, query).await?;

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&result),@r"
        +----------------------+
        | sum(json_parallel.a) |
        +----------------------+
        | -7                   |
        +----------------------+
        ");}

        assert_eq!(n_partitions, actual_partitions);

        Ok(())
    }

    #[tokio::test]
    async fn it_can_read_empty_ndjson() -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0);

        let ctx = SessionContext::new_with_config(config);

        let table_path = "tests/data/empty.json";
        let options = JsonReadOptions::default();

        ctx.register_json("json_parallel_empty", table_path, options)
            .await?;

        let query = "SELECT * FROM json_parallel_empty WHERE random() > 0.5;";

        let result = ctx.sql(query).await?.collect().await?;

        assert_snapshot!(batches_to_string(&result),@r"
        ++
        ++
        ");

        Ok(())
    }

    #[test]
    fn test_json_deserializer_finish() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
            Field::new("c4", DataType::Int64, true),
            Field::new("c5", DataType::Int64, true),
        ]));
        let mut deserializer = json_deserializer(1, &schema)?;

        deserializer.digest(r#"{ "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5 }"#.into());
        deserializer.digest(r#"{ "c1": 6, "c2": 7, "c3": 8, "c4": 9, "c5": 10 }"#.into());
        deserializer
            .digest(r#"{ "c1": 11, "c2": 12, "c3": 13, "c4": 14, "c5": 15 }"#.into());
        deserializer.finish();

        let mut all_batches = RecordBatch::new_empty(schema.clone());
        for _ in 0..3 {
            let output = deserializer.next()?;
            let DeserializerOutput::RecordBatch(batch) = output else {
                panic!("Expected RecordBatch, got {output:?}");
            };
            all_batches = concat_batches(&schema, &[all_batches, batch])?
        }
        assert_eq!(deserializer.next()?, DeserializerOutput::InputExhausted);

        assert_snapshot!(batches_to_string(&[all_batches]),@r"
        +----+----+----+----+----+
        | c1 | c2 | c3 | c4 | c5 |
        +----+----+----+----+----+
        | 1  | 2  | 3  | 4  | 5  |
        | 6  | 7  | 8  | 9  | 10 |
        | 11 | 12 | 13 | 14 | 15 |
        +----+----+----+----+----+
        ");

        Ok(())
    }

    #[test]
    fn test_json_deserializer_no_finish() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
            Field::new("c4", DataType::Int64, true),
            Field::new("c5", DataType::Int64, true),
        ]));
        let mut deserializer = json_deserializer(1, &schema)?;

        deserializer.digest(r#"{ "c1": 1, "c2": 2, "c3": 3, "c4": 4, "c5": 5 }"#.into());
        deserializer.digest(r#"{ "c1": 6, "c2": 7, "c3": 8, "c4": 9, "c5": 10 }"#.into());
        deserializer
            .digest(r#"{ "c1": 11, "c2": 12, "c3": 13, "c4": 14, "c5": 15 }"#.into());

        let mut all_batches = RecordBatch::new_empty(schema.clone());
        for _ in 0..2 {
            let output = deserializer.next()?;
            let DeserializerOutput::RecordBatch(batch) = output else {
                panic!("Expected RecordBatch, got {output:?}");
            };
            all_batches = concat_batches(&schema, &[all_batches, batch])?
        }
        assert_eq!(deserializer.next()?, DeserializerOutput::RequiresMoreData);

        insta::assert_snapshot!(fmt_batches(&[all_batches]),@r"
        +----+----+----+----+----+
        | c1 | c2 | c3 | c4 | c5 |
        +----+----+----+----+----+
        | 1  | 2  | 3  | 4  | 5  |
        | 6  | 7  | 8  | 9  | 10 |
        +----+----+----+----+----+
        ");

        Ok(())
    }

    fn json_deserializer(
        batch_size: usize,
        schema: &Arc<Schema>,
    ) -> Result<impl BatchDeserializer<Bytes>> {
        let decoder = ReaderBuilder::new(schema.clone())
            .with_batch_size(batch_size)
            .build_decoder()?;
        Ok(DecoderDeserializer::new(JsonDecoder::new(decoder)))
    }

    fn fmt_batches(batches: &[RecordBatch]) -> String {
        pretty::pretty_format_batches(batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn test_write_empty_json_from_sql() -> Result<()> {
        let ctx = SessionContext::new();
        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty_sql.json", tmp_dir.path().to_string_lossy());
        let df = ctx.sql("SELECT CAST(1 AS BIGINT) AS id LIMIT 0").await?;
        df.write_json(&path, crate::dataframe::DataFrameWriteOptions::new(), None)
            .await?;
        assert!(std::path::Path::new(&path).exists());
        let metadata = std::fs::metadata(&path)?;
        assert_eq!(metadata.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_write_empty_json_from_record_batch() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new())),
                Arc::new(arrow::array::StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )?;

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty_batch.json", tmp_dir.path().to_string_lossy());
        let df = ctx.read_batch(empty_batch.clone())?;
        df.write_json(&path, crate::dataframe::DataFrameWriteOptions::new(), None)
            .await?;
        assert!(std::path::Path::new(&path).exists());
        let metadata = std::fs::metadata(&path)?;
        assert_eq!(metadata.len(), 0);
        Ok(())
    }

    // ==================== JSON Array Format Tests ====================

    #[tokio::test]
    async fn test_json_array_schema_inference() -> Result<()> {
        let schema = infer_json_array_schema(
            r#"[{"a": 1, "b": 2.0, "c": true}, {"a": 2, "b": 3.5, "c": false}]"#,
        )
        .await?;

        let fields: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean"], fields);
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_empty() -> Result<()> {
        let schema = infer_json_array_schema("[]").await?;
        assert_eq!(schema.fields().len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_nested_struct() -> Result<()> {
        let schema = infer_json_array_schema(
            r#"[{"id": 1, "info": {"name": "Alice", "age": 30}}]"#,
        )
        .await?;

        let info_field = schema.field_with_name("info").unwrap();
        assert!(matches!(info_field.data_type(), DataType::Struct(_)));
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_list_type() -> Result<()> {
        let schema =
            infer_json_array_schema(r#"[{"id": 1, "tags": ["a", "b", "c"]}]"#).await?;

        let tags_field = schema.field_with_name("tags").unwrap();
        assert!(matches!(tags_field.data_type(), DataType::List(_)));
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_basic_query() -> Result<()> {
        let result = query_json_array_str(
            r#"[{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}, {"a": 3, "b": "test"}]"#,
            "SELECT a, b FROM test_table ORDER BY a",
        )
        .await?;

        assert_snapshot!(result, @r"
        +---+-------+
        | a | b     |
        +---+-------+
        | 1 | hello |
        | 2 | world |
        | 3 | test  |
        +---+-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_with_nulls() -> Result<()> {
        let result = query_json_array_str(
            r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": null}, {"id": 3, "name": "Charlie"}]"#,
            "SELECT id, name FROM test_table ORDER BY id",
        )
            .await?;

        assert_snapshot!(result, @r"
        +----+---------+
        | id | name    |
        +----+---------+
        | 1  | Alice   |
        | 2  |         |
        | 3  | Charlie |
        +----+---------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_unnest() -> Result<()> {
        let result = query_json_array_str(
            r#"[{"id": 1, "values": [10, 20, 30]}, {"id": 2, "values": [40, 50]}]"#,
            "SELECT id, unnest(values) as value FROM test_table ORDER BY id, value",
        )
        .await?;

        assert_snapshot!(result, @r"
        +----+-------+
        | id | value |
        +----+-------+
        | 1  | 10    |
        | 1  | 20    |
        | 1  | 30    |
        | 2  | 40    |
        | 2  | 50    |
        +----+-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_unnest_struct() -> Result<()> {
        let result = query_json_array_str(
            r#"[{"id": 1, "orders": [{"product": "A", "qty": 2}, {"product": "B", "qty": 3}]}, {"id": 2, "orders": [{"product": "C", "qty": 1}]}]"#,
            "SELECT id, unnest(orders)['product'] as product, unnest(orders)['qty'] as qty FROM test_table ORDER BY id, product",
        )
            .await?;

        assert_snapshot!(result, @r"
        +----+---------+-----+
        | id | product | qty |
        +----+---------+-----+
        | 1  | A       | 2   |
        | 1  | B       | 3   |
        | 2  | C       | 1   |
        +----+---------+-----+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_nested_struct_access() -> Result<()> {
        let result = query_json_array_str(
            r#"[{"id": 1, "dept": {"name": "Engineering", "head": "Alice"}}, {"id": 2, "dept": {"name": "Sales", "head": "Bob"}}]"#,
            "SELECT id, dept['name'] as dept_name, dept['head'] as head FROM test_table ORDER BY id",
        )
            .await?;

        assert_snapshot!(result, @r"
        +----+-------------+-------+
        | id | dept_name   | head  |
        +----+-------------+-------+
        | 1  | Engineering | Alice |
        | 2  | Sales       | Bob   |
        +----+-------------+-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_with_compression() -> Result<()> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/array.json.gz", tmp_dir.path().to_string_lossy());

        let file = std::fs::File::create(&path)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(
            r#"[{"a": 1, "b": "hello"}, {"a": 2, "b": "world"}]"#.as_bytes(),
        )?;
        encoder.finish()?;

        let ctx = SessionContext::new();
        let options = JsonReadOptions::default()
            .newline_delimited(false)
            .file_compression_type(FileCompressionType::GZIP)
            .file_extension(".json.gz");

        ctx.register_json("test_table", &path, options).await?;
        let result = ctx
            .sql("SELECT a, b FROM test_table ORDER BY a")
            .await?
            .collect()
            .await?;

        assert_snapshot!(batches_to_string(&result), @r"
        +---+-------+
        | a | b     |
        +---+-------+
        | 1 | hello |
        | 2 | world |
        +---+-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_list_of_structs() -> Result<()> {
        let batches = query_json_array(
            r#"[{"id": 1, "items": [{"name": "x", "price": 10.5}]}, {"id": 2, "items": []}]"#,
            "SELECT id, items FROM test_table ORDER BY id",
        )
            .await?;

        assert_eq!(1, batches.len());
        assert_eq!(2, batches[0].num_rows());
        Ok(())
    }
}
