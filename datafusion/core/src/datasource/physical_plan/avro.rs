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

//! Reexports the [`datafusion_datasource_json::source`] module, containing [Avro] based [`FileSource`].
//!
//! [Avro]: https://avro.apache.org/
//! [`FileSource`]: datafusion_datasource::file::FileSource

pub use datafusion_datasource_avro::source::*;

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::prelude::SessionContext;
    use crate::test::object_store::local_unpartitioned_file;
    use arrow::datatypes::{DataType, Field, SchemaBuilder};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{test_util, Result, ScalarValue};
    use datafusion_datasource::file_format::FileFormat;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::{PartitionedFile, TableSchema};
    use datafusion_datasource_avro::source::AvroSource;
    use datafusion_datasource_avro::AvroFormat;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_physical_plan::ExecutionPlan;

    use datafusion_datasource::source::DataSourceExec;
    use futures::StreamExt;
    use insta::assert_snapshot;
    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use rstest::*;
    use url::Url;

    #[tokio::test]
    async fn avro_exec_without_partition() -> Result<()> {
        test_with_stores(Arc::new(LocalFileSystem::new())).await
    }

    #[rstest]
    #[tokio::test]
    async fn test_chunked_avro(
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_with_stores(Arc::new(ChunkedStore::new(
            Arc::new(LocalFileSystem::new()),
            chunk_size,
        )))
        .await
    }

    async fn test_with_stores(store: Arc<dyn ObjectStore>) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let url = Url::parse("file://").unwrap();
        session_ctx.register_object_store(&url, store.clone());

        let testdata = test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let meta = local_unpartitioned_file(filename);

        let file_schema = AvroFormat {}
            .infer_schema(&state, &store, std::slice::from_ref(&meta))
            .await?;

        let source = Arc::new(AvroSource::new(Arc::clone(&file_schema)));
        let conf = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file(meta.into())
            .with_projection_indices(Some(vec![0, 1, 2]))?
            .build();

        let source_exec = DataSourceExec::from_data_source(conf);
        assert_eq!(
            source_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        let mut results = source_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch]), @r###"
            +----+----------+-------------+
            | id | bool_col | tinyint_col |
            +----+----------+-------------+
            | 4  | true     | 0           |
            | 5  | false    | 1           |
            | 6  | true     | 0           |
            | 7  | false    | 1           |
            | 2  | true     | 0           |
            | 3  | false    | 1           |
            | 0  | true     | 0           |
            | 1  | false    | 1           |
            +----+----------+-------------+
        "###);}

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn avro_exec_missing_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let actual_schema = AvroFormat {}
            .infer_schema(&state, &object_store, std::slice::from_ref(&meta))
            .await?;

        let mut builder = SchemaBuilder::from(actual_schema.fields());
        builder.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(builder.finish());
        // Include the missing column in the projection
        let projection = Some(vec![0, 1, 2, actual_schema.fields().len()]);

        let source = Arc::new(AvroSource::new(Arc::clone(&file_schema)));
        let conf = FileScanConfigBuilder::new(object_store_url, source)
            .with_file(meta.into())
            .with_projection_indices(projection)?
            .build();

        let source_exec = DataSourceExec::from_data_source(conf);
        assert_eq!(
            source_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );

        let mut results = source_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch]), @r###"
            +----+----------+-------------+-------------+
            | id | bool_col | tinyint_col | missing_col |
            +----+----------+-------------+-------------+
            | 4  | true     | 0           |             |
            | 5  | false    | 1           |             |
            | 6  | true     | 0           |             |
            | 7  | false    | 1           |             |
            | 2  | true     | 0           |             |
            | 3  | false    | 1           |             |
            | 0  | true     | 0           |             |
            | 1  | false    | 1           |             |
            +----+----------+-------------+-------------+
        "###);}

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn avro_exec_with_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let file_schema = AvroFormat {}
            .infer_schema(&state, &object_store, std::slice::from_ref(&meta))
            .await?;

        let mut partitioned_file = PartitionedFile::from(meta);
        partitioned_file.partition_values = vec![ScalarValue::from("2021-10-26")];

        let projection = Some(vec![0, 1, file_schema.fields().len(), 2]);
        let table_schema = TableSchema::new(
            file_schema.clone(),
            vec![Arc::new(Field::new("date", DataType::Utf8, false))],
        );
        let source = Arc::new(AvroSource::new(table_schema.clone()));
        let conf = FileScanConfigBuilder::new(object_store_url, source)
            // select specific columns of the files as well as the partitioning
            // column which is supposed to be the last column in the table schema.
            .with_projection_indices(projection)?
            .with_file(partitioned_file)
            .build();

        let source_exec = DataSourceExec::from_data_source(conf);

        assert_eq!(
            source_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );

        let mut results = source_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        insta::allow_duplicates! {assert_snapshot!(batches_to_string(&[batch]), @r###"
            +----+----------+------------+-------------+
            | id | bool_col | date       | tinyint_col |
            +----+----------+------------+-------------+
            | 4  | true     | 2021-10-26 | 0           |
            | 5  | false    | 2021-10-26 | 1           |
            | 6  | true     | 2021-10-26 | 0           |
            | 7  | false    | 2021-10-26 | 1           |
            | 2  | true     | 2021-10-26 | 0           |
            | 3  | false    | 2021-10-26 | 1           |
            | 0  | true     | 2021-10-26 | 0           |
            | 1  | false    | 2021-10-26 | 1           |
            +----+----------+------------+-------------+
        "###);}

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
