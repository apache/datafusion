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

//! DataFusion data sources: [`TableProvider`] and [`ListingTable`]
//!
//! [`ListingTable`]: crate::datasource::listing::ListingTable

pub mod dynamic_file;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod listing_table_factory;
mod memory_test;
pub mod physical_plan;
pub mod provider;
mod view_test;

// backwards compatibility
pub use self::default_table_source::{
    DefaultTableSource, provider_as_source, source_as_provider,
};
pub use self::memory::MemTable;
pub use self::view::ViewTable;
pub use crate::catalog::TableProvider;
pub use crate::logical_expr::TableType;
pub use datafusion_catalog::cte_worktable;
pub use datafusion_catalog::default_table_source;
pub use datafusion_catalog::memory;
pub use datafusion_catalog::stream;
pub use datafusion_catalog::view;
pub use datafusion_datasource::schema_adapter;
pub use datafusion_datasource::sink;
pub use datafusion_datasource::source;
pub use datafusion_datasource::table_schema;
pub use datafusion_execution::object_store;
pub use datafusion_physical_expr::create_ordering;

#[cfg(all(test, feature = "parquet"))]
mod tests {

    use crate::prelude::SessionContext;
    use ::object_store::{ObjectMeta, path::Path};
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use datafusion_common::{
        Result, ScalarValue,
        test_util::batches_to_sort_string,
        tree_node::{Transformed, TransformedResult, TreeNode},
    };
    use datafusion_datasource::{
        PartitionedFile, file_scan_config::FileScanConfigBuilder, source::DataSourceExec,
    };
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_physical_expr::expressions::{Column, Literal};
    use datafusion_physical_expr_adapter::{
        PhysicalExprAdapter, PhysicalExprAdapterFactory,
    };
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_plan::collect;
    use std::{fs, sync::Arc};
    use tempfile::TempDir;

    #[tokio::test]
    async fn can_override_physical_expr_adapter() {
        // Test shows that PhysicalExprAdapter can add a column that doesn't exist in the
        // record batches returned from parquet. This can be useful for schema evolution
        // where older files may not have all columns.

        use datafusion_execution::object_store::ObjectStoreUrl;
        let tmp_dir = TempDir::new().unwrap();
        let table_dir = tmp_dir.path().join("parquet_test");
        fs::DirBuilder::new().create(table_dir.as_path()).unwrap();
        let f1 = Field::new("id", DataType::Int32, true);

        let file_schema = Arc::new(Schema::new(vec![f1.clone()]));
        let filename = "part.parquet".to_string();
        let path = table_dir.as_path().join(filename.clone());
        let file = fs::File::create(path.clone()).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, file_schema.clone(), None)
                .unwrap();

        let ids = Arc::new(Int32Array::from(vec![1i32]));
        let rec_batch = RecordBatch::try_new(file_schema.clone(), vec![ids]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();

        let location = Path::parse(path.to_str().unwrap()).unwrap();
        let metadata = fs::metadata(path.as_path()).expect("Local file metadata");
        let meta = ObjectMeta {
            location,
            last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
            size: metadata.len(),
            e_tag: None,
            version: None,
        };

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let f1 = Field::new("id", DataType::Int32, true);
        let f2 = Field::new("extra_column", DataType::Utf8, true);

        let schema = Arc::new(Schema::new(vec![f1.clone(), f2.clone()]));
        let source = Arc::new(ParquetSource::new(Arc::clone(&schema)));
        let base_conf =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
                .with_file(partitioned_file)
                .with_expr_adapter(Some(Arc::new(TestPhysicalExprAdapterFactory)))
                .build();

        let parquet_exec = DataSourceExec::from_data_source(base_conf);

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let read = collect(parquet_exec, task_ctx).await.unwrap();

        insta::assert_snapshot!(batches_to_sort_string(&read),@r"
        +----+--------------+
        | id | extra_column |
        +----+--------------+
        | 1  | foo          |
        +----+--------------+
        ");
    }

    #[derive(Debug)]
    struct TestPhysicalExprAdapterFactory;

    impl PhysicalExprAdapterFactory for TestPhysicalExprAdapterFactory {
        fn create(
            &self,
            _logical_file_schema: SchemaRef,
            physical_file_schema: SchemaRef,
        ) -> Arc<dyn PhysicalExprAdapter> {
            Arc::new(TestPhysicalExprAdapter {
                physical_file_schema,
            })
        }
    }

    #[derive(Debug)]
    struct TestPhysicalExprAdapter {
        physical_file_schema: SchemaRef,
    }

    impl PhysicalExprAdapter for TestPhysicalExprAdapter {
        fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
            expr.transform(|e| {
                if let Some(column) = e.as_any().downcast_ref::<Column>() {
                    // If column is "extra_column" and missing from physical schema, inject "foo"
                    if column.name() == "extra_column"
                        && self.physical_file_schema.index_of("extra_column").is_err()
                    {
                        return Ok(Transformed::yes(Arc::new(Literal::new(
                            ScalarValue::Utf8(Some("foo".to_string())),
                        ))
                            as Arc<dyn PhysicalExpr>));
                    }
                }
                Ok(Transformed::no(e))
            })
            .data()
        }
    }
}
