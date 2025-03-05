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

pub mod cte_worktable;
pub mod default_table_source;
pub mod dynamic_file;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod listing_table_factory;
pub mod memory;
pub mod physical_plan;
pub mod provider;
mod statistics;
pub mod stream;
pub mod view;

pub use datafusion_datasource::schema_adapter;
pub use datafusion_datasource::source;

// backwards compatibility
pub use self::default_table_source::{
    provider_as_source, source_as_provider, DefaultTableSource,
};
pub use self::memory::MemTable;
pub use self::view::ViewTable;
pub use crate::catalog::TableProvider;
pub use crate::logical_expr::TableType;
pub use datafusion_execution::object_store;
pub use statistics::get_statistics_with_limit;

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{Expr, SortExpr};
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};

/// Converts logical sort expressions to physical sort expressions
///
/// This function transforms a collection of logical sort expressions into their physical
/// representation that can be used during query execution.
///
/// # Arguments
///
/// * `schema` - The schema containing column definitions
/// * `sort_order` - A collection of logical sort expressions grouped into lexicographic orderings
///
/// # Returns
///
/// A vector of lexicographic orderings for physical execution, or an error if the transformation fails
///
/// # Examples
///
/// ```
/// // Create orderings from columns "id" and "name"
/// # use arrow::datatypes::{Schema, Field, DataType};
/// # use datafusion::datasource::create_ordering;
/// # use datafusion_common::Column;
/// # use datafusion_expr::{Expr, SortExpr};
/// #
/// // Create a schema with two fields
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("name", DataType::Utf8, false),
/// ]);
///
/// let sort_exprs = vec![
///     vec![
///         SortExpr { expr: Expr::Column(Column::new(Some("t"), "id")), asc: true, nulls_first: false }
///     ],
///     vec![
///         SortExpr { expr: Expr::Column(Column::new(Some("t"), "name")), asc: false, nulls_first: true }
///     ]
/// ];
/// let result = create_ordering(&schema, &sort_exprs).unwrap();
/// ```
pub fn create_ordering(
    schema: &Schema,
    sort_order: &[Vec<SortExpr>],
) -> Result<Vec<LexOrdering>> {
    let mut all_sort_orders = vec![];

    for (group_idx, exprs) in sort_order.iter().enumerate() {
        // Construct PhysicalSortExpr objects from Expr objects:
        let mut sort_exprs = LexOrdering::default();
        for (expr_idx, sort) in exprs.iter().enumerate() {
            match &sort.expr {
                Expr::Column(col) => match expressions::col(&col.name, schema) {
                    Ok(expr) => {
                        sort_exprs.push(PhysicalSortExpr {
                            expr,
                            options: SortOptions {
                                descending: !sort.asc,
                                nulls_first: sort.nulls_first,
                            },
                        });
                    }
                    // Cannot find expression in the projected_schema, stop iterating
                    // since rest of the orderings are violated
                    Err(_) => break,
                },
                expr => {
                    return plan_err!(
                        "Expected single column reference in sort_order[{}][{}], got {}",
                        group_idx,
                        expr_idx,
                        expr
                    );
                }
            }
        }
        if !sort_exprs.is_empty() {
            all_sort_orders.push(sort_exprs);
        }
    }
    Ok(all_sort_orders)
}

#[cfg(all(test, feature = "parquet"))]
mod tests {

    use crate::prelude::SessionContext;

    use std::fs;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::assert_batches_sorted_eq;
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use datafusion_datasource::schema_adapter::{
        DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
    };
    use datafusion_datasource::PartitionedFile;
    use datafusion_datasource_parquet::source::ParquetSource;

    use datafusion_common::record_batch;

    use ::object_store::path::Path;
    use ::object_store::ObjectMeta;
    use datafusion_physical_plan::collect;
    use tempfile::TempDir;

    #[tokio::test]
    async fn can_override_schema_adapter() {
        // Test shows that SchemaAdapter can add a column that doesn't existing in the
        // record batches returned from parquet.  This can be useful for schema evolution
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
            size: metadata.len() as usize,
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
        let source = Arc::new(
            ParquetSource::default()
                .with_schema_adapter_factory(Arc::new(TestSchemaAdapterFactory {})),
        );
        let base_conf =
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema, source)
                .with_file(partitioned_file);

        let parquet_exec = base_conf.build();

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let read = collect(parquet_exec, task_ctx).await.unwrap();

        let expected = [
            "+----+--------------+",
            "| id | extra_column |",
            "+----+--------------+",
            "| 1  | foo          |",
            "+----+--------------+",
        ];

        assert_batches_sorted_eq!(expected, &read);
    }

    #[test]
    fn default_schema_adapter() {
        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        // file has a subset of the table schema fields and different type
        let file_schema = Schema::new(vec![
            Field::new("c", DataType::Float64, true), // not in table schema
            Field::new("b", DataType::Float64, true),
        ]);

        let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
        let (mapper, indices) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(indices, vec![1]);

        let file_batch = record_batch!(("b", Float64, vec![1.0, 2.0])).unwrap();

        let mapped_batch = mapper.map_batch(file_batch).unwrap();

        // the mapped batch has the correct schema and the "b" column has been cast to Utf8
        let expected_batch = record_batch!(
            ("a", Int32, vec![None, None]), // missing column filled with nulls
            ("b", Utf8, vec!["1.0", "2.0"])  // b was cast to string and order was changed
        )
        .unwrap();
        assert_eq!(mapped_batch, expected_batch);
    }

    #[test]
    fn default_schema_adapter_non_nullable_columns() {
        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false), // "a"" is declared non nullable
            Field::new("b", DataType::Utf8, true),
        ]);
        let file_schema = Schema::new(vec![
            // since file doesn't have "a" it will be filled with nulls
            Field::new("b", DataType::Float64, true),
        ]);

        let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
        let (mapper, indices) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(indices, vec![0]);

        let file_batch = record_batch!(("b", Float64, vec![1.0, 2.0])).unwrap();

        // Mapping fails because it tries to fill in a non-nullable column with nulls
        let err = mapper.map_batch(file_batch).unwrap_err().to_string();
        assert!(err.contains("Invalid argument error: Column 'a' is declared as non-nullable but contains null values"), "{err}");
    }

    #[derive(Debug)]
    struct TestSchemaAdapterFactory;

    impl SchemaAdapterFactory for TestSchemaAdapterFactory {
        fn create(
            &self,
            projected_table_schema: SchemaRef,
            _table_schema: SchemaRef,
        ) -> Box<dyn SchemaAdapter> {
            Box::new(TestSchemaAdapter {
                table_schema: projected_table_schema,
            })
        }
    }

    struct TestSchemaAdapter {
        /// Schema for the table
        table_schema: SchemaRef,
    }

    impl SchemaAdapter for TestSchemaAdapter {
        fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
            let field = self.table_schema.field(index);
            Some(file_schema.fields.find(field.name())?.0)
        }

        fn map_schema(
            &self,
            file_schema: &Schema,
        ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
            let mut projection = Vec::with_capacity(file_schema.fields().len());

            for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
                if self.table_schema.fields().find(file_field.name()).is_some() {
                    projection.push(file_idx);
                }
            }

            Ok((Arc::new(TestSchemaMapping {}), projection))
        }
    }

    #[derive(Debug)]
    struct TestSchemaMapping {}

    impl SchemaMapper for TestSchemaMapping {
        fn map_batch(
            &self,
            batch: RecordBatch,
        ) -> datafusion_common::Result<RecordBatch> {
            let f1 = Field::new("id", DataType::Int32, true);
            let f2 = Field::new("extra_column", DataType::Utf8, true);

            let schema = Arc::new(Schema::new(vec![f1, f2]));

            let extra_column = Arc::new(StringArray::from(vec!["foo"]));
            let mut new_columns = batch.columns().to_vec();
            new_columns.push(extra_column);

            Ok(RecordBatch::try_new(schema, new_columns).unwrap())
        }

        fn map_partial_batch(
            &self,
            batch: RecordBatch,
        ) -> datafusion_common::Result<RecordBatch> {
            self.map_batch(batch)
        }
    }
}
