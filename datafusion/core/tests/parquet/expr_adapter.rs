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

use std::sync::Arc;

use arrow::array::{RecordBatch, record_batch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::{BufMut, BytesMut};
use datafusion::assert_batches_eq;
use datafusion::common::Result;
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_datasource::ListingTableUrl;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{self, Column};
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapter, DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use parquet::arrow::ArrowWriter;

async fn write_parquet(batch: RecordBatch, store: Arc<dyn ObjectStore>, path: &str) {
    let mut out = BytesMut::new().writer();
    {
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let data = out.into_inner().freeze();
    store.put(&Path::from(path), data.into()).await.unwrap();
}

// Implement a custom PhysicalExprAdapterFactory that fills in missing columns with
// the default value for the field type:
// - Int64 columns are filled with `1`
// - Utf8 columns are filled with `'b'`
#[derive(Debug)]
struct CustomPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for CustomPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(CustomPhysicalExprAdapter {
            logical_file_schema: Arc::clone(&logical_file_schema),
            physical_file_schema: Arc::clone(&physical_file_schema),
            inner: Arc::new(DefaultPhysicalExprAdapter::new(
                logical_file_schema,
                physical_file_schema,
            )),
        }))
    }
}

#[derive(Debug, Clone)]
struct CustomPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for CustomPhysicalExprAdapter {
    fn rewrite(&self, mut expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr = expr
            .transform(|expr| {
                if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                    let field_name = column.name();
                    if self
                        .physical_file_schema
                        .field_with_name(field_name)
                        .ok()
                        .is_none()
                    {
                        let field = self
                            .logical_file_schema
                            .field_with_name(field_name)
                            .map_err(|_| {
                                DataFusionError::Plan(format!(
                                    "Field '{field_name}' not found in logical file schema",
                                ))
                            })?;
                        // If the field does not exist, create a default value expression
                        // Note that we use slightly different logic here to create a default value so that we can see different behavior in tests
                        let default_value = match field.data_type() {
                            DataType::Int64 => ScalarValue::Int64(Some(1)),
                            DataType::Utf8 => ScalarValue::Utf8(Some("b".to_string())),
                            _ => unimplemented!(
                                "Unsupported data type: {}",
                                field.data_type()
                            ),
                        };
                        return Ok(Transformed::yes(Arc::new(
                            expressions::Literal::new(default_value),
                        )));
                    }
                }

                Ok(Transformed::no(expr))
            })
            .data()?;
        self.inner.rewrite(expr)
    }
}

#[tokio::test]
async fn test_custom_schema_adapter_and_custom_expression_adapter() {
    let batch =
        record_batch!(("extra", Int64, [1, 2, 3]), ("c1", Int32, [1, 2, 3])).unwrap();

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    let path = "test.parquet";
    write_parquet(batch, store.clone(), path).await;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, true),
    ]));

    let mut cfg = SessionConfig::new()
        // Disable statistics collection for this test otherwise early pruning makes it hard to demonstrate data adaptation
        .with_collect_statistics(false)
        .with_parquet_pruning(false)
        .with_parquet_page_index_pruning(false);
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));
    assert!(
        !ctx.state()
            .config_mut()
            .options_mut()
            .execution
            .collect_statistics
    );
    assert!(!ctx.state().config().collect_statistics());

    // Test with DefaultPhysicalExprAdapterFactory - missing columns are filled with NULL
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));

    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "|    | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // Test with a custom physical expr adapter
    // PhysicalExprAdapterFactory now handles both predicates AND projections
    // CustomPhysicalExprAdapterFactory fills missing columns with 'b' for Utf8
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_expr_adapter_factory(Arc::new(CustomPhysicalExprAdapterFactory));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.deregister_table("t").unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 = 'b'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // With CustomPhysicalExprAdapterFactory, missing column c2 is filled with 'b'
    // in both the predicate (c2 = 'b' becomes 'b' = 'b' -> true) and the projection
    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "| b  | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);
}

/// Test demonstrating how to implement a custom PhysicalExprAdapterFactory
/// that fills missing columns with non-null default values.
///
/// PhysicalExprAdapterFactory rewrites expressions to use literals for
/// missing columns, handling schema evolution efficiently at planning time.
#[tokio::test]
async fn test_physical_expr_adapter_with_non_null_defaults() {
    // File only has c1 column
    let batch = record_batch!(("c1", Int32, [10, 20, 30])).unwrap();

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    write_parquet(batch, store.clone(), "defaults_test.parquet").await;

    // Table schema has additional columns c2 (Utf8) and c3 (Int64) that don't exist in file
    let table_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false), // type differs from file (Int32 vs Int64)
        Field::new("c2", DataType::Utf8, true),   // missing from file
        Field::new("c3", DataType::Int64, true),  // missing from file
    ]));

    let mut cfg = SessionConfig::new()
        .with_collect_statistics(false)
        .with_parquet_pruning(false);
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

    // CustomPhysicalExprAdapterFactory fills:
    // - missing Utf8 columns with 'b'
    // - missing Int64 columns with 1
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_expr_adapter_factory(Arc::new(CustomPhysicalExprAdapterFactory));

    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    // Query all columns - missing columns should have default values
    let batches = ctx
        .sql("SELECT c1, c2, c3 FROM t ORDER BY c1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // c1 is cast from Int32 to Int64, c2 defaults to 'b', c3 defaults to 1
    let expected = [
        "+----+----+----+",
        "| c1 | c2 | c3 |",
        "+----+----+----+",
        "| 10 | b  | 1  |",
        "| 20 | b  | 1  |",
        "| 30 | b  | 1  |",
        "+----+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // Verify predicates work with default values
    // c3 = 1 should match all rows since default is 1
    let batches = ctx
        .sql("SELECT c1 FROM t WHERE c3 = 1 ORDER BY c1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| c1 |",
        "+----+",
        "| 10 |",
        "| 20 |",
        "| 30 |",
        "+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // c3 = 999 should match no rows
    let batches = ctx
        .sql("SELECT c1 FROM t WHERE c3 = 999")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = [
        "++",
        "++",
    ];
    assert_batches_eq!(expected, &batches);
}

/// Test demonstrating that a single PhysicalExprAdapterFactory instance can be
/// reused across multiple ListingTable instances.
///
/// This addresses the concern: "This is important for ListingTable. A test for
/// ListingTable would add assurance that the functionality is retained [i.e. we
/// can re-use a PhysicalExprAdapterFactory]"
#[tokio::test]
async fn test_physical_expr_adapter_factory_reuse_across_tables() {
    // Create two different parquet files with different schemas
    // File 1: has column c1 only
    let batch1 = record_batch!(("c1", Int32, [1, 2, 3])).unwrap();
    // File 2: has column c1 only but different data
    let batch2 = record_batch!(("c1", Int32, [10, 20, 30])).unwrap();

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();

    // Write files to different paths
    write_parquet(batch1, store.clone(), "table1/data.parquet").await;
    write_parquet(batch2, store.clone(), "table2/data.parquet").await;

    // Table schema has additional columns that don't exist in files
    let table_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, true), // missing from files
    ]));

    let mut cfg = SessionConfig::new()
        .with_collect_statistics(false)
        .with_parquet_pruning(false);
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

    // Create ONE factory instance wrapped in Arc - this will be REUSED
    let factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(CustomPhysicalExprAdapterFactory);

    // Create ListingTable 1 using the shared factory
    let listing_table_config1 =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///table1/").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_expr_adapter_factory(Arc::clone(&factory)); // Clone the Arc, not create new factory

    let table1 = ListingTable::try_new(listing_table_config1).unwrap();
    ctx.register_table("t1", Arc::new(table1)).unwrap();

    // Create ListingTable 2 using the SAME factory instance
    let listing_table_config2 =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///table2/").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_expr_adapter_factory(Arc::clone(&factory)); // Reuse same factory

    let table2 = ListingTable::try_new(listing_table_config2).unwrap();
    ctx.register_table("t2", Arc::new(table2)).unwrap();

    // Verify table 1 works correctly with the shared factory
    // CustomPhysicalExprAdapterFactory fills missing Utf8 columns with 'b'
    let batches = ctx
        .sql("SELECT c1, c2 FROM t1 ORDER BY c1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = [
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 1  | b  |",
        "| 2  | b  |",
        "| 3  | b  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // Verify table 2 also works correctly with the SAME shared factory
    let batches = ctx
        .sql("SELECT c1, c2 FROM t2 ORDER BY c1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = [
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 10 | b  |",
        "| 20 | b  |",
        "| 30 | b  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // Verify predicates work on both tables with the shared factory
    let batches = ctx
        .sql("SELECT c1 FROM t1 WHERE c2 = 'b' ORDER BY c1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| c1 |",
        "+----+",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "+----+",
    ];
    assert_batches_eq!(expected, &batches);

    let batches = ctx
        .sql("SELECT c1 FROM t2 WHERE c2 = 'b' ORDER BY c1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| c1 |",
        "+----+",
        "| 10 |",
        "| 20 |",
        "| 30 |",
        "+----+",
    ];
    assert_batches_eq!(expected, &batches);
}
