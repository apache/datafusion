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

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Int32Array, Int64Array,
    LargeListArray, ListArray, RecordBatch, StringArray, StructArray, record_batch,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::concat_batches;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
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
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
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

#[derive(Debug, Clone, Copy)]
enum NestedListKind {
    List,
    LargeList,
    FixedSizeList,
}

const FIXED_SIZE_LIST_LEN: usize = 2;

impl NestedListKind {
    fn field_data_type(self, item_field: Arc<Field>) -> DataType {
        match self {
            Self::List => DataType::List(item_field),
            Self::LargeList => DataType::LargeList(item_field),
            Self::FixedSizeList => {
                DataType::FixedSizeList(item_field, FIXED_SIZE_LIST_LEN as i32)
            }
        }
    }

    fn array(
        self,
        item_field: Arc<Field>,
        lengths: Vec<usize>,
        values: ArrayRef,
    ) -> ArrayRef {
        match self {
            Self::List => Arc::new(ListArray::new(
                item_field,
                OffsetBuffer::<i32>::from_lengths(lengths),
                values,
                None,
            )),
            Self::LargeList => Arc::new(LargeListArray::new(
                item_field,
                OffsetBuffer::<i64>::from_lengths(lengths),
                values,
                None,
            )),
            Self::FixedSizeList => {
                assert_eq!(
                    lengths.as_slice(),
                    &[FIXED_SIZE_LIST_LEN],
                    "FixedSizeList fixtures must contain exactly {FIXED_SIZE_LIST_LEN} elements per row"
                );
                Arc::new(FixedSizeListArray::new(
                    item_field,
                    FIXED_SIZE_LIST_LEN as i32,
                    values,
                    None,
                ))
            }
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::LargeList => "large_list",
            Self::FixedSizeList => "fixed_size_list",
        }
    }
}

#[derive(Debug)]
// Fixture row for one nested struct element inside the `messages` list column.
struct NestedMessageRow<'a> {
    id: i32,
    name: &'a str,
    chain: Option<&'a str>,
    ignored: Option<i32>,
}

fn message_fields(
    chain_type: DataType,
    chain_nullable: bool,
    include_chain: bool,
    include_ignored: bool,
) -> Fields {
    let mut fields = vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("name", DataType::Utf8, true)),
    ];
    if include_chain {
        fields.push(Arc::new(Field::new("chain", chain_type, chain_nullable)));
    }
    if include_ignored {
        fields.push(Arc::new(Field::new("ignored", DataType::Int32, true)));
    }
    fields.into()
}

// Helper to construct the target message schema for struct evolution tests.
// The schema always has id (Int64), name (Utf8), and chain with parameterized type.
fn target_message_fields(chain_type: DataType, chain_nullable: bool) -> Fields {
    vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(Field::new("chain", chain_type, chain_nullable)),
    ]
    .into()
}

// Helper to build message columns in canonical order (id, name, chain, ignored)
// based on which optional fields are present in the schema.
fn build_message_columns(
    id_array: &ArrayRef,
    name_array: &ArrayRef,
    chain_vec: &[Option<&str>],
    ignored_array: &ArrayRef,
    fields: &Fields,
) -> Vec<ArrayRef> {
    let mut columns = vec![Arc::clone(id_array), Arc::clone(name_array)];

    for field in fields.iter().skip(2) {
        match field.name().as_str() {
            "chain" => {
                let chain_array = match field.data_type() {
                    DataType::Utf8 => {
                        Arc::new(StringArray::from(chain_vec.to_vec())) as ArrayRef
                    }
                    DataType::Struct(chain_fields) => {
                        let chain_struct = StructArray::new(
                            chain_fields.clone(),
                            vec![Arc::new(StringArray::from(chain_vec.to_vec()))
                                as ArrayRef],
                            None,
                        );
                        Arc::new(chain_struct) as ArrayRef
                    }
                    other => panic!("unexpected chain field type: {other:?}"),
                };
                columns.push(chain_array);
            }
            "ignored" => columns.push(Arc::clone(ignored_array)),
            _ => {}
        }
    }
    columns
}

fn nested_messages_batch(
    kind: NestedListKind,
    row_id: i32,
    messages: &[NestedMessageRow<'_>],
    fields: &Fields,
) -> RecordBatch {
    let item_field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));

    let (ids_vec, names_vec, chain_vec, ignored_vec) = messages.iter().fold(
        (
            Vec::with_capacity(messages.len()),
            Vec::with_capacity(messages.len()),
            Vec::with_capacity(messages.len()),
            Vec::with_capacity(messages.len()),
        ),
        |(mut ids, mut names, mut chains, mut ignoreds), msg| {
            ids.push(msg.id);
            names.push(Some(msg.name));
            chains.push(msg.chain);
            ignoreds.push(msg.ignored);
            (ids, names, chains, ignoreds)
        },
    );

    // Build all arrays once
    let id_array = Arc::new(Int32Array::from(ids_vec)) as ArrayRef;
    let name_array = Arc::new(StringArray::from(names_vec)) as ArrayRef;
    let ignored_array = Arc::new(Int32Array::from(ignored_vec)) as ArrayRef;

    // Build columns in canonical order (id, name, chain, ignored) based on field schema
    let columns =
        build_message_columns(&id_array, &name_array, &chain_vec, &ignored_array, fields);

    let struct_array = StructArray::new(fields.clone(), columns, None);

    // Compute the message data type first, then move item_field into kind.array()
    let message_data_type = kind.field_data_type(item_field.clone());
    let messages_array =
        kind.array(item_field, vec![messages.len()], Arc::new(struct_array));
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("messages", message_data_type, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![row_id])) as ArrayRef,
            messages_array,
        ],
    )
    .unwrap()
}

async fn register_memory_listing_table(
    ctx: &SessionContext,
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    table_schema: SchemaRef,
) {
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse(base_path).unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema)
            .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));

    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
}

fn test_context() -> SessionContext {
    let mut cfg = SessionConfig::new()
        .with_collect_statistics(false)
        .with_parquet_pruning(false)
        .with_parquet_page_index_pruning(false);
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    SessionContext::new_with_config(cfg)
}

fn nested_list_table_schema(
    kind: NestedListKind,
    target_message_fields: Fields,
) -> SchemaRef {
    let target_item = Arc::new(Field::new(
        "item",
        DataType::Struct(target_message_fields),
        true,
    ));
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("messages", kind.field_data_type(target_item), true),
    ]))
}

// Helper to extract message values from a nested list column.
// Returns the values at indices 0 and 1 from either a ListArray, LargeListArray,
// or FixedSizeListArray.
fn extract_nested_list_values(
    kind: NestedListKind,
    column: &ArrayRef,
) -> (ArrayRef, ArrayRef) {
    match kind {
        NestedListKind::List => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("messages should be a ListArray");
            (list.value(0), list.value(1))
        }
        NestedListKind::LargeList => {
            let list = column
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("messages should be a LargeListArray");
            (list.value(0), list.value(1))
        }
        NestedListKind::FixedSizeList => {
            let list = column
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("messages should be a FixedSizeListArray");
            (list.value(0), list.value(1))
        }
    }
}

fn evolved_messages(kind: NestedListKind) -> Vec<NestedMessageRow<'static>> {
    let mut messages = vec![NestedMessageRow {
        id: 30,
        name: "gamma",
        chain: Some("eth"),
        ignored: Some(99),
    }];
    if matches!(kind, NestedListKind::FixedSizeList) {
        messages.push(NestedMessageRow {
            id: 40,
            name: "delta",
            chain: Some("doge"),
            ignored: Some(100),
        });
    }
    messages
}

fn error_messages(kind: NestedListKind) -> Vec<NestedMessageRow<'static>> {
    let mut messages = vec![NestedMessageRow {
        id: 10,
        name: "alpha",
        chain: Some("eth"),
        ignored: None,
    }];
    if matches!(kind, NestedListKind::FixedSizeList) {
        messages.push(NestedMessageRow {
            id: 20,
            name: "beta",
            chain: Some("doge"),
            ignored: None,
        });
    }
    messages
}

// Helper to set up a nested list test fixture.
// Creates an in-memory store, writes the provided batches to parquet files,
// creates a SessionContext, and registers the resulting table.
// Returns the prepared context ready for queries.
async fn setup_nested_list_test(
    kind: NestedListKind,
    prefix_base: &str,
    batches: Vec<(String, RecordBatch)>,
    table_schema: SchemaRef,
) -> SessionContext {
    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let prefix = format!("{}_{}", kind.name(), prefix_base);

    for (filename, batch) in batches {
        write_parquet(batch, Arc::clone(&store), &format!("{prefix}/{filename}")).await;
    }

    let ctx = test_context();
    register_memory_listing_table(
        &ctx,
        Arc::clone(&store),
        &format!("memory:///{prefix}/"),
        table_schema,
    )
    .await;

    ctx
}

async fn assert_nested_list_struct_schema_evolution(kind: NestedListKind) -> Result<()> {
    // old.parquet shape: messages item struct has only (id, name), no `chain`.
    let old_batch = nested_messages_batch(
        kind,
        1,
        &[
            NestedMessageRow {
                id: 10,
                name: "alpha",
                chain: None,
                ignored: None,
            },
            NestedMessageRow {
                id: 20,
                name: "beta",
                chain: None,
                ignored: None,
            },
        ],
        &message_fields(DataType::Utf8, true, false, false),
    );

    // new.parquet shape: messages item struct adds nullable `chain` and extra `ignored`.
    let new_messages = evolved_messages(kind);
    let new_batch = nested_messages_batch(
        kind,
        2,
        &new_messages,
        &message_fields(DataType::Utf8, true, true, true),
    );

    // Logical table schema expects evolved shape (id, name, nullable `chain`) and
    // should ignore source-only `ignored` during reads.
    let table_schema =
        nested_list_table_schema(kind, target_message_fields(DataType::Utf8, true));

    let ctx = setup_nested_list_test(
        kind,
        "struct_evolution",
        vec![
            ("old.parquet".to_string(), old_batch),
            ("new.parquet".to_string(), new_batch),
        ],
        table_schema,
    )
    .await;

    let select_all = ctx
        .sql("SELECT * FROM t ORDER BY row_id")
        .await?
        .collect()
        .await?;
    let all_rows = concat_batches(&select_all[0].schema(), &select_all)?;

    let row_ids = all_rows
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("row_id should be Int32");
    assert_eq!(row_ids.values(), &[1, 2]);

    let (messages0, messages1) = extract_nested_list_values(kind, all_rows.column(1));

    let messages0 = messages0
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("messages[0] should be a StructArray");
    let old_ids = messages0
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(old_ids.values(), &[10, 20]);
    let old_chain = messages0
        .column_by_name("chain")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(old_chain.iter().collect::<Vec<_>>(), vec![None, None]);

    let messages1 = messages1
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("messages[1] should be a StructArray");
    assert!(
        messages1.column_by_name("ignored").is_none(),
        "extra source fields should not appear in the logical schema"
    );
    let new_chain = messages1
        .column_by_name("chain")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let expected_new_chain = if matches!(kind, NestedListKind::FixedSizeList) {
        vec![Some("eth"), Some("doge")]
    } else {
        vec![Some("eth")]
    };
    assert_eq!(new_chain.iter().collect::<Vec<_>>(), expected_new_chain);

    let projected = ctx
        .sql(
            "SELECT row_id, get_field(messages[1], 'id') AS msg_id, \
             get_field(messages[1], 'chain') AS chain \
             FROM t ORDER BY row_id",
        )
        .await?
        .collect()
        .await?;

    #[rustfmt::skip]
    let expected = [
        "+--------+--------+-------+",
        "| row_id | msg_id | chain |",
        "+--------+--------+-------+",
        "| 1      | 10     |       |",
        "| 2      | 30     | eth   |",
        "+--------+--------+-------+",
    ];
    assert_batches_eq!(expected, &projected);

    Ok(())
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
                if let Some(column) = expr.downcast_ref::<Column>() {
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

#[tokio::test]
async fn test_struct_schema_evolution_projection_and_filter() -> Result<()> {
    use std::collections::HashMap;

    // Physical struct: {id: Int32, name: Utf8}
    let physical_struct_fields: Fields = vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::new(Field::new("name", DataType::Utf8, true)),
    ]
    .into();

    let struct_array = StructArray::new(
        physical_struct_fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
        None,
    );

    let physical_schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(physical_struct_fields),
        true,
    )]));

    let batch =
        RecordBatch::try_new(Arc::clone(&physical_schema), vec![Arc::new(struct_array)])?;

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    write_parquet(batch, store.clone(), "struct_evolution.parquet").await;

    // Logical struct: {id: Int64?, name: Utf8?, extra: Boolean?} + metadata
    let logical_struct_fields: Fields = vec![
        Arc::new(Field::new("id", DataType::Int64, true)),
        Arc::new(Field::new("name", DataType::Utf8, true)),
        Arc::new(Field::new("extra", DataType::Boolean, true).with_metadata(
            HashMap::from([("nested_meta".to_string(), "1".to_string())]),
        )),
    ]
    .into();

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Struct(logical_struct_fields), false)
            .with_metadata(HashMap::from([("top_meta".to_string(), "1".to_string())])),
    ]));

    let mut cfg = SessionConfig::new()
        .with_collect_statistics(false)
        .with_parquet_pruning(false)
        .with_parquet_page_index_pruning(false);
    cfg.options_mut().execution.parquet.pushdown_filters = true;

    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

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
        .sql("SELECT s FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);

    // Verify top-level metadata propagation
    let output_schema = batches[0].schema();
    let s_field = output_schema.field_with_name("s").unwrap();
    assert_eq!(
        s_field.metadata().get("top_meta").map(String::as_str),
        Some("1")
    );

    // Verify nested struct type/field propagation + values
    let s_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("expected struct array");

    let id_array = s_array
        .column_by_name("id")
        .expect("id column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id should be cast to Int64");
    assert_eq!(id_array.values(), &[1, 2, 3]);

    let extra_array = s_array.column_by_name("extra").expect("extra column");
    assert_eq!(extra_array.null_count(), 3);

    // Verify nested field metadata propagation
    let extra_field = match s_field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .find(|f| f.name() == "extra")
            .expect("extra field"),
        other => panic!("expected struct type for s, got {other:?}"),
    };
    assert_eq!(
        extra_field
            .metadata()
            .get("nested_meta")
            .map(String::as_str),
        Some("1")
    );

    // Smoke test: filtering on a missing nested field evaluates correctly
    let filtered = ctx
        .sql("SELECT get_field(s, 'extra') AS extra FROM t WHERE get_field(s, 'extra') IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].num_rows(), 3);
    let extra = filtered[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("extra should be a boolean array");
    assert_eq!(extra.null_count(), 3);

    Ok(())
}

/// Macro to generate schema evolution tests for list-like variants.
macro_rules! test_struct_schema_evolution_variants {
    (
        list: $list_test:ident,
        large_list: $large_list_test:ident,
        fixed_size_list: $fixed_size_list_test:ident,
        fn: $assertion_fn:path $(, args: $($arg:expr),+)?
    ) => {
        #[tokio::test]
        async fn $list_test() {
            $assertion_fn(NestedListKind::List $(, $($arg),+)?).await;
        }

        #[tokio::test]
        async fn $large_list_test() {
            $assertion_fn(NestedListKind::LargeList $(, $($arg),+)?).await;
        }

        #[tokio::test]
        async fn $fixed_size_list_test() {
            $assertion_fn(NestedListKind::FixedSizeList $(, $($arg),+)?).await;
        }
    };
    (
        list: $list_test:ident,
        large_list: $large_list_test:ident,
        fixed_size_list: $fixed_size_list_test:ident,
        fn_result: $assertion_fn:path
    ) => {
        #[tokio::test]
        async fn $list_test() -> Result<()> {
            $assertion_fn(NestedListKind::List).await
        }

        #[tokio::test]
        async fn $large_list_test() -> Result<()> {
            $assertion_fn(NestedListKind::LargeList).await
        }

        #[tokio::test]
        async fn $fixed_size_list_test() -> Result<()> {
            $assertion_fn(NestedListKind::FixedSizeList).await
        }
    };
}

test_struct_schema_evolution_variants!(
    list: test_list_struct_schema_evolution_end_to_end,
    large_list: test_large_list_struct_schema_evolution_end_to_end,
    fixed_size_list: test_fixed_size_list_struct_schema_evolution_end_to_end,
    fn_result: assert_nested_list_struct_schema_evolution
);

async fn assert_nested_list_struct_schema_evolution_errors(
    kind: NestedListKind,
    source_includes_chain: bool,
    chain_type: DataType,
    chain_nullable: bool,
    expected_error: &str,
) {
    let messages = error_messages(kind);
    let batch = nested_messages_batch(
        kind,
        1,
        &messages,
        &message_fields(DataType::Utf8, true, source_includes_chain, false),
    );

    let table_schema =
        nested_list_table_schema(kind, target_message_fields(chain_type, chain_nullable));

    let ctx = setup_nested_list_test(
        kind,
        "struct_evolution_error",
        vec![("data.parquet".to_string(), batch)],
        table_schema,
    )
    .await;

    let err = ctx
        .sql("SELECT * FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains(expected_error),
        "expected error to contain '{expected_error}', got: {err}"
    );
}

async fn assert_non_nullable_missing_chain_field_fails(kind: NestedListKind) {
    assert_nested_list_struct_schema_evolution_errors(
        kind,
        false,
        DataType::Utf8,
        false,
        "non-nullable",
    )
    .await;
}

async fn assert_incompatible_chain_field_fails(kind: NestedListKind) {
    assert_nested_list_struct_schema_evolution_errors(
        kind,
        true,
        incompatible_chain_type(),
        true,
        "Cannot cast struct field 'chain'",
    )
    .await;
}

fn incompatible_chain_type() -> DataType {
    DataType::Struct(vec![Arc::new(Field::new("value", DataType::Utf8, true))].into())
}

test_struct_schema_evolution_variants!(
    list: test_list_struct_schema_evolution_non_nullable_missing_field_fails,
    large_list: test_large_list_struct_schema_evolution_non_nullable_missing_field_fails,
    fixed_size_list: test_fixed_size_list_struct_schema_evolution_non_nullable_missing_field_fails,
    fn: assert_non_nullable_missing_chain_field_fails
);

test_struct_schema_evolution_variants!(
    list: test_list_struct_schema_evolution_incompatible_field_fails,
    large_list: test_large_list_struct_schema_evolution_incompatible_field_fails,
    fixed_size_list: test_fixed_size_list_struct_schema_evolution_incompatible_field_fails,
    fn: assert_incompatible_chain_field_fails
);

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

// ---------------------------------------------------------------------------
// Nested projection pruning: when the table schema declares a nested column
// narrower than the physical parquet file, the scan should only read the
// leaves the declared schema names, instead of reading the whole column and
// discarding the extra subfields in the adapter-inserted cast.
//
// Each test registers two tables against the *same* physical file: `t_narrow`
// (the declared schema under test) and `t_full` (the file's own physical
// schema, so no cast is inserted and the scan always reads every leaf). That
// gives a same-context upper bound to compare `bytes_scanned` against,
// without needing a config flag to disable pruning.
// ---------------------------------------------------------------------------

mod nested_projection_pruning {
    use super::*;
    use arrow::buffer::NullBuffer;
    use datafusion::physical_plan::collect;
    use datafusion_physical_plan::metrics::MetricsSet;

    use crate::parquet::utils::MetricsFinder;

    const NUM_ELEMENTS: usize = 64;
    const PAD_LEN: usize = 2048;

    /// Physical item struct written to the file: the narrow fields plus fat
    /// pads the narrow table schema will not mention. `x` is Int32 in the
    /// file (the narrow schema declares Int64 to also exercise leaf
    /// promotion).
    fn wide_item_fields() -> Fields {
        Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
            Field::new("pad_a", DataType::Utf8, false),
            Field::new("pad_b", DataType::Utf8, false),
            Field::new("pad_c", DataType::Utf8, false),
        ])
    }

    /// The narrow item struct one table declares: a subset of the physical
    /// fields in a different order, a promoted leaf type for `x`, plus `z`
    /// which does not exist in the file (null-filled by the cast).
    fn narrow_item_fields() -> Fields {
        Fields::from(vec![
            Field::new("y", DataType::Utf8, true),
            Field::new("x", DataType::Int64, true),
            Field::new("z", DataType::Int64, true),
        ])
    }

    fn wide_struct_values(validity: Option<NullBuffer>) -> StructArray {
        let pad = |seed: usize| {
            let base = "x".repeat(PAD_LEN);
            Arc::new(StringArray::from_iter_values(
                (0..NUM_ELEMENTS).map(|i| format!("{}{base}", seed + i)),
            )) as ArrayRef
        };
        StructArray::new(
            wide_item_fields(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..NUM_ELEMENTS as i32)),
                Arc::new(StringArray::from_iter_values(
                    (0..NUM_ELEMENTS).map(|i| format!("y-{i}")),
                )),
                pad(1000),
                pad(2000),
                pad(3000),
            ],
            validity,
        )
    }

    fn wide_list_schema() -> SchemaRef {
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(wide_item_fields()),
            true,
        ));
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("events", DataType::List(item), true),
        ]))
    }

    /// File batch: `id Int32`, `events List<wide struct>` (one element per row).
    fn wide_list_batch() -> RecordBatch {
        let schema = wide_list_schema();
        let item = match schema.field(1).data_type() {
            DataType::List(item) => Arc::clone(item),
            other => unreachable!("expected List, got {other:?}"),
        };
        let events = ListArray::new(
            item,
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, NUM_ELEMENTS)),
            Arc::new(wide_struct_values(None)),
            None,
        );
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..NUM_ELEMENTS as i32)),
                Arc::new(events),
            ],
        )
        .unwrap()
    }

    fn narrow_list_table_schema() -> SchemaRef {
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(narrow_item_fields()),
            true,
        ));
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("events", DataType::List(item), true),
        ]))
    }

    fn wide_struct_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(wide_item_fields()), true),
        ]))
    }

    /// File batch: `id Int32`, `s <wide struct>`, with per-row struct
    /// validity so struct-level nullability can be asserted.
    fn wide_struct_batch() -> RecordBatch {
        // rows 0, 10, 20, ... have a NULL struct
        let validity =
            NullBuffer::from((0..NUM_ELEMENTS).map(|i| i % 10 != 0).collect::<Vec<_>>());
        RecordBatch::try_new(
            wide_struct_schema(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..NUM_ELEMENTS as i32)),
                Arc::new(wide_struct_values(Some(validity))),
            ],
        )
        .unwrap()
    }

    fn narrow_struct_table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(narrow_item_fields()), true),
        ]))
    }

    /// Registers `t_narrow` (the schema under test) and `t_full` (the file's
    /// own physical schema, so no cast is inserted) against the same store.
    async fn register_narrow_and_full(
        ctx: &SessionContext,
        store: Arc<dyn ObjectStore>,
        narrow_schema: SchemaRef,
        full_schema: SchemaRef,
    ) {
        let store_url = ObjectStoreUrl::parse("memory://").unwrap();
        ctx.register_object_store(store_url.as_ref(), store);

        for (name, schema) in [("t_narrow", narrow_schema), ("t_full", full_schema)] {
            let config = ListingTableConfig::new(
                ListingTableUrl::parse("memory:///data/").unwrap(),
            )
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(schema)
            .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));
            let table = ListingTable::try_new(config).unwrap();
            ctx.register_table(name, Arc::new(table)).unwrap();
        }
    }

    async fn setup_with_config(
        batches: Vec<(&str, RecordBatch)>,
        narrow_schema: SchemaRef,
        full_schema: SchemaRef,
        cfg: SessionConfig,
    ) -> SessionContext {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        for (name, batch) in batches {
            write_parquet(batch, Arc::clone(&store), &format!("data/{name}")).await;
        }
        let ctx = SessionContext::new_with_config(cfg);
        register_narrow_and_full(&ctx, store, narrow_schema, full_schema).await;
        ctx
    }

    async fn setup(
        batches: Vec<(&str, RecordBatch)>,
        narrow_schema: SchemaRef,
        full_schema: SchemaRef,
    ) -> SessionContext {
        setup_with_config(
            batches,
            narrow_schema,
            full_schema,
            SessionConfig::new().with_collect_statistics(false),
        )
        .await
    }

    async fn run(ctx: &SessionContext, sql: &str) -> (Vec<RecordBatch>, MetricsSet) {
        let df = ctx.sql(sql).await.unwrap();
        let (state, logical) = df.into_parts();
        let plan = state.create_physical_plan(&logical).await.unwrap();
        let batches = collect(Arc::clone(&plan), state.task_ctx()).await.unwrap();
        let metrics = MetricsFinder::find_metrics(plan.as_ref()).unwrap();
        (batches, metrics)
    }

    fn bytes_scanned(metrics: &MetricsSet) -> usize {
        metrics
            .sum(|m| m.value().name() == "bytes_scanned")
            .map(|v| v.as_usize())
            .expect("bytes_scanned metric")
    }

    /// Run `narrow_sql` against `t_narrow` and `full_sql` against `t_full`;
    /// assert the narrow scan read strictly less than half of the full
    /// scan's bytes (the pads dominate the file), and return the narrow
    /// scan's results for correctness assertions.
    ///
    /// The two SQL strings need not have the same shape: a `get_field` over
    /// a narrowed struct clips to the *cast target*, not further down to the
    /// specific field accessed (see `prunes_get_field_on_narrowed_struct`),
    /// so comparing against the same `get_field` query on `t_full` would
    /// unfairly compare this clip against `get_field`'s own, more precise,
    /// single-leaf pruning (which only applies when there is no cast in the
    /// way). Callers that aren't in that situation can just pass the same
    /// query shape with the table name substituted.
    async fn assert_prunes(
        batches: Vec<(&str, RecordBatch)>,
        narrow_schema: SchemaRef,
        full_schema: SchemaRef,
        narrow_sql: &str,
        full_sql: &str,
    ) -> Vec<RecordBatch> {
        let ctx = setup(batches, narrow_schema, full_schema).await;

        let (result_narrow, metrics_narrow) = run(&ctx, narrow_sql).await;
        let (_result_full, metrics_full) = run(&ctx, full_sql).await;

        let (narrow_bytes, full_bytes) =
            (bytes_scanned(&metrics_narrow), bytes_scanned(&metrics_full));
        assert!(
            narrow_bytes * 2 < full_bytes,
            "expected pruned scan to read less than half of {full_bytes} bytes, \
             read {narrow_bytes}: {narrow_sql}"
        );
        result_narrow
    }

    #[tokio::test]
    async fn prunes_list_of_struct() {
        // Narrow schema over the wide file: subset of fields, reordered,
        // promoted leaf (x: Int32 -> Int64), missing subfield z null-filled.
        let batches = assert_prunes(
            vec![("wide.parquet", wide_list_batch())],
            narrow_list_table_schema(),
            wide_list_schema(),
            "SELECT events FROM t_narrow ORDER BY id",
            "SELECT events FROM t_full ORDER BY id",
        )
        .await;

        let events = batches[0].column(0);
        let list = events.as_any().downcast_ref::<ListArray>().unwrap();
        let items = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(items.fields().len(), 3);
        let x = items
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(x.value(5), 5);
        let z = items.column_by_name("z").unwrap();
        assert_eq!(z.null_count(), z.len(), "z is not in the file");
    }

    #[tokio::test]
    async fn prunes_top_level_struct() {
        assert_prunes(
            vec![("wide.parquet", wide_struct_batch())],
            narrow_struct_table_schema(),
            wide_struct_schema(),
            "SELECT s FROM t_narrow ORDER BY id",
            "SELECT s FROM t_full ORDER BY id",
        )
        .await;
    }

    /// Struct-level nullability must survive the clip: rows where the struct
    /// itself is NULL stay NULL (not `{y: NULL, x: NULL, z: NULL}`).
    #[tokio::test]
    async fn preserves_struct_nullability() {
        let batches = assert_prunes(
            vec![("wide.parquet", wide_struct_batch())],
            narrow_struct_table_schema(),
            wide_struct_schema(),
            "SELECT id, s IS NULL AS s_null, s FROM t_narrow ORDER BY id",
            "SELECT id, s IS NULL AS s_null, s FROM t_full ORDER BY id",
        )
        .await;

        let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
        let s_null = combined
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..NUM_ELEMENTS {
            assert_eq!(s_null.value(i), i % 10 == 0, "row {i}");
        }
    }

    /// `get_field` on a schema-narrowed struct becomes
    /// `get_field(CAST(s), 'x')`; the read clips to the cast target (every
    /// field the *narrow* schema declares), not further down to just `x`.
    /// The fair "no clipping happened" baseline is therefore reading every
    /// physical leaf of `s` (`SELECT s FROM t_full`), not the same
    /// `get_field` query against `t_full` — that query needs no cast at all
    /// and takes `get_field`'s own, more precise, single-leaf pushdown path.
    #[tokio::test]
    async fn prunes_get_field_on_narrowed_struct() {
        let batches = assert_prunes(
            vec![("wide.parquet", wide_struct_batch())],
            narrow_struct_table_schema(),
            wide_struct_schema(),
            "SELECT s['x'] AS x FROM t_narrow ORDER BY id",
            "SELECT s FROM t_full ORDER BY id",
        )
        .await;
        let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
        let x = combined
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(x.value(5), 5);
        assert_eq!(x.value(NUM_ELEMENTS - 1), NUM_ELEMENTS as i64 - 1);
    }

    /// Mixed access: the whole (narrowed) column and a subfield of it.
    #[tokio::test]
    async fn prunes_mixed_struct_and_subfield_access() {
        assert_prunes(
            vec![("wide.parquet", wide_struct_batch())],
            narrow_struct_table_schema(),
            wide_struct_schema(),
            "SELECT s, s['y'] AS y FROM t_narrow ORDER BY id",
            "SELECT s, s['y'] AS y FROM t_full ORDER BY id",
        )
        .await;
    }

    /// Predicate on a primitive column with filter pushdown enabled while
    /// the projected nested column is clipped.
    #[tokio::test]
    async fn prunes_with_filter_pushdown() {
        let mut cfg = SessionConfig::new().with_collect_statistics(false);
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        let ctx = setup_with_config(
            vec![("wide.parquet", wide_list_batch())],
            narrow_list_table_schema(),
            wide_list_schema(),
            cfg,
        )
        .await;

        let filter = "WHERE id >= 32 ORDER BY id";
        let (result_narrow, metrics_narrow) =
            run(&ctx, &format!("SELECT events FROM t_narrow {filter}")).await;
        let (_result_full, metrics_full) =
            run(&ctx, &format!("SELECT events FROM t_full {filter}")).await;

        let combined =
            concat_batches(&result_narrow[0].schema(), &result_narrow).unwrap();
        assert_eq!(combined.num_rows(), NUM_ELEMENTS / 2);
        assert!(bytes_scanned(&metrics_narrow) * 2 < bytes_scanned(&metrics_full));
    }

    /// A scan over two files where one matches the table schema exactly (no
    /// cast is inserted) and one is wider (clipped): both must be read
    /// correctly in the same scan.
    #[tokio::test]
    async fn mixed_files_narrow_and_wide() {
        // The physically-narrow file has exactly the table's item struct.
        let narrow_item = narrow_item_fields();
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(narrow_item.clone()),
            true,
        ));
        let events = ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths([1]),
            Arc::new(StructArray::new(
                narrow_item,
                vec![
                    Arc::new(StringArray::from(vec![Some("y-narrow")])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(4242)])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
                ],
                None,
            )),
            None,
        );
        let narrow_batch = RecordBatch::try_new(
            narrow_list_table_schema(),
            vec![
                Arc::new(Int32Array::from(vec![NUM_ELEMENTS as i32])),
                Arc::new(events),
            ],
        )
        .unwrap();

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        write_parquet(wide_list_batch(), Arc::clone(&store), "data/wide.parquet").await;
        write_parquet(narrow_batch, Arc::clone(&store), "data/narrow.parquet").await;

        let ctx = test_context();
        register_memory_listing_table(
            &ctx,
            store,
            "memory:///data/",
            narrow_list_table_schema(),
        )
        .await;

        let (batches, _) = run(
            &ctx,
            "SELECT id, e['x'] AS x, e['z'] AS z \
             FROM (SELECT id, unnest(events) AS e FROM t) ORDER BY id",
        )
        .await;
        let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(combined.num_rows(), NUM_ELEMENTS + 1);
        let x = combined
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(x.value(NUM_ELEMENTS), 4242, "row from the narrow file");
        let z = combined.column(2);
        // z is null-filled for the wide file, present in the narrow file
        assert_eq!(z.null_count(), NUM_ELEMENTS);
    }

    /// Regression test for the exact shape reported in
    /// `datafusion-comet#4859`: a two-level `array<struct<...,
    /// items: array<struct<...>>>>` column, with a dropped struct sibling
    /// (`latency_parts`), a dropped map sibling (`feature_map`), a dropped
    /// nested-struct sibling (`diagnostics`), and dropped top-level sibling
    /// columns (`dimension_id`, `region_code`, `raw_payload`) — structurally
    /// the same `ReadSchema`/`InputSchema` pair from the issue (field names
    /// kept representative, not verbatim), which let Comet's production
    /// query read 1.35 TB where plain Spark, given the same pruned
    /// `ReadSchema`, read 30.9 GB.
    #[tokio::test]
    async fn comet_4859_two_level_nested_list_regression() {
        use arrow::array::{Float64Array, new_null_array};

        const NUM_ROWS: usize = 8;
        const EVENTS_PER_ROW: usize = 2;
        const ITEMS_PER_EVENT: usize = 2;
        const NUM_EVENTS: usize = NUM_ROWS * EVENTS_PER_ROW;
        const NUM_ITEMS: usize = NUM_EVENTS * ITEMS_PER_EVENT;

        // items: array<struct<group_id, entity_id, metric_value, feature_map, diagnostics, pad>>
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Float64, true),
                ])),
                false,
            )),
            false,
        );
        let diagnostics_type = DataType::Struct(Fields::from(vec![
            Field::new("module_id", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
        ]));
        let wide_item_struct_fields = Fields::from(vec![
            Field::new("group_id", DataType::Int64, false),
            Field::new("entity_id", DataType::Int64, false),
            Field::new("metric_value", DataType::Float64, false),
            Field::new("feature_map", map_type.clone(), true),
            Field::new("diagnostics", diagnostics_type.clone(), true),
            Field::new("pad", DataType::Utf8, false),
        ]);
        let pad_base = "x".repeat(PAD_LEN);
        let items_struct = StructArray::new(
            wide_item_struct_fields.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..NUM_ITEMS as i64)),
                Arc::new(Int64Array::from_iter_values(
                    (0..NUM_ITEMS).map(|i| 100 + i as i64),
                )),
                Arc::new(Float64Array::from_iter_values(
                    (0..NUM_ITEMS).map(|i| i as f64 * 1.5),
                )),
                new_null_array(&map_type, NUM_ITEMS),
                new_null_array(&diagnostics_type, NUM_ITEMS),
                Arc::new(StringArray::from_iter_values(
                    (0..NUM_ITEMS).map(|i| format!("{i:08}{pad_base}")),
                )),
            ],
            None,
        );
        let items_item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(wide_item_struct_fields),
            true,
        ));
        let items_list = ListArray::new(
            Arc::clone(&items_item_field),
            OffsetBuffer::from_lengths(std::iter::repeat_n(ITEMS_PER_EVENT, NUM_EVENTS)),
            Arc::new(items_struct),
            None,
        );

        // events: array<struct<is_available, event_time_ms, event_token, latency_parts, items>>
        let latency_type = DataType::Struct(Fields::from(vec![
            Field::new("queue_time_ms", DataType::Int64, true),
            Field::new("retry_count", DataType::Int32, true),
        ]));
        let wide_event_fields = Fields::from(vec![
            Field::new("is_available", DataType::Boolean, false),
            Field::new("event_time_ms", DataType::Int64, false),
            Field::new("event_token", DataType::Utf8, false),
            Field::new("latency_parts", latency_type.clone(), true),
            Field::new("items", DataType::List(items_item_field), true),
        ]);
        let events_struct = StructArray::new(
            wide_event_fields.clone(),
            vec![
                Arc::new(BooleanArray::from_iter(
                    (0..NUM_EVENTS).map(|i| Some(i % 2 == 0)),
                )),
                Arc::new(Int64Array::from_iter_values(0..NUM_EVENTS as i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..NUM_EVENTS).map(|i| format!("token-{i}")),
                )),
                new_null_array(&latency_type, NUM_EVENTS),
                Arc::new(items_list),
            ],
            None,
        );
        let events_item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(wide_event_fields),
            true,
        ));
        let events_list = ListArray::new(
            Arc::clone(&events_item_field),
            OffsetBuffer::from_lengths(std::iter::repeat_n(EVENTS_PER_ROW, NUM_ROWS)),
            Arc::new(events_struct),
            None,
        );

        let wide_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("is_flagged", DataType::Boolean, false),
            Field::new("dimension_id", DataType::Int64, true),
            Field::new("region_code", DataType::Utf8, true),
            Field::new(
                "events",
                DataType::List(Arc::clone(&events_item_field)),
                true,
            ),
            Field::new("raw_payload", DataType::Utf8, true),
        ]));
        let wide_batch = RecordBatch::try_new(
            Arc::clone(&wide_schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..NUM_ROWS as i32)),
                Arc::new(BooleanArray::from_iter(
                    (0..NUM_ROWS).map(|i| Some(i % 3 == 0)),
                )),
                new_null_array(&DataType::Int64, NUM_ROWS),
                new_null_array(&DataType::Utf8, NUM_ROWS),
                Arc::new(events_list),
                new_null_array(&DataType::Utf8, NUM_ROWS),
            ],
        )
        .unwrap();

        let narrow_item_type = DataType::Struct(Fields::from(vec![
            Field::new("group_id", DataType::Int64, true),
            Field::new("entity_id", DataType::Int64, true),
            Field::new("metric_value", DataType::Float64, true),
        ]));
        let narrow_event_type = DataType::Struct(Fields::from(vec![
            Field::new("is_available", DataType::Boolean, true),
            Field::new("event_time_ms", DataType::Int64, true),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", narrow_item_type, true))),
                true,
            ),
        ]));
        let narrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("is_flagged", DataType::Boolean, false),
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new("item", narrow_event_type, true))),
                true,
            ),
        ]));

        let batches = assert_prunes(
            vec![("wide.parquet", wide_batch)],
            narrow_schema,
            wide_schema,
            "SELECT events FROM t_narrow ORDER BY id",
            "SELECT events FROM t_full ORDER BY id",
        )
        .await;

        let events = batches[0].column(0);
        let events_list = events.as_any().downcast_ref::<ListArray>().unwrap();
        let event_structs = events_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        // event_token and latency_parts are dropped; only is_available,
        // event_time_ms, and items survive.
        assert_eq!(event_structs.fields().len(), 3);

        let items_list = event_structs
            .column_by_name("items")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let item_structs = items_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        // feature_map, diagnostics, and pad are dropped; only group_id,
        // entity_id, and metric_value survive, at the *inner* list<struct>
        // nested two levels deep inside the outer one.
        assert_eq!(item_structs.fields().len(), 3);
        assert_eq!(item_structs.len(), NUM_ITEMS);
        let group_id = item_structs
            .column_by_name("group_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            group_id.values(),
            &(0..NUM_ITEMS as i64).collect::<Vec<_>>()
        );
    }
}
