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
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, LargeListArray, ListArray,
    RecordBatch, StringArray, StructArray, record_batch,
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
}

impl NestedListKind {
    fn field_data_type(self, item_field: Arc<Field>) -> DataType {
        match self {
            Self::List => DataType::List(item_field),
            Self::LargeList => DataType::LargeList(item_field),
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
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::LargeList => "large_list",
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
// Returns the values at indices 0 and 1 from either a ListArray or LargeListArray.
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
    }
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
    let new_batch = nested_messages_batch(
        kind,
        2,
        &[NestedMessageRow {
            id: 30,
            name: "gamma",
            chain: Some("eth"),
            ignored: Some(99),
        }],
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
    assert_eq!(new_chain.iter().collect::<Vec<_>>(), vec![Some("eth")]);

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

/// Macro to generate paired test functions for List and LargeList variants.
/// Expands to two `#[tokio::test]` functions with the specified names.
macro_rules! test_struct_schema_evolution_pair {
    (
        list: $list_test:ident,
        large_list: $large_list_test:ident,
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
    };
    (
        list: $list_test:ident,
        large_list: $large_list_test:ident,
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
    };
}

test_struct_schema_evolution_pair!(
    list: test_list_struct_schema_evolution_end_to_end,
    large_list: test_large_list_struct_schema_evolution_end_to_end,
    fn_result: assert_nested_list_struct_schema_evolution
);

async fn assert_nested_list_struct_schema_evolution_errors(
    kind: NestedListKind,
    chain_type: DataType,
    chain_nullable: bool,
    expected_error: &str,
) {
    let batch = nested_messages_batch(
        kind,
        1,
        &[NestedMessageRow {
            id: 10,
            name: "alpha",
            chain: Some("eth"),
            ignored: None,
        }],
        &message_fields(DataType::Utf8, true, true, false),
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
        DataType::Utf8,
        false,
        "non-nullable",
    )
    .await;
}

async fn assert_incompatible_chain_field_fails(kind: NestedListKind) {
    assert_nested_list_struct_schema_evolution_errors(
        kind,
        incompatible_chain_type(),
        true,
        "Cannot cast struct field 'chain'",
    )
    .await;
}

fn incompatible_chain_type() -> DataType {
    DataType::Struct(vec![Arc::new(Field::new("value", DataType::Utf8, true))].into())
}

test_struct_schema_evolution_pair!(
    list: test_list_struct_schema_evolution_non_nullable_missing_field_fails,
    large_list: test_large_list_struct_schema_evolution_non_nullable_missing_field_fails,
    fn: assert_non_nullable_missing_chain_field_fails
);

test_struct_schema_evolution_pair!(
    list: test_list_struct_schema_evolution_incompatible_field_fails,
    large_list: test_large_list_struct_schema_evolution_incompatible_field_fails,
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
