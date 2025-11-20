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

//! See `main.rs` for how to run it.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};

use datafusion::assert_batches_eq;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion::common::{assert_contains, exec_datafusion_err, Result};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{expressions, ScalarFunctionExpr};
use datafusion::prelude::SessionConfig;
use datafusion::scalar::ScalarValue;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

// Example showing how to implement custom filter rewriting for JSON shredding.
//
// JSON shredding is a technique for optimizing queries on semi-structured data
// by materializing commonly accessed fields into separate columns for better
// columnar storage performance.
//
// In this example, we have a table with both:
// - Original JSON data: data: '{"age": 30}'
// - Shredded flat columns: _data.name: "Alice" (extracted from JSON)
//
// Our custom TableProvider uses a PhysicalExprAdapter to rewrite
// expressions like `json_get_str('name', data)` to use the pre-computed
// flat column `_data.name` when available. This allows the query engine to:
// 1. Push down predicates for better filtering
// 2. Avoid expensive JSON parsing at query time
// 3. Leverage columnar storage benefits for the materialized fields
pub async fn json_shredding() -> Result<()> {
    println!("=== Creating example data with flat columns and underscore prefixes ===");

    // Create sample data with flat columns using underscore prefixes
    let (table_schema, batch) = create_sample_data();

    let store = InMemory::new();
    let buf = {
        let mut buf = vec![];

        let props = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .expect("creating writer");

        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
        buf
    };
    let path = Path::from("example.parquet");
    let payload = PutPayload::from_bytes(buf.into());
    store.put(&path, payload).await?;

    // Set up query execution
    let mut cfg = SessionConfig::new();
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::new(store),
    );

    // Create a custom table provider that rewrites struct field access
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///example.parquet")?)
            .infer_options(&ctx.state())
            .await?
            .with_schema(table_schema)
            .with_expr_adapter_factory(Arc::new(ShreddedJsonRewriterFactory));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    let table_provider = Arc::new(table);

    // Register our table
    ctx.register_table("structs", table_provider)?;
    ctx.register_udf(ScalarUDF::new_from_impl(JsonGetStr::default()));

    println!("\n=== Showing all data ===");
    let batches = ctx.sql("SELECT * FROM structs").await?.collect().await?;
    arrow::util::pretty::print_batches(&batches)?;

    println!("\n=== Running query with flat column access and filter ===");
    let query = "SELECT json_get_str('age', data) as age FROM structs WHERE json_get_str('name', data) = 'Bob'";
    println!("Query: {query}");

    let batches = ctx.sql(query).await?.collect().await?;

    #[rustfmt::skip]
    let expected = [
        "+-----+",
        "| age |",
        "+-----+",
        "| 25  |",
        "+-----+",
    ];
    arrow::util::pretty::print_batches(&batches)?;
    assert_batches_eq!(expected, &batches);

    println!("\n=== Running explain analyze to confirm row group pruning ===");

    let batches = ctx
        .sql(&format!("EXPLAIN ANALYZE {query}"))
        .await?
        .collect()
        .await?;
    let plan = format!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    println!("{plan}");
    assert_contains!(&plan, "row_groups_pruned_statistics=2 total → 1 matched");
    assert_contains!(&plan, "pushdown_rows_pruned=1");

    Ok(())
}

/// Create the example data with flat columns using underscore prefixes.
///
/// This demonstrates the logical data structure:
/// - Table schema: What users see (just the 'data' JSON column)
/// - File schema: What's physically stored (both 'data' and materialized '_data.name')
///
/// The naming convention uses underscore prefixes to indicate shredded columns:
/// - `data` -> original JSON column
/// - `_data.name` -> materialized field from JSON data.name
fn create_sample_data() -> (SchemaRef, RecordBatch) {
    // The table schema only has the main data column - this is what users query against
    let table_schema = Schema::new(vec![Field::new("data", DataType::Utf8, false)]);

    // The file schema has both the main column and the shredded flat column with underscore prefix
    // This represents the actual physical storage with pre-computed columns
    let file_schema = Schema::new(vec![
        Field::new("data", DataType::Utf8, false), // Original JSON data
        Field::new("_data.name", DataType::Utf8, false), // Materialized name field
    ]);

    let batch = create_sample_record_batch(&file_schema);

    (Arc::new(table_schema), batch)
}

/// Create the actual RecordBatch with sample data
fn create_sample_record_batch(file_schema: &Schema) -> RecordBatch {
    // Build a RecordBatch with flat columns
    let data_array = StringArray::from(vec![
        r#"{"age": 30}"#,
        r#"{"age": 25}"#,
        r#"{"age": 35}"#,
        r#"{"age": 22}"#,
    ]);
    let names_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"]);

    RecordBatch::try_new(
        Arc::new(file_schema.clone()),
        vec![Arc::new(data_array), Arc::new(names_array)],
    )
    .unwrap()
}

/// Scalar UDF that uses serde_json to access json fields
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonGetStr {
    signature: Signature,
}

impl Default for JsonGetStr {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonGetStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert!(
            args.args.len() == 2,
            "json_get_str requires exactly 2 arguments"
        );
        let key = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(key))) => key,
            _ => {
                return Err(exec_datafusion_err!(
                    "json_get_str first argument must be a string"
                ))
            }
        };
        // We expect a string array that contains JSON strings
        let json_array = match &args.args[1] {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                exec_datafusion_err!(
                    "json_get_str second argument must be a string array"
                )
            })?,
            _ => {
                return Err(exec_datafusion_err!(
                    "json_get_str second argument must be a string array"
                ))
            }
        };
        let values = json_array
            .iter()
            .map(|value| {
                value.and_then(|v| {
                    let json_value: serde_json::Value =
                        serde_json::from_str(v).unwrap_or_default();
                    json_value.get(key).map(|v| v.to_string())
                })
            })
            .collect::<StringArray>();
        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}

/// Factory for creating ShreddedJsonRewriter instances
#[derive(Debug)]
struct ShreddedJsonRewriterFactory;

impl PhysicalExprAdapterFactory for ShreddedJsonRewriterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory
            .create(logical_file_schema.clone(), physical_file_schema.clone());

        Arc::new(ShreddedJsonRewriter {
            logical_file_schema,
            physical_file_schema,
            default_adapter,
            partition_values: Vec::new(),
        })
    }
}

/// Rewriter that converts json_get_str calls to direct flat column references
/// and wraps DefaultPhysicalExprAdapter for standard schema adaptation
#[derive(Debug)]
struct ShreddedJsonRewriter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    default_adapter: Arc<dyn PhysicalExprAdapter>,
    partition_values: Vec<(FieldRef, ScalarValue)>,
}

impl PhysicalExprAdapter for ShreddedJsonRewriter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // First try our custom JSON shredding rewrite
        let rewritten = expr
            .transform(|expr| self.rewrite_impl(expr, &self.physical_file_schema))
            .data()?;

        // Then apply the default adapter as a fallback to handle standard schema differences
        // like type casting, missing columns, and partition column handling
        let default_adapter = if !self.partition_values.is_empty() {
            self.default_adapter
                .with_partition_values(self.partition_values.clone())
        } else {
            self.default_adapter.clone()
        };

        default_adapter.rewrite(rewritten)
    }

    fn with_partition_values(
        &self,
        partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(ShreddedJsonRewriter {
            logical_file_schema: self.logical_file_schema.clone(),
            physical_file_schema: self.physical_file_schema.clone(),
            default_adapter: self.default_adapter.clone(),
            partition_values,
        })
    }
}

impl ShreddedJsonRewriter {
    fn rewrite_impl(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        physical_file_schema: &Schema,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
            if func.name() == "json_get_str" && func.args().len() == 2 {
                // Get the key from the first argument
                if let Some(literal) = func.args()[0]
                    .as_any()
                    .downcast_ref::<expressions::Literal>()
                {
                    if let ScalarValue::Utf8(Some(field_name)) = literal.value() {
                        // Get the column from the second argument
                        if let Some(column) = func.args()[1]
                            .as_any()
                            .downcast_ref::<expressions::Column>()
                        {
                            let column_name = column.name();
                            // Check if there's a flat column with underscore prefix
                            let flat_column_name = format!("_{column_name}.{field_name}");

                            if let Ok(flat_field_index) =
                                physical_file_schema.index_of(&flat_column_name)
                            {
                                let flat_field =
                                    physical_file_schema.field(flat_field_index);

                                if flat_field.data_type() == &DataType::Utf8 {
                                    // Replace the whole expression with a direct column reference
                                    let new_expr = Arc::new(expressions::Column::new(
                                        &flat_column_name,
                                        flat_field_index,
                                    ))
                                        as Arc<dyn PhysicalExpr>;

                                    return Ok(Transformed {
                                        data: new_expr,
                                        tnr: TreeNodeRecursion::Stop,
                                        transformed: true,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Transformed::no(expr))
    }
}
