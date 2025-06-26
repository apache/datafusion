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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    as_string_array, ArrayRef, Int32Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_schema::Fields;
use async_trait::async_trait;

use datafusion::assert_batches_eq;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter
};
use datafusion::common::{assert_contains, DFSchema, Result};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::source;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableProviderFilterPushDown, TableType, Volatility
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::schema_rewriter::PhysicalExprSchemaRewriteHook;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{expressions, ScalarFunctionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{lit, SessionConfig};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

// Example showing how to implement custom filter rewriting for struct fields.
//
// In this example, we have a table with a struct column like:
// struct_col: {"a": 1, "b": "foo"}
//
// Our custom TableProvider will use a FilterExpressionRewriter to rewrite
// expressions like `struct_col['a'] = 10` to use a flattened column name
// `_struct_col.a` if it exists in the file schema.
#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Creating example data with structs and flattened fields ===");

    // Create sample data with both struct columns and flattened fields
    let (table_schema, batch) = create_sample_data();

    let store = InMemory::new();
    let buf = {
        let mut buf = vec![];

        let props = WriterProperties::builder()
            .set_max_row_group_size(1)
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

    // Create a custom table provider that rewrites struct field access
    let table_provider = Arc::new(ExampleTableProvider::new(table_schema));

    // Set up query execution
    let mut cfg = SessionConfig::new();
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);

    // Register our table
    ctx.register_table("structs", table_provider)?;
    ctx.register_udf(ScalarUDF::new_from_impl(JsonGetStr::default()));

    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::new(store),
    );

    // println!("\n=== Showing all data ===");
    // let batches = ctx.sql("SELECT * FROM structs").await?.collect().await?;
    // arrow::util::pretty::print_batches(&batches)?;

    println!("\n=== Running query with struct field access and filter < 30 ===");
    let query = "SELECT count(*) FROM structs WHERE json_get_str('name', user_info) = 'Bob'";
    println!("Query: {query}");

    let batches = ctx
        .sql(query)
        .await?
        .collect()
        .await?;

    #[rustfmt::skip]
    let expected = [
        "+-------------------------+",
        "| structs.user_info[name] |",
        "+-------------------------+",
        "| Bob                     |",
        "| Dave                    |",
        "+-------------------------+",
    ];
    arrow::util::pretty::print_batches(&batches)?;
    println!("batches: {batches:?}");
    assert_batches_eq!(expected, &batches);

    // println!("\n=== Running explain analyze to confirm row group pruning ===");

    // let batches = ctx
    //     .sql("EXPLAIN ANALYZE SELECT user_info['name'] FROM structs WHERE user_info['age'] < 30")
    //     .await?
    //     .collect()
    //     .await?;
    // let plan = format!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    // println!("{plan}");
    // assert_contains!(&plan, "row_groups_pruned_statistics=2");

    Ok(())
}

/// Create the example data that has a struct column with `name` as a shredded field and `age` as a non-shredded field.
fn create_sample_data() -> (SchemaRef, RecordBatch) {
    // The table schema doesn't have any shredded fields
    let struct_fields = Fields::from(vec![
        Field::new("data", DataType::Utf8, false),
    ]);
    let struct_field = Field::new("user_info", DataType::Struct(struct_fields), false);
    let table_schema = Schema::new(vec![struct_field.clone()]);
    // The file schema has `name` as a shredded field
    let struct_fields = Fields::from(vec![
        Field::new("data", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let struct_field = Field::new("user_info", DataType::Struct(struct_fields), false);
    let file_schema = Schema::new(vec![struct_field.clone()]);
    // Build a RecordBatch with shredded data
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"]);
    let user_info = StructArray::from(vec![
        (
            Arc::new(Field::new("data", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec![
                r#"{"age": 30}"#,
                r#"{"age": 25}"#,
                r#"{"age": 35}"#,
                r#"{"age": 22}"#,
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(names.clone()) as ArrayRef,
        ),
    ]);

    (
        Arc::new(table_schema),
        RecordBatch::try_new(
            Arc::new(file_schema),
            vec![Arc::new(user_info)],
        )
        .unwrap(),
    )
}

/// Custom TableProvider that uses a StructFieldRewriter
#[derive(Debug)]
struct ExampleTableProvider {
    schema: SchemaRef,
}

impl ExampleTableProvider {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl TableProvider for ExampleTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Implementers can choose to mark these filters as exact or inexact.
        // If marked as exact they cannot have false positives and must always be applied.
        // If marked as Inexact they can have false positives and at runtime the rewriter
        // can decide to not rewrite / ignore some filters since they will be re-evaluated upstream.
        // For the purposes of this example we mark them as Exact to demonstrate the rewriter is working and the filtering is not being re-evaluated upstream.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema.clone();
        let df_schema = DFSchema::try_from(schema.clone())?;
        let filter = state.create_physical_expr(
            conjunction(filters.iter().cloned()).unwrap_or_else(|| lit(true)),
            &df_schema,
        )?;

        let parquet_source = ParquetSource::default()
            .with_predicate(filter)
            .with_pushdown_filters(true)
            // if the rewriter needs a reference to the table schema you can bind self.schema() here
            .with_predicate_rewrite_hook(Arc::new(ShreddedVariantRewriter) as _);

        let object_store_url = ObjectStoreUrl::parse("memory://")?;

        let store = state.runtime_env().object_store(object_store_url)?;

        let mut files = vec![];
        let mut listing = store.list(None);
        while let Some(file) = listing.next().await {
            if let Ok(file) = file {
                files.push(file);
            }
        }

        let file_group = files
            .iter()
            .map(|file| PartitionedFile::new(file.location.clone(), file.size))
            .collect();

        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://")?,
            schema,
            Arc::new(parquet_source),
        )
        .with_projection(projection.cloned())
        .with_limit(limit)
        .with_file_group(file_group);

        Ok(Arc::new(DataSourceExec::new(Arc::new(
            file_scan_config.build(),
        ))))
    }
}

/// Scalar UDF that uses serde_json to access json fields
#[derive(Debug)]
pub struct JsonGetStr {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetStr {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_str".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aliases[0].as_str()
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
                return Err(datafusion::error::DataFusionError::Execution(
                    "json_get_str first argument must be a string".to_string(),
                ))
            }
        };
        // We expect a struct array with a field called `data` that contains JSON strings
        let struct_array = match &args.args[1] {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "json_get_str second argument must be a struct array".to_string(),
                )
            })?,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "json_get_str second argument must be a struct array".to_string(),
                ))
            }
        };
        // Extract the "data" field from the struct array
        let data_array = struct_array.column_by_name("data").ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "json_get_str second argument must have a 'data' field".to_string(),
            )
        })?;
        let json_array = match data_array.as_any().downcast_ref::<StringArray>() {
            Some(array) => array,
            None => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "json_get_str second argument 'data' field must be a StringArray"
                        .to_string(),
                ))
            }
        };
        let values = json_array
            .iter()
            .map(|value| {
                value
                    .map(|v| {
                        let json_value: serde_json::Value =
                            serde_json::from_str(&v).unwrap_or_default();
                        json_value
                            .get(&key)
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .flatten()
            })
            .collect::<StringArray>();
        Ok(ColumnarValue::Array(Arc::new(values)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Rewriter that converts struct field access to flattened column references
#[derive(Debug)]
struct ShreddedVariantRewriter;

impl PhysicalExprSchemaRewriteHook for ShreddedVariantRewriter {
    fn rewrite(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        physical_file_schema: &Schema,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
            if 
            func.name() == "json_get_str" && func.args().len() == 2 {
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
                            // Get the physical file schema's field
                            if let Ok(source_field_index) =
                                physical_file_schema.index_of(column_name)
                            {
                                let source_field =
                                    physical_file_schema.field(source_field_index);
                                // If it's a struct field check if there is a shredded field with the name `field_name`
                                if let DataType::Struct(struct_fields) =
                                    source_field.data_type()
                                {
                                    if let Some((_, shredded_field)) =
                                        struct_fields.find(field_name)
                                    {
                                        if shredded_field.data_type() == &DataType::Utf8 {
                                            // Replace the whole expression with a struct field access on the shredded field
                                            let args = vec![
                                                Arc::new(expressions::Column::new(
                                                    source_field.name(),
                                                    source_field_index,
                                                ))
                                                    as Arc<dyn PhysicalExpr>,
                                                Arc::new(expressions::Literal::new(
                                                    ScalarValue::Utf8(Some(
                                                        field_name.clone(),
                                                    )),
                                                )),
                                            ];
                                            let return_field = Arc::new(
                                                Field::new(
                                                    format!(
                                                        "{}.{}",
                                                        source_field.name(),
                                                        field_name
                                                    ),
                                                    DataType::Utf8,
                                                    true,
                                                ),
                                            );
                                            let new_expr = Arc::new(
                                                ScalarFunctionExpr::new(
                                                    "get_field",
                                                    Arc::new(GetFieldFunc::new().into()),
                                                    args,
                                                    return_field,
                                                ),
                                            );
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
            }
        }
        Ok(Transformed::no(expr))
    }
}
