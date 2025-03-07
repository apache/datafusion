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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_schema::Fields;
use async_trait::async_trait;

use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, Result};
use datafusion::datasource::filter_expr_rewriter::{
    FilterExpressionRewriter, FilterExpressionRewriterFactory,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{expressions, ScalarFunctionExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::lit;
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
    println!("NOTE: This example demonstrates filter expression rewriting for struct field access.");
    println!("      We deliberately create different values in the struct field vs. flattened column");
    println!("      to prove that the rewriting system is actually using the flattened column.");

    // Create sample data with both struct columns and flattened fields
    let (table_schema, batch) = create_sample_data();

    let store = InMemory::new();
    let buf = {
        let mut buf = vec![];

        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)
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
    let ctx = SessionContext::new();

    // Register our table
    ctx.register_table("structs", table_provider)?;

    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::new(store),
    );

    // println!("\n=== Showing all data ===");
    // let batches = ctx.sql("SELECT * FROM structs").await?.collect().await?;
    // arrow::util::pretty::print_batches(&batches)?;

    println!("\n=== Running query with struct field access and filter < 30 ===");
    println!("Query: SELECT user_info['name'] FROM structs WHERE user_info['age'] < 30");

    let batches = ctx
        .sql("SELECT * FROM structs WHERE user_info['age'] > 30")
        .await?
        .collect()
        .await?;
    arrow::util::pretty::print_batches(&batches)?;

    // println!("\n=== Running query with struct field access and filter > 130 ===");
    // println!("Query: SELECT user_info['name'] FROM structs WHERE user_info['age'] > 100 and user_info['age'] < 130");

    // let batches = ctx
    //     .sql("SELECT * FROM structs WHERE user_info['age'] > 100 and user_info['age'] < 130")
    //     .await?
    //     .collect()
    //     .await?;
    // arrow::util::pretty::print_batches(&batches)?;

    Ok(())
}

/// Create the example data with both struct fields and flattened fields
fn create_sample_data() -> (SchemaRef, RecordBatch) {
    // Create a schema with a struct column
    let user_info_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]);

    let file_schema = Schema::new(vec![
        Field::new(
            "user_info",
            DataType::Struct(user_info_fields.clone()),
            false,
        ),
        // Include flattened fields (in real scenarios these might be in some files but not others)
        Field::new("_user_info.name", DataType::Utf8, true),
        Field::new("_user_info.age", DataType::Int32, true),
    ]);

    let table_schema = Schema::new(vec![Field::new(
        "user_info",
        DataType::Struct(user_info_fields.clone()),
        false,
    )]);

    // Create struct array for user_info
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"]);
    let ages = Int32Array::from(vec![30, 25, 35, 22]);
    // Purposefully craete different ages for the shredded version to prove filter rewriting works
    // Otherwise how would we know if the filter is actually using the shredded column or the struct field?
    // In a real-world scenario we'd expect that the shredded column would be the same as the struct field, this is just for demonstration!
    let shredded_ages = Int32Array::from(vec![130, 125, 135, 122]);
    let user_info = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(names.clone()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("age", DataType::Int32, false)),
            Arc::new(ages.clone()) as ArrayRef,
        ),
    ]);

    // Create a record batch with the data
    let batch = RecordBatch::try_new(
        Arc::new(file_schema.clone()),
        vec![
            Arc::new(user_info),
            Arc::new(names),         // Shredded name field
            Arc::new(shredded_ages), // Shredded age field
        ],
    )
    .unwrap();

    (Arc::new(table_schema), batch)
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
        // For simplicity, we'll mark all filters as Inexact to ensure the filter is
        // double-checked after scanning.
        // A real implementation can choose to mark filters as Exact if it's sure it can handle them properly with no false positives
        // and doesn't need the ability to fall back to upstream filter handlnig at runtime.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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
            .with_predicate(self.schema.clone(), filter)
            .with_pushdown_filters(true)
            .with_filter_expression_rewriter_factory(
                Arc::new(StructFieldRewriterFactory) as _,
            );

        let object_store_url = ObjectStoreUrl::parse("memory://")?;

        let store = state.runtime_env().object_store(object_store_url)?;

        let mut files = vec![];
        let mut listing = store.list(None);
        while let Some(file) = listing.next().await {
            if let Ok(file) = file {
                files.push(file);
            }
        }

        let file_groups = vec![files
            .iter()
            .map(|file| {
                PartitionedFile::new(
                    file.location.clone(),
                    u64::try_from(file.size.clone()).expect("fits in a u64"),
                )
            })
            .collect()];

        let file_scan_config = FileScanConfig::new(
            ObjectStoreUrl::parse("memory://")?,
            schema,
            Arc::new(parquet_source),
        )
        .with_projection(projection.cloned())
        .with_limit(limit)
        .with_file_groups(file_groups);

        Ok(file_scan_config.build())
    }
}

/// Factory for creating StructFieldRewriter instances
#[derive(Debug, Default)]
struct StructFieldRewriterFactory;

impl FilterExpressionRewriterFactory for StructFieldRewriterFactory {
    fn create(
        &self,
        _table_schema: SchemaRef,
        file_schema: SchemaRef,
    ) -> Box<dyn FilterExpressionRewriter> {
        Box::new(StructFieldRewriter { file_schema })
    }
}

/// Rewriter that converts struct field access to flattened column references
#[derive(Debug)]
struct StructFieldRewriter {
    file_schema: SchemaRef,
}

impl FilterExpressionRewriter for StructFieldRewriter {
    fn rewrite_physical_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // In a real implementation, we would have a visitor that traverses the expression
        // tree and properly rewrites struct field access expressions

        if let Some(binary_expr) = expr.as_any().downcast_ref::<expressions::BinaryExpr>()
        {
            let left = self.rewrite_physical_expr(binary_expr.left().clone())?;
            let right = self.rewrite_physical_expr(binary_expr.right().clone())?;
            return Ok(Arc::new(expressions::BinaryExpr::new(
                left,
                *binary_expr.op(),
                right,
            )));
        } else if let Some(scalar_function) =
            expr.as_any().downcast_ref::<ScalarFunctionExpr>()
        {
            if scalar_function.name() == "get_field" {
                if scalar_function.args().len() == 2 {
                    // First argument is the column, second argument is the field name
                    let column = scalar_function.args()[0].clone();
                    let field_name = scalar_function.args()[1].clone();
                    if let Some(literal) =
                        field_name.as_any().downcast_ref::<expressions::Literal>()
                    {
                        if let Some(field_name) = literal.value().try_as_str().flatten() {
                            if let Some(column) =
                                column.as_any().downcast_ref::<expressions::Column>()
                            {
                                let column_name = column.name();
                                let expected_flattened_column_name =
                                    format!("_{}.{}", column_name, field_name);
                                // Check if the flattened column exists in the file schema
                                if let Ok(_shredded_field) = self
                                    .file_schema
                                    .field_with_name(&expected_flattened_column_name)
                                {
                                    // TODO: more checks, does the field type match the expected type?
                                    // Rewrite the expression to use the flattened column
                                    let rewritten_expr = expressions::col(
                                        &expected_flattened_column_name,
                                        &self.file_schema,
                                    )?;
                                    return Ok(rewritten_expr);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(expr)
    }
}
