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

use datafusion::assert_batches_eq;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRewriter,
};
use datafusion::common::{assert_contains, DFSchema, Result};
use datafusion::datasource::file_expr_rewriter::FileExpressionRewriter;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
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
    let ctx = SessionContext::new();

    // Register our table
    ctx.register_table("structs", table_provider)?;

    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::new(store),
    );

    println!("\n=== Showing all data ===");
    let batches = ctx.sql("SELECT * FROM structs").await?.collect().await?;
    arrow::util::pretty::print_batches(&batches)?;

    println!("\n=== Running query with struct field access and filter < 30 ===");
    println!("Query: SELECT user_info['name'] FROM structs WHERE user_info['age'] < 30");

    let batches = ctx
        .sql("SELECT user_info['name'] FROM structs WHERE user_info['age'] < 30 ORDER BY user_info['name']")
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
    assert_batches_eq!(expected, &batches);

    println!("\n=== Running explain analyze to confirm row group pruning ===");

    let batches = ctx
        .sql("EXPLAIN ANALYZE SELECT user_info['name'] FROM structs WHERE user_info['age'] < 30")
        .await?
        .collect()
        .await?;
    let plan = format!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
    println!("{plan}");
    assert_contains!(&plan, "row_groups_pruned_statistics=2");

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
            Arc::new(ages), // Shredded age field
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
            .with_filter_expression_rewriter(Arc::new(StructFieldRewriter) as _);

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
            .map(|file| {
                PartitionedFile::new(
                    file.location.clone(),
                    u64::try_from(file.size).expect("fits in a u64"),
                )
            })
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

/// Rewriter that converts struct field access to flattened column references
#[derive(Debug)]
struct StructFieldRewriter;

impl FileExpressionRewriter for StructFieldRewriter {
    fn rewrite(
        &self,
        file_schema: SchemaRef,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let mut rewrite = StructFieldRewriterImpl { file_schema };
        expr.rewrite(&mut rewrite).data()
    }
}

struct StructFieldRewriterImpl {
    file_schema: SchemaRef,
}

impl TreeNodeRewriter for StructFieldRewriterImpl {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(scalar_function) = expr.as_any().downcast_ref::<ScalarFunctionExpr>()
        {
            if scalar_function.name() == "get_field" && scalar_function.args().len() == 2
            {
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
                            let source_field =
                                self.file_schema.field_with_name(column_name)?;
                            let expected_flattened_column_name =
                                format!("_{}.{}", column_name, field_name);
                            if let DataType::Struct(struct_fields) =
                                source_field.data_type()
                            {
                                // Check if the flattened column exists in the file schema and has the same type
                                if let Ok(shredded_field) = self
                                    .file_schema
                                    .field_with_name(&expected_flattened_column_name)
                                {
                                    if let Some((_, struct_field)) =
                                        struct_fields.find(field_name)
                                    {
                                        if struct_field.data_type()
                                            == shredded_field.data_type()
                                        {
                                            // Rewrite the expression to use the flattened column
                                            let rewritten_expr = expressions::col(
                                                &expected_flattened_column_name,
                                                &self.file_schema,
                                            )?;
                                            return Ok(Transformed::yes(rewritten_expr));
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
