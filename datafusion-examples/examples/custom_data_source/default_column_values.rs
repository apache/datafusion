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
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use datafusion::assert_batches_eq;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{lit, SessionConfig};
use datafusion_physical_expr_adapter::{
    replace_columns_with_literals, DefaultPhysicalExprAdapterFactory,
    PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

// Metadata key for storing default values in field metadata
const DEFAULT_VALUE_METADATA_KEY: &str = "example.default_value";

/// Example showing how to implement custom default value handling for missing columns
/// using field metadata and PhysicalExprAdapter.
///
/// This example demonstrates how to:
/// 1. Store default values in field metadata using a constant key
/// 2. Create a custom PhysicalExprAdapter that reads these defaults
/// 3. Inject default values for missing columns in filter predicates using `replace_columns_with_literals`
/// 4. Use the DefaultPhysicalExprAdapter as a fallback for standard schema adaptation
/// 5. Convert string default values to proper types using `ScalarValue::cast_to()` at planning time
///
/// Important: PhysicalExprAdapter is specifically designed for rewriting filter predicates
/// that get pushed down to file scans. For handling missing columns in projections,
/// other mechanisms in DataFusion are used (like SchemaAdapter).
///
/// The metadata-based approach provides a flexible way to store default values as strings
/// and cast them to the appropriate types at planning time, avoiding runtime overhead.
pub async fn default_column_values() -> Result<()> {
    println!("=== Creating example data with missing columns and default values ===");

    // Create sample data where the logical schema has more columns than the physical schema
    let (logical_schema, physical_schema, batch) = create_sample_data_with_defaults();

    let store = InMemory::new();
    let buf = {
        let mut buf = vec![];

        let props = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();

        let mut writer =
            ArrowWriter::try_new(&mut buf, physical_schema.clone(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;
        buf
    };
    let path = Path::from("example.parquet");
    let payload = PutPayload::from_bytes(buf.into());
    store.put(&path, payload).await?;

    // Create a custom table provider that handles missing columns with defaults
    let table_provider = Arc::new(DefaultValueTableProvider::new(logical_schema));

    // Set up query execution
    let mut cfg = SessionConfig::new();
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);

    // Register our table
    ctx.register_table("example_table", table_provider)?;

    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::new(store),
    );

    println!("\n=== Demonstrating default value injection in filter predicates ===");
    let query = "SELECT id, name FROM example_table WHERE status = 'active' ORDER BY id";
    println!("Query: {query}");
    println!("Note: The 'status' column doesn't exist in the physical schema,");
    println!(
        "but our adapter injects the default value 'active' for the filter predicate."
    );

    let batches = ctx.sql(query).await?.collect().await?;

    #[rustfmt::skip]
    let expected = [
        "+----+-------+",
        "| id | name  |",
        "+----+-------+",
        "| 1  | Alice |",
        "| 2  | Bob   |",
        "| 3  | Carol |",
        "+----+-------+",
    ];
    arrow::util::pretty::print_batches(&batches)?;
    assert_batches_eq!(expected, &batches);

    println!("\n=== Key Insight ===");
    println!("This example demonstrates how PhysicalExprAdapter works:");
    println!("1. Physical schema only has 'id' and 'name' columns");
    println!("2. Logical schema has 'id', 'name', 'status', and 'priority' columns with defaults");
    println!("3. Our custom adapter uses replace_columns_with_literals to inject default values");
    println!("4. Default values from metadata are cast to proper types at planning time");
    println!("5. The DefaultPhysicalExprAdapter handles other schema adaptations");
    println!("\nNote: PhysicalExprAdapter is specifically for filter predicates.");
    println!("For projection columns, different mechanisms handle missing columns.");

    Ok(())
}

/// Create sample data with a logical schema that has default values in metadata
/// and a physical schema that's missing some columns
fn create_sample_data_with_defaults() -> (SchemaRef, SchemaRef, RecordBatch) {
    // Create metadata for default values
    let mut status_metadata = HashMap::new();
    status_metadata.insert(DEFAULT_VALUE_METADATA_KEY.to_string(), "active".to_string());

    let mut priority_metadata = HashMap::new();
    priority_metadata.insert(DEFAULT_VALUE_METADATA_KEY.to_string(), "1".to_string());

    // The logical schema includes all columns with their default values in metadata
    // Note: We make the columns with defaults nullable to allow the default adapter to handle them
    let logical_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, true).with_metadata(status_metadata),
        Field::new("priority", DataType::Int32, true).with_metadata(priority_metadata),
    ]);

    // The physical schema only has some columns (simulating missing columns in storage)
    let physical_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    // Create sample data for the physical schema
    let batch = RecordBatch::try_new(
        Arc::new(physical_schema.clone()),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec![
                "Alice", "Bob", "Carol",
            ])),
        ],
    )
    .unwrap();

    (Arc::new(logical_schema), Arc::new(physical_schema), batch)
}

/// Custom TableProvider that uses DefaultValuePhysicalExprAdapter
#[derive(Debug)]
struct DefaultValueTableProvider {
    schema: SchemaRef,
}

impl DefaultValueTableProvider {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl TableProvider for DefaultValueTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::clone(&self.schema);
        let df_schema = DFSchema::try_from(schema.clone())?;
        let filter = state.create_physical_expr(
            conjunction(filters.iter().cloned()).unwrap_or_else(|| lit(true)),
            &df_schema,
        )?;

        let parquet_source = ParquetSource::new(schema.clone())
            .with_predicate(filter)
            .with_pushdown_filters(true);

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
            Arc::new(parquet_source),
        )
        .with_projection_indices(projection.cloned())?
        .with_limit(limit)
        .with_file_group(file_group)
        .with_expr_adapter(Some(Arc::new(DefaultValuePhysicalExprAdapterFactory) as _));

        Ok(Arc::new(DataSourceExec::new(Arc::new(
            file_scan_config.build(),
        ))))
    }
}

/// Factory for creating DefaultValuePhysicalExprAdapter instances
#[derive(Debug)]
struct DefaultValuePhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DefaultValuePhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&physical_file_schema),
        );

        Arc::new(DefaultValuePhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            default_adapter,
        })
    }
}

/// Custom PhysicalExprAdapter that handles missing columns with default values from metadata
/// and wraps DefaultPhysicalExprAdapter for standard schema adaptation
#[derive(Debug)]
struct DefaultValuePhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    default_adapter: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for DefaultValuePhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // Pre-compute replacements for missing columns with default values
        let mut replacements = HashMap::new();
        for field in self.logical_file_schema.fields() {
            // Skip columns that exist in physical schema
            if self.physical_file_schema.index_of(field.name()).is_ok() {
                continue;
            }

            // Check if this missing column has a default value in metadata
            if let Some(default_str) = field.metadata().get(DEFAULT_VALUE_METADATA_KEY) {
                // Create a Utf8 ScalarValue from the string and cast it to the target type
                let string_value = ScalarValue::Utf8(Some(default_str.to_string()));
                let typed_value = string_value.cast_to(field.data_type())?;
                replacements.insert(field.name().as_str(), typed_value);
            }
        }

        // Replace columns with their default literals if any
        let rewritten = if !replacements.is_empty() {
            let refs: HashMap<_, _> = replacements.iter().map(|(k, v)| (*k, v)).collect();
            replace_columns_with_literals(expr, &refs)?
        } else {
            expr
        };

        // Apply the default adapter as a fallback for other schema adaptations
        self.default_adapter.rewrite(rewritten)
    }
}
