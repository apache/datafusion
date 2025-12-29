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

use arrow::array::ListBuilder;
use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// This example demonstrates how to execute queries using array functions
/// (`array_has`, `array_has_all`, `array_has_any`) and displays their
/// logical and physical execution plans.
///
/// Array functions can be used to filter data based on array columns,
/// and when pushed down to the Parquet decoder, they can significantly
/// improve query performance by filtering rows during decoding.
///
/// ## Key Feature: Predicate Pushdown
///
/// In the physical plans below, look for the `predicate=` parameter in the
/// `DataSourceExec` node. This shows that the array function predicates are
/// being pushed down to the Parquet decoder, which means:
///
/// 1. Rows are filtered during Parquet decoding before creating Arrow arrays
/// 2. Fewer rows need to be materialized in memory
/// 3. Query performance is significantly improved for selective predicates
///
/// Without pushdown, you would see these predicates only in the `FilterExec`
/// node above the data source, meaning all rows would be decoded first and
/// then filtered in memory.
pub async fn array_functions_physical_plans() -> Result<()> {
    // Create a session context
    let ctx = SessionContext::new();

    // Create a temporary Parquet file with sample data
    let parquet_path = create_sample_parquet_file()?;

    // Register the Parquet file as a table
    ctx.register_parquet(
        "languages",
        parquet_path.path().to_str().unwrap(),
        Default::default(),
    )
    .await?;

    // Example 1: array_has - Check if a single element is in the array
    println!("=== Example 1: array_has ===");
    println!("Query: SELECT * FROM languages WHERE array_has(tags, 'rust')\n");
    let df = ctx
        .sql("SELECT * FROM languages WHERE array_has(tags, 'rust')")
        .await?;

    print_logical_and_physical_plans(&ctx, &df).await?;

    // Example 2: array_has_all - Check if all elements are in the array
    println!("\n=== Example 2: array_has_all ===");
    println!(
        "Query: SELECT * FROM languages WHERE array_has_all(tags, ['rust', 'performance'])\n"
    );
    let df = ctx
        .sql("SELECT * FROM languages WHERE array_has_all(tags, ['rust', 'performance'])")
        .await?;

    print_logical_and_physical_plans(&ctx, &df).await?;

    // Example 3: array_has_any - Check if any element is in the array
    println!("\n=== Example 3: array_has_any ===");
    println!(
        "Query: SELECT * FROM languages WHERE array_has_any(tags, ['python', 'go'])\n"
    );
    let df = ctx
        .sql("SELECT * FROM languages WHERE array_has_any(tags, ['python', 'go'])")
        .await?;

    print_logical_and_physical_plans(&ctx, &df).await?;

    // Example 4: Complex predicate with multiple array functions
    println!("\n=== Example 4: Complex Predicate (OR) ===");
    println!(
        "Query: SELECT * FROM languages WHERE array_has_all(tags, ['rust']) OR array_has_any(tags, ['python', 'go'])\n"
    );
    let df = ctx
        .sql("SELECT * FROM languages WHERE array_has_all(tags, ['rust']) OR array_has_any(tags, ['python', 'go'])")
        .await?;

    print_logical_and_physical_plans(&ctx, &df).await?;

    // Example 5: Array function combined with other conditions
    println!("\n=== Example 5: Array Function with Other Predicates ===");
    println!("Query: SELECT * FROM languages WHERE id > 1 AND array_has(tags, 'rust')\n");
    let df = ctx
        .sql("SELECT * FROM languages WHERE id > 1 AND array_has(tags, 'rust')")
        .await?;

    print_logical_and_physical_plans(&ctx, &df).await?;

    Ok(())
}

/// Helper function to create a temporary Parquet file with sample data
fn create_sample_parquet_file() -> Result<NamedTempFile> {
    use std::fs::File;

    // Create a sample table with an array column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    // Create sample data
    let mut id_builder = arrow::array::Int32Builder::new();
    let mut tags_builder = ListBuilder::new(StringBuilder::new());

    // Row 0: id=1, tags=["rust", "performance"]
    id_builder.append_value(1);
    tags_builder.values().append_value("rust");
    tags_builder.values().append_value("performance");
    tags_builder.append(true);

    // Row 1: id=2, tags=["python", "javascript"]
    id_builder.append_value(2);
    tags_builder.values().append_value("python");
    tags_builder.values().append_value("javascript");
    tags_builder.append(true);

    // Row 2: id=3, tags=["rust", "webassembly"]
    id_builder.append_value(3);
    tags_builder.values().append_value("rust");
    tags_builder.values().append_value("webassembly");
    tags_builder.append(true);

    // Row 3: id=4, tags=["go", "system"]
    id_builder.append_value(4);
    tags_builder.values().append_value("go");
    tags_builder.values().append_value("system");
    tags_builder.append(true);

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(tags_builder.finish()),
        ],
    )?;

    // Create a temporary file
    let temp_file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    // Write the batch to a Parquet file
    let file = File::create(temp_file.path())
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(temp_file)
}

/// Helper function to print both logical and physical plans
async fn print_logical_and_physical_plans(
    ctx: &SessionContext,
    df: &DataFrame,
) -> Result<()> {
    // Get the logical plan
    let logical_plan = df.logical_plan();
    println!("Logical plan:\n{logical_plan:#?}\n");

    // Create the physical plan
    let physical_plan = ctx.state().create_physical_plan(logical_plan).await?;
    println!(
        "Physical plan:\n{}\n",
        displayable(physical_plan.as_ref()).indent(true)
    );

    Ok(())
}
