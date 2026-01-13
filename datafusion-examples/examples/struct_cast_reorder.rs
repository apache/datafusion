use arrow::array::{Int64Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{cast, col};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    // Source: struct with fields [b=3, a=4]
    let source_fields = Fields::from(vec![
        Field::new("b", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
    ]);

    let source_struct = StructArray::new(
        source_fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3i64])), // b = 3
            Arc::new(Int64Array::from(vec![4i64])), // a = 4
        ],
        None,
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(source_fields),
            false,
        )])),
        vec![Arc::new(source_struct)],
    )?;

    let table = datafusion::datasource::memory::MemTable::try_new(
        batch.schema(),
        vec![vec![batch]],
    )?;

    ctx.register_table("t", Arc::new(table))?;

    // Validate source data: should be b=3, a=4
    let source_data = ctx.table("t").await?.collect().await?;
    use arrow::array::AsArray;
    let src_struct = source_data[0].column(0).as_struct();
    let src_a = src_struct
        .column_by_name("a")
        .unwrap()
        .as_primitive::<arrow::array::types::Int64Type>()
        .value(0);
    let src_b = src_struct
        .column_by_name("b")
        .unwrap()
        .as_primitive::<arrow::array::types::Int64Type>()
        .value(0);
    assert_eq!(src_a, 4, "Source field 'a' should be 4");
    assert_eq!(src_b, 3, "Source field 'b' should be 3");
    println!("✓ Source validation passed: b={src_b}, a={src_a}");

    // Target: reorder fields to [a, b]
    let target_type = DataType::Struct(Fields::from(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));

    // Execute cast
    let result = ctx
        .table("t")
        .await?
        .select(vec![cast(col("s"), target_type)])?
        .collect()
        .await?;

    // Validate result
    let res_struct = result[0].column(0).as_struct();
    let res_a = res_struct
        .column_by_name("a")
        .unwrap()
        .as_primitive::<arrow::array::types::Int64Type>()
        .value(0);
    let res_b = res_struct
        .column_by_name("b")
        .unwrap()
        .as_primitive::<arrow::array::types::Int64Type>()
        .value(0);

    if res_a == 4 && res_b == 3 {
        println!("✓ Cast result passed: a={res_a}, b={res_b}");
    } else {
        println!(
            "✗ Bug: Cast maps by position, not name. Expected a=4,b=3 but got a={res_a}, b={res_b}",
        );
    }

    Ok(())
}
