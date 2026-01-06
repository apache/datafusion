use arrow::array::{FixedSizeBinaryArray, RecordBatch};
use arrow_schema::extension::Uuid;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit_with_metadata};
use insta::assert_snapshot;
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![uuid_field()]))
}

fn uuid_field() -> Field {
    Field::new("my_uuids", DataType::FixedSizeBinary(16), false).with_extension_type(Uuid)
}

async fn create_test_table() -> Result<DataFrame> {
    let schema = test_schema();

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(FixedSizeBinaryArray::from(vec![
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 5, 6],
        ]))],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("test", batch)?;

    ctx.table("test").await
}

#[tokio::test]
async fn test_pretty_print_logical_plan() -> Result<()> {
    let result = create_test_table().await?.to_string().await?;

    assert_snapshot!(
        result,
        @r"
    +--------------------------------------------------+
    | my_uuids                                         |
    +--------------------------------------------------+
    | arrow.uuid(00000000-0000-0000-0000-000000000000) |
    | arrow.uuid(00010203-0405-0607-0809-000102030506) |
    +--------------------------------------------------+
    "
    );

    Ok(())
}

#[tokio::test]
async fn test_pretty_print_extension_types() -> Result<()> {
    let scalar = ScalarValue::FixedSizeBinary(
        16,
        Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 5, 6]),
    );
    let literal =
        lit_with_metadata(scalar, Some(FieldMetadata::new_from_field(&uuid_field())));

    let df = create_test_table()
        .await?
        .filter(col("my_uuids").eq(literal))?;

    assert_snapshot!(
        df.logical_plan(),
        @r"
    Filter: test.my_uuids = arrow.uuid(00010203-0405-0607-0809-000102030506)
      TableScan: test
    "
    );

    Ok(())
}
