use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{col, BuiltInWindowFunction, Expr, WindowFunctionDefinition};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![5, 4, 3, 2, 1]))],
    )?;

    let ctx = SessionContext::new();

    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let df = ctx.table("t").await?;

    let func = Expr::WindowFunction(WindowFunction::new(
        WindowFunctionDefinition::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
        vec![],
    ))
    .alias("row_num");

    df.clone()
        .select(vec![col("a"), func.clone()])?
        .show()
        .await?;

    df.with_column("r", func)?.show().await?;

    Ok(())
}
