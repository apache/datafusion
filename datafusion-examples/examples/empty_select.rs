use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::{self, Unparser};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("create table t (k int, v int)")
        .await?
        .collect()
        .await?;

    let df = ctx.sql("select from t").await?;

    let plan = df.into_optimized_plan()?;
    println!("{}", plan.display_indent());
    let sql =
        Unparser::new(&unparser::dialect::PostgreSqlDialect {}).plan_to_sql(&plan)?;
    println!("{sql}");

    Ok(())
}
