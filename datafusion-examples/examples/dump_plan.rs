// THROWAWAY: inspect the actual LogicalPlan / Expr struct shapes for the
// ROW_NUMBER() = 1 pattern. Delete this file when done.
//
// Run with:
//   cargo run -p datafusion-examples --example dump_plan
//
// `Display` (what EXPLAIN shows) vs `Debug` ({:#?}, the real enum structs) are
// printed side by side for both the unoptimized and optimized logical plans.

use datafusion::error::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.sql(
        "CREATE TABLE t AS \
         SELECT column1 AS p, column2 AS o, column3 AS payload \
         FROM (VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 5, 'c'), (2, 30, 'd'))",
    )
    .await?
    .collect()
    .await?;

    let sql = "SELECT * FROM ( \
                 SELECT *, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC) AS rn \
                 FROM t \
               ) WHERE rn = 1";

    let state = ctx.state();

    // Unoptimized: this is the tree your rule's `rewrite()` will be handed.
    let logical = state.create_logical_plan(sql).await?;
    println!("================ UNOPTIMIZED LOGICAL PLAN (Display) ================");
    println!("{logical}\n");
    println!("================ UNOPTIMIZED LOGICAL PLAN (Debug) ================");
    println!("{logical:#?}\n");

    // Optimized: what the current rule set produces (no top-1 rewrite yet).
    let optimized = state.optimize(&logical)?;
    println!("================ OPTIMIZED LOGICAL PLAN (Display) ================");
    println!("{optimized}\n");
    println!("================ OPTIMIZED LOGICAL PLAN (Debug) ================");
    println!("{optimized:#?}\n");

    Ok(())
}
