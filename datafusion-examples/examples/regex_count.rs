use datafusion::common::Result;
use datafusion::prelude::{CsvReadOptions, SessionContext};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "examples",
        "../../datafusion/physical-expr/tests/data/regex.csv",
        CsvReadOptions::new(),
    )
    .await?;

    //
    //
    //regexp_count examples
    //
    //
    // regexp_count format is (regexp_count(text, regex[, flags])
    //

    // use sql and regexp_count function to test col 'values', against patterns in col 'patterns' without flags
    let result = ctx
        .sql("select regexp_count(values, patterns) from examples")
        .await?
        .collect()
        .await?;

    println!("{:?}", result);
    assert_eq!(result.len(), 1);

    Ok(())
}
