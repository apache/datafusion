use datafusion::{dataframe::DataFrameWriteOptions, prelude::*};
use datafusion_common::DataFusionError;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use url::Url;

/// This example demonstrates the various methods to write out a DataFrame
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();
    let local = Arc::new(LocalFileSystem::new_with_prefix("./").unwrap());
    let local_url = Url::parse("file://local").unwrap();
    ctx.runtime_env().register_object_store(&local_url, local);

    let mut df = ctx.sql(
        "values ('a'), ('b'), ('c')"
    ).await
    .unwrap();

    // Ensure the column names and types match the target table
    df = df.with_column_renamed("column1", "tablecol1").unwrap();

    ctx.sql(
        "create external table 
    test(tablecol1 varchar)
    stored as parquet 
    location './datafusion-examples/test_table/'",
    )
    .await?
    .collect()
    .await?;

    df.clone()
        .write_table("test", DataFrameWriteOptions::new())
        .await?;

    df.clone()
        .write_parquet("file://local/datafusion-examples/test_parquet/", 
        DataFrameWriteOptions::new(),
        None,
    )
        .await?;

    df.clone()
        .write_csv("file://local/datafusion-examples/test_csv/", 
        DataFrameWriteOptions::new(),
        None,
    )
        .await?;

    df.clone()
        .write_json("file://local/datafusion-examples/test_json/", 
        DataFrameWriteOptions::new(),
    )
        .await?;

    Ok(())
}
