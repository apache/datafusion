use datafusion::prelude::*;
use datafusion_common::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    
    // Test that should cause overflow with fail_on_overflow=true (default)
    println!("Testing multiplication overflow...");
    match ctx.sql("SELECT 10000000000::bigint * 10000000000::bigint").await {
        Ok(df) => {
            match df.collect().await {
                Ok(results) => {
                    println!("Overflow not caught - result: {:?}", results);
                },
                Err(e) => {
                    println!("Overflow caught correctly: {}", e);
                }
            }
        },
        Err(e) => {
            println!("Query error: {}", e);
        }
    }
    
    Ok(())
}