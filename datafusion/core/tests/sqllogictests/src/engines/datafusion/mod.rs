use std::time::Duration;

use sqllogictest::DBOutput;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use self::error::{DFSqlLogicTestError, Result};
use sqlparser::ast::Statement as SQLStatement;
use datafusion_sql::parser::{DFParser, Statement};
use insert::insert;
use async_trait::async_trait;

mod error;
mod normalize;
mod insert;

pub struct DataFusion {
    ctx: SessionContext,
    file_name: String,
}

impl DataFusion {
    pub fn new(ctx: SessionContext, file_name: String) -> Self {
        Self { ctx, file_name }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        println!("[{}] Running query: \"{}\"", self.file_name, sql);
        let result = run_query(&self.ctx, sql).await?;
        Ok(result)
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universial to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}


async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<DBOutput> {
    let sql = sql.into();
    // Check if the sql is `insert`
    if let Ok(mut statements) = DFParser::parse_sql(&sql) {
        let statement0 = statements.pop_front().expect("at least one SQL statement");
        if let Statement::Statement(statement) = statement0 {
            let statement = *statement;
            if matches!(&statement, SQLStatement::Insert { .. }) {
                return insert(ctx, statement).await;
            }
        }
    }
    let df = ctx.sql(sql.as_str()).await?;
    let results: Vec<RecordBatch> = df.collect().await?;
    let formatted_batches = normalize::convert_batches(results)?;
    Ok(formatted_batches)
}
