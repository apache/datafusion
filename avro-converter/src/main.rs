#[macro_use]
extern crate structopt;

use datafusion::arrow::util::pretty;

use crate::structopt::StructOpt;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::path::PathBuf;

fn parse_csv(src: &str) -> Vec<String> {
    src.split(',').map(|s| s.to_string()).collect()
}

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct ArrowConverterOptions {
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,
    #[structopt(short, long)]
    partitions: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let mut ctx = ExecutionContext::new();

    let opts: ArrowConverterOptions = ArrowConverterOptions::from_args();

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(opts.input.to_str().unwrap())?
        .select_columns(&["id", "bool_col", "timestamp_col"])?;

    // execute the query
    let results = df.collect().await?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}
