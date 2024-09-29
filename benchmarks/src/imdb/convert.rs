// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion_common::instant::Instant;
use std::path::PathBuf;

use datafusion::error::Result;
use datafusion::prelude::*;
use structopt::StructOpt;

use datafusion::common::not_impl_err;

use super::get_imdb_table_schema;
use super::IMDB_TABLES;

#[derive(Debug, StructOpt)]
pub struct ConvertOpt {
    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Output file format: `csv` or `parquet`
    #[structopt(short = "f", long = "format")]
    file_format: String,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,
}

impl ConvertOpt {
    pub async fn run(self) -> Result<()> {
        let input_path = self.input_path.to_str().unwrap();
        let output_path = self.output_path.to_str().unwrap();
        let config = SessionConfig::new().with_batch_size(self.batch_size);
        let ctx = SessionContext::new_with_config(config);

        for table in IMDB_TABLES {
            let start = Instant::now();
            let schema = get_imdb_table_schema(table);
            let input_path = format!("{input_path}/{table}.csv");
            let output_path = format!("{output_path}/{table}.parquet");
            let options = CsvReadOptions::new()
                .schema(&schema)
                .has_header(false)
                .delimiter(b',')
                .escape(b'\\')
                .file_extension(".csv");

            let mut csv = ctx.read_csv(&input_path, options).await?;

            // Select all apart from the padding column
            let selection = csv
                .schema()
                .iter()
                .take(schema.fields.len())
                .map(Expr::from)
                .collect();

            csv = csv.select(selection)?;

            println!(
                "Converting '{}' to {} files in directory '{}'",
                &input_path, self.file_format, &output_path
            );
            match self.file_format.as_str() {
                "csv" => {
                    csv.write_csv(
                        output_path.as_str(),
                        DataFrameWriteOptions::new(),
                        None,
                    )
                    .await?;
                }
                "parquet" => {
                    csv.write_parquet(
                        output_path.as_str(),
                        DataFrameWriteOptions::new(),
                        None,
                    )
                    .await?;
                }
                other => {
                    return not_impl_err!("Invalid output format: {other}");
                }
            }
            println!("Conversion completed in {} ms", start.elapsed().as_millis());
        }
        Ok(())
    }
}
