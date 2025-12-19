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

use std::fmt::{Display, Formatter};
use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use crate::object_storage::instrumented::{
    InstrumentedObjectStoreMode, InstrumentedObjectStoreRegistry, RequestSummaries,
};
use crate::print_format::PrintFormat;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::common::instant::Instant;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;

use datafusion::config::FormatOptions;
use futures::StreamExt;

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum MaxRows {
    /// show all rows in the output
    Unlimited,
    /// Only show n rows
    Limited(usize),
}

impl FromStr for MaxRows {
    type Err = String;

    fn from_str(maxrows: &str) -> Result<Self, Self::Err> {
        if maxrows.to_lowercase() == "inf"
            || maxrows.to_lowercase() == "infinite"
            || maxrows.to_lowercase() == "none"
        {
            Ok(Self::Unlimited)
        } else {
            match maxrows.parse::<usize>() {
                Ok(nrows) => Ok(Self::Limited(nrows)),
                _ => Err(format!(
                    "Invalid maxrows {maxrows}. Valid inputs are natural numbers or \'none\', \'inf\', or \'infinite\' for no limit."
                )),
            }
        }
    }
}

impl Display for MaxRows {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlimited => write!(f, "unlimited"),
            Self::Limited(max_rows) => write!(f, "at most {max_rows}"),
        }
    }
}

const OBJECT_STORE_PROFILING_HEADER: &str = "Object Store Profiling";

#[derive(Debug, Clone)]
pub struct PrintOptions {
    pub format: PrintFormat,
    pub quiet: bool,
    pub maxrows: MaxRows,
    pub color: bool,
    pub instrumented_registry: Arc<InstrumentedObjectStoreRegistry>,
}

// Returns the query execution details formatted
fn get_execution_details_formatted(
    row_count: usize,
    maxrows: MaxRows,
    query_start_time: Instant,
) -> String {
    let nrows_shown_msg = match maxrows {
        MaxRows::Limited(nrows) if nrows < row_count => {
            format!("(First {nrows} displayed. Use --maxrows to adjust)")
        }
        _ => String::new(),
    };

    format!(
        "{} row(s) fetched. {}\nElapsed {:.3} seconds.\n",
        row_count,
        nrows_shown_msg,
        query_start_time.elapsed().as_secs_f64()
    )
}

impl PrintOptions {
    /// Print the batches to stdout using the specified format
    pub fn print_batches(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
        query_start_time: Instant,
        row_count: usize,
        format_options: &FormatOptions,
    ) -> Result<()> {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        self.format.print_batches(
            &mut writer,
            schema,
            batches,
            self.maxrows,
            true,
            format_options,
        )?;

        let formatted_exec_details = get_execution_details_formatted(
            row_count,
            if self.format == PrintFormat::Table {
                self.maxrows
            } else {
                MaxRows::Unlimited
            },
            query_start_time,
        );

        self.write_output(&mut writer, formatted_exec_details)
    }

    /// Print the stream to stdout using the specified format
    pub async fn print_stream(
        &self,
        mut stream: Pin<Box<dyn RecordBatchStream>>,
        query_start_time: Instant,
        format_options: &FormatOptions,
    ) -> Result<()> {
        if self.format == PrintFormat::Table {
            return Err(DataFusionError::External(
                "PrintFormat::Table is not implemented".to_string().into(),
            ));
        };

        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        let mut row_count = 0_usize;
        let mut with_header = true;

        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch?;
            row_count += batch.num_rows();
            self.format.print_batches(
                &mut writer,
                batch.schema(),
                &[batch],
                MaxRows::Unlimited,
                with_header,
                format_options,
            )?;
            with_header = false;
        }

        let formatted_exec_details = get_execution_details_formatted(
            row_count,
            MaxRows::Unlimited,
            query_start_time,
        );

        self.write_output(&mut writer, formatted_exec_details)
    }

    fn write_output<W: io::Write>(
        &self,
        writer: &mut W,
        formatted_exec_details: String,
    ) -> Result<()> {
        if !self.quiet {
            writeln!(writer, "{formatted_exec_details}")?;

            let instrument_mode = self.instrumented_registry.instrument_mode();
            if instrument_mode != InstrumentedObjectStoreMode::Disabled {
                writeln!(writer, "{OBJECT_STORE_PROFILING_HEADER}")?;
                for store in self.instrumented_registry.stores() {
                    let requests = store.take_requests();

                    if !requests.is_empty() {
                        writeln!(writer, "{store}")?;
                        if instrument_mode == InstrumentedObjectStoreMode::Trace {
                            for req in requests.iter() {
                                writeln!(writer, "{req}")?;
                            }
                            // Add an extra blank line to help visually organize the output
                            writeln!(writer)?;
                        }

                        writeln!(writer, "Summaries:")?;
                        let summaries = RequestSummaries::new(&requests);
                        writeln!(writer, "{summaries}")?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::error::Result;

    use super::*;

    #[test]
    fn write_output() -> Result<()> {
        let instrumented_registry = Arc::new(InstrumentedObjectStoreRegistry::new());
        let mut print_options = PrintOptions {
            format: PrintFormat::Automatic,
            quiet: true,
            maxrows: MaxRows::Unlimited,
            color: true,
            instrumented_registry: Arc::clone(&instrumented_registry),
        };

        let mut print_output: Vec<u8> = Vec::new();
        let exec_out = String::from("Formatted Exec Output");
        print_options.write_output(&mut print_output, exec_out.clone())?;
        assert!(print_output.is_empty());

        print_options.quiet = false;
        print_options.write_output(&mut print_output, exec_out.clone())?;
        let out_str: String = print_output
            .clone()
            .try_into()
            .expect("Expected successful String conversion");
        assert!(out_str.contains(&exec_out));

        // clear the previous data from the output so it doesn't pollute the next test
        print_output.clear();
        print_options
            .instrumented_registry
            .set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        print_options.write_output(&mut print_output, exec_out.clone())?;
        let out_str: String = print_output
            .clone()
            .try_into()
            .expect("Expected successful String conversion");
        assert!(out_str.contains(&exec_out));
        assert!(out_str.contains(OBJECT_STORE_PROFILING_HEADER));

        Ok(())
    }
}
