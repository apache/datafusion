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
use std::io::Write;
use std::pin::Pin;
use std::str::FromStr;

use crate::print_format::PrintFormat;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;

use datafusion::execution::SendableRecordBatchStream;
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
                Ok(nrows)  => Ok(Self::Limited(nrows)),
                _ => Err(format!("Invalid maxrows {}. Valid inputs are natural numbers or \'none\', \'inf\', or \'infinite\' for no limit.", maxrows)),
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

#[derive(Debug, Clone)]
pub struct PrintOptions {
    pub format: PrintFormat,
    pub quiet: bool,
    pub maxrows: MaxRows,
    pub color: bool,
}

impl PrintOptions {
    /// Print the batches to stdout using the specified format
    pub fn print_batches(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
        query_start_time: Instant,
        row_count: usize,
    ) -> Result<()> {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        self.format
            .print_batches(&mut writer, schema, batches, self.maxrows, true)?;

        let formatted_exec_details = self.get_execution_details_formatted(
            row_count,
            if self.format == PrintFormat::Table {
                self.maxrows
            } else {
                MaxRows::Unlimited
            },
            query_start_time,
        );

        if !self.quiet {
            writeln!(writer, "{formatted_exec_details}")?;
        }

        Ok(())
    }

    pub async fn print_table_batch(
        &self,
        print_options: &PrintOptions,
        schema: SchemaRef,
        stream: &mut SendableRecordBatchStream,
        max_rows: usize,
        writer: &mut dyn std::io::Write,
        now: Instant,
    ) -> Result<()> {
        let preview_limit: usize = 1000;
        let mut preview_batches: Vec<RecordBatch> = vec![];
        let mut preview_row_count = 0_usize;
        let mut total_count = 0_usize;
        let mut precomputed_widths: Option<Vec<usize>> = None;
        let mut header_printed = false;
        let mut max_rows_reached = false;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let batch_rows = batch.num_rows();

            if !max_rows_reached && total_count < max_rows {
                if total_count + batch_rows > max_rows {
                    let needed = max_rows - total_count;
                    let batch_to_print = batch.slice(0, needed);
                    print_options.format.process_batch(
                        &batch_to_print,
                        schema.clone(),
                        &mut preview_batches,
                        &mut preview_row_count,
                        preview_limit,
                        &mut precomputed_widths,
                        &mut header_printed,
                        writer,
                    )?;
                    if precomputed_widths.is_none() {
                        let widths = print_options
                            .format
                            .compute_column_widths(&preview_batches, schema.clone())?;
                        precomputed_widths = Some(widths.clone());
                        if !header_printed {
                            print_options
                                .format
                                .print_header(&schema, &widths, writer)?;
                            header_printed = true;
                        }
                        for preview_batch in preview_batches.drain(..) {
                            print_options.format.print_batch_with_widths(
                                &preview_batch,
                                &widths,
                                writer,
                            )?;
                        }
                    }
                    if let Some(ref widths) = precomputed_widths {
                        for _ in 0..3 {
                            print_options.format.print_dotted_line(widths, writer)?;
                        }
                        print_options.format.print_bottom_border(widths, writer)?;
                    }
                    max_rows_reached = true;
                } else {
                    print_options.format.process_batch(
                        &batch,
                        schema.clone(),
                        &mut preview_batches,
                        &mut preview_row_count,
                        preview_limit,
                        &mut precomputed_widths,
                        &mut header_printed,
                        writer,
                    )?;
                }
            }

            total_count += batch_rows;
        }

        if !max_rows_reached {
            if precomputed_widths.is_none() && !preview_batches.is_empty() {
                let widths = print_options
                    .format
                    .compute_column_widths(&preview_batches, schema.clone())?;
                precomputed_widths = Some(widths);
                if !header_printed {
                    print_options.format.print_header(
                        &schema,
                        precomputed_widths.as_ref().unwrap(),
                        writer,
                    )?;
                }
            }
            if let Some(ref widths) = precomputed_widths {
                print_options.format.print_bottom_border(widths, writer)?;
            }
        }

        let formatted_exec_details = print_options.get_execution_details_formatted(
            total_count,
            print_options.maxrows,
            now,
        );
        if !print_options.quiet {
            writeln!(writer, "{}", formatted_exec_details)?;
        }

        Ok(())
    }

    /// Print the stream to stdout using the specified format
    pub async fn print_stream(
        &self,
        max_rows: MaxRows,
        mut stream: Pin<Box<dyn RecordBatchStream>>,
        query_start_time: Instant,
    ) -> Result<()> {
        if self.format == PrintFormat::Table {
            return Err(DataFusionError::External(
                "PrintFormat::Table is not implemented".to_string().into(),
            ));
        };

        let max_count = match self.maxrows {
            MaxRows::Unlimited => usize::MAX,
            MaxRows::Limited(n) => n,
        };

        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        let mut row_count = 0_usize;
        let mut with_header = true;
        let mut max_rows_reached = false;

        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch?;
            let curr_batch_rows = batch.num_rows();
            if !max_rows_reached && row_count < max_count {
                if row_count + curr_batch_rows > max_count {
                    let needed = max_count - row_count;
                    let batch_to_print = batch.slice(0, needed);
                    self.format.print_batches(
                        &mut writer,
                        batch.schema(),
                        &[batch_to_print],
                        max_rows,
                        with_header,
                    )?;
                    max_rows_reached = true;
                } else {
                    self.format.print_batches(
                        &mut writer,
                        batch.schema(),
                        &[batch],
                        max_rows,
                        with_header,
                    )?;
                }
            }
            row_count += curr_batch_rows;
            with_header = false;
        }

        let formatted_exec_details =
            self.get_execution_details_formatted(row_count, max_rows, query_start_time);

        if !self.quiet {
            writeln!(writer, "{formatted_exec_details}")?;
        }

        Ok(())
    }

    // Returns the query execution details formatted
    pub fn get_execution_details_formatted(
        &self,
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
}
