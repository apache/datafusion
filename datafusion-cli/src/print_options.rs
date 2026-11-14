// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{Display, Formatter};
use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use crate::object_storage::instrumented::{
    InstrumentedObjectStoreMode, InstrumentedObjectStoreRegistry, RequestSummaries,
};
use crate::print_format::{PrintFormat, TableOptions};

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

/// Maximum output width for tables and explain plans.
///
/// ```
/// use datafusion_cli::print_options::MaxWidth;
///
/// assert_eq!("auto".parse::<MaxWidth>().unwrap(), MaxWidth::Auto);
/// assert_eq!("100".parse::<MaxWidth>().unwrap(), MaxWidth::Limited(100));
/// assert_eq!("0".parse::<MaxWidth>().unwrap(), MaxWidth::Unlimited);
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MaxWidth {
    /// Read the current width of the output terminal before each query.
    #[default]
    Auto,
    /// Do not limit output width.
    Unlimited,
    /// Limit output to this many terminal columns.
    Limited(u16),
}

impl FromStr for MaxWidth {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "inf" | "infinite" | "none" | "0" => Ok(Self::Unlimited),
            _ => value
                .parse::<u16>()
                .ok()
                .filter(|width| *width > 0)
                .map(Self::Limited)
                .ok_or_else(|| {
                    format!(
                        "Invalid maxwidth {value}. Expected auto, 1..65535, or 0/inf for no limit."
                    )
                }),
        }
    }
}

impl Display for MaxWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Unlimited => f.write_str("inf"),
            Self::Limited(width) => write!(f, "{width}"),
        }
    }
}

impl MaxWidth {
    pub(crate) fn resolve(self) -> Option<u16> {
        match self {
            Self::Auto => terminal_size::terminal_size_of(io::stdout())
                .map(|(width, _)| width.0)
                .filter(|width| *width > 0),
            Self::Unlimited | Self::Limited(0) => None,
            Self::Limited(width) => Some(width),
        }
    }
}

const OBJECT_STORE_PROFILING_HEADER: &str = "Object Store Profiling";

#[derive(Debug, Clone)]
pub struct PrintOptions {
    pub format: PrintFormat,
    pub quiet: bool,
    pub maxrows: MaxRows,
    /// Maximum output width, independent of the selected output format.
    pub maxwidth: MaxWidth,
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
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        let width = self.maxwidth.resolve();
        let format = if self.format == PrintFormat::Automatic && width.is_some() {
            PrintFormat::Table
        } else {
            self.format
        };

        format.print_batches_with_options(
            &mut writer,
            schema,
            batches,
            TableOptions {
                maxrows: self.maxrows,
                width,
            },
            true,
            format_options,
        )?;

        let formatted_exec_details = get_execution_details_formatted(
            row_count,
            if format == PrintFormat::Table {
                self.maxrows
            } else {
                MaxRows::Unlimited
            },
            query_start_time,
        );

        self.write_output(&mut writer, &formatted_exec_details)
    }

    pub(crate) fn print_tree_batches(
        &self,
        schema: SchemaRef,
        batches: &[RecordBatch],
        query_start_time: Instant,
        row_count: usize,
        format_options: &FormatOptions,
    ) -> Result<()> {
        if self.format != PrintFormat::Table || self.maxwidth.resolve().is_none() {
            return self.print_batches(
                schema,
                batches,
                query_start_time,
                row_count,
                format_options,
            );
        }

        let stdout = io::stdout();
        let mut writer = stdout.lock();
        crate::print_format::print_tree_batches(&mut writer, batches, format_options)?;
        self.write_plan_details(&mut writer, query_start_time, row_count)
    }

    pub(crate) fn print_plan_batches(
        &self,
        batches: &[RecordBatch],
        query_start_time: Instant,
        row_count: usize,
        format_options: &FormatOptions,
        width: Option<u16>,
    ) -> Result<()> {
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        crate::print_format::print_plan_batches(
            &mut writer,
            batches,
            format_options,
            width,
        )?;
        self.write_plan_details(&mut writer, query_start_time, row_count)
    }

    fn write_plan_details<W: io::Write>(
        &self,
        writer: &mut W,
        query_start_time: Instant,
        row_count: usize,
    ) -> Result<()> {
        self.write_output(
            writer,
            &get_execution_details_formatted(
                row_count,
                MaxRows::Unlimited,
                query_start_time,
            ),
        )
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
        }

        let stdout = io::stdout();
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

        self.write_output(&mut writer, &formatted_exec_details)
    }

    fn write_output<W: io::Write>(
        &self,
        writer: &mut W,
        formatted_exec_details: &str,
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
                        let batch = summaries.to_batch();
                        PrintFormat::Table.print_batches_with_options(
                            writer,
                            batch.schema(),
                            &[batch],
                            TableOptions {
                                maxrows: MaxRows::Unlimited,
                                width: self.maxwidth.resolve(),
                            },
                            true,
                            &FormatOptions::default(),
                        )?;
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
            maxwidth: MaxWidth::Auto,
            color: true,
            instrumented_registry: Arc::clone(&instrumented_registry),
        };

        let mut print_output: Vec<u8> = Vec::new();
        let exec_out = String::from("Formatted Exec Output");
        print_options.write_output(&mut print_output, &exec_out)?;
        assert!(print_output.is_empty());

        print_options.quiet = false;
        print_options.write_output(&mut print_output, &exec_out)?;
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
        print_options.write_output(&mut print_output, &exec_out)?;
        let out_str: String = print_output
            .clone()
            .try_into()
            .expect("Expected successful String conversion");
        assert!(out_str.contains(&exec_out));
        assert!(out_str.contains(OBJECT_STORE_PROFILING_HEADER));

        Ok(())
    }
}
