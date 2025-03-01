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

//! Print format variants

use std::str::FromStr;

use crate::print_options::MaxRows;

use arrow::csv::writer::WriterBuilder;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::json::{ArrayWriter, LineDelimitedWriter};
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::common::format::DEFAULT_CLI_FORMAT_OPTIONS;
use datafusion::error::Result;

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ValueEnum, Clone, Copy)]
pub enum PrintFormat {
    Csv,
    Tsv,
    Table,
    Json,
    NdJson,
    Automatic,
}

impl FromStr for PrintFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

macro_rules! batches_to_json {
    ($WRITER: ident, $writer: expr, $batches: expr) => {{
        {
            if !$batches.is_empty() {
                let mut json_writer = $WRITER::new(&mut *$writer);
                for batch in $batches {
                    json_writer.write(batch)?;
                }
                json_writer.finish()?;
                json_finish!($WRITER, $writer);
            }
        }
        Ok(()) as Result<()>
    }};
}

macro_rules! json_finish {
    (ArrayWriter, $writer: expr) => {{
        writeln!($writer)?;
    }};
    (LineDelimitedWriter, $writer: expr) => {{}};
}

fn print_batches_with_sep<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    delimiter: u8,
    with_header: bool,
) -> Result<()> {
    let builder = WriterBuilder::new()
        .with_header(with_header)
        .with_delimiter(delimiter);
    let mut csv_writer = builder.build(writer);

    for batch in batches {
        csv_writer.write(batch)?;
    }

    Ok(())
}

fn keep_only_maxrows(s: &str, maxrows: usize) -> String {
    let lines: Vec<String> = s.lines().map(String::from).collect();

    assert!(lines.len() >= maxrows + 4); // 4 lines for top and bottom border

    let last_line = &lines[lines.len() - 1]; // bottom border line

    let spaces = last_line.len().saturating_sub(4);
    let dotted_line = format!("| .{:<spaces$}|", "", spaces = spaces);

    let mut result = lines[0..(maxrows + 3)].to_vec(); // Keep top border and `maxrows` lines
    result.extend(vec![dotted_line; 3]); // Append ... lines
    result.push(last_line.clone());

    result.join("\n")
}

fn format_batches_with_maxrows<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    maxrows: MaxRows,
) -> Result<()> {
    match maxrows {
        MaxRows::Limited(maxrows) => {
            // Filter batches to meet the maxrows condition
            let mut filtered_batches = Vec::new();
            let mut row_count: usize = 0;
            let mut over_limit = false;
            for batch in batches {
                if row_count + batch.num_rows() > maxrows {
                    // If adding this batch exceeds maxrows, slice the batch
                    let limit = maxrows - row_count;
                    let sliced_batch = batch.slice(0, limit);
                    filtered_batches.push(sliced_batch);
                    over_limit = true;
                    break;
                } else {
                    filtered_batches.push(batch.clone());
                    row_count += batch.num_rows();
                }
            }

            let formatted = pretty_format_batches_with_options(
                &filtered_batches,
                &DEFAULT_CLI_FORMAT_OPTIONS,
            )?;
            if over_limit {
                let mut formatted_str = format!("{}", formatted);
                formatted_str = keep_only_maxrows(&formatted_str, maxrows);
                writeln!(writer, "{}", formatted_str)?;
            } else {
                writeln!(writer, "{}", formatted)?;
            }
        }
        MaxRows::Unlimited => {
            let formatted =
                pretty_format_batches_with_options(batches, &DEFAULT_CLI_FORMAT_OPTIONS)?;
            writeln!(writer, "{}", formatted)?;
        }
    }

    Ok(())
}

impl PrintFormat {
    /// Print the batches to a writer using the specified format
    pub fn print_batches<W: std::io::Write>(
        &self,
        writer: &mut W,
        schema: SchemaRef,
        batches: &[RecordBatch],
        maxrows: MaxRows,
        with_header: bool,
    ) -> Result<()> {
        // filter out any empty batches
        let batches: Vec<_> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        if batches.is_empty() {
            return self.print_empty(writer, schema);
        }

        match self {
            Self::Csv | Self::Automatic => {
                print_batches_with_sep(writer, &batches, b',', with_header)
            }
            Self::Tsv => print_batches_with_sep(writer, &batches, b'\t', with_header),
            Self::Table => {
                if maxrows == MaxRows::Limited(0) {
                    return Ok(());
                }
                format_batches_with_maxrows(writer, &batches, maxrows)
            }
            Self::Json => batches_to_json!(ArrayWriter, writer, &batches),
            Self::NdJson => batches_to_json!(LineDelimitedWriter, writer, &batches),
        }
    }

    /// Print when the result batches contain no rows
    fn print_empty<W: std::io::Write>(
        &self,
        writer: &mut W,
        schema: SchemaRef,
    ) -> Result<()> {
        match self {
            // Print column headers for Table format
            Self::Table if !schema.fields().is_empty() => {
                let empty_batch = RecordBatch::new_empty(schema);
                let formatted = pretty_format_batches_with_options(
                    &[empty_batch],
                    &DEFAULT_CLI_FORMAT_OPTIONS,
                )?;
                writeln!(writer, "{}", formatted)?;
            }
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_batch(
        &self,
        batch: &RecordBatch,
        schema: SchemaRef,
        preview_batches: &mut Vec<RecordBatch>,
        preview_row_count: &mut usize,
        preview_limit: usize,
        precomputed_widths: &mut Option<Vec<usize>>,
        header_printed: &mut bool,
        writer: &mut dyn std::io::Write,
    ) -> Result<()> {
        if precomputed_widths.is_none() {
            preview_batches.push(batch.clone());
            *preview_row_count += batch.num_rows();
            if *preview_row_count >= preview_limit {
                let widths =
                    Self::compute_column_widths(self, preview_batches, schema.clone())?;
                *precomputed_widths = Some(widths.clone());
                Self::print_header(self, &schema, &widths, writer)?;
                *header_printed = true;
                for preview_batch in preview_batches.drain(..) {
                    Self::print_batch_with_widths(self, &preview_batch, &widths, writer)?;
                }
            }
        } else {
            let widths = precomputed_widths.as_ref().unwrap();
            if !*header_printed {
                Self::print_header(self, &schema, widths, writer)?;
                *header_printed = true;
            }
            Self::print_batch_with_widths(self, batch, widths, writer)?;
        }
        Ok(())
    }

    pub fn compute_column_widths(
        &self,
        batches: &Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> Result<Vec<usize>> {
        let mut widths: Vec<usize> =
            schema.fields().iter().map(|f| f.name().len()).collect();
        for batch in batches {
            let formatters = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &DEFAULT_CLI_FORMAT_OPTIONS))
                .collect::<Result<Vec<_>, ArrowError>>()?;
            for row in 0..batch.num_rows() {
                for (i, formatter) in formatters.iter().enumerate() {
                    let cell = formatter.value(row).to_string();
                    let max_line_width =
                        cell.lines().map(|line| line.len()).max().unwrap_or(0);
                    widths[i] = widths[i].max(max_line_width);
                }
            }
        }
        Ok(widths)
    }

    pub fn print_header(
        &self,
        schema: &SchemaRef,
        widths: &[usize],
        writer: &mut dyn std::io::Write,
    ) -> Result<()> {
        Self::print_border(widths, writer)?;

        let header: Vec<String> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| Self::pad_cell(field.name(), widths[i]))
            .collect();
        writeln!(writer, "| {} |", header.join(" | "))?;

        Self::print_border(widths, writer)?;
        Ok(())
    }

    pub fn print_batch_with_widths(
        &self,
        batch: &RecordBatch,
        widths: &[usize],
        writer: &mut dyn std::io::Write,
    ) -> Result<(), ArrowError> {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &DEFAULT_CLI_FORMAT_OPTIONS))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let cell_lines: Vec<Vec<String>> = formatters
                .iter()
                .map(|formatter| {
                    let s = formatter.value(row).to_string();
                    if s.is_empty() {
                        vec!["".to_string()]
                    } else {
                        s.lines().map(|line| line.to_string()).collect()
                    }
                })
                .collect();

            let max_lines = cell_lines
                .iter()
                .map(|lines| lines.len())
                .max()
                .unwrap_or(1);

            for line_idx in 0..max_lines {
                let mut line_cells: Vec<String> = Vec::new();
                for (i, lines) in cell_lines.iter().enumerate() {
                    let cell_line = if line_idx < lines.len() {
                        lines[line_idx].clone()
                    } else {
                        "".to_string()
                    };
                    line_cells.push(format!("{:<width$}", cell_line, width = widths[i]));
                }
                writeln!(writer, "| {} |", line_cells.join(" | "))?;
            }
        }
        Ok(())
    }

    pub fn print_dotted_line(
        &self,
        widths: &[usize],
        writer: &mut dyn std::io::Write,
    ) -> Result<()> {
        let cells: Vec<String> = widths
            .iter()
            .map(|&w| format!(" {: <width$} ", ".", width = w))
            .collect();
        writeln!(writer, "|{}|", cells.join("|"))?;
        Ok(())
    }

    pub fn print_bottom_border(
        &self,
        widths: &[usize],
        writer: &mut dyn std::io::Write,
    ) -> Result<()> {
        let cells: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();
        writeln!(writer, "+{}+", cells.join("+"))?;
        Ok(())
    }

    fn print_border(widths: &[usize], writer: &mut dyn std::io::Write) -> Result<()> {
        let cells: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();
        writeln!(writer, "+{}+", cells.join("+"))?;
        Ok(())
    }

    fn pad_cell(cell: &str, width: usize) -> String {
        format!("{:<width$}", cell, width = width)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn print_empty() {
        for format in [
            PrintFormat::Csv,
            PrintFormat::Tsv,
            PrintFormat::Json,
            PrintFormat::NdJson,
            PrintFormat::Automatic,
        ] {
            // no output for empty batches, even with header set
            PrintBatchesTest::new()
                .with_format(format)
                .with_schema(three_column_schema())
                .with_batches(vec![])
                .with_expected(&[""])
                .run();
        }

        // output column headers for empty batches when format is Table
        #[rustfmt::skip]
        let expected = &[
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "+---+---+---+",
        ];
        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_schema(three_column_schema())
            .with_batches(vec![])
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_csv_no_header() {
        #[rustfmt::skip]
        let expected = &[
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Csv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::No)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_csv_with_header() {
        #[rustfmt::skip]
        let expected = &[
            "a,b,c",
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Csv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_tsv_no_header() {
        #[rustfmt::skip]
        let expected = &[
            "1\t4\t7",
            "2\t5\t8",
            "3\t6\t9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Tsv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::No)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_tsv_with_header() {
        #[rustfmt::skip]
        let expected = &[
            "a\tb\tc",
            "1\t4\t7",
            "2\t5\t8",
            "3\t6\t9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Tsv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_table() {
        let expected = &[
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 1 | 4 | 7 |",
            "| 2 | 5 | 8 |",
            "| 3 | 6 | 9 |",
            "+---+---+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Ignored)
            .with_expected(expected)
            .run();
    }
    #[test]
    fn print_json() {
        let expected =
            &[r#"[{"a":1,"b":4,"c":7},{"a":2,"b":5,"c":8},{"a":3,"b":6,"c":9}]"#];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Json)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Ignored)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_ndjson() {
        let expected = &[
            r#"{"a":1,"b":4,"c":7}"#,
            r#"{"a":2,"b":5,"c":8}"#,
            r#"{"a":3,"b":6,"c":9}"#,
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::NdJson)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Ignored)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_automatic_no_header() {
        #[rustfmt::skip]
            let expected = &[
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Automatic)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::No)
            .with_expected(expected)
            .run();
    }
    #[test]
    fn print_automatic_with_header() {
        #[rustfmt::skip]
            let expected = &[
            "a,b,c",
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Automatic)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_maxrows_unlimited() {
        #[rustfmt::skip]
            let expected = &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "+---+",
        ];

        // should print out entire output with no truncation if unlimited or
        // limit greater than number of batches or equal to the number of batches
        for max_rows in [MaxRows::Unlimited, MaxRows::Limited(5), MaxRows::Limited(3)] {
            PrintBatchesTest::new()
                .with_format(PrintFormat::Table)
                .with_schema(one_column_schema())
                .with_batches(vec![one_column_batch()])
                .with_maxrows(max_rows)
                .with_expected(expected)
                .run();
        }
    }

    #[test]
    fn print_maxrows_limited_one_batch() {
        #[rustfmt::skip]
            let expected = &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| . |",
            "| . |",
            "| . |",
            "+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![one_column_batch()])
            .with_maxrows(MaxRows::Limited(1))
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_maxrows_limited_multi_batched() {
        #[rustfmt::skip]
            let expected = &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "| 1 |",
            "| 2 |",
            "| . |",
            "| . |",
            "| . |",
            "+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![
                one_column_batch(),
                one_column_batch(),
                one_column_batch(),
            ])
            .with_maxrows(MaxRows::Limited(5))
            .with_expected(expected)
            .run();
    }

    #[test]
    fn test_print_batches_empty_batches() {
        let batch = one_column_batch();
        let empty_batch = RecordBatch::new_empty(batch.schema());

        #[rustfmt::skip]
        let expected =&[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![empty_batch.clone(), batch, empty_batch])
            .with_expected(expected)
            .run();
    }

    #[test]
    fn test_print_batches_empty_batch() {
        let empty_batch = RecordBatch::new_empty(one_column_batch().schema());

        // Print column headers for empty batch when format is Table
        #[rustfmt::skip]
        let expected =&[
            "+---+",
            "| a |",
            "+---+",
            "+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_schema(one_column_schema())
            .with_batches(vec![empty_batch])
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();

        // No output for empty batch when schema contains no columns
        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let expected = &[""];
        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_schema(Arc::new(Schema::empty()))
            .with_batches(vec![empty_batch])
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn test_compute_column_widths() {
        let schema = three_column_schema();
        let batches = vec![three_column_batch()];
        let format = PrintFormat::Table;
        let widths = format.compute_column_widths(&batches, schema).unwrap();
        assert_eq!(widths, vec![1, 1, 1]);

        let schema = one_column_schema();
        let batches = vec![one_column_batch()];
        let format = PrintFormat::Table;
        let widths = format.compute_column_widths(&batches, schema).unwrap();
        assert_eq!(widths, vec![1]);

        let schema = three_column_schema();
        let batches = vec![three_column_batch_with_widths()];
        let format = PrintFormat::Table;
        let widths = format.compute_column_widths(&batches, schema).unwrap();
        assert_eq!(widths, [7, 5, 6]);
    }

    /// add the test case for compute multi_lines
    #[test]
    fn test_compute_column_widths_multi_lines() {
        let schema = two_column_schema();
        let batches = vec![two_column_batch_multi_lines()];
        let format = PrintFormat::Table;
        let widths = format.compute_column_widths(&batches, schema).unwrap();
        assert_eq!(widths, vec![13, 53]);
    }

    #[test]
    fn test_print_header() {
        let schema = three_column_schema();
        let widths = vec![1, 1, 1];
        let mut writer = Vec::new();
        let format = PrintFormat::Table;
        format.print_header(&schema, &widths, &mut writer).unwrap();
        let expected = &["+---+---+---+", "| a | b | c |", "+---+---+---+"];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_batch_with_same_widths() {
        let batch = three_column_batch();
        let widths = vec![1, 1, 1];
        let mut writer = Vec::new();
        let format = PrintFormat::Table;
        format
            .print_batch_with_widths(&batch, &widths, &mut writer)
            .unwrap();
        let expected = &["| 1 | 4 | 7 |", "| 2 | 5 | 8 |", "| 3 | 6 | 9 |"];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    /// add the test case for print_batch with multi_lines
    #[test]
    fn test_print_batch_with_multi_lines() {
        let batch = two_column_batch_multi_lines();
        let widths = vec![13, 53];
        let mut writer = Vec::new();
        let format = PrintFormat::Table;
        format
            .print_batch_with_widths(&batch, &widths, &mut writer)
            .unwrap();
        let expected = &[
            "| logical_plan  | Filter: foo.x = Int32(4)                              |",
            "|               |   TableScan: foo projection=[x, y]                    |",
            "| physical_plan | CoalesceBatchesExec: target_batch_size=8192           |",
            "|               |   FilterExec: x@0 = 4                                 |",
            "|               |     DataSourceExec: partitions=1, partition_sizes=[1] |",
        ];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_batch_with_different_widths() {
        let batch = three_column_batch_with_widths();
        let widths = vec![7, 5, 6];
        let mut writer = Vec::new();
        let format = PrintFormat::Table;
        format
            .print_batch_with_widths(&batch, &widths, &mut writer)
            .unwrap();
        let expected = &[
            "| 1       | 42222 | 7      |",
            "| 2222222 | 5     | 8      |",
            "| 3       | 6     | 922222 |",
        ];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_dotted_line() {
        let widths = vec![1, 1, 1];
        let mut writer = Vec::new();
        let format = PrintFormat::Table;
        format.print_dotted_line(&widths, &mut writer).unwrap();
        let expected = &["| . | . | . |"];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_bottom_border() {
        let widths = vec![1, 1, 1];
        let mut writer = Vec::new();
        let format = PrintFormat::Table;
        format.print_bottom_border(&widths, &mut writer).unwrap();
        let expected = &["+---+---+---+"];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_batches_with_maxrows() {
        let batch = one_column_batch();
        let schema = one_column_schema();
        let format = PrintFormat::Table;

        // should print out entire output with no truncation if unlimited or
        // limit greater than number of batches or equal to the number of batches
        for max_rows in [MaxRows::Unlimited, MaxRows::Limited(5), MaxRows::Limited(3)] {
            let mut writer = Vec::new();
            format
                .print_batches(
                    &mut writer,
                    schema.clone(),
                    &[batch.clone()],
                    max_rows,
                    true,
                )
                .unwrap();
            let expected = &[
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
            ];
            let binding = String::from_utf8(writer.clone()).unwrap();
            let actual: Vec<_> = binding.trim_end().split('\n').collect();
            assert_eq!(actual, expected);
        }

        // should truncate output if limit is less than number of batches
        let mut writer = Vec::new();
        format
            .print_batches(
                &mut writer,
                schema.clone(),
                &[batch.clone()],
                MaxRows::Limited(1),
                true,
            )
            .unwrap();
        let expected = &[
            "+---+", "| a |", "+---+", "| 1 |", "| . |", "| . |", "| . |", "+---+",
        ];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    // test print_batch with different batch widths
    // and preview count is less than the first batch
    #[test]
    fn test_print_batches_with_preview_count_less_than_first_batch() {
        let batch = three_column_batch_with_widths();
        let schema = three_column_schema();
        let format = PrintFormat::Table;
        let preview_limit = 2;
        let mut preview_batches = Vec::new();
        let mut preview_row_count = 0;
        let mut precomputed_widths = None;
        let mut header_printed = false;
        let mut writer = Vec::new();

        format
            .process_batch(
                &batch,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        let expected = &[
            "+---------+-------+--------+",
            "| a       | b     | c      |",
            "+---------+-------+--------+",
            "| 1       | 42222 | 7      |",
            "| 2222222 | 5     | 8      |",
            "| 3       | 6     | 922222 |",
        ];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_batches_with_preview_and_later_batches() {
        let batch1 = three_column_batch();
        let batch2 = three_column_batch_with_widths();
        let schema = three_column_schema();
        let format = PrintFormat::Table;
        // preview limit is less than the first batch
        // so the second batch if it's width is greater than the first batch, it will be unformatted
        let preview_limit = 2;
        let mut preview_batches = Vec::new();
        let mut preview_row_count = 0;
        let mut precomputed_widths = None;
        let mut header_printed = false;
        let mut writer = Vec::new();

        format
            .process_batch(
                &batch1,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        format
            .process_batch(
                &batch2,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        format
            .process_batch(
                &batch1,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        let expected = &[
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 1 | 4 | 7 |",
            "| 2 | 5 | 8 |",
            "| 3 | 6 | 9 |",
            "| 1 | 42222 | 7 |",
            "| 2222222 | 5 | 8 |",
            "| 3 | 6 | 922222 |",
            "| 1 | 4 | 7 |",
            "| 2 | 5 | 8 |",
            "| 3 | 6 | 9 |",
        ];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_print_batches_with_preview_cover_later_batches() {
        let batch1 = three_column_batch();
        let batch2 = three_column_batch_with_widths();
        let schema = three_column_schema();
        let format = PrintFormat::Table;
        // preview limit is greater than the first batch
        let preview_limit = 4;
        let mut preview_batches = Vec::new();
        let mut preview_row_count = 0;
        let mut precomputed_widths = None;
        let mut header_printed = false;
        let mut writer = Vec::new();

        format
            .process_batch(
                &batch1,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        format
            .process_batch(
                &batch2,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        format
            .process_batch(
                &batch1,
                schema.clone(),
                &mut preview_batches,
                &mut preview_row_count,
                preview_limit,
                &mut precomputed_widths,
                &mut header_printed,
                &mut writer,
            )
            .unwrap();

        let expected = &[
            "+---------+-------+--------+",
            "| a       | b     | c      |",
            "+---------+-------+--------+",
            "| 1       | 4     | 7      |",
            "| 2       | 5     | 8      |",
            "| 3       | 6     | 9      |",
            "| 1       | 42222 | 7      |",
            "| 2222222 | 5     | 8      |",
            "| 3       | 6     | 922222 |",
            "| 1       | 4     | 7      |",
            "| 2       | 5     | 8      |",
            "| 3       | 6     | 9      |",
        ];
        let binding = String::from_utf8(writer.clone()).unwrap();
        let actual: Vec<_> = binding.trim_end().split('\n').collect();
        assert_eq!(actual, expected);
    }

    #[derive(Debug)]
    struct PrintBatchesTest {
        format: PrintFormat,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        maxrows: MaxRows,
        with_header: WithHeader,
        expected: Vec<&'static str>,
    }

    /// How to test with_header
    #[derive(Debug, Clone)]
    enum WithHeader {
        Yes,
        No,
        /// output should be the same with or without header
        Ignored,
    }

    impl PrintBatchesTest {
        fn new() -> Self {
            Self {
                format: PrintFormat::Table,
                schema: Arc::new(Schema::empty()),
                batches: vec![],
                maxrows: MaxRows::Unlimited,
                with_header: WithHeader::Ignored,
                expected: vec![],
            }
        }

        /// set the format
        fn with_format(mut self, format: PrintFormat) -> Self {
            self.format = format;
            self
        }

        // set the schema
        fn with_schema(mut self, schema: SchemaRef) -> Self {
            self.schema = schema;
            self
        }

        /// set the batches to convert
        fn with_batches(mut self, batches: Vec<RecordBatch>) -> Self {
            self.batches = batches;
            self
        }

        /// set maxrows
        fn with_maxrows(mut self, maxrows: MaxRows) -> Self {
            self.maxrows = maxrows;
            self
        }

        /// set with_header
        fn with_header(mut self, with_header: WithHeader) -> Self {
            self.with_header = with_header;
            self
        }

        /// set expected output
        fn with_expected(mut self, expected: &[&'static str]) -> Self {
            self.expected = expected.to_vec();
            self
        }

        /// run the test
        fn run(self) {
            let actual = self.output();
            let actual: Vec<_> = actual.trim_end().split('\n').collect();
            let expected = self.expected;
            assert_eq!(
                actual, expected,
                "\n\nactual:\n{actual:#?}\n\nexpected:\n{expected:#?}"
            );
        }

        /// formats batches using parameters and returns the resulting output
        fn output(&self) -> String {
            match self.with_header {
                WithHeader::Yes => self.output_with_header(true),
                WithHeader::No => self.output_with_header(false),
                WithHeader::Ignored => {
                    let output = self.output_with_header(true);
                    // ensure the output is the same without header
                    let output_without_header = self.output_with_header(false);
                    assert_eq!(
                        output, output_without_header,
                        "Expected output to be the same with or without header"
                    );
                    output
                }
            }
        }

        fn output_with_header(&self, with_header: bool) -> String {
            let mut buffer: Vec<u8> = vec![];
            self.format
                .print_batches(
                    &mut buffer,
                    self.schema.clone(),
                    &self.batches,
                    self.maxrows,
                    with_header,
                )
                .unwrap();
            String::from_utf8(buffer).unwrap()
        }
    }

    /// Return a schema with three columns
    fn three_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]))
    }

    /// Return a schema with two StringArray columns
    fn two_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
    }

    /// Return a batch with three columns and three rows
    fn three_column_batch() -> RecordBatch {
        RecordBatch::try_new(
            three_column_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap()
    }

    /// Return a batch with three columns and three rows, but with different widths
    fn three_column_batch_with_widths() -> RecordBatch {
        RecordBatch::try_new(
            three_column_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2222222, 3])),
                Arc::new(Int32Array::from(vec![42222, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 922222])),
            ],
        )
        .unwrap()
    }

    /// Return a batch with two columns, but with multi-line values for each column
    fn two_column_batch_multi_lines() -> RecordBatch {
        RecordBatch::try_new(
            two_column_schema(),
            vec![
                Arc::new(StringArray::from(vec!["logical_plan", "physical_plan"])),
                Arc::new(StringArray::from(vec![
                    "Filter: foo.x = Int32(4)\n  TableScan: foo projection=[x, y]",
                    "CoalesceBatchesExec: target_batch_size=8192\n  FilterExec: x@0 = 4\n    DataSourceExec: partitions=1, partition_sizes=[1]\n",
                ])),
            ],
        )
        .unwrap()
    }

    /// Return a schema with one column
    fn one_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// return a batch with one column and three rows
    fn one_column_batch() -> RecordBatch {
        RecordBatch::try_new(
            one_column_schema(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    /// Slice the record batch into 2 batches
    fn split_batch(batch: RecordBatch) -> Vec<RecordBatch> {
        assert!(batch.num_rows() > 1);
        let split = batch.num_rows() / 2;
        vec![
            batch.slice(0, split),
            batch.slice(split, batch.num_rows() - split),
        ]
    }
}
