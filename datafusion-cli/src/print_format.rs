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
use arrow::json::{ArrayWriter, LineDelimitedWriter};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion::error::Result;

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ArgEnum, Clone, Copy)]
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
        clap::ArgEnum::from_str(s, true)
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
                &DEFAULT_FORMAT_OPTIONS,
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
                pretty_format_batches_with_options(batches, &DEFAULT_FORMAT_OPTIONS)?;
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
        batches: &[RecordBatch],
        maxrows: MaxRows,
        with_header: bool,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        match self {
            Self::Csv | Self::Automatic => {
                print_batches_with_sep(writer, batches, b',', with_header)
            }
            Self::Tsv => print_batches_with_sep(writer, batches, b'\t', with_header),
            Self::Table => {
                if maxrows == MaxRows::Limited(0) {
                    return Ok(());
                }
                format_batches_with_maxrows(writer, batches, maxrows)
            }
            Self::Json => batches_to_json!(ArrayWriter, writer, batches),
            Self::NdJson => batches_to_json!(LineDelimitedWriter, writer, batches),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::sync::Arc;

    use super::*;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result;

    fn run_test<F>(batches: &[RecordBatch], test_fn: F) -> Result<String>
    where
        F: Fn(&mut Cursor<Vec<u8>>, &[RecordBatch]) -> Result<()>,
    {
        let mut buffer = Cursor::new(Vec::new());
        test_fn(&mut buffer, batches)?;
        buffer.set_position(0);
        let mut contents = String::new();
        buffer.read_to_string(&mut contents)?;
        Ok(contents)
    }

    #[test]
    fn test_print_batches_with_sep() -> Result<()> {
        let contents = run_test(&[], |buffer, batches| {
            print_batches_with_sep(buffer, batches, b',', true)
        })?;
        assert_eq!(contents, "");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let contents = run_test(&[batch], |buffer, batches| {
            print_batches_with_sep(buffer, batches, b',', true)
        })?;
        assert_eq!(contents, "a,b,c\n1,4,7\n2,5,8\n3,6,9\n");

        Ok(())
    }

    #[test]
    fn test_print_batches_to_json_empty() -> Result<()> {
        let contents = run_test(&[], |buffer, batches| {
            batches_to_json!(ArrayWriter, buffer, batches)
        })?;
        assert_eq!(contents, "");

        let contents = run_test(&[], |buffer, batches| {
            batches_to_json!(LineDelimitedWriter, buffer, batches)
        })?;
        assert_eq!(contents, "");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;
        let batches = vec![batch];

        let contents = run_test(&batches, |buffer, batches| {
            batches_to_json!(ArrayWriter, buffer, batches)
        })?;
        assert_eq!(contents, "[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]\n");

        let contents = run_test(&batches, |buffer, batches| {
            batches_to_json!(LineDelimitedWriter, buffer, batches)
        })?;
        assert_eq!(contents, "{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n");

        Ok(())
    }

    #[test]
    fn test_format_batches_with_maxrows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        #[rustfmt::skip]
        let all_rows_expected = [
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "+---+\n",
        ].join("\n");

        #[rustfmt::skip]
        let one_row_expected = [
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| . |",
            "| . |",
            "| . |",
            "+---+\n",
        ].join("\n");

        #[rustfmt::skip]
        let multi_batches_expected = [
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
            "+---+\n",
        ].join("\n");

        let no_limit = run_test(&[batch.clone()], |buffer, batches| {
            format_batches_with_maxrows(buffer, batches, MaxRows::Unlimited)
        })?;
        assert_eq!(no_limit, all_rows_expected);

        let maxrows_less_than_actual = run_test(&[batch.clone()], |buffer, batches| {
            format_batches_with_maxrows(buffer, batches, MaxRows::Limited(1))
        })?;
        assert_eq!(maxrows_less_than_actual, one_row_expected);

        let maxrows_more_than_actual = run_test(&[batch.clone()], |buffer, batches| {
            format_batches_with_maxrows(buffer, batches, MaxRows::Limited(5))
        })?;
        assert_eq!(maxrows_more_than_actual, all_rows_expected);

        let maxrows_equals_actual = run_test(&[batch.clone()], |buffer, batches| {
            format_batches_with_maxrows(buffer, batches, MaxRows::Limited(3))
        })?;
        assert_eq!(maxrows_equals_actual, all_rows_expected);

        let multi_batches = run_test(
            &[batch.clone(), batch.clone(), batch.clone()],
            |buffer, batches| {
                format_batches_with_maxrows(buffer, batches, MaxRows::Limited(5))
            },
        )?;
        assert_eq!(multi_batches, multi_batches_expected);

        Ok(())
    }


    #[test]
    fn test_print_batches_empty_batches() -> Result<()> {
        let batch = RecordBatch::try_from_iter(
            vec![
                ("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            ],
        )?;
        let empty_batch = RecordBatch::new_empty(batch.schema());

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![
                empty_batch.clone(),
                batch,
                empty_batch,
            ])
            .with_expected(
                &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "+---+\n",
        ])
            .run();
       Ok(())
    }

    struct PrintBatchesTest {
        format: PrintFormat,
        batches: Vec<RecordBatch>,
        maxrows: MaxRows,
        with_header: bool,
        expected: Vec<&'static str>,
    }

    impl PrintBatchesTest {
        fn new() -> Self {
            Self {
                format: PrintFormat::Table,
                batches: vec![],
                maxrows: MaxRows::Unlimited,
                with_header: false,
                expected: vec![]
            }
        }

        /// set the format
        fn with_format(mut self, format: PrintFormat) -> Self {
            self.format = format;
            self
        }

        /// set the batches to convert
        fn with_batches(mut self, batches: Vec<RecordBatch>) -> Self {
            self.batches = batches;
            self
        }

        /// set expected output
        fn with_expected(mut self, expected: &[&'static str]) -> Self {
            self.expected = expected.to_vec();
            self
        }

        /// run the test
        fn run(self) {
            let mut buffer: Vec<u8> = vec![];
            self.format.print_batches(&mut buffer, &self.batches, self.maxrows, self.with_header).unwrap();
            let actual = String::from_utf8(buffer).unwrap();
            let expected = self.expected.join("\n");
            assert_eq!(actual, expected, "actual:\n\n{actual}expected:\n\n{expected}");
        }
    }
}
