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
                writeln!($writer)?;
            }
        }
        Ok(()) as Result<()>
    }};
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
            for batch in batches {
                if row_count + batch.num_rows() > maxrows {
                    // If adding this batch exceeds maxrows, slice the batch
                    let limit = maxrows - row_count;
                    let sliced_batch = batch.slice(0, limit);
                    filtered_batches.push(sliced_batch);
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
            write!(writer, "{}", formatted)?;
        }
        MaxRows::Unlimited => {
            let formatted =
                pretty_format_batches_with_options(batches, &DEFAULT_FORMAT_OPTIONS)?;
            write!(writer, "{}", formatted)?;
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
        if batches.is_empty() || batches[0].num_rows() == 0 {
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
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result;
    use datafusion_common::DataFusionError;

    #[test]
    fn test_print_batches_with_sep() {
        let mut buffer = Cursor::new(Vec::new());
        let batches = vec![];
        print_batches_with_sep(&mut buffer, &batches, b',', true).unwrap();
        buffer.set_position(0);
        let mut contents = String::new();
        buffer.read_to_string(&mut contents).unwrap();
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
        )
        .unwrap();
        let batches = vec![batch];
        let mut buffer = Cursor::new(Vec::new());
        print_batches_with_sep(&mut buffer, &batches, b',', true).unwrap();
        buffer.set_position(0);
        let mut contents = String::new();
        buffer.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "a,b,c\n1,4,7\n2,5,8\n3,6,9\n");
    }

    #[test]
    fn test_print_batches_to_json_empty() -> Result<(), DataFusionError> {
        let mut buffer = Cursor::new(Vec::new());
        let batches = vec![];

        // Test with ArrayWriter
        batches_to_json!(ArrayWriter, &mut buffer, &batches)?;
        buffer.set_position(0);
        let mut contents = String::new();
        buffer.read_to_string(&mut contents)?;
        assert_eq!(contents, ""); // Expecting a newline for empty batches

        // Test with LineDelimitedWriter
        let mut buffer = Cursor::new(Vec::new()); // Re-initialize buffer
        batches_to_json!(LineDelimitedWriter, &mut buffer, &batches)?;
        buffer.set_position(0);
        contents.clear();
        buffer.read_to_string(&mut contents)?;
        assert_eq!(contents, ""); // Expecting a newline for empty batches

        Ok(())
    }

    #[test]
    fn test_format_batches_with_maxrows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
                .unwrap();

        let all_rows_expected = "+---+\n| a |\n+---+\n| 1 |\n| 2 |\n| 3 |\n+---+"; // Note the newline at the end
        let one_row_expected = "+---+\n| a |\n+---+\n| 1 |\n+---+"; // Newline at the end

        let mut buffer = Cursor::new(Vec::new());

        // Writing with unlimited rows
        format_batches_with_maxrows(&mut buffer, &[batch.clone()], MaxRows::Unlimited)?;
        buffer.set_position(0);
        let mut contents = String::new();
        buffer.read_to_string(&mut contents)?;
        assert_eq!(contents, all_rows_expected);

        // Reset buffer and contents for the next test
        buffer.set_position(0);
        buffer.get_mut().clear();
        contents.clear();

        // Writing with limited rows
        format_batches_with_maxrows(&mut buffer, &[batch.clone()], MaxRows::Limited(1))?;
        buffer.set_position(0);
        buffer.read_to_string(&mut contents)?;
        assert_eq!(contents, one_row_expected);

        Ok(())
    }
}
