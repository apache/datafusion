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
use crate::print_options::MaxRows;
use arrow::csv::writer::WriterBuilder;
use arrow::json::{ArrayWriter, LineDelimitedWriter};
use arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion::error::{DataFusionError, Result};
use std::str::FromStr;

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ArgEnum, Clone)]
pub enum PrintFormat {
    Csv,
    Tsv,
    Table,
    Json,
    NdJson,
}

impl FromStr for PrintFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ArgEnum::from_str(s, true)
    }
}

macro_rules! batches_to_json {
    ($WRITER: ident, $batches: expr) => {{
        let mut bytes = vec![];
        {
            let mut writer = $WRITER::new(&mut bytes);
            $batches.iter().try_for_each(|batch| writer.write(batch))?;
            writer.finish()?;
        }
        String::from_utf8(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?
    }};
}

fn print_batches_with_sep(batches: &[RecordBatch], delimiter: u8) -> Result<String> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new()
            .has_headers(true)
            .with_delimiter(delimiter);
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    let formatted =
        String::from_utf8(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(formatted)
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

fn format_batches_with_maxrows(
    batches: &[RecordBatch],
    maxrows: MaxRows,
) -> Result<String> {
    match maxrows {
        MaxRows::Limited(maxrows) => {
            // Only format enough batches for maxrows
            let mut filtered_batches = Vec::new();
            let mut batches = batches;
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            if row_count > maxrows {
                let mut accumulated_rows = 0;

                for batch in batches {
                    filtered_batches.push(batch.clone());
                    if accumulated_rows + batch.num_rows() > maxrows {
                        break;
                    }
                    accumulated_rows += batch.num_rows();
                }

                batches = &filtered_batches;
            }

            let mut formatted = format!(
                "{}",
                pretty_format_batches_with_options(batches, &DEFAULT_FORMAT_OPTIONS)?,
            );

            if row_count > maxrows {
                formatted = keep_only_maxrows(&formatted, maxrows);
            }

            Ok(formatted)
        }
        MaxRows::Unlimited => {
            // maxrows not specified, print all rows
            Ok(format!(
                "{}",
                pretty_format_batches_with_options(batches, &DEFAULT_FORMAT_OPTIONS)?,
            ))
        }
    }
}

impl PrintFormat {
    /// print the batches to stdout using the specified format
    /// `maxrows` option is only used for `Table` format:
    ///     If `maxrows` is Some(n), then at most n rows will be displayed
    ///     If `maxrows` is None, then every row will be displayed
    pub fn print_batches(&self, batches: &[RecordBatch], maxrows: MaxRows) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        match self {
            Self::Csv => println!("{}", print_batches_with_sep(batches, b',')?),
            Self::Tsv => println!("{}", print_batches_with_sep(batches, b'\t')?),
            Self::Table => {
                if maxrows == MaxRows::Limited(0) {
                    return Ok(());
                }
                println!("{}", format_batches_with_maxrows(batches, maxrows)?,)
            }
            Self::Json => println!("{}", batches_to_json!(ArrayWriter, batches)),
            Self::NdJson => {
                println!("{}", batches_to_json!(LineDelimitedWriter, batches))
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_print_batches_with_sep() {
        let batches = vec![];
        assert_eq!("", print_batches_with_sep(&batches, b',').unwrap());

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
        let r = print_batches_with_sep(&batches, b',').unwrap();
        assert_eq!("a,b,c\n1,4,7\n2,5,8\n3,6,9\n", r);
    }

    #[test]
    fn test_print_batches_to_json_empty() -> Result<()> {
        let batches = vec![];
        let r = batches_to_json!(ArrayWriter, &batches);
        assert_eq!("", r);

        let r = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!("", r);

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
        let r = batches_to_json!(ArrayWriter, &batches);
        assert_eq!("[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]", r);

        let r = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!("{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n", r);
        Ok(())
    }

    #[test]
    fn test_format_batches_with_maxrows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
                .unwrap();

        #[rustfmt::skip]
        let all_rows_expected = [
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "+---+",
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
            "+---+",
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
            "+---+",
        ].join("\n");

        let no_limit = format_batches_with_maxrows(&[batch.clone()], MaxRows::Unlimited)?;
        assert_eq!(all_rows_expected, no_limit);

        let maxrows_less_than_actual =
            format_batches_with_maxrows(&[batch.clone()], MaxRows::Limited(1))?;
        assert_eq!(one_row_expected, maxrows_less_than_actual);
        let maxrows_more_than_actual =
            format_batches_with_maxrows(&[batch.clone()], MaxRows::Limited(5))?;
        assert_eq!(all_rows_expected, maxrows_more_than_actual);
        let maxrows_equals_actual =
            format_batches_with_maxrows(&[batch.clone()], MaxRows::Limited(3))?;
        assert_eq!(all_rows_expected, maxrows_equals_actual);
        let multi_batches = format_batches_with_maxrows(
            &[batch.clone(), batch.clone(), batch.clone()],
            MaxRows::Limited(5),
        )?;
        assert_eq!(multi_batches_expected, multi_batches);

        Ok(())
    }
}
