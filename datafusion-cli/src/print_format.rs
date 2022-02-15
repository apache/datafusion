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
use arrow::io::json::write::{JsonArray, JsonFormat, LineDelimited};
use datafusion::arrow::io::csv::write;
use datafusion::error::{DataFusionError, Result};
use datafusion::field_util::SchemaExt;
use datafusion::record_batch::RecordBatch;
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

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ArgEnum::from_str(s, true)
    }
}

fn print_batches_to_json<J: JsonFormat>(batches: &[RecordBatch]) -> Result<String> {
    use arrow::io::json::write as json_write;

    if batches.is_empty() {
        return Ok("{}".to_string());
    }
    let mut bytes = vec![];
    let fields = batches
        .first()
        .map(|b| b.schema().field_names())
        .unwrap_or(vec![]);
    let format = J::default();
    let blocks = json_write::Serializer::new(
        batches.into_iter().map(|r| Ok(r.into())),
        fields,
        vec![],
        format,
    );
    json_write::write(&mut bytes, format, blocks)?;

    let formatted = String::from_utf8(bytes)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(formatted)
}

fn print_batches_with_sep(batches: &[RecordBatch], delimiter: u8) -> Result<String> {
    let mut bytes = vec![];
    {
        let mut writer = write::WriterBuilder::new()
            .has_headers(true)
            .delimiter(delimiter)
            .from_writer(&mut bytes);
        let mut is_first = true;
        for batch in batches {
            if is_first {
                write::write_header(&mut writer, &batches[0].schema().field_names())?;
                is_first = false;
            }
            write::write_chunk(
                &mut writer,
                &batch.into(),
                &write::SerializeOptions::default(),
            )?;
        }
    }
    let formatted = String::from_utf8(bytes)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(formatted)
}

impl PrintFormat {
    /// print the batches to stdout using the specified format
    pub fn print_batches(&self, batches: &[RecordBatch]) -> Result<()> {
        match self {
            Self::Csv => println!("{}", print_batches_with_sep(batches, b',')?),
            Self::Tsv => println!("{}", print_batches_with_sep(batches, b'\t')?),
            Self::Table => println!("{}", datafusion::arrow_print::write(batches)),
            Self::Json => {
                println!("{}", print_batches_to_json::<JsonArray>(batches)?)
            }
            Self::NdJson => {
                println!("{}", print_batches_to_json::<LineDelimited>(batches)?)
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
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
                Arc::new(Int32Array::from_slice(&[1, 2, 3])),
                Arc::new(Int32Array::from_slice(&[4, 5, 6])),
                Arc::new(Int32Array::from_slice(&[7, 8, 9])),
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
        let r = print_batches_to_json::<JsonArray>(&batches)?;
        assert_eq!("{}", r);

        let r = print_batches_to_json::<LineDelimited>(&batches)?;
        assert_eq!("{}", r);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_slice(&[1, 2, 3])),
                Arc::new(Int32Array::from_slice(&[4, 5, 6])),
                Arc::new(Int32Array::from_slice(&[7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r = print_batches_to_json::<JsonArray>(&batches)?;
        assert_eq!("[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]", r);

        let r = print_batches_to_json::<LineDelimited>(&batches)?;
        assert_eq!("{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n", r);
        Ok(())
    }
}
