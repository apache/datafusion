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

use arrow::{array, array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
use sqllogictest::{ColumnType, DBOutput};
use datafusion::error::DataFusionError;

use super::super::conversion::*;
use super::error::{DFSqlLogicTestError, Result};

/// Converts `batches` to a DBOutput as expected by sqllogicteset.
///
/// Assumes empty record batches are a successful statement completion
///
pub fn convert_batches(batches: Vec<RecordBatch>) -> Result<DBOutput> {
    if batches.is_empty() {
        // DataFusion doesn't report number of rows complete
        return Ok(DBOutput::StatementComplete(0));
    }

    let schema = batches[0].schema();

    // TODO: report the the actual types of the result
    // https://github.com/apache/arrow-datafusion/issues/4499
    let types = vec![ColumnType::Any; batches[0].num_columns()];

    let mut rows = vec![];
    for batch in batches {
        // Verify schema
        if schema != batch.schema() {
            return Err(DFSqlLogicTestError::DataFusion(DataFusionError::Internal(
                format!(
                    "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                    schema,
                    batch.schema()
                ),
            )));
        }
        rows.append(&mut convert_batch(batch)?);
    }

    Ok(DBOutput::Rows { types, rows })
}

/// Convert a single batch to a `Vec<Vec<String>>` for comparison
fn convert_batch(batch: RecordBatch) -> Result<Vec<Vec<String>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|col| cell_to_string(col, row))
                .collect::<Result<Vec<String>>>()
        })
        .collect()
}

macro_rules! get_row_value {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        array.value($row)
    }};
}

macro_rules! get_string_value {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = array.value($row);
        if s.is_empty() {
            "(empty)".to_string()
        } else {
            s.to_string()
        }
    }};
}

/// Normalizes the content of a single cell in RecordBatch prior to printing.
///
/// This is to make the output comparable to the semi-standard .slt format
///
/// Normalizations applied to [NULL Values and empty strings]
///
/// [NULL Values and empty strings]: https://duckdb.org/dev/sqllogictest/result_verification#null-values-and-empty-strings
///
/// Floating numbers are rounded to have a consistent representation with the Postgres runner.
///
pub fn cell_to_string(col: &ArrayRef, row: usize) -> Result<String> {
    f64::max;
    if !col.is_valid(row) {
        // represent any null value with the string "NULL"
        Ok("NULL".to_string())
    } else {
        match col.data_type() {
            DataType::Boolean => Ok(bool_to_str(get_row_value!(array::BooleanArray, col, row))),
            DataType::Float16 => Ok(float2_to_str(get_row_value!(array::Float16Array, col, row))),
            DataType::Float32 => Ok(float4_to_str(get_row_value!(array::Float32Array, col, row))),
            DataType::Float64 => Ok(float8_to_str(get_row_value!(array::Float64Array, col, row))),
            DataType::LargeUtf8 => Ok(varchar_to_str(get_row_value!(array::LargeStringArray, col, row))),
            DataType::Utf8 => Ok(varchar_to_str(get_row_value!(array::StringArray, col, row))),
            _ => arrow::util::display::array_value_to_string(col, row)
        }.map_err(DFSqlLogicTestError::Arrow)
    }
}
