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

use crate::error::{DFSqlLogicTestError, Result};
use arrow::{array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
use datafusion::error::DataFusionError;
use sqllogictest::{ColumnType, DBOutput};

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

/// Normalizes the content of a single cell in RecordBatch prior to printing.
///
/// This is to make the output comparable to the semi-standard .slt format
///
/// Normalizations applied to [NULL Values and empty strings]
///
/// [NULL Values and empty strings]: https://duckdb.org/dev/sqllogictest/result_verification#null-values-and-empty-strings
///
pub fn cell_to_string(col: &ArrayRef, row: usize) -> Result<String> {
    // represent any null value with the string "NULL"
    if !col.is_valid(row) {
        return Ok("NULL".into());
    }

    // Convert to normal string representation
    let mut s = arrow::util::display::array_value_to_string(col, row)
        .map_err(DFSqlLogicTestError::Arrow)?;

    // apply subsequent normalization depending on type if
    if matches!(col.data_type(), DataType::Utf8 | DataType::LargeUtf8) && s.is_empty() {
        // All empty strings are replaced with this value
        s = "(empty)".to_string();
    }

    Ok(s)
}
