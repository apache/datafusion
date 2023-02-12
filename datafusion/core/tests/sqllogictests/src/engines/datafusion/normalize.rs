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

use arrow::datatypes::SchemaRef;
use arrow::{array, array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
use datafusion_common::DataFusionError;
use lazy_static::lazy_static;
use sqllogictest::DBOutput;
use std::path::PathBuf;

use crate::output::{DFColumnType, DFOutput};

use super::super::conversion::*;
use super::error::{DFSqlLogicTestError, Result};

/// Converts `batches` to a DBOutput as expected by sqllogicteset.
///
/// Assumes empty record batches are a successful statement completion
///
pub fn convert_batches(batches: Vec<RecordBatch>) -> Result<DFOutput> {
    if batches.is_empty() {
        // DataFusion doesn't report number of rows complete
        return Ok(DBOutput::StatementComplete(0));
    }

    let schema = batches[0].schema();

    // TODO: report the the actual types of the result
    // https://github.com/apache/arrow-datafusion/issues/4499
    let types = vec![DFColumnType::Any; batches[0].num_columns()];

    let mut rows = vec![];
    for batch in batches {
        // Verify schema
        if !equivalent_names_and_types(&schema, batch.schema()) {
            return Err(DFSqlLogicTestError::DataFusion(DataFusionError::Internal(
                format!(
                    "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                    &schema,
                    batch.schema()
                ),
            )));
        }

        let new_rows = convert_batch(batch)?
            .into_iter()
            .flat_map(expand_row)
            .map(normalize_paths);
        rows.extend(new_rows);
    }

    Ok(DBOutput::Rows { types, rows })
}

/// special case rows that have newlines in them (like explain plans)
//
/// Transform inputs like:
/// ```text
/// [
///   "logical_plan",
///   "Sort: d.b ASC NULLS LAST\n  Projection: d.b, MAX(d.a) AS max_a",
/// ]
/// ```
///
/// Into one cell per line, adding lines if necessary
/// ```text
/// [
///   "logical_plan",
/// ]
/// [
///   "Sort: d.b ASC NULLS LAST",
/// ]
/// [ <--- newly added row
///   "  Projection: d.b, MAX(d.a) AS max_a",
/// ]
/// ```
fn expand_row(mut row: Vec<String>) -> impl Iterator<Item = Vec<String>> {
    use itertools::Either;
    use std::iter::once;

    // check last cell
    if let Some(cell) = row.pop() {
        let lines: Vec<_> = cell.split('\n').collect();

        // no newlines in last cell
        if lines.len() < 2 {
            row.push(cell);
            return Either::Left(once(row));
        }

        // form new rows with each additional line
        let new_lines: Vec<_> = lines.into_iter().map(|l| vec![l.to_string()]).collect();

        Either::Right(once(row).chain(new_lines.into_iter()))
    } else {
        Either::Left(once(row))
    }
}

/// normalize path references
///
/// ```
/// CsvExec: files={1 group: [[path/to/datafusion/testing/data/csv/aggregate_test_100.csv]]}, ...
/// ```
///
/// into:
///
/// ```
/// CsvExec: files={1 group: [[WORKSPACE_ROOT/testing/data/csv/aggregate_test_100.csv]]}, ...
/// ```
fn normalize_paths(mut row: Vec<String>) -> Vec<String> {
    row.iter_mut().for_each(|s| {
        let workspace_root: &str = WORKSPACE_ROOT.as_ref();
        if s.contains(workspace_root) {
            *s = s.replace(workspace_root, "WORKSPACE_ROOT");
        }
    });
    row
}

/// return the location of the datafusion checkout
fn workspace_root() -> object_store::path::Path {
    // e.g. /Software/arrow-datafusion/datafusion/core
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // e.g. /Software/arrow-datafusion/datafusion
    let workspace_root = dir
        .parent()
        .expect("Can not find parent of datafusion/core")
        // e.g. /Software/arrow-datafusion
        .parent()
        .expect("parent of datafusion")
        .to_string_lossy();

    object_store::path::Path::parse(workspace_root).unwrap()
}

// holds the root directory (
lazy_static! {
    static ref WORKSPACE_ROOT: object_store::path::Path = workspace_root();
}

/// Check two schemas for being equal for field names/types
fn equivalent_names_and_types(schema: &SchemaRef, other: SchemaRef) -> bool {
    if schema.fields().len() != other.fields().len() {
        return false;
    }
    let self_fields = schema.fields().iter();
    let other_fields = other.fields().iter();
    self_fields
        .zip(other_fields)
        .all(|(f1, f2)| f1.name() == f2.name() && f1.data_type() == f2.data_type())
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
    if !col.is_valid(row) {
        // represent any null value with the string "NULL"
        Ok(NULL_STR.to_string())
    } else {
        match col.data_type() {
            DataType::Boolean => {
                Ok(bool_to_str(get_row_value!(array::BooleanArray, col, row)))
            }
            DataType::Float16 => {
                Ok(f16_to_str(get_row_value!(array::Float16Array, col, row)))
            }
            DataType::Float32 => {
                Ok(f32_to_str(get_row_value!(array::Float32Array, col, row)))
            }
            DataType::Float64 => {
                Ok(f64_to_str(get_row_value!(array::Float64Array, col, row)))
            }
            DataType::Decimal128(_, scale) => {
                let value = get_row_value!(array::Decimal128Array, col, row);
                let decimal_scale = u32::try_from((*scale).max(0)).unwrap();
                Ok(i128_to_str(value, decimal_scale))
            }
            DataType::LargeUtf8 => Ok(varchar_to_str(get_row_value!(
                array::LargeStringArray,
                col,
                row
            ))),
            DataType::Utf8 => {
                Ok(varchar_to_str(get_row_value!(array::StringArray, col, row)))
            }
            _ => arrow::util::display::array_value_to_string(col, row),
        }
        .map_err(DFSqlLogicTestError::Arrow)
    }
}
