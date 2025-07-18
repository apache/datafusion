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

use super::super::conversion::*;
use super::error::{DFSqlLogicTestError, Result};
use crate::engines::output::DFColumnType;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Fields, Schema};
use arrow::util::display::ArrayFormatter;
use arrow::{array, array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
use datafusion::common::DataFusionError;
use datafusion::config::ConfigField;
use std::path::PathBuf;
use std::sync::LazyLock;

/// Converts `batches` to a result as expected by sqllogictest.
pub fn convert_batches(
    schema: &Schema,
    batches: Vec<RecordBatch>,
    is_spark_path: bool,
) -> Result<Vec<Vec<String>>> {
    let mut rows = vec![];
    for batch in batches {
        // Verify schema
        if !schema.contains(&batch.schema()) {
            return Err(DFSqlLogicTestError::DataFusion(DataFusionError::Internal(
                format!(
                    "Schema mismatch. Previously had\n{:#?}\n\nGot:\n{:#?}",
                    &schema,
                    batch.schema()
                ),
            )));
        }

        // Convert a single batch to a `Vec<Vec<String>>` for comparison, flatten expanded rows, and normalize each.
        let new_rows = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|col| cell_to_string(col, row, is_spark_path))
                    .collect::<Result<Vec<String>>>()
            })
            .collect::<Result<Vec<Vec<String>>>>()?
            .into_iter()
            .flat_map(expand_row)
            .map(normalize_paths);
        rows.extend(new_rows);
    }
    Ok(rows)
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
///   "|-- Projection: d.b, MAX(d.a) AS max_a",
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
        let new_lines: Vec<_> = lines
            .into_iter()
            .enumerate()
            .map(|(idx, l)| {
                // replace any leading spaces with '-' as
                // `sqllogictest` ignores whitespace differences
                //
                // See https://github.com/apache/datafusion/issues/6328
                let content = l.trim_start();
                let new_prefix = "-".repeat(l.len() - content.len());
                // maintain for each line a number, so
                // reviewing explain result changes is easier
                let line_num = idx + 1;
                vec![format!("{line_num:02}){new_prefix}{content}")]
            })
            .collect();

        Either::Right(once(row).chain(new_lines))
    } else {
        Either::Left(once(row))
    }
}

/// normalize path references
///
/// ```text
/// DataSourceExec: files={1 group: [[path/to/datafusion/testing/data/csv/aggregate_test_100.csv]]}, ...
/// ```
///
/// into:
///
/// ```text
/// DataSourceExec: files={1 group: [[WORKSPACE_ROOT/testing/data/csv/aggregate_test_100.csv]]}, ...
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

/// The location of the datafusion checkout
static WORKSPACE_ROOT: LazyLock<object_store::path::Path> = LazyLock::new(|| {
    // e.g. /Software/datafusion/datafusion/core
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // e.g. /Software/datafusion/datafusion
    let workspace_root = dir
        .parent()
        .expect("Can not find parent of datafusion/core")
        // e.g. /Software/datafusion
        .parent()
        .expect("parent of datafusion")
        .to_string_lossy();

    let sanitized_workplace_root = if cfg!(windows) {
        // Object store paths are delimited with `/`, e.g. `/datafusion/datafusion/testing/data/csv/aggregate_test_100.csv`.
        // The default windows delimiter is `\`, so the workplace path is `datafusion\datafusion`.
        workspace_root.replace(std::path::MAIN_SEPARATOR, object_store::path::DELIMITER)
    } else {
        workspace_root.to_string()
    };

    object_store::path::Path::parse(sanitized_workplace_root).unwrap()
});

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
pub fn cell_to_string(col: &ArrayRef, row: usize, is_spark_path: bool) -> Result<String> {
    if !col.is_valid(row) {
        // represent any null value with the string "NULL"
        Ok(NULL_STR.to_string())
    } else {
        match col.data_type() {
            DataType::Null => Ok(NULL_STR.to_string()),
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
                let result = get_row_value!(array::Float64Array, col, row);
                if is_spark_path {
                    Ok(spark_f64_to_str(result))
                } else {
                    Ok(f64_to_str(result))
                }
            }
            DataType::Decimal128(_, scale) => {
                let value = get_row_value!(array::Decimal128Array, col, row);
                Ok(decimal_128_to_str(value, *scale))
            }
            DataType::Decimal256(_, scale) => {
                let value = get_row_value!(array::Decimal256Array, col, row);
                Ok(decimal_256_to_str(value, *scale))
            }
            DataType::LargeUtf8 => Ok(varchar_to_str(get_row_value!(
                array::LargeStringArray,
                col,
                row
            ))),
            DataType::Utf8 => {
                Ok(varchar_to_str(get_row_value!(array::StringArray, col, row)))
            }
            DataType::Utf8View => Ok(varchar_to_str(get_row_value!(
                array::StringViewArray,
                col,
                row
            ))),
            DataType::Dictionary(_, _) => {
                let dict = col.as_any_dictionary();
                let key = dict.normalized_keys()[row];
                Ok(cell_to_string(dict.values(), key, is_spark_path)?)
            }
            _ => {
                let mut datafusion_format_options =
                    datafusion::config::FormatOptions::default();

                datafusion_format_options.set("null", "NULL").unwrap();

                let arrow_format_options: arrow::util::display::FormatOptions =
                    (&datafusion_format_options).try_into().unwrap();

                let f = ArrayFormatter::try_new(col.as_ref(), &arrow_format_options)?;

                Ok(f.value(row).to_string())
            }
        }
        .map_err(DFSqlLogicTestError::Arrow)
    }
}

/// Converts columns to a result as expected by sqllogicteset.
pub fn convert_schema_to_types(columns: &Fields) -> Vec<DFColumnType> {
    columns
        .iter()
        .map(|f| f.data_type())
        .map(|data_type| match data_type {
            DataType::Boolean => DFColumnType::Boolean,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DFColumnType::Integer,
            DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => DFColumnType::Float,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                DFColumnType::Text
            }
            DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => DFColumnType::DateTime,
            DataType::Timestamp(_, _) => DFColumnType::Timestamp,
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer() {
                    // mapping dictionary string types to Text
                    match value_type.as_ref() {
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                            DFColumnType::Text
                        }
                        _ => DFColumnType::Another,
                    }
                } else {
                    DFColumnType::Another
                }
            }
            _ => DFColumnType::Another,
        })
        .collect()
}
