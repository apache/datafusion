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

//! Utility functions to make testing DataFusion based crates easier

use crate::arrow::util::pretty::pretty_format_batches_with_options;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::error::ArrowError;
use std::fmt::Display;
use std::{error::Error, path::PathBuf};

/// Converts a vector or array into an ArrayRef.
pub trait IntoArrayRef {
    fn into_array_ref(self) -> ArrayRef;
}

pub fn format_batches(results: &[RecordBatch]) -> Result<impl Display, ArrowError> {
    let datafusion_format_options = crate::config::FormatOptions::default();

    let arrow_format_options: arrow::util::display::FormatOptions =
        (&datafusion_format_options).try_into().unwrap();

    pretty_format_batches_with_options(results, &arrow_format_options)
}

/// Compares formatted output of a record batch with an expected
/// vector of strings, with the result of pretty formatting record
/// batches. This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
///
/// `assert_batches_eq!(expected_lines: &[&str], batches: &[RecordBatch])`
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow::record_batch::RecordBatch;
/// # use arrow::array::{ArrayRef, Int32Array};
/// # use datafusion_common::assert_batches_eq;
/// let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
/// let batch = RecordBatch::try_from_iter([("column", col)]).unwrap();
/// // Expected output is a vec of strings
/// let expected = vec![
///     "+--------+",
///     "| column |",
///     "+--------+",
///     "| 1      |",
///     "| 2      |",
///     "+--------+",
/// ];
/// // compare the formatted output of the record batch with the expected output
/// assert_batches_eq!(expected, &[batch]);
/// ```
#[macro_export]
macro_rules! assert_batches_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> =
            $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        let formatted = $crate::test_util::format_batches($CHUNKS)
            .unwrap()
            .to_string();

        let actual_lines: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

pub fn batches_to_string(batches: &[RecordBatch]) -> String {
    let actual = format_batches(batches).unwrap().to_string();

    actual.trim().to_string()
}

pub fn batches_to_sort_string(batches: &[RecordBatch]) -> String {
    let actual_lines = format_batches(batches).unwrap().to_string();

    let mut actual_lines: Vec<&str> = actual_lines.trim().lines().collect();

    // sort except for header + footer
    let num_lines = actual_lines.len();
    if num_lines > 3 {
        actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    actual_lines.join("\n")
}

/// Compares formatted output of a record batch with an expected
/// vector of strings in a way that order does not matter.
/// This is a macro so errors appear on the correct line
///
/// See [`assert_batches_eq`] for more details and example.
///
/// Expects to be called about like this:
///
/// `assert_batch_sorted_eq!(expected_lines: &[&str], batches: &[RecordBatch])`
#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let mut expected_lines: Vec<String> =
            $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // sort except for header + footer
        let num_lines = expected_lines.len();
        if num_lines > 3 {
            expected_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        let formatted = $crate::test_util::format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->

        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

        // sort except for header + footer
        let num_lines = actual_lines.len();
        if num_lines > 3 {
            actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

/// A macro to assert that one string is contained within another with
/// a nice error message if they are not.
///
/// Usage: `assert_contains!(actual, expected)`
///
/// Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertible into Strings ([`Into`]<[`String`]>)
#[macro_export]
macro_rules! assert_contains {
    ($ACTUAL: expr, $EXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let expected_value: String = $EXPECTED.into();
        assert!(
            actual_value.contains(&expected_value),
            "Can not find expected in actual.\n\nExpected:\n{}\n\nActual:\n{}",
            expected_value,
            actual_value
        );
    };
}

/// A macro to assert that one string is NOT contained within another with
/// a nice error message if they are are.
///
/// Usage: `assert_not_contains!(actual, unexpected)`
///
/// Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertible into Strings ([`Into`]<[`String`]>)
#[macro_export]
macro_rules! assert_not_contains {
    ($ACTUAL: expr, $UNEXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let unexpected_value: String = $UNEXPECTED.into();
        assert!(
            !actual_value.contains(&unexpected_value),
            "Found unexpected in actual.\n\nUnexpected:\n{}\n\nActual:\n{}",
            unexpected_value,
            actual_value
        );
    };
}

/// Returns the datafusion test data directory, which is by default rooted at `datafusion/core/tests/data`.
///
/// The default can be overridden by the optional environment
/// variable `DATAFUSION_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion_common::test_util::datafusion_test_data();
/// let csvdata = format!("{}/window_1.csv", testdata);
/// assert!(std::path::PathBuf::from(csvdata).exists());
/// ```
pub fn datafusion_test_data() -> String {
    match get_data_dir("DATAFUSION_TEST_DATA", "../../datafusion/core/tests/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {err}"),
    }
}

/// Returns the arrow test data directory, which is by default stored
/// in a git submodule rooted at `testing/data`.
///
/// The default can be overridden by the optional environment
/// variable `ARROW_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion_common::test_util::arrow_test_data();
/// let csvdata = format!("{}/csv/aggregate_test_100.csv", testdata);
/// assert!(std::path::PathBuf::from(csvdata).exists());
/// ```
pub fn arrow_test_data() -> String {
    match get_data_dir("ARROW_TEST_DATA", "../../testing/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {err}"),
    }
}

/// Returns the parquet test data directory, which is by default
/// stored in a git submodule rooted at
/// `parquet-testing/data`.
///
/// The default can be overridden by the optional environment variable
/// `PARQUET_TEST_DATA`
///
/// panics when the directory can not be found.
///
/// Example:
/// ```
/// let testdata = datafusion_common::test_util::parquet_test_data();
/// let filename = format!("{}/binary.parquet", testdata);
/// assert!(std::path::PathBuf::from(filename).exists());
/// ```
#[cfg(feature = "parquet")]
pub fn parquet_test_data() -> String {
    match get_data_dir("PARQUET_TEST_DATA", "../../parquet-testing/data") {
        Ok(pb) => {
            let mut path = pb.display().to_string();
            if cfg!(target_os = "windows") {
                // Replace backslashes (Windows paths; avoids some test issues).
                path = path.replace("\\", "/");
            }
            path
        }
        Err(err) => panic!("failed to get parquet data dir: {err}"),
    }
}

/// Returns a directory path for finding test data.
///
/// udf_env: name of an environment variable
///
/// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
///
///  Returns either:
/// The path referred to in `udf_env` if that variable is set and refers to a directory
/// The submodule_data directory relative to CARGO_MANIFEST_PATH
pub fn get_data_dir(
    udf_env: &str,
    submodule_data: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = std::env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                .into());
            }
        }
    }

    // The env is undefined or its value is trimmed to empty, let's try default dir.

    // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
    // set by `cargo run` or `cargo test`, see:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
            udf_env,
            pb.display(),
        ).into())
    }
}

#[macro_export]
macro_rules! create_array {
    (Boolean, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::BooleanArray::from($values))
    };
    (Int8, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Int8Array::from($values))
    };
    (Int16, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Int16Array::from($values))
    };
    (Int32, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Int32Array::from($values))
    };
    (Int64, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Int64Array::from($values))
    };
    (UInt8, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::UInt8Array::from($values))
    };
    (UInt16, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::UInt16Array::from($values))
    };
    (UInt32, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::UInt32Array::from($values))
    };
    (UInt64, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::UInt64Array::from($values))
    };
    (Float16, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Float16Array::from($values))
    };
    (Float32, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Float32Array::from($values))
    };
    (Float64, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::Float64Array::from($values))
    };
    (Utf8, $values: expr) => {
        std::sync::Arc::new($crate::arrow::array::StringArray::from($values))
    };
}

/// Creates a record batch from literal slice of values, suitable for rapid
/// testing and development.
///
/// Example:
/// ```
/// use datafusion_common::record_batch;
/// let batch = record_batch!(
///     ("a", Int32, vec![1, 2, 3]),
///     ("b", Float64, vec![Some(4.0), None, Some(5.0)]),
///     ("c", Utf8, vec!["alpha", "beta", "gamma"])
/// );
/// ```
#[macro_export]
macro_rules! record_batch {
    ($(($name: expr, $type: ident, $values: expr)),*) => {
        {
            let schema = std::sync::Arc::new($crate::arrow::datatypes::Schema::new(vec![
                $(
                    $crate::arrow::datatypes::Field::new($name, $crate::arrow::datatypes::DataType::$type, true),
                )*
            ]));

            let batch = $crate::arrow::array::RecordBatch::try_new(
                schema,
                vec![$(
                    $crate::create_array!($type, $values),
                )*]
            );

            batch
        }
    }
}

pub mod array_conversion {
    use arrow::array::ArrayRef;

    use super::IntoArrayRef;

    impl IntoArrayRef for Vec<bool> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Boolean, self)
        }
    }

    impl IntoArrayRef for Vec<Option<bool>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Boolean, self)
        }
    }

    impl IntoArrayRef for &[bool] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Boolean, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<bool>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Boolean, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<i8> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int8, self)
        }
    }

    impl IntoArrayRef for Vec<Option<i8>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int8, self)
        }
    }

    impl IntoArrayRef for &[i8] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int8, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<i8>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int8, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<i16> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int16, self)
        }
    }

    impl IntoArrayRef for Vec<Option<i16>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int16, self)
        }
    }

    impl IntoArrayRef for &[i16] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int16, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<i16>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int16, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<i32> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int32, self)
        }
    }

    impl IntoArrayRef for Vec<Option<i32>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int32, self)
        }
    }

    impl IntoArrayRef for &[i32] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int32, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<i32>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int32, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<i64> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int64, self)
        }
    }

    impl IntoArrayRef for Vec<Option<i64>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int64, self)
        }
    }

    impl IntoArrayRef for &[i64] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int64, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<i64>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Int64, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<u8> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt8, self)
        }
    }

    impl IntoArrayRef for Vec<Option<u8>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt8, self)
        }
    }

    impl IntoArrayRef for &[u8] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt8, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<u8>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt8, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<u16> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt16, self)
        }
    }

    impl IntoArrayRef for Vec<Option<u16>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt16, self)
        }
    }

    impl IntoArrayRef for &[u16] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt16, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<u16>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt16, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<u32> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt32, self)
        }
    }

    impl IntoArrayRef for Vec<Option<u32>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt32, self)
        }
    }

    impl IntoArrayRef for &[u32] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt32, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<u32>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt32, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<u64> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt64, self)
        }
    }

    impl IntoArrayRef for Vec<Option<u64>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt64, self)
        }
    }

    impl IntoArrayRef for &[u64] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt64, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<u64>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(UInt64, self.to_vec())
        }
    }

    //#TODO add impl for f16

    impl IntoArrayRef for Vec<f32> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float32, self)
        }
    }

    impl IntoArrayRef for Vec<Option<f32>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float32, self)
        }
    }

    impl IntoArrayRef for &[f32] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float32, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<f32>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float32, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<f64> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float64, self)
        }
    }

    impl IntoArrayRef for Vec<Option<f64>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float64, self)
        }
    }

    impl IntoArrayRef for &[f64] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float64, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<f64>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Float64, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<&str> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self)
        }
    }

    impl IntoArrayRef for Vec<Option<&str>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self)
        }
    }

    impl IntoArrayRef for &[&str] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<&str>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self.to_vec())
        }
    }

    impl IntoArrayRef for Vec<String> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self)
        }
    }

    impl IntoArrayRef for Vec<Option<String>> {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self)
        }
    }

    impl IntoArrayRef for &[String] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self.to_vec())
        }
    }

    impl IntoArrayRef for &[Option<String>] {
        fn into_array_ref(self) -> ArrayRef {
            create_array!(Utf8, self.to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cast::{as_float64_array, as_int32_array, as_string_array};
    use crate::error::Result;

    use super::*;
    use std::env;

    #[test]
    fn test_data_dir() {
        let udf_env = "get_data_dir";
        let cwd = env::current_dir().unwrap();

        let existing_pb = cwd.join("..");
        let existing = existing_pb.display().to_string();
        let existing_str = existing.as_str();

        let non_existing = cwd.join("non-existing-dir").display().to_string();
        let non_existing_str = non_existing.as_str();

        unsafe {
            env::set_var(udf_env, non_existing_str);
            let res = get_data_dir(udf_env, existing_str);
            assert!(res.is_err());

            env::set_var(udf_env, "");
            let res = get_data_dir(udf_env, existing_str);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), existing_pb);

            env::set_var(udf_env, " ");
            let res = get_data_dir(udf_env, existing_str);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), existing_pb);

            env::set_var(udf_env, existing_str);
            let res = get_data_dir(udf_env, existing_str);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), existing_pb);

            env::remove_var(udf_env);
            let res = get_data_dir(udf_env, non_existing_str);
            assert!(res.is_err());

            let res = get_data_dir(udf_env, existing_str);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), existing_pb);
        }
    }

    #[test]
    #[cfg(feature = "parquet")]
    fn test_happy() {
        let res = arrow_test_data();
        assert!(PathBuf::from(res).is_dir());

        let res = parquet_test_data();
        assert!(PathBuf::from(res).is_dir());
    }

    #[test]
    fn test_create_record_batch() -> Result<()> {
        use arrow::array::Array;

        let batch = record_batch!(
            ("a", Int32, vec![1, 2, 3, 4]),
            ("b", Float64, vec![Some(4.0), None, Some(5.0), None]),
            ("c", Utf8, vec!["alpha", "beta", "gamma", "delta"])
        )?;

        assert_eq!(3, batch.num_columns());
        assert_eq!(4, batch.num_rows());

        let values: Vec<_> = as_int32_array(batch.column(0))?
            .values()
            .iter()
            .map(|v| v.to_owned())
            .collect();
        assert_eq!(values, vec![1, 2, 3, 4]);

        let values: Vec<_> = as_float64_array(batch.column(1))?
            .values()
            .iter()
            .map(|v| v.to_owned())
            .collect();
        assert_eq!(values, vec![4.0, 0.0, 5.0, 0.0]);

        let nulls: Vec<_> = as_float64_array(batch.column(1))?
            .nulls()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(nulls, vec![true, false, true, false]);

        let values: Vec<_> = as_string_array(batch.column(2))?.iter().flatten().collect();
        assert_eq!(values, vec!["alpha", "beta", "gamma", "delta"]);

        Ok(())
    }
}
