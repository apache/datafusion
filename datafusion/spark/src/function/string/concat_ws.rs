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

//! Spark-compatible `concat_ws`: joins strings (and array elements) with a separator.
//!
//! Null scalar args and null array elements are skipped; a null separator yields a
//! null row. Non-string args are coerced to STRING; list args (`List`, `LargeList`,
//! `ListView`, `LargeListView`, `FixedSizeList`) expand their elements.
//!
//! Differences with DataFusion core `concat_ws`:
//! - Accepts list arguments and expands their elements
//! - Always returns Utf8 (Spark's `STRING` type)
//! - Coerces non-string scalars (numbers, booleans, dates, ...) to Utf8

use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, LargeStringArray, OffsetSizeTrait,
    StringArray, StringBuilder, StringViewArray,
};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcatWs {
    signature: Signature,
}

impl Default for SparkConcatWs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkConcatWs {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkConcatWs {
    fn name(&self) -> &str {
        "concat_ws"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return Err(invalid_arg_count_exec_err("concat_ws", (1, i32::MAX), 0));
        }
        Ok(arg_types
            .iter()
            .enumerate()
            .map(|(i, dt)| match dt {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => dt.clone(),
                // Non-separator list args expand their elements at runtime.
                // Normalize the list variant so the kernel only sees
                // List/LargeList, AND force the element type to Utf8 so the
                // planner inserts a cast for non-string children (Spark
                // coerces them to STRING the same way it does for scalars).
                DataType::List(f)
                | DataType::ListView(f)
                | DataType::FixedSizeList(f, _)
                    if i > 0 =>
                {
                    DataType::List(Arc::new(Field::new(
                        f.name(),
                        DataType::Utf8,
                        f.is_nullable(),
                    )))
                }
                DataType::LargeList(f) | DataType::LargeListView(f) if i > 0 => {
                    DataType::LargeList(Arc::new(Field::new(
                        f.name(),
                        DataType::Utf8,
                        f.is_nullable(),
                    )))
                }
                // Spark casts everything else (numbers, booleans, dates,
                // binary, null...) to STRING.
                _ => DataType::Utf8,
            })
            .collect())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Only separator provided → empty string (or NULL if separator is null).
        // Arg-count validation happens in coerce_types at planning time.
        if args.args.len() == 1 {
            return only_separator(&args.args[0]);
        }

        spark_concat_ws(&args.args, args.number_rows)
    }
}

fn only_separator(sep: &ColumnarValue) -> Result<ColumnarValue> {
    match sep {
        ColumnarValue::Scalar(s) if s.is_null() => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            String::new(),
        )))),
        ColumnarValue::Array(arr) => {
            let mut builder = StringBuilder::with_capacity(arr.len(), 0);
            for row_idx in 0..arr.len() {
                if arr.is_null(row_idx) {
                    builder.append_null();
                } else {
                    builder.append_value("");
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
        }
    }
}

fn spark_concat_ws(args: &[ColumnarValue], num_rows: usize) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let sep_view = StringView::try_new(&arrays[0])?;
    let arg_views: Vec<ArgView> = arrays[1..]
        .iter()
        .map(ArgView::try_new)
        .collect::<Result<_>>()?;

    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);

    for row_idx in 0..num_rows {
        if sep_view.is_null(row_idx) {
            builder.append_null();
            continue;
        }

        // Write parts directly into the builder via its `fmt::Write` impl;
        // `append_value("")` then finalises the row (offset + validity) with
        // no extra copy from an intermediate `String`.
        let separator = sep_view.value(row_idx);
        let mut first = true;
        for view in &arg_views {
            view.write_row(row_idx, separator, &mut builder, &mut first)?;
        }
        builder.append_value("");
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
}

/// Typed view over a string array that downcasts once and exposes
/// per-row access without further dispatch.
enum StringView<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringView<'a> {
    fn try_new(arr: &'a ArrayRef) -> Result<Self> {
        match arr.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(arr.as_string::<i32>())),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(arr.as_string::<i64>())),
            DataType::Utf8View => Ok(Self::Utf8View(arr.as_string_view())),
            other => Err(unsupported_data_type_exec_err("concat_ws", "STRING", other)),
        }
    }

    fn value(&self, idx: usize) -> &str {
        match self {
            Self::Utf8(a) => a.value(idx),
            Self::LargeUtf8(a) => a.value(idx),
            Self::Utf8View(a) => a.value(idx),
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Utf8(a) => a.is_null(idx),
            Self::LargeUtf8(a) => a.is_null(idx),
            Self::Utf8View(a) => a.is_null(idx),
        }
    }
}

/// Per-argument view: a string array or a list of strings. The downcast
/// happens once at construction time. `DataType::Null` cannot appear here —
/// `coerce_types` rewrites it to `Utf8` before invocation.
enum ArgView<'a> {
    Str(StringView<'a>),
    List(&'a GenericListArray<i32>),
    LargeList(&'a GenericListArray<i64>),
}

impl<'a> ArgView<'a> {
    fn try_new(arr: &'a ArrayRef) -> Result<Self> {
        match arr.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Ok(Self::Str(StringView::try_new(arr)?))
            }
            DataType::List(_) => Ok(Self::List(arr.as_list::<i32>())),
            DataType::LargeList(_) => Ok(Self::LargeList(arr.as_list::<i64>())),
            other => Err(unsupported_data_type_exec_err(
                "concat_ws",
                "STRING or ARRAY<STRING>",
                other,
            )),
        }
    }

    fn write_row(
        &self,
        row_idx: usize,
        sep: &str,
        builder: &mut StringBuilder,
        first: &mut bool,
    ) -> Result<()> {
        match self {
            Self::Str(view) => {
                if !view.is_null(row_idx) {
                    push_part(builder, view.value(row_idx), sep, first);
                }
            }
            Self::List(list) => write_list_row(*list, row_idx, sep, builder, first)?,
            Self::LargeList(list) => write_list_row(*list, row_idx, sep, builder, first)?,
        }
        Ok(())
    }
}

fn write_list_row<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    row_idx: usize,
    sep: &str,
    builder: &mut StringBuilder,
    first: &mut bool,
) -> Result<()> {
    if list.is_null(row_idx) {
        return Ok(());
    }
    let values = list.value(row_idx);
    // An empty array (e.g. `array()`) contributes nothing — Spark renders it
    // as the empty string, not an error.
    if values.is_empty() {
        return Ok(());
    }
    let view = StringView::try_new(&values)?;
    for i in 0..values.len() {
        if !view.is_null(i) {
            push_part(builder, view.value(i), sep, first);
        }
    }
    Ok(())
}

// `StringBuilder::write_str` only does `extend_from_slice` and never errors;
// the `.expect(..)` is a documentation hint, not a real failure path.
fn push_part(builder: &mut StringBuilder, part: &str, sep: &str, first: &mut bool) {
    if !*first {
        builder
            .write_str(sep)
            .expect("StringBuilder::write_str is infallible");
    }
    *first = false;
    builder
        .write_str(part)
        .expect("StringBuilder::write_str is infallible");
}
