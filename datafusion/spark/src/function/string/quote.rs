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

use arrow::array::{ArrayRef, GenericStringBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use std::fmt::Write;
use std::sync::Arc;

/// Spark-compatible `quote` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#quote>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkQuote {
    signature: Signature,
}

impl Default for SparkQuote {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkQuote {
    pub fn new() -> Self {
        let str_coercion = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_string()),
            vec![TypeSignatureClass::Any],
            NativeType::String,
        );
        Self {
            signature: Signature::coercible(vec![str_coercion], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkQuote {
    fn name(&self) -> &str {
        "quote"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_quote_inner, vec![])(&args.args)
    }
}

fn spark_quote_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("quote", arg)?;
    match &array.data_type() {
        DataType::Utf8 => quote_array::<i32>(array),
        DataType::LargeUtf8 => quote_array::<i64>(array),
        DataType::Utf8View => quote_view(array),
        other => {
            exec_err!("unsupported data type {other:?} for function `quote`")
        }
    }
}

fn quote_array<T: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<T>(array)?;
    // Slicing an array keeps the whole value buffer and narrows only the
    // offsets, so measure the data through the offsets rather than through
    // `value_data()`. An offset buffer holds one more entry than the array has
    // rows, so it is never empty.
    let offsets = str_array.value_offsets();
    let data_len = offsets.last().unwrap().as_usize() - offsets[0].as_usize();
    Ok(quote_impl::<T, _>(str_array.iter(), data_len))
}

fn quote_view(str_view: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_string_view_array(str_view)?;
    // `total_bytes_len` walks the (sliced) views and counts inlined values,
    // unlike the buffer capacities reported by `get_buffer_memory_size`.
    Ok(quote_impl::<i32, _>(
        str_array.iter(),
        str_array.total_bytes_len(),
    ))
}

const QUOTE_CHAR: char = '\'';
/// A literal quote in the input is emitted as this two-character escape.
const ESCAPED_QUOTE: &str = "\\'";

/// Quotes every value, writing directly into the output buffer.
///
/// `data_capacity` is a hint for the total input byte length; the output adds two
/// surrounding quotes per row plus one byte per escaped quote.
fn quote_impl<'a, O: OffsetSizeTrait, I: Iterator<Item = Option<&'a str>>>(
    input: I,
    data_capacity: usize,
) -> ArrayRef {
    let len = input.size_hint().0;
    let mut builder =
        GenericStringBuilder::<O>::with_capacity(len, data_capacity + 2 * len);
    for value in input {
        match value {
            Some(value) => append_quoted(&mut builder, value),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Appends `s` wrapped in single quotes, with any embedded quote backslash-escaped.
///
/// Writes straight into the builder's buffer — finalized by the trailing
/// `append_value("")` — so no intermediate `String` is allocated per row, and
/// copies the runs between quotes rather than one character at a time.
fn append_quoted<O: OffsetSizeTrait>(builder: &mut GenericStringBuilder<O>, s: &str) {
    // `write_str` on a `GenericStringBuilder` is infallible.
    let mut runs = s.split(QUOTE_CHAR);
    builder.write_char(QUOTE_CHAR).unwrap();
    // `split` always yields at least one run.
    builder.write_str(runs.next().unwrap_or_default()).unwrap();
    for run in runs {
        builder.write_str(ESCAPED_QUOTE).unwrap();
        builder.write_str(run).unwrap();
    }
    builder.write_char(QUOTE_CHAR).unwrap();
    builder.append_value("");
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};

    fn quote(array: ArrayRef) -> ArrayRef {
        spark_quote_inner(&[array]).unwrap()
    }

    fn as_strings(array: &ArrayRef) -> Vec<Option<&str>> {
        as_generic_string_array::<i32>(array)
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn quote_preserves_the_offset_width() {
        let utf8 = quote(Arc::new(StringArray::from(vec!["it's"])) as ArrayRef);
        assert_eq!(utf8.data_type(), &DataType::Utf8);
        assert_eq!(as_strings(&utf8), vec![Some("'it\\'s'")]);

        let large = quote(Arc::new(LargeStringArray::from(vec!["it's"])) as ArrayRef);
        assert_eq!(large.data_type(), &DataType::LargeUtf8);
        let large = as_generic_string_array::<i64>(&large).unwrap();
        assert_eq!(large.value(0), "'it\\'s'");

        // A view input has no offsets to preserve, so it narrows to `Utf8`.
        let view = quote(Arc::new(StringViewArray::from(vec!["it's"])) as ArrayRef);
        assert_eq!(view.data_type(), &DataType::Utf8);
        assert_eq!(as_strings(&view), vec![Some("'it\\'s'")]);
    }

    /// Slicing keeps the whole value buffer, so the capacity hint has to be
    /// measured through the offsets rather than through `value_data()`.
    #[test]
    fn quote_sliced_array() {
        let array = Arc::new(StringArray::from(vec![
            Some("a very long leading value that inflates value_data"),
            Some("it's"),
            None,
            Some(""),
        ])) as ArrayRef;

        let result = quote(array.slice(1, 3));
        assert_eq!(
            as_strings(&result),
            vec![Some("'it\\'s'"), None, Some("''")]
        );
    }

    #[test]
    fn quote_sliced_view_array() {
        let array = Arc::new(StringViewArray::from(vec![
            // Longer than 12 bytes, so this one lives in a data buffer.
            Some("a very long leading value that inflates value_data"),
            Some("it's"),
            None,
        ])) as ArrayRef;

        let result = quote(array.slice(1, 2));
        assert_eq!(as_strings(&result), vec![Some("'it\\'s'"), None]);
    }

    #[test]
    fn quote_empty_array() {
        let empty = quote(Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef);
        assert_eq!(empty.len(), 0);

        let empty_view =
            quote(Arc::new(StringViewArray::from(Vec::<&str>::new())) as ArrayRef);
        assert_eq!(empty_view.len(), 0);
    }
}
