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

//! [`ScalarUDFImpl`] definitions for array_has, array_has_all and array_has_any functions.

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, Datum, Scalar,
    StringArrayType,
};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::cast::{as_fixed_size_list_array, as_generic_list_array};
use datafusion_common::utils::string_utils::string_array_to_vec;
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::ExprSimplifyResult;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility, in_list,
};
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::datum::compare_with_eq;
use itertools::Itertools;

use crate::make_array::make_array_udf;
use crate::utils::make_scalar_function;

use hashbrown::HashSet;
use std::any::Any;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(ArrayHas,
    array_has,
    haystack_array element, // arg names
    "returns true, if the element appears in the first array, otherwise false.", // doc
    array_has_udf // internal function name
);
make_udf_expr_and_func!(ArrayHasAll,
    array_has_all,
    haystack_array needle_array, // arg names
    "returns true if each element of the second array appears in the first array; otherwise, it returns false.", // doc
    array_has_all_udf // internal function name
);
make_udf_expr_and_func!(ArrayHasAny,
    array_has_any,
    first_array second_array, // arg names
    "returns true if at least one element of the second array appears in the first array; otherwise, it returns false.", // doc
    array_has_any_udf // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns true if the array contains the element.",
    syntax_example = "array_has(array, element)",
    sql_example = r#"```sql
> select array_has([1, 2, 3], 2);
+-----------------------------+
| array_has(List([1,2,3]), 2) |
+-----------------------------+
| true                        |
+-----------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Scalar or Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayHas {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayHas {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayHas {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![
                String::from("list_has"),
                String::from("array_contains"),
                String::from("list_contains"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayHas {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn simplify(
        &self,
        mut args: Vec<Expr>,
        _info: &datafusion_expr::simplify::SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [haystack, needle] = take_function_args(self.name(), &mut args)?;

        // if the haystack is a constant list, we can use an inlist expression which is more
        // efficient because the haystack is not varying per-row
        match haystack {
            Expr::Literal(scalar, _) if scalar.is_null() => {
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::Boolean(None),
                    None,
                )));
            }
            Expr::Literal(
                // FixedSizeList gets coerced to List
                scalar @ ScalarValue::List(_) | scalar @ ScalarValue::LargeList(_),
                _,
            ) => {
                if let Ok(scalar_values) =
                    ScalarValue::convert_array_to_scalar_vec(&scalar.to_array()?)
                {
                    assert_eq!(scalar_values.len(), 1);
                    let list = scalar_values
                        .into_iter()
                        .flatten()
                        .flatten()
                        .map(|v| Expr::Literal(v, None))
                        .collect();

                    return Ok(ExprSimplifyResult::Simplified(in_list(
                        std::mem::take(needle),
                        list,
                        false,
                    )));
                }
            }
            Expr::ScalarFunction(ScalarFunction { func, args })
                if func == &make_array_udf() =>
            {
                // make_array has a static set of arguments, so we can pull the arguments out from it
                return Ok(ExprSimplifyResult::Simplified(in_list(
                    std::mem::take(needle),
                    std::mem::take(args),
                    false,
                )));
            }
            _ => {}
        };
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [first_arg, second_arg] = take_function_args(self.name(), &args.args)?;
        if first_arg.data_type().is_null() {
            // Always return null if the first argument is null
            // i.e. array_has(null, element) -> null
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        match &second_arg {
            ColumnarValue::Array(array_needle) => {
                // the needle is already an array, convert the haystack to an array of the same length
                let haystack = first_arg.to_array(array_needle.len())?;
                let array = array_has_inner_for_array(&haystack, array_needle)?;
                Ok(ColumnarValue::Array(array))
            }
            ColumnarValue::Scalar(scalar_needle) => {
                // Always return null if the second argument is null
                // i.e. array_has(array, null) -> null
                if scalar_needle.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                // since the needle is a scalar, convert it to an array of size 1
                let haystack = first_arg.to_array(1)?;
                let needle = scalar_needle.to_array_of_size(1)?;
                let needle = Scalar::new(needle);
                let array = array_has_inner_for_scalar(&haystack, &needle)?;
                if let ColumnarValue::Scalar(_) = &first_arg {
                    // If both inputs are scalar, keeps output as scalar
                    let scalar_value = ScalarValue::try_from_array(&array, 0)?;
                    Ok(ColumnarValue::Scalar(scalar_value))
                } else {
                    Ok(ColumnarValue::Array(array))
                }
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_has_inner_for_scalar(
    haystack: &ArrayRef,
    needle: &dyn Datum,
) -> Result<ArrayRef> {
    let haystack = haystack.as_ref().try_into()?;
    array_has_dispatch_for_scalar(haystack, needle)
}

fn array_has_inner_for_array(haystack: &ArrayRef, needle: &ArrayRef) -> Result<ArrayRef> {
    let haystack = haystack.as_ref().try_into()?;
    array_has_dispatch_for_array(haystack, needle)
}

#[derive(Copy, Clone)]
enum ArrayWrapper<'a> {
    FixedSizeList(&'a arrow::array::FixedSizeListArray),
    List(&'a arrow::array::GenericListArray<i32>),
    LargeList(&'a arrow::array::GenericListArray<i64>),
}

impl<'a> TryFrom<&'a dyn Array> for ArrayWrapper<'a> {
    type Error = DataFusionError;

    fn try_from(
        value: &'a dyn Array,
    ) -> std::result::Result<ArrayWrapper<'a>, Self::Error> {
        match value.data_type() {
            DataType::List(_) => {
                Ok(ArrayWrapper::List(as_generic_list_array::<i32>(value)?))
            }
            DataType::LargeList(_) => Ok(ArrayWrapper::LargeList(
                as_generic_list_array::<i64>(value)?,
            )),
            DataType::FixedSizeList(_, _) => Ok(ArrayWrapper::FixedSizeList(
                as_fixed_size_list_array(value)?,
            )),
            _ => exec_err!("array_has does not support type '{}'.", value.data_type()),
        }
    }
}

impl<'a> ArrayWrapper<'a> {
    fn len(&self) -> usize {
        match self {
            ArrayWrapper::FixedSizeList(arr) => arr.len(),
            ArrayWrapper::List(arr) => arr.len(),
            ArrayWrapper::LargeList(arr) => arr.len(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Option<ArrayRef>> + 'a> {
        match self {
            ArrayWrapper::FixedSizeList(arr) => Box::new(arr.iter()),
            ArrayWrapper::List(arr) => Box::new(arr.iter()),
            ArrayWrapper::LargeList(arr) => Box::new(arr.iter()),
        }
    }

    fn values(&self) -> &ArrayRef {
        match self {
            ArrayWrapper::FixedSizeList(arr) => arr.values(),
            ArrayWrapper::List(arr) => arr.values(),
            ArrayWrapper::LargeList(arr) => arr.values(),
        }
    }

    fn value_type(&self) -> DataType {
        match self {
            ArrayWrapper::FixedSizeList(arr) => arr.value_type(),
            ArrayWrapper::List(arr) => arr.value_type(),
            ArrayWrapper::LargeList(arr) => arr.value_type(),
        }
    }

    fn offsets(&self) -> Box<dyn Iterator<Item = usize> + 'a> {
        match self {
            ArrayWrapper::FixedSizeList(arr) => {
                let value_length = arr.value_length() as usize;
                Box::new((0..=arr.len()).map(move |i| i * value_length))
            }
            ArrayWrapper::List(arr) => {
                Box::new(arr.offsets().iter().map(|o| (*o) as usize))
            }
            ArrayWrapper::LargeList(arr) => {
                Box::new(arr.offsets().iter().map(|o| (*o) as usize))
            }
        }
    }

    fn nulls(&self) -> Option<&arrow::buffer::NullBuffer> {
        match self {
            ArrayWrapper::FixedSizeList(arr) => arr.nulls(),
            ArrayWrapper::List(arr) => arr.nulls(),
            ArrayWrapper::LargeList(arr) => arr.nulls(),
        }
    }
}

fn array_has_dispatch_for_array<'a>(
    haystack: ArrayWrapper<'a>,
    needle: &ArrayRef,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    for (i, arr) in haystack.iter().enumerate() {
        if arr.is_none() || needle.is_null(i) {
            boolean_builder.append_null();
            continue;
        }
        let arr = arr.unwrap();
        let is_nested = arr.data_type().is_nested();
        let needle_row = Scalar::new(needle.slice(i, 1));
        let eq_array = compare_with_eq(&arr, &needle_row, is_nested)?;
        boolean_builder.append_value(eq_array.true_count() > 0);
    }

    Ok(Arc::new(boolean_builder.finish()))
}

fn array_has_dispatch_for_scalar(
    haystack: ArrayWrapper<'_>,
    needle: &dyn Datum,
) -> Result<ArrayRef> {
    let values = haystack.values();
    let is_nested = values.data_type().is_nested();
    // If first argument is empty list (second argument is non-null), return false
    // i.e. array_has([], non-null element) -> false
    if haystack.len() == 0 {
        return Ok(Arc::new(BooleanArray::new(
            BooleanBuffer::new_unset(haystack.len()),
            None,
        )));
    }
    let eq_array = compare_with_eq(values, needle, is_nested)?;

    // When a haystack element is null, `eq()` returns null (not false).
    // In Arrow, a null BooleanArray entry has validity=0 but an
    // undefined value bit that may happen to be 1. Since set_indices()
    // operates on the raw value buffer and ignores validity, we AND the
    // values with the validity bitmap to clear any undefined bits at
    // null positions. This ensures set_indices() only yields positions
    // where the comparison genuinely returned true.
    let eq_bits = match eq_array.nulls() {
        Some(nulls) => eq_array.values() & nulls.inner(),
        None => eq_array.values().clone(),
    };

    let validity = match &haystack {
        ArrayWrapper::FixedSizeList(arr) => arr.nulls(),
        ArrayWrapper::List(arr) => arr.nulls(),
        ArrayWrapper::LargeList(arr) => arr.nulls(),
    };
    let mut matches = eq_bits.set_indices().peekable();
    let mut values = BooleanBufferBuilder::new(haystack.len());
    values.append_n(haystack.len(), false);

    for (i, (_start, end)) in haystack.offsets().tuple_windows().enumerate() {
        let has_match = matches.peek().is_some_and(|&p| p < end);

        // Advance past all match positions in this row's range.
        while matches.peek().is_some_and(|&p| p < end) {
            matches.next();
        }

        if has_match && validity.is_none_or(|v| v.is_valid(i)) {
            values.set_bit(i, true);
        }
    }

    // A null haystack row always produces a null output, so we can
    // reuse the haystack's null buffer directly.
    Ok(Arc::new(BooleanArray::new(
        values.finish(),
        validity.cloned(),
    )))
}

fn array_has_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    array_has_all_and_any_inner(args, ComparisonType::All)
}

// General row comparison for array_has_all and array_has_any
fn general_array_has_for_all_and_any<'a>(
    haystack: ArrayWrapper<'a>,
    needle: ArrayWrapper<'a>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    let converter = RowConverter::new(vec![SortField::new(haystack.value_type())])?;

    for (arr, sub_arr) in haystack.iter().zip(needle.iter()) {
        if let (Some(arr), Some(sub_arr)) = (arr, sub_arr) {
            let arr_values = converter.convert_columns(&[arr])?;
            let sub_arr_values = converter.convert_columns(&[sub_arr])?;
            boolean_builder.append_value(general_array_has_all_and_any_kernel(
                &arr_values,
                &sub_arr_values,
                comparison_type,
            ));
        } else {
            boolean_builder.append_null();
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

// String comparison for array_has_all and array_has_any
fn array_has_all_and_any_string_internal<'a>(
    haystack: ArrayWrapper<'a>,
    needle: ArrayWrapper<'a>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    for (arr, sub_arr) in haystack.iter().zip(needle.iter()) {
        match (arr, sub_arr) {
            (Some(arr), Some(sub_arr)) => {
                let haystack_array = string_array_to_vec(&arr);
                let needle_array = string_array_to_vec(&sub_arr);
                boolean_builder.append_value(array_has_string_kernel(
                    &haystack_array,
                    &needle_array,
                    comparison_type,
                ));
            }
            (_, _) => {
                boolean_builder.append_null();
            }
        }
    }

    Ok(Arc::new(boolean_builder.finish()))
}

fn array_has_all_and_any_dispatch<'a>(
    haystack: ArrayWrapper<'a>,
    needle: ArrayWrapper<'a>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    if needle.values().is_empty() {
        let buffer = match comparison_type {
            ComparisonType::All => BooleanBuffer::new_set(haystack.len()),
            ComparisonType::Any => BooleanBuffer::new_unset(haystack.len()),
        };
        Ok(Arc::new(BooleanArray::from(buffer)))
    } else {
        match needle.value_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                array_has_all_and_any_string_internal(haystack, needle, comparison_type)
            }
            _ => general_array_has_for_all_and_any(haystack, needle, comparison_type),
        }
    }
}

fn array_has_all_and_any_inner(
    args: &[ArrayRef],
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let haystack: ArrayWrapper = args[0].as_ref().try_into()?;
    let needle: ArrayWrapper = args[1].as_ref().try_into()?;
    array_has_all_and_any_dispatch(haystack, needle, comparison_type)
}

fn array_has_any_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    array_has_all_and_any_inner(args, ComparisonType::Any)
}

/// Fast path for `array_has_any` when exactly one argument is a scalar.
fn array_has_any_with_scalar(
    columnar_arg: &ColumnarValue,
    scalar_arg: &ScalarValue,
) -> Result<ColumnarValue> {
    if scalar_arg.is_null() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
    }

    // Convert the scalar to a 1-element ListArray, then extract the inner values
    let scalar_array = scalar_arg.to_array_of_size(1)?;
    let scalar_list: ArrayWrapper = scalar_array.as_ref().try_into()?;
    let offsets: Vec<usize> = scalar_list.offsets().collect();
    let scalar_values = scalar_list
        .values()
        .slice(offsets[0], offsets[1] - offsets[0]);

    // If scalar list is empty, result is always false
    if scalar_values.is_empty() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))));
    }

    match scalar_values.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            array_has_any_with_scalar_string(columnar_arg, &scalar_values)
        }
        _ => array_has_any_with_scalar_general(columnar_arg, &scalar_values),
    }
}

/// When the scalar argument has more elements than this, the scalar fast path
/// builds a HashSet for O(1) lookups. At or below this threshold, it falls
/// back to a linear scan, since hashing every columnar element is more
/// expensive than a linear scan over a short array.
const SCALAR_SMALL_THRESHOLD: usize = 8;

/// String-specialized scalar fast path for `array_has_any`.
fn array_has_any_with_scalar_string(
    columnar_arg: &ColumnarValue,
    scalar_values: &ArrayRef,
) -> Result<ColumnarValue> {
    let (col_arr, is_scalar_output) = match columnar_arg {
        ColumnarValue::Array(arr) => (Arc::clone(arr), false),
        ColumnarValue::Scalar(s) => (s.to_array_of_size(1)?, true),
    };

    let col_list: ArrayWrapper = col_arr.as_ref().try_into()?;
    let col_values = col_list.values();
    let col_offsets: Vec<usize> = col_list.offsets().collect();
    let col_nulls = col_list.nulls();

    let scalar_lookup = ScalarStringLookup::new(scalar_values);
    let has_null_scalar = scalar_values.null_count() > 0;

    let result = match col_values.data_type() {
        DataType::Utf8 => array_has_any_string_inner(
            col_values.as_string::<i32>(),
            &col_offsets,
            col_nulls,
            has_null_scalar,
            &scalar_lookup,
        ),
        DataType::LargeUtf8 => array_has_any_string_inner(
            col_values.as_string::<i64>(),
            &col_offsets,
            col_nulls,
            has_null_scalar,
            &scalar_lookup,
        ),
        DataType::Utf8View => array_has_any_string_inner(
            col_values.as_string_view(),
            &col_offsets,
            col_nulls,
            has_null_scalar,
            &scalar_lookup,
        ),
        _ => unreachable!("array_has_any_with_scalar_string called with non-string type"),
    };

    if is_scalar_output {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

/// Pre-computed lookup structure for the scalar string fastpath.
enum ScalarStringLookup<'a> {
    /// Large scalar: HashSet for O(1) lookups.
    Set(HashSet<&'a str>),
    /// Small scalar: Vec for linear scan.
    List(Vec<Option<&'a str>>),
}

impl<'a> ScalarStringLookup<'a> {
    fn new(scalar_values: &'a ArrayRef) -> Self {
        let strings = string_array_to_vec(scalar_values.as_ref());
        if strings.len() > SCALAR_SMALL_THRESHOLD {
            ScalarStringLookup::Set(strings.into_iter().flatten().collect())
        } else {
            ScalarStringLookup::List(strings)
        }
    }

    fn contains(&self, value: &str) -> bool {
        match self {
            ScalarStringLookup::Set(set) => set.contains(value),
            ScalarStringLookup::List(list) => list.contains(&Some(value)),
        }
    }
}

/// Inner implementation of the string scalar fast path, generic over string
/// array type to allow direct element access by index.
fn array_has_any_string_inner<'a, C: StringArrayType<'a> + Copy>(
    col_strings: C,
    col_offsets: &[usize],
    col_nulls: Option<&arrow::buffer::NullBuffer>,
    has_null_scalar: bool,
    scalar_lookup: &ScalarStringLookup<'_>,
) -> ArrayRef {
    let num_rows = col_offsets.len() - 1;
    let mut builder = BooleanArray::builder(num_rows);

    for i in 0..num_rows {
        if col_nulls.is_some_and(|v| v.is_null(i)) {
            builder.append_null();
            continue;
        }
        let start = col_offsets[i];
        let end = col_offsets[i + 1];
        let found = (start..end).any(|j| {
            if col_strings.is_null(j) {
                has_null_scalar
            } else {
                scalar_lookup.contains(col_strings.value(j))
            }
        });
        builder.append_value(found);
    }

    Arc::new(builder.finish())
}

/// General scalar fast path for `array_has_any`, using RowConverter for
/// type-erased comparison.
fn array_has_any_with_scalar_general(
    columnar_arg: &ColumnarValue,
    scalar_values: &ArrayRef,
) -> Result<ColumnarValue> {
    let converter =
        RowConverter::new(vec![SortField::new(scalar_values.data_type().clone())])?;
    let scalar_rows = converter.convert_columns(&[Arc::clone(scalar_values)])?;

    let (col_arr, is_scalar_output) = match columnar_arg {
        ColumnarValue::Array(arr) => (Arc::clone(arr), false),
        ColumnarValue::Scalar(s) => (s.to_array_of_size(1)?, true),
    };

    let col_list: ArrayWrapper = col_arr.as_ref().try_into()?;
    let col_rows = converter.convert_columns(&[Arc::clone(col_list.values())])?;
    let col_offsets: Vec<usize> = col_list.offsets().collect();
    let col_nulls = col_list.nulls();

    let mut builder = BooleanArray::builder(col_list.len());
    let num_scalar = scalar_rows.num_rows();

    if num_scalar > SCALAR_SMALL_THRESHOLD {
        // Large scalar: build HashSet for O(1) lookups
        let scalar_set: HashSet<Box<[u8]>> = (0..num_scalar)
            .map(|i| Box::from(scalar_rows.row(i).as_ref()))
            .collect();

        for i in 0..col_list.len() {
            if col_nulls.is_some_and(|v| v.is_null(i)) {
                builder.append_null();
                continue;
            }
            let start = col_offsets[i];
            let end = col_offsets[i + 1];
            let found =
                (start..end).any(|j| scalar_set.contains(col_rows.row(j).as_ref()));
            builder.append_value(found);
        }
    } else {
        // Small scalar: linear scan avoids HashSet hashing overhead
        for i in 0..col_list.len() {
            if col_nulls.is_some_and(|v| v.is_null(i)) {
                builder.append_null();
                continue;
            }
            let start = col_offsets[i];
            let end = col_offsets[i + 1];
            let found = (start..end)
                .any(|j| (0..num_scalar).any(|k| col_rows.row(j) == scalar_rows.row(k)));
            builder.append_value(found);
        }
    }

    let result: ArrayRef = Arc::new(builder.finish());

    if is_scalar_output {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns true if all elements of sub-array exist in array.",
    syntax_example = "array_has_all(array, sub-array)",
    sql_example = r#"```sql
> select array_has_all([1, 2, 3, 4], [2, 3]);
+--------------------------------------------+
| array_has_all(List([1,2,3,4]), List([2,3])) |
+--------------------------------------------+
| true                                       |
+--------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "sub-array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayHasAll {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayHasAll {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayHasAll {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(2, None, Volatility::Immutable),
            aliases: vec![String::from("list_has_all")],
        }
    }
}

impl ScalarUDFImpl for ArrayHasAll {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has_all"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_has_all_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns true if the arrays have any elements in common.",
    syntax_example = "array_has_any(array1, array2)",
    sql_example = r#"```sql
> select array_has_any([1, 2, 3], [3, 4]);
+------------------------------------------+
| array_has_any(List([1,2,3]), List([3,4])) |
+------------------------------------------+
| true                                     |
+------------------------------------------+
```"#,
    argument(
        name = "array1",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "array2",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayHasAny {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayHasAny {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayHasAny {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(2, None, Volatility::Immutable),
            aliases: vec![String::from("list_has_any"), String::from("arrays_overlap")],
        }
    }
}

impl ScalarUDFImpl for ArrayHasAny {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_has_any"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [first_arg, second_arg] = take_function_args(self.name(), &args.args)?;

        // If either argument is scalar, use the fast path.
        match (&first_arg, &second_arg) {
            (cv, ColumnarValue::Scalar(scalar)) | (ColumnarValue::Scalar(scalar), cv) => {
                array_has_any_with_scalar(cv, scalar)
            }
            _ => make_scalar_function(array_has_any_inner)(&args.args),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Represents the type of comparison for array_has.
#[derive(Debug, PartialEq, Clone, Copy)]
enum ComparisonType {
    // array_has_all
    All,
    // array_has_any
    Any,
}

fn array_has_string_kernel(
    haystack: &[Option<&str>],
    needle: &[Option<&str>],
    comparison_type: ComparisonType,
) -> bool {
    match comparison_type {
        ComparisonType::All => needle
            .iter()
            .dedup()
            .all(|x| haystack.iter().dedup().any(|y| y == x)),
        ComparisonType::Any => needle
            .iter()
            .dedup()
            .any(|x| haystack.iter().dedup().any(|y| y == x)),
    }
}

fn general_array_has_all_and_any_kernel(
    haystack_rows: &Rows,
    needle_rows: &Rows,
    comparison_type: ComparisonType,
) -> bool {
    match comparison_type {
        ComparisonType::All => needle_rows.iter().all(|needle_row| {
            haystack_rows
                .iter()
                .any(|haystack_row| haystack_row == needle_row)
        }),
        ComparisonType::Any => needle_rows.iter().any(|needle_row| {
            haystack_rows
                .iter()
                .any(|haystack_row| haystack_row == needle_row)
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Int32Type;
    use arrow::{
        array::{Array, ArrayRef, AsArray, Int32Array, ListArray, create_array},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };
    use datafusion_common::{
        DataFusionError, ScalarValue, config::ConfigOptions,
        utils::SingleRowListArrayBuilder,
    };
    use datafusion_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, col, lit,
        simplify::ExprSimplifyResult,
    };

    use crate::expr_fn::make_array;

    use super::ArrayHas;

    #[test]
    fn test_simplify_array_has_to_in_list() {
        let haystack = lit(SingleRowListArrayBuilder::new(create_array!(
            Int32,
            [1, 2, 3]
        ))
        .build_list_scalar());
        let needle = col("c");

        let context = datafusion_expr::simplify::SimplifyContext::default();

        let Ok(ExprSimplifyResult::Simplified(Expr::InList(in_list))) =
            ArrayHas::new().simplify(vec![haystack, needle.clone()], &context)
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(
            in_list,
            datafusion_expr::expr::InList {
                expr: Box::new(needle),
                list: vec![lit(1), lit(2), lit(3)],
                negated: false,
            }
        );
    }

    #[test]
    fn test_simplify_array_has_with_make_array_to_in_list() {
        let haystack = make_array(vec![lit(1), lit(2), lit(3)]);
        let needle = col("c");

        let context = datafusion_expr::simplify::SimplifyContext::default();

        let Ok(ExprSimplifyResult::Simplified(Expr::InList(in_list))) =
            ArrayHas::new().simplify(vec![haystack, needle.clone()], &context)
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(
            in_list,
            datafusion_expr::expr::InList {
                expr: Box::new(needle),
                list: vec![lit(1), lit(2), lit(3)],
                negated: false,
            }
        );
    }

    #[test]
    fn test_simplify_array_has_with_null_to_null() {
        let haystack = Expr::Literal(ScalarValue::Null, None);
        let needle = col("c");

        let context = datafusion_expr::simplify::SimplifyContext::default();
        let Ok(ExprSimplifyResult::Simplified(simplified)) =
            ArrayHas::new().simplify(vec![haystack, needle], &context)
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(simplified, Expr::Literal(ScalarValue::Boolean(None), None));
    }

    #[test]
    fn test_simplify_array_has_with_null_list_to_null() {
        let haystack =
            ListArray::from_iter_primitive::<Int32Type, [Option<i32>; 0], _>([None]);
        let haystack = Expr::Literal(ScalarValue::List(Arc::new(haystack)), None);
        let needle = col("c");

        let context = datafusion_expr::simplify::SimplifyContext::default();
        let Ok(ExprSimplifyResult::Simplified(simplified)) =
            ArrayHas::new().simplify(vec![haystack, needle], &context)
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(simplified, Expr::Literal(ScalarValue::Boolean(None), None));
    }

    #[test]
    fn test_array_has_complex_list_not_simplified() {
        let haystack = col("c1");
        let needle = col("c2");

        let context = datafusion_expr::simplify::SimplifyContext::default();

        let Ok(ExprSimplifyResult::Original(args)) =
            ArrayHas::new().simplify(vec![haystack, needle.clone()], &context)
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(args, vec![col("c1"), col("c2")],);
    }

    #[test]
    fn test_array_has_list_empty_child() -> Result<(), DataFusionError> {
        let haystack_field = Arc::new(Field::new_list(
            "haystack",
            Field::new_list("", Field::new("", DataType::Int32, true), true),
            true,
        ));

        let needle_field = Arc::new(Field::new("needle", DataType::Int32, true));
        let return_field = Arc::new(Field::new("return", DataType::Boolean, true));
        let haystack = ListArray::new(
            Field::new_list_field(DataType::Int32, true).into(),
            OffsetBuffer::new(vec![0, 0].into()),
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Some(vec![false].into()),
        );

        let haystack = ColumnarValue::Array(Arc::new(haystack));
        let needle = ColumnarValue::Scalar(ScalarValue::Int32(Some(1)));
        let result = ArrayHas::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![haystack, needle],
            arg_fields: vec![haystack_field, needle_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let output = result.into_array(1)?;
        let output = output.as_boolean();
        assert_eq!(output.len(), 1);
        assert!(output.is_null(0));

        Ok(())
    }

    #[test]
    fn test_array_has_list_null_haystack() -> Result<(), DataFusionError> {
        let haystack_field = Arc::new(Field::new("haystack", DataType::Null, true));
        let needle_field = Arc::new(Field::new("needle", DataType::Int32, true));
        let return_field = Arc::new(Field::new("return", DataType::Boolean, true));
        let haystack =
            ListArray::from_iter_primitive::<Int32Type, [Option<i32>; 0], _>([
                None, None, None,
            ]);

        let haystack = ColumnarValue::Array(Arc::new(haystack));
        let needle = ColumnarValue::Scalar(ScalarValue::Int32(Some(1)));
        let result = ArrayHas::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![haystack, needle],
            arg_fields: vec![haystack_field, needle_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let output = result.into_array(1)?;
        let output = output.as_boolean();
        assert_eq!(output.len(), 3);
        for i in 0..3 {
            assert!(output.is_null(i));
        }

        Ok(())
    }
}
