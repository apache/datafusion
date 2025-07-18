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

use arrow::array::{Array, ArrayRef, BooleanArray, Datum, Scalar};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::cast::{as_fixed_size_list_array, as_generic_list_array};
use datafusion_common::utils::string_utils::string_array_to_vec;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::{InList, ScalarFunction};
use datafusion_expr::simplify::ExprSimplifyResult;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::datum::compare_with_eq;
use itertools::Itertools;

use crate::make_array::make_array_udf;
use crate::utils::make_scalar_function;

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
    haystack_array needle_array, // arg names
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
#[derive(Debug)]
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
        _info: &dyn datafusion_expr::simplify::SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let [haystack, needle] = take_function_args(self.name(), &mut args)?;

        // if the haystack is a constant list, we can use an inlist expression which is more
        // efficient because the haystack is not varying per-row
        if let Expr::Literal(ScalarValue::List(array), _) = haystack {
            // TODO: support LargeList
            // (not supported by `convert_array_to_scalar_vec`)
            // (FixedSizeList not supported either, but seems to have worked fine when attempting to
            // build a reproducer)

            assert_eq!(array.len(), 1); // guarantee of ScalarValue
            if let Ok(scalar_values) =
                ScalarValue::convert_array_to_scalar_vec(array.as_ref())
            {
                assert_eq!(scalar_values.len(), 1);
                let list = scalar_values
                    .into_iter()
                    .flatten()
                    .map(|v| Expr::Literal(v, None))
                    .collect();

                return Ok(ExprSimplifyResult::Simplified(Expr::InList(InList {
                    expr: Box::new(std::mem::take(needle)),
                    list,
                    negated: false,
                })));
            }
        } else if let Expr::ScalarFunction(ScalarFunction { func, args }) = haystack {
            // make_array has a static set of arguments, so we can pull the arguments out from it
            if func == &make_array_udf() {
                return Ok(ExprSimplifyResult::Simplified(Expr::InList(InList {
                    expr: Box::new(std::mem::take(needle)),
                    list: std::mem::take(args),
                    negated: false,
                })));
            }
        }

        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [first_arg, second_arg] = take_function_args(self.name(), &args.args)?;
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
            _ => exec_err!("array_has does not support type '{:?}'.", value.data_type()),
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
                let offsets = (0..=arr.len())
                    .step_by(arr.value_length() as usize)
                    .collect::<Vec<_>>();
                Box::new(offsets.into_iter())
            }
            ArrayWrapper::List(arr) => {
                Box::new(arr.offsets().iter().map(|o| (*o) as usize))
            }
            ArrayWrapper::LargeList(arr) => {
                Box::new(arr.offsets().iter().map(|o| (*o) as usize))
            }
        }
    }
}

fn array_has_dispatch_for_array(
    haystack: ArrayWrapper<'_>,
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
    if values.is_empty() {
        return Ok(Arc::new(BooleanArray::new(
            BooleanBuffer::new_unset(haystack.len()),
            None,
        )));
    }
    let eq_array = compare_with_eq(values, needle, is_nested)?;
    let mut final_contained = vec![None; haystack.len()];

    // Check validity buffer to distinguish between null and empty arrays
    let validity = match &haystack {
        ArrayWrapper::FixedSizeList(arr) => arr.nulls(),
        ArrayWrapper::List(arr) => arr.nulls(),
        ArrayWrapper::LargeList(arr) => arr.nulls(),
    };

    for (i, (start, end)) in haystack.offsets().tuple_windows().enumerate() {
        let length = end - start;

        // Check if the array at this position is null
        if let Some(validity_buffer) = validity {
            if !validity_buffer.is_valid(i) {
                final_contained[i] = None; // null array -> null result
                continue;
            }
        }

        // For non-null arrays: length is 0 for empty arrays
        if length == 0 {
            final_contained[i] = Some(false); // empty array -> false
        } else {
            let sliced_array = eq_array.slice(start, length);
            final_contained[i] = Some(sliced_array.true_count() > 0);
        }
    }

    Ok(Arc::new(BooleanArray::from(final_contained)))
}

fn array_has_all_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    array_has_all_and_any_inner(args, ComparisonType::All)
}

// General row comparison for array_has_all and array_has_any
fn general_array_has_for_all_and_any<'a>(
    haystack: &ArrayWrapper<'a>,
    needle: &ArrayWrapper<'a>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    let converter = RowConverter::new(vec![SortField::new(haystack.value_type())])?;

    for (arr, sub_arr) in haystack.iter().zip(needle.iter()) {
        if let (Some(arr), Some(sub_arr)) = (arr, sub_arr) {
            let arr_values = converter.convert_columns(&[arr])?;
            let sub_arr_values = converter.convert_columns(&[sub_arr])?;
            boolean_builder.append_value(general_array_has_all_and_any_kernel(
                arr_values,
                sub_arr_values,
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
    haystack: &ArrayWrapper<'a>,
    needle: &ArrayWrapper<'a>,
    comparison_type: ComparisonType,
) -> Result<ArrayRef> {
    let mut boolean_builder = BooleanArray::builder(haystack.len());
    for (arr, sub_arr) in haystack.iter().zip(needle.iter()) {
        match (arr, sub_arr) {
            (Some(arr), Some(sub_arr)) => {
                let haystack_array = string_array_to_vec(&arr);
                let needle_array = string_array_to_vec(&sub_arr);
                boolean_builder.append_value(array_has_string_kernel(
                    haystack_array,
                    needle_array,
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
    haystack: &ArrayWrapper<'a>,
    needle: &ArrayWrapper<'a>,
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
    array_has_all_and_any_dispatch(&haystack, &needle, comparison_type)
}

fn array_has_any_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    array_has_all_and_any_inner(args, ComparisonType::Any)
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
#[derive(Debug)]
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
            signature: Signature::any(2, Volatility::Immutable),
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
    description = "Returns true if any elements exist in both arrays.",
    syntax_example = "array_has_any(array, sub-array)",
    sql_example = r#"```sql
> select array_has_any([1, 2, 3], [3, 4]);
+------------------------------------------+
| array_has_any(List([1,2,3]), List([3,4])) |
+------------------------------------------+
| true                                     |
+------------------------------------------+
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
#[derive(Debug)]
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
            signature: Signature::any(2, Volatility::Immutable),
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
        make_scalar_function(array_has_any_inner)(&args.args)
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
    haystack: Vec<Option<&str>>,
    needle: Vec<Option<&str>>,
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
    haystack_rows: Rows,
    needle_rows: Rows,
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
    use arrow::array::create_array;
    use datafusion_common::utils::SingleRowListArrayBuilder;
    use datafusion_expr::{
        col, execution_props::ExecutionProps, lit, simplify::ExprSimplifyResult, Expr,
        ScalarUDFImpl,
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

        let props = ExecutionProps::new();
        let context = datafusion_expr::simplify::SimplifyContext::new(&props);

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

        let props = ExecutionProps::new();
        let context = datafusion_expr::simplify::SimplifyContext::new(&props);

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
    fn test_array_has_complex_list_not_simplified() {
        let haystack = col("c1");
        let needle = col("c2");

        let props = ExecutionProps::new();
        let context = datafusion_expr::simplify::SimplifyContext::new(&props);

        let Ok(ExprSimplifyResult::Original(args)) =
            ArrayHas::new().simplify(vec![haystack, needle.clone()], &context)
        else {
            panic!("Expected simplified expression");
        };

        assert_eq!(args, vec![col("c1"), col("c2")],);
    }
}
