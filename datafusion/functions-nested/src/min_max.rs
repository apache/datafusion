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

//! [`ScalarUDFImpl`] definitions for array_min and array_max functions.
use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, GenericListArray,
    OffsetSizeTrait, PrimitiveBuilder, downcast_primitive,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeList, List};
use datafusion_common::Result;
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{ScalarValue, exec_err, plan_err};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_aggregate_common::min_max::{max_batch, min_batch};
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayMax,
    array_max,
    array,
    "returns the maximum value in the array.",
    array_max_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the maximum value in the array.",
    syntax_example = "array_max(array)",
    sql_example = r#"```sql
> select array_max([3,1,4,2]);
+-----------------------------------------+
| array_max(List([3,1,4,2]))              |
+-----------------------------------------+
| 4                                       |
+-----------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayMax {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayMax {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMax {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_max".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayMax {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array] = take_function_args(self.name(), arg_types)?;
        match array {
            List(field) | LargeList(field) => Ok(field.data_type().clone()),
            arg_type => plan_err!("{} does not support type {arg_type}", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_max_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_max_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_max", args)?;
    match array.data_type() {
        List(_) => array_min_max_helper(as_list_array(array)?, false),
        LargeList(_) => array_min_max_helper(as_large_list_array(array)?, false),
        arg_type => exec_err!("array_max does not support type: {arg_type}"),
    }
}

make_udf_expr_and_func!(
    ArrayMin,
    array_min,
    array,
    "returns the minimum value in the array",
    array_min_udf
);
#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the minimum value in the array.",
    syntax_example = "array_min(array)",
    sql_example = r#"```sql
> select array_min([3,1,4,2]);
+-----------------------------------------+
| array_min(List([3,1,4,2]))              |
+-----------------------------------------+
| 1                                       |
+-----------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayMin {
    signature: Signature,
}

impl Default for ArrayMin {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayMin {
    fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayMin {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array] = take_function_args(self.name(), arg_types)?;
        match array {
            List(field) | LargeList(field) => Ok(field.data_type().clone()),
            arg_type => plan_err!("{} does not support type {}", self.name(), arg_type),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_min_inner)(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_min_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_min", args)?;
    match array.data_type() {
        List(_) => array_min_max_helper(as_list_array(array)?, true),
        LargeList(_) => array_min_max_helper(as_large_list_array(array)?, true),
        arg_type => exec_err!("array_min does not support type: {arg_type}"),
    }
}

fn array_min_max_helper<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    is_min: bool,
) -> Result<ArrayRef> {
    // Try the primitive fast path first
    if let Some(result) = try_primitive_array_min_max(array, is_min) {
        return result;
    }

    // Fallback: per-row ScalarValue path for non-primitive types
    let agg_fn = if is_min { min_batch } else { max_batch };
    let null_value = ScalarValue::try_from(array.value_type())?;
    let result_vec: Vec<ScalarValue> = array
        .iter()
        .map(|arr| arr.as_ref().map_or_else(|| Ok(null_value.clone()), agg_fn))
        .try_collect()?;
    ScalarValue::iter_to_array(result_vec)
}

/// Dispatches to a typed primitive min/max implementation, or returns `None` if
/// the element type is not a primitive.
fn try_primitive_array_min_max<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    is_min: bool,
) -> Option<Result<ArrayRef>> {
    macro_rules! helper {
        ($t:ty) => {
            return Some(primitive_array_min_max::<O, $t>(list_array, is_min))
        };
    }
    downcast_primitive! {
        list_array.value_type() => (helper),
        _ => {}
    }
    None
}

/// Threshold to switch from direct iteration to using `min` / `max` kernel from
/// `arrow::compute`. The latter has enough per-invocation overhead that direct
/// iteration is faster for small lists.
const ARROW_COMPUTE_THRESHOLD: usize = 32;

/// Computes min or max for each row of a primitive ListArray.
fn primitive_array_min_max<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
    list_array: &GenericListArray<O>,
    is_min: bool,
) -> Result<ArrayRef> {
    let values_array = list_array.values().as_primitive::<T>();
    let values_slice = values_array.values();
    let values_nulls = values_array.nulls();
    let mut result_builder = PrimitiveBuilder::<T>::with_capacity(list_array.len())
        .with_data_type(values_array.data_type().clone());

    for (row, w) in list_array.offsets().windows(2).enumerate() {
        let row_result = if list_array.is_null(row) {
            None
        } else {
            let start = w[0].as_usize();
            let end = w[1].as_usize();
            let len = end - start;

            match len {
                0 => None,
                _ if len < ARROW_COMPUTE_THRESHOLD => {
                    scalar_min_max::<T>(values_slice, values_nulls, start, end, is_min)
                }
                _ => {
                    let slice = values_array.slice(start, len);
                    if is_min {
                        arrow::compute::min::<T>(&slice)
                    } else {
                        arrow::compute::max::<T>(&slice)
                    }
                }
            }
        };

        result_builder.append_option(row_result);
    }

    Ok(Arc::new(result_builder.finish()) as ArrayRef)
}

/// Computes min or max for a single list row by directly scanning a slice of
/// the flat values buffer.
#[inline]
fn scalar_min_max<T: ArrowPrimitiveType>(
    values_slice: &[T::Native],
    values_nulls: Option<&arrow::buffer::NullBuffer>,
    start: usize,
    end: usize,
    is_min: bool,
) -> Option<T::Native> {
    let mut best: Option<T::Native> = None;
    for (i, &val) in values_slice[start..end].iter().enumerate() {
        if let Some(nulls) = values_nulls
            && !nulls.is_valid(start + i)
        {
            continue;
        }
        let update_best = match best {
            None => true,
            Some(current) if is_min => val.is_lt(current),
            Some(current) => val.is_gt(current),
        };
        if update_best {
            best = Some(val);
        }
    }
    best
}
