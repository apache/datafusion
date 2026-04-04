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

//! [`ScalarUDFImpl`] definitions for array_sum function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, GenericListArray,
    OffsetSizeTrait, PrimitiveBuilder, downcast_primitive,
};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeList, List};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArraySum,
    array_sum,
    array,
    "returns the sum of all values in the array.",
    array_sum_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the sum of all values in the array. The return type is the same as the element type (no widening). NULL elements are skipped; an all-NULL or empty array returns NULL.",
    syntax_example = "array_sum(array)",
    sql_example = r#"```sql
> select array_sum([1, 2, 3, 4]);
+--------------------------------------+
| array_sum(List([1,2,3,4]))           |
+--------------------------------------+
| 10                                   |
+--------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraySum {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraySum {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySum {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_sum".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySum {
    fn name(&self) -> &str {
        "array_sum"
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
        make_scalar_function(array_sum_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_sum_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_sum", args)?;
    match array.data_type() {
        List(_) => array_sum_dispatch(as_list_array(array)?),
        LargeList(_) => array_sum_dispatch(as_large_list_array(array)?),
        arg_type => exec_err!("array_sum does not support type: {arg_type}"),
    }
}

fn array_sum_dispatch<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    if let Some(result) = try_primitive_array_sum(list_array) {
        return result;
    }
    exec_err!(
        "array_sum does not support element type: {}",
        list_array.value_type()
    )
}

/// Dispatches to a typed primitive sum implementation.
fn try_primitive_array_sum<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
) -> Option<Result<ArrayRef>> {
    macro_rules! helper {
        ($t:ty) => {
            return Some(primitive_array_sum::<O, $t>(list_array))
        };
    }
    downcast_primitive! {
        list_array.value_type() => (helper),
        _ => {}
    }
    None
}

/// Computes the sum for each row of a primitive ListArray.
fn primitive_array_sum<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
    list_array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let values_array = list_array.values().as_primitive::<T>();
    let values_slice = values_array.values();
    let values_nulls = values_array.nulls();
    let mut result_builder = PrimitiveBuilder::<T>::with_capacity(list_array.len())
        .with_data_type(values_array.data_type().clone());

    for (row, w) in list_array.offsets().windows(2).enumerate() {
        if list_array.is_null(row) {
            result_builder.append_null();
            continue;
        }

        let start = w[0].as_usize();
        let end = w[1].as_usize();

        if start == end {
            // Empty array — return NULL (consistent with SQL SUM over empty set)
            result_builder.append_null();
            continue;
        }

        let mut has_non_null = false;
        let mut sum = T::Native::default();

        for (i, &val) in values_slice[start..end].iter().enumerate() {
            if let Some(nulls) = values_nulls
                && !nulls.is_valid(start + i)
            {
                continue;
            }
            has_non_null = true;
            sum = sum.add_wrapping(val);
        }

        if has_non_null {
            result_builder.append_value(sum);
        } else {
            result_builder.append_null();
        }
    }

    Ok(Arc::new(result_builder.finish()) as ArrayRef)
}
