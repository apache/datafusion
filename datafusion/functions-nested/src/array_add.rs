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

//! [`ScalarUDFImpl`] definitions for array_add function.

use crate::utils::{
    array_math_binary_op, coerce_array_math_arg_types, make_scalar_function,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{
    DataType,
    DataType::{LargeList, List},
};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

make_udf_expr_and_func!(
    ArrayAdd,
    array_add,
    array1 array2,
    "returns the element-wise sum of two numeric arrays.",
    array_add_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the element-wise sum of two numeric arrays of equal length, computed as `array1[i] + array2[i]` per position. NULL is propagated per element: if either input element at position `i` is NULL, the corresponding output element is NULL (positions are preserved). Returns NULL if either entire input array is NULL. Errors if the per-row lengths differ. Returns an empty array if both inputs are empty.",
    syntax_example = "array_add(array1, array2)",
    sql_example = r#"```sql
> select array_add([1.0, 2.0, 3.0], [10.0, 20.0, 30.0]);
+---------------------------------------------------------+
| array_add(List([1.0,2.0,3.0]),List([10.0,20.0,30.0]))   |
+---------------------------------------------------------+
| [11.0, 22.0, 33.0]                                      |
+---------------------------------------------------------+
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
pub struct ArrayAdd {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAdd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_add".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayAdd {
    fn name(&self) -> &str {
        "array_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // After `coerce_types`, both args share the same List/LargeList<Float64> shape.
        Ok(arg_types[0].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, _] = take_function_args(self.name(), arg_types)?;
        coerce_array_math_arg_types(self.name(), arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_add_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_add_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_add", args)?;
    let add = |a: f64, b: f64| a + b;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => {
            array_math_binary_op::<i32, _>("array_add", array1, array2, add)
        }
        (LargeList(_), LargeList(_)) => {
            array_math_binary_op::<i64, _>("array_add", array1, array2, add)
        }
        (arg_type1, arg_type2) => exec_err!(
            "array_add received unexpected types after coercion: {arg_type1} and {arg_type2}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_sliced_capacity() -> Result<()> {
        crate::utils::tests::check_sliced_list_behavior(|input| {
            array_add_inner(&[Arc::clone(input), Arc::clone(input)])
        })
    }
}
