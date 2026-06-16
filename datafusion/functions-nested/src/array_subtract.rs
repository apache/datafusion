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

//! [`ScalarUDFImpl`] definitions for array_subtract function.

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
    ArraySubtract,
    array_subtract,
    array1 array2,
    "returns the element-wise difference of two numeric arrays.",
    array_subtract_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the element-wise difference of two numeric arrays of equal length, computed as `array1[i] - array2[i]` per position. NULL is propagated per element: if either input element at position `i` is NULL, the corresponding output element is NULL (positions are preserved). Returns NULL if either entire input array is NULL. Errors if the per-row lengths differ. Returns an empty array if both inputs are empty.",
    syntax_example = "array_subtract(array1, array2)",
    sql_example = r#"```sql
> select array_subtract([10.0, 20.0, 30.0], [1.0, 2.0, 3.0]);
+--------------------------------------------------------------+
| array_subtract(List([10.0,20.0,30.0]),List([1.0,2.0,3.0]))   |
+--------------------------------------------------------------+
| [9.0, 18.0, 27.0]                                            |
+--------------------------------------------------------------+
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
pub struct ArraySubtract {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraySubtract {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraySubtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_subtract".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArraySubtract {
    fn name(&self) -> &str {
        "array_subtract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, _] = take_function_args(self.name(), arg_types)?;
        coerce_array_math_arg_types(self.name(), arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_subtract_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_subtract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_subtract", args)?;
    let sub = |a: f64, b: f64| a - b;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => {
            array_math_binary_op::<i32, _>("array_subtract", array1, array2, sub)
        }
        (LargeList(_), LargeList(_)) => {
            array_math_binary_op::<i64, _>("array_subtract", array1, array2, sub)
        }
        (arg_type1, arg_type2) => exec_err!(
            "array_subtract received unexpected types after coercion: {arg_type1} and {arg_type2}"
        ),
    }
}
