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

use crate::utils::take_function_args;
use arrow::array::Array;
use arrow::compute::is_not_null;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use datafusion_common::{internal_err, Result};
use datafusion_expr::{
    type_coercion::binary::comparison_coercion, ColumnarValue, Documentation,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns _expression2_ if _expression1_ is not NULL; otherwise it returns _expression3_.",
    syntax_example = "nvl2(expression1, expression2, expression3)",
    sql_example = r#"```sql
> select nvl2(null, 'a', 'b');
+--------------------------------+
| nvl2(NULL,Utf8("a"),Utf8("b")) |
+--------------------------------+
| b                              |
+--------------------------------+
> select nvl2('data', 'a', 'b');
+----------------------------------------+
| nvl2(Utf8("data"),Utf8("a"),Utf8("b")) |
+----------------------------------------+
| a                                      |
+----------------------------------------+
```
"#,
    argument(
        name = "expression1",
        description = "Expression to test for null. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "expression2",
        description = "Expression to return if expr1 is not null. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "expression3",
        description = "Expression to return if expr1 is null. Can be a constant, column, or function, and any combination of operators."
    )
)]
#[derive(Debug)]
pub struct NVL2Func {
    signature: Signature,
}

impl Default for NVL2Func {
    fn default() -> Self {
        Self::new()
    }
}

impl NVL2Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NVL2Func {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nvl2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        nvl2_func(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [tested, if_non_null, if_null] = take_function_args(self.name(), arg_types)?;
        let new_type =
            [if_non_null, if_null]
                .iter()
                .try_fold(tested.clone(), |acc, x| {
                    // The coerced types found by `comparison_coercion` are not guaranteed to be
                    // coercible for the arguments. `comparison_coercion` returns more loose
                    // types that can be coerced to both `acc` and `x` for comparison purpose.
                    // See `maybe_data_types` for the actual coercion.
                    let coerced_type = comparison_coercion(&acc, x);
                    if let Some(coerced_type) = coerced_type {
                        Ok(coerced_type)
                    } else {
                        internal_err!("Coercion from {acc:?} to {x:?} failed.")
                    }
                })?;
        Ok(vec![new_type; arg_types.len()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn nvl2_func(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let mut len = 1;
    let mut is_array = false;
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            len = array.len();
            is_array = true;
            break;
        }
    }
    if is_array {
        let args = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len),
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
            })
            .collect::<Result<Vec<_>>>()?;
        let [tested, if_non_null, if_null] = take_function_args("nvl2", args)?;
        let to_apply = is_not_null(&tested)?;
        let value = zip(&to_apply, &if_non_null, &if_null)?;
        Ok(ColumnarValue::Array(value))
    } else {
        let [tested, if_non_null, if_null] = take_function_args("nvl2", args)?;
        match &tested {
            ColumnarValue::Array(_) => {
                internal_err!("except Scalar value, but got Array")
            }
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    Ok(if_null.clone())
                } else {
                    Ok(if_non_null.clone())
                }
            }
        }
    }
}
