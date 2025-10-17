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

use arrow::compute::is_not_null;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use datafusion_common::{plan_err, utils::take_function_args, Result};
use datafusion_expr::{
    type_coercion::binary::comparison_coercion, ColumnarValue, Documentation,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

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
#[derive(Debug, PartialEq, Eq, Hash)]
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        nvl2_func(&args.args, args.number_rows)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [tested, if_non_null, if_null] = take_function_args(self.name(), arg_types)?;
        // The coerced types found by `comparison_coercion` are not guaranteed to be
        // coercible for the arguments. `comparison_coercion` returns more loose
        // types that can be coerced to both `acc` and `x` for comparison purpose.
        // See `maybe_data_types` for the actual coercion.
        let coerced = if let Some(coerced) = comparison_coercion(if_non_null, if_null) {
            coerced
        } else {
            return plan_err!("Coercion from {if_non_null} to {if_null} failed.");
        };
        Ok(vec![tested.clone(), coerced.clone(), coerced])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn nvl2_func(args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
    let [tested, if_non_null, if_null] = take_function_args("nvl2", args)?;
    match (tested, if_non_null, if_null) {
        // Fast paths if all scalars
        (
            ColumnarValue::Scalar(tested),
            ColumnarValue::Scalar(_),
            ColumnarValue::Scalar(_),
        ) if tested.is_null() => Ok(if_null.clone()),
        (
            ColumnarValue::Scalar(_),
            ColumnarValue::Scalar(_),
            ColumnarValue::Scalar(_),
        ) => Ok(if_non_null.clone()),
        // Otherwise regular path
        (tested, if_non_null, if_null) => {
            let tested = tested.to_array(number_rows)?;
            let if_non_null = if_non_null.to_array(number_rows)?;
            let if_null = if_null.to_array(number_rows)?;

            let mask = is_not_null(&tested)?;
            let array = zip(&mask, &if_non_null, &if_null)?;
            Ok(ColumnarValue::Array(array))
        }
    }
}
