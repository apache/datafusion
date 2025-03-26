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

//! [`ScalarUDFImpl`] definitions for array_max function.
use crate::utils::make_scalar_function;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::List;
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_aggregate::min_max;
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::any::Any;

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
#[derive(Debug)]
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

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        match &arg_types[0] {
            List(field) => Ok(field.data_type().clone()),
            _ => exec_err!("Not reachable, data_type should be List"),
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function(array_max_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_max SQL function
///
/// There is one argument for array_max as the array.
/// `array_max(array)`
///
/// For example:
/// > array_max(\[1, 3, 2]) -> 3
pub fn array_max_inner(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    let [arg1] = take_function_args("array_max", args)?;

    match arg1.data_type() {
        List(_) => {
            let input_list_array = as_list_array(&arg1)?;
            let result_vec = input_list_array
                .iter()
                .flat_map(|arr| min_max::max_batch(&arr.unwrap()))
                .collect_vec();
            ScalarValue::iter_to_array(result_vec)
        }
        _ => exec_err!("array_max does not support type: {:?}", arg1.data_type()),
    }
}
