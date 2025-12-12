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
use arrow::array::{ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeList, List};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::Result;
use datafusion_common::{exec_err, plan_err, ScalarValue};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions_aggregate_common::min_max::{max_batch, min_batch};
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
        List(_) => array_min_max_helper(as_list_array(array)?, max_batch),
        LargeList(_) => array_min_max_helper(as_large_list_array(array)?, max_batch),
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
        List(_) => array_min_max_helper(as_list_array(array)?, min_batch),
        LargeList(_) => array_min_max_helper(as_large_list_array(array)?, min_batch),
        arg_type => exec_err!("array_min does not support type: {arg_type}"),
    }
}

fn array_min_max_helper<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    agg_fn: fn(&ArrayRef) -> Result<ScalarValue>,
) -> Result<ArrayRef> {
    let null_value = ScalarValue::try_from(array.value_type())?;
    let result_vec: Vec<ScalarValue> = array
        .iter()
        .map(|arr| arr.as_ref().map_or_else(|| Ok(null_value.clone()), agg_fn))
        .try_collect()?;
    ScalarValue::iter_to_array(result_vec)
}
