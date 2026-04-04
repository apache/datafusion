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

//! [`ScalarUDFImpl`] definitions for array_avg function.

use crate::utils::make_scalar_function;
use crate::vector_math::convert_to_f64_array;
use arrow::array::{ArrayRef, Float64Array, GenericListArray, OffsetSizeTrait};
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
    ArrayAvg,
    array_avg,
    array,
    "returns the average of all values in the array.",
    array_avg_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the average (arithmetic mean) of all numeric values in the array. Always returns Float64. Supports integer and floating-point element types. NULL elements are skipped; an all-NULL or empty array returns NULL.",
    syntax_example = "array_avg(array)",
    sql_example = r#"```sql
> select array_avg([1, 2, 3, 4]);
+--------------------------------------+
| array_avg(List([1,2,3,4]))           |
+--------------------------------------+
| 2.5                                  |
+--------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAvg {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayAvg {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAvg {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_avg".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayAvg {
    fn name(&self) -> &str {
        "array_avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [array] = take_function_args(self.name(), arg_types)?;
        match array {
            List(_) | LargeList(_) => Ok(DataType::Float64),
            arg_type => plan_err!("{} does not support type {arg_type}", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_avg_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_avg_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_avg", args)?;
    match array.data_type() {
        List(_) => array_avg_dispatch(as_list_array(array)?),
        LargeList(_) => array_avg_dispatch(as_large_list_array(array)?),
        arg_type => exec_err!("array_avg does not support type: {arg_type}"),
    }
}

fn array_avg_dispatch<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let result = list_array
        .iter()
        .map(compute_array_avg)
        .collect::<Result<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

fn compute_array_avg(arr: Option<ArrayRef>) -> Result<Option<f64>> {
    let value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };

    let values = convert_to_f64_array(&value)?;

    if values.is_empty() {
        return Ok(None);
    }

    let mut sum = 0.0;
    let mut count = 0u64;

    for val in values.iter().flatten() {
        sum += val;
        count += 1;
    }

    if count == 0 {
        Ok(None)
    } else {
        Ok(Some(sum / count as f64))
    }
}
