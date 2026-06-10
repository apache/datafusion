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
use arrow::array::{Array, ArrayRef, Float64Array, OffsetSizeTrait};
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, Null},
    Field,
};
use datafusion_common::cast::{as_float64_array, as_generic_list_array};
use datafusion_common::utils::{ListCoercion, coerced_type_with_base_type_only};
use datafusion_common::{Result, internal_err, plan_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArraySum,
    array_sum,
    array,
    "returns the sum of elements in a numeric array.",
    array_sum_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the sum of the elements of the input array, computed as `array[0] + array[1] + ...`. NULL elements are skipped (per SQL aggregate convention). Returns NULL if the input row is NULL, every element is NULL, or the array is empty.",
    syntax_example = "array_sum(array)",
    sql_example = r#"```sql
> select array_sum([1.0, 2.0, 3.0]);
+----------------------------+
| array_sum(List([1.0,2.0,3.0])) |
+----------------------------+
| 6.0                        |
+----------------------------+
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
            signature: Signature::user_defined(Volatility::Immutable),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg_type] = take_function_args(self.name(), arg_types)?;
        let coercion = Some(&ListCoercion::FixedSizedListToList);

        if !matches!(arg_type, Null | List(_) | LargeList(_) | FixedSizeList(..)) {
            return plan_err!("{} does not support type {arg_type}", self.name());
        }

        let coerced = if matches!(arg_type, Null) {
            List(Arc::new(Field::new_list_field(DataType::Float64, true)))
        } else {
            coerced_type_with_base_type_only(arg_type, &DataType::Float64, coercion)
        };

        Ok(vec![coerced])
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
        List(_) => general_array_sum::<i32>(array),
        LargeList(_) => general_array_sum::<i64>(array),
        arg_type => {
            internal_err!("array_sum received unexpected type after coercion: {arg_type}")
        }
    }
}

fn general_array_sum<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(array)?;
    let values = as_float64_array(list_array.values())?;
    let offsets = list_array.value_offsets();

    let mut builder = Float64Array::builder(list_array.len());

    for row in 0..list_array.len() {
        if list_array.is_null(row) {
            builder.append_null();
            continue;
        }

        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();

        // Skip NULL elements per SQL aggregate convention (matches PostgreSQL
        // array_sum, DuckDB list_sum, Spark aggregate). Empty arrays and
        // all-NULL arrays both yield NULL — same behavior as SQL SUM over
        // an empty set or all-NULL column.
        let mut sum = 0.0_f64;
        let mut any_valid = false;
        for i in start..end {
            if values.is_valid(i) {
                sum += values.value(i);
                any_valid = true;
            }
        }

        if any_valid {
            builder.append_value(sum);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}
