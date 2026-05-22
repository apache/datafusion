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

//! [`ScalarUDFImpl`] definitions for array_normalize function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Float64Array, GenericListArray, NullBufferBuilder,
    OffsetBufferBuilder, OffsetSizeTrait,
};
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
    ArrayNormalize,
    array_normalize,
    array,
    "returns the L2-normalized vector for a numeric array.",
    array_normalize_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the L2-normalized vector for the input numeric array, computed as `array[i] / sqrt(sum(array[i]^2))` per element. Returns NULL if the input is NULL, contains NULL elements, or has zero magnitude (all elements are zero). Returns an empty array for an empty input array.",
    syntax_example = "array_normalize(array)",
    sql_example = r#"```sql
> select array_normalize([3.0, 4.0]);
+-----------------------------+
| array_normalize(List([3.0,4.0])) |
+-----------------------------+
| [0.6, 0.8]                  |
+-----------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayNormalize {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayNormalize {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayNormalize {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_normalize".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayNormalize {
    fn name(&self) -> &str {
        "array_normalize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // After `coerce_types`, `arg_types[0]` is one of List(Float64) or LargeList(Float64).
        Ok(arg_types[0].clone())
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
        make_scalar_function(array_normalize_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_normalize_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_normalize", args)?;
    match array.data_type() {
        List(_) => general_array_normalize::<i32>(args),
        LargeList(_) => general_array_normalize::<i64>(args),
        arg_type => internal_err!(
            "array_normalize received unexpected type after coercion: {arg_type}"
        ),
    }
}

fn general_array_normalize<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&arrays[0])?;
    let values = as_float64_array(list_array.values())?;
    let offsets = list_array.value_offsets();

    let mut new_values: Vec<f64> = Vec::with_capacity(values.len());
    let mut new_offsets = OffsetBufferBuilder::<O>::new(list_array.len());
    let mut nulls = NullBufferBuilder::new(list_array.len());

    for row in 0..list_array.len() {
        if list_array.is_null(row) {
            nulls.append_null();
            new_offsets.push_length(0);
            continue;
        }

        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let len = end - start;

        let slice = values.slice(start, len);
        if slice.null_count() != 0 {
            nulls.append_null();
            new_offsets.push_length(0);
            continue;
        }

        let vals = slice.values();

        // Empty array: return empty array (no normalization needed, no division by zero risk)
        if len == 0 {
            nulls.append_non_null();
            new_offsets.push_length(0);
            continue;
        }

        // Compute squared magnitude.
        let mut sq_sum = 0.0;
        for i in 0..len {
            sq_sum += vals[i] * vals[i];
        }

        // Zero magnitude: undefined normalization. Emit NULL row.
        if sq_sum == 0.0 {
            nulls.append_null();
            new_offsets.push_length(0);
            continue;
        }

        let mag = sq_sum.sqrt();
        for i in 0..len {
            new_values.push(vals[i] / mag);
        }
        nulls.append_non_null();
        new_offsets.push_length(len);
    }

    let values_array = Arc::new(Float64Array::from(new_values));
    let field = Arc::new(Field::new_list_field(DataType::Float64, true));

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        new_offsets.finish(),
        values_array,
        nulls.finish(),
    )?))
}
