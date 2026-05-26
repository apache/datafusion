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

//! [`ScalarUDFImpl`] definitions for array_scale function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Float64Array, GenericListArray, OffsetBufferBuilder, OffsetSizeTrait,
};
use arrow::buffer::NullBuffer;
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
    ArrayScale,
    array_scale,
    array scalar,
    "scales each element of a numeric array by a scalar.",
    array_scale_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns a new array with each element of the input array multiplied by a scalar value, computed as `array[i] * scalar`. Returns NULL if the input row is NULL or the scalar is NULL. If a NULL element appears in the input array at position `i`, the result element at position `i` is NULL. Returns an empty array for an empty input array.",
    syntax_example = "array_scale(array, scalar)",
    sql_example = r#"```sql
> select array_scale([1.0, 2.0, 3.0], 2.0);
+----------------------------------+
| array_scale(List([1.0,2.0,3.0]),Float64(2.0)) |
+----------------------------------+
| [2.0, 4.0, 6.0]                  |
+----------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "scalar",
        description = "Numeric scalar to multiply each element by. Can be a constant or column expression."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayScale {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayScale {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayScale {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_scale".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayScale {
    fn name(&self) -> &str {
        "array_scale"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // After `coerce_types`, `arg_types[0]` is one of List(Float64) or LargeList(Float64).
        Ok(arg_types[0].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [array_type, scalar_type] = take_function_args(self.name(), arg_types)?;
        let coercion = Some(&ListCoercion::FixedSizedListToList);

        if !matches!(
            array_type,
            Null | List(_) | LargeList(_) | FixedSizeList(..)
        ) {
            return plan_err!(
                "{} first argument must be a list type, got {array_type}",
                self.name()
            );
        }

        if !scalar_type.is_numeric() && !matches!(scalar_type, Null) {
            return plan_err!(
                "{} second argument must be numeric, got {scalar_type}",
                self.name()
            );
        }

        let coerced_array = if matches!(array_type, Null) {
            List(Arc::new(Field::new_list_field(DataType::Float64, true)))
        } else {
            coerced_type_with_base_type_only(array_type, &DataType::Float64, coercion)
        };

        Ok(vec![coerced_array, DataType::Float64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_scale_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_scale_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, scalar] = take_function_args("array_scale", args)?;
    match array.data_type() {
        List(_) => general_array_scale::<i32>(array, scalar),
        LargeList(_) => general_array_scale::<i64>(array, scalar),
        arg_type => internal_err!(
            "array_scale received unexpected type after coercion: {arg_type}"
        ),
    }
}

fn general_array_scale<O: OffsetSizeTrait>(
    array: &ArrayRef,
    scalar: &ArrayRef,
) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(array)?;
    let scalar_array = as_float64_array(scalar)?;

    let values = as_float64_array(list_array.values())?;
    let offsets = list_array.value_offsets();

    // A row is null whenever either input row is null. The scalar applies
    // uniformly across the array, so a null scalar makes the whole row
    // undefined; union the two row-level null buffers in a single pass
    // rather than tracking row nulls inside the value loop.
    let row_nulls = NullBuffer::union(list_array.nulls(), scalar_array.nulls());

    let mut value_builder = Float64Array::builder(values.len());
    let mut new_offsets = OffsetBufferBuilder::<O>::new(list_array.len());

    for row in 0..list_array.len() {
        if row_nulls.as_ref().is_some_and(|nb| nb.is_null(row)) {
            new_offsets.push_length(0);
            continue;
        }

        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let len = end - start;
        let scalar_val = scalar_array.value(row);

        let slice = values.slice(start, len);

        // Per-element NULL propagation for NULL elements inside the array.
        for i in 0..len {
            if slice.is_null(i) {
                value_builder.append_null();
            } else {
                value_builder.append_value(slice.value(i) * scalar_val);
            }
        }

        new_offsets.push_length(len);
    }

    let values_array = Arc::new(value_builder.finish());

    // Preserve the inner field from the input array (including any user
    // metadata). After `coerce_types` the inner type is Float64, but the
    // input may still carry field-level annotations worth keeping.
    let field = match list_array.data_type() {
        List(f) | LargeList(f) => Arc::clone(f),
        other => {
            return internal_err!("array_scale unexpected list type: {other}");
        }
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        new_offsets.finish(),
        values_array,
        row_nulls,
    )?))
}
