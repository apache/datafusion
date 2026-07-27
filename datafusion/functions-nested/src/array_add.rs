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

use crate::utils::{coerce_array_math_arg_types, make_scalar_function};
use arrow::array::{
    Array, ArrayRef, Float64Array, GenericListArray, NullBufferBuilder, OffsetSizeTrait,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{
    DataType,
    DataType::{LargeList, List},
    Field,
};
use datafusion_common::cast::{as_float64_array, as_generic_list_array};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

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
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => general_array_add::<i32>(array1, array2),
        (LargeList(_), LargeList(_)) => general_array_add::<i64>(array1, array2),
        (arg_type1, arg_type2) => exec_err!(
            "array_add received unexpected types after coercion: {arg_type1} and {arg_type2}"
        ),
    }
}

fn general_array_add<O: OffsetSizeTrait>(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
) -> Result<ArrayRef> {
    let lhs = as_generic_list_array::<O>(lhs)?;
    let rhs = as_generic_list_array::<O>(rhs)?;

    let lhs_values = as_float64_array(lhs.values())?;
    let rhs_values = as_float64_array(rhs.values())?;
    let lhs_offsets = lhs.value_offsets();
    let rhs_offsets = rhs.value_offsets();

    // Row-level validity: a row is valid iff both sides are valid at that row.
    let row_nulls = NullBuffer::union(lhs.nulls(), rhs.nulls());

    let mut out_values: Vec<f64> = Vec::with_capacity(lhs_values.len());
    let mut out_inner_nulls = NullBufferBuilder::new(lhs_values.len());
    let mut out_offsets = Vec::<O>::with_capacity(lhs.len() + 1);
    out_offsets.push(O::zero());

    for row in 0..lhs.len() {
        // Whole-row NULL on either side -> NULL output row, no elements.
        if row_nulls.as_ref().is_some_and(|nb| nb.is_null(row)) {
            out_offsets.push(out_offsets[row]);
            continue;
        }

        let start1 = lhs_offsets[row].as_usize();
        let len1 = lhs.value_length(row).as_usize();
        let start2 = rhs_offsets[row].as_usize();
        let len2 = rhs.value_length(row).as_usize();

        if len1 != len2 {
            return exec_err!(
                "array_add requires both list inputs to have the same length per row, got {len1} and {len2} at row {row}"
            );
        }

        let l_slice = lhs_values.slice(start1, len1);
        let r_slice = rhs_values.slice(start2, len2);

        let l_vals = l_slice.values();
        let r_vals = r_slice.values();

        for i in 0..len1 {
            out_values.push(l_vals[i] + r_vals[i]);
        }

        // Per-element validity: position `i` is valid iff both lhs[i] and rhs[i]
        // are valid. `NullBuffer::union` returns `None` when both sides are
        // entirely valid.
        match NullBuffer::union(l_slice.nulls(), r_slice.nulls()) {
            Some(nb) => out_inner_nulls.append_buffer(&nb),
            None => out_inner_nulls.append_n_non_nulls(len1),
        }

        out_offsets.push(out_offsets[row] + O::usize_as(len1));
    }

    let values_array = Arc::new(Float64Array::new(
        out_values.into(),
        out_inner_nulls.finish(),
    ));
    let field = Arc::new(Field::new_list_field(DataType::Float64, true));

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(out_offsets.into()),
        values_array,
        row_nulls,
    )?))
}
