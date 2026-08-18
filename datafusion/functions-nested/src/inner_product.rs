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

//! [`ScalarUDFImpl`] definitions for inner_product function.

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, Float64Array, OffsetSizeTrait};
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, Null},
    Field,
};
use datafusion_common::cast::{as_float64_array, as_generic_list_array};
use datafusion_common::utils::{ListCoercion, coerced_type_with_base_type_only};
use datafusion_common::{
    Result, exec_err, internal_err, plan_err, utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    InnerProduct,
    inner_product,
    array1 array2,
    "returns the inner product (dot product) of two numeric arrays.",
    inner_product_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the inner product (dot product) of two input arrays of equal length, computed as `sum(array1[i] * array2[i])`. Returns NULL if either array is NULL or contains NULL elements. Returns 0.0 for two empty arrays.",
    syntax_example = "inner_product(array1, array2)",
    sql_example = r#"```sql
> select inner_product([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]);
+-------------------------------------------------------+
| inner_product(List([1.0,2.0,3.0]),List([4.0,5.0,6.0])) |
+-------------------------------------------------------+
| 32.0                                                  |
+-------------------------------------------------------+
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
pub struct InnerProduct {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for InnerProduct {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerProduct {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["dot_product".to_string()],
        }
    }
}

impl ScalarUDFImpl for InnerProduct {
    fn name(&self) -> &str {
        "inner_product"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [_, _] = take_function_args(self.name(), arg_types)?;
        let coercion = Some(&ListCoercion::FixedSizedListToList);

        for arg_type in arg_types {
            if !matches!(arg_type, Null | List(_) | LargeList(_) | FixedSizeList(..)) {
                return plan_err!("{} does not support type {arg_type}", self.name());
            }
        }

        // If any input is `LargeList`, both sides must be widened to `LargeList`
        // so the runtime dispatch in `inner_product_inner` sees a homogeneous
        // pair. Follows the pattern in `ArrayConcat::coerce_types`.
        let any_large_list = arg_types.iter().any(|t| matches!(t, LargeList(_)));

        let coerced = arg_types
            .iter()
            .map(|arg_type| {
                if matches!(arg_type, Null) {
                    let field = Arc::new(Field::new_list_field(DataType::Float64, true));
                    return if any_large_list {
                        LargeList(field)
                    } else {
                        List(field)
                    };
                }
                let coerced = coerced_type_with_base_type_only(
                    arg_type,
                    &DataType::Float64,
                    coercion,
                );
                match coerced {
                    List(field) if any_large_list => LargeList(field),
                    other => other,
                }
            })
            .collect();

        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(inner_product_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn inner_product_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("inner_product", args)?;
    match (array1.data_type(), array2.data_type()) {
        (List(_), List(_)) => general_inner_product::<i32>(args),
        (LargeList(_), LargeList(_)) => general_inner_product::<i64>(args),
        (arg_type1, arg_type2) => internal_err!(
            "inner_product received unexpected types after coercion: {arg_type1} and {arg_type2}"
        ),
    }
}

fn general_inner_product<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let values1 = as_float64_array(list_array1.values())?;
    let values2 = as_float64_array(list_array2.values())?;
    let offsets1 = list_array1.value_offsets();
    let offsets2 = list_array2.value_offsets();

    let mut builder = Float64Array::builder(list_array1.len());
    for row in 0..list_array1.len() {
        if list_array1.is_null(row) || list_array2.is_null(row) {
            builder.append_null();
            continue;
        }

        let start1 = offsets1[row].as_usize();
        let end1 = offsets1[row + 1].as_usize();
        let start2 = offsets2[row].as_usize();
        let end2 = offsets2[row + 1].as_usize();
        let len1 = end1 - start1;
        let len2 = end2 - start2;

        if len1 != len2 {
            return exec_err!(
                "inner_product requires both list inputs to have the same length, got {len1} and {len2}"
            );
        }

        let slice1 = values1.slice(start1, len1);
        let slice2 = values2.slice(start2, len2);
        if slice1.null_count() != 0 || slice2.null_count() != 0 {
            builder.append_null();
            continue;
        }

        let vals1 = slice1.values();
        let vals2 = slice2.values();

        let mut dot = 0.0;
        for i in 0..len1 {
            dot += vals1[i] * vals2[i];
        }
        builder.append_value(dot);
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}
