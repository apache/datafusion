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

//! [`ScalarUDFImpl`] definitions for array_product function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, GenericListArray,
    OffsetSizeTrait, PrimitiveBuilder, downcast_primitive,
};
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
    ArrayProduct,
    array_product,
    array,
    "returns the product of all values in the array.",
    array_product_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the product of all values in the array. The return type is the same as the element type (no widening). NULL elements are skipped; an all-NULL or empty array returns NULL.",
    syntax_example = "array_product(array)",
    sql_example = r#"```sql
> select array_product([2, 3, 4]);
+-----------------------------------------+
| array_product(List([2,3,4]))            |
+-----------------------------------------+
| 24                                      |
+-----------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayProduct {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayProduct {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayProduct {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_product".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayProduct {
    fn name(&self) -> &str {
        "array_product"
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
        make_scalar_function(array_product_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_product_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("array_product", args)?;
    match array.data_type() {
        List(_) => array_product_dispatch(as_list_array(array)?),
        LargeList(_) => array_product_dispatch(as_large_list_array(array)?),
        arg_type => exec_err!("array_product does not support type: {arg_type}"),
    }
}

fn array_product_dispatch<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    if let Some(result) = try_primitive_array_product(list_array) {
        return result;
    }
    exec_err!(
        "array_product does not support element type: {}",
        list_array.value_type()
    )
}

fn try_primitive_array_product<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
) -> Option<Result<ArrayRef>> {
    // Reject Decimal types: mul_wrapping on raw scaled integers does not
    // adjust scale, producing incorrect results for Decimal128/256.
    if matches!(
        list_array.value_type(),
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) {
        return Some(exec_err!("array_product does not support Decimal types"));
    }
    macro_rules! helper {
        ($t:ty) => {
            return Some(primitive_array_product::<O, $t>(list_array))
        };
    }
    downcast_primitive! {
        list_array.value_type() => (helper),
        _ => {}
    }
    None
}

fn primitive_array_product<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
    list_array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let values_array = list_array.values().as_primitive::<T>();
    let values_slice = values_array.values();
    let values_nulls = values_array.nulls();
    let mut result_builder = PrimitiveBuilder::<T>::with_capacity(list_array.len())
        .with_data_type(values_array.data_type().clone());

    for (row, w) in list_array.offsets().windows(2).enumerate() {
        if list_array.is_null(row) {
            result_builder.append_null();
            continue;
        }

        let start = w[0].as_usize();
        let end = w[1].as_usize();

        if start == end {
            result_builder.append_null();
            continue;
        }

        let mut has_non_null = false;
        let mut product = T::Native::ONE;

        for (i, &val) in values_slice[start..end].iter().enumerate() {
            if let Some(nulls) = values_nulls
                && !nulls.is_valid(start + i)
            {
                continue;
            }
            has_non_null = true;
            product = product.mul_wrapping(val);
        }

        if has_non_null {
            result_builder.append_value(product);
        } else {
            result_builder.append_null();
        }
    }

    Ok(Arc::new(result_builder.finish()) as ArrayRef)
}
