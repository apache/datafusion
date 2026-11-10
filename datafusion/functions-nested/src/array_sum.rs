// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`ScalarUDFImpl`] definitions for array_sum function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, ArrowNumericType, GenericListArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{
    DataType,
    DataType::{
        Decimal32, Decimal64, Decimal128, Decimal256, FixedSizeList, Float64, Int8,
        Int16, Int32, Int64, LargeList, List, Null, UInt8, UInt16, UInt32, UInt64,
    },
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Field, Float64Type,
    Int64Type, UInt64Type,
};
use datafusion_common::cast::{as_large_list_array, as_list_array, as_primitive_array};
use datafusion_common::utils::{
    ListCoercion, base_type, coerced_type_with_base_type_only,
};
use datafusion_common::{Result, internal_err, plan_err, utils::take_function_args};
use datafusion_expr::{
    AggregateUDFImpl, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_functions_aggregate::sum::Sum;
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
    description = "Returns the sum of the elements of the input array, computed as `array[0] + array[1] + ...`. NULL elements are skipped (per SQL aggregate convention). Returns NULL if the input row is NULL, every element is NULL, or the array is empty. The result type follows the `sum` aggregate function: `Int64` for signed integer arrays, `UInt64` for unsigned integer arrays, and a decimal with 10 more digits of precision (up to the maximum) for decimal arrays. Integer sums wrap on overflow, as `sum` does. Other arrays are summed as `Float64`.",
    syntax_example = "array_sum(array)",
    sql_example = r#"```sql
> select array_sum([1.0, 2.0, 3.0]);
+--------------------------------+
| array_sum(List([1.0,2.0,3.0])) |
+--------------------------------+
| 6.0                            |
+--------------------------------+
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = take_function_args(self.name(), arg_types)?;
        let element_type = sum_element_type(&base_type(arg_type));
        Sum::new().return_type(&[element_type])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg_type] = take_function_args(self.name(), arg_types)?;
        let coercion = Some(&ListCoercion::FixedSizedListToList);

        if !matches!(arg_type, Null | List(_) | LargeList(_) | FixedSizeList(..)) {
            return plan_err!("{} does not support type {arg_type}", self.name());
        }

        let coerced = if matches!(arg_type, Null) {
            List(Arc::new(Field::new_list_field(Float64, true)))
        } else {
            let element_type = sum_element_type(&base_type(arg_type));
            coerced_type_with_base_type_only(arg_type, &element_type, coercion)
        };

        Ok(vec![coerced])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_type().clone();
        make_scalar_function(|arrays: &[ArrayRef]| array_sum_inner(arrays, &return_type))(
            &args.args,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns the element type `array_sum` accumulates in. As in the `sum`
/// aggregate, integers widen to 64 bits and decimals are kept; any other
/// element type is cast to `Float64`.
fn sum_element_type(element_type: &DataType) -> DataType {
    match element_type {
        Int8 | Int16 | Int32 | Int64 => Int64,
        UInt8 | UInt16 | UInt32 | UInt64 => UInt64,
        Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) => {
            element_type.clone()
        }
        _ => Float64,
    }
}

fn array_sum_inner(args: &[ArrayRef], return_type: &DataType) -> Result<ArrayRef> {
    let [array] = take_function_args("array_sum", args)?;
    match array.data_type() {
        List(_) => general_array_sum(as_list_array(array)?, return_type),
        LargeList(_) => general_array_sum(as_large_list_array(array)?, return_type),
        arg_type => {
            internal_err!("array_sum received unexpected type after coercion: {arg_type}")
        }
    }
}

fn general_array_sum<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    return_type: &DataType,
) -> Result<ArrayRef> {
    match return_type {
        Int64 => sum_rows::<O, Int64Type>(list_array, return_type),
        UInt64 => sum_rows::<O, UInt64Type>(list_array, return_type),
        Float64 => sum_rows::<O, Float64Type>(list_array, return_type),
        Decimal32(..) => sum_rows::<O, Decimal32Type>(list_array, return_type),
        Decimal64(..) => sum_rows::<O, Decimal64Type>(list_array, return_type),
        Decimal128(..) => sum_rows::<O, Decimal128Type>(list_array, return_type),
        Decimal256(..) => sum_rows::<O, Decimal256Type>(list_array, return_type),
        other => internal_err!("array_sum received unexpected return type: {other}"),
    }
}

/// Sums each row with [`arrow::compute::sum`], the kernel the `sum` aggregate
/// uses: NULL elements are skipped, an empty or all-NULL row yields NULL, and
/// integer overflow wraps.
fn sum_rows<O: OffsetSizeTrait, T: ArrowNumericType>(
    list_array: &GenericListArray<O>,
    return_type: &DataType,
) -> Result<ArrayRef> {
    let values = as_primitive_array::<T>(list_array.values())?;
    let sums: PrimitiveArray<T> = list_array
        .value_offsets()
        .windows(2)
        .enumerate()
        .map(|(row, offsets)| {
            if list_array.is_null(row) {
                return None;
            }
            let start = offsets[0].as_usize();
            let len = offsets[1].as_usize() - start;
            arrow::compute::sum(&values.slice(start, len))
        })
        .collect();
    // Decimal results carry the widened precision from `return_type`
    Ok(Arc::new(sums.with_data_type(return_type.clone())))
}
