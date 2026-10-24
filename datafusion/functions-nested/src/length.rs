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

//! [`ScalarUDFImpl`] definitions for array_length function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int64Array, LargeListArray, ListArray,
    UInt64Array,
};
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, UInt64},
};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ExpressionPlacement, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::downcast_arg;
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayLength,
    array_length,
    array,
    "returns the length of the array dimension.",
    array_length_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the length of the array dimension.",
    syntax_example = "array_length(array[, dimension])",
    sql_example = r#"```sql
> select array_length([1, 2, 3, 4, 5], 1);
+-------------------------------------------+
| array_length(List([1,2,3,4,5]), 1)        |
+-------------------------------------------+
| 5                                         |
+-------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "dimension", description = "Array dimension. Default is 1")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayLength {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayLength {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayLength {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: None,
                    }),
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Index,
                        ],
                        array_coercion: None,
                    }),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("list_length")],
        }
    }
}

impl ScalarUDFImpl for ArrayLength {
    fn name(&self) -> &str {
        "array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // An explicit scalar dimension of one uses the same fast path as an
        // omitted dimension.
        let args = match args.args.as_slice() {
            [_, ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))] => &args.args[..1],
            args => args,
        };
        make_scalar_function(array_length_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        if args[0].should_push_to_leaves() {
            ExpressionPlacement::MoveTowardsLeafNodes
        } else {
            ExpressionPlacement::KeepInPlace
        }
    }
}

fn array_length_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args {
        [array] => first_dimension_length(array),
        [array, dimension] => nth_dimension_length(array, as_int64_array(dimension)?),
        _ => exec_err!("array_length expects one or two arguments"),
    }
}

/// Returns each row's length along the first dimension.
///
/// The first dimension counts a row's immediate elements, whatever their
/// type, so the lengths come straight from the offsets or the fixed width
/// without slicing out any row.
fn first_dimension_length(array: &ArrayRef) -> Result<ArrayRef> {
    let lengths: Vec<u64> = match array.data_type() {
        List(_) => as_list_array(array)?
            .offsets()
            .lengths()
            .map(|len| len as u64)
            .collect(),
        LargeList(_) => as_large_list_array(array)?
            .offsets()
            .lengths()
            .map(|len| len as u64)
            .collect(),
        FixedSizeList(_, size) => vec![*size as u64; array.len()],
        array_type => {
            return exec_err!("array_length does not support type '{array_type}'");
        }
    };
    Ok(Arc::new(UInt64Array::new(
        lengths.into(),
        array.nulls().cloned(),
    )))
}

/// Returns each row's length along the dimension given for that row.
fn nth_dimension_length(array: &ArrayRef, dimension: &Int64Array) -> Result<ArrayRef> {
    match array.data_type() {
        List(_) => lengths_at_dimension(as_list_array(array)?.iter(), dimension),
        LargeList(_) => {
            lengths_at_dimension(as_large_list_array(array)?.iter(), dimension)
        }
        FixedSizeList(..) => {
            lengths_at_dimension(as_fixed_size_list_array(array)?.iter(), dimension)
        }
        array_type => exec_err!("array_length does not support type '{array_type}'"),
    }
}

fn lengths_at_dimension(
    rows: impl Iterator<Item = Option<ArrayRef>>,
    dimension: &Int64Array,
) -> Result<ArrayRef> {
    let result = rows
        .zip(dimension.iter())
        .map(|(row, dim)| compute_array_length(row, dim))
        .collect::<Result<UInt64Array>>()?;
    Ok(Arc::new(result))
}

/// Returns the length of a concrete array dimension
fn compute_array_length(
    arr: Option<ArrayRef>,
    dimension: Option<i64>,
) -> Result<Option<u64>> {
    let mut current_dimension: i64 = 1;
    let Some(mut value) = arr else {
        return Ok(None);
    };
    let dimension = match dimension {
        Some(value) => {
            if value < 1 {
                return Ok(None);
            }

            value
        }
        None => return Ok(None),
    };

    loop {
        if current_dimension == dimension {
            return Ok(Some(value.len() as u64));
        }

        match value.data_type() {
            List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                current_dimension += 1;
            }
            LargeList(..) => {
                value = downcast_arg!(value, LargeListArray).value(0);
                current_dimension += 1;
            }
            FixedSizeList(_, _) => {
                value = downcast_arg!(value, FixedSizeListArray).value(0);
                current_dimension += 1;
            }
            _ => return Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{GenericListArray, Int32Array, OffsetSizeTrait};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    fn check_slices(array: &dyn Array, expected: &UInt64Array) -> Result<()> {
        let udf = ArrayLength::new();
        for (offset, len) in [(0, 4), (1, 3), (2, 0)] {
            let array = array.slice(offset, len);
            for explicit_dimension in [false, true] {
                let mut args = vec![ColumnarValue::Array(Arc::clone(&array))];
                if explicit_dimension {
                    args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
                }
                let arg_fields = args
                    .iter()
                    .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                    .collect();
                let result = udf.invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields,
                    number_rows: len,
                    return_field: Arc::new(Field::new("length", UInt64, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })?;
                let ColumnarValue::Array(result) = result else {
                    panic!("expected an array result");
                };
                assert_eq!(result.as_ref(), &expected.slice(offset, len));
            }
        }
        Ok(())
    }

    #[test]
    fn array_length_list_offsets() -> Result<()> {
        fn check<O: OffsetSizeTrait>() -> Result<()> {
            let values = Arc::new(Int32Array::new_null(5));
            let array = GenericListArray::<O>::new(
                Arc::new(Field::new_list_field(DataType::Int32, true)),
                OffsetBuffer::from_lengths([1, 2, 0, 2]),
                values,
                Some(NullBuffer::from(vec![true, true, true, false])),
            );
            check_slices(
                &array,
                &UInt64Array::from(vec![Some(1), Some(2), Some(0), None]),
            )
        }
        check::<i32>()?;
        check::<i64>()
    }

    #[test]
    fn array_length_fixed_size_lists() -> Result<()> {
        for width in [0, 2] {
            let array = FixedSizeListArray::try_new_with_length(
                Arc::new(Field::new_list_field(DataType::Int32, true)),
                width,
                Arc::new(Int32Array::new_null(4 * width as usize)),
                Some(NullBuffer::from(vec![true, true, false, true])),
                4,
            )?;
            check_slices(
                &array,
                &UInt64Array::from(vec![
                    Some(width as u64),
                    Some(width as u64),
                    None,
                    Some(width as u64),
                ]),
            )?;
        }
        Ok(())
    }
}
