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

use arrow::array::{
    Array, ArrayRef, Capacities, FixedSizeListArray, GenericListArray, MutableArrayData,
    OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::FieldRef;
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion_common::{
    exec_err, internal_err, utils::take_function_args, Result, ScalarValue,
};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use rand::rng;
use rand::rngs::StdRng;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkShuffle {
    signature: Signature,
}

impl Default for SparkShuffle {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkShuffle {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    // Only array argument
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: None,
                    }),
                    // Array + Index (seed) argument
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Array,
                            ArrayFunctionArgument::Index,
                        ],
                        array_coercion: None,
                    }),
                ]),
                volatility: Volatility::Volatile,
                parameter_names: None,
            },
        }
    }
}

impl ScalarUDFImpl for SparkShuffle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shuffle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        // Shuffle returns an array with the same type and nullability as the input
        Ok(Arc::clone(&args.arg_fields[0]))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("shuffle expects at least 1 argument");
        }
        if args.args.len() > 2 {
            return exec_err!("shuffle expects at most 2 arguments");
        }

        // Extract seed from second argument if present
        let seed = if args.args.len() == 2 {
            extract_seed(&args.args[1])?
        } else {
            None
        };

        // Convert arguments to arrays
        let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
        array_shuffle_with_seed(&arrays, seed).map(ColumnarValue::Array)
    }
}

/// Extract seed value from ColumnarValue
fn extract_seed(seed_arg: &ColumnarValue) -> Result<Option<u64>> {
    match seed_arg {
        ColumnarValue::Scalar(scalar) => {
            let seed = match scalar {
                ScalarValue::Int64(Some(v)) => Some(*v as u64),
                ScalarValue::Null => None,
                _ => {
                    return exec_err!(
                        "shuffle seed must be Int64 type, got '{}'",
                        scalar.data_type()
                    );
                }
            };
            Ok(seed)
        }
        ColumnarValue::Array(_) => {
            exec_err!("shuffle seed must be a scalar value, not an array")
        }
    }
}

/// array_shuffle SQL function with optional seed
fn array_shuffle_with_seed(arg: &[ArrayRef], seed: Option<u64>) -> Result<ArrayRef> {
    let [input_array] = take_function_args("shuffle", arg)?;
    match &input_array.data_type() {
        List(field) => {
            let array = as_list_array(input_array)?;
            general_array_shuffle::<i32>(array, field, seed)
        }
        LargeList(field) => {
            let array = as_large_list_array(input_array)?;
            general_array_shuffle::<i64>(array, field, seed)
        }
        FixedSizeList(field, _) => {
            let array = as_fixed_size_list_array(input_array)?;
            fixed_size_array_shuffle(array, field, seed)
        }
        Null => Ok(Arc::clone(input_array)),
        array_type => exec_err!("shuffle does not support type '{array_type}'."),
    }
}

fn general_array_shuffle<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
    seed: Option<u64>,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);
    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        // Use a random seed from the thread-local RNG
        let seed = rng().random::<u64>();
        StdRng::seed_from_u64(seed)
    };

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            offsets.push(offsets[row_index] + O::one());
            mutable.extend(0, 0, 1);
            continue;
        }
        nulls.push(true);
        let start = offset_window[0];
        let end = offset_window[1];
        let length = (end - start).to_usize().unwrap();

        // Create indices and shuffle them
        let mut indices: Vec<usize> =
            (start.to_usize().unwrap()..end.to_usize().unwrap()).collect();
        indices.shuffle(&mut rng);

        // Add shuffled elements
        for &index in &indices {
            mutable.extend(0, index, index + 1);
        }

        offsets.push(offsets[row_index] + O::usize_as(length));
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

fn fixed_size_array_shuffle(
    array: &FixedSizeListArray,
    field: &FieldRef,
    seed: Option<u64>,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);
    let value_length = array.value_length() as usize;
    let mut rng = if let Some(s) = seed {
        StdRng::seed_from_u64(s)
    } else {
        // Use a random seed from the thread-local RNG
        let seed = rng().random::<u64>();
        StdRng::seed_from_u64(seed)
    };

    for row_index in 0..array.len() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            mutable.extend(0, 0, value_length);
            continue;
        }
        nulls.push(true);

        let start = row_index * value_length;
        let end = start + value_length;

        // Create indices and shuffle them
        let mut indices: Vec<usize> = (start..end).collect();
        indices.shuffle(&mut rng);

        // Add shuffled elements
        for &index in &indices {
            mutable.extend(0, index, index + 1);
        }
    }

    let data = mutable.freeze();
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(field),
        array.value_length(),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_expr::ReturnFieldArgs;

    #[test]
    fn test_shuffle_nullability() {
        let shuffle = SparkShuffle::new();

        // Test with non-nullable array
        let non_nullable_field = Arc::new(Field::new(
            "arr",
            List(Arc::new(Field::new("item", DataType::Int32, true))),
            false, // not nullable
        ));

        let result = shuffle
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_field)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // The result should not be nullable (same as input)
        assert!(!result.is_nullable());
        assert_eq!(result.data_type(), non_nullable_field.data_type());

        // Test with nullable array
        let nullable_field = Arc::new(Field::new(
            "arr",
            List(Arc::new(Field::new("item", DataType::Int32, true))),
            true, // nullable
        ));

        let result = shuffle
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_field)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // The result should be nullable (same as input)
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), nullable_field.data_type());
    }
}
