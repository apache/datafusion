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

use std::sync::Arc;

use arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use arrow::compute::{concat, take};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type,
};
use datafusion_common::Result;

use super::array_static_filter::ArrayStaticFilter;
use super::byte_view_filter::instantiate_byte_view_filter;
use super::dictionary_filter::DictionaryFilter;
use super::fixed_size_binary_filter::instantiate_fixed_size_binary_filter;
use super::primitive_filter::instantiate_primitive_filter;
use super::static_filter::StaticFilterRef;

pub(super) fn instantiate_static_filter(
    in_array: ArrayRef,
    needle_data_type: &DataType,
) -> Result<StaticFilterRef> {
    let in_array = flatten_dictionary_haystack(in_array)?;
    // Expand before choosing a filter so its cutoff counts every physical key.
    let in_array = match in_array.data_type() {
        DataType::Float16 => add_missing_signed_zero::<Float16Type>(in_array)?,
        DataType::Float32 => add_missing_signed_zero::<Float32Type>(in_array)?,
        DataType::Float64 => add_missing_signed_zero::<Float64Type>(in_array)?,
        _ => in_array,
    };

    let filter = if view_types_match(needle_data_type, in_array.data_type())
        && let Some(filter) = instantiate_byte_view_filter(&in_array)?
    {
        filter
    } else if let Some(filter) = instantiate_fixed_size_binary_filter(&in_array)? {
        filter
    } else if let Some(filter) = instantiate_primitive_filter(&in_array)? {
        filter
    } else {
        Arc::new(ArrayStaticFilter::try_new(in_array)?)
    };

    // Plain inputs can call the concrete filter directly. Dictionary inputs
    // share one adapter across all concrete filter types.
    if matches!(needle_data_type, DataType::Dictionary(_, _)) {
        Ok(Arc::new(DictionaryFilter::new(filter)))
    } else {
        Ok(filter)
    }
}

/// SQL membership equates the zero signs, while the primitive filters compare bits.
fn add_missing_signed_zero<T: ArrowPrimitiveType>(
    in_array: ArrayRef,
) -> Result<ArrayRef> {
    let array = in_array.as_primitive::<T>();
    let positive_zero = T::Native::ZERO;
    let negative_zero = positive_zero.neg_wrapping();
    let mut positive_present = false;
    let mut negative_present = false;
    for value in array.iter().flatten() {
        positive_present = positive_present || value.is_eq(positive_zero);
        negative_present = negative_present || value.is_eq(negative_zero);
        if positive_present && negative_present {
            return Ok(in_array);
        }
    }
    let missing = match (positive_present, negative_present) {
        (true, false) => negative_zero,
        (false, true) => positive_zero,
        _ => return Ok(in_array),
    };
    let alias = PrimitiveArray::<T>::from_value(missing, 1);
    Ok(concat(&[array, &alias])?)
}

/// Raw view access requires the expression and list to use the same view type.
/// Dictionary wrappers do not change the expression's value type.
fn view_types_match(needle_type: &DataType, list_type: &DataType) -> bool {
    matches!(list_type, DataType::Utf8View | DataType::BinaryView)
        && dictionary_value_type(needle_type) == list_type
}

pub(super) fn dictionary_value_type(mut data_type: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value_type) = data_type {
        data_type = value_type;
    }
    data_type
}

fn flatten_dictionary_haystack(mut in_array: ArrayRef) -> Result<ArrayRef> {
    // Flatten every dictionary layer so the final value type can use a
    // specialized filter.
    while let Some(dictionary) = in_array.as_any_dictionary_opt() {
        in_array = take(dictionary.values().as_ref(), dictionary.keys(), None)?;
    }

    Ok(in_array)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, DictionaryArray, Float32Array, Int8Array, Int16Array, Int32Array,
    };

    use super::super::branchless_filter::BranchlessFilterType;
    use super::*;

    fn nested_dictionary(keys: Int16Array) -> Result<ArrayRef> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        // This dictionary represents [1, 3, NULL].
        let inner: ArrayRef = Arc::new(DictionaryArray::try_new(
            Int8Array::from(vec![0, 2, 1]),
            values,
        )?);
        Ok(Arc::new(DictionaryArray::try_new(keys, inner)?))
    }

    #[test]
    fn nested_dictionary_haystacks_only_include_referenced_values() -> Result<()> {
        let needles = Int32Array::from(vec![1, 2, 3]);

        // The null in the inner dictionary is not referenced by the outer one.
        let filter = instantiate_static_filter(
            nested_dictionary(Int16Array::from(vec![0, 1]))?,
            &DataType::Int32,
        )?;
        assert_eq!(filter.null_count(), 0);
        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![true, false, true])
        );

        // Referencing that same value gives the list normal SQL null semantics.
        let filter = instantiate_static_filter(
            nested_dictionary(Int16Array::from(vec![0, 2]))?,
            &DataType::Int32,
        )?;
        assert_eq!(filter.null_count(), 1);
        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, None])
        );

        Ok(())
    }

    #[test]
    fn float_zero_expansion_preserves_values_and_reuses_complete_lists() -> Result<()> {
        let nan = f32::from_bits(0x7fc0_0001);
        for zero in [0.0_f32, -0.0_f32] {
            let array: ArrayRef = Arc::new(
                Float32Array::from(vec![Some(9.0), None, Some(nan), Some(zero)])
                    .slice(1, 3),
            );
            let expanded = add_missing_signed_zero::<Float32Type>(array)?;
            let bits = expanded
                .as_primitive::<Float32Type>()
                .iter()
                .map(|value| value.map(f32::to_bits))
                .collect::<Vec<_>>();
            assert_eq!(
                bits,
                vec![
                    None,
                    Some(nan.to_bits()),
                    Some(zero.to_bits()),
                    Some((-zero).to_bits())
                ]
            );
            assert!(Arc::ptr_eq(
                &expanded,
                &add_missing_signed_zero::<Float32Type>(Arc::clone(&expanded))?
            ));
        }

        let unchanged: [ArrayRef; 4] = [
            Arc::new(Float32Array::from(Vec::<f32>::new())),
            Arc::new(Float32Array::from(vec![None::<f32>])),
            Arc::new(Float32Array::from(vec![0.0_f32, -0.0_f32])),
            Arc::new(Float32Array::from(vec![0.0_f32, 1.0]).slice(1, 1)),
        ];
        for array in unchanged {
            assert!(Arc::ptr_eq(
                &array,
                &add_missing_signed_zero::<Float32Type>(Arc::clone(&array))?
            ));
        }
        Ok(())
    }

    #[test]
    fn static_float_filters_match_both_zeros_at_cutoffs() -> Result<()> {
        fn check<T: BranchlessFilterType>() -> Result<()> {
            let positive_zero = T::Native::ZERO;
            let negative_zero = positive_zero.neg_wrapping();
            let needles = PrimitiveArray::<T>::from_iter([
                Some(positive_zero),
                Some(negative_zero),
                Some(T::Native::ONE),
                None,
            ]);
            for len in [T::MAX_LIST_LEN - 1, T::MAX_LIST_LEN] {
                for zero in [positive_zero, negative_zero] {
                    let array = Arc::new(PrimitiveArray::<T>::from_value(zero, len));
                    let filter = instantiate_static_filter(array, &T::DATA_TYPE)?;
                    assert_eq!(
                        filter.contains(&needles, false)?,
                        BooleanArray::from(vec![
                            Some(true),
                            Some(true),
                            Some(false),
                            None
                        ])
                    );
                    assert_eq!(
                        filter.contains(&needles, true)?,
                        BooleanArray::from(vec![
                            Some(false),
                            Some(false),
                            Some(true),
                            None
                        ])
                    );
                }
            }
            Ok(())
        }
        check::<Float16Type>()?;
        check::<Float32Type>()?;
        check::<Float64Type>()
    }

    #[test]
    fn byte_view_routing_requires_same_physical_type() {
        let dict_view =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8View));

        assert!(view_types_match(&DataType::Utf8View, &DataType::Utf8View));
        assert!(view_types_match(&dict_view, &DataType::Utf8View));
        assert!(!view_types_match(
            &DataType::Utf8View,
            &DataType::BinaryView
        ));
        assert!(!view_types_match(&DataType::Utf8, &DataType::Utf8View));
    }
}
