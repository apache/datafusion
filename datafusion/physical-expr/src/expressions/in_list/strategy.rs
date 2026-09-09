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

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::take;
use arrow::datatypes::DataType;
use datafusion_common::Result;

use super::array_static_filter::ArrayStaticFilter;
use super::dictionary_filter::DictionaryFilter;
use super::fixed_size_binary_filter::instantiate_fixed_size_binary_filter;
use super::primitive_filter::instantiate_primitive_filter;
use super::static_filter::StaticFilterRef;

pub(super) fn instantiate_static_filter(
    in_array: ArrayRef,
    needle_data_type: &DataType,
) -> Result<StaticFilterRef> {
    let in_array = flatten_dictionary_haystack(in_array)?;

    let filter = if let Some(filter) = instantiate_fixed_size_binary_filter(&in_array)? {
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
        BooleanArray, DictionaryArray, Int8Array, Int16Array, Int32Array,
    };

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
}
