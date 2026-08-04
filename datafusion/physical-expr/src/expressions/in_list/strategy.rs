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

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion_common::Result;

use super::array_static_filter::ArrayStaticFilter;
use super::byte_view_filter::instantiate_byte_view_filter;
use super::fixed_size_binary_filter::instantiate_fixed_size_binary_filter;
use super::primitive_filter::instantiate_primitive_filter;
use super::static_filter::StaticFilterRef;

pub(super) fn instantiate_static_filter(
    in_array: ArrayRef,
    expr_data_type: &DataType,
) -> Result<StaticFilterRef> {
    let in_array = flatten_dictionary_haystack(in_array)?;

    if view_types_match(expr_data_type, in_array.data_type())
        && let Some(filter) = instantiate_byte_view_filter(&in_array)?
    {
        return Ok(filter);
    }

    if let Some(filter) = instantiate_fixed_size_binary_filter(&in_array)? {
        return Ok(filter);
    }

    if let Some(filter) = instantiate_primitive_filter(&in_array)? {
        return Ok(filter);
    }

    Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?))
}

/// Raw view access requires the expression and list to use the same view type.
/// Dictionary wrappers do not change the expression's value type.
fn view_types_match(expr_type: &DataType, list_type: &DataType) -> bool {
    matches!(list_type, DataType::Utf8View | DataType::BinaryView)
        && dictionary_value_type(expr_type) == list_type
}

fn dictionary_value_type(mut data_type: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value_type) = data_type {
        data_type = value_type;
    }
    data_type
}

fn flatten_dictionary_haystack(in_array: ArrayRef) -> Result<ArrayRef> {
    // Flatten dictionary-encoded haystacks to their value type so that
    // specialized primitive filters are used instead of falling through to the
    // generic ArrayStaticFilter.
    match in_array.data_type() {
        DataType::Dictionary(_, value_type) => Ok(cast(&in_array, value_type.as_ref())?),
        _ => Ok(in_array),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
