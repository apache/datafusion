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
use super::fixed_size_binary_filter::instantiate_fixed_size_binary_filter;
use super::primitive_filter::instantiate_primitive_filter;
use super::static_filter::StaticFilterRef;

pub(super) fn instantiate_static_filter(in_array: ArrayRef) -> Result<StaticFilterRef> {
    let in_array = flatten_dictionary_haystack(in_array)?;

    if let Some(filter) = instantiate_fixed_size_binary_filter(&in_array)? {
        return Ok(filter);
    }

    if let Some(filter) = instantiate_primitive_filter(&in_array)? {
        return Ok(filter);
    }

    Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?))
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
