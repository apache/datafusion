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

//! Filter selection strategy for InList expressions

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::Result;

use super::nested_filter::NestedTypeFilter;
use super::primitive_filter::*;
use super::static_filter::StaticFilter;

/// Creates the optimal static filter for the given array.
///
/// This is the main entry point for filter creation. It analyzes the array's
/// data type and size to select the best lookup strategy.
pub(crate) fn instantiate_static_filter(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    match in_array.data_type() {
        DataType::Int8 => Ok(Arc::new(Int8StaticFilter::try_new(&in_array)?)),
        DataType::Int16 => Ok(Arc::new(Int16StaticFilter::try_new(&in_array)?)),
        DataType::Int32 => Ok(Arc::new(Int32StaticFilter::try_new(&in_array)?)),
        DataType::Int64 => Ok(Arc::new(Int64StaticFilter::try_new(&in_array)?)),
        DataType::UInt8 => Ok(Arc::new(BitmapFilter::<U8Config>::try_new(&in_array)?)),
        DataType::UInt16 => Ok(Arc::new(BitmapFilter::<U16Config>::try_new(&in_array)?)),
        DataType::UInt32 => Ok(Arc::new(UInt32StaticFilter::try_new(&in_array)?)),
        DataType::UInt64 => Ok(Arc::new(UInt64StaticFilter::try_new(&in_array)?)),
        DataType::Float32 => Ok(Arc::new(Float32StaticFilter::try_new(&in_array)?)),
        DataType::Float64 => Ok(Arc::new(Float64StaticFilter::try_new(&in_array)?)),
        _ => Ok(Arc::new(NestedTypeFilter::try_new(in_array)?)),
    }
}
