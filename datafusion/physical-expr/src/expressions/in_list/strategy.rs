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

//! Filter strategy selection for InList expressions

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::*;
use datafusion_common::Result;

use super::array_filter::{ArrayStaticFilter, StaticFilter};
use super::primitive::{PrimitiveFilter, U8Config, U16Config};
use super::transform::{make_bitmap_filter, make_primitive_filter};

pub(crate) fn instantiate_static_filter(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    match in_array.data_type() {
        // 1-byte types: use bitmap (256 bits = 32 bytes)
        DataType::Int8 | DataType::UInt8 => make_bitmap_filter::<U8Config>(&in_array),
        // 2-byte types: use bitmap (65536 bits = 8 KB)
        DataType::Int16 | DataType::UInt16 => make_bitmap_filter::<U16Config>(&in_array),
        // 4-byte integer types
        DataType::Int32 => Ok(Arc::new(PrimitiveFilter::<Int32Type>::try_new(&in_array)?)),
        DataType::UInt32 => Ok(Arc::new(PrimitiveFilter::<UInt32Type>::try_new(&in_array)?)),
        // 8-byte integer types
        DataType::Int64 => Ok(Arc::new(PrimitiveFilter::<Int64Type>::try_new(&in_array)?)),
        DataType::UInt64 => Ok(Arc::new(PrimitiveFilter::<UInt64Type>::try_new(&in_array)?)),
        // Float types: reinterpret as unsigned integers (same bit pattern = equal)
        DataType::Float32 => make_primitive_filter::<UInt32Type>(&in_array),
        DataType::Float64 => make_primitive_filter::<UInt64Type>(&in_array),
        _ => {
            /* fall through to generic implementation for unsupported types (Struct, etc.) */
            Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?))
        }
    }
}

