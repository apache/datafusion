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

use crate::types::{LogicalTypeRef, NativeType};
use std::sync::{Arc, LazyLock};

macro_rules! singleton {
    ($name:ident, $getter:ident, $ty:ident) => {
        static $name: LazyLock<LogicalTypeRef> =
            LazyLock::new(|| Arc::new(NativeType::$ty));

        #[doc = "Getter for singleton instance of a logical type representing"]
        #[doc = concat!("[`NativeType::", stringify!($ty), "`].")]
        pub fn $getter() -> LogicalTypeRef {
            Arc::clone(&$name)
        }
    };
}

singleton!(LOGICAL_NULL, logical_null, Null);
singleton!(LOGICAL_BOOLEAN, logical_boolean, Boolean);
singleton!(LOGICAL_INT8, logical_int8, Int8);
singleton!(LOGICAL_INT16, logical_int16, Int16);
singleton!(LOGICAL_INT32, logical_int32, Int32);
singleton!(LOGICAL_INT64, logical_int64, Int64);
singleton!(LOGICAL_UINT8, logical_uint8, UInt8);
singleton!(LOGICAL_UINT16, logical_uint16, UInt16);
singleton!(LOGICAL_UINT32, logical_uint32, UInt32);
singleton!(LOGICAL_UINT64, logical_uint64, UInt64);
singleton!(LOGICAL_FLOAT16, logical_float16, Float16);
singleton!(LOGICAL_FLOAT32, logical_float32, Float32);
singleton!(LOGICAL_FLOAT64, logical_float64, Float64);
singleton!(LOGICAL_DATE, logical_date, Date);
singleton!(LOGICAL_BINARY, logical_binary, Binary);
singleton!(LOGICAL_STRING, logical_string, String);
