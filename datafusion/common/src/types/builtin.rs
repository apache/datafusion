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
    ($name:ident, $ty:ident) => {
        #[doc = concat!("Singleton instance of a logical type representing [`NativeType::", stringify!($ty), "`].")]
        pub static $name: LazyLock<LogicalTypeRef> =
            LazyLock::new(|| Arc::new(NativeType::$ty));
    };
}

singleton!(LOGICAL_NULL, Null);
singleton!(LOGICAL_BOOLEAN, Boolean);
singleton!(LOGICAL_INT8, Int8);
singleton!(LOGICAL_INT16, Int16);
singleton!(LOGICAL_INT32, Int32);
singleton!(LOGICAL_INT64, Int64);
singleton!(LOGICAL_UINT8, UInt8);
singleton!(LOGICAL_UINT16, UInt16);
singleton!(LOGICAL_UINT32, UInt32);
singleton!(LOGICAL_UINT64, UInt64);
singleton!(LOGICAL_FLOAT16, Float16);
singleton!(LOGICAL_FLOAT32, Float32);
singleton!(LOGICAL_FLOAT64, Float64);
singleton!(LOGICAL_DATE, Date);
singleton!(LOGICAL_BINARY, Binary);
singleton!(LOGICAL_STRING, String);
