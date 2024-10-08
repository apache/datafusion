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

use std::{
    hash::Hash,
    sync::{Arc, OnceLock},
};

use arrow::datatypes::DataType;

pub trait LogicalType: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    fn can_decode_to(&self, data_type: &DataType) -> bool;
    fn decode_types(&self) -> Vec<DataType>;
}

/// A reference-counted reference to a generic `LogicalType`
pub type LogicalTypeRef = Arc<dyn LogicalType>;

impl Eq for dyn LogicalType {}

impl PartialEq for dyn LogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Hash for dyn LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Display for dyn LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// DataFusion native type
#[derive(Debug)]
pub struct String;

impl LogicalType for String {
    fn name(&self) -> &str {
        "string"
    }

    fn can_decode_to(&self, data_type: &DataType) -> bool {
        match data_type {
            DataType::Dictionary(_, v) => self.can_decode_to(v),
            _ => matches!(
                data_type,
                DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8
            ),
        }
    }

    fn decode_types(&self) -> Vec<DataType> {
        vec![DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8]
    }
}

#[derive(Debug)]
pub struct Float64;

#[derive(Debug)]
pub struct Float32;

#[derive(Debug)]
pub struct Float16;

// Macro to generate simple LogicalType implementation
macro_rules! impl_logical_type {
    ($data_type:ident, $name:expr) => {
        impl LogicalType for $data_type {
            fn name(&self) -> &str {
                $name
            }

            fn can_decode_to(&self, data_type: &DataType) -> bool {
                matches!(data_type, DataType::$data_type)
            }

            fn decode_types(&self) -> Vec<DataType> {
                vec![DataType::$data_type]
            }
        }
    };
}

// Applying the macro for each float type
impl_logical_type!(Float64, "f64");
impl_logical_type!(Float32, "f32");
impl_logical_type!(Float16, "f16");

// Singleton instances
// TODO: Replace with LazyLock
pub static LOGICAL_STRING: OnceLock<LogicalTypeRef> = OnceLock::new();
pub static LOGICAL_FLOAT16: OnceLock<LogicalTypeRef> = OnceLock::new();
pub static LOGICAL_FLOAT32: OnceLock<LogicalTypeRef> = OnceLock::new();
pub static LOGICAL_FLOAT64: OnceLock<LogicalTypeRef> = OnceLock::new();

// Usage functions
pub fn logical_string() -> LogicalTypeRef {
    Arc::clone(LOGICAL_STRING.get_or_init(|| Arc::new(String)))
}

pub fn logical_float16() -> LogicalTypeRef {
    Arc::clone(LOGICAL_FLOAT16.get_or_init(|| Arc::new(Float16)))
}

pub fn logical_float32() -> LogicalTypeRef {
    Arc::clone(LOGICAL_FLOAT32.get_or_init(|| Arc::new(Float32)))
}

pub fn logical_float64() -> LogicalTypeRef {
    Arc::clone(LOGICAL_FLOAT64.get_or_init(|| Arc::new(Float64)))
}
