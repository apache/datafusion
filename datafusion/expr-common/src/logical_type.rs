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

use std::{hash::Hash, sync::Arc};

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
        matches!(
            data_type,
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8
        )
    }

    fn decode_types(&self) -> Vec<DataType> {
        vec![DataType::Utf8View, DataType::LargeUtf8, DataType::Utf8]
    }
}

#[derive(Debug)]
pub struct Float;

impl LogicalType for Float {
    fn name(&self) -> &str {
        "float"
    }

    fn can_decode_to(&self, data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Float64 | DataType::Float32 | DataType::Float16
        )
    }

    fn decode_types(&self) -> Vec<DataType> {
        vec![DataType::Float64, DataType::Float32, DataType::Float16]
    }
}

// TODO: Make this singleton
pub fn logical_string() -> LogicalTypeRef {
    Arc::new(String)
}

pub fn logical_float() -> LogicalTypeRef {
    Arc::new(Float)
}
