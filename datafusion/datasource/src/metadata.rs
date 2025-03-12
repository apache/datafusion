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

//! Functions that support extracting metadata from files based on their ObjectMeta.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use datafusion_common::plan_err;
use datafusion_common::Result;

use arrow::{
    array::{Array, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder},
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion_common::ScalarValue;

use datafusion_common::DataFusionError;
use object_store::ObjectMeta;

/// A metadata column that can be used to filter files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataColumn {
    /// The location of the file in object store
    Location,
    /// The last modified timestamp of the file
    LastModified,
    /// The size of the file in bytes
    Size,
}

impl fmt::Display for MetadataColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl MetadataColumn {
    /// The name of the metadata column (one of `location`, `last_modified`, or `size`)
    pub fn name(&self) -> &str {
        match self {
            MetadataColumn::Location => "location",
            MetadataColumn::LastModified => "last_modified",
            MetadataColumn::Size => "size",
        }
    }

    /// Returns the arrow type of this metadata column
    pub fn arrow_type(&self) -> DataType {
        match self {
            MetadataColumn::Location => DataType::Utf8,
            MetadataColumn::LastModified => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            MetadataColumn::Size => DataType::UInt64,
        }
    }

    /// Returns the arrow field for this metadata column
    pub fn field(&self) -> Field {
        Field::new(self.to_string(), self.arrow_type(), true)
    }

    /// Returns the scalar value for this metadata column given an object meta
    pub fn to_scalar_value(&self, meta: &ObjectMeta) -> ScalarValue {
        match self {
            MetadataColumn::Location => {
                ScalarValue::Utf8(Some(meta.location.to_string()))
            }
            MetadataColumn::LastModified => ScalarValue::TimestampMicrosecond(
                Some(meta.last_modified.timestamp_micros()),
                Some("UTC".into()),
            ),
            MetadataColumn::Size => ScalarValue::UInt64(Some(meta.size as u64)),
        }
    }

    pub fn builder(&self, capacity: usize) -> MetadataBuilder {
        match self {
            MetadataColumn::Location => MetadataBuilder::Location(
                StringBuilder::with_capacity(capacity, capacity * 10),
            ),
            MetadataColumn::LastModified => MetadataBuilder::LastModified(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_data_type(
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ),
            ),
            MetadataColumn::Size => {
                MetadataBuilder::Size(UInt64Builder::with_capacity(capacity))
            }
        }
    }
}

impl FromStr for MetadataColumn {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "location" => Ok(MetadataColumn::Location),
            "last_modified" => Ok(MetadataColumn::LastModified),
            "size" => Ok(MetadataColumn::Size),
            _ => plan_err!(
                "Invalid metadata column: {}, expected: location, last_modified, or size",
                s
            ),
        }
    }
}

pub enum MetadataBuilder {
    Location(StringBuilder),
    LastModified(TimestampMicrosecondBuilder),
    Size(UInt64Builder),
}

impl MetadataBuilder {
    pub fn append(&mut self, meta: &ObjectMeta) {
        match self {
            Self::Location(builder) => builder.append_value(&meta.location),
            Self::LastModified(builder) => {
                builder.append_value(meta.last_modified.timestamp_micros())
            }
            Self::Size(builder) => builder.append_value(meta.size as u64),
        }
    }

    pub fn finish(self) -> Arc<dyn Array> {
        match self {
            MetadataBuilder::Location(mut builder) => Arc::new(builder.finish()),
            MetadataBuilder::LastModified(mut builder) => Arc::new(builder.finish()),
            MetadataBuilder::Size(mut builder) => Arc::new(builder.finish()),
        }
    }
}
