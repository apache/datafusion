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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use object_store::path::Path;
    use std::str::FromStr;
    use arrow::array::{StringArray, TimestampMicrosecondArray, UInt64Array};
    
    // Helper function to create a test ObjectMeta
    fn create_test_object_meta(path: &str, size: usize, timestamp: DateTime<Utc>) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: timestamp,
            size,
            e_tag: None,
            version: None,
        }
    }
    
    #[test]
    fn test_metadata_column_name() {
        assert_eq!(MetadataColumn::Location.name(), "location");
        assert_eq!(MetadataColumn::LastModified.name(), "last_modified");
        assert_eq!(MetadataColumn::Size.name(), "size");
    }
    
    #[test]
    fn test_metadata_column_arrow_type() {
        assert_eq!(MetadataColumn::Location.arrow_type(), DataType::Utf8);
        assert_eq!(
            MetadataColumn::LastModified.arrow_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(MetadataColumn::Size.arrow_type(), DataType::UInt64);
    }
    
    #[test]
    fn test_metadata_column_field() {
        let field = MetadataColumn::Location.field();
        assert_eq!(field.name(), "location");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(field.is_nullable());
        
        let field = MetadataColumn::LastModified.field();
        assert_eq!(field.name(), "last_modified");
        assert_eq!(
            field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(field.is_nullable());
        
        let field = MetadataColumn::Size.field();
        assert_eq!(field.name(), "size");
        assert_eq!(field.data_type(), &DataType::UInt64);
        assert!(field.is_nullable());
    }
    
    #[test]
    fn test_metadata_column_to_scalar_value() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let meta = create_test_object_meta("test/file.parquet", 1024, timestamp);
        
        // Test Location scalar value
        let scalar = MetadataColumn::Location.to_scalar_value(&meta);
        assert_eq!(scalar, ScalarValue::Utf8(Some("test/file.parquet".to_string())));
        
        // Test LastModified scalar value
        let scalar = MetadataColumn::LastModified.to_scalar_value(&meta);
        assert_eq!(
            scalar,
            ScalarValue::TimestampMicrosecond(
                Some(timestamp.timestamp_micros()),
                Some("UTC".into())
            )
        );
        
        // Test Size scalar value
        let scalar = MetadataColumn::Size.to_scalar_value(&meta);
        assert_eq!(scalar, ScalarValue::UInt64(Some(1024)));
    }
    
    #[test]
    fn test_metadata_column_from_str() {
        // Test valid values
        assert_eq!(
            MetadataColumn::from_str("location").unwrap(),
            MetadataColumn::Location
        );
        assert_eq!(
            MetadataColumn::from_str("last_modified").unwrap(),
            MetadataColumn::LastModified
        );
        assert_eq!(
            MetadataColumn::from_str("size").unwrap(),
            MetadataColumn::Size
        );
        
        // Test invalid value
        let err = MetadataColumn::from_str("invalid").unwrap_err();
        assert!(err.to_string().contains("Invalid metadata column"));
    }
    
    #[test]
    fn test_metadata_column_display() {
        assert_eq!(format!("{}", MetadataColumn::Location), "location");
        assert_eq!(format!("{}", MetadataColumn::LastModified), "last_modified");
        assert_eq!(format!("{}", MetadataColumn::Size), "size");
    }
    
    #[test]
    fn test_metadata_builder_location() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let meta1 = create_test_object_meta("file1.parquet", 1024, timestamp);
        let meta2 = create_test_object_meta("file2.parquet", 2048, timestamp);
        
        // Create a location builder and append values
        let mut builder = MetadataColumn::Location.builder(2);
        builder.append(&meta1);
        builder.append(&meta2);
        
        // Finish and check the array
        let array = builder.finish();
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "file1.parquet");
        assert_eq!(string_array.value(1), "file2.parquet");
    }
    
    #[test]
    fn test_metadata_builder_last_modified() {
        let timestamp1 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let timestamp2 = Utc.with_ymd_and_hms(2023, 1, 2, 10, 0, 0).unwrap();
        
        let meta1 = create_test_object_meta("file1.parquet", 1024, timestamp1);
        let meta2 = create_test_object_meta("file2.parquet", 2048, timestamp2);
        
        // Create a last_modified builder and append values
        let mut builder = MetadataColumn::LastModified.builder(2);
        builder.append(&meta1);
        builder.append(&meta2);
        
        // Finish and check the array
        let array = builder.finish();
        let ts_array = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        
        assert_eq!(ts_array.len(), 2);
        assert_eq!(ts_array.value(0), timestamp1.timestamp_micros());
        assert_eq!(ts_array.value(1), timestamp2.timestamp_micros());
    }
    
    #[test]
    fn test_metadata_builder_size() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let meta1 = create_test_object_meta("file1.parquet", 1024, timestamp);
        let meta2 = create_test_object_meta("file2.parquet", 2048, timestamp);
        
        // Create a size builder and append values
        let mut builder = MetadataColumn::Size.builder(2);
        builder.append(&meta1);
        builder.append(&meta2);
        
        // Finish and check the array
        let array = builder.finish();
        let uint64_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        
        assert_eq!(uint64_array.len(), 2);
        assert_eq!(uint64_array.value(0), 1024);
        assert_eq!(uint64_array.value(1), 2048);
    }
    
    #[test]
    fn test_metadata_builder_empty() {
        // Test with empty builders
        let location_builder = MetadataColumn::Location.builder(0);
        let location_array = location_builder.finish();
        assert_eq!(location_array.len(), 0);
        
        let last_modified_builder = MetadataColumn::LastModified.builder(0);
        let last_modified_array = last_modified_builder.finish();
        assert_eq!(last_modified_array.len(), 0);
        
        let size_builder = MetadataColumn::Size.builder(0);
        let size_array = size_builder.finish();
        assert_eq!(size_array.len(), 0);
    }
}
