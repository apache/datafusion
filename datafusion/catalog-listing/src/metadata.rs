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

use arrow::{
    array::RecordBatch,
    datatypes::{Field, Fields, Schema},
};
use datafusion_common::Result;
use datafusion_datasource::{metadata::MetadataColumn, PartitionedFile};
use datafusion_expr::{execution_props::ExecutionProps, Expr};

use crate::helpers::apply_filters;

/// Determine if the given file matches the input metadata filters.
/// `filters` should only contain expressions that can be evaluated
/// using only the metadata columns.
pub fn apply_metadata_filters(
    file: PartitionedFile,
    filters: &[Expr],
    metadata_cols: &[MetadataColumn],
) -> Result<Option<PartitionedFile>> {
    // if no metadata col => simply return all the files
    if metadata_cols.is_empty() {
        return Ok(Some(file));
    }

    let mut builders: Vec<_> = metadata_cols.iter().map(|col| col.builder(1)).collect();

    for builder in builders.iter_mut() {
        builder.append(&file.object_meta);
    }

    let arrays = builders
        .into_iter()
        .map(|builder| builder.finish())
        .collect::<Vec<_>>();

    let fields: Fields = metadata_cols
        .iter()
        .map(|col| Field::new(col.to_string(), col.arrow_type(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays)?;

    let props = ExecutionProps::new();

    // Don't retain rows that evaluated to null
    let prepared = apply_filters(&batch, filters, &props)?;

    // If the filter evaluates to true, return the file
    if prepared.true_count() == 1 {
        return Ok(Some(file));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};
    use object_store::{path::Path, ObjectMeta};
    
    // Helper function to create a test file with specific metadata
    fn create_test_file(
        path: &str,
        size: u64,
        last_modified: DateTime<Utc>,
    ) -> PartitionedFile {
        let object_meta = ObjectMeta {
            location: Path::from(path),
            last_modified,
            size: size as usize, // Convert u64 to usize
            e_tag: None,
            version: None,
        };
        
        PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }
    }
    
    #[test]
    fn test_apply_metadata_filters_empty_filters() {
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap(),
        );
        
        // Test with empty filters
        let result = apply_metadata_filters(
            file.clone(),
            &[],
            &[MetadataColumn::Location, MetadataColumn::Size, MetadataColumn::LastModified],
        )
        .unwrap();
        
        // With empty filters, the file should be returned
        assert!(result.is_some());
        assert_eq!(result.unwrap().object_meta.location.as_ref(), "test/file.parquet");
    }
    
    #[test]
    fn test_apply_metadata_filters_empty_metadata_cols() {
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap(),
        );
        
        // Test with a filter but empty metadata columns
        let filter = col("location").eq(lit("test/file.parquet"));
        let result = apply_metadata_filters(file.clone(), &[filter], &[]).unwrap();
        
        // With no metadata columns, the file should be returned regardless of the filter
        assert!(result.is_some());
        assert_eq!(result.unwrap().object_meta.location.as_ref(), "test/file.parquet");
    }
    
    #[test]
    fn test_apply_metadata_filters_location() {
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap(),
        );
        
        // Test with location filter - matching
        let filter = col("location").eq(lit("test/file.parquet"));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
        assert_eq!(result.unwrap().object_meta.location.as_ref(), "test/file.parquet");
        
        // Test with location filter - not matching
        let filter = col("location").eq(lit("test/different.parquet"));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location],
        )
        .unwrap();
        
        // The file should not match
        assert!(result.is_none());
        
        // Test with location filter - partial match (contains)
        let filter = col("location").like(lit("%file.parquet"));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
    }
    
    #[test]
    fn test_apply_metadata_filters_size() {
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap(),
        );
        
        // Test with size filter - matching
        let filter = col("size").eq(lit(1024u64));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
        
        // Test with size filter - not matching
        let filter = col("size").eq(lit(2048u64));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should not match
        assert!(result.is_none());
        
        // Test with size filter - range comparison
        let filter = col("size").gt(lit(512u64)).and(col("size").lt(lit(2048u64)));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
    }
    
    #[test]
    fn test_apply_metadata_filters_last_modified() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            timestamp,
        );
        
        // Convert to micros timestamp for comparison with the Arrow type
        let timestamp_micros = timestamp.timestamp_micros();
        
        // Test with last_modified filter - matching
        let filter = col("last_modified").eq(lit(ScalarValue::TimestampMicrosecond(
            Some(timestamp_micros),
            Some("UTC".to_string().into()),
        )));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::LastModified],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
        
        // Test with last_modified filter - not matching
        let different_timestamp = Utc.with_ymd_and_hms(2023, 2, 1, 10, 0, 0).unwrap();
        let different_timestamp_micros = different_timestamp.timestamp_micros();
        let filter = col("last_modified").eq(lit(ScalarValue::TimestampMicrosecond(
            Some(different_timestamp_micros),
            Some("UTC".to_string().into()),
        )));
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::LastModified],
        )
        .unwrap();
        
        // The file should not match
        assert!(result.is_none());
        
        // Test with last_modified filter - range comparison
        let earlier = Utc.with_ymd_and_hms(2022, 12, 1, 0, 0, 0).unwrap();
        let earlier_micros = earlier.timestamp_micros();
        let later = Utc.with_ymd_and_hms(2023, 2, 1, 0, 0, 0).unwrap();
        let later_micros = later.timestamp_micros();
        
        let filter = col("last_modified")
            .gt(lit(ScalarValue::TimestampMicrosecond(
                Some(earlier_micros),
                Some("UTC".to_string().into()),
            )))
            .and(col("last_modified").lt(lit(ScalarValue::TimestampMicrosecond(
                Some(later_micros),
                Some("UTC".to_string().into()),
            ))));
            
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::LastModified],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
    }
    
    #[test]
    fn test_apply_metadata_filters_multiple_columns() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let timestamp_micros = timestamp.timestamp_micros();
        
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            timestamp,
        );
        
        // Test with multiple metadata columns - all matching
        let filter = col("location")
            .eq(lit("test/file.parquet"))
            .and(col("size").eq(lit(1024u64)));
            
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location, MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
        
        // Test with multiple metadata columns - one not matching
        let filter = col("location")
            .eq(lit("test/file.parquet"))
            .and(col("size").eq(lit(2048u64)));
            
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location, MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should not match
        assert!(result.is_none());
        
        // Test with all three metadata columns
        let filter = col("location")
            .eq(lit("test/file.parquet"))
            .and(col("size").gt(lit(512u64)))
            .and(col("last_modified").eq(lit(ScalarValue::TimestampMicrosecond(
                Some(timestamp_micros),
                Some("UTC".to_string().into()),
            ))));
            
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location, MetadataColumn::Size, MetadataColumn::LastModified],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
    }
    
    #[test]
    fn test_apply_metadata_filters_complex_expressions() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let timestamp_micros = timestamp.timestamp_micros();
        
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            timestamp,
        );
        
        // Test with a complex expression (OR condition)
        let filter = col("location")
            .eq(lit("test/different.parquet"))
            .or(col("size").eq(lit(1024u64)));
            
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location, MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should match because one condition is true
        assert!(result.is_some());
        
        // Test with a more complex nested expression
        let filter = col("location")
            .like(lit("%file.parquet"))
            .and(
                col("size").lt(lit(2048u64))
                .or(col("last_modified").gt(lit(ScalarValue::TimestampMicrosecond(
                    Some(timestamp_micros),
                    Some("UTC".to_string().into()),
                ))))
            );
            
        let result = apply_metadata_filters(
            file.clone(),
            &[filter],
            &[MetadataColumn::Location, MetadataColumn::Size, MetadataColumn::LastModified],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
    }
    
    #[test]
    fn test_apply_metadata_filters_multiple_filters() {
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        
        // Create a test file
        let file = create_test_file(
            "test/file.parquet",
            1024, // 1KB
            timestamp,
        );
        
        // Test with multiple separate filters (AND semantics)
        let filter1 = col("location").eq(lit("test/file.parquet"));
        let filter2 = col("size").eq(lit(1024u64));
        
        let result = apply_metadata_filters(
            file.clone(),
            &[filter1, filter2],
            &[MetadataColumn::Location, MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should match
        assert!(result.is_some());
        
        // Test with multiple separate filters - one not matching
        let filter1 = col("location").eq(lit("test/file.parquet"));
        let filter2 = col("size").eq(lit(2048u64));
        
        let result = apply_metadata_filters(
            file.clone(),
            &[filter1, filter2],
            &[MetadataColumn::Location, MetadataColumn::Size],
        )
        .unwrap();
        
        // The file should not match
        assert!(result.is_none());
    }
}
