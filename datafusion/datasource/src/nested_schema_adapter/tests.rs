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

use crate::{
    nested_schema_adapter::{
        NestedStructSchemaAdapter, NestedStructSchemaAdapterFactory,
    },
    schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory},
};
use arrow::{
    array::{Array, StringBuilder, StructArray, TimestampMillisecondArray},
    compute::cast,
    datatypes::{
        DataType::{Float64, Int16, Int32, Struct, Timestamp, Utf8},
        Field, Schema, SchemaRef,
        TimeUnit::Millisecond,
    },
    record_batch::RecordBatch,
};
use datafusion_common::{ColumnStatistics, Result, ScalarValue};
use std::sync::Arc;

// ================================
// Schema Evolution Tests
// ================================

#[test]
fn test_adapter_factory_selection() -> Result<()> {
    // Test schemas for adapter selection logic
    let simple_schema = Arc::new(Schema::new(vec![
        Field::new("id", Int32, false),
        Field::new("name", Utf8, true),
        Field::new("age", Int16, true),
    ]));

    let nested_schema = Arc::new(Schema::new(vec![
        Field::new("id", Int32, false),
        Field::new(
            "metadata",
            Struct(
                vec![
                    Field::new("created", Utf8, true),
                    Field::new("modified", Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
    ]));

    // Source schema with missing field
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", Int32, false),
        Field::new(
            "metadata",
            Struct(
                vec![
                    Field::new("created", Utf8, true),
                    // "modified" field is missing
                ]
                .into(),
            ),
            true,
        ),
    ]));

    // Test struct detection
    assert!(
        !NestedStructSchemaAdapterFactory::has_nested_structs(&simple_schema),
        "Simple schema should not be detected as having nested structs"
    );
    assert!(
        NestedStructSchemaAdapterFactory::has_nested_structs(&nested_schema),
        "Nested schema should be detected as having nested structs"
    );

    // Test adapter behavior with schema evolution
    let default_adapter =
        DefaultSchemaAdapterFactory.create(nested_schema.clone(), nested_schema.clone());
    let nested_adapter = NestedStructSchemaAdapterFactory
        .create(nested_schema.clone(), nested_schema.clone());

    // Default adapter should fail with schema evolution
    assert!(default_adapter.map_schema(&source_schema).is_err());

    // Nested adapter should handle schema evolution
    assert!(
        nested_adapter.map_schema(&source_schema).is_ok(),
        "Nested adapter should handle schema with missing fields"
    );

    // Test factory selection logic
    let adapter: Box<dyn SchemaAdapter> =
        if NestedStructSchemaAdapterFactory::has_nested_structs(&nested_schema) {
            NestedStructSchemaAdapterFactory
                .create(nested_schema.clone(), nested_schema.clone())
        } else {
            DefaultSchemaAdapterFactory
                .create(nested_schema.clone(), nested_schema.clone())
        };

    assert!(
        adapter.map_schema(&source_schema).is_ok(),
        "Factory should select appropriate adapter that handles schema evolution"
    );

    Ok(())
}

#[test]
fn test_adapt_struct_with_added_nested_fields() -> Result<()> {
    // Create test schemas
    let (file_schema, table_schema) = create_test_schemas_with_nested_fields();

    // Create batch with test data
    let batch = create_test_batch_with_struct_data(&file_schema)?;

    // Create adapter and apply it
    let mapped_batch =
        adapt_batch_with_nested_schema_adapter(&file_schema, &table_schema, batch)?;

    // Verify the results
    verify_adapted_batch_with_nested_fields(&mapped_batch, &table_schema)?;

    Ok(())
}

/// Create file and table schemas for testing nested field evolution
fn create_test_schemas_with_nested_fields() -> (SchemaRef, SchemaRef) {
    // Create file schema with just location and timestamp_utc
    let file_schema = Arc::new(Schema::new(vec![Field::new(
        "info",
        Struct(
            vec![
                Field::new("location", Utf8, true),
                Field::new(
                    "timestamp_utc",
                    Timestamp(Millisecond, Some("UTC".into())),
                    true,
                ),
            ]
            .into(),
        ),
        true,
    )]));

    // Create table schema with additional nested reason field
    let table_schema = Arc::new(Schema::new(vec![Field::new(
        "info",
        Struct(
            vec![
                Field::new("location", Utf8, true),
                Field::new(
                    "timestamp_utc",
                    Timestamp(Millisecond, Some("UTC".into())),
                    true,
                ),
                Field::new(
                    "reason",
                    Struct(
                        vec![
                            Field::new("_level", Float64, true),
                            Field::new(
                                "details",
                                Struct(
                                    vec![
                                        Field::new("rurl", Utf8, true),
                                        Field::new("s", Float64, true),
                                        Field::new("t", Utf8, true),
                                    ]
                                    .into(),
                                ),
                                true,
                            ),
                        ]
                        .into(),
                    ),
                    true,
                ),
            ]
            .into(),
        ),
        true,
    )]));

    (file_schema, table_schema)
}

/// Create a test RecordBatch with struct data matching the file schema
fn create_test_batch_with_struct_data(file_schema: &SchemaRef) -> Result<RecordBatch> {
    let mut location_builder = StringBuilder::new();
    location_builder.append_value("San Francisco");
    location_builder.append_value("New York");

    // Create timestamp array
    let timestamp_array = TimestampMillisecondArray::from(vec![
        Some(1640995200000), // 2022-01-01
        Some(1641081600000), // 2022-01-02
    ]);

    // Create data type with UTC timezone to match the schema
    let timestamp_type = Timestamp(Millisecond, Some("UTC".into()));

    // Cast the timestamp array to include the timezone metadata
    let timestamp_array = cast(&timestamp_array, &timestamp_type)?;

    let info_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("location", Utf8, true)),
            Arc::new(location_builder.finish()) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("timestamp_utc", timestamp_type, true)),
            timestamp_array,
        ),
    ]);

    Ok(RecordBatch::try_new(
        Arc::clone(file_schema),
        vec![Arc::new(info_struct)],
    )?)
}

/// Apply the nested schema adapter to the batch
fn adapt_batch_with_nested_schema_adapter(
    file_schema: &SchemaRef,
    table_schema: &SchemaRef,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let adapter = NestedStructSchemaAdapter::new(
        Arc::clone(table_schema),
        Arc::clone(table_schema),
    );

    let (mapper, _) = adapter.map_schema(file_schema.as_ref())?;
    mapper.map_batch(batch)
}

/// Verify the adapted batch has the expected structure and data
fn verify_adapted_batch_with_nested_fields(
    mapped_batch: &RecordBatch,
    table_schema: &SchemaRef,
) -> Result<()> {
    // Verify the mapped batch structure and data
    assert_eq!(mapped_batch.schema(), *table_schema);
    assert_eq!(mapped_batch.num_rows(), 2);

    // Extract and verify the info struct column
    let info_col = mapped_batch.column(0);
    let info_array = info_col
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Expected info column to be a StructArray");

    // Verify the original fields are preserved
    verify_preserved_fields(info_array)?;

    // Verify the reason field exists with correct structure
    verify_reason_field_structure(info_array)?;

    Ok(())
}

/// Verify the original fields from file schema are preserved in the adapted batch
fn verify_preserved_fields(info_array: &StructArray) -> Result<()> {
    // Verify location field
    let location_col = info_array
        .column_by_name("location")
        .expect("Expected location field in struct");
    let location_array = location_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("Expected location to be a StringArray");

    // Verify the location values are preserved
    assert_eq!(location_array.value(0), "San Francisco");
    assert_eq!(location_array.value(1), "New York");

    // Verify timestamp field
    let timestamp_col = info_array
        .column_by_name("timestamp_utc")
        .expect("Expected timestamp_utc field in struct");
    let timestamp_array = timestamp_col
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("Expected timestamp_utc to be a TimestampMillisecondArray");

    assert_eq!(timestamp_array.value(0), 1640995200000);
    assert_eq!(timestamp_array.value(1), 1641081600000);

    Ok(())
}

/// Verify the added reason field structure and null values
fn verify_reason_field_structure(info_array: &StructArray) -> Result<()> {
    // Verify the reason field exists and is null
    let reason_col = info_array
        .column_by_name("reason")
        .expect("Expected reason field in struct");
    let reason_array = reason_col
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Expected reason to be a StructArray");

    // Verify reason has correct structure
    assert_eq!(reason_array.fields().len(), 2);
    assert!(reason_array.column_by_name("_level").is_some());
    assert!(reason_array.column_by_name("details").is_some());

    // Verify details field has correct nested structure
    let details_col = reason_array
        .column_by_name("details")
        .expect("Expected details field in reason struct");
    let details_array = details_col
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Expected details to be a StructArray");

    assert_eq!(details_array.fields().len(), 3);
    assert!(details_array.column_by_name("rurl").is_some());
    assert!(details_array.column_by_name("s").is_some());
    assert!(details_array.column_by_name("t").is_some());

    // Verify all added fields are null
    for i in 0..2 {
        assert!(reason_array.is_null(i), "reason field should be null");
    }

    Ok(())
}

// ================================
// Data Mapping Tests
// ================================

// Helper function to verify column statistics match expected values
fn verify_column_statistics(
    stats: &ColumnStatistics,
    expected_null_count: Option<usize>,
    expected_distinct_count: Option<usize>,
    expected_min: Option<ScalarValue>,
    expected_max: Option<ScalarValue>,
    expected_sum: Option<ScalarValue>,
) {
    if let Some(count) = expected_null_count {
        assert_eq!(
            stats.null_count,
            datafusion_common::stats::Precision::Exact(count),
            "Null count should match expected value"
        );
    }

    if let Some(count) = expected_distinct_count {
        assert_eq!(
            stats.distinct_count,
            datafusion_common::stats::Precision::Exact(count),
            "Distinct count should match expected value"
        );
    }

    if let Some(min) = expected_min {
        assert_eq!(
            stats.min_value,
            datafusion_common::stats::Precision::Exact(min),
            "Min value should match expected value"
        );
    }

    if let Some(max) = expected_max {
        assert_eq!(
            stats.max_value,
            datafusion_common::stats::Precision::Exact(max),
            "Max value should match expected value"
        );
    }

    if let Some(sum) = expected_sum {
        assert_eq!(
            stats.sum_value,
            datafusion_common::stats::Precision::Exact(sum),
            "Sum value should match expected value"
        );
    }
}

// Helper to create test column statistics
fn create_test_column_statistics(
    null_count: usize,
    distinct_count: usize,
    min_value: Option<ScalarValue>,
    max_value: Option<ScalarValue>,
    sum_value: Option<ScalarValue>,
) -> ColumnStatistics {
    ColumnStatistics {
        null_count: datafusion_common::stats::Precision::Exact(null_count),
        distinct_count: datafusion_common::stats::Precision::Exact(distinct_count),
        min_value: min_value.map_or_else(
            || datafusion_common::stats::Precision::Absent,
            datafusion_common::stats::Precision::Exact,
        ),
        max_value: max_value.map_or_else(
            || datafusion_common::stats::Precision::Absent,
            datafusion_common::stats::Precision::Exact,
        ),
        sum_value: sum_value.map_or_else(
            || datafusion_common::stats::Precision::Absent,
            datafusion_common::stats::Precision::Exact,
        ),
    }
}

#[test]
fn test_map_column_statistics() -> Result<()> {
    // Create schemas with multiple columns
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("id", Int32, false),
        Field::new(
            "additionalInfo",
            Struct(
                vec![
                    Field::new("location", Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        Timestamp(Millisecond, Some("UTC".into())),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        ),
    ]));

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", Int32, false),
        Field::new(
            "additionalInfo",
            Struct(
                vec![
                    Field::new("location", Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        Timestamp(Millisecond, Some("UTC".into())),
                        true,
                    ),
                    Field::new(
                        "reason",
                        Struct(vec![Field::new("_level", Float64, true)].into()),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        ),
        Field::new("status", Utf8, true), // Extra column in table schema
    ]));

    // Create adapter and mapping
    let adapter = NestedStructSchemaAdapter::new(
        Arc::clone(&table_schema),
        Arc::clone(&table_schema),
    );

    let (mapper, _) = adapter.map_schema(file_schema.as_ref())?;

    // Create file column statistics
    let file_stats = vec![
        create_test_column_statistics(
            0,
            100,
            Some(ScalarValue::Int32(Some(1))),
            Some(ScalarValue::Int32(Some(100))),
            Some(ScalarValue::Int32(Some(5100))),
        ),
        create_test_column_statistics(10, 50, None, None, None),
    ];

    // Map statistics
    let table_stats = mapper.map_column_statistics(&file_stats)?;

    // Verify mapped statistics
    assert_eq!(
        table_stats.len(),
        3,
        "Should have stats for all 3 columns in table schema"
    );

    // Verify ID column stats
    verify_column_statistics(
        &table_stats[0],
        Some(0),
        Some(100),
        Some(ScalarValue::Int32(Some(1))),
        Some(ScalarValue::Int32(Some(100))),
        Some(ScalarValue::Int32(Some(5100))),
    );

    // Verify additionalInfo column stats
    verify_column_statistics(&table_stats[1], Some(10), Some(50), None, None, None);

    // Verify status column has unknown stats
    assert_eq!(
        table_stats[2],
        ColumnStatistics::new_unknown(),
        "Missing column should have unknown statistics",
    );

    // Verify behavior when no statistics are provided
    let empty_stats = vec![];
    let missing_stats = mapper.map_column_statistics(&empty_stats)?;

    assert_eq!(missing_stats.len(), 3);
    for stat in missing_stats {
        assert_eq!(stat, ColumnStatistics::new_unknown());
    }

    Ok(())
}
