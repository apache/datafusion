// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

//! [`NestedStructSchemaAdapter`] and [`NestedStructSchemaAdapterFactory`] to adapt file-level record batches to a table schema.
//!
//! Adapter provides a method of translating the RecordBatches that come out of the
//! physical format into how they should be used by DataFusion.  For instance, a schema
//! can be stored external to a parquet file that maps parquet logical types to arrow types.

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion_common::{ColumnStatistics, Result};
use std::collections::HashMap;
use std::sync::Arc;

use crate::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use arrow::array::{Array, ArrayRef, StructArray};
use arrow::compute::cast;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::arrow::array::new_null_array;

/// Factory for creating [`NestedStructSchemaAdapter`]
///
/// This factory creates schema adapters that properly handle schema evolution
/// for nested struct fields, allowing new fields to be added to struct columns
/// over time.
#[derive(Debug, Clone, Default)]
pub struct NestedStructSchemaAdapterFactory;

impl SchemaAdapterFactory for NestedStructSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(NestedStructSchemaAdapter::new(
            projected_table_schema,
            table_schema,
        ))
    }
}

impl NestedStructSchemaAdapterFactory {
    /// Create a new factory for mapping batches from a file schema to a table
    /// schema with support for nested struct evolution.
    ///
    /// This is a convenience method that handles nested struct fields properly.
    pub fn from_schema(table_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Self.create(Arc::clone(&table_schema), table_schema)
    }

    /// Determines if a schema contains nested struct fields that would benefit
    /// from special handling during schema evolution
    pub fn has_nested_structs(schema: &Schema) -> bool {
        schema
            .fields()
            .iter()
            .any(|field| matches!(field.data_type(), DataType::Struct(_)))
    }

    /// Create an appropriate schema adapter based on schema characteristics.
    /// Returns a NestedStructSchemaAdapter if the projected schema contains nested structs,
    /// otherwise returns a DefaultSchemaAdapter.
    pub fn create_adapter(
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        // Use nested adapter if target has nested structs
        if Self::has_nested_structs(table_schema.as_ref()) {
            NestedStructSchemaAdapterFactory.create(projected_table_schema, table_schema)
        } else {
            // Default case for simple schemas
            DefaultSchemaAdapterFactory.create(projected_table_schema, table_schema)
        }
    }
}

/// A SchemaAdapter that handles schema evolution for nested struct types
#[derive(Debug, Clone)]
pub struct NestedStructSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetSource
    projected_table_schema: SchemaRef,
    /// The entire table schema for the table we're using this to adapt.
    ///
    /// This is used to evaluate any filters pushed down into the scan
    /// which may refer to columns that are not referred to anywhere
    /// else in the plan.
    table_schema: SchemaRef,
}

/// Adapt the source schema fields to match the target schema while preserving
/// nested struct fields and handling field additions/removals
///
/// The helper function adapt_fields creates a HashMap from the source fields for each call.
/// If this function is called frequently or on large schemas, consider whether the
/// performance overhead is acceptable or if caching/optimizing the lookup could be beneficial.
fn adapt_fields(source_fields: &Fields, target_fields: &Fields) -> Vec<Field> {
    let mut adapted_fields = Vec::new();
    let source_map: HashMap<_, _> = source_fields
        .iter()
        .map(|f| (f.name().as_str(), f))
        .collect();

    for target_field in target_fields {
        match source_map.get(target_field.name().as_str()) {
            Some(source_field) => {
                match (source_field.data_type(), target_field.data_type()) {
                    // Recursively adapt nested struct fields
                    (
                        DataType::Struct(source_children),
                        DataType::Struct(target_children),
                    ) => {
                        let adapted_children =
                            adapt_fields(source_children, target_children);
                        adapted_fields.push(Field::new(
                            target_field.name(),
                            DataType::Struct(adapted_children.into()),
                            target_field.is_nullable(),
                        ));
                    }
                    // If types match exactly, keep source field
                    _ if source_field.data_type() == target_field.data_type() => {
                        adapted_fields.push(source_field.as_ref().clone());
                    }
                    // Types don't match - use target field definition
                    _ => {
                        adapted_fields.push(target_field.as_ref().clone());
                    }
                }
            }
            // Field doesn't exist in source - add from target
            None => {
                adapted_fields.push(target_field.as_ref().clone());
            }
        }
    }

    adapted_fields
}
impl NestedStructSchemaAdapter {
    /// Create a new NestedStructSchemaAdapter with the target schema
    pub fn new(projected_table_schema: SchemaRef, table_schema: SchemaRef) -> Self {
        Self {
            projected_table_schema,
            table_schema,
        }
    }

    pub fn projected_table_schema(&self) -> &Schema {
        self.projected_table_schema.as_ref()
    }

    pub fn table_schema(&self) -> &Schema {
        self.table_schema.as_ref()
    }

    // Takes a source schema and transforms it to match the structure of the target schema.
    fn adapt_schema(&self, source_schema: SchemaRef) -> Result<SchemaRef> {
        let adapted_fields =
            adapt_fields(source_schema.fields(), self.table_schema.fields());

        Ok(Arc::new(Schema::new_with_metadata(
            adapted_fields,
            self.table_schema.metadata().clone(),
        )))
    }

    /// Create a schema mapping that can transform data from source schema to target schema
    fn create_schema_mapping(
        &self,
        source_schema: &Schema,
        target_schema: &Schema,
    ) -> Result<Arc<dyn SchemaMapper>> {
        // Map field names between schemas
        let mut field_mappings = Vec::new();

        for target_field in target_schema.fields() {
            let index = source_schema.index_of(target_field.name());
            field_mappings.push(index.ok());
        }

        // Create our custom NestedStructSchemaMapping
        let mapping = NestedStructSchemaMapping::new(
            Arc::new(target_schema.clone()), // projected_table_schema
            field_mappings,                  // field_mappings
        );

        Ok(Arc::new(mapping))
    }
}

impl SchemaAdapter for NestedStructSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field_name = self.table_schema.field(index).name();
        file_schema.index_of(field_name).ok()
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        // Adapt the file schema to match the target schema structure
        let adapted_schema = self.adapt_schema(Arc::new(file_schema.clone()))?;

        // Create a mapper that can transform data from file schema to the adapted schema
        let mapper = self.create_schema_mapping(file_schema, &adapted_schema)?;

        // Collect column indices to project from the file
        let mut projection = Vec::new();
        for field_name in file_schema.fields().iter().map(|f| f.name()) {
            if let Ok(idx) = file_schema.index_of(field_name) {
                projection.push(idx);
            }
        }

        Ok((mapper, projection))
    }
}

/// A SchemaMapping implementation specifically for nested structs
#[derive(Debug)]
struct NestedStructSchemaMapping {
    /// The schema for the table, projected to include only the fields being output
    projected_table_schema: SchemaRef,
    /// Field mappings from projected table to file schema
    field_mappings: Vec<Option<usize>>,
}

impl NestedStructSchemaMapping {
    /// Create a new nested struct schema mapping
    pub fn new(
        projected_table_schema: SchemaRef,
        field_mappings: Vec<Option<usize>>,
    ) -> Self {
        Self {
            projected_table_schema,
            field_mappings,
        }
    }
}

/// Maps a `RecordBatch` to a new `RecordBatch` according to the schema mapping defined in `NestedStructSchemaMapping`.
///
/// # Arguments
///
/// * `batch` - The input `RecordBatch` to be mapped.
///
/// # Returns
///
/// A `Result` containing the new `RecordBatch` with columns adapted according to the schema mapping, or an error if the mapping fails.
///
/// # Behavior
///
/// - For each field in the projected table schema, the corresponding column in the input batch is adapted.
/// - If a field does not exist in the input batch, a null array of the appropriate data type and length is created and used in the output batch.
/// - If a field exists in the input batch, the column is adapted to handle potential nested struct adaptation.
///
/// # Errors
///
/// Returns an error if the column adaptation fails or if the new `RecordBatch` cannot be created.
impl SchemaMapper for NestedStructSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        let cols = self
            .projected_table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
            .map(|(field, file_idx)| {
                file_idx.map_or_else(
                    // If field doesn't exist in file, return null array
                    || Ok(new_null_array(field.data_type(), batch_rows)),
                    // If field exists, handle potential nested struct adaptation
                    |batch_idx| adapt_column(&batch_cols[batch_idx], field),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create record batch with adapted columns
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let schema = Arc::clone(&self.projected_table_schema);
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }

    /// Adapts file-level column `Statistics` to match the `table_schema`
    ///
    /// Maps statistics from the file schema to the projected table schema using field mappings.
    /// For fields not present in the file schema, uses unknown statistics.
    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        let mut table_col_statistics = vec![];

        // Map statistics for each field based on field_mappings
        for (_, file_col_idx) in self
            .projected_table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
        {
            if let Some(file_col_idx) = file_col_idx {
                // Use statistics from file if available, otherwise default
                table_col_statistics.push(
                    file_col_statistics
                        .get(*file_col_idx)
                        .cloned()
                        .unwrap_or_default(),
                );
            } else {
                // Field doesn't exist in file schema, use unknown statistics
                table_col_statistics.push(ColumnStatistics::new_unknown());
            }
        }

        Ok(table_col_statistics)
    }
}

// Helper methods for the NestedStructSchemaMapping
/// Adapt a column to match the target field type, handling nested structs specially
fn adapt_column(source_col: &ArrayRef, target_field: &Field) -> Result<ArrayRef> {
    match target_field.data_type() {
        DataType::Struct(target_fields) => {
            // For struct arrays, we need to handle them specially
            if let Some(struct_array) = source_col.as_any().downcast_ref::<StructArray>()
            {
                // Create a vector to store field-array pairs with the correct type
                let mut children: Vec<(Arc<Field>, Arc<dyn Array>)> = Vec::new();
                let num_rows = source_col.len();

                // For each field in the target schema
                for target_child_field in target_fields {
                    // Create Arc<Field> directly (not Arc<Arc<Field>>)
                    let field_arc = Arc::clone(target_child_field);

                    // Try to find corresponding field in source
                    match struct_array.column_by_name(target_child_field.name()) {
                        Some(source_child_col) => {
                            // Field exists in source, adapt it
                            let adapted_child =
                                adapt_column(source_child_col, target_child_field)?;
                            children.push((field_arc, adapted_child));
                        }
                        None => {
                            // Field doesn't exist in source, add null array
                            children.push((
                                field_arc,
                                new_null_array(target_child_field.data_type(), num_rows),
                            ));
                        }
                    }
                }

                // Create new struct array with all target fields
                let struct_array = StructArray::from(children);
                Ok(Arc::new(struct_array))
            } else {
                // Not a struct array, but target expects struct - return nulls
                Ok(new_null_array(target_field.data_type(), source_col.len()))
            }
        }
        // For non-struct types, just cast
        _ => Ok(cast(source_col, target_field.data_type())?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, Int32Array, StringBuilder, StructArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion_common::ScalarValue;

    // ================================
    // Schema Creation Helper Functions
    // ================================

    /// Helper function to create a flat schema without nested fields
    fn create_flat_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user", DataType::Utf8, true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]))
    }

    /// Helper function to create a nested schema with struct and list types
    fn create_nested_schema() -> SchemaRef {
        // Define user_info struct fields to reuse for list of structs
        let user_info_fields: Vec<Field> = vec![
            Field::new("name", DataType::Utf8, true), // will map from "user" field
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ), // will map from "timestamp" field
            Field::new(
                "settings",
                DataType::Struct(
                    vec![
                        Field::new("theme", DataType::Utf8, true),
                        Field::new("notifications", DataType::Boolean, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ];

        // Create the user_info struct type
        let user_info_struct_type = DataType::Struct(user_info_fields.into());

        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            // Add a list of user_info structs (without the individual user_info field)
            Field::new(
                "user_infos",
                DataType::List(Arc::new(Field::new("item", user_info_struct_type, true))),
                true,
            ),
        ]))
    }

    /// Helper function to create a basic nested schema with additionalInfo
    fn create_basic_nested_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            create_additional_info_field(false), // without reason field
        ]))
    }

    /// Helper function to create a deeply nested schema with additionalInfo including reason field
    fn create_deep_nested_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            create_additional_info_field(true), // with reason field
        ]))
    }

    /// Helper function to create the additionalInfo field with or without the reason subfield
    fn create_additional_info_field(with_reason: bool) -> Field {
        let mut field_children = vec![
            Field::new("location", DataType::Utf8, true),
            Field::new(
                "timestamp_utc",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ];

        // Add the reason field if requested (for target schema)
        if with_reason {
            field_children.push(create_reason_field());
        }

        Field::new(
            "additionalInfo",
            DataType::Struct(field_children.into()),
            true,
        )
    }

    /// Helper function to create the reason nested field with its details subfield
    fn create_reason_field() -> Field {
        Field::new(
            "reason",
            DataType::Struct(
                vec![
                    Field::new("_level", DataType::Float64, true),
                    // Inline the details field creation
                    Field::new(
                        "details",
                        DataType::Struct(
                            vec![
                                Field::new("rurl", DataType::Utf8, true),
                                Field::new("s", DataType::Float64, true),
                                Field::new("t", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )
    }

    // ================================
    // Schema Evolution Tests
    // ================================

    #[test]
    fn test_nested_struct_evolution() -> Result<()> {
        // Test basic schema evolution with nested structs
        let source_schema = create_basic_nested_schema();
        let target_schema = create_deep_nested_schema();

        let adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());
        let adapted = adapter.adapt_schema(source_schema)?;

        // Verify the adapted schema matches target
        assert_eq!(
            adapted.fields(),
            target_schema.fields(),
            "Adapted schema should match target schema"
        );
        Ok(())
    }

    #[test]
    fn test_map_schema() -> Result<()> {
        // Create test schemas with schema evolution scenarios
        let source_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("created", DataType::Utf8, true),
                        Field::new("modified", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        // Target schema has additional fields
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("created", DataType::Utf8, true),
                        Field::new("modified", DataType::Utf8, true),
                        Field::new("version", DataType::Int64, true), // Added field
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("description", DataType::Utf8, true), // Added field
        ]));

        let adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());

        // Test schema mapping functionality
        let (_, projection) = adapter.map_schema(&source_schema)?;
        assert_eq!(
            projection.len(),
            3,
            "Projection should include all source columns"
        );
        assert_eq!(
            projection,
            vec![0, 1, 2],
            "Projection should match source column indices"
        );

        // Test schema adaptation
        let adapted = adapter.adapt_schema(Arc::new(source_schema))?;
        assert_eq!(
            adapted.fields().len(),
            4,
            "Adapted schema should have all target fields"
        );

        // Verify field presence and structure in adapted schema
        assert!(
            adapted.index_of("description").is_ok(),
            "Description field should exist in adapted schema"
        );

        if let DataType::Struct(fields) = adapted
            .field(adapted.index_of("metadata").unwrap())
            .data_type()
        {
            assert_eq!(
                fields.len(),
                3,
                "Metadata struct should have all 3 fields including version"
            );
            assert!(
                fields.iter().any(|f| f.name() == "version"),
                "Version field should exist in metadata struct"
            );
        } else {
            panic!("Expected struct type for metadata field");
        }

        Ok(())
    }

    #[test]
    fn test_adapter_factory_selection() -> Result<()> {
        // Test schemas for adapter selection logic
        let simple_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int16, true),
        ]));

        let nested_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("created", DataType::Utf8, true),
                        Field::new("modified", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        // Source schema with missing field
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("created", DataType::Utf8, true),
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
        let default_adapter = DefaultSchemaAdapterFactory
            .create(nested_schema.clone(), nested_schema.clone());
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
        let adapter = NestedStructSchemaAdapterFactory::create_adapter(
            nested_schema.clone(),
            nested_schema.clone(),
        );

        assert!(
            adapter.map_schema(&source_schema).is_ok(),
            "Factory should select appropriate adapter that handles schema evolution"
        );

        Ok(())
    }

    #[test]
    fn test_adapt_simple_to_nested_schema() -> Result<()> {
        // Test adapting a flat schema to a nested schema with struct and list fields
        let source_schema = create_flat_schema();
        let target_schema = create_nested_schema();

        let adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());
        let adapted = adapter.adapt_schema(source_schema.clone())?;

        // Verify structure of adapted schema
        assert_eq!(
            adapted.fields().len(),
            2,
            "Adapted schema should have id and user_infos fields"
        );

        // Test user_infos list field
        if let Ok(idx) = adapted.index_of("user_infos") {
            let user_infos_field = adapted.field(idx);
            assert!(
                matches!(user_infos_field.data_type(), DataType::List(_)),
                "user_infos field should be a List type"
            );

            if let DataType::List(list_field) = user_infos_field.data_type() {
                assert!(
                    matches!(list_field.data_type(), DataType::Struct(_)),
                    "List items should be Struct type"
                );

                if let DataType::Struct(fields) = list_field.data_type() {
                    assert_eq!(fields.len(), 3, "List item structs should have 3 fields");
                    assert!(
                        fields.iter().any(|f| f.name() == "settings"),
                        "List items should contain settings field"
                    );

                    // Verify settings field in list item structs
                    if let Some(settings_field) =
                        fields.iter().find(|f| f.name() == "settings")
                    {
                        if let DataType::Struct(settings_fields) =
                            settings_field.data_type()
                        {
                            assert_eq!(
                                settings_fields.len(),
                                2,
                                "Settings should have 2 fields"
                            );
                            assert!(
                                settings_fields.iter().any(|f| f.name() == "theme"),
                                "Settings should have theme field"
                            );
                            assert!(
                                settings_fields
                                    .iter()
                                    .any(|f| f.name() == "notifications"),
                                "Settings should have notifications field"
                            );
                        }
                    }
                }
            }
        } else {
            panic!("Expected user_infos field in adapted schema");
        }

        // Test mapper creation
        let (_, projection) = adapter.map_schema(&source_schema)?;
        assert_eq!(
            projection.len(),
            source_schema.fields().len(),
            "Projection should include all source fields"
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
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
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
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                        true,
                    ),
                    Field::new(
                        "reason",
                        DataType::Struct(
                            vec![
                                Field::new("_level", DataType::Float64, true),
                                Field::new(
                                    "details",
                                    DataType::Struct(
                                        vec![
                                            Field::new("rurl", DataType::Utf8, true),
                                            Field::new("s", DataType::Float64, true),
                                            Field::new("t", DataType::Utf8, true),
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
    fn create_test_batch_with_struct_data(
        file_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let mut location_builder = StringBuilder::new();
        location_builder.append_value("San Francisco");
        location_builder.append_value("New York");

        let timestamp_array = TimestampMillisecondArray::from(vec![
            Some(1640995200000), // 2022-01-01
            Some(1641081600000), // 2022-01-02
        ]);

        let info_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("location", DataType::Utf8, true)),
                Arc::new(location_builder.finish()) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new(
                    "timestamp_utc",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    true,
                )),
                Arc::new(timestamp_array),
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
        assert_eq!(reason_array.fields().size(), 2);
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

        assert_eq!(details_array.fields().size(), 3);
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

    #[test]
    fn test_schema_mapping_map_batch() -> Result<()> {
        // Test batch mapping with schema evolution
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![Field::new("created", DataType::Utf8, true)].into(),
                ),
                true,
            ),
        ]));

        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("created", DataType::Utf8, true),
                        Field::new("version", DataType::Int64, true), // Added field
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("status", DataType::Utf8, true), // Added field
        ]));

        // Create a record batch with source data
        let mut created_builder = StringBuilder::new();
        created_builder.append_value("2023-01-01");

        let metadata = StructArray::from(vec![(
            Arc::new(Field::new("created", DataType::Utf8, true)),
            Arc::new(created_builder.finish()) as Arc<dyn Array>,
        )]);

        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])), Arc::new(metadata)],
        )?;

        // Create mapping and map batch
        let field_mappings = vec![Some(0), Some(1), None]; // id, metadata, status (missing)
        let mapping =
            NestedStructSchemaMapping::new(target_schema.clone(), field_mappings);
        let mapped_batch = mapping.map_batch(batch)?;

        // Verify mapped batch
        assert_eq!(
            mapped_batch.schema(),
            target_schema,
            "Mapped batch should have target schema"
        );
        assert_eq!(
            mapped_batch.num_columns(),
            3,
            "Mapped batch should have 3 columns"
        );

        // Check metadata struct column
        if let DataType::Struct(fields) = mapped_batch.schema().field(1).data_type() {
            assert_eq!(
                fields.len(),
                2,
                "Metadata should have both created and version fields"
            );
            assert_eq!(
                fields[0].name(),
                "created",
                "First field should be 'created'"
            );
            assert_eq!(
                fields[1].name(),
                "version",
                "Second field should be 'version'"
            );
        }

        // Check added status column has nulls
        let status_col = mapped_batch.column(2);
        assert_eq!(status_col.len(), 1, "Status column should have 1 row");
        assert!(status_col.is_null(0), "Status column value should be null");

        Ok(())
    }

    #[test]
    fn test_adapt_column_with_nested_struct() -> Result<()> {
        // Test adapting a column with nested struct fields
        let source_schema = create_basic_nested_schema();
        let target_schema = create_deep_nested_schema();

        // Create batch with additionalInfo data
        let mut location_builder = StringBuilder::new();
        location_builder.append_value("USA");

        let additional_info = StructArray::from(vec![
            (
                Arc::new(Field::new("location", DataType::Utf8, true)),
                Arc::new(location_builder.finish()) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new(
                    "timestamp_utc",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )),
                Arc::new(TimestampMillisecondArray::from(vec![Some(1640995200000)])),
            ),
        ]);

        let batch =
            RecordBatch::try_new(source_schema.clone(), vec![Arc::new(additional_info)])?;

        // Map batch through adapter
        let adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());
        let (mapper, _) = adapter.map_schema(&source_schema)?;
        let mapped_batch = mapper.map_batch(batch)?;

        // Verify mapped batch structure
        assert_eq!(
            mapped_batch.schema().fields().len(),
            1,
            "Should only have additionalInfo field"
        );

        // Verify additionalInfo structure
        let mapped_batch_schema = mapped_batch.schema();
        let info_field = mapped_batch_schema.field(0);
        if let DataType::Struct(fields) = info_field.data_type() {
            assert_eq!(fields.len(), 3, "additionalInfo should have 3 fields");

            // Check the reason field structure
            if let Some(reason_field) = fields.iter().find(|f| f.name() == "reason") {
                if let DataType::Struct(reason_fields) = reason_field.data_type() {
                    assert_eq!(reason_fields.len(), 2, "reason should have 2 fields");

                    // Verify details field
                    if let Some(details_field) =
                        reason_fields.iter().find(|f| f.name() == "details")
                    {
                        if let DataType::Struct(details_fields) =
                            details_field.data_type()
                        {
                            assert_eq!(
                                details_fields.len(),
                                3,
                                "details should have 3 fields"
                            );
                            assert!(
                                details_fields.iter().any(|f| f.name() == "rurl"),
                                "details should have rurl field"
                            );
                        }
                    } else {
                        panic!("details field missing in reason struct");
                    }
                }
            } else {
                panic!("reason field missing in additionalInfo struct");
            }
        }

        // Verify data length
        assert_eq!(mapped_batch.column(0).len(), 1, "Should have 1 row");

        Ok(())
    }

    #[test]
    fn test_nested_schema_mapping_map_statistics() -> Result<()> {
        // Create file schema with struct fields
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "additionalInfo",
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )]));

        // Create table schema with additional nested struct field
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "additionalInfo",
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                        true,
                    ),
                    Field::new(
                        "reason",
                        DataType::Struct(
                            vec![
                                Field::new("_level", DataType::Float64, true),
                                Field::new(
                                    "details",
                                    DataType::Struct(
                                        vec![
                                            Field::new("rurl", DataType::Utf8, true),
                                            Field::new("s", DataType::Float64, true),
                                            Field::new("t", DataType::Utf8, true),
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

        // Create adapter
        let adapter = NestedStructSchemaAdapter::new(
            Arc::clone(&table_schema),
            Arc::clone(&table_schema),
        );

        // Map schema and get mapper
        let (mapper, _projection) = adapter.map_schema(file_schema.as_ref())?;

        // Create file column statistics
        let file_stats = vec![ColumnStatistics {
            null_count: datafusion_common::stats::Precision::Exact(5),
            max_value: datafusion_common::stats::Precision::Exact(ScalarValue::Utf8(
                Some("max_value".to_string()),
            )),
            min_value: datafusion_common::stats::Precision::Exact(ScalarValue::Utf8(
                Some("min_value".to_string()),
            )),
            sum_value: datafusion_common::stats::Precision::Exact(ScalarValue::Utf8(
                Some("sum_value".to_string()),
            )),
            distinct_count: datafusion_common::stats::Precision::Exact(100),
        }];

        // Map statistics
        let table_stats = mapper.map_column_statistics(&file_stats)?;

        // Verify statistics mapping
        assert_eq!(
            table_stats.len(),
            1,
            "Should have stats for one struct column"
        );

        // The file column stats should be preserved in the mapped result
        assert_eq!(
            table_stats[0].null_count,
            datafusion_common::stats::Precision::Exact(5),
            "Null count should be preserved"
        );

        assert_eq!(
            table_stats[0].distinct_count,
            datafusion_common::stats::Precision::Exact(100),
            "Distinct count should be preserved"
        );

        assert_eq!(
            table_stats[0].max_value,
            datafusion_common::stats::Precision::Exact(ScalarValue::Utf8(Some(
                "max_value".to_string()
            ))),
            "Max value should be preserved"
        );

        assert_eq!(
            table_stats[0].min_value,
            datafusion_common::stats::Precision::Exact(ScalarValue::Utf8(Some(
                "min_value".to_string()
            ))),
            "Min value should be preserved"
        );

        // Test with missing statistics
        let empty_stats = vec![];
        let mapped_empty_stats = mapper.map_column_statistics(&empty_stats)?;

        assert_eq!(
            mapped_empty_stats.len(),
            1,
            "Should have stats for one column even with empty input"
        );

        assert_eq!(
            mapped_empty_stats[0],
            ColumnStatistics::new_unknown(),
            "Empty input should result in unknown statistics"
        );

        Ok(())
    }

    #[test]
    fn test_nested_struct_mapping_multiple_columns() -> Result<()> {
        // Test with multiple columns including nested structs

        // Create file schema with an ID column and a struct column
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "additionalInfo",
                DataType::Struct(
                    vec![
                        Field::new("location", DataType::Utf8, true),
                        Field::new(
                            "timestamp_utc",
                            DataType::Timestamp(
                                TimeUnit::Millisecond,
                                Some("UTC".into()),
                            ),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        // Create table schema with an extra field in struct and extra column
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "additionalInfo",
                DataType::Struct(
                    vec![
                        Field::new("location", DataType::Utf8, true),
                        Field::new(
                            "timestamp_utc",
                            DataType::Timestamp(
                                TimeUnit::Millisecond,
                                Some("UTC".into()),
                            ),
                            true,
                        ),
                        Field::new(
                            "reason",
                            DataType::Struct(
                                vec![
                                    Field::new("_level", DataType::Float64, true),
                                    Field::new(
                                        "details",
                                        DataType::Struct(
                                            vec![
                                                Field::new("rurl", DataType::Utf8, true),
                                                Field::new("s", DataType::Float64, true),
                                                Field::new("t", DataType::Utf8, true),
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
            ),
            Field::new("status", DataType::Utf8, true), // Extra column in table schema
        ]));

        // Create adapter and mapping
        let adapter = NestedStructSchemaAdapter::new(
            Arc::clone(&table_schema),
            Arc::clone(&table_schema),
        );

        let (mapper, _projection) = adapter.map_schema(file_schema.as_ref())?;

        // Create file column statistics
        let file_stats = vec![
            ColumnStatistics {
                // Statistics for ID column
                null_count: datafusion_common::stats::Precision::Exact(0),
                min_value: datafusion_common::stats::Precision::Exact(
                    ScalarValue::Int32(Some(1)),
                ),
                max_value: datafusion_common::stats::Precision::Exact(
                    ScalarValue::Int32(Some(100)),
                ),
                sum_value: datafusion_common::stats::Precision::Exact(
                    ScalarValue::Int32(Some(5100)),
                ),
                distinct_count: datafusion_common::stats::Precision::Exact(100),
            },
            ColumnStatistics {
                // Statistics for additionalInfo column
                null_count: datafusion_common::stats::Precision::Exact(10),
                ..Default::default()
            },
        ];

        // Map statistics
        let table_stats = mapper.map_column_statistics(&file_stats)?;

        // Verify mapped statistics
        assert_eq!(
            table_stats.len(),
            3,
            "Should have stats for all 3 columns in table schema"
        );

        // ID column stats should be preserved
        assert_eq!(
            table_stats[0].null_count,
            datafusion_common::stats::Precision::Exact(0),
            "ID null count should be preserved"
        );

        assert_eq!(
            table_stats[0].min_value,
            datafusion_common::stats::Precision::Exact(ScalarValue::Int32(Some(1))),
            "ID min value should be preserved"
        );

        // additionalInfo column stats should be preserved
        assert_eq!(
            table_stats[1].null_count,
            datafusion_common::stats::Precision::Exact(10),
            "additionalInfo null count should be preserved"
        );

        // status column should have unknown stats
        assert_eq!(
            table_stats[2],
            ColumnStatistics::new_unknown(),
            "Missing column should have unknown statistics"
        );

        Ok(())
    }
}
