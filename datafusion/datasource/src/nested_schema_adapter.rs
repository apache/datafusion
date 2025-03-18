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

//! [`SchemaAdapter`] and [`SchemaAdapterFactory`] to adapt file-level record batches to a table schema.
//!
//! Adapter provides a method of translating the RecordBatches that come out of the
//! physical format into how they should be used by DataFusion.  For instance, a schema
//! can be stored external to a parquet file that maps parquet logical types to arrow types.

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion_common::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::schema_adapter::DefaultSchemaAdapterFactory;
use crate::schema_adapter::SchemaAdapter;
use crate::schema_adapter::SchemaAdapterFactory;
use crate::schema_adapter::SchemaMapper;
use crate::schema_adapter::SchemaMapping;

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
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(NestedStructSchemaAdapter::new(projected_table_schema))
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
    /// Returns a NestedStructSchemaAdapter for schemas with nested structs,
    /// or falls back to DefaultSchemaAdapter for simple schemas.
    pub fn create_appropriate_adapter(
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        if Self::has_nested_structs(projected_table_schema.as_ref()) {
            NestedStructSchemaAdapterFactory.create(projected_table_schema, table_schema)
        } else {
            DefaultSchemaAdapterFactory.create(projected_table_schema, table_schema)
        }
    }
}

/// A SchemaAdapter that handles schema evolution for nested struct types
#[derive(Debug, Clone)]
pub struct NestedStructSchemaAdapter {
    target_schema: SchemaRef,
}

impl NestedStructSchemaAdapter {
    /// Create a new NestedStructSchemaAdapter with the target schema
    pub fn new(target_schema: SchemaRef) -> Self {
        Self { target_schema }
    }

    /// Adapt the source schema fields to match the target schema while preserving
    /// nested struct fields and handling field additions/removals
    fn adapt_fields(&self, source_fields: &Fields, target_fields: &Fields) -> Vec<Field> {
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
                                self.adapt_fields(source_children, target_children);
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

    fn adapt_schema(&self, source_schema: SchemaRef) -> Result<SchemaRef> {
        let adapted_fields =
            self.adapt_fields(source_schema.fields(), self.target_schema.fields());

        Ok(Arc::new(Schema::new_with_metadata(
            adapted_fields,
            self.target_schema.metadata().clone(),
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

        // Create a SchemaMapping with appropriate mappings
        let mapping = SchemaMapping::new(
            Arc::new(target_schema.clone()), // projected_table_schema
            field_mappings,                  // field_mappings
            Arc::new(source_schema.clone()), // full table_schema
        );

        Ok(Arc::new(mapping))
    }
}

impl SchemaAdapter for NestedStructSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field_name = self.target_schema.field(index).name();
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn test_nested_struct_evolution() -> Result<()> {
        // Original schema with basic nested struct
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "additionalInfo",
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )]));

        // Enhanced schema with new nested fields
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "additionalInfo",
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
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

        let adapter = NestedStructSchemaAdapter::new(target_schema.clone());
        let adapted = adapter.adapt_schema(source_schema)?;

        // Verify the adapted schema matches target
        assert_eq!(adapted.fields(), target_schema.fields());
        Ok(())
    }

    #[test]
    fn test_map_schema() -> Result<()> {
        // Create source schema with a subset of fields
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

        // Create target schema with additional/different fields
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

        let adapter = NestedStructSchemaAdapter::new(target_schema.clone());
        let (_, projection) = adapter.map_schema(&source_schema)?;

        // Verify projection contains all columns from source schema
        assert_eq!(projection.len(), 3);
        assert_eq!(projection, vec![0, 1, 2]);

        // Verify adapted schema separately
        let adapted = adapter.adapt_schema(Arc::new(source_schema))?;
        assert_eq!(adapted.fields().len(), 4); // Should have all target fields

        // Check if description field exists
        let description_idx = adapted.index_of("description");
        assert!(description_idx.is_ok(), "Should have description field");

        // Check nested struct has the new field
        let metadata_idx = adapted.index_of("metadata").unwrap();
        let metadata_field = adapted.field(metadata_idx);
        if let DataType::Struct(fields) = metadata_field.data_type() {
            assert_eq!(fields.len(), 3); // Should have all 3 fields including version

            // Find version field in the Fields collection
            let version_exists = fields.iter().any(|f| f.name() == "version");
            assert!(
                version_exists,
                "Should have version field in metadata struct"
            );
        } else {
            panic!("Expected struct type for metadata field");
        }

        Ok(())
    }
}
