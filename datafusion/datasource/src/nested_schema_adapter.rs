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
    pub fn create_appropriate_adapter(
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use arrow::datatypes::TimeUnit;
    // Add imports for the new test
    use arrow::array::{
        Array, Float64Array, StringArray, StructArray, TimestampMillisecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use datafusion_common::DataFusionError;

    use datafusion_expr::col;
    use std::fs;

    #[test]
    fn test_nested_struct_evolution() -> Result<()> {
        // Create source and target schemas using helper functions
        let source_schema = create_basic_nested_schema();
        let target_schema = create_deep_nested_schema();

        let adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());
        let adapted = adapter.adapt_schema(source_schema)?;

        // Verify the adapted schema matches target
        assert_eq!(adapted.fields(), target_schema.fields());
        Ok(())
    }

    /// Helper function to create a basic schema with a simple nested struct
    fn create_basic_nested_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            create_additional_info_field(false), // without reason field
        ]))
    }

    /// Helper function to create an enhanced schema with deeper nested structs
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

    /// Helper function to create the reason nested field
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

        let adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());
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

    #[test]
    fn test_create_appropriate_adapter() -> Result<()> {
        // Setup test schemas
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

        // Create source schema with missing field in struct
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

        // Test has_nested_structs detection
        assert!(!NestedStructSchemaAdapterFactory::has_nested_structs(
            &simple_schema
        ));
        assert!(NestedStructSchemaAdapterFactory::has_nested_structs(
            &nested_schema
        ));

        // Test DefaultSchemaAdapter fails with nested schema evolution
        let default_adapter = DefaultSchemaAdapterFactory
            .create(nested_schema.clone(), nested_schema.clone());
        let default_result = default_adapter.map_schema(&source_schema);

        assert!(default_result.is_err());
        if let Err(e) = default_result {
            assert!(
                format!("{}", e).contains("Cannot cast file schema field metadata"),
                "Expected casting error, got: {e}"
            );
        }

        // Test NestedStructSchemaAdapter handles the same case successfully
        let nested_adapter = NestedStructSchemaAdapterFactory
            .create(nested_schema.clone(), nested_schema.clone());
        assert!(nested_adapter.map_schema(&source_schema).is_ok());

        // Test factory selects appropriate adapter based on schema
        let complex_adapter =
            NestedStructSchemaAdapterFactory::create_appropriate_adapter(
                nested_schema.clone(),
                nested_schema.clone(),
            );

        // Verify complex_adapter can handle schema evolution
        assert!(
            complex_adapter.map_schema(&source_schema).is_ok(),
            "Complex adapter should handle schema with missing fields"
        );

        Ok(())
    }

    #[test]
    fn test_adapt_simple_to_nested_schema() -> Result<()> {
        // Simple source schema with flat fields
        let source_schema = create_flat_schema();

        // Target schema with nested struct fields
        let target_schema = create_nested_schema();

        // Create mapping with our adapter - should handle missing nested fields
        let nested_adapter =
            NestedStructSchemaAdapter::new(target_schema.clone(), target_schema.clone());
        let adapted = nested_adapter.adapt_schema(source_schema.clone())?;

        // Verify structure of adapted schema
        assert_eq!(adapted.fields().len(), 2); // Should have id and user_info

        // Check that user_info is a struct
        if let Ok(idx) = adapted.index_of("user_info") {
            let user_info_field = adapted.field(idx);
            assert!(matches!(user_info_field.data_type(), DataType::Struct(_)));

            if let DataType::Struct(fields) = user_info_field.data_type() {
                assert_eq!(fields.len(), 3); // Should have name, created_at, and settings

                // Check that settings field exists and is a struct
                let settings_idx = fields.iter().position(|f| f.name() == "settings");
                assert!(settings_idx.is_some(), "Settings field should exist");

                let settings_field = &fields[settings_idx.unwrap()];
                assert!(matches!(settings_field.data_type(), DataType::Struct(_)));

                if let DataType::Struct(settings_fields) = settings_field.data_type() {
                    assert_eq!(settings_fields.len(), 2); // Should have theme and notifications

                    // Verify field names within settings
                    let theme_exists =
                        settings_fields.iter().any(|f| f.name() == "theme");
                    let notif_exists =
                        settings_fields.iter().any(|f| f.name() == "notifications");

                    assert!(theme_exists, "Settings should contain theme field");
                    assert!(notif_exists, "Settings should contain notifications field");
                } else {
                    panic!("Expected struct type for settings field");
                }
            } else {
                panic!("Expected struct type for user_info field");
            }
        } else {
            panic!("Expected user_info field in adapted schema");
        }

        // Test mapper creation
        let (_mapper, projection) = nested_adapter.map_schema(&source_schema)?;

        // Verify the mapper was created successfully and projection includes expected columns
        assert_eq!(projection.len(), source_schema.fields().len());

        // Or check against the adapted schema we already confirmed is correct
        assert_eq!(adapted.fields().len(), 2);

        Ok(())
    }

    fn create_nested_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "user_info",
                DataType::Struct(
                    vec![
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
                    ]
                    .into(),
                ),
                true,
            ),
        ]))
    }

    fn create_flat_schema() -> Arc<Schema> {
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

    #[tokio::test]
    async fn test_datafusion_schema_evolution_with_compaction(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use datafusion_expr::col;
        let ctx = SessionContext::new();

        let schema1 = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("stack", DataType::Utf8, true),
            Field::new("timestamp", DataType::Utf8, true),
            Field::new(
                "timestamp_utc",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
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
            ),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("component1")])),
                Arc::new(StringArray::from(vec![Some("message1")])),
                Arc::new(StringArray::from(vec![Some("stack_trace")])),
                Arc::new(StringArray::from(vec![Some("2025-02-18T00:00:00Z")])),
                Arc::new(TimestampMillisecondArray::from(vec![Some(1640995200000)])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("location", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec![Some("USA")])) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new(
                            "timestamp_utc",
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        )),
                        Arc::new(TimestampMillisecondArray::from(vec![Some(
                            1640995200000,
                        )])),
                    ),
                ])),
            ],
        )?;

        let path1 = "test_data1.parquet";
        let _ = fs::remove_file(path1);

        let df1 = ctx.read_batch(batch1)?;
        df1.write_parquet(
            path1,
            DataFrameWriteOptions::default()
                .with_single_file_output(true)
                .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
            None,
        )
        .await?;

        let schema2 = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
            Field::new("stack", DataType::Utf8, true),
            Field::new("timestamp", DataType::Utf8, true),
            Field::new(
                "timestamp_utc",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
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
            ),
        ]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("component1")])),
                Arc::new(StringArray::from(vec![Some("message1")])),
                Arc::new(StringArray::from(vec![Some("stack_trace")])),
                Arc::new(StringArray::from(vec![Some("2025-02-18T00:00:00Z")])),
                Arc::new(TimestampMillisecondArray::from(vec![Some(1640995200000)])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("location", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec![Some("USA")])) as Arc<dyn Array>,
                    ),
                    (
                        Arc::new(Field::new(
                            "timestamp_utc",
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        )),
                        Arc::new(TimestampMillisecondArray::from(vec![Some(
                            1640995200000,
                        )])),
                    ),
                    (
                        Arc::new(Field::new(
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
                        )),
                        Arc::new(StructArray::from(vec![
                            (
                                Arc::new(Field::new("_level", DataType::Float64, true)),
                                Arc::new(Float64Array::from(vec![Some(1.5)]))
                                    as Arc<dyn Array>,
                            ),
                            (
                                Arc::new(Field::new(
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
                                )),
                                Arc::new(StructArray::from(vec![
                                    (
                                        Arc::new(Field::new(
                                            "rurl",
                                            DataType::Utf8,
                                            true,
                                        )),
                                        Arc::new(StringArray::from(vec![Some(
                                            "https://example.com",
                                        )]))
                                            as Arc<dyn Array>,
                                    ),
                                    (
                                        Arc::new(Field::new(
                                            "s",
                                            DataType::Float64,
                                            true,
                                        )),
                                        Arc::new(Float64Array::from(vec![Some(3.14)]))
                                            as Arc<dyn Array>,
                                    ),
                                    (
                                        Arc::new(Field::new("t", DataType::Utf8, true)),
                                        Arc::new(StringArray::from(vec![Some("data")]))
                                            as Arc<dyn Array>,
                                    ),
                                ])),
                            ),
                        ])),
                    ),
                ])),
            ],
        )?;

        let path2 = "test_data2.parquet";
        let _ = fs::remove_file(path2);

        let df2 = ctx.read_batch(batch2)?;
        df2.write_parquet(
            path2,
            DataFrameWriteOptions::default()
                .with_single_file_output(true)
                .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
            None,
        )
        .await?;

        let paths_str = vec![path1.to_string(), path2.to_string()];
        let config = ListingTableConfig::new_with_multi_paths(
            paths_str
                .into_iter()
                .map(|p| ListingTableUrl::parse(&p))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .with_schema(schema2.as_ref().clone().into())
        .infer(&ctx.state())
        .await?;

        let config = ListingTableConfig {
            options: Some(ListingOptions {
                file_sort_order: vec![vec![col("timestamp_utc").sort(true, true)]],
                ..config.options.unwrap_or_else(|| {
                    ListingOptions::new(Arc::new(ParquetFormat::default()))
                })
            }),
            ..config
        };

        let listing_table = ListingTable::try_new(config)?;
        ctx.register_table("events", Arc::new(listing_table))?;

        let df = ctx
            .sql("SELECT * FROM events ORDER BY timestamp_utc")
            .await?;
        let results = df.clone().collect().await?;

        assert_eq!(results[0].num_rows(), 2);

        let compacted_path = "test_data_compacted.parquet";
        let _ = fs::remove_file(compacted_path);

        df.write_parquet(
            compacted_path,
            DataFrameWriteOptions::default()
                .with_single_file_output(true)
                .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
            None,
        )
        .await?;

        let new_ctx = SessionContext::new();
        let config =
            ListingTableConfig::new_with_multi_paths(vec![ListingTableUrl::parse(
                compacted_path,
            )?])
            .with_schema(schema2.as_ref().clone().into())
            .infer(&new_ctx.state())
            .await?;

        let listing_table = ListingTable::try_new(config)?;
        new_ctx.register_table("events", Arc::new(listing_table))?;

        let df = new_ctx
            .sql("SELECT * FROM events ORDER BY timestamp_utc")
            .await?;
        let compacted_results = df.collect().await?;

        assert_eq!(compacted_results[0].num_rows(), 2);
        assert_eq!(results, compacted_results);

        let _ = fs::remove_file(path1);
        let _ = fs::remove_file(path2);
        let _ = fs::remove_file(compacted_path);

        Ok(())
    }
}
