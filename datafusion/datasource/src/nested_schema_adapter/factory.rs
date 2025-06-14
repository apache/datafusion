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

use super::adapter::NestedStructSchemaAdapter;
use crate::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
use arrow::datatypes::{DataType::Struct, Schema, SchemaRef};
use std::sync::Arc;

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
            .any(|field| matches!(field.data_type(), Struct(_)))
    }
}
