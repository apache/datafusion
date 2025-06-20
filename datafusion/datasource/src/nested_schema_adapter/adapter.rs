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

use crate::schema_adapter::{
    create_field_mapping, SchemaAdapter, SchemaMapper, SchemaMapping,
};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType::Struct, Field, Schema, SchemaRef},
};
use datafusion_common::nested_struct::{cast_column, validate_struct_compatibility};
use datafusion_common::Result;
use std::sync::Arc;

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
        let (field_mappings, projection) = create_field_mapping(
            file_schema,
            &self.projected_table_schema,
            |file_field, table_field| {
                // Special handling for struct fields - validate internal structure compatibility
                match (file_field.data_type(), table_field.data_type()) {
                    (Struct(source_fields), Struct(target_fields)) => {
                        validate_struct_compatibility(source_fields, target_fields)
                    }
                    _ => crate::schema_adapter::can_cast_field(file_field, table_field),
                }
            },
        )?;

        Ok((
            Arc::new(SchemaMapping::new(
                Arc::clone(&self.projected_table_schema),
                field_mappings,
                Arc::new(|array: &ArrayRef, field: &Field| cast_column(array, field)),
            )),
            projection,
        ))
    }
}
