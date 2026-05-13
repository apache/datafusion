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

//! Internal helpers for recursive CTE schema reconciliation.
//!
//! Recursive CTE work-table references and children must expose schemas that
//! are conservative for nullability, while preserving every other schema
//! dimension exactly.

use std::sync::Arc;

use arrow::datatypes::{FieldRef, Schema, SchemaRef};

use crate::{DFSchema, DFSchemaRef, DataFusionError, Result};

/// Return an Arrow schema with all fields marked nullable, preserving field and
/// schema metadata.
#[doc(hidden)]
pub fn make_schema_nullable(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

/// Return a recursive query output schema that preserves `static_schema` except
/// for nullability widened by `recursive_schema`.
///
/// This helper assumes recursive term expressions have already been coerced to
/// the static term's schema, and only reads field nullability from
/// `recursive_schema`. All other output schema dimensions come from
/// `static_schema`.
#[doc(hidden)]
pub fn recursive_query_output_schema(
    static_schema: &DFSchema,
    recursive_schema: &DFSchema,
) -> Result<DFSchemaRef> {
    if static_schema.fields().len() != recursive_schema.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "Non-recursive term and recursive term must have the same number of columns ({} != {})",
            static_schema.fields().len(),
            recursive_schema.fields().len()
        )));
    }

    let fields = static_schema
        .iter()
        .zip(recursive_schema.fields())
        .map(|((qualifier, static_field), recursive_field)| {
            let field = static_field
                .as_ref()
                .clone()
                .with_nullable(
                    static_field.is_nullable() || recursive_field.is_nullable(),
                )
                .into();
            (qualifier.cloned(), field)
        })
        .collect::<Vec<_>>();

    DFSchema::new_with_metadata(fields, static_schema.metadata().clone())?
        .with_functional_dependencies(static_schema.functional_dependencies().clone())
        .map(DFSchemaRef::new)
}

/// Reconcile `logical_schema` with an Arrow schema, but only when the Arrow
/// schema differs by being more nullable. Returns `Ok(None)` if any other
/// schema dimension differs, so callers can report their normal schema error.
#[doc(hidden)]
pub fn reconcile_dfschema_with_schema_nullability(
    logical_schema: &DFSchema,
    physical_schema: &Schema,
) -> Result<Option<DFSchema>> {
    if logical_schema.metadata() != physical_schema.metadata()
        || logical_schema.fields().len() != physical_schema.fields().len()
    {
        return Ok(None);
    }

    let physical_fields = physical_schema.fields().iter();
    widen_dfschema_nullability_with_fields(logical_schema, physical_fields)
}

fn widen_dfschema_nullability_with_fields<'a>(
    base_schema: &DFSchema,
    widening_fields: impl Iterator<Item = &'a FieldRef>,
) -> Result<Option<DFSchema>> {
    let mut widened_nullability = false;
    let mut fields = Vec::with_capacity(base_schema.fields().len());

    for ((qualifier, base_field), widening_field) in
        base_schema.iter().zip(widening_fields)
    {
        if base_field.name() != widening_field.name()
            || base_field.data_type() != widening_field.data_type()
            || base_field.metadata() != widening_field.metadata()
        {
            return Ok(None);
        }

        widened_nullability |= !base_field.is_nullable() && widening_field.is_nullable();
        let field = base_field
            .as_ref()
            .clone()
            .with_nullable(base_field.is_nullable() || widening_field.is_nullable())
            .into();
        fields.push((qualifier.cloned(), field));
    }

    if !widened_nullability {
        return Ok(None);
    }

    DFSchema::new_with_metadata(fields, base_schema.metadata().clone())?
        .with_functional_dependencies(base_schema.functional_dependencies().clone())
        .map(Some)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::ToDFSchema as _;

    use super::*;

    #[test]
    fn make_schema_nullable_preserves_metadata() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("c1", DataType::Int32, false)
                    .with_metadata(HashMap::from([("field".into(), "value".into())])),
            ],
            HashMap::from([("schema".into(), "value".into())]),
        );

        let nullable = make_schema_nullable(&schema);

        assert!(nullable.field(0).is_nullable());
        assert_eq!(nullable.field(0).metadata(), schema.field(0).metadata());
        assert_eq!(nullable.metadata(), schema.metadata());
    }

    #[test]
    fn recursive_output_schema_preserves_static_dimensions_and_widens_nullability() {
        let static_schema = Schema::new_with_metadata(
            vec![
                Field::new("anchor_name", DataType::Int32, false)
                    .with_metadata(HashMap::from([("field".into(), "value".into())])),
            ],
            HashMap::from([("schema".into(), "value".into())]),
        )
        .to_dfschema_ref()
        .unwrap();
        let recursive_schema = Schema::new(vec![Field::new(
            "recursive_expr_name",
            DataType::Int32,
            true,
        )])
        .to_dfschema_ref()
        .unwrap();

        let output =
            recursive_query_output_schema(&static_schema, &recursive_schema).unwrap();

        assert_eq!(output.field(0).name(), "anchor_name");
        assert_eq!(output.field(0).data_type(), &DataType::Int32);
        assert_eq!(
            output.field(0).metadata(),
            static_schema.field(0).metadata()
        );
        assert_eq!(output.metadata(), static_schema.metadata());
        assert!(output.field(0).is_nullable());
    }

    #[test]
    fn reconciliation_only_widens_nullability() {
        let logical_schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)])
            .to_dfschema_ref()
            .unwrap();
        let physical_schema = Schema::new(vec![Field::new("c1", DataType::Int32, true)]);

        let reconciled =
            reconcile_dfschema_with_schema_nullability(&logical_schema, &physical_schema)
                .unwrap()
                .expect("nullability widening should reconcile");

        assert!(reconciled.field(0).is_nullable());
        assert_eq!(reconciled.field(0).name(), "c1");
        assert_eq!(reconciled.field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn reconciliation_rejects_other_mismatches() {
        let logical_schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)])
            .to_dfschema_ref()
            .unwrap();

        let cases = [
            Schema::new(vec![
                Field::new("c1", DataType::Int32, true),
                Field::new("c2", DataType::Int32, true),
            ]),
            Schema::new(vec![Field::new("different", DataType::Int32, true)]),
            Schema::new(vec![Field::new("c1", DataType::Int64, true)]),
            Schema::new(vec![
                Field::new("c1", DataType::Int32, true)
                    .with_metadata(HashMap::from([("key".into(), "value".into())])),
            ]),
            Schema::new(vec![Field::new("c1", DataType::Int32, true)])
                .with_metadata(HashMap::from([("key".into(), "value".into())])),
        ];

        for physical_schema in cases {
            assert!(
                reconcile_dfschema_with_schema_nullability(
                    &logical_schema,
                    &physical_schema,
                )
                .unwrap()
                .is_none(),
                "should not reconcile unsupported mismatch: {physical_schema:?}"
            );
        }
    }
}
