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

//! DFSchema is an extended schema struct that DataFusion uses to provide support for
//! fields with optional relation names.

use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use crate::logical_plan::Column;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::fmt::{Display, Formatter};

/// A reference-counted reference to a `DFSchema`.
pub type DFSchemaRef = Arc<DFSchema>;

/// DFSchema wraps an Arrow schema and adds relation names
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFSchema {
    /// Fields
    fields: Vec<DFField>,
}

impl DFSchema {
    /// Creates an empty `DFSchema`
    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    /// Create a new `DFSchema`
    pub fn new(fields: Vec<DFField>) -> Result<Self> {
        let mut qualified_names = HashSet::new();
        let mut unqualified_names = HashSet::new();

        for field in &fields {
            if let Some(qualifier) = field.qualifier() {
                if !qualified_names.insert((qualifier, field.name())) {
                    return Err(DataFusionError::Plan(format!(
                        "Schema contains duplicate qualified field name '{}'",
                        field.qualified_name()
                    )));
                }
            } else if !unqualified_names.insert(field.name()) {
                return Err(DataFusionError::Plan(format!(
                    "Schema contains duplicate unqualified field name '{}'",
                    field.name()
                )));
            }
        }

        // check for mix of qualified and unqualified field with same unqualified name
        // note that we need to sort the contents of the HashSet first so that errors are
        // deterministic
        let mut qualified_names = qualified_names
            .iter()
            .map(|(l, r)| (l.to_owned(), r.to_owned()))
            .collect::<Vec<(&String, &String)>>();
        qualified_names.sort_by(|a, b| {
            let a = format!("{}.{}", a.0, a.1);
            let b = format!("{}.{}", b.0, b.1);
            a.cmp(&b)
        });
        for (qualifier, name) in &qualified_names {
            if unqualified_names.contains(name) {
                return Err(DataFusionError::Plan(format!(
                    "Schema contains qualified field name '{}.{}' \
                    and unqualified field name '{}' which would be ambiguous",
                    qualifier, name, name
                )));
            }
        }
        Ok(Self { fields })
    }

    /// Create a `DFSchema` from an Arrow schema
    pub fn try_from_qualified_schema(qualifier: &str, schema: &Schema) -> Result<Self> {
        Self::new(
            schema
                .fields()
                .iter()
                .map(|f| DFField::from_qualified(qualifier, f.clone()))
                .collect(),
        )
    }

    /// Combine two schemas
    pub fn join(&self, schema: &DFSchema) -> Result<Self> {
        let mut fields = self.fields.clone();
        fields.extend_from_slice(schema.fields().as_slice());
        Self::new(fields)
    }

    /// Merge a schema into self
    pub fn merge(&mut self, other_schema: &DFSchema) {
        for field in other_schema.fields() {
            // skip duplicate columns
            let duplicated_field = match field.qualifier() {
                Some(q) => self.field_with_name(Some(q.as_str()), field.name()).is_ok(),
                // for unqualifed columns, check as unqualified name
                None => self.field_with_unqualified_name(field.name()).is_ok(),
            };
            if !duplicated_field {
                self.fields.push(field.clone());
            }
        }
    }

    /// Get a list of fields
    pub fn fields(&self) -> &Vec<DFField> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector
    pub fn field(&self, i: usize) -> &DFField {
        &self.fields[i]
    }

    /// Find the index of the column with the given unqualified name
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            }
        }
        Err(DataFusionError::Plan(format!(
            "No field named '{}'. Valid fields are {}.",
            name,
            self.get_field_names()
        )))
    }

    fn index_of_column_by_name(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<usize> {
        let mut matches = self
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| match (qualifier, &field.qualifier) {
                // field to lookup is qualified.
                // current field is qualified and not shared between relations, compare both
                // qualifier and name.
                (Some(q), Some(field_q)) => q == field_q && field.name() == name,
                // field to lookup is qualified but current field is unqualified.
                (Some(_), None) => false,
                // field to lookup is unqualified, no need to compare qualifier
                (None, Some(_)) | (None, None) => field.name() == name,
            })
            .map(|(idx, _)| idx);
        match matches.next() {
            None => Err(DataFusionError::Plan(format!(
                "No field named '{}.{}'. Valid fields are {}.",
                qualifier.unwrap_or("<unqualified>"),
                name,
                self.get_field_names()
            ))),
            Some(idx) => match matches.next() {
                None => Ok(idx),
                // found more than one matches
                Some(_) => Err(DataFusionError::Internal(format!(
                    "Ambiguous reference to qualified field named '{}.{}'",
                    qualifier.unwrap_or("<unqualified>"),
                    name
                ))),
            },
        }
    }

    /// Find the index of the column with the given qualifier and name
    pub fn index_of_column(&self, col: &Column) -> Result<usize> {
        self.index_of_column_by_name(col.relation.as_deref(), &col.name)
    }

    /// Find the field with the given name
    pub fn field_with_name(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<&DFField> {
        if let Some(qualifier) = qualifier {
            self.field_with_qualified_name(qualifier, name)
        } else {
            self.field_with_unqualified_name(name)
        }
    }

    /// Find all fields match the given name
    pub fn fields_with_unqualified_name(&self, name: &str) -> Vec<&DFField> {
        self.fields
            .iter()
            .filter(|field| field.name() == name)
            .collect()
    }

    /// Find the field with the given name
    pub fn field_with_unqualified_name(&self, name: &str) -> Result<&DFField> {
        let matches = self.fields_with_unqualified_name(name);
        match matches.len() {
            0 => Err(DataFusionError::Plan(format!(
                "No field with unqualified name '{}'. Valid fields are {}.",
                name,
                self.get_field_names()
            ))),
            1 => Ok(matches[0]),
            _ => Err(DataFusionError::Plan(format!(
                "Ambiguous reference to field named '{}'",
                name
            ))),
        }
    }

    /// Find the field with the given qualified name
    pub fn field_with_qualified_name(
        &self,
        qualifier: &str,
        name: &str,
    ) -> Result<&DFField> {
        let idx = self.index_of_column_by_name(Some(qualifier), name)?;
        Ok(self.field(idx))
    }

    /// Find the field with the given qualified column
    pub fn field_from_column(&self, column: &Column) -> Result<&DFField> {
        match &column.relation {
            Some(r) => self.field_with_qualified_name(r, &column.name),
            None => self.field_with_unqualified_name(&column.name),
        }
    }

    /// Check to see if unqualified field names matches field names in Arrow schema
    pub fn matches_arrow_schema(&self, arrow_schema: &Schema) -> bool {
        self.fields
            .iter()
            .zip(arrow_schema.fields().iter())
            .all(|(dffield, arrowfield)| dffield.name() == arrowfield.name())
    }

    /// Strip all field qualifier in schema
    pub fn strip_qualifiers(self) -> Self {
        DFSchema {
            fields: self
                .fields
                .into_iter()
                .map(|f| f.strip_qualifier())
                .collect(),
        }
    }

    /// Replace all field qualifier with new value in schema
    pub fn replace_qualifier(self, qualifier: &str) -> Self {
        DFSchema {
            fields: self
                .fields
                .into_iter()
                .map(|f| {
                    DFField::new(
                        Some(qualifier),
                        f.name(),
                        f.data_type().to_owned(),
                        f.is_nullable(),
                    )
                })
                .collect(),
        }
    }

    /// Get comma-seperated list of field names for use in error messages
    fn get_field_names(&self) -> String {
        self.fields
            .iter()
            .map(|f| match f.qualifier() {
                Some(qualifier) => format!("'{}.{}'", qualifier, f.name()),
                None => format!("'{}'", f.name()),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Into<Schema> for DFSchema {
    /// Convert a schema into a DFSchema
    fn into(self) -> Schema {
        Schema::new(
            self.fields
                .into_iter()
                .map(|f| {
                    if f.qualifier().is_some() {
                        Field::new(
                            f.name().as_str(),
                            f.data_type().to_owned(),
                            f.is_nullable(),
                        )
                    } else {
                        f.field
                    }
                })
                .collect(),
        )
    }
}

impl Into<Schema> for &DFSchema {
    /// Convert a schema into a DFSchema
    fn into(self) -> Schema {
        Schema::new(self.fields.iter().map(|f| f.field.clone()).collect())
    }
}

/// Create a `DFSchema` from an Arrow schema
impl TryFrom<Schema> for DFSchema {
    type Error = DataFusionError;
    fn try_from(schema: Schema) -> std::result::Result<Self, Self::Error> {
        Self::new(
            schema
                .fields()
                .iter()
                .map(|f| DFField::from(f.clone()))
                .collect(),
        )
    }
}

impl Into<SchemaRef> for DFSchema {
    fn into(self) -> SchemaRef {
        SchemaRef::new(self.into())
    }
}

/// Convenience trait to convert Schema like things to DFSchema and DFSchemaRef with fewer keystrokes
pub trait ToDFSchema
where
    Self: Sized,
{
    /// Attempt to create a DSSchema
    #[allow(clippy::wrong_self_convention)]
    fn to_dfschema(self) -> Result<DFSchema>;

    /// Attempt to create a DSSchemaRef
    #[allow(clippy::wrong_self_convention)]
    fn to_dfschema_ref(self) -> Result<DFSchemaRef> {
        Ok(Arc::new(self.to_dfschema()?))
    }
}

impl ToDFSchema for Schema {
    #[allow(clippy::wrong_self_convention)]
    fn to_dfschema(self) -> Result<DFSchema> {
        DFSchema::try_from(self)
    }
}

impl ToDFSchema for SchemaRef {
    #[allow(clippy::wrong_self_convention)]
    fn to_dfschema(self) -> Result<DFSchema> {
        // Attempt to use the Schema directly if there are no other
        // references, otherwise clone
        match Self::try_unwrap(self) {
            Ok(schema) => DFSchema::try_from(schema),
            Err(schemaref) => DFSchema::try_from(schemaref.as_ref().clone()),
        }
    }
}

impl ToDFSchema for Vec<DFField> {
    fn to_dfschema(self) -> Result<DFSchema> {
        DFSchema::new(self)
    }
}

impl Display for DFSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.fields
                .iter()
                .map(|field| field.qualified_name())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

/// DFField wraps an Arrow field and adds an optional qualifier
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFField {
    /// Optional qualifier (usually a table or relation name)
    qualifier: Option<String>,
    /// Arrow field definition
    field: Field,
}

impl DFField {
    /// Creates a new `DFField`
    pub fn new(
        qualifier: Option<&str>,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        DFField {
            qualifier: qualifier.map(|s| s.to_owned()),
            field: Field::new(name, data_type, nullable),
        }
    }

    /// Create an unqualified field from an existing Arrow field
    pub fn from(field: Field) -> Self {
        Self {
            qualifier: None,
            field,
        }
    }

    /// Create a qualified field from an existing Arrow field
    pub fn from_qualified(qualifier: &str, field: Field) -> Self {
        Self {
            qualifier: Some(qualifier.to_owned()),
            field,
        }
    }

    /// Returns an immutable reference to the `DFField`'s unqualified name
    pub fn name(&self) -> &String {
        self.field.name()
    }

    /// Returns an immutable reference to the `DFField`'s data-type
    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    /// Indicates whether this `DFField` supports null values
    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    /// Returns a string to the `DFField`'s qualified name
    pub fn qualified_name(&self) -> String {
        if let Some(qualifier) = &self.qualifier {
            format!("{}.{}", qualifier, self.field.name())
        } else {
            self.field.name().to_owned()
        }
    }

    /// Builds a qualified column based on self
    pub fn qualified_column(&self) -> Column {
        Column {
            relation: self.qualifier.clone(),
            name: self.field.name().to_string(),
        }
    }

    /// Builds an unqualified column based on self
    pub fn unqualified_column(&self) -> Column {
        Column {
            relation: None,
            name: self.field.name().to_string(),
        }
    }

    /// Get the optional qualifier
    pub fn qualifier(&self) -> Option<&String> {
        self.qualifier.as_ref()
    }

    /// Get the arrow field
    pub fn field(&self) -> &Field {
        &self.field
    }

    /// Return field with qualifier stripped
    pub fn strip_qualifier(mut self) -> Self {
        self.qualifier = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn from_unqualified_field() {
        let field = Field::new("c0", DataType::Boolean, true);
        let field = DFField::from(field);
        assert_eq!("c0", field.name());
        assert_eq!("c0", field.qualified_name());
    }

    #[test]
    fn from_qualified_field() {
        let field = Field::new("c0", DataType::Boolean, true);
        let field = DFField::from_qualified("t1", field);
        assert_eq!("c0", field.name());
        assert_eq!("t1.c0", field.qualified_name());
    }

    #[test]
    fn from_unqualified_schema() -> Result<()> {
        let schema = DFSchema::try_from(test_schema_1())?;
        assert_eq!("c0, c1", schema.to_string());
        Ok(())
    }

    #[test]
    fn from_qualified_schema() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        assert_eq!("t1.c0, t1.c1", schema.to_string());
        Ok(())
    }

    #[test]
    fn from_qualified_schema_into_arrow_schema() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let arrow_schema: Schema = schema.into();
        let expected = "Field { name: \"c0\", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None }, \
        Field { name: \"c1\", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: None }";
        assert_eq!(expected, arrow_schema.to_string());
        Ok(())
    }

    #[test]
    fn join_qualified() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from_qualified_schema("t2", &test_schema_1())?;
        let join = left.join(&right)?;
        assert_eq!("t1.c0, t1.c1, t2.c0, t2.c1", join.to_string());
        // test valid access
        assert!(join.field_with_qualified_name("t1", "c0").is_ok());
        assert!(join.field_with_qualified_name("t2", "c0").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t2.c0").is_err());
        Ok(())
    }

    #[test]
    fn join_qualified_duplicate() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let join = left.join(&right);
        assert!(join.is_err());
        assert_eq!(
            "Error during planning: Schema contains duplicate \
        qualified field name \'t1.c0\'",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[test]
    fn join_unqualified_duplicate() -> Result<()> {
        let left = DFSchema::try_from(test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert!(join.is_err());
        assert_eq!(
            "Error during planning: Schema contains duplicate \
        unqualified field name \'c0\'",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[test]
    fn join_mixed() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from(test_schema_2())?;
        let join = left.join(&right)?;
        assert_eq!("t1.c0, t1.c1, c100, c101", join.to_string());
        // test valid access
        assert!(join.field_with_qualified_name("t1", "c0").is_ok());
        assert!(join.field_with_unqualified_name("c0").is_ok());
        assert!(join.field_with_unqualified_name("c100").is_ok());
        assert!(join.field_with_name(None, "c100").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c100").is_err());
        assert!(join.field_with_qualified_name("", "c100").is_err());
        Ok(())
    }

    #[test]
    fn join_mixed_duplicate() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert!(join.is_err());
        assert_eq!(
            "Error during planning: Schema contains qualified \
        field name \'t1.c0\' and unqualified field name \'c0\' which would be ambiguous",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[test]
    fn helpful_error_messages() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let expected_help = "Valid fields are \'t1.c0\', \'t1.c1\'.";
        assert!(schema
            .field_with_qualified_name("x", "y")
            .unwrap_err()
            .to_string()
            .contains(expected_help));
        assert!(schema
            .field_with_unqualified_name("y")
            .unwrap_err()
            .to_string()
            .contains(expected_help));
        assert!(schema
            .index_of("y")
            .unwrap_err()
            .to_string()
            .contains(expected_help));
        Ok(())
    }

    #[test]
    fn into() {
        // Demonstrate how to convert back and forth between Schema, SchemaRef, DFSchema, and DFSchemaRef
        let arrow_schema = Schema::new(vec![Field::new("c0", DataType::Int64, true)]);
        let arrow_schema_ref = Arc::new(arrow_schema.clone());

        let df_schema =
            DFSchema::new(vec![DFField::new(None, "c0", DataType::Int64, true)]).unwrap();
        let df_schema_ref = Arc::new(df_schema.clone());

        {
            let arrow_schema = arrow_schema.clone();
            let arrow_schema_ref = arrow_schema_ref.clone();

            assert_eq!(df_schema, arrow_schema.to_dfschema().unwrap());
            assert_eq!(df_schema, arrow_schema_ref.to_dfschema().unwrap());
        }

        {
            let arrow_schema = arrow_schema.clone();
            let arrow_schema_ref = arrow_schema_ref.clone();

            assert_eq!(df_schema_ref, arrow_schema.to_dfschema_ref().unwrap());
            assert_eq!(df_schema_ref, arrow_schema_ref.to_dfschema_ref().unwrap());
        }

        // Now, consume the refs
        assert_eq!(df_schema_ref, arrow_schema.to_dfschema_ref().unwrap());
        assert_eq!(df_schema_ref, arrow_schema_ref.to_dfschema_ref().unwrap());
    }

    fn test_schema_1() -> Schema {
        Schema::new(vec![
            Field::new("c0", DataType::Boolean, true),
            Field::new("c1", DataType::Boolean, true),
        ])
    }

    fn test_schema_2() -> Schema {
        Schema::new(vec![
            Field::new("c100", DataType::Boolean, true),
            Field::new("c101", DataType::Boolean, true),
        ])
    }
}
