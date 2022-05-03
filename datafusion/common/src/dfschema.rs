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

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;

use crate::error::{DataFusionError, Result, SchemaError};
use crate::{field_not_found, Column};

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::fmt::{Display, Formatter};

/// A reference-counted reference to a `DFSchema`.
pub type DFSchemaRef = Arc<DFSchema>;

/// DFSchema wraps an Arrow schema and adds relation names
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFSchema {
    /// Fields
    fields: Vec<DFField>,
    /// Additional metadata in form of key value pairs
    metadata: HashMap<String, String>,
}

impl DFSchema {
    /// Creates an empty `DFSchema`
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: HashMap::new(),
        }
    }

    #[deprecated(since = "7.0.0", note = "please use `new_with_metadata` instead")]
    /// Create a new `DFSchema`
    pub fn new(fields: Vec<DFField>) -> Result<Self> {
        Self::new_with_metadata(fields, HashMap::new())
    }

    /// Create a new `DFSchema`
    pub fn new_with_metadata(
        fields: Vec<DFField>,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        let mut qualified_names = HashSet::new();
        let mut unqualified_names = HashSet::new();

        for field in &fields {
            if let Some(qualifier) = field.qualifier() {
                if !qualified_names.insert((qualifier, field.name())) {
                    return Err(DataFusionError::SchemaError(
                        SchemaError::DuplicateQualifiedField {
                            qualifier: qualifier.to_string(),
                            name: field.name().to_string(),
                        },
                    ));
                }
            } else if !unqualified_names.insert(field.name()) {
                return Err(DataFusionError::SchemaError(
                    SchemaError::DuplicateUnqualifiedField {
                        name: field.name().to_string(),
                    },
                ));
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
                return Err(DataFusionError::SchemaError(
                    SchemaError::AmbiguousReference {
                        qualifier: Some(qualifier.to_string()),
                        name: name.to_string(),
                    },
                ));
            }
        }
        Ok(Self { fields, metadata })
    }

    /// Create a `DFSchema` from an Arrow schema
    pub fn try_from_qualified_schema(qualifier: &str, schema: &Schema) -> Result<Self> {
        Self::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| DFField::from_qualified(qualifier, f.clone()))
                .collect(),
            schema.metadata().clone(),
        )
    }

    /// Create a new schema that contains the fields from this schema followed by the fields
    /// from the supplied schema. An error will be returned if there are duplicate field names.
    pub fn join(&self, schema: &DFSchema) -> Result<Self> {
        let mut fields = self.fields.clone();
        let mut metadata = self.metadata.clone();
        fields.extend_from_slice(schema.fields().as_slice());
        metadata.extend(schema.metadata.clone());
        Self::new_with_metadata(fields, metadata)
    }

    /// Modify this schema by appending the fields from the supplied schema, ignoring any
    /// duplicate fields.
    pub fn merge(&mut self, other_schema: &DFSchema) {
        if other_schema.fields.is_empty() {
            return;
        }
        for field in other_schema.fields() {
            // skip duplicate columns
            let duplicated_field = match field.qualifier() {
                Some(q) => self.field_with_name(Some(q.as_str()), field.name()).is_ok(),
                // for unqualified columns, check as unqualified name
                None => self.field_with_unqualified_name(field.name()).is_ok(),
            };
            if !duplicated_field {
                self.fields.push(field.clone());
            }
        }
        self.metadata.extend(other_schema.metadata.clone())
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

    #[deprecated(since = "8.0.0", note = "please use `index_of_column_by_name` instead")]
    /// Find the index of the column with the given unqualified name
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            } else {
                // Now that `index_of` is deprecated an error is thrown if
                // a fully qualified field name is provided.
                match &self.fields[i].qualifier {
                    Some(qualifier) => {
                        if (qualifier.to_owned() + "." + self.fields[i].name()) == name {
                            return Err(DataFusionError::Plan(format!(
                                "Fully qualified field name '{}' was supplied to `index_of` \
                                which is deprecated. Please use `index_of_column_by_name` instead",
                                name
                            )));
                        }
                    }
                    None => (),
                }
            }
        }

        Err(field_not_found(None, name, self))
    }

    pub fn index_of_column_by_name(
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
            None => Err(field_not_found(
                qualifier.map(|s| s.to_string()),
                name,
                self,
            )),
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

    /// Find all fields having the given qualifier
    pub fn fields_with_qualified(&self, qualifier: &str) -> Vec<&DFField> {
        self.fields
            .iter()
            .filter(|field| field.qualifier().map(|q| q.eq(qualifier)).unwrap_or(false))
            .collect()
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
            0 => Err(field_not_found(None, name, self)),
            1 => Ok(matches[0]),
            _ => Err(DataFusionError::SchemaError(
                SchemaError::AmbiguousReference {
                    qualifier: None,
                    name: name.to_string(),
                },
            )),
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

    /// Check to see if fields in 2 Arrow schemas are compatible
    pub fn check_arrow_schema_type_compatible(
        &self,
        arrow_schema: &Schema,
    ) -> Result<()> {
        let self_arrow_schema: Schema = self.into();
        self_arrow_schema
            .fields()
            .iter()
            .zip(arrow_schema.fields().iter())
            .try_for_each(|(l_field, r_field)| {
                if !can_cast_types(r_field.data_type(), l_field.data_type()) {
                    Err(DataFusionError::Plan(
                        format!("Column {} (type: {}) is not compatible with column {} (type: {})",
                            r_field.name(),
                            r_field.data_type(),
                            l_field.name(),
                            l_field.data_type())))
                } else {
                    Ok(())
                }
            })
    }

    /// Strip all field qualifier in schema
    pub fn strip_qualifiers(self) -> Self {
        DFSchema {
            fields: self
                .fields
                .into_iter()
                .map(|f| f.strip_qualifier())
                .collect(),
            ..self
        }
    }

    /// Replace all field qualifier with new value in schema
    pub fn replace_qualifier(self, qualifier: &str) -> Self {
        DFSchema {
            fields: self
                .fields
                .into_iter()
                .map(|f| DFField::from_qualified(qualifier, f.field))
                .collect(),
            ..self
        }
    }

    /// Get list of fully-qualified field names in this schema
    pub fn field_names(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|f| f.qualified_name())
            .collect::<Vec<_>>()
    }

    /// Get metadata of this schema
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
}

impl From<DFSchema> for Schema {
    /// Convert DFSchema into a Schema
    fn from(df_schema: DFSchema) -> Self {
        Schema::new_with_metadata(
            df_schema
                .fields
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
            df_schema.metadata,
        )
    }
}

impl From<&DFSchema> for Schema {
    /// Convert DFSchema reference into a Schema
    fn from(df_schema: &DFSchema) -> Self {
        Schema::new_with_metadata(
            df_schema.fields.iter().map(|f| f.field.clone()).collect(),
            df_schema.metadata.clone(),
        )
    }
}

/// Create a `DFSchema` from an Arrow schema
impl TryFrom<Schema> for DFSchema {
    type Error = DataFusionError;
    fn try_from(schema: Schema) -> std::result::Result<Self, Self::Error> {
        Self::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| DFField::from(f.clone()))
                .collect(),
            schema.metadata().clone(),
        )
    }
}

impl From<DFSchema> for SchemaRef {
    fn from(df_schema: DFSchema) -> Self {
        SchemaRef::new(df_schema.into())
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
        DFSchema::new_with_metadata(self, HashMap::new())
    }
}

impl Display for DFSchema {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "fields:[{}], metadata:{:?}",
            self.fields
                .iter()
                .map(|field| field.qualified_name())
                .collect::<Vec<String>>()
                .join(", "),
            self.metadata
        )
    }
}

/// Provides schema information needed by certain methods of `Expr`
/// (defined in the datafusion-common crate).
///
/// Note that this trait is implemented for &[DFSchema] which is
/// widely used in the DataFusion codebase.
pub trait ExprSchema {
    /// Is this column reference nullable?
    fn nullable(&self, col: &Column) -> Result<bool>;

    /// What is the datatype of this column?
    fn data_type(&self, col: &Column) -> Result<&DataType>;
}

// Implement `ExprSchema` for `Arc<DFSchema>`
impl<P: AsRef<DFSchema>> ExprSchema for P {
    fn nullable(&self, col: &Column) -> Result<bool> {
        self.as_ref().nullable(col)
    }

    fn data_type(&self, col: &Column) -> Result<&DataType> {
        self.as_ref().data_type(col)
    }
}

impl ExprSchema for DFSchema {
    fn nullable(&self, col: &Column) -> Result<bool> {
        Ok(self.field_from_column(col)?.is_nullable())
    }

    fn data_type(&self, col: &Column) -> Result<&DataType> {
        Ok(self.field_from_column(col)?.data_type())
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
        assert_eq!("fields:[c0, c1], metadata:{}", schema.to_string());
        Ok(())
    }

    #[test]
    fn from_qualified_schema() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        assert_eq!("fields:[t1.c0, t1.c1], metadata:{}", schema.to_string());
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
        assert_eq!(
            "fields:[t1.c0, t1.c1, t2.c0, t2.c1], metadata:{}",
            join.to_string()
        );
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
            "Schema error: Schema contains duplicate \
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
            "Schema error: Schema contains duplicate \
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
        assert_eq!(
            "fields:[t1.c0, t1.c1, c100, c101], metadata:{}",
            join.to_string()
        );
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
            "Schema error: Schema contains qualified \
        field name \'t1.c0\' and unqualified field name \'c0\' which would be ambiguous",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[allow(deprecated)]
    #[test]
    fn helpful_error_messages() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let expected_help = "Valid fields are \'t1.c0\', \'t1.c1\'.";
        // Pertinent message parts
        let expected_err_msg = "Fully qualified field name \'t1.c0\'";
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
        assert!(schema
            .index_of("t1.c0")
            .unwrap_err()
            .to_string()
            .contains(expected_err_msg));
        Ok(())
    }

    #[test]
    fn into() {
        // Demonstrate how to convert back and forth between Schema, SchemaRef, DFSchema, and DFSchemaRef
        let metadata = test_metadata();
        let arrow_schema = Schema::new_with_metadata(
            vec![Field::new("c0", DataType::Int64, true)],
            metadata.clone(),
        );
        let arrow_schema_ref = Arc::new(arrow_schema.clone());

        let df_schema = DFSchema::new_with_metadata(
            vec![DFField::new(None, "c0", DataType::Int64, true)],
            metadata,
        )
        .unwrap();
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

    fn test_metadata() -> HashMap<String, String> {
        vec![("k1", "v1"), ("k2", "v2")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>()
    }
}
