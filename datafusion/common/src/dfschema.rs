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
use crate::{field_not_found, Column, TableReference};

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
                (Some(qq), None) => {
                    // the original field may now be aliased with a name that matches the
                    // original qualified name
                    let table_ref = TableReference::parse_str(field.name().as_str());
                    match table_ref {
                        TableReference::Partial { schema, table } => {
                            schema == qq && table == name
                        }
                        TableReference::Full { schema, table, .. } => {
                            schema == qq && table == name
                        }
                        _ => false,
                    }
                }
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

    /// Returns true if the two schemas have the same qualified named
    /// fields with the same data types. Returns false otherwise.
    ///
    /// This is a specialized version of Eq that ignores differences
    /// in nullability and metadata.
    pub fn equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }
        let self_fields = self.fields().iter();
        let other_fields = other.fields().iter();
        self_fields.zip(other_fields).all(|(f1, f2)| {
            f1.qualifier() == f2.qualifier()
                && f1.name() == f2.name()
                && Self::datatype_is_semantically_equal(f1.data_type(), f2.data_type())
        })
    }

    /// Returns true of two [`DataType`]s are semantically equal (same
    /// name and type), ignoring both metadata and nullability.
    ///
    /// request to upstream: <https://github.com/apache/arrow-rs/issues/3199>
    fn datatype_is_semantically_equal(dt1: &DataType, dt2: &DataType) -> bool {
        // check nested fields
        match (dt1, dt2) {
            (DataType::Dictionary(k1, v1), DataType::Dictionary(k2, v2)) => {
                Self::datatype_is_semantically_equal(k1.as_ref(), k2.as_ref())
                    && Self::datatype_is_semantically_equal(v1.as_ref(), v2.as_ref())
            }
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2))
            | (DataType::FixedSizeList(f1, _), DataType::FixedSizeList(f2, _))
            | (DataType::Map(f1, _), DataType::Map(f2, _)) => {
                Self::field_is_semantically_equal(f1, f2)
            }
            (DataType::Struct(fields1), DataType::Struct(fields2))
            | (DataType::Union(fields1, _, _), DataType::Union(fields2, _, _)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                        // all fields have to be the same
                    iter1
                    .zip(iter2)
                        .all(|(f1, f2)| Self::field_is_semantically_equal(f1, f2))
            }
            _ => dt1 == dt2,
        }
    }

    fn field_is_semantically_equal(f1: &Field, f2: &Field) -> bool {
        f1.name() == f2.name()
            && Self::datatype_is_semantically_equal(f1.data_type(), f2.data_type())
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
            df_schema.fields.into_iter().map(|f| f.field).collect(),
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
    fn qualifier_in_name() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        // lookup with unqualified name "t1.c0"
        let err = schema.index_of_column_by_name(None, "t1.c0").err().unwrap();
        assert_eq!(
            "Schema error: No field named 't1.c0'. Valid fields are 't1'.'c0', 't1'.'c1'.",
            &format!("{}", err)
        );
        Ok(())
    }

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
        let expected = "Field { name: \"c0\", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"c1\", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }";
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
        qualified field name \'t1\'.\'c0\'",
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
        field name \'t1\'.\'c0\' and unqualified field name \'c0\' which would be ambiguous",
            &format!("{}", join.err().unwrap())
        );
        Ok(())
    }

    #[allow(deprecated)]
    #[test]
    fn helpful_error_messages() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let expected_help = "Valid fields are \'t1\'.\'c0\', \'t1\'.\'c1\'.";
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
    fn equivalent_names_and_types() {
        let field1_i16_t = DFField::from(Field::new("f1", DataType::Int16, true));
        let field1_i16_t_meta = DFField::from(
            field1_i16_t
                .field()
                .clone()
                .with_metadata(test_metadata_n(2)),
        );
        let field1_i16_t_qualified =
            DFField::from_qualified("foo", field1_i16_t.field().clone());
        let field1_i16_f = DFField::from(Field::new("f1", DataType::Int16, false));
        let field1_i32_t = DFField::from(Field::new("f1", DataType::Int32, true));
        let field2_i16_t = DFField::from(Field::new("f2", DataType::Int16, true));
        let field3_i16_t = DFField::from(Field::new("f3", DataType::Int16, true));

        let dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let field_dict_t = DFField::from(Field::new("f_dict", dict.clone(), true));
        let field_dict_f = DFField::from(Field::new("f_dict", dict, false));

        let list_t = DFField::from(Field::new(
            "f_list",
            DataType::List(Box::new(field1_i16_t.field().clone())),
            true,
        ));
        let list_f = DFField::from(Field::new(
            "f_list",
            DataType::List(Box::new(field1_i16_f.field().clone())),
            false,
        ));

        let list_f_name = DFField::from(Field::new(
            "f_list",
            DataType::List(Box::new(field2_i16_t.field().clone())),
            false,
        ));

        let struct_t = DFField::from(Field::new(
            "f_struct",
            DataType::Struct(vec![field1_i16_t.field().clone()]),
            true,
        ));
        let struct_f = DFField::from(Field::new(
            "f_struct",
            DataType::Struct(vec![field1_i16_f.field().clone()]),
            false,
        ));

        let struct_f_meta = DFField::from(Field::new(
            "f_struct",
            DataType::Struct(vec![field1_i16_t_meta.field().clone()]),
            false,
        ));

        let struct_f_type = DFField::from(Field::new(
            "f_struct",
            DataType::Struct(vec![field1_i32_t.field().clone()]),
            false,
        ));

        // same
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i16_t],
            expected: true,
        }
        .run();

        // same but metadata is different, should still be true
        TestCase {
            fields1: vec![&field1_i16_t_meta],
            fields2: vec![&field1_i16_t],
            expected: true,
        }
        .run();

        // different name
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field2_i16_t],
            expected: false,
        }
        .run();

        // different type
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i32_t],
            expected: false,
        }
        .run();

        // different nullability
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i16_f],
            expected: true,
        }
        .run();

        // different qualifier
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i16_t_qualified],
            expected: false,
        }
        .run();

        // different name after first
        TestCase {
            fields1: vec![&field2_i16_t, &field1_i16_t],
            fields2: vec![&field2_i16_t, &field3_i16_t],
            expected: false,
        }
        .run();

        // different number
        TestCase {
            fields1: vec![&field1_i16_t, &field2_i16_t],
            fields2: vec![&field1_i16_t],
            expected: false,
        }
        .run();

        // dictionary
        TestCase {
            fields1: vec![&field_dict_t],
            fields2: vec![&field_dict_t],
            expected: true,
        }
        .run();

        // dictionary (different nullable)
        TestCase {
            fields1: vec![&field_dict_t],
            fields2: vec![&field_dict_f],
            expected: true,
        }
        .run();

        // dictionary (wrong type)
        TestCase {
            fields1: vec![&field_dict_t],
            fields2: vec![&field1_i16_t],
            expected: false,
        }
        .run();

        // list (different embedded nullability)
        TestCase {
            fields1: vec![&list_t],
            fields2: vec![&list_f],
            expected: true,
        }
        .run();

        // list (different sub field names)
        TestCase {
            fields1: vec![&list_t],
            fields2: vec![&list_f_name],
            expected: false,
        }
        .run();

        // struct
        TestCase {
            fields1: vec![&struct_t],
            fields2: vec![&struct_f],
            expected: true,
        }
        .run();

        // struct (different embedded meta)
        TestCase {
            fields1: vec![&struct_t],
            fields2: vec![&struct_f_meta],
            expected: true,
        }
        .run();

        // struct (different field type)
        TestCase {
            fields1: vec![&struct_t],
            fields2: vec![&struct_f_type],
            expected: false,
        }
        .run();

        #[derive(Debug)]
        struct TestCase<'a> {
            fields1: Vec<&'a DFField>,
            fields2: Vec<&'a DFField>,
            expected: bool,
        }

        impl<'a> TestCase<'a> {
            fn run(self) {
                println!("Running {:#?}", self);
                let schema1 = to_df_schema(self.fields1);
                let schema2 = to_df_schema(self.fields2);
                assert_eq!(
                    schema1.equivalent_names_and_types(&schema2),
                    self.expected,
                    "Comparison did not match expected: {}\n\n\
                     schema1:\n\n{:#?}\n\nschema2:\n\n{:#?}",
                    self.expected,
                    schema1,
                    schema2
                );
            }
        }

        fn to_df_schema(fields: Vec<&DFField>) -> DFSchema {
            let fields = fields.into_iter().cloned().collect();
            DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
        }
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
    #[test]
    fn test_dfschema_to_schema_convertion() {
        let mut a: DFField = DFField::new(Some("table1"), "a", DataType::Int64, false);
        let mut b: DFField = DFField::new(Some("table1"), "b", DataType::Int64, false);
        let mut a_metadata = HashMap::new();
        a_metadata.insert("key".to_string(), "value".to_string());
        a.field.set_metadata(a_metadata);
        let mut b_metadata = HashMap::new();
        b_metadata.insert("key".to_string(), "value".to_string());
        b.field.set_metadata(b_metadata);

        let df_schema = Arc::new(
            DFSchema::new_with_metadata([a, b].to_vec(), HashMap::new()).unwrap(),
        );
        let schema: Schema = df_schema.as_ref().clone().into();
        let a_df = df_schema.fields.get(0).unwrap().field();
        let a_arrow = schema.fields.get(0).unwrap();
        assert_eq!(a_df.metadata(), a_arrow.metadata())
    }

    fn test_schema_2() -> Schema {
        Schema::new(vec![
            Field::new("c100", DataType::Boolean, true),
            Field::new("c101", DataType::Boolean, true),
        ])
    }

    fn test_metadata() -> HashMap<String, String> {
        test_metadata_n(2)
    }

    fn test_metadata_n(n: usize) -> HashMap<String, String> {
        (0..n)
            .into_iter()
            .map(|i| (format!("k{}", i), format!("v{}", i)))
            .collect()
    }
}
