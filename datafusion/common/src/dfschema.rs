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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use crate::error::{DataFusionError, Result, _plan_err};
use crate::{
    field_not_found, functional_dependencies, unqualified_field_not_found, Column,
    FunctionalDependencies, OwnedTableReference, TableReference,
};

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow_schema::SchemaBuilder;

/// A reference-counted reference to a [DFSchema].
pub type DFSchemaRef = Arc<DFSchema>;

/// DFSchema wraps an Arrow schema and adds relation names.
///
/// The schema may hold the fields across multiple tables. Some fields may be
/// qualified and some unqualified. A qualified field is a field that has a
/// relation name associated with it.
///
/// Unqualified fields must be unique not only amongst themselves, but also must
/// have a distinct name from any qualified field names. This allows finding a
/// qualified field by name to be possible, so long as there aren't multiple
/// qualified fields with the same name.
///
/// There is an alias to `Arc<DFSchema>` named [DFSchemaRef].
///
/// # Creating qualified schemas
///
/// Use [DFSchema::try_from_qualified_schema] to create a qualified schema from
/// an Arrow schema.
///
/// ```rust
/// use datafusion_common::{DFSchema, Column};
/// use arrow_schema::{DataType, Field, Schema};
///
/// let arrow_schema = Schema::new(vec![
///    Field::new("c1", DataType::Int32, false),
/// ]);
///
/// let df_schema = DFSchema::try_from_qualified_schema("t1", &arrow_schema).unwrap();
/// let column = Column::from_qualified_name("t1.c1");
/// assert!(df_schema.has_column(&column));
///
/// // Can also access qualified fields with unqualified name, if it's unambiguous
/// let column = Column::from_qualified_name("c1");
/// assert!(df_schema.has_column(&column));
/// ```
///
/// # Creating unqualified schemas
///
/// Create an unqualified schema using TryFrom:
///
/// ```rust
/// use datafusion_common::{DFSchema, Column};
/// use arrow_schema::{DataType, Field, Schema};
///
/// let arrow_schema = Schema::new(vec![
///    Field::new("c1", DataType::Int32, false),
/// ]);
///
/// let df_schema = DFSchema::try_from(arrow_schema).unwrap();
/// let column = Column::new_unqualified("c1");
/// assert!(df_schema.has_column(&column));
/// ```
///
/// # Converting back to Arrow schema
///
/// Use the `Into` trait to convert `DFSchema` into an Arrow schema:
///
/// ```rust
/// use datafusion_common::{DFSchema, DFField};
/// use arrow_schema::Schema;
///
/// let df_schema = DFSchema::new(vec![
///    DFField::new_unqualified("c1", arrow::datatypes::DataType::Int32, false),
/// ]).unwrap();
/// let schema = Schema::from(df_schema);
/// assert_eq!(schema.fields().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFSchema {
    /// Inner Arrow schema reference.
    inner: SchemaRef,
    /// Optional qualifiers for each column in this schema. In the same order as
    /// the `self.inner.fields()`
    field_qualifiers: Vec<Option<OwnedTableReference>>,
    /// Stores functional dependencies in the schema.
    functional_dependencies: FunctionalDependencies,
}

impl DFSchema {
    /// Creates an empty `DFSchema`
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(Schema::new([])),
            field_qualifiers: vec![],
            functional_dependencies: FunctionalDependencies::empty(),
        }
    }

    pub fn new_with_metadata(
        fields: Vec<Field>,
        metadata: HashMap<String, String>,
    ) -> Self {
        let field_count = fields.len();
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        Self {
            inner: schema,
            field_qualifiers: vec![None; field_count],
            functional_dependencies: FunctionalDependencies::empty(),
        }
    }

    // TODO Check this vs `try_from_qualified_schema`
    /// Create a `DFSchema` from an Arrow schema where all the fields have a given qualifier
    pub fn from_qualified_schema<'a>(
        qualifier: impl Into<TableReference<'a>>,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let qualifier = qualifier.into();
        let owned_qualifier = qualifier.to_owned_reference();
        let new_self = Self {
            inner: schema.clone(),
            field_qualifiers: vec![Some(owned_qualifier); schema.fields().len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        // new_self.check_names()?;
        Ok(new_self)
    }

    // TODO ADD TESTS FOR THIS NEW FUNCTION
    /// Create a `DFSchema` from an Arrow schema where all the fields have a given qualifier
    pub fn from_field_specific_qualified_schema<'a>(
        qualifiers: Vec<Option<impl Into<TableReference<'a>>>>,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let owned_qualifiers = qualifiers
            .into_iter()
            .map(|maybe_q| {
                maybe_q.map(|q| {
                    let qualifier = q.into();
                    let owned_qualifier = qualifier.to_owned_reference();
                    owned_qualifier
                })
            })
            .collect();
        let new_self = Self {
            inner: schema.clone(),
            field_qualifiers: owned_qualifiers,
            functional_dependencies: FunctionalDependencies::empty(),
        };
        // new_self.check_names()?;
        Ok(new_self)
    }

    /// Create a `DFSchema` from an Arrow where all fields have no qualifier.
    pub fn from_unqualified_schema(schema: &SchemaRef) -> Result<Self> {
        let new_self = Self {
            inner: schema.clone(),
            field_qualifiers: vec![None; schema.fields.len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        // new_self.check_names()?;
        Ok(new_self)
    }

    // TODO Add tests
    pub fn from_qualified_fields(
        qualified_fields: Vec<(Option<OwnedTableReference>, Arc<Field>)>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let (qualifiers, fields): (Vec<Option<OwnedTableReference>>, Vec<Arc<Field>>) =
            qualified_fields.into_iter().unzip();

        let schema = if let Some(metadata) = metadata {
            Arc::new(Schema::new_with_metadata(fields, metadata))
        } else {
            Arc::new(Schema::new(fields))
        };

        let dfschema = Self {
            inner: schema,
            field_qualifiers: qualifiers,
            functional_dependencies: FunctionalDependencies::empty(),
        };
        Ok(dfschema)
    }

    // fn check_names(&self) -> Result<()> {
    //     let mut qualified_names = HashSet::new();
    //     let mut unqualified_names = HashSet::new();
    //
    //     for (field, qualifier) in self.inner.fields().iter().zip(&self.field_qualifiers) {
    //
    //     }
    // }

    // #[deprecated(since = "7.0.0", note = "please use `new_with_metadata` instead")]
    /// Create a new `DFSchema`
    // pub fn new(fields: Vec<DFField>) -> Result<Self> {
    //     Self::new_with_metadata(fields, HashMap::new())
    // }

    /// Create a new `DFSchema`
    // pub fn new_with_metadata(
    //     fields: Vec<DFField>,
    //     metadata: HashMap<String, String>,
    // ) -> Result<Self> {
    //     let mut qualified_names = HashSet::new();
    //     let mut unqualified_names = HashSet::new();
    //
    //     for field in &fields {
    //         if let Some(qualifier) = field.qualifier() {
    //             qualified_names.insert((qualifier, field.name()));
    //         } else if !unqualified_names.insert(field.name()) {
    //             return _schema_err!(SchemaError::DuplicateUnqualifiedField {
    //                 name: field.name().to_string(),
    //             });
    //         }
    //     }
    //
    //     // check for mix of qualified and unqualified field with same unqualified name
    //     // note that we need to sort the contents of the HashSet first so that errors are
    //     // deterministic
    //     let mut qualified_names = qualified_names
    //         .iter()
    //         .map(|(l, r)| (l.to_owned(), r.to_owned()))
    //         .collect::<Vec<(&OwnedTableReference, &String)>>();
    //     qualified_names.sort();
    //     for (qualifier, name) in &qualified_names {
    //         if unqualified_names.contains(name) {
    //             return _schema_err!(SchemaError::AmbiguousReference {
    //                 field: Column {
    //                     relation: Some((*qualifier).clone()),
    //                     name: name.to_string(),
    //                 }
    //             });
    //         }
    //     }
    //     Ok(Self {
    //         fields,
    //         metadata,
    //         functional_dependencies: FunctionalDependencies::empty(),
    //     })
    // }

    /// Create a `DFSchema` from an Arrow schema and a given qualifier
    ///
    /// To create a schema from an Arrow schema without a qualifier, use
    /// `DFSchema::try_from`.
    pub fn try_from_qualified_schema<'a>(
        qualifier: impl Into<TableReference<'a>>,
        schema: &Schema,
    ) -> Result<Self> {
        let qualifier = qualifier.into();
        let owned_qualifier = qualifier.to_owned_reference();
        let schema = DFSchema {
            inner: schema.clone().into(),
            field_qualifiers: vec![Some(owned_qualifier); schema.fields.len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        Ok(schema)
    }

    /// Assigns functional dependencies.
    pub fn with_functional_dependencies(
        mut self,
        functional_dependencies: FunctionalDependencies,
    ) -> Result<Self> {
        if functional_dependencies.is_valid(self.inner.fields.len()) {
            self.functional_dependencies = functional_dependencies;
            Ok(self)
        } else {
            _plan_err!(
                "Invalid functional dependency: {:?}",
                functional_dependencies
            )
        }
    }

    /// Create a new schema that contains the fields from this schema followed by the fields
    /// from the supplied schema. An error will be returned if there are duplicate field names.
    pub fn join(&self, schema: &DFSchema) -> Result<Self> {
        let fields = self.inner.fields().clone();
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.extend(fields.iter().map(|f| f.clone()));

        let other_fields = schema.inner.fields.clone();
        schema_builder.extend(other_fields.iter().map(|f| f.clone()));
        let new_schema = schema_builder.finish();

        let mut new_metadata = self.inner.metadata.clone();
        new_metadata.extend(schema.inner.metadata.clone());

        let new_schema_with_metadata = new_schema.with_metadata(new_metadata);

        let mut new_qualifiers = self.field_qualifiers.clone();
        new_qualifiers.extend_from_slice(schema.field_qualifiers.as_slice());

        let mut functional_dependencies = self.functional_dependencies.clone();
        functional_dependencies.extend(schema.functional_dependencies.clone());

        let new_self = Self {
            inner: Arc::new(new_schema_with_metadata),
            field_qualifiers: new_qualifiers,
            functional_dependencies,
        };
        Ok(new_self)
    }

    /// Modify this schema by appending the fields from the supplied schema, ignoring any
    /// duplicate fields.
    pub fn merge(&mut self, other_schema: &DFSchema) {
        if other_schema.inner.fields.is_empty() {
            return;
        }
        let mut schema_builder = SchemaBuilder::from(self.inner.fields.clone());
        for (qualifier, field) in other_schema.iter() {
            // skip duplicate columns
            let duplicated_field = match qualifier {
                Some(q) => self.has_column_with_qualified_name(q, field.name()),
                // for unqualified columns, check as unqualified name
                None => self.has_column_with_unqualified_name(field.name()),
            };
            if !duplicated_field {
                // self.inner.fields.push(field.clone());
                schema_builder.push(field.clone())
            }
        }
        let mut metadata = self.inner.metadata.clone();
        metadata.extend(other_schema.inner.metadata.clone());

        let finished = schema_builder.finish();
        let finished_with_metadata = finished.with_metadata(metadata);
        self.inner = finished_with_metadata.into();
    }

    /// Get a list of fields
    pub fn fields(&self) -> &Fields {
        &self.inner.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector
    pub fn field(&self, i: usize) -> &Field {
        &self.inner.fields[i]
    }

    // #[deprecated(since = "8.0.0", note = "please use `index_of_column_by_name` instead")]
    /// Find the index of the column with the given unqualified name
    // pub fn index_of(&self, name: &str) -> Result<usize> {
    //     for i in 0..self.fields.len() {
    //         if self.fields[i].name() == name {
    //             return Ok(i);
    //         } else {
    //             // Now that `index_of` is deprecated an error is thrown if
    //             // a fully qualified field name is provided.
    //             match &self.fields[i].qualifier {
    //                 Some(qualifier) => {
    //                     if (qualifier.to_string() + "." + self.fields[i].name()) == name {
    //                         return _plan_err!(
    //                             "Fully qualified field name '{name}' was supplied to `index_of` \
    //                             which is deprecated. Please use `index_of_column_by_name` instead"
    //                         );
    //                     }
    //                 }
    //                 None => (),
    //             }
    //         }
    //     }
    //
    //     Err(unqualified_field_not_found(name, self))
    // }

    pub fn index_of_column_by_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Result<Option<usize>> {
        let mut matches = self
            .iter()
            .enumerate()
            .filter(|(_, (q, f))| match (qualifier, q) {
                // field to lookup is qualified.
                // current field is qualified and not shared between relations, compare both
                // qualifier and name.
                (Some(q), Some(field_q)) => q.resolved_eq(field_q) && f.name() == name,
                // field to lookup is qualified but current field is unqualified.
                (Some(qq), None) => {
                    // the original field may now be aliased with a name that matches the
                    // original qualified name
                    let column = Column::from_qualified_name(f.name());
                    match column {
                        Column {
                            relation: Some(r),
                            name: column_name,
                        } => &r == qq && column_name == name,
                        _ => false,
                    }
                }
                // field to lookup is unqualified, no need to compare qualifier
                (None, Some(_)) | (None, None) => f.name() == name,
            })
            .map(|(idx, _)| idx);
        Ok(matches.next())
    }

    /// Find the index of the column with the given qualifier and name
    pub fn index_of_column(&self, col: &Column) -> Result<usize> {
        self.index_of_column_by_name(col.relation.as_ref(), &col.name)?
            .ok_or_else(|| field_not_found(col.relation.clone(), &col.name, self))
    }

    /// Check if the column is in the current schema
    pub fn is_column_from_schema(&self, col: &Column) -> Result<bool> {
        self.index_of_column_by_name(col.relation.as_ref(), &col.name)
            .map(|idx| idx.is_some())
    }

    /// Find the field with the given name
    pub fn field_with_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Result<&Field> {
        if let Some(qualifier) = qualifier {
            self.field_with_qualified_name(qualifier, name)
        } else {
            self.field_with_unqualified_name(name)
        }
    }

    /// Find all fields having the given qualifier
    pub fn fields_with_qualified(&self, qualifier: &TableReference) -> Vec<&Field> {
        let fields: Vec<&Field> = self
            .iter()
            .filter(|(q, _)| q.map(|q| q == qualifier).unwrap_or(false))
            .map(|(_, f)| f.as_ref())
            .collect();
        fields
    }

    /// Find all fields indices having the given qualifier
    pub fn fields_indices_with_qualified(
        &self,
        qualifier: &TableReference,
    ) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter_map(|(idx, (q, _))| {
                let qualifier_matches = match q {
                    Some(q) => *q == *qualifier,
                    None => false,
                };
                match qualifier_matches {
                    true => Some(idx),
                    false => None,
                }
            })
            .collect()
    }

    /// Find all fields that match the given name
    pub fn fields_with_unqualified_name(&self, name: &str) -> Vec<&Field> {
        self.iter()
            .filter(|(_, field)| field.name() == name)
            .map(|(_, f)| f.as_ref())
            .collect()
    }

    /// Find all fields that match the given name and return them with their qualifier
    pub fn fields_and_qualifiers_with_unqualified_name(
        &self,
        name: &str,
    ) -> Vec<(Option<&TableReference>, &Field)> {
        self.iter()
            .filter(|(_, field)| field.name() == name)
            .map(|(q, f)| (q, f.as_ref()))
            .collect()
    }

    /// Find all fields that match the given name and convert to column
    pub fn columns_with_unqualified_name(&self, name: &str) -> Vec<Column> {
        self.iter()
            .filter(|(_, field)| field.name() == name)
            .map(|(_, f)| Column::from_name(f.name()))
            .collect()
    }

    /// Return all `Column`s for the schema
    pub fn columns(&self) -> Vec<Column> {
        self.iter()
            .map(|(q, f)| Column::new(q.map(|q| q.clone()), f.name().clone()))
            .collect()
    }

    /// Find the field with the given name
    pub fn field_with_unqualified_name(&self, name: &str) -> Result<&Field> {
        let matches = self.fields_and_qualifiers_with_unqualified_name(name);
        match matches.len() {
            0 => Err(unqualified_field_not_found(name, self)),
            1 => Ok(matches[0].1),
            _ => {
                let fields_without_qualifier = matches
                    .iter()
                    .filter(|(q, _)| q.is_none())
                    .collect::<Vec<_>>();
                if fields_without_qualifier.len() == 1 {
                    Ok(fields_without_qualifier[0].1)
                } else {
                    Err(DataFusionError::Internal("Field not found".to_string()))
                    // _schema_err!(SchemaError::AmbiguousReference {
                    //     field: Column {
                    //         relation: None,
                    //         name: name.to_string(),
                    //     },
                    // })
                }
            }
        }
        // let field = self.iter().find(|(_, f)| name == f.name());
        // let field = self.iter().find(|(q, f)| match q {
        //     Some(_) => false,
        //     None => name == f.name(),
        // });
        // match field {
        //     Some((_, f)) => Ok(f),
        //     None => Err(DataFusionError::Internal("Field not found".to_string())),
        // }
    }

    /// Find the field with the given qualified name
    pub fn field_with_qualified_name(
        &self,
        qualifier: &TableReference,
        name: &str,
    ) -> Result<&Field> {
        let qualifier_and_field = self.iter().find(|(q, f)| match q {
            Some(q) => *q == qualifier && name == f.name(),
            None => false,
        });
        match qualifier_and_field {
            Some((_, f)) => Ok(f),
            None => Err(DataFusionError::Internal("Field not found".to_string())),
        }
    }

    /// Find the field with the given qualified column
    pub fn field_from_column(&self, column: &Column) -> Result<&Field> {
        match &column.relation {
            Some(r) => self.field_with_qualified_name(r, &column.name),
            None => self.field_with_unqualified_name(&column.name),
        }
    }

    /// Find if the field exists with the given name
    pub fn has_column_with_unqualified_name(&self, name: &str) -> bool {
        self.fields().iter().any(|field| field.name() == name)
    }

    /// Find if the field exists with the given qualified name
    pub fn has_column_with_qualified_name(
        &self,
        qualifier: &TableReference,
        name: &str,
    ) -> bool {
        self.iter()
            .any(|(q, f)| q.map(|q| q == qualifier).unwrap_or(false) && f.name() == name)
    }

    /// Find if the field exists with the given qualified column
    pub fn has_column(&self, column: &Column) -> bool {
        match &column.relation {
            Some(r) => self.has_column_with_qualified_name(r, &column.name),
            None => self.has_column_with_unqualified_name(&column.name),
        }
    }

    /// Check to see if unqualified field names matches field names in Arrow schema
    pub fn matches_arrow_schema(&self, arrow_schema: &Schema) -> bool {
        self.inner
            .fields
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
                    _plan_err!("Column {} (type: {}) is not compatible with column {} (type: {})",
                                r_field.name(),
                                r_field.data_type(),
                                l_field.name(),
                                l_field.data_type())
                } else {
                    Ok(())
                }
            })
    }

    /// Returns true if the two schemas have the same qualified named
    /// fields with logically equivalent data types. Returns false otherwise.
    ///
    /// Use [DFSchema]::equivalent_names_and_types for stricter semantic type
    /// equivalence checking.
    pub fn logically_equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }
        let self_fields = self.iter();
        let other_fields = other.iter();
        self_fields.zip(other_fields).all(|((q1, f1), (q2, f2))| {
            q1 == q2
                && f1.name() == f2.name()
                && Self::datatype_is_logically_equal(f1.data_type(), f2.data_type())
        })
    }

    /// Returns true if the two schemas have the same qualified named
    /// fields with the same data types. Returns false otherwise.
    ///
    /// This is a specialized version of Eq that ignores differences
    /// in nullability and metadata.
    ///
    /// Use [DFSchema]::logically_equivalent_names_and_types for a weaker
    /// logical type checking, which for example would consider a dictionary
    /// encoded UTF8 array to be equivalent to a plain UTF8 array.
    pub fn equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }
        let self_fields = self.iter();
        let other_fields = other.iter();
        self_fields.zip(other_fields).all(|((q1, f1), (q2, f2))| {
            q1 == q2
                && f1.name() == f2.name()
                && Self::datatype_is_semantically_equal(f1.data_type(), f2.data_type())
        })
    }

    /// Checks if two [`DataType`]s are logically equal. This is a notably weaker constraint
    /// than datatype_is_semantically_equal in that a Dictionary<K,V> type is logically
    /// equal to a plain V type, but not semantically equal. Dictionary<K1, V1> is also
    /// logically equal to Dictionary<K2, V1>.
    fn datatype_is_logically_equal(dt1: &DataType, dt2: &DataType) -> bool {
        // check nested fields
        match (dt1, dt2) {
            (DataType::Dictionary(_, v1), DataType::Dictionary(_, v2)) => {
                v1.as_ref() == v2.as_ref()
            }
            (DataType::Dictionary(_, v1), othertype) => v1.as_ref() == othertype,
            (othertype, DataType::Dictionary(_, v1)) => v1.as_ref() == othertype,
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2))
            | (DataType::FixedSizeList(f1, _), DataType::FixedSizeList(f2, _))
            | (DataType::Map(f1, _), DataType::Map(f2, _)) => {
                Self::field_is_logically_equal(f1, f2)
            }
            (DataType::Struct(fields1), DataType::Struct(fields2)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                        // all fields have to be the same
                    iter1
                    .zip(iter2)
                        .all(|(f1, f2)| Self::field_is_logically_equal(f1, f2))
            }
            (DataType::Union(fields1, _), DataType::Union(fields2, _)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                    // all fields have to be the same
                    iter1
                        .zip(iter2)
                        .all(|((t1, f1), (t2, f2))| t1 == t2 && Self::field_is_logically_equal(f1, f2))
            }
            _ => dt1 == dt2,
        }
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
            (DataType::Struct(fields1), DataType::Struct(fields2)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                        // all fields have to be the same
                    iter1
                    .zip(iter2)
                        .all(|(f1, f2)| Self::field_is_semantically_equal(f1, f2))
            }
            (DataType::Union(fields1, _), DataType::Union(fields2, _)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                    // all fields have to be the same
                    iter1
                        .zip(iter2)
                        .all(|((t1, f1), (t2, f2))| t1 == t2 && Self::field_is_semantically_equal(f1, f2))
            }
            (
                DataType::Decimal128(_l_precision, _l_scale),
                DataType::Decimal128(_r_precision, _r_scale),
            ) => true,
            (
                DataType::Decimal256(_l_precision, _l_scale),
                DataType::Decimal256(_r_precision, _r_scale),
            ) => true,
            _ => dt1 == dt2,
        }
    }

    fn field_is_logically_equal(f1: &Field, f2: &Field) -> bool {
        f1.name() == f2.name()
            && Self::datatype_is_logically_equal(f1.data_type(), f2.data_type())
    }

    fn field_is_semantically_equal(f1: &Field, f2: &Field) -> bool {
        f1.name() == f2.name()
            && Self::datatype_is_semantically_equal(f1.data_type(), f2.data_type())
    }

    /// Strip all field qualifier in schema
    pub fn strip_qualifiers(self) -> Self {
        let stripped_schema = DFSchema {
            inner: self.inner.clone(),
            field_qualifiers: vec![None; self.inner.fields.len()],
            functional_dependencies: self.functional_dependencies.clone(),
        };
        stripped_schema
    }

    /// Replace all field qualifier with new value in schema
    pub fn replace_qualifier(self, qualifier: impl Into<OwnedTableReference>) -> Self {
        let qualifier = qualifier.into();
        let replaced_schema = DFSchema {
            inner: self.inner.clone(),
            field_qualifiers: vec![Some(qualifier); self.inner.fields.len()],
            functional_dependencies: self.functional_dependencies.clone(),
        };
        replaced_schema
    }

    /// Get list of fully-qualified field names in this schema
    pub fn field_names(&self) -> Vec<String> {
        self.iter()
            .map(|(qualifier, field)| qualified_name(qualifier, field.name()))
            .collect::<Vec<_>>()
    }

    /// Get metadata of this schema
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.inner.metadata
    }

    /// Get functional dependencies
    pub fn functional_dependencies(&self) -> &FunctionalDependencies {
        &self.functional_dependencies
    }

    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (Option<&OwnedTableReference>, &'a FieldRef)> {
        self.field_qualifiers
            .iter()
            .zip(self.inner.fields().iter())
            .map(|(qualifier, field)| (qualifier.as_ref(), field))
    }
}

impl From<DFSchema> for Schema {
    /// Convert DFSchema into a Schema
    fn from(df_schema: DFSchema) -> Self {
        let fields: Fields = df_schema.inner.fields.clone();
        Schema::new_with_metadata(fields, df_schema.inner.metadata.clone())
    }
}

impl From<&DFSchema> for Schema {
    /// Convert DFSchema reference into a Schema
    fn from(df_schema: &DFSchema) -> Self {
        let fields: Fields = df_schema.inner.fields.clone();
        Schema::new_with_metadata(fields, df_schema.inner.metadata.clone())
    }
}

/// Create a `DFSchema` from an Arrow schema
impl TryFrom<Schema> for DFSchema {
    type Error = DataFusionError;
    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        let field_count = schema.fields.len();
        let dfschema = Self {
            inner: schema.into(),
            field_qualifiers: vec![None; field_count],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        Ok(dfschema)
    }
}

impl From<DFSchema> for SchemaRef {
    fn from(df_schema: DFSchema) -> Self {
        SchemaRef::new(df_schema.into())
    }
}

// Hashing refers to a subset of fields considered in PartialEq.
impl Hash for DFSchema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.fields.hash(state);
        self.inner.metadata.len().hash(state); // HashMap is not hashable
    }
}

/// Convenience trait to convert Schema like things to DFSchema and DFSchemaRef with fewer keystrokes
pub trait ToDFSchema
where
    Self: Sized,
{
    /// Attempt to create a DSSchema
    fn to_dfschema(self) -> Result<DFSchema>;

    /// Attempt to create a DSSchemaRef
    fn to_dfschema_ref(self) -> Result<DFSchemaRef> {
        Ok(Arc::new(self.to_dfschema()?))
    }
}

impl ToDFSchema for Schema {
    fn to_dfschema(self) -> Result<DFSchema> {
        DFSchema::try_from(self)
    }
}

impl ToDFSchema for SchemaRef {
    fn to_dfschema(self) -> Result<DFSchema> {
        // Attempt to use the Schema directly if there are no other
        // references, otherwise clone
        match Self::try_unwrap(self) {
            Ok(schema) => DFSchema::try_from(schema),
            Err(schemaref) => DFSchema::try_from(schemaref.as_ref().clone()),
        }
    }
}

impl ToDFSchema for Vec<Field> {
    fn to_dfschema(self) -> Result<DFSchema> {
        let field_count = self.len();
        let schema = Schema {
            fields: self.into(),
            metadata: HashMap::new(),
        };
        let dfschema = DFSchema {
            inner: schema.into(),
            field_qualifiers: vec![None; field_count],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        Ok(dfschema)
    }
}

impl Display for DFSchema {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "fields:[{}], metadata:{:?}",
            self.iter()
                .map(|(q, f)| qualified_name(q, f.name()))
                .collect::<Vec<String>>()
                .join(", "),
            self.inner.metadata
        )
    }
}

/// Provides schema information needed by certain methods of `Expr`
/// (defined in the datafusion-common crate).
///
/// Note that this trait is implemented for &[DFSchema] which is
/// widely used in the DataFusion codebase.
pub trait ExprSchema: std::fmt::Debug {
    /// Is this column reference nullable?
    fn nullable(&self, col: &Column) -> Result<bool>;

    /// What is the datatype of this column?
    fn data_type(&self, col: &Column) -> Result<&DataType>;

    /// Returns the column's optional metadata.
    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>>;
}

// Implement `ExprSchema` for `Arc<DFSchema>`
impl<P: AsRef<DFSchema> + std::fmt::Debug> ExprSchema for P {
    fn nullable(&self, col: &Column) -> Result<bool> {
        self.as_ref().nullable(col)
    }

    fn data_type(&self, col: &Column) -> Result<&DataType> {
        self.as_ref().data_type(col)
    }

    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>> {
        ExprSchema::metadata(self.as_ref(), col)
    }
}

impl ExprSchema for DFSchema {
    fn nullable(&self, col: &Column) -> Result<bool> {
        Ok(self.field_from_column(col)?.is_nullable())
    }

    fn data_type(&self, col: &Column) -> Result<&DataType> {
        Ok(self.field_from_column(col)?.data_type())
    }

    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>> {
        Ok(self.field_from_column(col)?.metadata())
    }
}

/// DataFusion-specific extensions to [`Schema`].
pub trait SchemaExt {
    /// This is a specialized version of Eq that ignores differences
    /// in nullability and metadata.
    ///
    /// It works the same as [`DFSchema::equivalent_names_and_types`].
    fn equivalent_names_and_types(&self, other: &Self) -> bool;

    /// Returns true if the two schemas have the same qualified named
    /// fields with logically equivalent data types. Returns false otherwise.
    ///
    /// Use [DFSchema]::equivalent_names_and_types for stricter semantic type
    /// equivalence checking.
    fn logically_equivalent_names_and_types(&self, other: &Self) -> bool;
}

impl SchemaExt for Schema {
    fn equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }

        self.fields()
            .iter()
            .zip(other.fields().iter())
            .all(|(f1, f2)| {
                f1.name() == f2.name()
                    && DFSchema::datatype_is_semantically_equal(
                        f1.data_type(),
                        f2.data_type(),
                    )
            })
    }

    fn logically_equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }

        self.fields()
            .iter()
            .zip(other.fields().iter())
            .all(|(f1, f2)| {
                f1.name() == f2.name()
                    && DFSchema::datatype_is_logically_equal(
                        f1.data_type(),
                        f2.data_type(),
                    )
            })
    }
}

fn qualified_name(qualifier: Option<&TableReference>, name: &str) -> String {
    match qualifier {
        Some(q) => format!("{}.{}", q, name),
        None => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_contains;

    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn qualifier_in_name() -> Result<()> {
        let col = Column::from_name("t1.c0");
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        // lookup with unqualified name "t1.c0"
        let err = schema.index_of_column(&col).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Schema error: No field named \"t1.c0\". Valid fields are t1.c0, t1.c1."
        );
        Ok(())
    }

    #[test]
    fn quoted_qualifiers_in_name() -> Result<()> {
        let col = Column::from_name("t1.c0");
        let schema = DFSchema::try_from_qualified_schema(
            "t1",
            &Schema::new(vec![
                Field::new("CapitalColumn", DataType::Boolean, true),
                Field::new("field.with.period", DataType::Boolean, true),
            ]),
        )?;

        // lookup with unqualified name "t1.c0"
        let err = schema.index_of_column(&col).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Schema error: No field named \"t1.c0\". Valid fields are t1.\"CapitalColumn\", t1.\"field.with.period\"."
        );
        Ok(())
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
        assert!(join
            .field_with_qualified_name(&TableReference::bare("t1"), "c0")
            .is_ok());
        assert!(join
            .field_with_qualified_name(&TableReference::bare("t2"), "c0")
            .is_ok());
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
        assert!(join.err().is_none());
        Ok(())
    }

    #[test]
    fn join_unqualified_duplicate() -> Result<()> {
        let left = DFSchema::try_from(test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert_eq!(
            join.unwrap_err().strip_backtrace(),
            "Schema error: Schema contains duplicate unqualified field name c0"
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
        assert!(join
            .field_with_qualified_name(&TableReference::bare("t1"), "c0")
            .is_ok());
        assert!(join.field_with_unqualified_name("c0").is_ok());
        assert!(join.field_with_unqualified_name("c100").is_ok());
        assert!(join.field_with_name(None, "c100").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c100").is_err());
        assert!(join
            .field_with_qualified_name(&TableReference::bare(""), "c100")
            .is_err());
        Ok(())
    }

    #[test]
    fn join_mixed_duplicate() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert_contains!(join.unwrap_err().to_string(),
                         "Schema error: Schema contains qualified \
                          field name t1.c0 and unqualified field name c0 which would be ambiguous");
        Ok(())
    }

    #[allow(deprecated)]
    #[test]
    fn helpful_error_messages() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let expected_help = "Valid fields are t1.c0, t1.c1.";
        // Pertinent message parts
        let expected_err_msg = "Fully qualified field name 't1.c0'";
        assert_contains!(
            schema
                .field_with_qualified_name(&TableReference::bare("x"), "y")
                .unwrap_err()
                .to_string(),
            expected_help
        );
        assert_contains!(
            schema
                .field_with_unqualified_name("y")
                .unwrap_err()
                .to_string(),
            expected_help
        );
        let y_col = Column::new_unqualified("y");
        assert_contains!(
            schema.index_of_column(&y_col).unwrap_err().to_string(),
            expected_help
        );
        let c0_column = Column::new(Some("t1"), "c0");
        assert_contains!(
            schema.index_of_column(&c0_column).unwrap_err().to_string(),
            expected_err_msg
        );
        Ok(())
    }

    #[test]
    fn select_without_valid_fields() {
        let schema = DFSchema::empty();

        let col = Column::from_qualified_name("t1.c0");
        let err = schema.index_of_column(&col).unwrap_err();
        assert_eq!(err.strip_backtrace(), "Schema error: No field named t1.c0.");

        // the same check without qualifier
        let col = Column::from_name("c0");
        let err = schema.index_of_column(&col).err().unwrap();
        assert_eq!(err.strip_backtrace(), "Schema error: No field named c0.");
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

        let df_schema = DFSchema {
            inner: arrow_schema_ref.clone(),
            field_qualifiers: vec![None; arrow_schema_ref.fields.len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };
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
        let mut a_metadata = HashMap::new();
        a_metadata.insert("key".to_string(), "value".to_string());
        let a_field = Field::new("a", DataType::Int64, false).with_metadata(a_metadata);

        let mut b_metadata = HashMap::new();
        b_metadata.insert("key".to_string(), "value".to_string());
        let b_field = Field::new("b", DataType::Int64, false).with_metadata(b_metadata);

        let schema = Arc::new(Schema::new(vec![a_field, b_field]));

        let df_schema = DFSchema {
            inner: schema.clone(),
            field_qualifiers: vec![None; schema.fields.len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };

        assert_eq!(df_schema.inner.metadata(), schema.metadata())
    }

    #[test]
    fn test_contain_column() -> Result<()> {
        // qualified exists
        {
            let col = Column::from_qualified_name("t1.c0");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(schema.is_column_from_schema(&col)?);
        }

        // qualified not exists
        {
            let col = Column::from_qualified_name("t1.c2");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(!schema.is_column_from_schema(&col)?);
        }

        // unqualified exists
        {
            let col = Column::from_name("c0");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(schema.is_column_from_schema(&col)?);
        }

        // unqualified not exists
        {
            let col = Column::from_name("c2");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(!schema.is_column_from_schema(&col)?);
        }

        Ok(())
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
        (0..n).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
    }
}
