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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use crate::error::{_plan_err, _schema_err, DataFusionError, Result};
use crate::{
    Column, FunctionalDependencies, SchemaError, TableReference, field_not_found,
    unqualified_field_not_found,
};

use arrow::compute::can_cast_types;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Schema, SchemaBuilder, SchemaRef,
};

/// A reference-counted reference to a [DFSchema].
pub type DFSchemaRef = Arc<DFSchema>;

/// DFSchema wraps an Arrow schema and add a relation (table) name.
///
/// The schema may hold the fields across multiple tables. Some fields may be
/// qualified and some unqualified. A qualified field is a field that has a
/// relation name associated with it.
///
/// Unqualified fields must be unique not only amongst themselves, but also must
/// have a distinct name from any qualified field names. This allows finding a
/// qualified field by name to be possible, so long as there aren't multiple
/// qualified fields with the same name.
///]
/// # See Also
/// * [DFSchemaRef], an alias to `Arc<DFSchema>`
/// * [DataTypeExt], common methods for working with Arrow [DataType]s
/// * [FieldExt], extension methods for working with Arrow [Field]s
///
/// [DataTypeExt]: crate::datatype::DataTypeExt
/// [FieldExt]: crate::datatype::FieldExt
///
/// # Creating qualified schemas
///
/// Use [DFSchema::try_from_qualified_schema] to create a qualified schema from
/// an Arrow schema.
///
/// ```rust
/// use arrow::datatypes::{DataType, Field, Schema};
/// use datafusion_common::{Column, DFSchema};
///
/// let arrow_schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
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
/// use arrow::datatypes::{DataType, Field, Schema};
/// use datafusion_common::{Column, DFSchema};
///
/// let arrow_schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
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
/// use arrow::datatypes::{Field, Schema};
/// use datafusion_common::DFSchema;
/// use std::collections::HashMap;
///
/// let df_schema = DFSchema::from_unqualified_fields(
///     vec![Field::new("c1", arrow::datatypes::DataType::Int32, false)].into(),
///     HashMap::new(),
/// )
/// .unwrap();
/// let schema: &Schema = df_schema.as_arrow();
/// assert_eq!(schema.fields().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFSchema {
    /// Inner Arrow schema reference.
    inner: SchemaRef,
    /// Optional qualifiers for each column in this schema. In the same order as
    /// the `self.inner.fields()`
    field_qualifiers: Vec<Option<TableReference>>,
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

    /// Return a reference to the inner Arrow [`Schema`]
    ///
    /// Note this does not have the qualifier information
    pub fn as_arrow(&self) -> &Schema {
        self.inner.as_ref()
    }

    /// Return a reference to the inner Arrow [`SchemaRef`]
    ///
    /// Note this does not have the qualifier information
    pub fn inner(&self) -> &SchemaRef {
        &self.inner
    }

    /// Create a `DFSchema` from an Arrow schema where all the fields have a given qualifier
    pub fn new_with_metadata(
        qualified_fields: Vec<(Option<TableReference>, Arc<Field>)>,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        let (qualifiers, fields): (Vec<Option<TableReference>>, Vec<Arc<Field>>) =
            qualified_fields.into_iter().unzip();

        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));

        let dfschema = Self {
            inner: schema,
            field_qualifiers: qualifiers,
            functional_dependencies: FunctionalDependencies::empty(),
        };
        dfschema.check_names()?;
        Ok(dfschema)
    }

    /// Create a new `DFSchema` from a list of Arrow [Field]s
    pub fn from_unqualified_fields(
        fields: Fields,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        let field_count = fields.len();
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let dfschema = Self {
            inner: schema,
            field_qualifiers: vec![None; field_count],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        dfschema.check_names()?;
        Ok(dfschema)
    }

    /// Create a `DFSchema` from an Arrow schema and a given qualifier
    ///
    /// To create a schema from an Arrow schema without a qualifier, use
    /// `DFSchema::try_from`.
    pub fn try_from_qualified_schema(
        qualifier: impl Into<TableReference>,
        schema: &Schema,
    ) -> Result<Self> {
        let qualifier = qualifier.into();
        let schema = DFSchema {
            inner: schema.clone().into(),
            field_qualifiers: vec![Some(qualifier); schema.fields.len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        schema.check_names()?;
        Ok(schema)
    }

    /// Create a `DFSchema` from an Arrow schema where all the fields have a given qualifier
    pub fn from_field_specific_qualified_schema(
        qualifiers: Vec<Option<TableReference>>,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let dfschema = Self {
            inner: Arc::clone(schema),
            field_qualifiers: qualifiers,
            functional_dependencies: FunctionalDependencies::empty(),
        };
        dfschema.check_names()?;
        Ok(dfschema)
    }

    /// Return the same schema, where all fields have a given qualifier.
    pub fn with_field_specific_qualified_schema(
        &self,
        qualifiers: Vec<Option<TableReference>>,
    ) -> Result<Self> {
        if qualifiers.len() != self.fields().len() {
            return _plan_err!(
                "Number of qualifiers must match number of fields. Expected {}, got {}",
                self.fields().len(),
                qualifiers.len()
            );
        }
        Ok(DFSchema {
            inner: Arc::clone(&self.inner),
            field_qualifiers: qualifiers,
            functional_dependencies: self.functional_dependencies.clone(),
        })
    }

    /// Check if the schema have some fields with the same name
    pub fn check_names(&self) -> Result<()> {
        let mut qualified_names = BTreeSet::new();
        let mut unqualified_names = BTreeSet::new();

        for (field, qualifier) in self.inner.fields().iter().zip(&self.field_qualifiers) {
            if let Some(qualifier) = qualifier {
                if !qualified_names.insert((qualifier, field.name())) {
                    return _schema_err!(SchemaError::DuplicateQualifiedField {
                        qualifier: Box::new(qualifier.clone()),
                        name: field.name().to_string(),
                    });
                }
            } else if !unqualified_names.insert(field.name()) {
                return _schema_err!(SchemaError::DuplicateUnqualifiedField {
                    name: field.name().to_string()
                });
            }
        }

        for (qualifier, name) in qualified_names {
            if unqualified_names.contains(name) {
                return _schema_err!(SchemaError::AmbiguousReference {
                    field: Box::new(Column::new(Some(qualifier.clone()), name))
                });
            }
        }
        Ok(())
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
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.extend(self.inner.fields().iter().cloned());
        schema_builder.extend(schema.fields().iter().cloned());
        let new_schema = schema_builder.finish();

        let mut new_metadata = self.inner.metadata.clone();
        new_metadata.extend(schema.inner.metadata.clone());
        let new_schema_with_metadata = new_schema.with_metadata(new_metadata);

        let mut new_qualifiers = self.field_qualifiers.clone();
        new_qualifiers.extend_from_slice(schema.field_qualifiers.as_slice());

        let new_self = Self {
            inner: Arc::new(new_schema_with_metadata),
            field_qualifiers: new_qualifiers,
            functional_dependencies: FunctionalDependencies::empty(),
        };
        new_self.check_names()?;
        Ok(new_self)
    }

    /// Modify this schema by appending the fields from the supplied schema, ignoring any
    /// duplicate fields.
    ///
    /// ## Merge Precedence
    ///
    /// **Schema-level metadata**: Metadata from both schemas is merged.
    /// If both schemas have the same metadata key, the value from the `other_schema` parameter takes precedence.
    ///
    /// **Field-level merging**: Only non-duplicate fields are added. This means that the
    /// `self` fields will always take precedence over the `other_schema` fields.
    /// Duplicate field detection is based on:
    /// - For qualified fields: both qualifier and field name must match
    /// - For unqualified fields: only field name needs to match
    ///
    /// Take note how the precedence for fields & metadata merging differs;
    /// merging prefers fields from `self` but prefers metadata from `other_schema`.
    pub fn merge(&mut self, other_schema: &DFSchema) {
        if other_schema.inner.fields.is_empty() {
            return;
        }

        let self_fields: HashSet<(Option<&TableReference>, &FieldRef)> =
            self.iter().collect();
        let self_unqualified_names: HashSet<&str> = self
            .inner
            .fields
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        let mut schema_builder = SchemaBuilder::from(self.inner.fields.clone());
        let mut qualifiers = Vec::new();
        for (qualifier, field) in other_schema.iter() {
            // skip duplicate columns
            let duplicated_field = match qualifier {
                Some(q) => self_fields.contains(&(Some(q), field)),
                // for unqualified columns, check as unqualified name
                None => self_unqualified_names.contains(field.name().as_str()),
            };
            if !duplicated_field {
                schema_builder.push(Arc::clone(field));
                qualifiers.push(qualifier.cloned());
            }
        }
        let mut metadata = self.inner.metadata.clone();
        metadata.extend(other_schema.inner.metadata.clone());

        let finished = schema_builder.finish();
        let finished_with_metadata = finished.with_metadata(metadata);
        self.inner = finished_with_metadata.into();
        self.field_qualifiers.extend(qualifiers);
    }

    /// Get a list of fields for this schema
    pub fn fields(&self) -> &Fields {
        &self.inner.fields
    }

    /// Returns a reference to [`FieldRef`] for a column at specific index
    /// within the schema.
    ///
    /// See also [Self::qualified_field] to get both qualifier and field
    pub fn field(&self, i: usize) -> &FieldRef {
        &self.inner.fields[i]
    }

    /// Returns the qualifier (if any) and [`FieldRef`] for a column at specific
    /// index within the schema.
    pub fn qualified_field(&self, i: usize) -> (Option<&TableReference>, &FieldRef) {
        (self.field_qualifiers[i].as_ref(), self.field(i))
    }

    pub fn index_of_column_by_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Option<usize> {
        let mut matches = self
            .iter()
            .enumerate()
            .filter(|(_, (q, f))| match (qualifier, q) {
                // field to lookup is qualified.
                // current field is qualified and not shared between relations, compare both
                // qualifier and name.
                (Some(q), Some(field_q)) => q.resolved_eq(field_q) && f.name() == name,
                // field to lookup is qualified but current field is unqualified.
                (Some(_), None) => false,
                // field to lookup is unqualified, no need to compare qualifier
                (None, Some(_)) | (None, None) => f.name() == name,
            })
            .map(|(idx, _)| idx);
        matches.next()
    }

    /// Find the index of the column with the given qualifier and name,
    /// returning `None` if not found
    ///
    /// See [Self::index_of_column] for a version that returns an error if the
    /// column is not found
    pub fn maybe_index_of_column(&self, col: &Column) -> Option<usize> {
        self.index_of_column_by_name(col.relation.as_ref(), &col.name)
    }

    /// Find the index of the column with the given qualifier and name,
    /// returning `Err` if not found
    ///
    /// See [Self::maybe_index_of_column] for a version that returns `None` if
    /// the column is not found
    pub fn index_of_column(&self, col: &Column) -> Result<usize> {
        self.maybe_index_of_column(col)
            .ok_or_else(|| field_not_found(col.relation.clone(), &col.name, self))
    }

    /// Check if the column is in the current schema
    pub fn is_column_from_schema(&self, col: &Column) -> bool {
        self.index_of_column_by_name(col.relation.as_ref(), &col.name)
            .is_some()
    }

    /// Find the [`FieldRef`] with the given name and optional qualifier
    pub fn field_with_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Result<&FieldRef> {
        if let Some(qualifier) = qualifier {
            self.field_with_qualified_name(qualifier, name)
        } else {
            self.field_with_unqualified_name(name)
        }
    }

    /// Find the qualified field with the given name
    pub fn qualified_field_with_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Result<(Option<&TableReference>, &FieldRef)> {
        if let Some(qualifier) = qualifier {
            let idx = self
                .index_of_column_by_name(Some(qualifier), name)
                .ok_or_else(|| field_not_found(Some(qualifier.clone()), name, self))?;
            Ok((self.field_qualifiers[idx].as_ref(), self.field(idx)))
        } else {
            self.qualified_field_with_unqualified_name(name)
        }
    }

    /// Find all fields having the given qualifier
    pub fn fields_with_qualified(&self, qualifier: &TableReference) -> Vec<&FieldRef> {
        self.iter()
            .filter(|(q, _)| q.map(|q| q.eq(qualifier)).unwrap_or(false))
            .map(|(_, f)| f)
            .collect()
    }

    /// Find all fields indices having the given qualifier
    pub fn fields_indices_with_qualified(
        &self,
        qualifier: &TableReference,
    ) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter_map(|(idx, (q, _))| q.and_then(|q| q.eq(qualifier).then_some(idx)))
            .collect()
    }

    /// Find all fields that match the given name
    pub fn fields_with_unqualified_name(&self, name: &str) -> Vec<&FieldRef> {
        self.fields()
            .iter()
            .filter(|field| field.name() == name)
            .collect()
    }

    /// Find all fields that match the given name and return them with their qualifier
    pub fn qualified_fields_with_unqualified_name(
        &self,
        name: &str,
    ) -> Vec<(Option<&TableReference>, &FieldRef)> {
        self.iter()
            .filter(|(_, field)| field.name() == name)
            .collect()
    }

    /// Find all fields that match the given name and convert to column
    pub fn columns_with_unqualified_name(&self, name: &str) -> Vec<Column> {
        self.iter()
            .filter(|(_, field)| field.name() == name)
            .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
            .collect()
    }

    /// Return all `Column`s for the schema
    pub fn columns(&self) -> Vec<Column> {
        self.iter()
            .map(|(qualifier, field)| {
                Column::new(qualifier.cloned(), field.name().clone())
            })
            .collect()
    }

    /// Find the qualified field with the given unqualified name
    pub fn qualified_field_with_unqualified_name(
        &self,
        name: &str,
    ) -> Result<(Option<&TableReference>, &FieldRef)> {
        let matches = self.qualified_fields_with_unqualified_name(name);
        match matches.len() {
            0 => Err(unqualified_field_not_found(name, self)),
            1 => Ok((matches[0].0, matches[0].1)),
            _ => {
                // When `matches` size > 1, it doesn't necessarily mean an `ambiguous name` problem.
                // Because name may generate from Alias/... . It means that it don't own qualifier.
                // For example:
                //             Join on id = b.id
                // Project a.id as id   TableScan b id
                // In this case, there isn't `ambiguous name` problem. When `matches` just contains
                // one field without qualifier, we should return it.
                let fields_without_qualifier = matches
                    .iter()
                    .filter(|(q, _)| q.is_none())
                    .collect::<Vec<_>>();
                if fields_without_qualifier.len() == 1 {
                    Ok((fields_without_qualifier[0].0, fields_without_qualifier[0].1))
                } else {
                    _schema_err!(SchemaError::AmbiguousReference {
                        field: Box::new(Column::new_unqualified(name.to_string()))
                    })
                }
            }
        }
    }

    /// Find the field with the given name
    pub fn field_with_unqualified_name(&self, name: &str) -> Result<&FieldRef> {
        self.qualified_field_with_unqualified_name(name)
            .map(|(_, field)| field)
    }

    /// Find the field with the given qualified name
    pub fn field_with_qualified_name(
        &self,
        qualifier: &TableReference,
        name: &str,
    ) -> Result<&FieldRef> {
        let idx = self
            .index_of_column_by_name(Some(qualifier), name)
            .ok_or_else(|| field_not_found(Some(qualifier.clone()), name, self))?;

        Ok(self.field(idx))
    }

    /// Find the field with the given qualified column
    pub fn qualified_field_from_column(
        &self,
        column: &Column,
    ) -> Result<(Option<&TableReference>, &FieldRef)> {
        self.qualified_field_with_name(column.relation.as_ref(), &column.name)
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
            .any(|(q, f)| q.map(|q| q.eq(qualifier)).unwrap_or(false) && f.name() == name)
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
    #[deprecated(since = "47.0.0", note = "This method is no longer used")]
    pub fn check_arrow_schema_type_compatible(
        &self,
        arrow_schema: &Schema,
    ) -> Result<()> {
        let self_arrow_schema = self.as_arrow();
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

    #[deprecated(since = "47.0.0", note = "Use has_equivalent_names_and_types` instead")]
    pub fn equivalent_names_and_types(&self, other: &Self) -> bool {
        self.has_equivalent_names_and_types(other).is_ok()
    }

    /// Returns Ok if the two schemas have the same qualified named
    /// fields with the compatible data types.
    ///
    /// Returns an `Err` with a message otherwise.
    ///
    /// This is a specialized version of Eq that ignores differences in
    /// nullability and metadata.
    ///
    /// Use [DFSchema]::logically_equivalent_names_and_types for a weaker
    /// logical type checking, which for example would consider a dictionary
    /// encoded UTF8 array to be equivalent to a plain UTF8 array.
    pub fn has_equivalent_names_and_types(&self, other: &Self) -> Result<()> {
        // case 1 : schema length mismatch
        if self.fields().len() != other.fields().len() {
            _plan_err!(
                "Schema mismatch: the schema length are not same \
            Expected schema length: {}, got: {}",
                self.fields().len(),
                other.fields().len()
            )
        } else {
            // case 2 : schema length match, but fields mismatch
            // check if the fields name are the same and have the same data types
            self.fields()
                .iter()
                .zip(other.fields().iter())
                .try_for_each(|(f1, f2)| {
                    if f1.name() != f2.name()
                        || (!DFSchema::datatype_is_semantically_equal(
                            f1.data_type(),
                            f2.data_type(),
                        ))
                    {
                        _plan_err!(
                            "Schema mismatch: Expected field '{}' with type {}, \
                            but got '{}' with type {}.",
                            f1.name(),
                            f1.data_type(),
                            f2.name(),
                            f2.data_type()
                        )
                    } else {
                        Ok(())
                    }
                })
        }
    }

    /// Checks if two [`DataType`]s are logically equal. This is a notably weaker constraint
    /// than datatype_is_semantically_equal in that different representations of same data can be
    /// logically but not semantically equivalent. Semantically equivalent types are always also
    /// logically equivalent. For example:
    /// - a Dictionary<K,V> type is logically equal to a plain V type
    /// - a Dictionary<K1, V1> is also logically equal to Dictionary<K2, V1>
    /// - Utf8 and Utf8View are logically equal
    pub fn datatype_is_logically_equal(dt1: &DataType, dt2: &DataType) -> bool {
        // check nested fields
        match (dt1, dt2) {
            (DataType::Dictionary(_, v1), DataType::Dictionary(_, v2)) => {
                v1.as_ref() == v2.as_ref()
            }
            (DataType::Dictionary(_, v1), othertype) => v1.as_ref() == othertype,
            (othertype, DataType::Dictionary(_, v1)) => v1.as_ref() == othertype,
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2))
            | (DataType::FixedSizeList(f1, _), DataType::FixedSizeList(f2, _)) => {
                // Don't compare the names of the technical inner field
                // Usually "item" but that's not mandated
                Self::datatype_is_logically_equal(f1.data_type(), f2.data_type())
            }
            (DataType::Map(f1, _), DataType::Map(f2, _)) => {
                // Don't compare the names of the technical inner fields
                // Usually "entries", "key", "value" but that's not mandated
                match (f1.data_type(), f2.data_type()) {
                    (DataType::Struct(f1_inner), DataType::Struct(f2_inner)) => {
                        f1_inner.len() == f2_inner.len()
                            && f1_inner.iter().zip(f2_inner.iter()).all(|(f1, f2)| {
                                Self::datatype_is_logically_equal(
                                    f1.data_type(),
                                    f2.data_type(),
                                )
                            })
                    }
                    _ => panic!("Map type should have an inner struct field"),
                }
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
            // Utf8 and Utf8View are logically equivalent
            (DataType::Utf8, DataType::Utf8View) => true,
            (DataType::Utf8View, DataType::Utf8) => true,
            _ => Self::datatype_is_semantically_equal(dt1, dt2),
        }
    }

    /// Returns true of two [`DataType`]s are semantically equal (same
    /// name and type), ignoring both metadata and nullability, decimal precision/scale,
    /// and timezone time units/timezones.
    ///
    /// request to upstream: <https://github.com/apache/arrow-rs/issues/3199>
    pub fn datatype_is_semantically_equal(dt1: &DataType, dt2: &DataType) -> bool {
        // check nested fields
        match (dt1, dt2) {
            (DataType::Dictionary(k1, v1), DataType::Dictionary(k2, v2)) => {
                Self::datatype_is_semantically_equal(k1.as_ref(), k2.as_ref())
                    && Self::datatype_is_semantically_equal(v1.as_ref(), v2.as_ref())
            }
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2))
            | (DataType::FixedSizeList(f1, _), DataType::FixedSizeList(f2, _)) => {
                // Don't compare the names of the technical inner field
                // Usually "item" but that's not mandated
                Self::datatype_is_semantically_equal(f1.data_type(), f2.data_type())
            }
            (DataType::Map(f1, _), DataType::Map(f2, _)) => {
                // Don't compare the names of the technical inner fields
                // Usually "entries", "key", "value" but that's not mandated
                match (f1.data_type(), f2.data_type()) {
                    (DataType::Struct(f1_inner), DataType::Struct(f2_inner)) => {
                        f1_inner.len() == f2_inner.len()
                            && f1_inner.iter().zip(f2_inner.iter()).all(|(f1, f2)| {
                                Self::datatype_is_semantically_equal(
                                    f1.data_type(),
                                    f2.data_type(),
                                )
                            })
                    }
                    _ => panic!("Map type should have an inner struct field"),
                }
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
                DataType::Decimal32(_l_precision, _l_scale),
                DataType::Decimal32(_r_precision, _r_scale),
            ) => true,
            (
                DataType::Decimal64(_l_precision, _l_scale),
                DataType::Decimal64(_r_precision, _r_scale),
            ) => true,
            (
                DataType::Decimal128(_l_precision, _l_scale),
                DataType::Decimal128(_r_precision, _r_scale),
            ) => true,
            (
                DataType::Decimal256(_l_precision, _l_scale),
                DataType::Decimal256(_r_precision, _r_scale),
            ) => true,
            (
                DataType::Timestamp(_l_time_unit, _l_timezone),
                DataType::Timestamp(_r_time_unit, _r_timezone),
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
        DFSchema {
            field_qualifiers: vec![None; self.inner.fields.len()],
            inner: self.inner,
            functional_dependencies: self.functional_dependencies,
        }
    }

    /// Replace all field qualifier with new value in schema
    pub fn replace_qualifier(self, qualifier: impl Into<TableReference>) -> Self {
        let qualifier = qualifier.into();
        DFSchema {
            field_qualifiers: vec![Some(qualifier); self.inner.fields.len()],
            inner: self.inner,
            functional_dependencies: self.functional_dependencies,
        }
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

    /// Iterate over the qualifiers and fields in the DFSchema
    pub fn iter(&self) -> impl Iterator<Item = (Option<&TableReference>, &FieldRef)> {
        self.field_qualifiers
            .iter()
            .zip(self.inner.fields().iter())
            .map(|(qualifier, field)| (qualifier.as_ref(), field))
    }
    /// Returns a tree-like string representation of the schema.
    ///
    /// This method formats the schema
    /// with a tree-like structure showing field names, types, and nullability.
    ///
    /// # Example
    ///
    /// ```
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use datafusion_common::DFSchema;
    /// use std::collections::HashMap;
    ///
    /// let schema = DFSchema::from_unqualified_fields(
    ///     vec![
    ///         Field::new("id", DataType::Int32, false),
    ///         Field::new("name", DataType::Utf8, true),
    ///     ]
    ///     .into(),
    ///     HashMap::new(),
    /// )
    /// .unwrap();
    ///
    /// assert_eq!(
    ///     schema.tree_string().to_string(),
    ///     r#"root
    ///  |-- id: int32 (nullable = false)
    ///  |-- name: utf8 (nullable = true)"#
    /// );
    /// ```
    pub fn tree_string(&self) -> impl Display + '_ {
        let mut result = String::from("root\n");

        for (qualifier, field) in self.iter() {
            let field_name = match qualifier {
                Some(q) => format!("{}.{}", q, field.name()),
                None => field.name().to_string(),
            };

            format_field_with_indent(
                &mut result,
                &field_name,
                field.data_type(),
                field.is_nullable(),
                " ",
            );
        }

        // Remove the trailing newline
        if result.ends_with('\n') {
            result.pop();
        }

        result
    }
}

/// Format field with proper nested indentation for complex types
fn format_field_with_indent(
    result: &mut String,
    field_name: &str,
    data_type: &DataType,
    nullable: bool,
    indent: &str,
) {
    let nullable_str = nullable.to_string().to_lowercase();
    let child_indent = format!("{indent}|    ");

    match data_type {
        DataType::List(field) => {
            result.push_str(&format!(
                "{indent}|-- {field_name}: list (nullable = {nullable_str})\n"
            ));
            format_field_with_indent(
                result,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                &child_indent,
            );
        }
        DataType::LargeList(field) => {
            result.push_str(&format!(
                "{indent}|-- {field_name}: large list (nullable = {nullable_str})\n"
            ));
            format_field_with_indent(
                result,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                &child_indent,
            );
        }
        DataType::FixedSizeList(field, _size) => {
            result.push_str(&format!(
                "{indent}|-- {field_name}: fixed size list (nullable = {nullable_str})\n"
            ));
            format_field_with_indent(
                result,
                field.name(),
                field.data_type(),
                field.is_nullable(),
                &child_indent,
            );
        }
        DataType::Map(field, _) => {
            result.push_str(&format!(
                "{indent}|-- {field_name}: map (nullable = {nullable_str})\n"
            ));
            if let DataType::Struct(inner_fields) = field.data_type()
                && inner_fields.len() == 2
            {
                format_field_with_indent(
                    result,
                    "key",
                    inner_fields[0].data_type(),
                    inner_fields[0].is_nullable(),
                    &child_indent,
                );
                let value_contains_null = field.is_nullable().to_string().to_lowercase();
                // Handle complex value types properly
                match inner_fields[1].data_type() {
                    DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Map(_, _) => {
                        format_field_with_indent(
                            result,
                            "value",
                            inner_fields[1].data_type(),
                            inner_fields[1].is_nullable(),
                            &child_indent,
                        );
                    }
                    _ => {
                        result.push_str(&format!("{child_indent}|-- value: {} (nullable = {value_contains_null})\n",
                                format_simple_data_type(inner_fields[1].data_type())));
                    }
                }
            }
        }
        DataType::Struct(fields) => {
            result.push_str(&format!(
                "{indent}|-- {field_name}: struct (nullable = {nullable_str})\n"
            ));
            for struct_field in fields {
                format_field_with_indent(
                    result,
                    struct_field.name(),
                    struct_field.data_type(),
                    struct_field.is_nullable(),
                    &child_indent,
                );
            }
        }
        _ => {
            let type_str = format_simple_data_type(data_type);
            result.push_str(&format!(
                "{indent}|-- {field_name}: {type_str} (nullable = {nullable_str})\n"
            ));
        }
    }
}

/// Format simple DataType in lowercase format (for leaf nodes)
fn format_simple_data_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::FixedSizeBinary(_) => "fixed_size_binary".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Time32(_) => "time32".to_string(),
        DataType::Time64(_) => "time64".to_string(),
        DataType::Timestamp(_, tz) => match tz {
            Some(tz_str) => format!("timestamp ({tz_str})"),
            None => "timestamp".to_string(),
        },
        DataType::Interval(_) => "interval".to_string(),
        DataType::Dictionary(_, value_type) => {
            format_simple_data_type(value_type.as_ref())
        }
        DataType::Decimal32(precision, scale) => {
            format!("decimal32({precision}, {scale})")
        }
        DataType::Decimal64(precision, scale) => {
            format!("decimal64({precision}, {scale})")
        }
        DataType::Decimal128(precision, scale) => {
            format!("decimal128({precision}, {scale})")
        }
        DataType::Decimal256(precision, scale) => {
            format!("decimal256({precision}, {scale})")
        }
        DataType::Null => "null".to_string(),
        _ => format!("{data_type}").to_lowercase(),
    }
}

/// Allow DFSchema to be converted into an Arrow `&Schema`
impl AsRef<Schema> for DFSchema {
    fn as_ref(&self) -> &Schema {
        self.as_arrow()
    }
}

/// Allow DFSchema to be converted into an Arrow `&SchemaRef` (to clone, for
/// example)
impl AsRef<SchemaRef> for DFSchema {
    fn as_ref(&self) -> &SchemaRef {
        self.inner()
    }
}

/// Create a `DFSchema` from an Arrow schema
impl TryFrom<Schema> for DFSchema {
    type Error = DataFusionError;
    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        Self::try_from(Arc::new(schema))
    }
}

impl TryFrom<SchemaRef> for DFSchema {
    type Error = DataFusionError;
    fn try_from(schema: SchemaRef) -> Result<Self, Self::Error> {
        let field_count = schema.fields.len();
        let dfschema = Self {
            inner: schema,
            field_qualifiers: vec![None; field_count],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        // Without checking names, because schema here may have duplicate field names.
        // For example, Partial AggregateMode will generate duplicate field names from
        // state_fields.
        // See <https://github.com/apache/datafusion/issues/17715>
        // dfschema.check_names()?;
        Ok(dfschema)
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
        DFSchema::try_from(self)
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
    fn nullable(&self, col: &Column) -> Result<bool> {
        Ok(self.field_from_column(col)?.is_nullable())
    }

    /// What is the datatype of this column?
    fn data_type(&self, col: &Column) -> Result<&DataType> {
        Ok(self.field_from_column(col)?.data_type())
    }

    /// Returns the column's optional metadata.
    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>> {
        Ok(self.field_from_column(col)?.metadata())
    }

    /// Return the column's datatype and nullability
    fn data_type_and_nullable(&self, col: &Column) -> Result<(&DataType, bool)> {
        let field = self.field_from_column(col)?;
        Ok((field.data_type(), field.is_nullable()))
    }

    // Return the column's field
    fn field_from_column(&self, col: &Column) -> Result<&FieldRef>;
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

    fn data_type_and_nullable(&self, col: &Column) -> Result<(&DataType, bool)> {
        self.as_ref().data_type_and_nullable(col)
    }

    fn field_from_column(&self, col: &Column) -> Result<&FieldRef> {
        self.as_ref().field_from_column(col)
    }
}

impl ExprSchema for DFSchema {
    fn field_from_column(&self, col: &Column) -> Result<&FieldRef> {
        match &col.relation {
            Some(r) => self.field_with_qualified_name(r, &col.name),
            None => self.field_with_unqualified_name(&col.name),
        }
    }
}

/// DataFusion-specific extensions to [`Schema`].
pub trait SchemaExt {
    /// This is a specialized version of Eq that ignores differences
    /// in nullability and metadata.
    ///
    /// It works the same as [`DFSchema::equivalent_names_and_types`].
    fn equivalent_names_and_types(&self, other: &Self) -> bool;

    /// Returns nothing if the two schemas have the same qualified named
    /// fields with logically equivalent data types. Returns internal error otherwise.
    ///
    /// Use [DFSchema]::equivalent_names_and_types for stricter semantic type
    /// equivalence checking.
    ///
    /// It is only used by insert into cases.
    fn logically_equivalent_names_and_types(&self, other: &Self) -> Result<()>;
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

    // It is only used by insert into cases.
    fn logically_equivalent_names_and_types(&self, other: &Self) -> Result<()> {
        // case 1 : schema length mismatch
        if self.fields().len() != other.fields().len() {
            _plan_err!(
                "Inserting query must have the same schema length as the table. \
            Expected table schema length: {}, got: {}",
                self.fields().len(),
                other.fields().len()
            )
        } else {
            // case 2 : schema length match, but fields mismatch
            // check if the fields name are the same and have the same data types
            self.fields()
                .iter()
                .zip(other.fields().iter())
                .try_for_each(|(f1, f2)| {
                    if f1.name() != f2.name() || (!DFSchema::datatype_is_logically_equal(f1.data_type(), f2.data_type()) && !can_cast_types(f2.data_type(), f1.data_type())) {
                        _plan_err!(
                            "Inserting query schema mismatch: Expected table field '{}' with type {}, \
                            but got '{}' with type {}.",
                            f1.name(),
                            f1.data_type(),
                            f2.name(),
                            f2.data_type())
                    } else {
                        Ok(())
                    }
                })
        }
    }
}

pub fn qualified_name(qualifier: Option<&TableReference>, name: &str) -> String {
    match qualifier {
        Some(q) => format!("{q}.{name}"),
        None => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_contains;

    use super::*;

    #[test]
    fn qualifier_in_name() -> Result<()> {
        let col = Column::from_name("t1.c0");
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        // lookup with unqualified name "t1.c0"
        let err = schema.index_of_column(&col).unwrap_err();
        let expected = "Schema error: No field named \"t1.c0\". \
            Column names are case sensitive. \
            You can use double quotes to refer to the \"\"t1.c0\"\" column \
            or set the datafusion.sql_parser.enable_ident_normalization configuration. \
            Did you mean 't1.c0'?.";
        assert_eq!(err.strip_backtrace(), expected);
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
        let expected = "Schema error: No field named \"t1.c0\". \
            Valid fields are t1.\"CapitalColumn\", t1.\"field.with.period\".";
        assert_eq!(err.strip_backtrace(), expected);
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
    fn test_from_field_specific_qualified_schema() -> Result<()> {
        let schema = DFSchema::from_field_specific_qualified_schema(
            vec![Some("t1".into()), None],
            &Arc::new(Schema::new(vec![
                Field::new("c0", DataType::Boolean, true),
                Field::new("c1", DataType::Boolean, true),
            ])),
        )?;
        assert_eq!("fields:[t1.c0, c1], metadata:{}", schema.to_string());
        Ok(())
    }

    #[test]
    fn test_from_qualified_fields() -> Result<()> {
        let schema = DFSchema::new_with_metadata(
            vec![
                (
                    Some("t0".into()),
                    Arc::new(Field::new("c0", DataType::Boolean, true)),
                ),
                (None, Arc::new(Field::new("c1", DataType::Boolean, true))),
            ],
            HashMap::new(),
        )?;
        assert_eq!("fields:[t0.c0, c1], metadata:{}", schema.to_string());
        Ok(())
    }

    #[test]
    fn from_qualified_schema_into_arrow_schema() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let arrow_schema = schema.as_arrow();
        insta::assert_snapshot!(arrow_schema.to_string(), @r#"Field { "c0": nullable Boolean }, Field { "c1": nullable Boolean }"#);
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
        assert!(
            join.field_with_qualified_name(&TableReference::bare("t1"), "c0")
                .is_ok()
        );
        assert!(
            join.field_with_qualified_name(&TableReference::bare("t2"), "c0")
                .is_ok()
        );
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
        assert_eq!(
            join.unwrap_err().strip_backtrace(),
            "Schema error: Schema contains duplicate qualified field name t1.c0",
        );
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
        assert!(
            join.field_with_qualified_name(&TableReference::bare("t1"), "c0")
                .is_ok()
        );
        assert!(join.field_with_unqualified_name("c0").is_ok());
        assert!(join.field_with_unqualified_name("c100").is_ok());
        assert!(join.field_with_name(None, "c100").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c100").is_err());
        assert!(
            join.field_with_qualified_name(&TableReference::bare(""), "c100")
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn join_mixed_duplicate() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert_contains!(
            join.unwrap_err().to_string(),
            "Schema error: Schema contains qualified \
                          field name t1.c0 and unqualified field name c0 which would be ambiguous"
        );
        Ok(())
    }

    #[test]
    fn helpful_error_messages() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let expected_help = "Valid fields are t1.c0, t1.c1.";
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
        assert!(schema.index_of_column_by_name(None, "y").is_none());
        assert!(schema.index_of_column_by_name(None, "t1.c0").is_none());

        Ok(())
    }

    #[test]
    fn select_without_valid_fields() {
        let schema = DFSchema::empty();

        let col = Column::from_qualified_name("t1.c0");
        let err = schema.index_of_column(&col).unwrap_err();
        let expected = "Schema error: No field named t1.c0.";
        assert_eq!(err.strip_backtrace(), expected);

        // the same check without qualifier
        let col = Column::from_name("c0");
        let err = schema.index_of_column(&col).err().unwrap();
        let expected = "Schema error: No field named c0.";
        assert_eq!(err.strip_backtrace(), expected);
    }

    #[test]
    fn into() {
        // Demonstrate how to convert back and forth between Schema, SchemaRef, DFSchema, and DFSchemaRef
        let arrow_schema = Schema::new_with_metadata(
            vec![Field::new("c0", DataType::Int64, true)],
            test_metadata(),
        );
        let arrow_schema_ref = Arc::new(arrow_schema.clone());

        let df_schema = DFSchema {
            inner: Arc::clone(&arrow_schema_ref),
            field_qualifiers: vec![None; arrow_schema_ref.fields.len()],
            functional_dependencies: FunctionalDependencies::empty(),
        };
        let df_schema_ref = Arc::new(df_schema.clone());

        {
            let arrow_schema = arrow_schema.clone();
            let arrow_schema_ref = Arc::clone(&arrow_schema_ref);

            assert_eq!(df_schema, arrow_schema.to_dfschema().unwrap());
            assert_eq!(df_schema, arrow_schema_ref.to_dfschema().unwrap());
        }

        {
            let arrow_schema = arrow_schema.clone();
            let arrow_schema_ref = Arc::clone(&arrow_schema_ref);

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
    fn test_dfschema_to_schema_conversion() {
        let mut a_metadata = HashMap::new();
        a_metadata.insert("key".to_string(), "value".to_string());
        let a_field = Field::new("a", DataType::Int64, false).with_metadata(a_metadata);

        let mut b_metadata = HashMap::new();
        b_metadata.insert("key".to_string(), "value".to_string());
        let b_field = Field::new("b", DataType::Int64, false).with_metadata(b_metadata);

        let schema = Arc::new(Schema::new(vec![a_field, b_field]));

        let df_schema = DFSchema {
            inner: Arc::clone(&schema),
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
            assert!(schema.is_column_from_schema(&col));
        }

        // qualified not exists
        {
            let col = Column::from_qualified_name("t1.c2");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(!schema.is_column_from_schema(&col));
        }

        // unqualified exists
        {
            let col = Column::from_name("c0");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(schema.is_column_from_schema(&col));
        }

        // unqualified not exists
        {
            let col = Column::from_name("c2");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(!schema.is_column_from_schema(&col));
        }

        Ok(())
    }

    #[test]
    fn test_datatype_is_logically_equal() {
        assert!(DFSchema::datatype_is_logically_equal(
            &DataType::Int8,
            &DataType::Int8
        ));

        assert!(!DFSchema::datatype_is_logically_equal(
            &DataType::Int8,
            &DataType::Int16
        ));

        // Test lists

        // Succeeds if both have the same element type, disregards names and nullability
        assert!(DFSchema::datatype_is_logically_equal(
            &DataType::List(Field::new_list_field(DataType::Int8, true).into()),
            &DataType::List(Field::new("element", DataType::Int8, false).into())
        ));

        // Fails if element type is different
        assert!(!DFSchema::datatype_is_logically_equal(
            &DataType::List(Field::new_list_field(DataType::Int8, true).into()),
            &DataType::List(Field::new_list_field(DataType::Int16, true).into())
        ));

        // Test maps
        let map_field = DataType::Map(
            Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Int8, false),
                    Field::new("value", DataType::Int8, true),
                ])),
                true,
            )
            .into(),
            true,
        );

        // Succeeds if both maps have the same key and value types, disregards names and nullability
        assert!(DFSchema::datatype_is_logically_equal(
            &map_field,
            &DataType::Map(
                Field::new(
                    "pairs",
                    DataType::Struct(Fields::from(vec![
                        Field::new("one", DataType::Int8, false),
                        Field::new("two", DataType::Int8, false)
                    ])),
                    true
                )
                .into(),
                true
            )
        ));
        // Fails if value type is different
        assert!(!DFSchema::datatype_is_logically_equal(
            &map_field,
            &DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int8, false),
                        Field::new("value", DataType::Int16, true)
                    ])),
                    true
                )
                .into(),
                true
            )
        ));

        // Fails if key type is different
        assert!(!DFSchema::datatype_is_logically_equal(
            &map_field,
            &DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int16, false),
                        Field::new("value", DataType::Int8, true)
                    ])),
                    true
                )
                .into(),
                true
            )
        ));

        // Test structs

        let struct_field = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Int8, true),
        ]));

        // Succeeds if both have same names and datatypes, ignores nullability
        assert!(DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int8, false),
                Field::new("b", DataType::Int8, true),
            ]))
        ));

        // Fails if field names are different
        assert!(!DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int8, true),
                Field::new("y", DataType::Int8, true),
            ]))
        ));

        // Fails if types are different
        assert!(!DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int16, true),
                Field::new("b", DataType::Int8, true),
            ]))
        ));

        // Fails if more or less fields
        assert!(!DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int8, true),]))
        ));
    }

    #[test]
    fn test_datatype_is_logically_equivalent_to_dictionary() {
        // Dictionary is logically equal to its value type
        assert!(DFSchema::datatype_is_logically_equal(
            &DataType::Utf8,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        ));
    }

    #[test]
    fn test_datatype_is_semantically_equal() {
        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::Int8,
            &DataType::Int8
        ));

        assert!(!DFSchema::datatype_is_semantically_equal(
            &DataType::Int8,
            &DataType::Int16
        ));

        // Succeeds if decimal precision and scale are different
        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::Decimal32(1, 2),
            &DataType::Decimal32(2, 1),
        ));

        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::Decimal64(1, 2),
            &DataType::Decimal64(2, 1),
        ));

        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::Decimal128(1, 2),
            &DataType::Decimal128(2, 1),
        ));

        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::Decimal256(1, 2),
            &DataType::Decimal256(2, 1),
        ));

        // Any two timestamp types should match
        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into())
            ),
            &DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        ));

        // Test lists

        // Succeeds if both have the same element type, disregards names and nullability
        assert!(DFSchema::datatype_is_semantically_equal(
            &DataType::List(Field::new_list_field(DataType::Int8, true).into()),
            &DataType::List(Field::new("element", DataType::Int8, false).into())
        ));

        // Fails if element type is different
        assert!(!DFSchema::datatype_is_semantically_equal(
            &DataType::List(Field::new_list_field(DataType::Int8, true).into()),
            &DataType::List(Field::new_list_field(DataType::Int16, true).into())
        ));

        // Test maps
        let map_field = DataType::Map(
            Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Int8, false),
                    Field::new("value", DataType::Int8, true),
                ])),
                true,
            )
            .into(),
            true,
        );

        // Succeeds if both maps have the same key and value types, disregards names and nullability
        assert!(DFSchema::datatype_is_semantically_equal(
            &map_field,
            &DataType::Map(
                Field::new(
                    "pairs",
                    DataType::Struct(Fields::from(vec![
                        Field::new("one", DataType::Int8, false),
                        Field::new("two", DataType::Int8, false)
                    ])),
                    true
                )
                .into(),
                true
            )
        ));
        // Fails if value type is different
        assert!(!DFSchema::datatype_is_semantically_equal(
            &map_field,
            &DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int8, false),
                        Field::new("value", DataType::Int16, true)
                    ])),
                    true
                )
                .into(),
                true
            )
        ));

        // Fails if key type is different
        assert!(!DFSchema::datatype_is_semantically_equal(
            &map_field,
            &DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int16, false),
                        Field::new("value", DataType::Int8, true)
                    ])),
                    true
                )
                .into(),
                true
            )
        ));

        // Test structs

        let struct_field = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Int8, true),
        ]));

        // Succeeds if both have same names and datatypes, ignores nullability
        assert!(DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int8, false),
                Field::new("b", DataType::Int8, true),
            ]))
        ));

        // Fails if field names are different
        assert!(!DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int8, true),
                Field::new("y", DataType::Int8, true),
            ]))
        ));

        // Fails if types are different
        assert!(!DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int16, true),
                Field::new("b", DataType::Int8, true),
            ]))
        ));

        // Fails if more or less fields
        assert!(!DFSchema::datatype_is_logically_equal(
            &struct_field,
            &DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int8, true),]))
        ));
    }

    #[test]
    fn test_datatype_is_not_semantically_equivalent_to_dictionary() {
        // Dictionary is not semantically equal to its value type
        assert!(!DFSchema::datatype_is_semantically_equal(
            &DataType::Utf8,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        ));
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

    #[test]
    fn test_print_schema_unqualified() {
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("age", DataType::Int64, true),
                Field::new("active", DataType::Boolean, false),
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- id: int32 (nullable = false)
         |-- name: utf8 (nullable = true)
         |-- age: int64 (nullable = true)
         |-- active: boolean (nullable = false)
        ");
    }

    #[test]
    fn test_print_schema_qualified() {
        let schema = DFSchema::try_from_qualified_schema(
            "table1",
            &Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- table1.id: int32 (nullable = false)
         |-- table1.name: utf8 (nullable = true)
        ");
    }

    #[test]
    fn test_print_schema_complex_types() {
        let struct_field = Field::new(
            "address",
            DataType::Struct(Fields::from(vec![
                Field::new("street", DataType::Utf8, true),
                Field::new("city", DataType::Utf8, true),
            ])),
            true,
        );

        let list_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );

        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("id", DataType::Int32, false),
                struct_field,
                list_field,
                Field::new("score", DataType::Decimal128(10, 2), true),
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();
        insta::assert_snapshot!(output, @r"
        root
         |-- id: int32 (nullable = false)
         |-- address: struct (nullable = true)
         |    |-- street: utf8 (nullable = true)
         |    |-- city: utf8 (nullable = true)
         |-- tags: list (nullable = true)
         |    |-- item: utf8 (nullable = true)
         |-- score: decimal128(10, 2) (nullable = true)
        ");
    }

    #[test]
    fn test_print_schema_empty() {
        let schema = DFSchema::empty();
        let output = schema.tree_string();
        insta::assert_snapshot!(output, @r###"root"###);
    }

    #[test]
    fn test_print_schema_deeply_nested_types() {
        // Create a deeply nested structure to test indentation and complex type formatting
        let inner_struct = Field::new(
            "inner",
            DataType::Struct(Fields::from(vec![
                Field::new("level1", DataType::Utf8, true),
                Field::new("level2", DataType::Int32, false),
            ])),
            true,
        );

        let nested_list = Field::new(
            "nested_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("value", DataType::Float64, true),
                ])),
                true,
            ))),
            true,
        );

        let map_field = Field::new(
            "map_data",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new(
                            "value",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
                            true,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );

        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("simple_field", DataType::Utf8, true),
                inner_struct,
                nested_list,
                map_field,
                Field::new(
                    "timestamp_field",
                    DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ),
                    false,
                ),
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- simple_field: utf8 (nullable = true)
         |-- inner: struct (nullable = true)
         |    |-- level1: utf8 (nullable = true)
         |    |-- level2: int32 (nullable = false)
         |-- nested_list: list (nullable = true)
         |    |-- item: struct (nullable = true)
         |    |    |-- id: int64 (nullable = false)
         |    |    |-- value: float64 (nullable = true)
         |-- map_data: map (nullable = true)
         |    |-- key: utf8 (nullable = false)
         |    |-- value: list (nullable = true)
         |    |    |-- item: int32 (nullable = true)
         |-- timestamp_field: timestamp (UTC) (nullable = false)
        ");
    }

    #[test]
    fn test_print_schema_mixed_qualified_unqualified() {
        // Test a schema with mixed qualified and unqualified fields
        let schema = DFSchema::new_with_metadata(
            vec![
                (
                    Some("table1".into()),
                    Arc::new(Field::new("id", DataType::Int32, false)),
                ),
                (None, Arc::new(Field::new("name", DataType::Utf8, true))),
                (
                    Some("table2".into()),
                    Arc::new(Field::new("score", DataType::Float64, true)),
                ),
                (
                    None,
                    Arc::new(Field::new("active", DataType::Boolean, false)),
                ),
            ],
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- table1.id: int32 (nullable = false)
         |-- name: utf8 (nullable = true)
         |-- table2.score: float64 (nullable = true)
         |-- active: boolean (nullable = false)
        ");
    }

    #[test]
    fn test_print_schema_array_of_map() {
        // Test the specific example from user feedback: array of map
        let map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        );

        let array_of_map_field = Field::new(
            "array_map_field",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Map(Arc::new(map_field), false),
                false,
            ))),
            false,
        );

        let schema = DFSchema::from_unqualified_fields(
            vec![array_of_map_field].into(),
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- array_map_field: list (nullable = false)
         |    |-- item: map (nullable = false)
         |    |    |-- key: utf8 (nullable = false)
         |    |    |-- value: utf8 (nullable = false)
        ");
    }

    #[test]
    fn test_print_schema_complex_type_combinations() {
        // Test various combinations of list, struct, and map types

        // List of structs
        let list_of_structs = Field::new(
            "list_of_structs",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("score", DataType::Float64, true),
                ])),
                true,
            ))),
            true,
        );

        // Struct containing lists
        let struct_with_lists = Field::new(
            "struct_with_lists",
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
                Field::new(
                    "scores",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    false,
                ),
                Field::new("metadata", DataType::Utf8, true),
            ])),
            false,
        );

        // Map with struct values
        let map_with_struct_values = Field::new(
            "map_with_struct_values",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new(
                            "value",
                            DataType::Struct(Fields::from(vec![
                                Field::new("count", DataType::Int64, false),
                                Field::new("active", DataType::Boolean, true),
                            ])),
                            true,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );

        // List of maps
        let list_of_maps = Field::new(
            "list_of_maps",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ))),
            true,
        );

        // Deeply nested: struct containing list of structs containing maps
        let deeply_nested = Field::new(
            "deeply_nested",
            DataType::Struct(Fields::from(vec![
                Field::new("level1", DataType::Utf8, true),
                Field::new(
                    "level2",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![
                            Field::new("id", DataType::Int32, false),
                            Field::new(
                                "properties",
                                DataType::Map(
                                    Arc::new(Field::new(
                                        "entries",
                                        DataType::Struct(Fields::from(vec![
                                            Field::new("key", DataType::Utf8, false),
                                            Field::new("value", DataType::Float64, true),
                                        ])),
                                        false,
                                    )),
                                    false,
                                ),
                                true,
                            ),
                        ])),
                        true,
                    ))),
                    false,
                ),
            ])),
            true,
        );

        let schema = DFSchema::from_unqualified_fields(
            vec![
                list_of_structs,
                struct_with_lists,
                map_with_struct_values,
                list_of_maps,
                deeply_nested,
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- list_of_structs: list (nullable = true)
         |    |-- item: struct (nullable = true)
         |    |    |-- id: int32 (nullable = false)
         |    |    |-- name: utf8 (nullable = true)
         |    |    |-- score: float64 (nullable = true)
         |-- struct_with_lists: struct (nullable = false)
         |    |-- tags: list (nullable = true)
         |    |    |-- item: utf8 (nullable = true)
         |    |-- scores: list (nullable = false)
         |    |    |-- item: int32 (nullable = true)
         |    |-- metadata: utf8 (nullable = true)
         |-- map_with_struct_values: map (nullable = true)
         |    |-- key: utf8 (nullable = false)
         |    |-- value: struct (nullable = true)
         |    |    |-- count: int64 (nullable = false)
         |    |    |-- active: boolean (nullable = true)
         |-- list_of_maps: list (nullable = true)
         |    |-- item: map (nullable = true)
         |    |    |-- key: utf8 (nullable = false)
         |    |    |-- value: int32 (nullable = false)
         |-- deeply_nested: struct (nullable = true)
         |    |-- level1: utf8 (nullable = true)
         |    |-- level2: list (nullable = false)
         |    |    |-- item: struct (nullable = true)
         |    |    |    |-- id: int32 (nullable = false)
         |    |    |    |-- properties: map (nullable = true)
         |    |    |    |    |-- key: utf8 (nullable = false)
         |    |    |    |    |-- value: float64 (nullable = false)
        ");
    }

    #[test]
    fn test_print_schema_edge_case_types() {
        // Test edge cases and special types
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("null_field", DataType::Null, true),
                Field::new("binary_field", DataType::Binary, false),
                Field::new("large_binary", DataType::LargeBinary, true),
                Field::new("large_utf8", DataType::LargeUtf8, false),
                Field::new("fixed_size_binary", DataType::FixedSizeBinary(16), true),
                Field::new(
                    "fixed_size_list",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Int32, true)),
                        5,
                    ),
                    false,
                ),
                Field::new("decimal32", DataType::Decimal32(9, 4), true),
                Field::new("decimal64", DataType::Decimal64(9, 4), true),
                Field::new("decimal128", DataType::Decimal128(18, 4), true),
                Field::new("decimal256", DataType::Decimal256(38, 10), false),
                Field::new("date32", DataType::Date32, true),
                Field::new("date64", DataType::Date64, false),
                Field::new(
                    "time32_seconds",
                    DataType::Time32(arrow::datatypes::TimeUnit::Second),
                    true,
                ),
                Field::new(
                    "time64_nanoseconds",
                    DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
                    false,
                ),
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap();

        let output = schema.tree_string();

        insta::assert_snapshot!(output, @r"
        root
         |-- null_field: null (nullable = true)
         |-- binary_field: binary (nullable = false)
         |-- large_binary: large_binary (nullable = true)
         |-- large_utf8: large_utf8 (nullable = false)
         |-- fixed_size_binary: fixed_size_binary (nullable = true)
         |-- fixed_size_list: fixed size list (nullable = false)
         |    |-- item: int32 (nullable = true)
         |-- decimal32: decimal32(9, 4) (nullable = true)
         |-- decimal64: decimal64(9, 4) (nullable = true)
         |-- decimal128: decimal128(18, 4) (nullable = true)
         |-- decimal256: decimal256(38, 10) (nullable = false)
         |-- date32: date32 (nullable = true)
         |-- date64: date64 (nullable = false)
         |-- time32_seconds: time32 (nullable = true)
         |-- time64_nanoseconds: time64 (nullable = false)
        ");
    }
}
