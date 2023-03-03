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

//! Column

use crate::utils::{parse_identifiers_normalized, quote_identifier};
use crate::{DFSchema, DataFusionError, OwnedTableReference, Result, SchemaError};
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    /// relation/table reference.
    pub relation: Option<OwnedTableReference>,
    /// field/column name.
    pub name: String,
}

impl Column {
    /// Create Column from optional qualifier and name. The optional qualifier, if present,
    /// will be parsed and normalized by default.
    ///
    /// See full details on [`TableReference::parse_str`]
    ///
    /// [`TableReference::parse_str`]: crate::TableReference::parse_str
    pub fn new(
        relation: Option<impl Into<OwnedTableReference>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            relation: relation.map(|r| r.into()),
            name: name.into(),
        }
    }

    /// Convenience method for when there is no qualifier
    pub fn new_unqualified(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
        }
    }

    /// Create Column from unqualified name.
    pub fn from_name(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
        }
    }

    /// Deserialize a fully qualified name string into a column
    pub fn from_qualified_name(flat_name: impl Into<String>) -> Self {
        let flat_name = flat_name.into();
        let mut idents = parse_identifiers_normalized(&flat_name);

        let (relation, name) = match idents.len() {
            1 => (None, idents.remove(0)),
            2 => (
                Some(OwnedTableReference::Bare {
                    table: idents.remove(0),
                }),
                idents.remove(0),
            ),
            3 => (
                Some(OwnedTableReference::Partial {
                    schema: idents.remove(0),
                    table: idents.remove(0),
                }),
                idents.remove(0),
            ),
            4 => (
                Some(OwnedTableReference::Full {
                    catalog: idents.remove(0),
                    schema: idents.remove(0),
                    table: idents.remove(0),
                }),
                idents.remove(0),
            ),
            // any expression that failed to parse or has more than 4 period delimited
            // identifiers will be treated as an unqualified column name
            _ => (None, flat_name),
        };
        Self { relation, name }
    }

    /// Serialize column into a flat name string
    pub fn flat_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}", r, self.name),
            None => self.name.clone(),
        }
    }

    /// Serialize column into a quoted flat name string
    pub fn quoted_flat_name(&self) -> String {
        match &self.relation {
            Some(r) => {
                format!("{}.{}", r.to_quoted_string(), quote_identifier(&self.name))
            }
            None => quote_identifier(&self.name),
        }
    }

    /// Qualify column if not done yet.
    ///
    /// If this column already has a [relation](Self::relation), it will be returned as is and the given parameters are
    /// ignored. Otherwise this will search through the given schemas to find the column. This will use the first schema
    /// that matches.
    ///
    /// A schema matches if there is a single column that -- when unqualified -- matches this column. There is an
    /// exception for `USING` statements, see below.
    ///
    /// # Using columns
    /// Take the following SQL statement:
    ///
    /// ```sql
    /// SELECT id FROM t1 JOIN t2 USING(id)
    /// ```
    ///
    /// In this case, both `t1.id` and `t2.id` will match unqualified column `id`. To express this possibility, use
    /// `using_columns`. Each entry in this array is a set of columns that are bound together via a `USING` clause. So
    /// in this example this would be `[{t1.id, t2.id}]`.
    pub fn normalize_with_schemas(
        self,
        schemas: &[&Arc<DFSchema>],
        using_columns: &[HashSet<Column>],
    ) -> Result<Self> {
        if self.relation.is_some() {
            return Ok(self);
        }

        for schema in schemas {
            let fields = schema.fields_with_unqualified_name(&self.name);
            match fields.len() {
                0 => continue,
                1 => {
                    return Ok(fields[0].qualified_column());
                }
                _ => {
                    // More than 1 fields in this schema have their names set to self.name.
                    //
                    // This should only happen when a JOIN query with USING constraint references
                    // join columns using unqualified column name. For example:
                    //
                    // ```sql
                    // SELECT id FROM t1 JOIN t2 USING(id)
                    // ```
                    //
                    // In this case, both `t1.id` and `t2.id` will match unqualified column `id`.
                    // We will use the relation from the first matched field to normalize self.

                    // Compare matched fields with one USING JOIN clause at a time
                    for using_col in using_columns {
                        let all_matched = fields
                            .iter()
                            .all(|f| using_col.contains(&f.qualified_column()));
                        // All matched fields belong to the same using column set, in orther words
                        // the same join clause. We simply pick the qualifer from the first match.
                        if all_matched {
                            return Ok(fields[0].qualified_column());
                        }
                    }
                }
            }
        }

        Err(DataFusionError::SchemaError(SchemaError::FieldNotFound {
            field: Box::new(Column::new(self.relation.clone(), self.name)),
            valid_fields: schemas
                .iter()
                .flat_map(|s| s.fields().iter().map(|f| f.qualified_column()))
                .collect(),
        }))
    }
}

impl From<&str> for Column {
    fn from(c: &str) -> Self {
        Self::from_qualified_name(c)
    }
}

/// Create a column, cloning the string
impl From<&String> for Column {
    fn from(c: &String) -> Self {
        Self::from_qualified_name(c)
    }
}

/// Create a column, reusing the existing string
impl From<String> for Column {
    fn from(c: String) -> Self {
        Self::from_qualified_name(c)
    }
}

impl FromStr for Column {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.flat_name())
    }
}
