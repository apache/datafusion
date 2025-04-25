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

use crate::error::{_schema_err, add_possible_columns_to_diag};
use crate::utils::{parse_identifiers_normalized, quote_identifier};
use crate::{DFSchema, Diagnostic, Result, SchemaError, Spans, TableReference};
use arrow::datatypes::{Field, FieldRef};
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

/// A named reference to a qualified field in a schema.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    /// relation/table reference.
    pub relation: Option<TableReference>,
    /// field/column name.
    pub name: String,
    /// Original source code location, if known
    pub spans: Spans,
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Column")
            .field("relation", &self.relation)
            .field("name", &self.name)
            .finish()
    }
}

impl Column {
    /// Create Column from optional qualifier and name. The optional qualifier, if present,
    /// will be parsed and normalized by default.
    ///
    /// See full details on [`TableReference::parse_str`]
    ///
    /// [`TableReference::parse_str`]: crate::TableReference::parse_str
    pub fn new(
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            relation: relation.map(|r| r.into()),
            name: name.into(),
            spans: Spans::new(),
        }
    }

    /// Convenience method for when there is no qualifier
    pub fn new_unqualified(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
            spans: Spans::new(),
        }
    }

    /// Create Column from unqualified name.
    ///
    /// Alias for `Column::new_unqualified`
    pub fn from_name(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
            spans: Spans::new(),
        }
    }

    /// Create a Column from multiple normalized identifiers
    ///
    /// For example, `foo.bar` would be represented as a two element vector
    /// `["foo", "bar"]`
    fn from_idents(mut idents: Vec<String>) -> Option<Self> {
        let (relation, name) = match idents.len() {
            1 => (None, idents.remove(0)),
            2 => (
                Some(TableReference::Bare {
                    table: idents.remove(0).into(),
                }),
                idents.remove(0),
            ),
            3 => (
                Some(TableReference::Partial {
                    schema: idents.remove(0).into(),
                    table: idents.remove(0).into(),
                }),
                idents.remove(0),
            ),
            4 => (
                Some(TableReference::Full {
                    catalog: idents.remove(0).into(),
                    schema: idents.remove(0).into(),
                    table: idents.remove(0).into(),
                }),
                idents.remove(0),
            ),
            // any expression that failed to parse or has more than 4 period delimited
            // identifiers will be treated as an unqualified column name
            _ => return None,
        };
        Some(Self {
            relation,
            name,
            spans: Spans::new(),
        })
    }

    /// Deserialize a fully qualified name string into a column
    ///
    /// Treats the name as a SQL identifier. For example
    /// `foo.BAR` would be parsed to a reference to relation `foo`, column name `bar` (lower case)
    /// where `"foo.BAR"` would be parsed to a reference to column named `foo.BAR`
    pub fn from_qualified_name(flat_name: impl Into<String>) -> Self {
        let flat_name = flat_name.into();
        Self::from_idents(parse_identifiers_normalized(&flat_name, false)).unwrap_or(
            Self {
                relation: None,
                name: flat_name,
                spans: Spans::new(),
            },
        )
    }

    /// Deserialize a fully qualified name string into a column preserving column text case
    pub fn from_qualified_name_ignore_case(flat_name: impl Into<String>) -> Self {
        let flat_name = flat_name.into();
        Self::from_idents(parse_identifiers_normalized(&flat_name, true)).unwrap_or(
            Self {
                relation: None,
                name: flat_name,
                spans: Spans::new(),
            },
        )
    }

    /// return the column's name.
    ///
    /// Note: This ignores the relation and returns the column name only.
    pub fn name(&self) -> &str {
        &self.name
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
                format!(
                    "{}.{}",
                    r.to_quoted_string(),
                    quote_identifier(self.name.as_str())
                )
            }
            None => quote_identifier(&self.name).to_string(),
        }
    }

    /// Qualify column if not done yet.
    ///
    /// If this column already has a [relation](Self::relation), it will be returned as is and the given parameters are
    /// ignored. Otherwise this will search through the given schemas to find the column.
    ///
    /// Will check for ambiguity at each level of `schemas`.
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
    ///
    /// Regarding ambiguity check, `schemas` is structured to allow levels of schemas to be passed in.
    /// For example:
    ///
    /// ```text
    /// schemas = &[
    ///    &[schema1, schema2], // first level
    ///    &[schema3, schema4], // second level
    /// ]
    /// ```
    ///
    /// Will search for a matching field in all schemas in the first level. If a matching field according to above
    /// mentioned conditions is not found, then will check the next level. If found more than one matching column across
    /// all schemas in a level, that isn't a USING column, will return an error due to ambiguous column.
    ///
    /// If checked all levels and couldn't find field, will return field not found error.
    pub fn normalize_with_schemas_and_ambiguity_check(
        self,
        schemas: &[&[&DFSchema]],
        using_columns: &[HashSet<Column>],
    ) -> Result<Self> {
        if self.relation.is_some() {
            return Ok(self);
        }

        for schema_level in schemas {
            let qualified_fields = schema_level
                .iter()
                .flat_map(|s| s.qualified_fields_with_unqualified_name(&self.name))
                .collect::<Vec<_>>();
            match qualified_fields.len() {
                0 => continue,
                1 => return Ok(Column::from(qualified_fields[0])),
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
                    let columns = schema_level
                        .iter()
                        .flat_map(|s| s.columns_with_unqualified_name(&self.name))
                        .collect::<Vec<_>>();
                    for using_col in using_columns {
                        let all_matched = columns.iter().all(|c| using_col.contains(c));
                        // All matched fields belong to the same using column set, in other words
                        // the same join clause. We simply pick the qualifier from the first match.
                        if all_matched {
                            return Ok(columns[0].clone());
                        }
                    }

                    // If not due to USING columns then due to ambiguous column name
                    return _schema_err!(SchemaError::AmbiguousReference {
                        field: Column::new_unqualified(&self.name),
                    })
                    .map_err(|err| {
                        let mut diagnostic = Diagnostic::new_error(
                            format!("column '{}' is ambiguous", &self.name),
                            self.spans().first(),
                        );
                        // TODO If [`DFSchema`] had spans, we could show the
                        // user which columns are candidates, or which table
                        // they come from. For now, let's list the table names
                        // only.
                        add_possible_columns_to_diag(
                            &mut diagnostic,
                            &Column::new_unqualified(&self.name),
                            &columns,
                        );
                        err.with_diagnostic(diagnostic)
                    });
                }
            }
        }

        _schema_err!(SchemaError::FieldNotFound {
            field: Box::new(self),
            valid_fields: schemas
                .iter()
                .flat_map(|s| s.iter())
                .flat_map(|s| s.columns())
                .collect(),
        })
    }

    /// Returns a reference to the set of locations in the SQL query where this
    /// column appears, if known.
    pub fn spans(&self) -> &Spans {
        &self.spans
    }

    /// Returns a mutable reference to the set of locations in the SQL query
    /// where this column appears, if known.
    pub fn spans_mut(&mut self) -> &mut Spans {
        &mut self.spans
    }

    /// Replaces the set of locations in the SQL query where this column
    /// appears, if known.
    pub fn with_spans(mut self, spans: Spans) -> Self {
        self.spans = spans;
        self
    }

    /// Qualifies the column with the given table reference.
    pub fn with_relation(&self, relation: TableReference) -> Self {
        Self {
            relation: Some(relation),
            ..self.clone()
        }
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

/// Create a column, use qualifier and field name
impl From<(Option<&TableReference>, &Field)> for Column {
    fn from((relation, field): (Option<&TableReference>, &Field)) -> Self {
        Self::new(relation.cloned(), field.name())
    }
}

/// Create a column, use qualifier and field name
impl From<(Option<&TableReference>, &FieldRef)> for Column {
    fn from((relation, field): (Option<&TableReference>, &FieldRef)) -> Self {
        Self::new(relation.cloned(), field.name())
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, SchemaBuilder};
    use std::sync::Arc;

    fn create_qualified_schema(qualifier: &str, names: Vec<&str>) -> Result<DFSchema> {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.extend(
            names
                .iter()
                .map(|f| Field::new(*f, DataType::Boolean, true)),
        );
        let schema = Arc::new(schema_builder.finish());
        DFSchema::try_from_qualified_schema(qualifier, &schema)
    }

    #[test]
    fn test_normalize_with_schemas_and_ambiguity_check() -> Result<()> {
        let schema1 = create_qualified_schema("t1", vec!["a", "b"])?;
        let schema2 = create_qualified_schema("t2", vec!["c", "d"])?;
        let schema3 = create_qualified_schema("t3", vec!["a", "b", "c", "d", "e"])?;

        // already normalized
        let col = Column::new(Some("t1"), "a");
        let col = col.normalize_with_schemas_and_ambiguity_check(&[], &[])?;
        assert_eq!(col, Column::new(Some("t1"), "a"));

        // should find in first level (schema1)
        let col = Column::from_name("a");
        let col = col.normalize_with_schemas_and_ambiguity_check(
            &[&[&schema1, &schema2], &[&schema3]],
            &[],
        )?;
        assert_eq!(col, Column::new(Some("t1"), "a"));

        // should find in second level (schema3)
        let col = Column::from_name("e");
        let col = col.normalize_with_schemas_and_ambiguity_check(
            &[&[&schema1, &schema2], &[&schema3]],
            &[],
        )?;
        assert_eq!(col, Column::new(Some("t3"), "e"));

        // using column in first level (pick schema1)
        let mut using_columns = HashSet::new();
        using_columns.insert(Column::new(Some("t1"), "a"));
        using_columns.insert(Column::new(Some("t3"), "a"));
        let col = Column::from_name("a");
        let col = col.normalize_with_schemas_and_ambiguity_check(
            &[&[&schema1, &schema3], &[&schema2]],
            &[using_columns],
        )?;
        assert_eq!(col, Column::new(Some("t1"), "a"));

        // not found in any level
        let col = Column::from_name("z");
        let err = col
            .normalize_with_schemas_and_ambiguity_check(
                &[&[&schema1, &schema2], &[&schema3]],
                &[],
            )
            .expect_err("should've failed to find field");
        let expected = "Schema error: No field named z. \
            Valid fields are t1.a, t1.b, t2.c, t2.d, t3.a, t3.b, t3.c, t3.d, t3.e.";
        assert_eq!(err.strip_backtrace(), expected);

        // ambiguous column reference
        let col = Column::from_name("a");
        let err = col
            .normalize_with_schemas_and_ambiguity_check(
                &[&[&schema1, &schema3], &[&schema2]],
                &[],
            )
            .expect_err("should've found ambiguous field");
        let expected = "Schema error: Ambiguous reference to unqualified field a";
        assert_eq!(err.strip_backtrace(), expected);

        Ok(())
    }
}
