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

use crate::{DFSchema, DataFusionError, Result, SchemaError};
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    /// relation/table name.
    pub relation: Option<String>,
    /// field/column name.
    pub name: String,
}

impl Column {
    /// Create Column from optional qualifier and name
    pub fn new(relation: Option<impl Into<String>>, name: impl Into<String>) -> Self {
        Self {
            relation: relation.map(|r| r.into()),
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
        use sqlparser::tokenizer::Token;

        let dialect = sqlparser::dialect::GenericDialect {};
        let mut tokenizer = sqlparser::tokenizer::Tokenizer::new(&dialect, &flat_name);
        if let Ok(tokens) = tokenizer.tokenize() {
            if let [Token::Word(relation), Token::Period, Token::Word(name)] =
                tokens.as_slice()
            {
                return Column {
                    relation: Some(relation.value.clone()),
                    name: name.value.clone(),
                };
            }
        }
        // any expression that's not in the form of `foo.bar` will be treated as unqualified column
        // name
        Column {
            relation: None,
            name: flat_name,
        }
    }

    /// Serialize column into a flat name string
    pub fn flat_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}", r, self.name),
            None => self.name.clone(),
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
    #[deprecated(
        since = "20.0.0",
        note = "use normalize_with_schemas_and_ambiguity_check instead"
    )]
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
            field: Column::new(self.relation.clone(), self.name),
            valid_fields: schemas
                .iter()
                .flat_map(|s| s.fields().iter().map(|f| f.qualified_column()))
                .collect(),
        }))
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
            let fields = schema_level
                .iter()
                .flat_map(|s| s.fields_with_unqualified_name(&self.name))
                .collect::<Vec<_>>();
            match fields.len() {
                0 => continue,
                1 => return Ok(fields[0].qualified_column()),
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

                    // If not due to USING columns then due to ambiguous column name
                    return Err(DataFusionError::SchemaError(
                        SchemaError::AmbiguousReference {
                            qualifier: None,
                            name: self.name,
                        },
                    ));
                }
            }
        }

        Err(DataFusionError::SchemaError(SchemaError::FieldNotFound {
            field: self,
            valid_fields: schemas
                .iter()
                .flat_map(|s| s.iter())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DFField;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn create_schema(names: &[(Option<&str>, &str)]) -> Result<DFSchema> {
        let fields = names
            .iter()
            .map(|(qualifier, name)| {
                DFField::new(qualifier.to_owned(), name, DataType::Boolean, true)
            })
            .collect::<Vec<_>>();
        DFSchema::new_with_metadata(fields, HashMap::new())
    }

    #[test]
    fn test_normalize_with_schemas_and_ambiguity_check() -> Result<()> {
        let schema1 = create_schema(&[(Some("t1"), "a"), (Some("t1"), "b")])?;
        let schema2 = create_schema(&[(Some("t2"), "c"), (Some("t2"), "d")])?;
        let schema3 = create_schema(&[
            (Some("t3"), "a"),
            (Some("t3"), "b"),
            (Some("t3"), "c"),
            (Some("t3"), "d"),
            (Some("t3"), "e"),
        ])?;

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
        let expected = "Schema error: No field named 'z'. \
        Valid fields are 't1'.'a', 't1'.'b', 't2'.'c', \
        't2'.'d', 't3'.'a', 't3'.'b', 't3'.'c', 't3'.'d', 't3'.'e'.";
        assert_eq!(err.to_string(), expected);

        // ambiguous column reference
        let col = Column::from_name("a");
        let err = col
            .normalize_with_schemas_and_ambiguity_check(
                &[&[&schema1, &schema3], &[&schema2]],
                &[],
            )
            .expect_err("should've found ambiguous field");
        let expected = "Schema error: Ambiguous reference to unqualified field 'a'";
        assert_eq!(err.to_string(), expected);

        Ok(())
    }
}
