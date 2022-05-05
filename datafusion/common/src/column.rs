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
    /// Create Column from unqualified name.
    pub fn from_name(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
        }
    }

    /// Deserialize a fully qualified name string into a column
    pub fn from_qualified_name(flat_name: &str) -> Self {
        use sqlparser::tokenizer::Token;

        let dialect = sqlparser::dialect::GenericDialect {};
        let mut tokenizer = sqlparser::tokenizer::Tokenizer::new(&dialect, flat_name);
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
            name: String::from(flat_name),
        }
    }

    /// Serialize column into a flat name string
    pub fn flat_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}", r, self.name),
            None => self.name.clone(),
        }
    }

    // Internal implementation of normalize
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
            qualifier: self.relation.clone(),
            name: self.name,
            valid_fields: Some(
                schemas
                    .iter()
                    .flat_map(|s| s.fields().iter().map(|f| f.qualified_name()))
                    .collect(),
            ),
        }))
    }
}

impl From<&str> for Column {
    fn from(c: &str) -> Self {
        Self::from_qualified_name(c)
    }
}

impl FromStr for Column {
    type Err = Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.relation {
            Some(r) => write!(f, "#{}.{}", r, self.name),
            None => write!(f, "#{}", self.name),
        }
    }
}
