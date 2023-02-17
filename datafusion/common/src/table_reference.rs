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

use crate::error::Result;
use sqlparser::{
    ast::Ident,
    dialect::GenericDialect,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithLocation},
};
use std::borrow::Cow;

/// A resolved path to a table of the form "catalog.schema.table"
#[derive(Debug, Clone)]
pub struct ResolvedTableReference<'a> {
    /// The catalog (aka database) containing the table
    pub catalog: Cow<'a, str>,
    /// The schema containing the table
    pub schema: Cow<'a, str>,
    /// The table name
    pub table: Cow<'a, str>,
}

impl<'a> std::fmt::Display for ResolvedTableReference<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Represents a path to a table that may require further resolution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableReference<'a> {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: Cow<'a, str>,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: Cow<'a, str>,
        /// The table name
        table: Cow<'a, str>,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: Cow<'a, str>,
        /// The schema containing the table
        schema: Cow<'a, str>,
        /// The table name
        table: Cow<'a, str>,
    },
}

/// Represents a path to a table that may require further resolution
/// that owns the underlying names
#[derive(Debug, Clone)]
pub enum OwnedTableReference {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: String,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: String,
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
}

impl OwnedTableReference {
    /// Return a `TableReference` view of this `OwnedTableReference`
    pub fn as_table_reference(&self) -> TableReference<'_> {
        match self {
            Self::Bare { table } => TableReference::Bare {
                table: table.into(),
            },
            Self::Partial { schema, table } => TableReference::Partial {
                schema: schema.into(),
                table: table.into(),
            },
            Self::Full {
                catalog,
                schema,
                table,
            } => TableReference::Full {
                catalog: catalog.into(),
                schema: schema.into(),
                table: table.into(),
            },
        }
    }

    /// Retrieve the actual table name, regardless of qualification
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. }
            | Self::Partial { table, .. }
            | Self::Bare { table } => table,
        }
    }
}

impl std::fmt::Display for OwnedTableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedTableReference::Bare { table } => write!(f, "{table}"),
            OwnedTableReference::Partial { schema, table } => {
                write!(f, "{schema}.{table}")
            }
            OwnedTableReference::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}

/// Convert `OwnedTableReference` into a `TableReference`. Somewhat
/// awkward to use but 'idiomatic': `(&table_ref).into()`
impl<'a> From<&'a OwnedTableReference> for TableReference<'a> {
    fn from(r: &'a OwnedTableReference) -> Self {
        r.as_table_reference()
    }
}

impl<'a> TableReference<'a> {
    /// Retrieve the actual table name, regardless of qualification
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. }
            | Self::Partial { table, .. }
            | Self::Bare { table } => table,
        }
    }

    /// Given a default catalog and schema, ensure this table reference is fully resolved
    pub fn resolve(
        self,
        default_catalog: &'a str,
        default_schema: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableReference {
                catalog,
                schema,
                table,
            },
            Self::Partial { schema, table } => ResolvedTableReference {
                catalog: default_catalog.into(),
                schema,
                table,
            },
            Self::Bare { table } => ResolvedTableReference {
                catalog: default_catalog.into(),
                schema: default_schema.into(),
                table,
            },
        }
    }

    /// Forms a [`TableReference`] by attempting to parse `s` as a multipart identifier,
    /// failing that then taking the entire unnormalized input as the identifier itself.
    ///
    /// Will normalize (convert to lowercase) any unquoted identifiers.
    ///
    /// e.g. `Foo` will be parsed as `foo`, and `"Foo"".bar"` will be parsed as
    /// `Foo".bar` (note the preserved case and requiring two double quotes to represent
    /// a single double quote in the identifier)
    pub fn parse_str(s: &'a str) -> Self {
        let mut parts = parse_identifiers(s)
            .unwrap_or_default()
            .into_iter()
            .map(|id| match id.quote_style {
                Some(_) => id.value,
                None => id.value.to_ascii_lowercase(),
            })
            .collect::<Vec<_>>();

        match parts.len() {
            1 => Self::Bare {
                table: parts.remove(0).into(),
            },
            2 => Self::Partial {
                schema: parts.remove(0).into(),
                table: parts.remove(0).into(),
            },
            3 => Self::Full {
                catalog: parts.remove(0).into(),
                schema: parts.remove(0).into(),
                table: parts.remove(0).into(),
            },
            _ => Self::Bare { table: s.into() },
        }
    }
}

// TODO: remove when can use https://github.com/sqlparser-rs/sqlparser-rs/issues/805
fn parse_identifiers(s: &str) -> Result<Vec<Ident>> {
    let dialect = GenericDialect;
    let mut parser = Parser::new(&dialect).try_with_sql(s)?;
    let mut idents = vec![];

    // expecting at least one word for identifier
    match parser.next_token_no_skip() {
        Some(TokenWithLocation {
            token: Token::Word(w),
            ..
        }) => idents.push(w.to_ident()),
        Some(TokenWithLocation { token, .. }) => {
            return Err(ParserError::ParserError(format!(
                "Unexpected token in identifier: {token}"
            )))?
        }
        None => {
            return Err(ParserError::ParserError(
                "Empty input when parsing identifier".to_string(),
            ))?
        }
    };

    while let Some(TokenWithLocation { token, .. }) = parser.next_token_no_skip() {
        match token {
            // ensure that optional period is succeeded by another identifier
            Token::Period => match parser.next_token_no_skip() {
                Some(TokenWithLocation {
                    token: Token::Word(w),
                    ..
                }) => idents.push(w.to_ident()),
                Some(TokenWithLocation { token, .. }) => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token following period in identifier: {token}"
                    )))?
                }
                None => {
                    return Err(ParserError::ParserError(
                        "Trailing period in identifier".to_string(),
                    ))?
                }
            },
            _ => {
                return Err(ParserError::ParserError(format!(
                    "Unexpected token in identifier: {token}"
                )))?
            }
        }
    }
    Ok(idents)
}

/// Parse a string into a TableReference, normalizing where appropriate
///
/// See full details on [`TableReference::parse_str`]
impl<'a> From<&'a str> for TableReference<'a> {
    fn from(s: &'a str) -> Self {
        Self::parse_str(s)
    }
}

impl<'a> From<ResolvedTableReference<'a>> for TableReference<'a> {
    fn from(resolved: ResolvedTableReference<'a>) -> Self {
        Self::Full {
            catalog: resolved.catalog,
            schema: resolved.schema,
            table: resolved.table,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_identifiers() -> Result<()> {
        let s = "CATALOG.\"F(o)o. \"\"bar\".table";
        let actual = parse_identifiers(s)?;
        let expected = vec![
            Ident {
                value: "CATALOG".to_string(),
                quote_style: None,
            },
            Ident {
                value: "F(o)o. \"bar".to_string(),
                quote_style: Some('"'),
            },
            Ident {
                value: "table".to_string(),
                quote_style: None,
            },
        ];
        assert_eq!(expected, actual);

        let s = "";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Empty input when parsing identifier\"))",
            format!("{err:?}")
        );

        let s = "*schema.table";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Unexpected token in identifier: *\"))",
            format!("{err:?}")
        );

        let s = "schema.table*";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Unexpected token in identifier: *\"))",
            format!("{err:?}")
        );

        let s = "schema.table.";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Trailing period in identifier\"))",
            format!("{err:?}")
        );

        let s = "schema.*";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Unexpected token following period in identifier: *\"))",
            format!("{err:?}")
        );

        Ok(())
    }

    #[test]
    fn test_table_reference_from_str_normalizes() {
        let expected = TableReference::Full {
            catalog: Cow::Owned("catalog".to_string()),
            schema: Cow::Owned("FOO\".bar".to_string()),
            table: Cow::Owned("table".to_string()),
        };
        let actual = TableReference::from("catalog.\"FOO\"\".bar\".TABLE");
        assert_eq!(expected, actual);

        let expected = TableReference::Partial {
            schema: Cow::Owned("FOO\".bar".to_string()),
            table: Cow::Owned("table".to_string()),
        };
        let actual = TableReference::from("\"FOO\"\".bar\".TABLE");
        assert_eq!(expected, actual);

        let expected = TableReference::Bare {
            table: Cow::Owned("table".to_string()),
        };
        let actual = TableReference::from("TABLE");
        assert_eq!(expected, actual);

        // if fail to parse, take entire input string as identifier
        let expected = TableReference::Bare {
            table: Cow::Owned("TABLE()".to_string()),
        };
        let actual = TableReference::from("TABLE()");
        assert_eq!(expected, actual);
    }
}
