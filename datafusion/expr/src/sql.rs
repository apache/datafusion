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

//! Local copies of [`sqlparser::ast`] structures
//!
//! These types are used when the `sql` feature is disabled. When `sql` is
//! enabled, the upstream types from [`sqlparser`] are used instead.
//!
//! These definitions should be structurally compatible with the upstream
//! `sqlparser` types, so that code which switches between them via `cfg` keeps
//! compiling.
//!
//! See [#17332](https://github.com/apache/datafusion/pull/17332) for
//! more detail.

use crate::expr::display_comma_separated;
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct IlikeSelectItem {
    pub pattern: String,
}

impl Display for IlikeSelectItem {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ILIKE '{}'", &self.pattern)?;
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum ExcludeSelectItem {
    Single(ObjectName),
    Multiple(Vec<ObjectName>),
}

impl Display for ExcludeSelectItem {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "EXCLUDE")?;
        match self {
            Self::Single(column) => {
                write!(f, " {column}")?;
            }
            Self::Multiple(columns) => {
                write!(f, " ({})", display_comma_separated(columns))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct ObjectName(pub Vec<ObjectNamePart>);

impl Display for ObjectName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let parts: Vec<String> = self.0.iter().map(|p| format!("{p}")).collect();
        write!(f, "{}", parts.join("."))
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum ObjectNamePart {
    Identifier(Ident),
}

impl ObjectNamePart {
    pub fn as_ident(&self) -> Option<&Ident> {
        match self {
            ObjectNamePart::Identifier(ident) => Some(ident),
        }
    }
}

impl Display for ObjectNamePart {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ObjectNamePart::Identifier(ident) => write!(f, "{ident}"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct ExceptSelectItem {
    pub first_element: Ident,
    pub additional_elements: Vec<Ident>,
}

impl Display for ExceptSelectItem {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "EXCEPT ")?;
        if self.additional_elements.is_empty() {
            write!(f, "({})", self.first_element)?;
        } else {
            write!(
                f,
                "({}, {})",
                self.first_element,
                display_comma_separated(&self.additional_elements)
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum RenameSelectItem {
    Single(String),
    Multiple(Vec<String>),
}

impl Display for RenameSelectItem {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RENAME")?;
        match self {
            Self::Single(column) => {
                write!(f, " {column}")?;
            }
            Self::Multiple(columns) => {
                write!(f, " ({})", display_comma_separated(columns))?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: Option<char>,
    /// The span of the identifier in the original SQL string.
    pub span: String,
}

impl Display for Ident {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "[{}]", self.value)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct ReplaceSelectElement {
    pub expr: String,
    pub column_name: Ident,
    pub as_keyword: bool,
}

impl Display for ReplaceSelectElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.as_keyword {
            write!(f, "{} AS {}", self.expr, self.column_name)
        } else {
            write!(f, "{} {}", self.expr, self.column_name)
        }
    }
}
