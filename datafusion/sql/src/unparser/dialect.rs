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

/// Dialect is used to capture dialect specific syntax.
/// Note: this trait will eventually be replaced by the Dialect in the SQLparser package
///
/// See <https://github.com/sqlparser-rs/sqlparser-rs/pull/1170>
pub trait Dialect {
    fn identifier_quote_style(&self) -> Option<char>;
}
pub struct DefaultDialect {}

impl Dialect for DefaultDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('"')
    }
}

pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('"')
    }
}

pub struct MySqlDialect {}

impl Dialect for MySqlDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('`')
    }
}

pub struct SqliteDialect {}

impl Dialect for SqliteDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        Some('`')
    }
}

pub struct CustomDialect {
    identifier_quote_style: Option<char>,
}

impl CustomDialect {
    pub fn new(identifier_quote_style: Option<char>) -> Self {
        Self {
            identifier_quote_style,
        }
    }
}

impl Dialect for CustomDialect {
    fn identifier_quote_style(&self) -> Option<char> {
        self.identifier_quote_style
    }
}
