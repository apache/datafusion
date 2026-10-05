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

/// Indicates the type of this table for metadata/catalog purposes.
///
/// Lives in `datafusion-common` (and is re-exported as `datafusion_expr::TableType`)
/// rather than in `datafusion-expr`, so that lower-level crates that cannot
/// depend on `datafusion-expr` — such as the shared `information_schema`
/// builders in [`crate::information_schema`] — can still use it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialized table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

impl std::fmt::Display for TableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableType::Base => write!(f, "Base"),
            TableType::View => write!(f, "View"),
            TableType::Temporary => write!(f, "Temporary"),
        }
    }
}
