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

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaReference {
    Bare { schema: Arc<str> },
    Full { schema: Arc<str>, catalog: Arc<str> },
}

impl SchemaReference {
    /// Get only the schema name that this references.
    pub fn schema_name(&self) -> &str {
        match self {
            SchemaReference::Bare { schema } => schema,
            SchemaReference::Full { schema, catalog: _ } => schema,
        }
    }
}

impl std::fmt::Display for SchemaReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bare { schema } => write!(f, "{schema}"),
            Self::Full { schema, catalog } => write!(f, "{catalog}.{schema}"),
        }
    }
}
