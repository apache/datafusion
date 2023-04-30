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

use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaReference<'a> {
    Bare {
        schema: Cow<'a, str>,
    },
    Full {
        schema: Cow<'a, str>,
        catalog: Cow<'a, str>,
    },
}

impl SchemaReference<'_> {
    /// Get only the schema name that this references.
    pub fn schema_name(&self) -> &str {
        match self {
            SchemaReference::Bare { schema } => schema,
            SchemaReference::Full { schema, catalog: _ } => schema,
        }
    }
}

pub type OwnedSchemaReference = SchemaReference<'static>;

impl std::fmt::Display for SchemaReference<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bare { schema } => write!(f, "{schema}"),
            Self::Full { schema, catalog } => write!(f, "{catalog}.{schema}"),
        }
    }
}

impl<'a> From<&'a OwnedSchemaReference> for SchemaReference<'a> {
    fn from(value: &'a OwnedSchemaReference) -> Self {
        match value {
            SchemaReference::Bare { schema } => SchemaReference::Bare {
                schema: Cow::Borrowed(schema),
            },
            SchemaReference::Full { schema, catalog } => SchemaReference::Full {
                schema: Cow::Borrowed(schema),
                catalog: Cow::Borrowed(catalog),
            },
        }
    }
}
