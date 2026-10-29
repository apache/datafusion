// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
