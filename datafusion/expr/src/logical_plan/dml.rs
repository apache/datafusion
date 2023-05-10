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

use std::{
    collections::HashMap,
    fmt::{self, Display},
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion_common::{DFSchemaRef, OwnedTableReference, ScalarValue};

use crate::LogicalPlan;

/// The operator that modifies the content of a database (adapted from
/// substrait WriteRel)
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DmlStatement {
    /// The table name
    pub table_name: OwnedTableReference,
    /// The schema of the table (must align with Rel input)
    pub table_schema: DFSchemaRef,
    /// The type of operation to perform
    pub op: WriteOp,
    /// The relation that determines the tuples to add/remove/modify the schema must match with table_schema
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum WriteOp {
    Insert,
    Delete,
    Update,
    Ctas,
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteOp::Insert => write!(f, "Insert"),
            WriteOp::Delete => write!(f, "Delete"),
            WriteOp::Update => write!(f, "Update"),
            WriteOp::Ctas => write!(f, "Ctas"),
        }
    }
}

/// The operator that modifies the content of a database (adapted from
/// substrait WriteRel)
#[derive(Clone, PartialEq, Eq)]
pub struct CopyTo {
    /// Plan to read the data from
    pub input: Arc<LogicalPlan>,

    /// the URL to which the data is headed (e.g. 'foo.parquet' or 'foo')
    pub target: String,

    /// User supplied name/value pairs that are interpreted for each targe type
    pub options: HashMap<String, ScalarValue>,

    /// output schema (is empty)
    pub dummy_schema: DFSchemaRef,
}

// Hashing refers to a subset of fields considered in PartialEq. needed b/c HashMap is not Hash
#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for CopyTo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.target.hash(state);
        self.options.len().hash(state); // HashMap is not hashable
        self.dummy_schema.hash(state);
    }
}
