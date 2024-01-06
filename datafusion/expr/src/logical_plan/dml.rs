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
    fmt::{self, Display},
    sync::Arc,
};

use datafusion_common::{
    file_options::StatementOptions, DFSchemaRef, FileType,
    OwnedTableReference,
};

use crate::LogicalPlan;

/// Operator that copies the contents of a database to file(s)
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CopyTo {
    /// The relation that determines the tuples to write to the output file(s)
    pub input: Arc<LogicalPlan>,
    /// The location to write the file(s)
    pub output_url: String,
    /// If false, it is assumed output_url is a file to which all data should be written
    /// regardless of input partitioning. Otherwise, output_url is assumed to be a directory
    /// to which each output partition is written to its own output file
    pub single_file_output: bool,
    /// Arbitrary options as tuples
    pub copy_options: StatementOptions,
}


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

impl DmlStatement {
    /// Return a descriptive name of this [`DmlStatement`]
    pub fn name(&self) -> &str {
        self.op.name()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum WriteOp {
    InsertOverwrite,
    InsertInto,
    Delete,
    Update,
    Ctas,
}

impl WriteOp {
    /// Return a descriptive name of this [`WriteOp`]
    pub fn name(&self) -> &str {
        match self {
            WriteOp::InsertOverwrite => "Insert Overwrite",
            WriteOp::InsertInto => "Insert Into",
            WriteOp::Delete => "Delete",
            WriteOp::Update => "Update",
            WriteOp::Ctas => "Ctas",
        }
    }
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
