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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{DFSchemaRef, TableReference};

use crate::{LogicalPlan, TableSource};

/// Operator that copies the contents of a database to file(s)
#[derive(Clone)]
pub struct CopyTo {
    /// The relation that determines the tuples to write to the output file(s)
    pub input: Arc<LogicalPlan>,
    /// The location to write the file(s)
    pub output_url: String,
    /// Determines which, if any, columns should be used for hive-style partitioned writes
    pub partition_by: Vec<String>,
    /// File type trait
    pub file_type: Arc<dyn FileType>,
    /// SQL Options that can affect the formats
    pub options: HashMap<String, String>,
    /// The schema of the output (a single column "count")
    pub output_schema: DFSchemaRef,
}

impl Debug for CopyTo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyTo")
            .field("input", &self.input)
            .field("output_url", &self.output_url)
            .field("partition_by", &self.partition_by)
            .field("file_type", &"...")
            .field("options", &self.options)
            .field("output_schema", &self.output_schema)
            .finish_non_exhaustive()
    }
}

// Implement PartialEq manually
impl PartialEq for CopyTo {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input && self.output_url == other.output_url
    }
}

// Implement Eq (no need for additional logic over PartialEq)
impl Eq for CopyTo {}

// Manual implementation needed because of `file_type` and `options` fields.
// Comparison excludes these field.
impl PartialOrd for CopyTo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => match self.output_url.partial_cmp(&other.output_url)
            {
                Some(Ordering::Equal) => {
                    self.partition_by.partial_cmp(&other.partition_by)
                }
                cmp => cmp,
            },
            cmp => cmp,
        }
    }
}

// Implement Hash manually
impl Hash for CopyTo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.output_url.hash(state);
    }
}

impl CopyTo {
    pub fn new(
        input: Arc<LogicalPlan>,
        output_url: String,
        partition_by: Vec<String>,
        file_type: Arc<dyn FileType>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            input,
            output_url,
            partition_by,
            file_type,
            options,
            // The output schema is always a single column "count" with the number of rows copied
            output_schema: make_count_schema(),
        }
    }
}

/// Modifies the content of a database
///
/// This operator is used to perform DML operations such as INSERT, DELETE,
/// UPDATE, and CTAS (CREATE TABLE AS SELECT).
///
/// * `INSERT` - Appends new rows to the existing table. Calls
///   [`TableProvider::insert_into`]
///
/// * `DELETE` - Removes rows from the table. Currently NOT supported by the
///   [`TableProvider`] trait or builtin sources.
///
/// * `UPDATE` - Modifies existing rows in the table. Currently NOT supported by
///   the [`TableProvider`] trait or builtin sources.
///
/// * `CREATE TABLE AS SELECT` - Creates a new table and populates it with data
///   from a query. This is similar to the `INSERT` operation, but it creates a new
///   table instead of modifying an existing one.
///
/// Note that the structure is adapted from substrait WriteRel)
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
/// [`TableProvider::insert_into`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#method.insert_into
#[derive(Clone)]
pub struct DmlStatement {
    /// The table name
    pub table_name: TableReference,
    /// this is target table to insert into
    pub target: Arc<dyn TableSource>,
    /// The type of operation to perform
    pub op: WriteOp,
    /// The relation that determines the tuples to add/remove/modify the schema must match with table_schema
    pub input: Arc<LogicalPlan>,
    /// The schema of the output relation
    pub output_schema: DFSchemaRef,
}
impl Eq for DmlStatement {}
impl Hash for DmlStatement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.target.schema().hash(state);
        self.op.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
    }
}

impl PartialEq for DmlStatement {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.target.schema() == other.target.schema()
            && self.op == other.op
            && self.input == other.input
            && self.output_schema == other.output_schema
    }
}

impl Debug for DmlStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmlStatement")
            .field("table_name", &self.table_name)
            .field("target", &"...")
            .field("target_schema", &self.target.schema())
            .field("op", &self.op)
            .field("input", &self.input)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl DmlStatement {
    /// Creates a new DML statement with the output schema set to a single `count` column.
    pub fn new(
        table_name: TableReference,
        target: Arc<dyn TableSource>,
        op: WriteOp,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            table_name,
            target,
            op,
            input,

            // The output schema is always a single column with the number of rows affected
            output_schema: make_count_schema(),
        }
    }

    /// Return a descriptive name of this [`DmlStatement`]
    pub fn name(&self) -> &str {
        self.op.name()
    }
}

// Manual implementation needed because of `table_schema` and `output_schema` fields.
// Comparison excludes these fields.
impl PartialOrd for DmlStatement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.table_name.partial_cmp(&other.table_name) {
            Some(Ordering::Equal) => match self.op.partial_cmp(&other.op) {
                Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
                cmp => cmp,
            },
            cmp => cmp,
        }
    }
}

/// The type of DML operation to perform.
///
/// See [`DmlStatement`] for more details.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum WriteOp {
    /// `INSERT INTO` operation
    Insert(InsertOp),
    /// `DELETE` operation
    Delete,
    /// `UPDATE` operation
    Update,
    /// `CREATE TABLE AS SELECT` operation
    Ctas,
}

impl WriteOp {
    /// Return a descriptive name of this [`WriteOp`]
    pub fn name(&self) -> &str {
        match self {
            WriteOp::Insert(insert) => insert.name(),
            WriteOp::Delete => "Delete",
            WriteOp::Update => "Update",
            WriteOp::Ctas => "Ctas",
        }
    }
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum InsertOp {
    /// Appends new rows to the existing table without modifying any
    /// existing rows. This corresponds to the SQL `INSERT INTO` query.
    Append,
    /// Overwrites all existing rows in the table with the new rows.
    /// This corresponds to the SQL `INSERT OVERWRITE` query.
    Overwrite,
    /// If any existing rows collides with the inserted rows (typically based
    /// on a unique key or primary key), those existing rows are replaced.
    /// This corresponds to the SQL `REPLACE INTO` query and its equivalents.
    Replace,
}

impl InsertOp {
    /// Return a descriptive name of this [`InsertOp`]
    pub fn name(&self) -> &str {
        match self {
            InsertOp::Append => "Insert Into",
            InsertOp::Overwrite => "Insert Overwrite",
            InsertOp::Replace => "Replace Into",
        }
    }
}

impl Display for InsertOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

fn make_count_schema() -> DFSchemaRef {
    Arc::new(
        Schema::new(vec![Field::new("count", DataType::UInt64, false)])
            .try_into()
            .unwrap(),
    )
}
