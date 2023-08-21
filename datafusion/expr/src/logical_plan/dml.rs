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
    str::FromStr,
    sync::Arc,
};

use datafusion_common::{
    not_impl_err, DFSchemaRef, DataFusionError, OwnedTableReference,
};

use crate::LogicalPlan;

/// Operator that copies the contents of a database to file(s)
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CopyTo {
    /// The relation that determines the tuples to write to the output file(s)
    pub input: Arc<LogicalPlan>,
    /// The location to write the file(s)
    pub output_url: String,
    /// The file format to output (explicitly defined or inferred from file extension)
    pub file_format: OutputFileFormat,
    /// If false, it is assumed output_url is a file to which all data should be written
    /// regardless of input partitioning. Otherwise, output_url is assumed to be a directory
    /// to which each output partition is written to its own output file
    pub per_thread_output: bool,
    /// Arbitrary options as tuples
    pub options: Vec<(String, String)>,
}

/// The file formats that CopyTo can output
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum OutputFileFormat {
    CSV,
    JSON,
    PARQUET,
    AVRO,
    ARROW,
}

impl FromStr for OutputFileFormat {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, DataFusionError> {
        match s {
            "csv" => Ok(OutputFileFormat::CSV),
            "json" => Ok(OutputFileFormat::JSON),
            "parquet" => Ok(OutputFileFormat::PARQUET),
            "avro" => Ok(OutputFileFormat::AVRO),
            "arrow" => Ok(OutputFileFormat::ARROW),
            _ => not_impl_err!("Unknown or not supported file format {s}!"),
        }
    }
}

impl Display for OutputFileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = match self {
            OutputFileFormat::CSV => "csv",
            OutputFileFormat::JSON => "json",
            OutputFileFormat::PARQUET => "parquet",
            OutputFileFormat::AVRO => "avro",
            OutputFileFormat::ARROW => "arrow",
        };
        write!(f, "{}", out)
    }
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum WriteOp {
    InsertOverwrite,
    InsertInto,
    Delete,
    Update,
    Ctas,
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteOp::InsertOverwrite => write!(f, "Insert Overwrite"),
            WriteOp::InsertInto => write!(f, "Insert Into"),
            WriteOp::Delete => write!(f, "Delete"),
            WriteOp::Update => write!(f, "Update"),
            WriteOp::Ctas => write!(f, "Ctas"),
        }
    }
}
