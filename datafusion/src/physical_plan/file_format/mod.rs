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

//! Execution plans that read file formats

mod avro;
mod csv;
mod file_stream;
mod json;
mod parquet;

pub use self::parquet::ParquetExec;
use arrow::datatypes::{Schema, SchemaRef};
pub use avro::AvroExec;
pub use csv::CsvExec;
pub use json::NdJsonExec;

use crate::datasource::PartitionedFile;
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};

use super::Statistics;

/// A wrapper to customize partitioned file display
#[derive(Debug)]
struct FileGroupsDisplay<'a>(&'a [Vec<PartitionedFile>]);

impl<'a> Display for FileGroupsDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let parts: Vec<_> = self
            .0
            .iter()
            .map(|pp| {
                pp.iter()
                    .map(|pf| pf.file_meta.path())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .collect();
        write!(f, "[{}]", parts.join(", "))
    }
}

/// Project the schema and the statistics on the given column indices
fn project(
    projection: &Option<Vec<usize>>,
    schema: SchemaRef,
    statistics: Statistics,
) -> (SchemaRef, Statistics) {
    let projection = if projection.is_none() {
        return (schema, statistics);
    } else {
        projection.as_ref().unwrap()
    };
    let projected_schema = Schema::new(
        projection
            .iter()
            .map(|i| schema.field(*i).clone())
            .collect(),
    );

    let new_column_statistics = statistics.column_statistics.map(|stats| {
        let mut projected_stats = Vec::with_capacity(projection.len());
        for proj in projection {
            projected_stats.push(stats[*proj].clone());
        }
        projected_stats
    });

    let statistics = Statistics {
        num_rows: statistics.num_rows,
        total_byte_size: statistics.total_byte_size,
        column_statistics: new_column_statistics,
        is_exact: statistics.is_exact,
    };

    (Arc::new(projected_schema), statistics)
}
