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

use arrow::{
    array::RecordBatch,
    datatypes::{Field, Fields, Schema},
};
use datafusion_common::Result;
use datafusion_datasource::{metadata::MetadataColumn, PartitionedFile};
use datafusion_expr::{execution_props::ExecutionProps, Expr};

use crate::helpers::apply_filters;

/// Determine if the given file matches the input metadata filters.
/// `filters` should only contain expressions that can be evaluated
/// using only the metadata columns.
pub fn apply_metadata_filters(
    file: PartitionedFile,
    filters: &[Expr],
    metadata_cols: &[MetadataColumn],
) -> Result<Option<PartitionedFile>> {
    // if no metadata col => simply return all the files
    if metadata_cols.is_empty() {
        return Ok(Some(file));
    }

    let mut builders: Vec<_> = metadata_cols.iter().map(|col| col.builder(1)).collect();

    for builder in builders.iter_mut() {
        builder.append(&file.object_meta);
    }

    let arrays = builders
        .into_iter()
        .map(|builder| builder.finish())
        .collect::<Vec<_>>();

    let fields: Fields = metadata_cols
        .iter()
        .map(|col| Field::new(col.to_string(), col.arrow_type(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(schema, arrays)?;

    let props = ExecutionProps::new();

    // Don't retain rows that evaluated to null
    let prepared = apply_filters(&batch, filters, &props)?;

    // If the filter evaluates to true, return the file
    if prepared.true_count() == 1 {
        return Ok(Some(file));
    }

    Ok(None)
}
