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

//! Internal metadata for hash aggregate repartition subpartitions.
//!
//! This module does NOT partition or aggregate rows. It only attaches and reads
//! the relative aggregate partition assigned by [`RepartitionExec`].
//!
//! Entry points: [`with_relative_partition`] and [`relative_partition`].
//!
//! [`RepartitionExec`]: crate::repartition::RepartitionExec

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::cast::as_uint32_array;
use datafusion_common::{DataFusionError, Result, internal_err};

const AGGR_RELATIVE_PARTITION_METADATA_KEY: &str =
    "datafusion.internal.hash_aggr_relative_partition";
const AGGR_SUBPARTITION_COLUMN_NAME: &str =
    "__datafusion_internal_hash_aggr_subpartition";

/// A contiguous run of rows for one relative aggregate partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionRun {
    /// Relative aggregate partition inside one output partition.
    pub relative_partition: usize,
    /// Number of contiguous rows in this run.
    pub len: usize,
}

impl PartitionRun {
    /// Create a non-empty partition run.
    pub(crate) fn new(relative_partition: usize, len: usize) -> Result<Self> {
        if len == 0 {
            return internal_err!("Hash aggregate partition run length must be nonzero");
        }
        Ok(Self {
            relative_partition,
            len,
        })
    }
}

/// Return a schema with the internal subpartition column appended.
fn subpartition_schema(schema: &SchemaRef) -> SchemaRef {
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        AGGR_SUBPARTITION_COLUMN_NAME,
        DataType::UInt32,
        false,
    )));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Append an internal subpartition column using `runs`.
pub(crate) fn append_subpartition_column(
    batch: &RecordBatch,
    runs: &[PartitionRun],
) -> Result<RecordBatch> {
    let mut values = Vec::with_capacity(batch.num_rows());
    for run in runs {
        let relative_partition =
            u32::try_from(run.relative_partition).map_err(|err| {
                DataFusionError::Internal(format!(
                    "Hash aggregate relative partition does not fit UInt32: {err}"
                ))
            })?;
        values.extend(std::iter::repeat_n(relative_partition, run.len));
    }

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt32Array::from(values)) as ArrayRef);
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(
        subpartition_schema(batch.schema_ref()),
        columns,
        &options,
    )
    .map_err(Into::into)
}

/// Strip the internal subpartition column and return its contiguous runs.
pub(crate) fn partition_runs_from_column(
    batch: RecordBatch,
) -> Result<(RecordBatch, Vec<PartitionRun>)> {
    let subpartition_idx = batch.num_columns().checked_sub(1).ok_or_else(|| {
        DataFusionError::Internal(
            "Missing relative partition column for partitioned batch coalescing"
                .to_string(),
        )
    })?;
    if batch.schema().field(subpartition_idx).name() != AGGR_SUBPARTITION_COLUMN_NAME {
        return internal_err!(
            "Missing relative partition column for partitioned batch coalescing"
        );
    }
    let subpartitions = as_uint32_array(batch.column(subpartition_idx).as_ref())?;
    if subpartitions.null_count() != 0 {
        return internal_err!(
            "Hash aggregate subpartition column must not contain nulls"
        );
    }

    let mut runs = Vec::new();
    let mut values = subpartitions.values().iter().copied();
    if let Some(first_partition) = values.next() {
        let mut current_partition = first_partition as usize;
        let mut current_len = 1;
        for relative_partition in values {
            let relative_partition = relative_partition as usize;
            if relative_partition == current_partition {
                current_len += 1;
            } else {
                runs.push(PartitionRun::new(current_partition, current_len)?);
                current_partition = relative_partition;
                current_len = 1;
            }
        }
        runs.push(PartitionRun::new(current_partition, current_len)?);
    }

    let (source_schema, mut columns, num_rows) = batch.into_parts();
    let fields = source_schema
        .fields()
        .iter()
        .take(subpartition_idx)
        .cloned()
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    columns.pop();
    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    let stripped_batch = RecordBatch::try_new_with_options(schema, columns, &options)?;
    Ok((stripped_batch, runs))
}

/// Return a schema carrying `relative_partition` as internal metadata.
pub(crate) fn relative_partition_schema(
    schema: &SchemaRef,
    relative_partition: usize,
) -> SchemaRef {
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        AGGR_RELATIVE_PARTITION_METADATA_KEY.to_string(),
        relative_partition.to_string(),
    );
    Arc::new(Schema::new_with_metadata(
        schema.fields().iter().cloned().collect::<Vec<_>>(),
        metadata,
    ))
}

/// Attach the relative aggregate partition to `batch` schema metadata.
pub(crate) fn with_relative_partition(
    mut batch: RecordBatch,
    relative_partition: usize,
) -> RecordBatch {
    batch.schema_metadata_mut().insert(
        AGGR_RELATIVE_PARTITION_METADATA_KEY.to_string(),
        relative_partition.to_string(),
    );
    batch
}

/// Read the relative aggregate partition from schema metadata.
pub(crate) fn relative_partition(schema: &Schema) -> Result<Option<usize>> {
    schema
        .metadata()
        .get(AGGR_RELATIVE_PARTITION_METADATA_KEY)
        .map(|value| {
            value.parse::<usize>().map_err(|err| {
                DataFusionError::Internal(format!(
                    "Invalid hash aggregate relative partition '{value}': {err}"
                ))
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn relative_partition_round_trip() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))],
        )?;

        let batch = with_relative_partition(batch, 3);
        assert_eq!(relative_partition(batch.schema_ref())?, Some(3));
        Ok(())
    }

    #[test]
    fn subpartition_column_round_trip() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![10, 11, 12, 13]))],
        )?;
        let expected_runs = vec![PartitionRun::new(0, 2)?, PartitionRun::new(1, 2)?];

        let batch = append_subpartition_column(&batch, &expected_runs)?;
        let (batch, actual_runs) = partition_runs_from_column(batch)?;

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(actual_runs, expected_runs);
        Ok(())
    }
}
