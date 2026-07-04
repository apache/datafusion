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

#![allow(dead_code)]

//! Internal helpers for hash aggregate repartition subpartitions.
//!
//! This module does NOT perform repartitioning or aggregation. It only owns the
//! internal contracts used to describe relative aggregate subpartitions passed
//! from repartition to final hash aggregation.
//!
//! Entry points: [`append_subpartition_column`], [`reorder_by_subpartition`],
//! [`set_partition_runs_metadata`] and [`partition_runs`].

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, MutableArrayData, RecordBatch, RecordBatchOptions, UInt32Array,
    make_array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::cast::as_uint32_array;
use datafusion_common::{Result, internal_err};

const PARTITION_RUN_SEPARATOR: char = ',';
const PARTITION_RUN_FIELD_SEPARATOR: char = ':';

pub(crate) const AGGR_PARTITION_RUNS_METADATA_KEY: &str =
    "datafusion.internal.hash_aggr_partition_runs";

pub(crate) const AGGR_SUBPARTITION_COLUMN_NAME: &str =
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
pub(crate) fn subpartition_schema(schema: &SchemaRef) -> SchemaRef {
    if subpartition_column_index(schema.as_ref()).is_some() {
        return Arc::clone(schema);
    }

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
    validate_partition_runs(runs, batch.num_rows())?;
    if subpartition_column_index(batch.schema_ref()).is_some() {
        return internal_err!("Hash aggregate subpartition column already exists");
    }

    let mut values = Vec::with_capacity(batch.num_rows());
    for run in runs {
        let relative_partition =
            u32::try_from(run.relative_partition).map_err(|err| {
                datafusion_common::DataFusionError::Internal(format!(
                    "Hash aggregate relative partition does not fit UInt32: {err}"
                ))
            })?;
        values.extend(std::iter::repeat_n(relative_partition, run.len));
    }

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt32Array::from(values)) as ArrayRef);
    let schema = subpartition_schema(batch.schema_ref());
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(Into::into)
}

/// Reusable scratch space for [`reorder_by_subpartition`].
#[derive(Default)]
pub(crate) struct SubpartitionReorderBuffer {
    counts: Vec<usize>,
    offsets: Vec<usize>,
    source_runs: Vec<SourcePartitionRun>,
    runs: Vec<PartitionRun>,
}

impl SubpartitionReorderBuffer {
    /// Create an empty reorder scratch buffer.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return partition runs produced by the latest reorder.
    pub(crate) fn runs(&self) -> &[PartitionRun] {
        &self.runs
    }

    fn clear(&mut self) {
        self.counts.clear();
        self.offsets.clear();
        self.source_runs.clear();
        self.runs.clear();
    }
}

/// Reorder `batch` by its internal subpartition column and strip that column.
pub(crate) fn reorder_by_subpartition(
    batch: &RecordBatch,
    buffer: &mut SubpartitionReorderBuffer,
) -> Result<Option<RecordBatch>> {
    buffer.clear();

    let Some(subpartition_idx) = subpartition_column_index(batch.schema_ref()) else {
        return Ok(None);
    };

    let subpartitions = as_uint32_array(batch.column(subpartition_idx).as_ref())?;
    if subpartitions.null_count() != 0 {
        return internal_err!(
            "Hash aggregate subpartition column must not contain nulls"
        );
    }

    let num_rows = batch.num_rows();
    if num_rows > u32::MAX as usize {
        return internal_err!(
            "Hash aggregate batch row count {num_rows} does not fit UInt32"
        );
    }

    for relative_partition in subpartitions.values().iter().copied() {
        let relative_partition = relative_partition as usize;
        if relative_partition >= buffer.counts.len() {
            buffer.counts.resize(relative_partition + 1, 0);
        }
        buffer.counts[relative_partition] += 1;
    }

    buffer.offsets.resize(buffer.counts.len(), 0);
    let mut output_offset = 0;
    for (relative_partition, len) in buffer.counts.iter().copied().enumerate() {
        buffer.offsets[relative_partition] = output_offset;
        if len != 0 {
            buffer
                .runs
                .push(PartitionRun::new(relative_partition, len)?);
        }
        output_offset += len;
    }
    debug_assert_eq!(output_offset, num_rows);

    let mut is_identity = true;
    let mut values = subpartitions.values().iter().copied().enumerate();
    if let Some((run_start, first_partition)) = values.next() {
        let mut current_partition = first_partition as usize;
        let mut current_start = run_start;
        let mut current_len = 1;
        for (row_idx, relative_partition) in values {
            let relative_partition = relative_partition as usize;
            if relative_partition == current_partition {
                current_len += 1;
            } else {
                push_source_run(
                    buffer,
                    current_partition,
                    current_start,
                    current_len,
                    &mut is_identity,
                );
                current_partition = relative_partition;
                current_start = row_idx;
                current_len = 1;
            }
        }
        push_source_run(
            buffer,
            current_partition,
            current_start,
            current_len,
            &mut is_identity,
        );
    }

    let schema = strip_subpartition_schema(batch.schema_ref(), subpartition_idx);
    let columns = strip_subpartition_columns(batch, subpartition_idx);
    let output = if is_identity {
        RecordBatch::try_new(schema, columns)?
    } else {
        buffer
            .source_runs
            .sort_unstable_by_key(|run| run.output_start);
        let columns =
            reorder_columns_by_source_runs(&columns, &buffer.source_runs, num_rows);
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(schema, columns, &options)?
    };

    Ok(Some(output))
}

#[derive(Debug, Clone, Copy)]
struct SourcePartitionRun {
    start: usize,
    len: usize,
    output_start: usize,
}

fn push_source_run(
    buffer: &mut SubpartitionReorderBuffer,
    relative_partition: usize,
    start: usize,
    len: usize,
    is_identity: &mut bool,
) {
    let output_start = buffer.offsets[relative_partition];
    buffer.offsets[relative_partition] += len;
    *is_identity &= output_start == start;
    buffer.source_runs.push(SourcePartitionRun {
        start,
        len,
        output_start,
    });
}

fn reorder_columns_by_source_runs(
    columns: &[ArrayRef],
    source_runs: &[SourcePartitionRun],
    num_rows: usize,
) -> Vec<ArrayRef> {
    columns
        .iter()
        .map(|column| reorder_array_by_source_runs(column, source_runs, num_rows))
        .collect()
}

fn reorder_array_by_source_runs(
    array: &ArrayRef,
    source_runs: &[SourcePartitionRun],
    num_rows: usize,
) -> ArrayRef {
    let array_data = array.to_data();
    let mut mutable = MutableArrayData::new(vec![&array_data], false, num_rows);
    for run in source_runs {
        mutable.extend(0, run.start, run.start + run.len);
    }
    make_array(mutable.freeze())
}

/// Return partition runs encoded on `schema`, if present.
pub(crate) fn partition_runs(schema: &Schema) -> Result<Option<Vec<PartitionRun>>> {
    schema
        .metadata()
        .get(AGGR_PARTITION_RUNS_METADATA_KEY)
        .map(|value| decode_partition_runs(value))
        .transpose()
}

/// Encode partition runs as schema metadata value.
#[cfg(test)]
pub(crate) fn encode_partition_runs(runs: &[PartitionRun]) -> String {
    runs.iter()
        .map(|run| format!("{}:{}", run.relative_partition, run.len))
        .collect::<Vec<_>>()
        .join(",")
}

/// Decode partition runs from schema metadata value.
pub(crate) fn decode_partition_runs(value: &str) -> Result<Vec<PartitionRun>> {
    if value.is_empty() {
        return Ok(vec![]);
    }

    value
        .split(PARTITION_RUN_SEPARATOR)
        .map(decode_partition_run)
        .collect()
}

/// Set partition runs metadata on `batch`.
#[cfg(test)]
pub(crate) fn set_partition_runs_metadata(
    mut batch: RecordBatch,
    runs: &[PartitionRun],
) -> Result<RecordBatch> {
    validate_partition_runs(runs, batch.num_rows())?;
    let value = encode_partition_runs(runs);
    debug_assert_eq!(decode_partition_runs(&value)?, runs);
    batch
        .schema_metadata_mut()
        .insert(AGGR_PARTITION_RUNS_METADATA_KEY.to_string(), value);
    Ok(batch)
}

pub(crate) fn subpartition_column_index(schema: &Schema) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == AGGR_SUBPARTITION_COLUMN_NAME)
}

fn strip_subpartition_schema(schema: &SchemaRef, subpartition_idx: usize) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != subpartition_idx)
        .map(|(_, field)| Arc::clone(field))
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn strip_subpartition_columns(
    batch: &RecordBatch,
    subpartition_idx: usize,
) -> Vec<ArrayRef> {
    batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != subpartition_idx)
        .map(|(_, column)| Arc::clone(column))
        .collect()
}

fn decode_partition_run(value: &str) -> Result<PartitionRun> {
    let Some((relative_partition, len)) = value.split_once(PARTITION_RUN_FIELD_SEPARATOR)
    else {
        return internal_err!(
            "Invalid hash aggregate partition run metadata entry '{value}'"
        );
    };

    let relative_partition = relative_partition.parse::<usize>().map_err(|err| {
        datafusion_common::DataFusionError::Internal(format!(
            "Invalid hash aggregate partition run partition '{relative_partition}': {err}"
        ))
    })?;
    let len = len.parse::<usize>().map_err(|err| {
        datafusion_common::DataFusionError::Internal(format!(
            "Invalid hash aggregate partition run length '{len}': {err}"
        ))
    })?;

    PartitionRun::new(relative_partition, len)
}

fn validate_partition_runs(runs: &[PartitionRun], num_rows: usize) -> Result<()> {
    if num_rows == 0 {
        if runs.is_empty() {
            return Ok(());
        }
        return internal_err!(
            "Hash aggregate partition runs metadata must be empty for empty batches"
        );
    }

    if runs.is_empty() {
        return internal_err!(
            "Hash aggregate partition runs metadata must not be empty for non-empty batches"
        );
    }

    let total_rows: usize = runs.iter().map(|run| run.len).sum();
    if total_rows != num_rows {
        return internal_err!(
            "Hash aggregate partition runs contain {total_rows} rows, expected {num_rows}"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_partition_runs_encode_decode_multiple_runs() -> Result<()> {
        let runs = vec![PartitionRun::new(0, 3)?, PartitionRun::new(2, 4)?];
        let encoded = encode_partition_runs(&runs);
        assert_eq!(encoded, "0:3,2:4");
        assert_eq!(decode_partition_runs(&encoded)?, runs);
        Ok(())
    }

    #[test]
    fn test_set_partition_runs_metadata_sets_batch_schema_metadata() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))],
        )?;
        let runs = vec![PartitionRun::new(1, 3)?];
        let batch = set_partition_runs_metadata(batch, &runs)?;
        assert_eq!(partition_runs(batch.schema_ref())?, Some(runs.clone()));
        assert_eq!(decode_partition_runs(&encode_partition_runs(&runs))?, runs);
        Ok(())
    }

    #[test]
    fn test_reorder_by_subpartition_groups_rows_and_strips_column() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from(vec![10, 11, 12, 13]))],
        )?;
        let batch = append_subpartition_column(
            &batch,
            &[
                PartitionRun::new(1, 1)?,
                PartitionRun::new(0, 2)?,
                PartitionRun::new(1, 1)?,
            ],
        )?;

        let mut buffer = SubpartitionReorderBuffer::new();
        let batch = reorder_by_subpartition(&batch, &mut buffer)?.unwrap();
        let values = as_uint32_array(batch.column(0).as_ref())?;
        assert_eq!(values.values(), &[11, 12, 10, 13]);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(
            buffer.runs(),
            &[PartitionRun::new(0, 2)?, PartitionRun::new(1, 2)?]
        );
        Ok(())
    }
}
