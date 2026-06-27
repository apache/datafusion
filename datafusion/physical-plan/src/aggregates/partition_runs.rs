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

//! Metadata helpers for hash aggregate repartition runs.
//!
//! This module does NOT perform repartitioning or aggregation. It only owns the
//! schema metadata contract used to describe contiguous relative aggregate
//! partition runs inside a coalesced record batch.
//!
//! Entry points: [`set_partition_runs_metadata`] and [`partition_runs`].

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};

const PARTITION_RUN_SEPARATOR: char = ',';
const PARTITION_RUN_FIELD_SEPARATOR: char = ':';

pub(crate) const AGGR_PARTITION_RUNS_METADATA_KEY: &str =
    "datafusion.internal.hash_aggr_partition_runs";

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

/// Return partition runs encoded on `schema`, if present.
pub(crate) fn partition_runs(schema: &Schema) -> Result<Option<Vec<PartitionRun>>> {
    schema
        .metadata()
        .get(AGGR_PARTITION_RUNS_METADATA_KEY)
        .map(|value| decode_partition_runs(value))
        .transpose()
}

/// Encode partition runs as schema metadata value.
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

    if runs.iter().any(|run| run.len == 0) {
        return internal_err!("Hash aggregate partition run length must be nonzero");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    // Covers encoding and decoding multiple relative aggregate partition runs.
    // Example: runs [(0, 3), (2, 4)] are encoded as "0:3,2:4".
    #[test]
    fn test_partition_runs_encode_decode_multiple_runs() -> Result<()> {
        let runs = vec![PartitionRun::new(0, 3)?, PartitionRun::new(2, 4)?];

        let encoded = encode_partition_runs(&runs);
        assert_eq!(encoded, "0:3,2:4");
        assert_eq!(decode_partition_runs(&encoded)?, runs);

        Ok(())
    }

    // Covers attaching partition runs to record batch schema metadata.
    // Example: a 3-row batch with run [(1, 3)] stores metadata "1:3".
    #[test]
    fn test_set_partition_runs_metadata_sets_batch_schema_metadata() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))],
        )?;
        let runs = vec![PartitionRun::new(1, 3)?];

        let batch = set_partition_runs_metadata(batch, &runs)?;

        assert_eq!(decode_partition_runs(&encode_partition_runs(&runs))?, runs);
        assert_eq!(
            batch
                .schema_ref()
                .metadata()
                .get(AGGR_PARTITION_RUNS_METADATA_KEY),
            Some(&"1:3".to_string())
        );
        Ok(())
    }
}
