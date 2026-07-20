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

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result};

const AGGR_RELATIVE_PARTITION_METADATA_KEY: &str =
    "datafusion.internal.hash_aggr_relative_partition";

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
}
