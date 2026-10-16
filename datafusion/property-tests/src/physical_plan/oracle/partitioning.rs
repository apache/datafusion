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

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::Time;
use datafusion_physical_plan::repartition::BatchPartitioner;

/// Splits the rows of `batches` into `partitions` groups by the hash of
/// `exprs`, the same way `RepartitionExec` does for `Partitioning::Hash`.
///
/// Uses `BatchPartitioner` so that the result matches the hash that
/// DataFusion operators assume for co-partitioned inputs.
pub fn hash_partition(
    batches: &[RecordBatch],
    exprs: &[Arc<dyn PhysicalExpr>],
    partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    let mut partitioner =
        BatchPartitioner::new_hash_partitioner(exprs.to_vec(), partitions, Time::new())?;
    let mut output = vec![vec![]; partitions];
    for batch in batches {
        partitioner.partition(batch.clone(), |partition, batch| {
            output[partition].push(batch);
            Ok(())
        })?;
    }
    Ok(output)
}

/// Returns the number of rows in `batches` that do not belong to hash
/// partition `partition` of `partitions` by the hash of `exprs`.
pub fn rows_outside_hash_partition(
    batches: &[RecordBatch],
    exprs: &[Arc<dyn PhysicalExpr>],
    partitions: usize,
    partition: usize,
) -> Result<usize> {
    let split = hash_partition(batches, exprs, partitions)?;
    Ok(split
        .iter()
        .enumerate()
        .filter(|(p, _)| *p != partition)
        .flat_map(|(_, batches)| batches)
        .map(RecordBatch::num_rows)
        .sum())
}
