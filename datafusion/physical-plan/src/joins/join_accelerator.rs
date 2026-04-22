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

//! Join accelerator interfaces used by [`crate::joins::NestedLoopJoinExec`].
//!
//! The current default implementation is a naive cartesian accelerator that
//! enumerates all build-side rows for each probe row in fixed-size chunks.

use std::cmp::min;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{UInt32Array, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_datafusion_err, internal_err};

/// Shared reference to a [`JoinAcceleratorFactory`].
pub type JoinAcceleratorFactoryRef = Arc<dyn JoinAcceleratorFactory>;

/// Factory for creating a [`JoinAccelerator`] for a specific NLJ build side.
pub trait JoinAcceleratorFactory: Debug + Send + Sync {
    /// User-facing name of the accelerator.
    fn name(&self) -> &'static str;

    /// Create an accelerator for a build-side schema and execution batch size.
    fn create(
        &self,
        build_schema: SchemaRef,
        batch_size: usize,
    ) -> Box<dyn JoinAccelerator>;
}

/// Runtime join accelerator state.
///
/// Implementations consume build-side batches incrementally, finalize internal
/// state in [`Self::finish`], and then create row-oriented probers for
/// `ALL_BUILD x probe_batch[row_i]`.
pub trait JoinAccelerator: Debug + Send + Sync {
    /// Create a new, uninitialized accelerator.
    fn new(build_schema: SchemaRef, batch_size: usize) -> Self
    where
        Self: Sized;

    /// Add one build-side batch to this accelerator.
    fn add_build_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Finalize the accelerator after the build side has been consumed.
    ///
    /// Implementations may use this hook to publish dynamic filters in the
    /// future. The default NLJ accelerator only materializes the buffered build
    /// batch here.
    fn finish(&mut self) -> Result<()>;

    /// Initialize a prober for `ALL_BUILD x probe_batch[probe_row]`.
    fn init_prober(
        &self,
        probe_batch: &RecordBatch,
        probe_row: usize,
    ) -> Result<Box<dyn JoinAcceleratorProber>>;

    /// Returns the fully materialized build-side batch.
    fn build_batch(&self) -> &RecordBatch;
}

/// Stateful row-at-a-time probe cursor returned by a [`JoinAccelerator`].
pub trait JoinAcceleratorProber: Debug + Send {
    /// Return the next candidate chunk for the active probe row.
    ///
    /// Implementations must return chunks whose cardinality does not exceed
    /// the configured batch size.
    fn probe(&mut self) -> Result<Option<JoinProbeBatch>>;
}

/// Candidate rows produced by a [`JoinAcceleratorProber`].
#[derive(Debug, Clone)]
pub enum JoinProbeBatch {
    /// Consecutive build rows, represented as a range for efficiency.
    BuildRange(Range<usize>),
    /// Arbitrary build row indices.
    BuildIndices(UInt64Array),
}

impl JoinProbeBatch {
    /// Number of candidate build rows in this probe batch.
    pub fn len(&self) -> usize {
        match self {
            Self::BuildRange(range) => range.len(),
            Self::BuildIndices(indices) => indices.len(),
        }
    }

    /// Returns `true` if this probe batch contains no candidate build rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert the batch into explicit build/probe index arrays.
    pub fn into_indices(self, probe_row: usize) -> Result<(UInt64Array, UInt32Array)> {
        let build_indices = match self {
            Self::BuildRange(range) => {
                let build_indices = range
                    .map(|index| {
                        u64::try_from(index).map_err(|_| {
                            internal_datafusion_err!(
                                "Join probe range index does not fit into u64: {index}"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                UInt64Array::from(build_indices)
            }
            Self::BuildIndices(indices) => indices,
        };

        let probe_row = u32::try_from(probe_row).map_err(|_| {
            internal_datafusion_err!("Probe row index does not fit into u32: {probe_row}")
        })?;
        let probe_indices = UInt32Array::from_iter_values(std::iter::repeat_n(
            probe_row,
            build_indices.len(),
        ));
        Ok((build_indices, probe_indices))
    }
}

/// Default cartesian NLJ accelerator.
#[derive(Debug, Default)]
pub struct NaiveJoinAcceleratorFactory;

impl JoinAcceleratorFactory for NaiveJoinAcceleratorFactory {
    fn name(&self) -> &'static str {
        "naive"
    }

    fn create(
        &self,
        build_schema: SchemaRef,
        batch_size: usize,
    ) -> Box<dyn JoinAccelerator> {
        Box::new(NaiveJoinAccelerator::new(build_schema, batch_size))
    }
}

#[derive(Debug)]
struct NaiveJoinAccelerator {
    build_schema: SchemaRef,
    build_batch: RecordBatch,
    buffered_build_batches: Vec<RecordBatch>,
    batch_size: usize,
}

impl NaiveJoinAccelerator {
    fn new(build_schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            build_batch: RecordBatch::new_empty(Arc::clone(&build_schema)),
            build_schema,
            buffered_build_batches: vec![],
            batch_size,
        }
    }
}

impl JoinAccelerator for NaiveJoinAccelerator {
    fn new(build_schema: SchemaRef, batch_size: usize) -> Self
    where
        Self: Sized,
    {
        Self {
            build_batch: RecordBatch::new_empty(Arc::clone(&build_schema)),
            build_schema,
            buffered_build_batches: vec![],
            batch_size,
        }
    }

    fn add_build_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.buffered_build_batches.push(batch);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.build_batch = if self.buffered_build_batches.is_empty() {
            RecordBatch::new_empty(Arc::clone(&self.build_schema))
        } else {
            concat_batches(&self.build_schema, &self.buffered_build_batches)?
        };
        self.buffered_build_batches.clear();
        Ok(())
    }

    fn init_prober(
        &self,
        probe_batch: &RecordBatch,
        probe_row: usize,
    ) -> Result<Box<dyn JoinAcceleratorProber>> {
        if probe_row >= probe_batch.num_rows() {
            return internal_err!(
                "Probe row index {probe_row} is out of bounds for a batch of {} rows",
                probe_batch.num_rows()
            );
        }

        Ok(Box::new(NaiveJoinAcceleratorProber {
            batch_size: self.batch_size.max(1),
            build_rows: self.build_batch.num_rows(),
            offset: 0,
        }))
    }

    fn build_batch(&self) -> &RecordBatch {
        &self.build_batch
    }
}

#[derive(Debug)]
struct NaiveJoinAcceleratorProber {
    batch_size: usize,
    build_rows: usize,
    offset: usize,
}

impl JoinAcceleratorProber for NaiveJoinAcceleratorProber {
    fn probe(&mut self) -> Result<Option<JoinProbeBatch>> {
        if self.offset >= self.build_rows {
            return Ok(None);
        }

        let end = min(self.offset + self.batch_size, self.build_rows);
        let range = self.offset..end;
        self.offset = end;
        Ok(Some(JoinProbeBatch::BuildRange(range)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn naive_join_accelerator_chunks_build_rows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;
        let probe_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20]))],
        )?;

        let factory = NaiveJoinAcceleratorFactory;
        let mut accelerator = factory.create(schema, 2);
        accelerator.add_build_batch(batch)?;
        accelerator.finish()?;

        let mut prober = accelerator.init_prober(&probe_batch, 1)?;
        let first = prober.probe()?.unwrap();
        let second = prober.probe()?.unwrap();
        let third = prober.probe()?.unwrap();
        let fourth = prober.probe()?;

        assert!(
            matches!(first, JoinProbeBatch::BuildRange(ref range) if range == &(0..2))
        );
        assert!(
            matches!(second, JoinProbeBatch::BuildRange(ref range) if range == &(2..4))
        );
        assert!(
            matches!(third, JoinProbeBatch::BuildRange(ref range) if range == &(4..5))
        );
        assert!(fourth.is_none());

        Ok(())
    }
}
