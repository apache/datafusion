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
//! The default probing implementation is a naive cartesian fallback that
//! enumerates all build-side rows for each probe row in fixed-size chunks.

use std::cmp::min;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{UInt32Array, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::JoinSide;
use datafusion_expr::JoinType;

use super::join_filter::JoinFilter;
use datafusion_common::{Result, internal_datafusion_err, internal_err};

/// Shared reference to a selected join accelerator.
pub type JoinAcceleratorRef = Arc<dyn JoinAccelerator>;

/// Planning-time specification used to select a join accelerator.
#[derive(Debug, Clone)]
pub struct JoinAcceleratorSpec {
    join_type: JoinType,
    build_side: JoinSide,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    filter: Option<JoinFilter>,
}

impl JoinAcceleratorSpec {
    pub fn new(
        join_type: JoinType,
        build_side: JoinSide,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        filter: Option<JoinFilter>,
    ) -> Self {
        Self {
            join_type,
            build_side,
            left_schema,
            right_schema,
            filter,
        }
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn build_side(&self) -> JoinSide {
        self.build_side
    }

    pub fn left_schema(&self) -> &SchemaRef {
        &self.left_schema
    }

    pub fn right_schema(&self) -> &SchemaRef {
        &self.right_schema
    }

    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    pub fn build_schema(&self) -> &SchemaRef {
        match self.build_side {
            JoinSide::Left => &self.left_schema,
            JoinSide::Right => &self.right_schema,
            JoinSide::None => unreachable!("Join accelerator build side cannot be None"),
        }
    }
}

/// Selects a planning-time join accelerator.
#[derive(Debug, Default)]
pub struct JoinAcceleratorBuilder;

impl JoinAcceleratorBuilder {
    /// Select the accelerator for a nested loop join.
    ///
    /// This always succeeds because NLJ has a naive cartesian fallback.
    pub fn try_new(spec: JoinAcceleratorSpec) -> Result<JoinAcceleratorRef> {
        Ok(Arc::new(FallbackNestedLoopJoinAccelerator::new(spec)))
    }
}

/// Nested loop join accelerator selected at planning time.
///
/// The accelerator is used by joins that first buffer one input, then probe the
/// other input one batch at a time. It provides two extension points for reducing
/// probe work:
///
/// - Runtime indexing on the build side. After all build batches are buffered,
///   the accelerator can create an index-like representation that finds the
///   candidate build rows for each probe row more cheaply than a cartesian scan.
/// - Dynamic filtering on the probe side. Build-side statistics can produce a
///   probe-side filter that eliminates rows which cannot match any build row.
///
/// Accelerators should only handle an accelerated predicate: a conjunct from a
/// conjunction join condition (e.g. `cond1` from `cond1 AND cond2 AND cond3`),
/// and:
///
/// - It should emit candidate pairs efficiently, using the accelerated predicate
///   to avoid unnecessary cartesian comparisons, for example by building an index
///   on the build side before probing.
/// - It must not discard any pair that could satisfy the full join filter. The
///   accelerator may emit a superset of the final matches.
///
/// Residual join filter evaluation and outer, semi, anti, and mark join match
/// tracking are handled by the outer nested-loop join driver. See the examples
/// below for term definitions.
///
/// # Example: PiecewiseMergeJoin
///
/// For workload
///
/// ```sql
/// select *
/// from generate_series(1000) as t1(v1)
/// join generate_series(1000000) as t2(v1)
/// on (t1.v1 > t2.v1) AND ((t1.v1 + t2.v1) % 2 = 0)
/// ```
///
/// `(t1.v1 > t2.v1)` is the accelerated predicate because it can be evaluated
/// efficiently with a specific algorithm; see below for details.
/// `((t1.v1 + t2.v1) % 2 = 0)` is the residual predicate and is evaluated by
/// the outer nested-loop join driver after candidate pairs are produced.
///
/// ## (TODO) Dynamic filter
///
/// After buffering `t1` as the build side, the accelerator knows
/// `max(t1.v1) = 1000` for `generate_series(1000)`. For an inner join, probe
/// rows with `t2.v1 >= 1000` cannot satisfy `t1.v1 > t2.v1` and can be filtered
/// out.
/// This reduces the candidate search space from roughly `1K x 1M` pairs to
/// roughly `1K x 1K` pairs before residual filter evaluation.
///
/// ## Runtime index
///
/// The accelerator can also sort the buffered `t1` rows by `v1`. For each
/// incoming `t2` batch, it sorts the probe rows by `v1` and scans the two sorted
/// runs once. For a probe row where `t2.v1 = 10`, the matching build rows are the
/// suffix of sorted `t1` rows with `t1.v1 > 10`; for a later probe row where
/// `t2.v1 = 20`, the scan cursor only moves forward. This avoids checking every
/// build row for every probe row while still producing all pairs that satisfy the
/// accelerated predicate. The residual predicate is then applied to those pairs
/// by the join driver.
/// This step further reduces the operation count from `1K x 1K` to
/// `log(1K) + 1K + 1K` (sort and linear scans).
///
/// # Control flow
/// The following pseudo-code demonstrates the high-level join control flow and
/// how functions in this trait are used.
///
/// ```text
/// for build_batch in build_input {
///     accelerator.add_build_batch(build_batch)?;
/// }
/// accelerator.finish()?;
///
/// for probe_batch in probe_input {
///     let (probe_batch, mut prober) =
///         accelerator.init_prober(probe_batch, batch_size)?;
///
///     while let Some(candidates) = prober.probe()? {
///         // The join driver consumes candidates with the prepared probe_batch,
///         // applies the residual filter, and records join matches.
///     }
/// }
/// ```
pub trait JoinAccelerator: Debug + Send + Sync {
    fn name(&self) -> &'static str;

    /// Return `true` only if this accelerator supports the nested-loop join
    /// spilling execution path. See [`super::NestedLoopJoinExec`] for details.
    fn support_spilling(&self) -> bool {
        false
    }

    /// Create a fresh mutable accelerator for one execution.
    fn clone_accelerator(&self) -> Box<dyn JoinAccelerator>;

    /// Add one build-side input batch to the buffer, and optionally build a
    /// runtime index.
    fn add_build_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Signal the end of build-side input and prepare for probing.
    fn finish(&mut self) -> Result<()>;

    /// Return the concatenated build-side batch from all batches added with
    /// [`Self::add_build_batch`].
    fn build_batch(&self) -> &RecordBatch;

    /// Return the number of rows in the accumulated build side.
    fn num_build_rows(&self) -> usize;

    /// Initialize probing for one probe-side batch.
    ///
    /// Returns the prepared probe batch and a prober that iterates over
    /// candidate pairs satisfying the accelerated predicate.
    ///
    /// - Prepared probe batch: implementations may preprocess the probe batch;
    ///   the outer join loop manages this returned batch while consuming the
    ///   prober.
    /// - Output shape: an iterator over candidate pairs from
    ///   `ALL_BUILD_BATCHES x CURRENT_PROBE_BATCH` that satisfy the accelerated
    ///   predicate.
    ///
    /// # Default implementation
    ///
    /// The default implementation is the nested-loop fallback: it returns the
    /// probe batch unchanged and emits every build-side row as a candidate for
    /// each probe row. Implementations that only provide dynamic filtering
    /// should reuse this default.
    fn init_prober(
        &self,
        probe_batch: RecordBatch,
        batch_size: usize,
    ) -> Result<(RecordBatch, Box<dyn JoinAcceleratorProber>)> {
        let prober = Box::new(FallbackNestedLoopJoinProber {
            batch_size: batch_size.max(1),
            build_batch_size: self.num_build_rows(),
            probe_batch_size: probe_batch.num_rows(),
            cur_probe_offset: 0,
            cur_build_offset: 0,
        });

        Ok((probe_batch, prober))
    }
}

/// Stateful batch-level probe cursor returned by a runtime [`JoinAccelerator`].
pub trait JoinAcceleratorProber: Debug + Send {
    /// Incrementally return the next candidate chunk for the active probe batch,
    /// returns `None` when the probing ends.
    ///
    /// Candidate indices are relative to the accelerator's concatenated build
    /// batch and the prepared probe batch returned by [`JoinAccelerator::init_prober`].
    ///
    /// Implementations should:
    /// - Try to emit as many matches as possible at once for efficiency
    /// - Emit at most `batch_size` matches in one iteration, to make subsequent
    ///   operations more memory-efficient
    ///
    /// TODO(PR): Currently NLJ assumes all exact `batch_size` matches. A new join
    /// index might output fewer, so the caller might want a `BatchCoalescer` to
    /// vectorize the later remaining join filter evaluation.
    fn probe(&mut self) -> Result<Option<JoinProbeCandidates>>;
}

/// Candidate join pairs produced by a [`JoinAcceleratorProber`].
///
/// # Index representation
/// - Probe-side indices are relative to the current preprocessed probe batch
///   produced from [`JoinAccelerator::init_prober`].
/// - Build-side indices are relative to the concatenated build-side batch from
///   [`JoinAccelerator::build_batch`].
#[derive(Debug, Clone)]
pub enum JoinProbeCandidates {
    /// Consecutive build rows for one probe row, represented as a range for
    /// efficiency in NLJ.
    BuildRange {
        probe_row: usize,
        build_range: Range<usize>,
    },
    /// Arbitrary build/probe row pairs.
    BuildIndices {
        build_indices: UInt64Array,
        probe_indices: UInt32Array,
    },
}

impl JoinProbeCandidates {
    pub fn try_new_indices(
        build_indices: UInt64Array,
        probe_indices: UInt32Array,
    ) -> Result<Self> {
        if build_indices.len() != probe_indices.len() {
            return internal_err!(
                "Join probe build/probe index lengths differ: {} vs {}",
                build_indices.len(),
                probe_indices.len()
            );
        }

        Ok(Self::BuildIndices {
            build_indices,
            probe_indices,
        })
    }

    /// Number of candidate build rows in this probe batch.
    pub fn len(&self) -> usize {
        match self {
            Self::BuildRange { build_range, .. } => build_range.len(),
            Self::BuildIndices { build_indices, .. } => build_indices.len(),
        }
    }

    /// Returns `true` if this probe batch contains no candidate build rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert the batch into explicit build/probe index arrays.
    pub fn into_indices(self) -> Result<(UInt64Array, UInt32Array)> {
        match self {
            Self::BuildRange {
                probe_row,
                build_range,
            } => {
                let build_indices = build_range
                    .map(|index| {
                        u64::try_from(index).map_err(|_| {
                            internal_datafusion_err!(
                                "Join probe range index does not fit into u64: {index}"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let build_indices = UInt64Array::from(build_indices);
                let probe_row = u32::try_from(probe_row).map_err(|_| {
                    internal_datafusion_err!(
                        "Probe row index does not fit into u32: {probe_row}"
                    )
                })?;
                let probe_indices = UInt32Array::from_iter_values(std::iter::repeat_n(
                    probe_row,
                    build_indices.len(),
                ));
                Ok((build_indices, probe_indices))
            }
            Self::BuildIndices {
                build_indices,
                probe_indices,
                ..
            } => Ok((build_indices, probe_indices)),
        }
    }
}

#[derive(Debug, Clone)]
struct FallbackNestedLoopJoinAccelerator {
    spec: JoinAcceleratorSpec,
    build_batches: Vec<RecordBatch>,
    build_batch: Option<RecordBatch>,
    build_rows: usize,
    finished: bool,
}

impl FallbackNestedLoopJoinAccelerator {
    fn new(spec: JoinAcceleratorSpec) -> Self {
        Self {
            spec,
            build_batches: Vec::new(),
            build_batch: None,
            build_rows: 0,
            finished: false,
        }
    }

    fn build_batch_ref(&self) -> &RecordBatch {
        debug_assert!(
            self.finished,
            "JoinAccelerator::build_batch called before finish"
        );
        self.build_batch
            .as_ref()
            .expect("JoinAccelerator::build_batch called before finish")
    }
}

impl JoinAccelerator for FallbackNestedLoopJoinAccelerator {
    fn name(&self) -> &'static str {
        "naive"
    }

    fn support_spilling(&self) -> bool {
        true
    }

    fn clone_accelerator(&self) -> Box<dyn JoinAccelerator> {
        Box::new(Self::new(self.spec.clone()))
    }

    fn add_build_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.finished {
            return internal_err!("Cannot add build batches after accelerator finish");
        }

        self.build_rows =
            self.build_rows
                .checked_add(batch.num_rows())
                .ok_or_else(|| {
                    internal_datafusion_err!("Build-side row count overflowed usize")
                })?;
        self.build_batches.push(batch);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            return internal_err!("Cannot finish accelerator more than once");
        }

        let build_schema = self.spec.build_schema();
        let build_batch = if self.build_batches.is_empty() {
            RecordBatch::new_empty(Arc::clone(build_schema))
        } else {
            concat_batches(build_schema, &self.build_batches)?
        };
        self.build_rows = build_batch.num_rows();
        self.build_batch = Some(build_batch);
        self.build_batches.clear();
        self.finished = true;
        Ok(())
    }

    fn build_batch(&self) -> &RecordBatch {
        self.build_batch_ref()
    }

    fn num_build_rows(&self) -> usize {
        self.build_rows
    }
}

#[derive(Debug)]
struct FallbackNestedLoopJoinProber {
    /// From configuration that controls the max output chunk size.
    batch_size: usize,
    /// Row count from the concatenated build side batch.
    build_batch_size: usize,
    /// Current index on the build side.
    cur_build_offset: usize,
    /// Row count from the current probe batch.
    probe_batch_size: usize,
    /// Current index on the probe side.
    cur_probe_offset: usize,
}

impl JoinAcceleratorProber for FallbackNestedLoopJoinProber {
    /// Each time returns candidates like:
    ///
    /// ```text
    /// for probe_row in probe_batch:
    ///    for batch_slice in build_batch:
    ///      output(build_slice] x probe_row)
    /// ```
    ///
    /// Each output batch is chunked to `batch_size` rows.
    fn probe(&mut self) -> Result<Option<JoinProbeCandidates>> {
        if self.cur_probe_offset >= self.probe_batch_size || self.build_batch_size == 0 {
            return Ok(None);
        }

        let end = min(
            self.cur_build_offset + self.batch_size,
            self.build_batch_size,
        );
        let build_range = self.cur_build_offset..end;
        let probe_row = self.cur_probe_offset;
        self.cur_build_offset = end;

        if self.cur_build_offset >= self.build_batch_size {
            self.cur_probe_offset += 1;
            self.cur_build_offset = 0;
        }

        Ok(Some(JoinProbeCandidates::BuildRange {
            probe_row,
            build_range,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::JoinSide;
    use datafusion_expr::JoinType;

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

        let spec = JoinAcceleratorSpec::new(
            JoinType::Inner,
            JoinSide::Left,
            Arc::clone(&schema),
            Arc::clone(&schema),
            None,
        );
        let accelerator = JoinAcceleratorBuilder::try_new(spec)?;
        let mut runtime = accelerator.clone_accelerator();
        runtime.add_build_batch(batch)?;
        runtime.finish()?;
        assert_eq!(runtime.build_batch().num_rows(), 5);

        let (_probe_batch, mut prober) = runtime.init_prober(probe_batch, 2)?;
        let first = prober.probe()?.unwrap();
        let second = prober.probe()?.unwrap();
        let third = prober.probe()?.unwrap();
        let fourth = prober.probe()?.unwrap();

        assert!(
            matches!(first, JoinProbeCandidates::BuildRange { probe_row: 0, ref build_range } if build_range == &(0..2))
        );
        assert!(
            matches!(second, JoinProbeCandidates::BuildRange { probe_row: 0, ref build_range } if build_range == &(2..4))
        );
        assert!(
            matches!(third, JoinProbeCandidates::BuildRange { probe_row: 0, ref build_range } if build_range == &(4..5))
        );
        assert!(
            matches!(fourth, JoinProbeCandidates::BuildRange { probe_row: 1, ref build_range } if build_range == &(0..2))
        );

        Ok(())
    }

    #[test]
    fn try_new_selects_naive_join_accelerator() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let spec = JoinAcceleratorSpec::new(
            JoinType::Inner,
            JoinSide::Left,
            Arc::clone(&schema),
            Arc::clone(&schema),
            None,
        );
        let accelerator = JoinAcceleratorBuilder::try_new(spec)?;
        assert_eq!(accelerator.name(), "naive");
        assert!(accelerator.support_spilling());
        Ok(())
    }
}
