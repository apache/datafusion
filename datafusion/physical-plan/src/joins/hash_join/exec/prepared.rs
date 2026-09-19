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

//! Explicit immutable build reuse for embedding executors.

use super::*;
use arrow::array::{Array, AsArray};
use datafusion_common::exec_datafusion_err;
use datafusion_execution::memory_pool::MemoryPool;

/// An immutable, fully prepared broadcast build, independent of any probe task.
///
/// Created by [`HashJoinExec::prepare_build`]. The embedding executor owns cache
/// identity, admission, single-flight coordination, cancellation and eviction.
/// This object retains its input buffers and memory reservation until its last
/// lease is dropped; it never retains an input stream or task context. Prepared
/// builds support fixed-width and UTF-8 build columns, with direct-column keys
/// and non-spilling INNER joins. Residual conditions belong to each consuming
/// join; null-aware joins remain unsupported.
///
/// Hash-join gathers copy supported build columns into output buffers,
/// including contiguous selections. Output batches can therefore outlive this
/// object without retaining unaccounted cached payload. View, dictionary and
/// nested build columns remain unsupported. UTF-8 and fixed-size binary keys
/// use hash-table membership filters instead of copying range or IN-list values.
pub struct PreparedHashJoinBuild {
    build: Arc<JoinBuildData>,
    keys: Vec<usize>,
    null_equality: NullEquality,
}

impl fmt::Debug for PreparedHashJoinBuild {
    /// Describe immutable metadata without dumping table contents.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedHashJoinBuild")
            .field("schema", &self.build.batch.schema())
            .field("keys", &self.keys)
            .field("rows", &self.num_rows())
            .field("reserved_bytes", &self.reserved_bytes())
            .finish()
    }
}

impl PreparedHashJoinBuild {
    /// Return the retained build reservation, excluding all per-probe state.
    pub fn reserved_bytes(&self) -> usize {
        self.build.reservation.size()
    }

    /// Return the complete build row count, including duplicate and null keys.
    pub fn num_rows(&self) -> usize {
        self.build.batch.num_rows()
    }

    /// Create independent mutable state for one consuming join.
    pub(super) fn probe_data(&self, probe_threads: usize) -> JoinLeftData {
        JoinLeftData {
            build: Arc::clone(&self.build),
            null_aware_mark_scope_map: None,
            null_value_scope_map: None,
            visited_indices_bitmap: Mutex::new(BooleanBufferBuilder::new(0)),
            null_indices_bitmap: Mutex::new(BooleanBufferBuilder::new(0)),
            probe_completion: ProbeCompletion::new(probe_threads),
            build_side_has_null: false,
            _probe_reservation: self.build.reservation.new_empty(),
        }
    }

    /// Validate the build descriptor and current execution restrictions without
    /// consuming input or modifying either plan. Cache identity is caller-owned.
    pub(super) fn validate(&self, join: &HashJoinExec) -> Result<()> {
        let keys = prepared_key_indices(join)?;
        if join.left.schema() != self.build.batch.schema()
            || keys != self.keys
            || join.null_equality != self.null_equality
        {
            return plan_err!(
                "Prepared hash-join build does not match schema, keys or null equality"
            );
        }
        if let Some(filter) = &join.dynamic_filter {
            let filter_keys = filter.filter.children();
            if filter_keys.len() != join.on.len()
                || filter_keys
                    .iter()
                    .zip(&join.on)
                    .any(|(filter_key, (_, probe_key))| {
                        filter_key.as_ref() != probe_key.as_ref()
                    })
            {
                return plan_err!(
                    "Prepared hash-join dynamic filter keys do not match probe keys"
                );
            }
        }
        Ok(())
    }
}

impl HashJoinExec {
    /// Prepare one immutable build using an embedding executor's durable pool.
    ///
    /// The supplied stream must own its native buffers independently of producer
    /// task cleanup. This method consumes only that stream, never `self.left`,
    /// and reserves retained data, hash buckets and row-index chains against
    /// `pool`. The caller must keep original producer allocations charged until
    /// its stream releases them. `config` controls ordinary perfect-map and
    /// dynamic-filter choices. UTF-8 and fixed-size binary keys
    /// retain hash membership only:
    /// range bounds and IN-list literals would allocate unaccounted key copies.
    ///
    /// Validates eligibility and the stream schema before polling. On error or
    /// future cancellation, all work and reservations are dropped; no partially
    /// prepared object is returned. Concurrent preparation/cache publication is
    /// the caller's responsibility. Bounds and membership are prepared once, but
    /// each consuming join publishes them into its own dynamic filter.
    pub async fn prepare_build(
        &self,
        input: SendableRecordBatchStream,
        pool: Arc<dyn MemoryPool>,
        config: Arc<ConfigOptions>,
    ) -> Result<Arc<PreparedHashJoinBuild>> {
        let keys = prepared_key_indices(self)?;
        let schema = self.left.schema();
        if input.schema() != schema {
            return plan_err!(
                "Prepared hash-join input schema does not match build schema"
            );
        }
        let byte_keys = keys.iter().any(|&key| {
            matches!(
                schema.field(key).data_type(),
                DataType::Utf8 | DataType::FixedSizeBinary(_)
            )
        });
        // Range accumulation and IN-list publication materialize ScalarValue
        // copies of byte keys. Hash membership borrows the admitted table instead.
        let config = if byte_keys {
            let mut config = config.as_ref().clone();
            config.optimizer.hash_join_inlist_pushdown_max_size = 0;
            Arc::new(config)
        } else {
            config
        };
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = BuildProbeJoinMetrics::new(0, &metrics_set);
        let count = MetricBuilder::new(&metrics_set)
            .counter(ARRAY_MAP_CREATED_COUNT_METRIC_NAME, 0);
        let reservation = MemoryConsumer::new("PreparedHashJoinBuild").register(&pool);
        let data = collect_left_input(
            self.random_state.random_state().clone(),
            input,
            self.on.iter().map(|(left, _)| Arc::clone(left)).collect(),
            metrics,
            reservation,
            false,
            0,
            !byte_keys,
            config,
            self.null_equality,
            None,
            count,
            true,
        )
        .await?;
        Ok(Arc::new(PreparedHashJoinBuild {
            build: data.build,
            keys,
            null_equality: self.null_equality,
        }))
    }
}

/// Bound copy allocations, including validity, offsets and alignment. Aliased
/// columns count separately because concatenation materializes each column.
pub(super) fn prepared_copy_bytes(batch: &RecordBatch) -> Result<usize> {
    let rows = batch.num_rows();
    batch.columns().iter().try_fold(0usize, |total, array| {
        let values = match array.data_type() {
            DataType::Utf8 => rows
                .checked_add(1)
                .and_then(|len| len.checked_mul(4))
                .and_then(|offsets| utf8_value_span(array.as_ref()).checked_add(offsets))
                // UTF-8 has a third allocation for offsets.
                .and_then(|bytes| bytes.checked_add(64)),
            DataType::Null => Some(0),
            DataType::Boolean => Some(rows.div_ceil(8)),
            DataType::FixedSizeBinary(width) => usize::try_from(*width)
                .ok()
                .and_then(|width| width.checked_mul(rows)),
            ty => ty
                .primitive_width()
                .and_then(|width| width.checked_mul(rows)),
        };
        values
            .and_then(|bytes| bytes.checked_add(rows.div_ceil(8)))
            .and_then(|bytes| bytes.checked_add(2 * 64))
            .and_then(|bytes| total.checked_add(bytes))
            .ok_or_else(|| exec_datafusion_err!("Prepared hash-join copy size overflow"))
    })
}

/// Include values hidden by nulls but exclude bytes outside a sliced array.
fn utf8_value_span(array: &dyn Array) -> usize {
    let offsets = array.as_string::<i32>().value_offsets();
    (offsets[offsets.len() - 1] - offsets[0]) as usize
}

/// Add `batch`'s byte-column spans to `totals`, one entry per schema column, before
/// an eventual single-batch concat. Reject offset overflow before allocating
/// the value buffer; fixed-width columns leave their totals unchanged.
pub(super) fn check_byte_concat_sizes(
    batch: &RecordBatch,
    totals: &mut [usize],
) -> Result<()> {
    for (array, total) in batch.columns().iter().zip(totals) {
        if matches!(array.data_type(), DataType::Utf8) {
            *total = total.checked_add(utf8_value_span(array.as_ref()))
                .filter(|&sum| i32::try_from(sum).is_ok())
                .ok_or_else(|| exec_datafusion_err!(
                    "Prepared hash-join UTF-8 column exceeds its offset limit; a compact build is required"
                ))?;
        }
    }
    Ok(())
}

impl HashJoinExecBuilder {
    /// Attach a fully prepared build to a fresh, compatible join execution.
    ///
    /// [`Self::build`] validates compatibility. Attaching resets the build future
    /// and execution metrics. A previously attached task-local dynamic filter
    /// keeps its expression handle (also referenced by the probe plan), while
    /// its build-report accumulator is reset. The caller must provide a fresh
    /// filter expression/probe plan for each independent task.
    /// Residual filters also remain consumer-local, so compatible INNER joins
    /// may use different predicates with the same prepared data.
    /// The resulting join plan retains the build lease. A caller retaining a
    /// dynamic-filter expression beyond that plan must retain a prepared-build
    /// lease alongside it, because membership filters can reference build data.
    /// Attach after child-rewriting physical optimizations. The unused left
    /// subtree is replaced with an empty schema placeholder so plan resets
    /// preserve it. Probe-only rewrites must retain the attached join's `left()`.
    /// Replacing that child or changing to incompatible join keys fails; other
    /// incompatible join-mode/type changes fail validation in `build`.
    pub fn with_prepared_build(mut self, prepared: Arc<PreparedHashJoinBuild>) -> Self {
        // This removes the ignored child's ordering/equivalences and preserves
        // its identity through plan resets.
        self.exec.left = Arc::new(crate::empty::EmptyExec::new(self.exec.left.schema()));
        self.reset_prepared_runtime_state();
        self.exec.prepared_build = Some(prepared);
        self.preserve_properties = false;
        self
    }
}

/// Validate the narrow prepared-build contract and return ordered build key
/// indices. Reads schemas/expressions only; invalid columns are rejected before
/// any array access. Matching probe types preserve hashing and equality semantics.
/// INNER joins need no shared build-match bitmap. Residual predicates are
/// evaluated by each consumer after hash lookup.
fn prepared_key_indices(join: &HashJoinExec) -> Result<Vec<usize>> {
    if join.join_type != JoinType::Inner
        || join.mode != PartitionMode::CollectLeft
        || join.null_aware
    {
        return plan_err!("Prepared hash-join builds require a CollectLeft INNER join");
    }
    let left = join.left.schema();
    let right = join.right.schema();
    if left.fields().iter().any(|field| {
        let ty = field.data_type();
        ty.primitive_width().is_none()
            && !matches!(
                ty,
                DataType::Null
                    | DataType::Boolean
                    | DataType::FixedSizeBinary(_)
                    | DataType::Utf8
            )
    }) {
        return plan_err!(
            "Prepared hash-join builds require fixed-width or UTF-8 build columns"
        );
    }
    join.on
        .iter()
        .map(|(l, r)| {
            let Some(l) = l.downcast_ref::<Column>() else {
                return plan_err!("Prepared hash-join builds require direct column keys");
            };
            let Some(r) = r.downcast_ref::<Column>() else {
                return plan_err!("Prepared hash-join builds require direct column keys");
            };
            let Some(left_type) = left.fields().get(l.index()).map(|f| f.data_type())
            else {
                return plan_err!("Prepared hash-join build key is out of bounds");
            };
            let Some(right_type) = right.fields().get(r.index()).map(|f| f.data_type())
            else {
                return plan_err!("Prepared hash-join probe key is out of bounds");
            };
            if left_type != right_type {
                return plan_err!("Prepared hash-join key types must match");
            }
            Ok(l.index())
        })
        .collect()
}

#[cfg(test)]
mod tests;
