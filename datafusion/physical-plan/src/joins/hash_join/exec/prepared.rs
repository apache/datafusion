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
use crate::spill::spill_manager::GetSlicedSize;
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
            null_aware_scope_map: None,
            null_value_build_rows: None,
            visited_indices_bitmap: Mutex::new(BooleanBufferBuilder::new(0)),
            null_indices_bitmap: Mutex::new(BooleanBufferBuilder::new(0)),
            probe_completion: ProbeCompletion::new(probe_threads),
            _probe_reservation: self.build.reservation.new_empty(),
            build_side_has_null: false,
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
            let probe_keys = join.on.iter().map(|(_, right)| right);
            if !filter.filter.children().into_iter().eq(probe_keys) {
                return plan_err!(
                    "Prepared hash-join dynamic filter keys do not match probe keys"
                );
            }
        }
        Ok(())
    }
}

impl HashJoinExec {
    /// Prepare the supplied build snapshot using an embedding executor's durable pool.
    ///
    /// The caller owns snapshot identity: this consumes `input`, never `self.left`.
    /// Schema and key compatibility cannot establish that two inputs contain the
    /// same data. Attaching the wrong snapshot can silently change query results.
    ///
    /// # Caller contract
    ///
    /// * Input buffers must remain valid independently of producer task cleanup.
    ///   Keep producer allocations charged until the stream releases them.
    /// * Supply a pool whose lifetime covers every consumer. It accounts for
    ///   retained input, hash storage, and preparation's copy/scratch buffers.
    /// * Coordinate concurrent preparation, publication, and invalidation in the
    ///   embedding executor. This method does not provide a cache.
    ///
    /// Errors and cancellation release unfinished work. `config` controls
    /// perfect-map and dynamic-filter choices. Byte keys use hash membership
    /// to avoid copying payloads into range bounds and IN-list literals.
    /// Consumer-local dynamic-filter allocations are outside this reservation.
    ///
    /// # Example
    ///
    /// Given two independently planned, compatible joins over different probe
    /// inputs, a service can prepare one dimension snapshot for both consumers.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use datafusion_common::{Result, config::ConfigOptions};
    /// # use datafusion_execution::memory_pool::MemoryPool;
    /// # use datafusion_physical_plan::SendableRecordBatchStream;
    /// # use datafusion_physical_plan::joins::HashJoinExec;
    /// # async fn example(first_join: HashJoinExec, second_join: HashJoinExec,
    /// #     dimension_snapshot: SendableRecordBatchStream,
    /// #     durable_pool: Arc<dyn MemoryPool>, config: Arc<ConfigOptions>) -> Result<()> {
    /// let prepared = first_join
    ///     .prepare_build(dimension_snapshot, durable_pool, config)
    ///     .await?;
    /// let first = first_join.builder()
    ///     .with_prepared_build(Arc::clone(&prepared))
    ///     .build()?;
    /// let second = second_join.builder()
    ///     .with_prepared_build(prepared)
    ///     .build()?;
    /// # let _ = (first, second);
    /// # Ok(())
    /// # }
    /// ```
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
            BuildMode::Prepared,
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
pub(super) fn prepared_copy_bytes(batches: &[RecordBatch]) -> Result<usize> {
    let mut bytes = 0;
    for batch in batches {
        // Concat can materialize validity for an all-valid input when another input
        // contains nulls. Arrow's slice measurement only counts existing bitmaps.
        let missing_validity = batch
            .columns()
            .iter()
            .filter(|array| array.nulls().is_none())
            .count();
        bytes +=
            batch.get_sliced_size()? + batch.num_rows().div_ceil(8) * missing_validity;
    }
    // These are flat arrays: allow one alignment unit per output buffer,
    // including potential validity. Concat allocates each buffer only once.
    let buffers = batches[0]
        .columns()
        .iter()
        .map(|array| array.to_data().buffers().len() + 1)
        .sum::<usize>();
    Ok(bytes + buffers * 64)
}

impl HashJoinExecBuilder {
    /// Attach a fully prepared build to a fresh, compatible join execution.
    ///
    /// [`Self::build`] checks schema, keys, null equality, and supported join modes.
    /// Attaching resets execution state and replaces the unused left subtree with
    /// an empty schema placeholder. Residual predicates remain consumer-local.
    ///
    /// # Caller contract
    ///
    /// * Choose the correct build snapshot; compatibility checks do not verify
    ///   input identity. See [`HashJoinExec::prepare_build`].
    /// * Supply fresh dynamic-filter expressions and probe plans for independent
    ///   executions. An existing expression handle is preserved for its probe
    ///   consumers, while the build-report accumulator is reset.
    /// * Retain a prepared-build lease if a dynamic-filter expression outlives
    ///   this plan. Its array/map references preserve allocation lifetime, but
    ///   the lease is what preserves the corresponding memory charge.
    /// * Attach after physical optimizations that rewrite the build child.
    ///   Probe-only rewrites must preserve the attached join's `left()`; replacing
    ///   it or making incompatible key, mode, or join-type changes fails.
    pub fn with_prepared_build(mut self, prepared: Arc<PreparedHashJoinBuild>) -> Self {
        // This removes the ignored child's ordering/equivalences and preserves
        // its identity through plan resets.
        self.exec.left = Arc::new(crate::empty::EmptyExec::new(self.exec.left.schema()));
        self.exec.prepared_build = Some(prepared);
        self.preserve_properties = false;
        self
    }
}

/// Validate eligibility and return build-key indices in join-key order.
/// INNER joins need no shared build-match bitmap.
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
            if l.data_type(&left)? != r.data_type(&right)? {
                return plan_err!("Prepared hash-join key types must match");
            }
            Ok(l.index())
        })
        .collect()
}

#[cfg(test)]
mod tests;
