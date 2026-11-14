// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Build preparation and shared data for regular hash joins.
//!
//! Collection is shared across probe partitions in `CollectLeft` mode. Before
//! probing, each partition also waits for the join's dynamic filter to be ready.
//! The prepared result contains only the data and coordination regular joins need.

use std::sync::Arc;

use arrow::array::{BooleanBufferBuilder, UInt64Array};
use arrow::buffer::BooleanBuffer;
use datafusion_common::Result;
use futures::future::poll_fn;
use parking_lot::Mutex;

use super::exec::RegularBuildData;
use crate::joins::PartitionMode;
use crate::joins::hash_join::shared_bounds::{
    PartitionBounds, PartitionBuildData, SharedBuildAccumulator,
};
use crate::joins::utils::{BuildProbeJoinMetrics, OnceFut};

/// [`RegularBuildData`] plus cross-partition coordination. Only one partition is
/// responsible for final build-side unmatched output for Left/Full joins.
pub(super) struct JoinBuildState {
    unmatched: Option<Mutex<UnmatchedRows>>,
    // Keep the build lease last: its reservation also covers the bitmap.
    pub(super) data: Arc<RegularBuildData>,
}

struct UnmatchedRows {
    visited: BooleanBufferBuilder,
    remaining_probes: usize,
}

impl JoinBuildState {
    pub(super) fn new(
        data: Arc<RegularBuildData>,
        visited: Option<BooleanBufferBuilder>,
        probe_partitions: usize,
    ) -> Self {
        Self {
            unmatched: visited.map(|visited| {
                Mutex::new(UnmatchedRows {
                    visited,
                    remaining_probes: probe_partitions,
                })
            }),
            data,
        }
    }

    /// Record matches after the ON filter has accepted them.
    pub(super) fn record_matches(&self, indices: &UInt64Array) {
        if let Some(unmatched) = &self.unmatched {
            let mut unmatched = unmatched.lock();
            for index in indices.iter().flatten() {
                unmatched.visited.set_bit(index as usize, true);
            }
        }
    }

    /// Report actual probe EOF; a fetch cutoff must not call this.
    pub(super) fn finish_probe(&self) -> Option<BooleanBuffer> {
        let mut unmatched = self.unmatched.as_ref()?.lock();
        // Counting and snapshotting under the same lock ensures the last probe
        // sees every sibling's matches and is the only one to emit unmatched rows.
        unmatched.remaining_probes -= 1;
        (unmatched.remaining_probes == 0).then(|| unmatched.visited.finish_cloned())
    }
}

/// Collecting the build-side data of a hash join.
///
/// It handles coordination/synchronization among all parallel partitions for:
/// - Input batch collection
/// - Dynamic filter preparation
pub(super) struct HashJoinBuild {
    partition: usize,
    mode: PartitionMode,

    // ---------------------------------------------------
    // Shared structures for coordination among partitions.
    // ---------------------------------------------------
    /// Coordination for the dynamic filter.
    build_accumulator: Option<Arc<SharedBuildAccumulator>>,
    /// Coordination for buffered input batches.
    left_fut: OnceFut<JoinBuildState>,

    metrics: BuildProbeJoinMetrics,
}

impl HashJoinBuild {
    pub(super) fn new(
        partition: usize,
        mode: PartitionMode,
        left_fut: OnceFut<JoinBuildState>,
        build_accumulator: Option<Arc<SharedBuildAccumulator>>,
        metrics: BuildProbeJoinMetrics,
    ) -> Self {
        Self {
            partition,
            mode,
            build_accumulator,
            left_fut,
            metrics,
        }
    }

    /// Prepare the build-side data and handle coordination.
    ///
    /// It handles coordination for build-side data preparation and dynamic filters.
    ///
    /// Example of coordination for build-side data (coordination for the dynamic
    /// filter is similar; after all partitions are done, it can publish the dynamic
    /// filter to pre-filter the probe side):
    ///
    /// ```text
    /// With shared build-side configuration:
    /// - partition0: call `prepare()`, and execute and cache all build-side data
    /// - partition1~k: call `prepare()` for synchronization, wait for partition 0 to finish
    ///   and reuse the shared build-side data
    /// ```
    pub(super) async fn prepare(mut self) -> Result<Arc<JoinBuildState>> {
        let build = poll_fn(|cx| {
            // Count active polling time, excluding time suspended on input.
            let _timer = self.metrics.build_time.timer();
            self.left_fut.get_shared(cx)
        })
        .await?;

        if let Some(accumulator) = &self.build_accumulator {
            let data = &build.data;
            let pushdown = data.membership.clone();
            let bounds = data
                .bounds
                .clone()
                .unwrap_or_else(|| PartitionBounds::new(vec![]));
            // Dictionary keys can point at NULL values without physical NULL keys.
            let keys_have_null = data
                .values
                .iter()
                .any(|array| array.logical_null_count() > 0);
            let report = match self.mode {
                PartitionMode::Partitioned => PartitionBuildData::Partitioned {
                    partition_id: self.partition,
                    pushdown,
                    bounds,
                    keys_have_null,
                },
                PartitionMode::CollectLeft => PartitionBuildData::CollectLeft {
                    pushdown,
                    bounds,
                    keys_have_null,
                },
                PartitionMode::Auto => {
                    unreachable!("PartitionMode::Auto must be resolved before execution")
                }
            };

            // Handle coordination for the dynamic filter.
            accumulator.report_build_data(report).await?;
        }
        Ok(build)
    }
}

impl Drop for HashJoinBuild {
    fn drop(&mut self) {
        if self.mode == PartitionMode::Partitioned
            && let Some(accumulator) = &self.build_accumulator
        {
            // A LIMIT may drop a stream before it reports. Unblock siblings,
            // keeping that partition's filter permissive: unknown is not empty.
            // The accumulator ignores cancellation after a report, including a
            // report whose future was dropped while waiting for other partitions.
            accumulator.report_canceled_partition(self.partition);
        }
    }
}
