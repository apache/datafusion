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

use super::tests::{
    build_merge_builder, build_spill_manager, make_sorted_spill_file, test_schema,
};
use super::*;
use arrow::array::{AsArray, Int64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::Int64Type;
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool,
};
use datafusion_execution::runtime_env::RuntimeEnv;
use std::fmt::Display;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy)]
enum PressurePoint {
    ReplayHeadroom,
    IntermediateEof,
}

/// Schedule another consumer immediately when the merge returns memory to the
/// shared pool. Taking only available bytes models an ordinary competing
/// reservation while making both release boundaries deterministic.
#[derive(Debug)]
struct CompetingMemoryPool {
    inner: Arc<dyn MemoryPool>,
    contender: MemoryReservation,
    capacity: usize,
    contender_target: usize,
    pressure_point: PressurePoint,
    active: Mutex<bool>,
}

impl CompetingMemoryPool {
    fn new(
        capacity: usize,
        contender_target: usize,
        pressure_point: PressurePoint,
    ) -> Self {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(capacity));
        let contender = MemoryConsumer::new("competing partition").register(&inner);
        Self {
            inner,
            contender,
            capacity,
            contender_target,
            pressure_point,
            active: Mutex::new(false),
        }
    }
}

impl Display for CompetingMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompetingMemoryPool({})", self.inner)
    }
}

impl MemoryPool for CompetingMemoryPool {
    fn name(&self) -> &str {
        "competing"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        if shrink == 0 {
            return;
        }
        let mut active = self.active.lock().unwrap();
        *active |= match self.pressure_point {
            PressurePoint::ReplayHeadroom => reservation.size() > 0,
            PressurePoint::IntermediateEof => reservation.size() == 0,
        };
        if *active {
            let additional = (self.contender_target - self.contender.size())
                .min(self.capacity - self.inner.reserved());
            self.contender.try_grow(additional).unwrap();
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[rstest::rstest]
#[case::headroom_release(PressurePoint::ReplayHeadroom)]
#[case::intermediate_eof(PressurePoint::IntermediateEof)]
#[tokio::test]
async fn intermediate_sizing_does_not_add_rewrites_under_contention(
    #[case] pressure_point: PressurePoint,
    #[values(2, 3)] final_fan_in: usize,
    #[values(8, 9)] runs: usize,
) -> Result<()> {
    const ROWS: usize = 128;
    let mut rewrites = Vec::new();
    for size_intermediate_merges in [false, true] {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spilled_rows = spill_manager.metrics.spilled_rows.clone();
        let files: Vec<_> = (0..runs)
            .map(|run| {
                make_sorted_spill_file(
                    &spill_manager,
                    &schema,
                    (0..ROWS).map(|row| (row * runs + run) as i64).collect(),
                )
            })
            .collect();
        let batch_memory = files[0].max_record_batch_memory;
        // Seven runs initially fit with read-ahead and equal replay headroom.
        // The competing partition takes released bytes until only two or three
        // runs would fit a fresh admission. Both runs use the same release hook.
        let competing_pool = Arc::new(CompetingMemoryPool::new(
            8 * 7 * batch_memory,
            8 * (7 - final_fan_in) * batch_memory,
            pressure_point,
        ));
        let pool = Arc::clone(&competing_pool) as Arc<dyn MemoryPool>;
        let builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), files, &pool, ROWS)
                .with_replay_headroom(true)
                .with_intermediate_merge_sizing(size_intermediate_merges.then_some(ROWS));
        let mut stream = builder.create_spillable_merge_stream();
        let first = stream.try_next().await?.expect("nonempty merge");
        if matches!(pressure_point, PressurePoint::IntermediateEof)
            && size_intermediate_merges
            && runs == 8
        {
            // Sizing these eight runs retains the admitted buffers across the
            // intermediate EOF. The contender cannot take them before the final
            // merge is admitted, so this boundary must not have fired yet.
            assert_eq!(competing_pool.contender.size(), 0);
        } else {
            assert_eq!(
                competing_pool.contender.size(),
                competing_pool.contender_target
            );
        }
        let mut batches = vec![first];
        batches.extend(stream.try_collect::<Vec<_>>().await?);
        let merged = concat_batches(&schema, &batches)?;
        let expected = Int64Array::from_iter_values(0..(runs * ROWS) as i64);
        assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);
        rewrites.push(spilled_rows.value() - runs * ROWS);
        assert_eq!(
            competing_pool.contender.size(),
            competing_pool.contender_target
        );
        assert_eq!(pool.reserved(), competing_pool.contender.size());
        competing_pool.contender.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        assert_eq!(env.disk_manager.used_disk_space(), 0);
    }
    assert_eq!(
        rewrites[0],
        if runs == 9 && final_fan_in == 2 { 9 } else { 7 } * ROWS
    );
    assert!(
        rewrites[1] <= rewrites[0],
        "sizing rewrote {} rows versus {} without sizing under {pressure_point:?}",
        rewrites[1],
        rewrites[0],
    );
    if runs == 8 {
        assert_eq!(rewrites[1], 6 * ROWS);
    }
    Ok(())
}

#[tokio::test]
async fn sized_merge_retains_reservation_until_builder_drop() -> Result<()> {
    const RUNS: usize = 8;
    const ROWS: usize = 128;
    let env = Arc::new(RuntimeEnv::default());
    let schema = test_schema();
    let spill_manager = build_spill_manager(&env, &schema);
    let files: Vec<_> = (0..RUNS)
        .map(|run| {
            make_sorted_spill_file(
                &spill_manager,
                &schema,
                (0..ROWS).map(|row| (row * RUNS + run) as i64).collect(),
            )
        })
        .collect();
    let batch_memory = files[0].max_record_batch_memory;
    let competing_pool = Arc::new(CompetingMemoryPool::new(
        8 * 7 * batch_memory,
        8 * 5 * batch_memory,
        PressurePoint::ReplayHeadroom,
    ));
    let pool = Arc::clone(&competing_pool) as Arc<dyn MemoryPool>;
    let mut builder =
        build_merge_builder(spill_manager, Arc::clone(&schema), files, &pool, ROWS)
            .with_replay_headroom(true)
            .with_intermediate_merge_sizing(Some(ROWS));
    let MergeStep::Stream { stream, .. } =
        builder.merge_sorted_runs_within_mem_limit(false)?
    else {
        panic!("six inputs should be selected for the intermediate merge");
    };
    assert_eq!(builder.sorted_spill_files.len(), 2);
    let retained = 4 * 7 * batch_memory;
    assert_eq!(builder.reservation.size(), retained);
    assert_eq!(pool.reserved(), retained + competing_pool.contender.size());

    // The intermediate stream may finish or be cancelled before the builder
    // starts final admission. Its drop must leave the grant with the builder.
    drop(stream);
    assert_eq!(builder.reservation.size(), retained);
    assert_eq!(pool.reserved(), retained + competing_pool.contender.size());
    drop(builder);
    assert_eq!(pool.reserved(), competing_pool.contender.size());
    competing_pool.contender.free();
    assert_eq!(pool.reserved(), 0);
    assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
    assert_eq!(env.disk_manager.used_disk_space(), 0);
    Ok(())
}
