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

use super::memory_pool::PeakRecordingPool;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::{DATAFUSION_VERSION, error::Result};
use datafusion_common::utils::get_available_parallelism;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

fn serialize_start_time<S>(start_time: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_u64(
        start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("current time is later than UNIX_EPOCH")
            .as_secs(),
    )
}
fn serialize_elapsed<S>(elapsed: &Duration, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ms = elapsed.as_secs_f64() * 1000.0;
    ser.serialize_f64(ms)
}
#[derive(Debug, Serialize)]
pub struct RunContext {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// DataFusion crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Start time
    #[serde(serialize_with = "serialize_start_time")]
    pub start_time: SystemTime,
    /// CLI arguments
    pub arguments: Vec<String>,
}

impl Default for RunContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RunContext {
    pub fn new() -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: get_available_parallelism(),
            start_time: SystemTime::now(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
        }
    }
}

/// A single iteration of a benchmark query
#[derive(Debug, Serialize)]
struct QueryIter {
    #[serde(serialize_with = "serialize_elapsed")]
    elapsed: Duration,
    row_count: usize,
}
/// A single benchmark case
#[derive(Debug, Serialize)]
pub struct BenchQuery {
    query: String,
    iterations: Vec<QueryIter>,
    #[serde(serialize_with = "serialize_start_time")]
    start_time: SystemTime,
    success: bool,
    /// Peak [`MemoryPool`] reservation observed while running this query, in
    /// bytes. Recorded for failed queries too, since a query that ran out of
    /// memory is one whose peak is worth seeing.
    ///
    /// Each iteration is measured on its own and the smallest reading wins, so
    /// this is the peak of the least-contended pass rather than of the whole
    /// run — see [`BenchmarkRun::write_iter`]. A failed query reports what it
    /// held when it gave up instead, having completed no iteration.
    ///
    /// `None` (and omitted from the JSON) only when the benchmark ran without a
    /// memory limit, since there is then no pool to record.
    ///
    /// [`MemoryPool`]: datafusion::execution::memory_pool::MemoryPool
    #[serde(skip_serializing_if = "Option::is_none")]
    pool_peak_bytes: Option<usize>,
}
/// Internal representation of a single benchmark query iteration result.
pub struct QueryResult {
    pub elapsed: Duration,
    pub row_count: usize,
    /// Peak [`MemoryPool`] reservation for *this* iteration alone, from
    /// [`take_iteration_peak`]. `None` when the benchmark ran without a
    /// recording pool.
    ///
    /// [`MemoryPool`]: datafusion::execution::memory_pool::MemoryPool
    pub pool_peak_bytes: Option<usize>,
}

/// Read the peak reservation accumulated since the last call and start a fresh
/// reading, so each iteration is measured on its own.
///
/// Benchmarks run every iteration of a query before reporting any of them, so
/// the reading has to be taken here, at the end of an iteration, rather than
/// where the results are later written out — by then the recorder holds the
/// high-water mark of the whole set.
///
/// Returns `None` when the benchmark is running without a memory limit, since
/// there is then no recording pool in front of the runtime's.
pub fn take_iteration_peak(pool: &Arc<dyn MemoryPool>) -> Option<usize> {
    PeakRecordingPool::from_pool(pool.as_ref()).map(|recorder| {
        let peak = recorder.peak_reserved();
        recorder.reset_peak();
        peak
    })
}
/// collects benchmark run data and then serializes it at the end
pub struct BenchmarkRun {
    context: RunContext,
    queries: Vec<BenchQuery>,
    current_case: Option<usize>,
    /// The pool queries run against, when one was handed over with
    /// [`BenchmarkRun::set_memory_pool`]. Only read through
    /// [`BenchmarkRun::peak_recorder`].
    memory_pool: Option<Arc<dyn MemoryPool>>,
}

impl Default for BenchmarkRun {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkRun {
    // create new
    pub fn new() -> Self {
        Self {
            context: RunContext::new(),
            queries: vec![],
            current_case: None,
            memory_pool: None,
        }
    }

    /// Report the peak reservation of `memory_pool` alongside each query.
    ///
    /// Call this with the pool of the [`RuntimeEnv`] the queries run against.
    /// Has no effect unless a [`PeakRecordingPool`] is installed, which
    /// [`CommonOpt::runtime_env_builder`] does whenever a memory limit is
    /// configured; without one `pool_peak_bytes` is omitted from the results.
    ///
    /// Benchmarks that build a runtime per query should call this each time, so
    /// each query reports against the pool it actually ran on.
    ///
    /// [`RuntimeEnv`]: datafusion::execution::runtime_env::RuntimeEnv
    /// [`CommonOpt::runtime_env_builder`]: super::CommonOpt::runtime_env_builder
    pub fn set_memory_pool(&mut self, memory_pool: &Arc<dyn MemoryPool>) {
        self.memory_pool = Some(Arc::clone(memory_pool));
    }

    /// The recorder in front of the pool set by [`Self::set_memory_pool`].
    fn peak_recorder(&self) -> Option<&PeakRecordingPool> {
        PeakRecordingPool::from_pool(self.memory_pool.as_deref()?)
    }

    /// begin a new case. iterations added after this will be included in the new case
    pub fn start_new_case(&mut self, id: &str) {
        // Give this query its own memory pool reading rather than inheriting
        // the high-water mark of the queries that ran before it.
        if let Some(recorder) = self.peak_recorder() {
            recorder.reset_peak();
        }
        self.queries.push(BenchQuery {
            query: id.to_owned(),
            iterations: vec![],
            start_time: SystemTime::now(),
            success: true,
            pool_peak_bytes: None,
        });
        if let Some(c) = self.current_case.as_mut() {
            *c += 1;
        } else {
            self.current_case = Some(0);
        }
    }
    /// Write a new iteration to the current case.
    ///
    /// `pool_peak_bytes` is this iteration's own reading, from
    /// [`take_iteration_peak`]. What reaches the results JSON is the
    /// *smallest* across the query's iterations: a peak reservation is a max
    /// over the operators holding memory at one moment, so it moves with how
    /// the runtime happened to interleave partitions on that pass. Reducing
    /// with `min` keeps the reading reproducible, where taking the largest —
    /// as this did while the recorder ran unreset across every iteration —
    /// lets one unlucky pass set the number for the whole query.
    pub fn write_iter(
        &mut self,
        elapsed: Duration,
        row_count: usize,
        pool_peak_bytes: Option<usize>,
    ) {
        if let Some(idx) = self.current_case {
            self.queries[idx]
                .iterations
                .push(QueryIter { elapsed, row_count });
            self.queries[idx].pool_peak_bytes =
                match (self.queries[idx].pool_peak_bytes, pool_peak_bytes) {
                    (Some(seen), Some(peak)) => Some(seen.min(peak)),
                    (None, peak) => peak,
                    (seen, None) => seen,
                };
        } else {
            panic!("no cases existed yet");
        }
    }

    /// Print the names of failed queries, if any
    pub fn maybe_print_failures(&self) {
        let failed_queries: Vec<&str> = self
            .queries
            .iter()
            .filter_map(|q| (!q.success).then_some(q.query.as_str()))
            .collect();

        if !failed_queries.is_empty() {
            println!("Failed Queries: {}", failed_queries.join(", "));
        }
    }

    /// Mark current query
    pub fn mark_failed(&mut self) {
        // A query that failed under a memory limit wrote no iteration, so this
        // is the only chance to record what it had reserved when it gave up.
        let pool_peak_bytes = self.peak_recorder().map(PeakRecordingPool::peak_reserved);
        if let Some(idx) = self.current_case {
            self.queries[idx].success = false;
            self.queries[idx].pool_peak_bytes = pool_peak_bytes;
        } else {
            unreachable!("Cannot mark failure: no current case");
        }
    }

    /// Stringify data into formatted json
    pub fn to_json(&self) -> String {
        let mut output = HashMap::<&str, Value>::new();
        output.insert("context", serde_json::to_value(&self.context).unwrap());
        output.insert("queries", serde_json::to_value(&self.queries).unwrap());
        serde_json::to_string_pretty(&output).unwrap()
    }

    /// Write data as json into output path if it exists.
    pub fn maybe_write_json(&self, maybe_path: Option<impl AsRef<Path>>) -> Result<()> {
        if let Some(path) = maybe_path {
            std::fs::write(path, self.to_json())?;
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};

    use super::*;

    fn recording_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(PeakRecordingPool::new(Arc::new(GreedyMemoryPool::new(
            limit,
        ))))
    }

    /// One iteration of a query: reserve, release, then take the reading —
    /// the order a benchmark's iteration loop runs in.
    fn run_iteration(run: &mut BenchmarkRun, pool: &Arc<dyn MemoryPool>, grow: usize) {
        let reservation = MemoryConsumer::new("q").register(pool);
        reservation.try_grow(grow).unwrap();
        drop(reservation);
        let peak = take_iteration_peak(pool);
        run.write_iter(Duration::from_millis(1), 1, peak);
    }

    #[test]
    fn each_case_reports_its_own_peak() {
        let pool = recording_pool(1024);
        let mut run = BenchmarkRun::new();
        run.set_memory_pool(&pool);

        run.start_new_case("q1");
        run_iteration(&mut run, &pool, 600);

        // The second case must not inherit the first case's high-water mark.
        run.start_new_case("q2");
        run_iteration(&mut run, &pool, 100);

        assert_eq!(run.queries[0].pool_peak_bytes, Some(600));
        assert_eq!(run.queries[1].pool_peak_bytes, Some(100));
    }

    #[test]
    fn the_quietest_iteration_sets_the_peak() {
        let pool = recording_pool(1024);
        let mut run = BenchmarkRun::new();
        run.set_memory_pool(&pool);

        run.start_new_case("q1");
        // Three passes over the same query. The 900 is what a pass looks like
        // when more operators happened to hold memory at the same moment.
        for grow in [300, 900, 400] {
            run_iteration(&mut run, &pool, grow);
        }

        // Not 900: one unlucky pass must not set the number for the query.
        assert_eq!(run.queries[0].pool_peak_bytes, Some(300));
    }

    #[test]
    fn an_iteration_does_not_inherit_the_previous_peak() {
        let pool = recording_pool(1024);
        let mut run = BenchmarkRun::new();
        run.set_memory_pool(&pool);

        run.start_new_case("q1");
        run_iteration(&mut run, &pool, 800);
        // Before per-iteration readings the recorder still held 800 here, so
        // this pass reported 800 rather than its own 100.
        run_iteration(&mut run, &pool, 100);

        assert_eq!(run.queries[0].pool_peak_bytes, Some(100));
    }

    #[test]
    fn an_iteration_reading_covers_only_that_iteration() {
        let pool = recording_pool(1024);
        let setup = MemoryConsumer::new("setup").register(&pool);
        setup.try_grow(700).unwrap();

        // Bytes held when a reading is taken stay in it: `reset_peak` rebases
        // on what is reserved, not on zero, so data the benchmark loaded up
        // front counts for every iteration that runs with it.
        assert_eq!(take_iteration_peak(&pool), Some(700));
        assert_eq!(take_iteration_peak(&pool), Some(700));

        // Releasing it does not pull the current reading down — a high-water
        // mark only falls at a reset, so the interval that began with those
        // bytes held still reports them...
        drop(setup);
        assert_eq!(take_iteration_peak(&pool), Some(700));
        // ...and only the interval after that is clear of them.
        assert_eq!(take_iteration_peak(&pool), Some(0));
    }

    #[test]
    fn a_later_pool_replaces_an_earlier_one() {
        let first = recording_pool(1024);
        let mut run = BenchmarkRun::new();
        run.set_memory_pool(&first);
        MemoryConsumer::new("q1")
            .register(&first)
            .try_grow(600)
            .unwrap();

        // Benchmarks that build a runtime per query hand over the new pool
        // before the next case; the reading follows it.
        let second = recording_pool(1024);
        run.set_memory_pool(&second);
        run.start_new_case("q2");
        MemoryConsumer::new("q2")
            .register(&second)
            .try_grow(100)
            .unwrap();
        let peak = take_iteration_peak(&second);
        run.write_iter(Duration::from_millis(1), 1, peak);

        assert_eq!(run.queries[0].pool_peak_bytes, Some(100));
    }

    #[test]
    fn a_failed_query_still_reports_its_peak() {
        let pool = recording_pool(1024);
        let mut run = BenchmarkRun::new();
        run.set_memory_pool(&pool);

        run.start_new_case("q1");
        let reservation = MemoryConsumer::new("q1").register(&pool);
        reservation.try_grow(600).unwrap();
        // No `write_iter`: the query failed before completing an iteration.
        run.mark_failed();

        assert_eq!(run.queries[0].pool_peak_bytes, Some(600));
    }

    #[test]
    fn the_peak_is_omitted_without_a_recording_pool() {
        let mut run = BenchmarkRun::new();
        run.start_new_case("q1");
        run.write_iter(Duration::from_millis(1), 1, None);

        assert_eq!(run.queries[0].pool_peak_bytes, None);
        assert!(!run.to_json().contains("pool_peak_bytes"));
    }
}
