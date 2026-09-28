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

//! Independent INNER joins over the same in-memory build snapshot.
//!
//! SQL planning does not expose prepared builds, so these benchmarks exercise
//! the public execution API. Cases vary build size, integer/string keys, and
//! concurrent consumers. Every consumer probes 4096 rows with 50% matches.
//! `cold` includes preparation; `warm` retains a build across iterations.
//! Input generation is excluded, but plan construction and draining outputs are
//! timed. This does not measure Comet decoding, its cache, or its single-flight
//! wait. Sparse integer keys avoid perfect-hash selection.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinType, Result};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{
    HashJoinExec, HashJoinExecBuilder, PartitionMode, PreparedHashJoinBuild,
};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::test::TestMemoryExec;
use futures::TryStreamExt;
use tokio::runtime::Builder;
use tokio::sync::Barrier;

const PROBE_ROWS: usize = 4096;

struct Workload {
    build: Vec<RecordBatch>,
    probe: RecordBatch,
    expected_sum: i64,
}

impl Workload {
    fn new(build_rows: usize, strings: bool) -> Self {
        let batch = |keys: Vec<usize>| {
            let array: ArrayRef = if strings {
                Arc::new(StringArray::from_iter_values(
                    keys.iter().map(|key| format!("key-{key:08}")),
                ))
            } else {
                Arc::new(Int64Array::from_iter_values(
                    keys.iter().map(|key| (key * 17) as i64),
                ))
            };
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", array.data_type().clone(), false),
                    Field::new("payload", DataType::Int64, false),
                ])),
                vec![
                    array,
                    Arc::new(Int64Array::from_iter_values(
                        keys.iter().map(|key| *key as i64),
                    )),
                ],
            )
            .unwrap()
        };
        let build = batch((0..build_rows).collect());
        let probe = batch(
            (0..PROBE_ROWS)
                .map(|i| (i / 2) % build_rows + (i % 2) * build_rows)
                .collect(),
        );
        Self {
            build: (0..build_rows)
                .step_by(8192)
                .map(|offset| build.slice(offset, (build_rows - offset).min(8192)))
                .collect(),
            probe,
            expected_sum: (0..PROBE_ROWS / 2).map(|i| (i % build_rows) as i64).sum(),
        }
    }

    fn join(&self) -> Result<HashJoinExec> {
        let schema = self.probe.schema();
        HashJoinExecBuilder::new(
            TestMemoryExec::try_new_exec(
                std::slice::from_ref(&self.build),
                Arc::clone(&schema),
                None,
            )?,
            TestMemoryExec::try_new_exec(
                &[vec![self.probe.clone()]],
                Arc::clone(&schema),
                None,
            )?,
            vec![(col("key", &schema)?, col("key", &schema)?)],
            JoinType::Inner,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .build()
    }

    async fn prepare(
        &self,
        join: &HashJoinExec,
        context: &TaskContext,
    ) -> Result<Arc<PreparedHashJoinBuild>> {
        join.prepare_build(
            Box::pin(MemoryStream::try_new(
                self.build.clone(),
                self.probe.schema(),
                None,
            )?),
            Arc::clone(context.memory_pool()),
            Arc::clone(context.session_config().options()),
        )
        .await
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Rebuild,
    Cold,
    Warm,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Rebuild => "rebuild",
            Self::Cold => "cold",
            Self::Warm => "warm",
        }
    }
}

async fn run(
    workload: &Workload,
    consumers: usize,
    mode: Mode,
    warm: Option<Arc<PreparedHashJoinBuild>>,
    context: Arc<TaskContext>,
) -> Result<(usize, i64)> {
    let joins = (0..consumers)
        .map(|_| workload.join())
        .collect::<Result<Vec<_>>>()?;
    let prepared = match mode {
        Mode::Rebuild => None,
        Mode::Cold => Some(workload.prepare(&joins[0], &context).await?),
        Mode::Warm => warm,
    };
    let start = Arc::new(Barrier::new(consumers));
    let tasks = joins
        .into_iter()
        .map(|join| {
            let join = match &prepared {
                Some(build) => join
                    .builder()
                    .with_prepared_build(Arc::clone(build))
                    .build()?,
                None => join,
            };
            let context = Arc::clone(&context);
            let start = Arc::clone(&start);
            Ok(SpawnedTask::spawn(async move {
                start.wait().await;
                let mut stream = join.execute(0, context)?;
                let mut result = (0, 0);
                while let Some(batch) = stream.try_next().await? {
                    result.0 += batch.num_rows();
                    let payload = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    result.1 += payload.values().iter().sum::<i64>();
                }
                Ok::<_, datafusion_common::DataFusionError>(result)
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut result = (0, 0);
    for task in tasks {
        let (rows, sum) = task.await.unwrap()?;
        result.0 += rows;
        result.1 += sum;
    }
    Ok(result)
}

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("prepared_hash_join");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    for (key_type, strings) in [("int64", false), ("utf8", true)] {
        for rows in [64, 65536] {
            let workload = Workload::new(rows, strings);
            for consumers in [1, 4] {
                for mode in [Mode::Rebuild, Mode::Cold, Mode::Warm] {
                    let context = Arc::new(TaskContext::default());
                    let warm = matches!(mode, Mode::Warm).then(|| {
                        runtime
                            .block_on(
                                workload.prepare(&workload.join().unwrap(), &context),
                            )
                            .unwrap()
                    });
                    let output = runtime
                        .block_on(run(
                            &workload,
                            consumers,
                            mode,
                            warm.clone(),
                            Arc::clone(&context),
                        ))
                        .unwrap();
                    assert_eq!(
                        output,
                        (
                            consumers * PROBE_ROWS / 2,
                            consumers as i64 * workload.expected_sum
                        )
                    );
                    let case = format!("{key_type}/{rows}/{consumers}_consumers");
                    group.bench_function(BenchmarkId::new(mode.name(), case), |b| {
                        b.iter(|| {
                            black_box(
                                runtime
                                    .block_on(run(
                                        &workload,
                                        consumers,
                                        mode,
                                        warm.clone(),
                                        Arc::clone(&context),
                                    ))
                                    .unwrap(),
                            )
                        });
                    });
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
