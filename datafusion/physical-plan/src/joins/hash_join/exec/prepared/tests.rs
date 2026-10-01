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

//! Regressions for immutable preparation, task isolation and buffer ownership.

use super::*;
use crate::{
    common,
    display::DisplayableExecutionPlan,
    empty::EmptyExec,
    filter::FilterExec,
    memory::MemoryStream,
    sorts::sort::SortExec,
    statistics::{StatisticsArgs, StatisticsContext},
    stream::RecordBatchStreamAdapter,
    test::TestMemoryExec,
};
use arrow::array::{
    BooleanArray, FixedSizeBinaryArray, Int64Array, NullArray, StringArray,
    StringViewArray, StructArray,
};
use arrow::buffer::Buffer;
use arrow::compute::kernels::sort::SortOptions;
use arrow_schema::Field;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::{DataFusionError, assert_batches_eq};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use futures::{StreamExt, stream};

fn batch(keys: Vec<Option<i64>>) -> RecordBatch {
    let rows = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("payload", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| i as i64 + 10),
            )),
        ],
    )
    .unwrap()
}

fn join(build_schema: SchemaRef, probe: RecordBatch) -> Result<HashJoinExec> {
    let right =
        TestMemoryExec::try_new_exec(&[vec![probe.clone()]], probe.schema(), None)?;
    HashJoinExecBuilder::new(
        Arc::new(EmptyExec::new(build_schema)),
        right,
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )],
        JoinType::Inner,
    )
    .with_partition_mode(PartitionMode::CollectLeft)
    .build()
}

fn input(batches: Vec<RecordBatch>, schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(MemoryStream::try_new(batches, schema, None).unwrap())
}

async fn prepare(
    join: &HashJoinExec,
    batches: Vec<RecordBatch>,
    pool: Arc<dyn MemoryPool>,
) -> Result<Arc<PreparedHashJoinBuild>> {
    join.prepare_build(
        input(batches, join.left().schema()),
        pool,
        Arc::new(ConfigOptions::default()),
    )
    .await
}

fn buffer_addresses(arrays: &[ArrayRef]) -> Vec<usize> {
    arrays
        .iter()
        .flat_map(|array| {
            let data = array.to_data();
            data.buffers()
                .iter()
                .chain(data.nulls().map(|nulls| nulls.inner().inner()))
                .filter(|buffer| buffer.capacity() != 0)
                .map(|buffer| buffer.data_ptr().as_ptr() as usize)
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn run(join: &HashJoinExec) -> Result<Vec<RecordBatch>> {
    let partitions = (0..join.properties().output_partitioning().partition_count()).map(
        |partition| async move {
            common::collect(join.execute(partition, Arc::new(TaskContext::default()))?)
                .await
        },
    );
    Ok(futures::future::try_join_all(partitions)
        .await?
        .into_iter()
        .flatten()
        .collect())
}

fn with_probe_filter(join: HashJoinExec) -> Result<HashJoinExec> {
    let filter = HashJoinExec::create_dynamic_filter(&join.on);
    let probe = Arc::new(FilterExec::try_new(
        Arc::clone(&filter) as _,
        Arc::clone(join.right()),
    )?);
    join.builder()
        .with_new_children(vec![Arc::clone(join.left()), probe])?
        .with_dynamic_filter(Some(HashJoinExecDynamicFilter {
            filter,
            build_accumulator: OnceLock::new(),
        }))
        .build()
}

fn payload_filter(op: Operator) -> JoinFilter {
    JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("build_payload", 0)),
            op,
            Arc::new(Column::new("probe_payload", 1)),
        )),
        vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("build_payload", DataType::Int64, true),
            Field::new("probe_payload", DataType::Int64, true),
        ])),
    )
}

#[tokio::test]
async fn prepared_build_placeholder_properties_and_reset() -> Result<()> {
    let ascending = |name, index| PhysicalSortExpr {
        expr: Arc::new(Column::new(name, index)),
        options: SortOptions::default(),
    };
    let source_schema =
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let placeholder = ProjectionExec::try_new(
        vec![
            (
                Arc::new(Column::new("key", 0)) as PhysicalExprRef,
                "key".into(),
            ),
            (
                Arc::new(Column::new("key", 0)) as PhysicalExprRef,
                "payload".into(),
            ),
        ],
        Arc::new(EmptyExec::new(source_schema)),
    )?;
    let left: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
        LexOrdering::new(vec![ascending("payload", 1)]).unwrap(),
        Arc::new(placeholder),
    ));
    let build = RecordBatch::try_new(
        left.schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![2, 1])),
        ],
    )?;
    let probe_schema =
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let probe = RecordBatch::try_new(
        Arc::clone(&probe_schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )?;
    let right: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
        LexOrdering::new(vec![ascending("key", 0)]).unwrap(),
        TestMemoryExec::try_new_exec(&[vec![probe]], probe_schema, None)?,
    ));
    let base = HashJoinExecBuilder::new(
        left,
        right,
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )],
        JoinType::Inner,
    )
    .with_partition_mode(PartitionMode::CollectLeft)
    .build()?;
    let build_key: PhysicalExprRef = Arc::new(Column::new("key", 0));
    let build_payload: PhysicalExprRef = Arc::new(Column::new("payload", 1));
    let asserted_order = [ascending("key", 2), ascending("payload", 1)];
    let base_properties = base.properties().equivalence_properties();
    assert!(
        base_properties
            .eq_group()
            .exprs_equal(&build_key, &build_payload)
    );
    assert!(base_properties.ordering_satisfy(asserted_order.clone())?);

    let prepared =
        prepare(&base, vec![build], Arc::new(GreedyMemoryPool::new(1 << 20))).await?;
    let attached = base.builder().with_prepared_build(prepared).build()?;
    assert!(
        DisplayableExecutionPlan::new(&attached)
            .indent(false)
            .to_string()
            .contains("prepared_build=2 rows")
    );
    let properties = attached.properties().equivalence_properties();
    assert!(
        !properties
            .eq_group()
            .exprs_equal(&build_key, &build_payload)
    );
    assert!(properties.ordering_satisfy([ascending("key", 2)])?);
    assert!(!properties.ordering_satisfy(asserted_order)?);
    let stats = StatisticsContext::new().compute(&attached, &StatisticsArgs::new())?;
    assert_ne!(stats.num_rows.get_value(), Some(&0));
    let output = run(&attached).await?;
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    // Resetting the unused projection/sort must retain the prepared rows.
    let reset = crate::execution_plan::reset_plan_states(Arc::new(attached))?;
    assert_eq!(
        common::collect(reset.execute(0, Arc::new(TaskContext::default()))?).await?,
        output
    );
    Ok(())
}

#[tokio::test]
async fn prepared_build_projection_pushdown_preserves_rows() -> Result<()> {
    let build = batch(vec![Some(1)]);
    let base = join(build.schema(), build.clone())?;
    let prepared =
        prepare(&base, vec![build], Arc::new(GreedyMemoryPool::new(1 << 20))).await?;
    let task = Arc::new(base.builder().with_prepared_build(prepared).build()?);
    let projection = ProjectionExec::try_new(
        vec![
            (
                Arc::new(Column::new("key", 0)) as PhysicalExprRef,
                "build_key".to_owned(),
            ),
            (
                Arc::new(Column::new("key", 2)) as PhysicalExprRef,
                "probe_key".to_owned(),
            ),
        ],
        Arc::clone(&task) as _,
    )?;
    let transformed = task
        .try_swapping_with_projection(&projection)?
        .unwrap_or_else(|| Arc::new(projection));
    let output =
        common::collect(transformed.execute(0, Arc::new(TaskContext::default()))?)
            .await?;
    assert_batches_eq!(
        [
            "+-----------+-----------+",
            "| build_key | probe_key |",
            "+-----------+-----------+",
            "| 1         | 1         |",
            "+-----------+-----------+",
        ],
        &output
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_build_reuses_data_with_independent_dynamic_filters() -> Result<()> {
    let build = batch(vec![Some(1), Some(1), None, Some(-3)]);
    let base = join(build.schema(), batch(vec![Some(1)]))?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let prepared = prepare(&base, vec![build.clone()], Arc::clone(&pool)).await?;
    let bytes = pool.reserved();
    assert_eq!(prepared.num_rows(), 4);
    assert_eq!(bytes, prepared.reserved_bytes());
    assert!(bytes > 0);

    let mut plans = Vec::new();
    let mut expected = Vec::new();
    for (op, probe) in [
        (Operator::Gt, batch(vec![Some(1), Some(2), None])),
        (Operator::Lt, batch(vec![Some(1), Some(1)])),
    ] {
        let right = TestMemoryExec::try_new_exec(
            &[
                vec![probe.slice(0, 1)],
                vec![probe.slice(1, probe.num_rows() - 1)],
            ],
            probe.schema(),
            None,
        )?;
        let task = base
            .builder()
            .with_new_children(vec![Arc::clone(base.left()), right])?
            .with_filter(Some(payload_filter(op)))
            .build()?;
        let ordinary = task
            .builder()
            .reset_state()
            .with_new_children(vec![
                TestMemoryExec::try_new_exec(
                    &[vec![build.clone()]],
                    build.schema(),
                    None,
                )?,
                Arc::clone(task.right()),
            ])?
            .build()?;
        expected.push(run(&ordinary).await?);
        let task = with_probe_filter(task)?
            .builder()
            .with_prepared_build(Arc::clone(&prepared))
            .build()?;
        plans.push(Arc::new(task));
    }
    let cancelled = base
        .builder()
        .with_prepared_build(Arc::clone(&prepared))
        .build()?;
    drop(cancelled.execute(0, Arc::new(TaskContext::default()))?);
    drop(cancelled);
    assert_eq!(pool.reserved(), bytes);
    let start = Arc::new(tokio::sync::Barrier::new(plans.len()));
    let tasks = plans.iter().map(|plan| {
        let plan = Arc::clone(plan);
        let start = Arc::clone(&start);
        SpawnedTask::spawn(async move {
            start.wait().await;
            run(&plan).await
        })
    });
    let output = futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|task| task.expect("probe task panicked"))
        .collect::<Result<Vec<_>>>()?;
    for (index, plan) in plans.iter().enumerate() {
        assert_eq!(
            batches_to_sort_string(&output[index]),
            batches_to_sort_string(&expected[index])
        );
        let metrics = plan.metrics().unwrap();
        assert_eq!(
            metrics.sum_by_name("build_input_rows").unwrap().as_usize(),
            0
        );
        assert_eq!(metrics.output_rows(), Some(1));
        let dynamic = plan.dynamic_filter.as_ref().unwrap();
        assert!(futures::poll!(Box::pin(dynamic.filter.wait_complete())).is_ready());
    }
    assert_eq!(pool.reserved(), bytes);
    drop(plans);
    drop(prepared);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_build_errors_and_cancellation_release_reservations() -> Result<()> {
    let build = batch(vec![Some(1), Some(2)]);
    let base = join(build.schema(), build.clone())?;
    let exhausted: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
    assert!(matches!(
        prepare(&base, vec![build.clone()], Arc::clone(&exhausted)).await,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(exhausted.reserved(), 0);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let failing = stream::iter(vec![
        Ok(build.clone()),
        datafusion_common::exec_err!("producer failed"),
    ]);
    let result = base
        .prepare_build(
            Box::pin(RecordBatchStreamAdapter::new(build.schema(), failing)),
            Arc::clone(&pool),
            Arc::new(ConfigOptions::default()),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(pool.reserved(), 0);
    let pending = stream::iter(vec![Ok(build.clone())]).chain(stream::pending());
    let mut preparation = Box::pin(base.prepare_build(
        Box::pin(RecordBatchStreamAdapter::new(build.schema(), pending)),
        Arc::clone(&pool),
        Arc::new(ConfigOptions::default()),
    ));
    assert!(futures::poll!(&mut preparation).is_pending());
    assert!(pool.reserved() > 0);
    drop(preparation);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_build_shares_single_batch_but_owns_output() -> Result<()> {
    for array in [
        Arc::new(Int64Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(2),
            Some(2),
            Some(3),
        ])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            Some("prefix"),
            Some("a"),
            None,
            Some("b"),
            Some("b"),
            Some("suffix"),
        ])),
    ] {
        let build = RecordBatch::try_from_iter([("key", array.slice(1, 4))])?;
        let probe = RecordBatch::try_from_iter([("key", array.slice(1, 3))])?;
        let base = join(build.schema(), probe)?
            .builder()
            .with_null_equality(NullEquality::NullEqualsNull)
            .build()?;
        for sources in [
            vec![build.clone()],
            vec![build.slice(0, 2), build.slice(2, 2)],
        ] {
            let single_batch = sources.len() == 1;
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let prepared = prepare(&base, sources, Arc::clone(&pool)).await?;
            let addresses = buffer_addresses(prepared.build.batch.columns());
            if single_batch {
                assert_eq!(addresses, buffer_addresses(build.columns()));
            }
            let bytes = prepared.reserved_bytes();
            let task = base.builder().with_prepared_build(prepared).build()?;
            let output = run(&task).await?;
            assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
            assert!(
                output
                    .iter()
                    .flat_map(|batch| buffer_addresses(&batch.columns()[..1]))
                    .all(|address| !addresses.contains(&address))
            );
            assert_eq!(pool.reserved(), bytes);
            drop(task);
            assert_eq!(pool.reserved(), 0);
            for batch in output {
                assert_eq!(batch.column(0).to_data(), batch.column(1).to_data());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_build_validates_descriptor_and_builder_changes() -> Result<()> {
    let build = batch(vec![Some(1), Some(2)]);
    let base = with_probe_filter(join(build.schema(), build.clone())?)?;
    let prepared = prepare(
        &base,
        vec![build.clone()],
        Arc::new(GreedyMemoryPool::new(1 << 20)),
    )
    .await?;
    let keys: JoinOn = vec![(
        Arc::new(Column::new("payload", 1)),
        Arc::new(Column::new("payload", 1)),
    )];
    for builder in [
        base.builder()
            .with_null_equality(NullEquality::NullEqualsNull),
        base.builder().with_type(JoinType::Right),
        base.builder()
            .with_partition_mode(PartitionMode::Partitioned),
        base.builder().with_on(keys),
    ] {
        assert!(
            builder
                .with_prepared_build(Arc::clone(&prepared))
                .build()
                .is_err()
        );
    }
    let attached = Arc::new(base.builder().with_prepared_build(prepared).build()?);
    // Changing only the probe key must not reuse its existing dynamic filter.
    assert!(
        attached
            .builder()
            .with_on(vec![(
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("payload", 1)),
            )])
            .build()
            .is_err()
    );
    let replacement = Arc::new(EmptyExec::new(build.schema())) as Arc<dyn ExecutionPlan>;
    assert!(
        Arc::clone(&attached)
            .replace_children(
                vec![replacement, Arc::clone(attached.right())],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .is_err()
    );
    let right = TestMemoryExec::try_new_exec(
        &[vec![batch(vec![Some(1), Some(3)])]],
        build.schema(),
        None,
    )?;
    let new_probe = attached
        .builder()
        .with_new_children(vec![Arc::clone(attached.left()), right])?
        .build()?;
    assert_eq!(
        run(&new_probe)
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn prepared_build_revalidates_public_keys_at_execution() -> Result<()> {
    let build = batch(vec![Some(1), Some(2)]);
    let base = join(build.schema(), build.clone())?;
    let keys: JoinOn = vec![(
        Arc::new(Column::new("payload", 1)),
        Arc::new(Column::new("payload", 1)),
    )];
    let ordinary = base
        .builder()
        .with_on(keys.clone())
        .with_new_children(vec![
            TestMemoryExec::try_new_exec(&[vec![build.clone()]], build.schema(), None)?,
            Arc::clone(base.right()),
        ])?
        .build()?;
    assert_batches_eq!(
        [
            "+-----+---------+-----+---------+",
            "| key | payload | key | payload |",
            "+-----+---------+-----+---------+",
            "| 1   | 10      | 1   | 10      |",
            "| 2   | 11      | 2   | 11      |",
            "+-----+---------+-----+---------+",
        ],
        &run(&ordinary).await?
    );

    let prepared =
        prepare(&base, vec![build], Arc::new(GreedyMemoryPool::new(1 << 20))).await?;
    let mut attached = base.builder().with_prepared_build(prepared).build()?;
    attached.on = keys;
    let error = run(&attached)
        .await
        .expect_err("changed public keys must not probe the old prepared table");
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(
        error
            .to_string()
            .contains("does not match schema, keys or null equality")
    );
    Ok(())
}

#[tokio::test]
async fn prepared_build_rejects_view_payload_before_consumption() -> Result<()> {
    let build = RecordBatch::try_from_iter([
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("payload", Arc::new(StringViewArray::from(vec!["x"]))),
    ])?;
    let base = join(build.schema(), batch(vec![Some(1)]))?;
    let never_polled =
        stream::poll_fn(|_| -> std::task::Poll<Option<Result<RecordBatch>>> {
            panic!("unsupported input must not be polled")
        });
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    assert!(
        base.prepare_build(
            Box::pin(RecordBatchStreamAdapter::new(build.schema(), never_polled)),
            Arc::clone(&pool),
            Arc::new(ConfigOptions::default()),
        )
        .await
        .is_err()
    );
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_build_matches_collect_left() -> Result<()> {
    let split = |batch: &RecordBatch| {
        (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect::<Vec<_>>()
    };
    for (build_keys, probe_keys) in [
        (vec![], vec![Some(1)]),
        (vec![Some(1)], vec![]),
        (vec![None, None], vec![None, Some(1)]),
        (
            vec![None, Some(0), Some(0), Some(-4096), Some(4096)],
            vec![Some(0), None, Some(4096), Some(2)],
        ),
    ] {
        let build = batch(build_keys);
        let probe = batch(probe_keys);
        let sources = split(&build);
        for nulls in [
            NullEquality::NullEqualsNothing,
            NullEquality::NullEqualsNull,
        ] {
            let base = join(build.schema(), probe.clone())?
                .builder()
                .with_new_children(vec![
                    TestMemoryExec::try_new_exec(
                        std::slice::from_ref(&sources),
                        build.schema(),
                        None,
                    )?,
                    TestMemoryExec::try_new_exec(&[split(&probe)], probe.schema(), None)?,
                ])?
                .with_null_equality(nulls)
                .build()?;
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let prepared = prepare(&base, sources.clone(), Arc::clone(&pool)).await?;
            for batch_size in [1, 7] {
                let context = Arc::new(
                    TaskContext::default().with_session_config(
                        datafusion_execution::config::SessionConfig::new()
                            .with_batch_size(batch_size),
                    ),
                );
                let ordinary = base.builder().reset_state().build()?;
                let attached = base
                    .builder()
                    .with_prepared_build(Arc::clone(&prepared))
                    .build()?;
                let mut expected =
                    common::collect(ordinary.execute(0, Arc::clone(&context))?).await?;
                let mut actual = common::collect(attached.execute(0, context)?).await?;
                assert!(actual.iter().all(|batch| batch.num_rows() <= batch_size));
                expected.retain(|batch| batch.num_rows() != 0);
                actual.retain(|batch| batch.num_rows() != 0);
                assert_eq!(
                    batches_to_sort_string(&actual),
                    batches_to_sort_string(&expected)
                );
            }
            drop(prepared);
            assert_eq!(pool.reserved(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn prepared_concat_admission_with_small_batches() -> Result<()> {
    let rows = 65_536;
    let build = batch((0..rows).map(|i| Some(i as i64 * 32)).collect());
    let base = join(build.schema(), batch(vec![Some(0)]))?;
    for batch_rows in [32, 64, 8192] {
        let batches = (0..rows)
            .step_by(batch_rows)
            .map(|offset| build.slice(offset, batch_rows))
            .collect();
        // Small input batches should not multiply concatenation's alignment charge.
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(9 * 1024 * 1024 / 2));
        let prepared = prepare(&base, batches, Arc::clone(&pool)).await?;
        assert_eq!(prepared.num_rows(), rows);
        assert_eq!(pool.reserved(), prepared.reserved_bytes());
        drop(prepared);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[test]
fn prepared_concat_admits_validity_for_non_nullable_input_arrays() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Boolean,
        true,
    )]));
    // A single null in the second batch creates validity for every row in the first.
    // Boolean values make that extra allocation as large as the values themselves.
    let batches = [
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BooleanArray::from(vec![true; 100_000]))],
        )?,
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BooleanArray::from(vec![None]))],
        )?,
    ];
    assert!(batches[0].column(0).nulls().is_none());
    assert!(batches[1].column(0).nulls().is_some());
    let admitted = prepared_copy_bytes(&batches)?;
    let copied = concat_batches(&schema, &batches)?;
    assert_eq!(copied.column(0).null_count(), 1);
    let allocated = copied.column(0).get_buffer_memory_size();
    assert!(
        admitted >= allocated,
        "copy admission {admitted} is smaller than allocated capacity {allocated}"
    );
    Ok(())
}

#[tokio::test]
async fn prepared_inlist_shares_accounted_build_buffers() -> Result<()> {
    let build = batch(vec![Some(1), Some(1_000_000), None, Some(1)]);
    for keys in [1, 2] {
        let on = (0..keys)
            .map(|index| {
                let name = build.schema().field(index).name().clone();
                let key: PhysicalExprRef = Arc::new(Column::new(&name, index));
                (Arc::clone(&key), key)
            })
            .collect();
        let base = join(build.schema(), build.clone())?
            .builder()
            .with_on(on)
            .build()?;
        for batches in [
            vec![build.clone()],
            vec![build.slice(0, 2), build.slice(2, 2)],
        ] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let prepared = prepare(&base, batches.clone(), Arc::clone(&pool)).await?;
            let PushdownStrategy::InList(values) = &prepared.build.membership else {
                panic!("expected IN-list membership");
            };
            let columns = prepared.build.batch.columns();
            if keys == 1 {
                assert!(Arc::ptr_eq(values, &columns[0]));
            } else {
                let values = values.as_any().downcast_ref::<StructArray>().unwrap();
                for (value, column) in values.columns().iter().zip(columns) {
                    assert!(Arc::ptr_eq(value, column));
                }
            }
            let bytes = prepared.reserved_bytes();
            let mut counter = RecordBatchMemoryCounter::new();
            let batch_bytes = counter.count_batch(&prepared.build.batch);
            assert!(bytes > batch_bytes, "hash storage must also be reserved");
            let membership_batch =
                RecordBatch::try_from_iter([("membership", Arc::clone(values))])?;
            assert_eq!(counter.count_batch(&membership_batch), 0);
            drop(membership_batch);

            // Disabling IN-list membership changes no retained key buffers.
            let mut config = ConfigOptions::default();
            config.optimizer.hash_join_inlist_pushdown_max_size = 0;
            let map = base
                .prepare_build(
                    input(batches, build.schema()),
                    Arc::clone(&pool),
                    Arc::new(config),
                )
                .await?;
            assert!(matches!(map.build.membership, PushdownStrategy::Map(_)));
            assert_eq!(map.reserved_bytes(), bytes);
            assert_eq!(pool.reserved(), 2 * bytes);
            drop(map);
            let lease = Arc::clone(&prepared);
            drop(prepared);
            assert_eq!(pool.reserved(), bytes);
            drop(lease);
            assert_eq!(pool.reserved(), 0);
        }
    }
    Ok(())
}

#[test]
fn prepared_concat_rejects_under_admission_without_shrinking() -> Result<()> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation = MemoryConsumer::new("prepared concat test").register(&pool);
    // 128 bytes of map storage plus 128 bytes of input/copy allowance.
    reservation.try_grow(256)?;
    let error =
        reconcile_prepared_copy_reservation(&reservation, 64, 64, 129).unwrap_err();
    assert!(matches!(error, DataFusionError::Internal(_)));
    assert!(
        error
            .to_string()
            .contains("exceeded its admitted copy bound")
    );
    assert_eq!(reservation.size(), 256);
    assert!(reconcile_prepared_copy_reservation(&reservation, usize::MAX, 1, 0).is_err());
    assert_eq!(reservation.size(), 256);
    reconcile_prepared_copy_reservation(&reservation, 64, 64, 128)?;
    assert_eq!(reservation.size(), 256);
    reconcile_prepared_copy_reservation(&reservation, 64, 64, 80)?;
    assert_eq!(reservation.size(), 208);
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[test]
fn prepared_sizing_rejects_overflow() -> Result<()> {
    use NullEquality::{NullEqualsNothing, NullEqualsNull};
    for (rows, keys, equality) in [
        (usize::MAX / 8 + 1, 1, NullEqualsNull),
        (0, usize::MAX, NullEqualsNothing),
        (0, usize::MAX / 64, NullEqualsNothing),
        (usize::MAX / 8, 0, NullEqualsNothing),
    ] {
        let error = prepared_scratch_bytes(rows, keys, equality).unwrap_err();
        assert!(error.to_string().contains("scratch size overflow"));
    }
    assert!(prepared_scratch_bytes(usize::MAX / 8, 1, NullEqualsNull).is_ok());

    // Null arrays can describe huge row counts without allocating data buffers.
    // Exercise both per-batch validity multiplication and cumulative addition.
    let array: ArrayRef = Arc::new(NullArray::new(usize::MAX));
    for (columns, batches) in [(8, 1), (4, 2)] {
        let batch = RecordBatch::try_from_iter(
            (0..columns).map(|i| (format!("key{i}"), Arc::clone(&array))),
        )?;
        let error = prepared_copy_bytes(&vec![batch; batches]).unwrap_err();
        assert!(error.to_string().contains("copy size overflow"));
    }
    Ok(())
}

#[tokio::test]
async fn prepared_byte_keys_use_hash_membership() -> Result<()> {
    for array in [
        Arc::new(StringArray::from(vec!["aaaa", "bbbb"])) as ArrayRef,
        Arc::new(FixedSizeBinaryArray::try_new(
            4,
            Buffer::from_slice_ref(b"aaaabbbb"),
            None,
        )?),
    ] {
        let build = RecordBatch::try_from_iter([("key", array.slice(0, 1))])?;
        let probe = RecordBatch::try_from_iter([("key", array)])?;
        let base = join(build.schema(), probe)?;
        let mut options = ConfigOptions::default();
        options.optimizer.hash_join_inlist_pushdown_max_size = 1024;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let prepared = base
            .prepare_build(
                input(vec![build.clone()], build.schema()),
                Arc::clone(&pool),
                Arc::new(options),
            )
            .await?;
        assert!(prepared.build.bounds.is_none());
        assert!(matches!(
            &prepared.build.membership,
            PushdownStrategy::Map(_)
        ));
        let task = with_probe_filter(base)?
            .builder()
            .with_prepared_build(prepared)
            .build()?;
        let output = run(&task).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let filter = &task.dynamic_filter.as_ref().unwrap().filter;
        assert!(futures::poll!(Box::pin(filter.wait_complete())).is_ready());
        drop(task);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
