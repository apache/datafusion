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
    empty::EmptyExec,
    filter::FilterExec,
    memory::MemoryStream,
    sorts::sort::SortExec,
    statistics::{ChildStats, StatisticsArgs, StatisticsContext},
    stream::RecordBatchStreamAdapter,
    test::TestMemoryExec,
};
use arrow::array::{Array, Int64Array, LargeStringArray, StringViewArray};
use arrow::compute::kernels::sort::SortOptions;
use arrow_schema::Field;
use datafusion_common::assert_batches_eq;
use datafusion_common::utils::memory::get_record_batch_memory_size;
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
    let properties = attached.properties().equivalence_properties();
    assert!(
        !properties
            .eq_group()
            .exprs_equal(&build_key, &build_payload)
    );
    assert!(properties.ordering_satisfy([ascending("key", 2)])?);
    assert!(!properties.ordering_satisfy(asserted_order)?);
    assert_eq!(base.child_stats_requests(None)[0], ChildStats::At(None));
    assert_eq!(attached.child_stats_requests(None)[0], ChildStats::Skip);
    assert_eq!(attached.child_stats_requests(Some(0))[0], ChildStats::Skip);
    let stats = StatisticsContext::new().compute(&attached, &StatisticsArgs::new())?;
    assert_ne!(stats.num_rows.get_value(), Some(&0));
    let output = run(&attached).await?;
    assert_batches_eq!(
        [
            "+-----+---------+-----+",
            "| key | payload | key |",
            "+-----+---------+-----+",
            "| 1   | 2       | 1   |",
            "| 1   | 1       | 1   |",
            "+-----+---------+-----+",
        ],
        &output
    );
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

#[tokio::test]
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
        let task = with_probe_filter(task)?
            .builder()
            .with_prepared_build(Arc::clone(&prepared))
            .build()?;
        plans.push(task);
    }
    let cancelled = base
        .builder()
        .with_prepared_build(Arc::clone(&prepared))
        .build()?;
    drop(cancelled.execute(0, Arc::new(TaskContext::default()))?);
    drop(cancelled);
    assert_eq!(pool.reserved(), bytes);
    let (first, second) = tokio::join!(run(&plans[0]), run(&plans[1]));
    let output = [first?, second?];
    for (index, plan) in plans.iter().enumerate() {
        assert_batches_eq!(
            [
                "+-----+---------+-----+---------+",
                "| key | payload | key | payload |",
                "+-----+---------+-----+---------+",
                [
                    "| 1   | 11      | 1   | 10      |",
                    "| 1   | 10      | 1   | 11      |"
                ][index],
                "+-----+---------+-----+---------+",
            ],
            &output[index]
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
async fn prepared_build_accounts_for_hash_map_row_indices() -> Result<()> {
    let rows = 32_768;
    // Sparse keys exclude the dense ArrayMap without overriding configuration.
    let build = batch((0..rows).map(|i| Some(i as i64 * 32)).collect());
    let base = join(build.schema(), batch(vec![Some(0)]))?;
    let retained = get_record_batch_memory_size(&build);
    let buckets = estimate_memory_size::<(u32, u64)>(rows, size_of::<JoinHashMapU32>())?;
    let scratch = rows * size_of::<u64>();
    let row_indices = rows * size_of::<u32>();
    let denied: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
        retained + buckets + scratch + row_indices - 1,
    ));
    let result = prepare(&base, vec![build.clone()], Arc::clone(&denied)).await;
    assert!(
        matches!(
            result,
            Err(datafusion_common::DataFusionError::ResourcesExhausted(_))
        ),
        "bucket-only budget must reject the unadmitted row indices: {result:?}"
    );
    assert_eq!(denied.reserved(), 0);

    let admitted: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
        retained + buckets + scratch + row_indices,
    ));
    let prepared = prepare(&base, vec![build], Arc::clone(&admitted)).await?;
    assert!(matches!(prepared.build.map.as_ref(), Map::HashMap(_)));
    assert_eq!(prepared.reserved_bytes(), retained + buckets + row_indices);
    assert_eq!(admitted.reserved(), prepared.reserved_bytes());
    drop(prepared);
    assert_eq!(admitted.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_single_batch_shares_build_but_owns_output() -> Result<()> {
    let rows = 4096;
    let build = batch((0..rows + 2).map(|i| Some(i as i64 * 4)).collect()).slice(1, rows);
    let source_values = build.column(0).to_data().buffers()[0].as_ptr();
    let base = join(build.schema(), batch(vec![Some(4), Some(8)]))?;
    let retained = get_record_batch_memory_size(&build);
    let map_bytes = ArrayMap::estimate_memory_size(4, rows as u64 * 4, rows);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(retained + map_bytes));
    let prepared = prepare(&base, vec![build], Arc::clone(&pool)).await?;
    assert!(matches!(prepared.build.map.as_ref(), Map::ArrayMap(_)));
    assert_eq!(
        prepared.build.batch.column(0).to_data().buffers()[0].as_ptr(),
        source_values
    );
    assert_eq!(pool.reserved(), retained + map_bytes);
    let source_addresses = buffer_addresses(prepared.build.batch.columns());
    let task = base
        .builder()
        .with_prepared_build(Arc::clone(&prepared))
        .build()?;
    drop(prepared);
    let output = run(&task).await?;
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in &output {
        for address in buffer_addresses(&batch.columns()[..2]) {
            assert!(!source_addresses.contains(&address));
        }
    }
    assert_eq!(pool.reserved(), retained + map_bytes);
    drop(task);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        output[0]
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .values()
            .as_ref(),
        &[4, 8]
    );
    Ok(())
}

#[tokio::test]
async fn prepared_build_validates_descriptor_and_builder_changes() -> Result<()> {
    let build = batch(vec![Some(1), Some(2)]);
    let base = with_probe_filter(join(build.schema(), build.clone())?)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let prepared = prepare(&base, vec![build.clone()], pool).await?;
    assert!(
        base.builder()
            .with_null_equality(NullEquality::NullEqualsNull)
            .with_prepared_build(Arc::clone(&prepared))
            .build()
            .is_err()
    );
    assert!(
        base.builder()
            .with_type(JoinType::Right)
            .with_prepared_build(Arc::clone(&prepared))
            .build()
            .is_err()
    );
    assert!(
        base.builder()
            .with_partition_mode(PartitionMode::Partitioned)
            .with_prepared_build(Arc::clone(&prepared))
            .build()
            .is_err()
    );
    let keys: JoinOn = vec![(
        Arc::new(Column::new("payload", 1)),
        Arc::new(Column::new("payload", 1)),
    )];
    assert!(
        base.builder()
            .with_on(keys.clone())
            .with_prepared_build(Arc::clone(&prepared))
            .build()
            .is_err()
    );
    let attached = Arc::new(base.builder().with_prepared_build(prepared).build()?);
    assert!(
        attached
            .builder()
            .reset_state()
            .build()?
            .swap_inputs(PartitionMode::CollectLeft)
            .is_err()
    );
    let same_keys = attached.builder().with_on(attached.on().to_vec()).build()?;
    assert_eq!(
        run(&same_keys)
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    assert!(attached.builder().with_on(keys.clone()).build().is_err());
    // Public key mutation bypasses the builder, so execution validates it too.
    let mut mutated = attached.builder().build()?;
    mutated.on = keys;
    let error = run(&mutated)
        .await
        .expect_err("changed keys require a new build");
    assert!(error.to_string().contains("does not match"), "{error}");

    // A compatible build cannot make an existing probe filter follow new keys.
    let probe_payload: JoinOn = vec![(
        Arc::new(Column::new("key", 0)),
        Arc::new(Column::new("payload", 1)),
    )];
    assert!(attached.builder().with_on(probe_payload).build().is_err());
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
    assert!(
        attached
            .builder()
            .with_type(JoinType::Left)
            .build()
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn prepared_build_rejects_view_payload_before_consumption() -> Result<()> {
    let payloads: [ArrayRef; 2] = [
        Arc::new(StringViewArray::from(vec!["x"])),
        Arc::new(LargeStringArray::from(vec!["x"])),
    ];
    for payload in payloads {
        let build = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("payload", payload.data_type().clone(), false),
            ])),
            vec![Arc::new(Int64Array::from(vec![1])), payload],
        )?;
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
                Arc::new(ConfigOptions::default())
            )
            .await
            .is_err()
        );
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn prepared_build_preserves_nulls_duplicates_and_batch_cap() -> Result<()> {
    let build = batch(vec![None, None, Some(1), Some(1)]);
    for (nulls, expected) in [
        (NullEquality::NullEqualsNothing, 2),
        (NullEquality::NullEqualsNull, 4),
    ] {
        let base = join(build.schema(), batch(vec![None, Some(1)]))?
            .builder()
            .with_null_equality(nulls)
            .build()?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let prepared = prepare(&base, vec![build.clone()], pool).await?;
        let task = base.builder().with_prepared_build(prepared).build()?;
        let context = TaskContext::default().with_session_config(
            datafusion_execution::config::SessionConfig::new().with_batch_size(1),
        );
        let output = common::collect(task.execute(0, Arc::new(context))?).await?;
        assert!(output.iter().all(|batch| batch.num_rows() <= 1));
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn prepared_build_empty_and_all_null_inputs() -> Result<()> {
    let utf8_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let all_null = batch(vec![None, None]);
    for (schema, sources) in [
        (Arc::clone(&utf8_schema), vec![]),
        (
            Arc::clone(&utf8_schema),
            vec![RecordBatch::new_empty(utf8_schema)],
        ),
        (all_null.schema(), vec![all_null]),
    ] {
        let base = join(schema, batch(vec![None, Some(1)]))?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let prepared = prepare(&base, sources, Arc::clone(&pool)).await?;
        // Empty UTF-8 arrays still retain an offset buffer.
        assert!(prepared.reserved_bytes() > 0);
        let task = base.builder().with_prepared_build(prepared).build()?;
        assert_eq!(
            run(&task)
                .await?
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        drop(task);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

mod plain_bytes;

#[tokio::test]
async fn prepared_composite_keys_admit_hash_and_null_mask_scratch() -> Result<()> {
    let make_batch = |start: usize, len: usize| {
        RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int64Array::from_iter(
                    (start..start + len).map(|i| (i % 7 != 0).then_some(i as i64 * 32)),
                )) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(Int64Array::from_iter(
                    (start..start + len).map(|i| (i % 5 != 0).then_some(i as i64)),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    // Differing batch sizes exercise the peak scratch admission across batches;
    // two nullable keys also need temporary combined validity masks.
    let batches = vec![make_batch(1000, 1001), make_batch(0, 1000)];
    let base = join(batches[0].schema(), batches[1].slice(0, 10))?
        .builder()
        .with_on(vec![
            (
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("key", 0)),
            ),
            (
                Arc::new(Column::new("payload", 1)),
                Arc::new(Column::new("payload", 1)),
            ),
        ])
        .build()?;
    let mut retained = RecordBatchMemoryCounter::new();
    let mut copy_bytes = 0;
    for batch in &batches {
        retained.count_batch(batch);
        copy_bytes += prepared_copy_bytes(batch)?;
    }
    let rows = 2001;
    let buckets = estimate_memory_size::<(u32, u64)>(rows, size_of::<JoinHashMapU32>())?;
    let row_indices = rows * size_of::<u32>();
    let scratch = 1001 * size_of::<u64>() + 2 * (1001usize.div_ceil(8) + 64);
    let peak = retained.memory_usage() + copy_bytes + buckets + row_indices + scratch;
    let denied: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(peak - 1));
    let result = prepare(&base, batches.clone(), Arc::clone(&denied)).await;
    assert!(matches!(
        result,
        Err(datafusion_common::DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(denied.reserved(), 0);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(peak));
    let prepared = prepare(&base, batches, Arc::clone(&pool)).await?;
    let task = base.builder().with_prepared_build(prepared).build()?;
    let output = run(&task).await?;
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 7);
    drop(task);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn prepared_null_keys_admit_materialized_validity() -> Result<()> {
    use arrow::array::NullArray;

    let rows = 1024;
    let build = RecordBatch::try_from_iter(vec![(
        "key",
        Arc::new(NullArray::new(rows)) as ArrayRef,
    )])?;
    let base = join(build.schema(), build.slice(0, 1))?;
    let retained = get_record_batch_memory_size(&build);
    let buckets = estimate_memory_size::<(u32, u64)>(rows, size_of::<JoinHashMapU32>())?;
    let row_indices = rows * size_of::<u32>();
    let scratch = rows * size_of::<u64>() + rows.div_ceil(8) + 64;
    let peak = retained + buckets + row_indices + scratch;
    let denied: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(peak - 1));
    assert!(
        prepare(&base, vec![build.clone()], Arc::clone(&denied))
            .await
            .is_err()
    );
    assert_eq!(denied.reserved(), 0);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(peak));
    let prepared = prepare(&base, vec![build], Arc::clone(&pool)).await?;
    let task = base.builder().with_prepared_build(prepared).build()?;
    assert_eq!(
        run(&task)
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    drop(task);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
