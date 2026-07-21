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

use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, DictionaryArray, Float64Array, Int32Array, StringArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::utils::memory::{
    RecordBatchMemoryCounter, get_record_batch_memory_size,
};
use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::Column;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::common;
use crate::joins::{IEJoinCondition, IEJoinExec};
use crate::test::TestMemoryExec;
use crate::{ExecutionPlan, PhysicalExpr};

use super::exec::concat_memory_upper_bound;

#[derive(Clone)]
struct Row {
    id: i32,
    group: Option<i32>,
    first: Option<i32>,
    second: Option<i32>,
}

fn batch(rows: &[Row]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("group", DataType::Int32, true),
        Field::new("first", DataType::Int32, true),
        Field::new("second", DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.id))),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.group).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.first).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.second).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[test]
fn concat_reservation_covers_duplicated_dictionary_buffers() -> Result<()> {
    let values = Arc::new(StringArray::from(vec![
        "x".repeat(8_192),
        "y".repeat(8_192),
    ])) as ArrayRef;
    let dictionary = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0, 1]),
        values,
    )?) as ArrayRef;
    let dictionary_type = dictionary.data_type().clone();
    let schema = Arc::new(Schema::new(
        (0..3)
            .map(|index| {
                Field::new(
                    format!("dictionary_{index}"),
                    dictionary_type.clone(),
                    false,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&dictionary), Arc::clone(&dictionary), dictionary],
    )?;
    let batches = vec![batch.clone(), batch];

    let mut retained_counter = RecordBatchMemoryCounter::new();
    for batch in &batches {
        retained_counter.count_batch(batch);
    }
    let concatenated = concat_batches(&schema, &batches)?;
    let concatenated_size = get_record_batch_memory_size(&concatenated);
    assert!(
        concatenated_size > retained_counter.memory_usage(),
        "the fixture must reproduce dictionary-buffer duplication"
    );
    assert!(concat_memory_upper_bound(&batches)? >= concatenated_size);
    Ok(())
}

fn compare(left: i32, right: i32, operator: Operator) -> bool {
    match operator {
        Operator::Lt => left < right,
        Operator::LtEq => left <= right,
        Operator::Gt => left > right,
        Operator::GtEq => left >= right,
        _ => unreachable!(),
    }
}

async fn run_join(
    left_rows: &[Row],
    right_rows: &[Row],
    first_operator: Operator,
    second_operator: Operator,
    null_equality: NullEquality,
    keyed: bool,
) -> Result<Vec<(i32, i32)>> {
    let schema = batch(&[]).schema();
    let left_batches = left_rows.chunks(11).map(batch).collect::<Vec<_>>();
    let right_batches = right_rows.chunks(13).map(batch).collect::<Vec<_>>();
    let left = TestMemoryExec::try_new_exec(&[left_batches], Arc::clone(&schema), None)?;
    let right = TestMemoryExec::try_new_exec(&[right_batches], schema, None)?;
    let on = if keyed {
        vec![(
            Arc::new(Column::new("group", 1)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("group", 1)) as Arc<dyn PhysicalExpr>,
        )]
    } else {
        vec![]
    };
    let conditions = [
        IEJoinCondition::new(
            Arc::new(Column::new("first", 2)),
            Arc::new(Column::new("first", 2)),
            first_operator,
        ),
        IEJoinCondition::new(
            Arc::new(Column::new("second", 3)),
            Arc::new(Column::new("second", 3)),
            second_operator,
        ),
    ];
    let join = IEJoinExec::try_new(
        left,
        right,
        on,
        conditions,
        None,
        JoinType::Inner,
        null_equality,
    )?;
    let context = TaskContext::default()
        .with_session_config(SessionConfig::new().with_batch_size(7));
    let batches = common::collect(join.execute(0, Arc::new(context))?).await?;
    assert!(batches.iter().all(|batch| batch.num_rows() <= 7));
    assert!(
        join.metrics()
            .and_then(|metrics| metrics.sum_by_name("peak_mem_used"))
            .map(|memory| memory.as_usize())
            .is_some_and(|memory| memory > 0),
        "IEJoin should report its peak memory usage"
    );

    let mut pairs = Vec::new();
    for batch in batches {
        let left_ids = batch.column(0).as_primitive::<Int32Type>();
        let right_ids = batch.column(4).as_primitive::<Int32Type>();
        pairs.extend(
            left_ids
                .values()
                .iter()
                .zip(right_ids.values())
                .map(|(left, right)| (*left, *right)),
        );
    }
    pairs.sort_unstable();
    Ok(pairs)
}

fn expected(
    left_rows: &[Row],
    right_rows: &[Row],
    first_operator: Operator,
    second_operator: Operator,
    null_equality: NullEquality,
    keyed: bool,
) -> Vec<(i32, i32)> {
    let mut pairs = Vec::new();
    for left in left_rows {
        for right in right_rows {
            let equality_matches = !keyed
                || match null_equality {
                    NullEquality::NullEqualsNothing => {
                        left.group.is_some() && left.group == right.group
                    }
                    NullEquality::NullEqualsNull => left.group == right.group,
                };
            let range_matches =
                left.first
                    .zip(right.first)
                    .is_some_and(|(left, right)| compare(left, right, first_operator))
                    && left.second.zip(right.second).is_some_and(|(left, right)| {
                        compare(left, right, second_operator)
                    });
            if equality_matches && range_matches {
                pairs.push((left.id, right.id));
            }
        }
    }
    pairs.sort_unstable();
    pairs
}

#[tokio::test]
async fn random_differential_all_operator_pairs() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(0x1e_10_1e_10);
    let operators = [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq];

    for case in 0..8 {
        let mut make_rows = |len: usize, id_offset: i32| {
            (0..len)
                .map(|index| Row {
                    id: id_offset + index as i32,
                    group: (rng.random_ratio(4, 5)).then(|| rng.random_range(0..4)),
                    first: (rng.random_ratio(4, 5)).then(|| rng.random_range(-4..5)),
                    second: (rng.random_ratio(4, 5)).then(|| rng.random_range(-4..5)),
                })
                .collect::<Vec<_>>()
        };
        let left = make_rows(36 + case, 0);
        let right = make_rows(40 + case, 1_000);

        for first in operators {
            for second in operators {
                for null_equality in [
                    NullEquality::NullEqualsNothing,
                    NullEquality::NullEqualsNull,
                ] {
                    for keyed in [false, true] {
                        let actual =
                            run_join(&left, &right, first, second, null_equality, keyed)
                                .await?;
                        assert_eq!(
                            actual,
                            expected(&left, &right, first, second, null_equality, keyed),
                            "case={case}, first={first}, second={second}, null_equality={null_equality:?}, keyed={keyed}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn hierarchical_bitmap_and_cross_batch_cursor() -> Result<()> {
    // Make the first ordering identical to row order, then activate sparse
    // positions on both sides of the bitmap's 64-bit word and 4,096-bit
    // summary boundaries. More than one output batch is required, which also
    // verifies that the current bitmap cursor survives across stream polls.
    const RIGHT_ROWS: usize = 5_000;
    const ACTIVE_POSITIONS: [usize; 14] = [
        0, 1, 6, 7, 8, 62, 63, 64, 65, 4_094, 4_095, 4_096, 4_097, 4_999,
    ];
    let left = vec![Row {
        id: 0,
        group: Some(7),
        first: Some(0),
        second: Some(1),
    }];
    let right = (0..RIGHT_ROWS)
        .map(|position| Row {
            id: 1_000 + position as i32,
            group: Some(7),
            first: Some((RIGHT_ROWS - position) as i32),
            second: Some(if ACTIVE_POSITIONS.contains(&position) {
                0
            } else {
                2
            }),
        })
        .collect::<Vec<_>>();
    let fixture_expected = ACTIVE_POSITIONS
        .iter()
        .map(|position| (0, 1_000 + *position as i32))
        .collect::<Vec<_>>();

    for keyed in [false, true] {
        let expected = expected(
            &left,
            &right,
            Operator::LtEq,
            Operator::GtEq,
            NullEquality::NullEqualsNothing,
            keyed,
        );
        assert_eq!(expected, fixture_expected, "keyed={keyed}");
        assert_eq!(
            run_join(
                &left,
                &right,
                Operator::LtEq,
                Operator::GtEq,
                NullEquality::NullEqualsNothing,
                keyed,
            )
            .await?,
            expected,
            "keyed={keyed}"
        );
    }
    Ok(())
}

#[cfg(feature = "force_hash_collisions")]
#[tokio::test]
async fn forced_hash_collisions_differential() -> Result<()> {
    let make_row = |id, group, first, second| Row {
        id,
        group,
        first: Some(first),
        second: Some(second),
    };
    let left = vec![
        make_row(0, Some(0), 0, 10),
        make_row(1, Some(1), 10, 0),
        make_row(2, Some(2), 5, 5),
        make_row(3, None, 6, 6),
    ];
    let right = vec![
        make_row(10, Some(0), 5, 5),
        make_row(11, Some(1), 5, 5),
        make_row(12, Some(3), 6, 4),
        make_row(13, None, 6, 6),
    ];

    let operators = [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq];
    for first in operators {
        for second in operators {
            for null_equality in [
                NullEquality::NullEqualsNothing,
                NullEquality::NullEqualsNull,
            ] {
                assert_eq!(
                    run_join(&left, &right, first, second, null_equality, true).await?,
                    expected(&left, &right, first, second, null_equality, true),
                    "first={first}, second={second}, null_equality={null_equality:?}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn duplicates_strictness_and_nulls() -> Result<()> {
    let left = vec![
        Row {
            id: 0,
            group: Some(1),
            first: Some(2),
            second: Some(2),
        },
        Row {
            id: 1,
            group: Some(1),
            first: Some(2),
            second: Some(2),
        },
        Row {
            id: 2,
            group: Some(1),
            first: None,
            second: Some(2),
        },
    ];
    let right = vec![
        Row {
            id: 10,
            group: Some(1),
            first: Some(2),
            second: Some(2),
        },
        Row {
            id: 11,
            group: Some(1),
            first: Some(3),
            second: Some(2),
        },
    ];

    assert_eq!(
        run_join(
            &left,
            &right,
            Operator::LtEq,
            Operator::GtEq,
            NullEquality::NullEqualsNothing,
            true,
        )
        .await?,
        vec![(0, 10), (0, 11), (1, 10), (1, 11)]
    );
    assert!(
        run_join(
            &left,
            &right,
            Operator::Lt,
            Operator::Gt,
            NullEquality::NullEqualsNothing,
            true,
        )
        .await?
        .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn empty_inputs() -> Result<()> {
    let row = Row {
        id: 1,
        group: Some(1),
        first: Some(1),
        second: Some(1),
    };
    for keyed in [false, true] {
        assert!(
            run_join(
                &[],
                std::slice::from_ref(&row),
                Operator::LtEq,
                Operator::GtEq,
                NullEquality::NullEqualsNothing,
                keyed,
            )
            .await?
            .is_empty()
        );
        assert!(
            run_join(
                std::slice::from_ref(&row),
                &[],
                Operator::LtEq,
                Operator::GtEq,
                NullEquality::NullEqualsNothing,
                keyed,
            )
            .await?
            .is_empty()
        );
    }
    Ok(())
}

#[tokio::test]
async fn float_total_order_and_signed_zero() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("group", DataType::Float64, false),
        Field::new("first", DataType::Float64, false),
        Field::new("second", DataType::Float64, false),
    ]));
    let make_batch = |ids, group, first, second| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Float64Array::from(group)),
                Arc::new(Float64Array::from(first)),
                Arc::new(Float64Array::from(second)),
            ],
        )
        .unwrap()
    };
    let left_batch = make_batch(
        vec![0, 1],
        vec![-0.0, f64::NAN],
        vec![-0.0, f64::NAN],
        vec![0.0, f64::NAN],
    );
    let right_batch = make_batch(
        vec![10, 11],
        vec![0.0, f64::NAN],
        vec![0.0, f64::NAN],
        vec![-0.0, f64::NAN],
    );
    let left =
        TestMemoryExec::try_new_exec(&[vec![left_batch]], Arc::clone(&schema), None)?;
    let right =
        TestMemoryExec::try_new_exec(&[vec![right_batch]], Arc::clone(&schema), None)?;
    let conditions = [
        IEJoinCondition::new(
            Arc::new(Column::new("first", 2)),
            Arc::new(Column::new("first", 2)),
            Operator::LtEq,
        ),
        IEJoinCondition::new(
            Arc::new(Column::new("second", 3)),
            Arc::new(Column::new("second", 3)),
            Operator::GtEq,
        ),
    ];
    let join = IEJoinExec::try_new(
        left,
        right,
        vec![(
            Arc::new(Column::new("group", 1)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("group", 1)) as Arc<dyn PhysicalExpr>,
        )],
        conditions,
        None,
        JoinType::Inner,
        NullEquality::NullEqualsNothing,
    )?;
    let batches =
        common::collect(join.execute(0, Arc::new(TaskContext::default()))?).await?;
    let mut pairs = Vec::new();
    for batch in batches {
        let left_ids = batch.column(0).as_primitive::<Int32Type>();
        let right_ids = batch.column(4).as_primitive::<Int32Type>();
        pairs.extend(
            left_ids
                .values()
                .iter()
                .zip(right_ids.values())
                .map(|(left, right)| (*left, *right)),
        );
    }
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(0, 10), (1, 11)]);
    Ok(())
}

#[test]
fn programmatic_callers_get_a_clear_type_coercion_error() -> Result<()> {
    let left_batch = batch(&[Row {
        id: 0,
        group: Some(1),
        first: Some(1),
        second: Some(2),
    }]);
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("group", DataType::Int32, true),
        Field::new("first", DataType::Float64, true),
        Field::new("second", DataType::Float64, true),
    ]));
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![10])),
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(Float64Array::from(vec![Some(1.0)])),
            Arc::new(Float64Array::from(vec![Some(2.0)])),
        ],
    )?;
    let left = TestMemoryExec::try_new_exec(
        &[vec![left_batch.clone()]],
        left_batch.schema(),
        None,
    )?;
    let right = TestMemoryExec::try_new_exec(&[vec![right_batch]], right_schema, None)?;
    let conditions = [
        IEJoinCondition::new(
            Arc::new(Column::new("first", 2)),
            Arc::new(Column::new("first", 2)),
            Operator::LtEq,
        ),
        IEJoinCondition::new(
            Arc::new(Column::new("second", 3)),
            Arc::new(Column::new("second", 3)),
            Operator::GtEq,
        ),
    ];

    let error = IEJoinExec::try_new(
        left,
        right,
        vec![],
        conditions,
        None,
        JoinType::Inner,
        NullEquality::NullEqualsNothing,
    )
    .expect_err("Int32 and Float64 drivers require an explicit physical cast");
    assert!(
        error.to_string().contains(
            "physical expressions must be explicitly coerced to identical types"
        )
    );
    Ok(())
}

#[tokio::test]
async fn memory_limit_returns_error_and_releases_reservation() -> Result<()> {
    let row = Row {
        id: 1,
        group: Some(1),
        first: Some(1),
        second: Some(1),
    };
    let left_batch = batch(std::slice::from_ref(&row));
    let right_batch = batch(std::slice::from_ref(&row));
    let left = TestMemoryExec::try_new_exec(
        &[vec![left_batch.clone()]],
        left_batch.schema(),
        None,
    )?;
    let right = TestMemoryExec::try_new_exec(
        &[vec![right_batch.clone()]],
        right_batch.schema(),
        None,
    )?;
    let conditions = [
        IEJoinCondition::new(
            Arc::new(Column::new("first", 2)),
            Arc::new(Column::new("first", 2)),
            Operator::LtEq,
        ),
        IEJoinCondition::new(
            Arc::new(Column::new("second", 3)),
            Arc::new(Column::new("second", 3)),
            Operator::GtEq,
        ),
    ];
    let join = IEJoinExec::try_new(
        left,
        right,
        vec![],
        conditions,
        None,
        JoinType::Inner,
        NullEquality::NullEqualsNothing,
    )?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()?;
    let context = Arc::new(TaskContext::default().with_runtime(runtime));
    let error = common::collect(join.execute(0, context)?)
        .await
        .expect_err("one byte of memory must not be enough for IEJoin");
    assert!(matches!(
        error.find_root(),
        datafusion_common::DataFusionError::ResourcesExhausted(_)
    ));
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
