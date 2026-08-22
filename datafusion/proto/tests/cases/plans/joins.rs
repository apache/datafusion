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

//! The join execs.

use super::{roundtrip_test, roundtrip_test_and_return};
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, PhysicalSortExpr};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    AsOfJoinExec, AsOfMatchExpr, HashJoinExec, NestedLoopJoinExec, PartitionMode,
    SortMergeJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion::prelude::SessionContext;
use datafusion_common::{JoinSide, NullEquality, Result};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_hash_join() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);
    let on = vec![(
        Arc::new(Column::new("col", schema_left.index_of("col")?)) as _,
        Arc::new(Column::new("col", schema_right.index_of("col")?)) as _,
    )];

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    for join_type in &[
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftSemi,
        JoinType::RightSemi,
    ] {
        for partition_mode in &[PartitionMode::Partitioned, PartitionMode::CollectLeft] {
            roundtrip_test(Arc::new(HashJoinExec::try_new(
                Arc::new(EmptyExec::new(schema_left.clone())),
                Arc::new(EmptyExec::new(schema_right.clone())),
                on.clone(),
                None,
                join_type,
                None,
                *partition_mode,
                NullEquality::NullEqualsNothing,
                false,
            )?))?;
        }
    }
    Ok(())
}

#[test]
fn roundtrip_asof_join() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("id", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("price", DataType::Int32, false),
    ]));
    let on = vec![(
        Arc::new(Column::new("symbol", 0)) as _,
        Arc::new(Column::new("symbol", 0)) as _,
    )];

    for projection in [None, Some(vec![]), Some(vec![0, 5])] {
        for op in [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq] {
            roundtrip_test(Arc::new(AsOfJoinExec::try_new(
                Arc::new(EmptyExec::new(Arc::clone(&left_schema))),
                Arc::new(EmptyExec::new(Arc::clone(&right_schema))),
                on.clone(),
                AsOfMatchExpr::new(
                    Arc::new(Column::new("ts", 1)),
                    op,
                    Arc::new(Column::new("ts", 1)),
                ),
                projection.clone(),
            )?))?;
        }
    }
    Ok(())
}

#[test]
fn roundtrip_nested_loop_join() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_a]);

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    for join_type in &[
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftSemi,
        JoinType::RightSemi,
    ] {
        roundtrip_test(Arc::new(NestedLoopJoinExec::try_new(
            Arc::new(EmptyExec::new(schema_left.clone())),
            Arc::new(EmptyExec::new(schema_right.clone())),
            None,
            join_type,
            Some(vec![0]),
        )?))?;
    }
    Ok(())
}

/// Regression: proto3 `repeated` fields cannot distinguish "absent" from "empty",
/// so a naive encoding collapses `Some(vec![])` and `None` into the same wire
/// representation. `try_embed_projection` (DataFusion 53+) produces
/// `HashJoinExec.projection = Some(vec![])` for `SELECT count(1) … JOIN …`,
/// which previously round-tripped to `None` and caused downstream consumers (e.g.
/// distributed Flight executors) to receive a different number of output
/// columns than the planner declared. Verify all three states preserve.
#[test]
fn roundtrip_hash_join_projection_states() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Arc::new(Schema::new(vec![field_a.clone()]));
    let schema_right = Arc::new(Schema::new(vec![field_a]));
    let on = vec![(
        Arc::new(Column::new("col", schema_left.index_of("col")?)) as _,
        Arc::new(Column::new("col", schema_right.index_of("col")?)) as _,
    )];

    for projection in [None, Some(vec![]), Some(vec![0]), Some(vec![1])] {
        roundtrip_test(Arc::new(HashJoinExec::try_new(
            Arc::new(EmptyExec::new(schema_left.clone())),
            Arc::new(EmptyExec::new(schema_right.clone())),
            on.clone(),
            None,
            &JoinType::Inner,
            projection,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?))?;
    }
    Ok(())
}

/// Regression: `HashJoinExecNode` had no `fetch` field, so the row limit that
/// the `limit_pushdown` physical optimizer rule pushes into the join via
/// `ExecutionPlan::with_fetch` was silently dropped by serde. Because that rule
/// also removes the enclosing `GlobalLimitExec` once the join absorbs the limit,
/// a round-tripped plan had no limit left at all and a distributed executor
/// returned more rows than the query asked for.
///
/// Note this cannot be covered by `roundtrip_test`: that helper compares
/// `format!("{plan:?}")`, and `HashJoinExec`'s `Debug` output does not include
/// `fetch`, so the before/after strings match even when the value is lost. The
/// assertions below therefore inspect `fetch()` directly.
#[test]
fn roundtrip_hash_join_fetch() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Arc::new(Schema::new(vec![field_a.clone()]));
    let schema_right = Arc::new(Schema::new(vec![field_a]));
    let on = vec![(
        Arc::new(Column::new("col", schema_left.index_of("col")?)) as _,
        Arc::new(Column::new("col", schema_right.index_of("col")?)) as _,
    )];

    // `usize::MAX` and `u32::MAX as usize` pin the decode-side `u64 -> usize`
    // conversion: it is a checked `usize::try_from`, and a large fetch must
    // survive the round trip exactly rather than being truncated or clamped.
    // Both are representable on every target (on a 32-bit target `usize::MAX`
    // is simply `u32::MAX`), so this stays portable. The truncating case
    // itself -- a `u64` fetch above `usize::MAX` -- is only reachable on a
    // 32-bit target and so is not exercised by this test on a 64-bit host.
    for fetch in [None, Some(7), Some(u32::MAX as usize), Some(usize::MAX)] {
        let join = HashJoinExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&schema_left))),
            Arc::new(EmptyExec::new(Arc::clone(&schema_right))),
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?;

        let plan: Arc<dyn ExecutionPlan> = match fetch {
            // This is how `limit_pushdown` installs the limit.
            Some(fetch) => join
                .with_fetch(Some(fetch))
                .expect("HashJoinExec supports fetch"),
            None => Arc::new(join),
        };
        assert_eq!(plan.fetch(), fetch);

        let ctx = SessionContext::new();
        let codec = DefaultPhysicalExtensionCodec {};
        let proto_converter = DefaultPhysicalProtoConverter {};
        let deserialized =
            roundtrip_test_and_return(plan, &ctx, &codec, &proto_converter)?;

        let deserialized_join = deserialized
            .downcast_ref::<HashJoinExec>()
            .expect("should be a HashJoinExec");
        assert_eq!(deserialized_join.fetch(), fetch);
    }
    Ok(())
}

/// Same regression coverage for `NestedLoopJoinExec`, which shares the
/// `repeated uint32 projection` proto field shape with `HashJoinExec`.
#[test]
fn roundtrip_nested_loop_join_projection_states() -> Result<()> {
    let field_a = Field::new("col", DataType::Int64, false);
    let schema_left = Arc::new(Schema::new(vec![field_a.clone()]));
    let schema_right = Arc::new(Schema::new(vec![field_a]));

    for projection in [None, Some(vec![]), Some(vec![0]), Some(vec![1])] {
        roundtrip_test(Arc::new(NestedLoopJoinExec::try_new(
            Arc::new(EmptyExec::new(schema_left.clone())),
            Arc::new(EmptyExec::new(schema_right.clone())),
            None,
            &JoinType::Inner,
            projection,
        )?))?;
    }
    Ok(())
}

#[test]
fn roundtrip_sym_hash_join() -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let field_a = Field::new("col_a", DataType::Int64, false);
    let field_b = Field::new("col_b", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_b.clone()]);
    let on = vec![(
        Arc::new(Column::new("col_a", schema_left.index_of("col_a")?)) as _,
        Arc::new(Column::new("col_b", schema_right.index_of("col_b")?)) as _,
    )];
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("col_a", 0)),
            Operator::Gt,
            Arc::new(Column::new("col_b", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![field_a, field_b])),
    );

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    let left_order: LexOrdering = [PhysicalSortExpr {
        expr: Arc::new(Column::new("col_a", schema_left.index_of("col_a")?)),
        options: SortOptions {
            descending: true,
            nulls_first: false,
        },
    }]
    .into();
    let right_order: LexOrdering = [PhysicalSortExpr {
        expr: Arc::new(Column::new("col_b", schema_right.index_of("col_b")?)),
        options: SortOptions {
            descending: false,
            nulls_first: true,
        },
    }]
    .into();
    let ordering_cases = [
        (None, None),
        (Some(left_order.clone()), None),
        (None, Some(right_order.clone())),
        (Some(left_order), Some(right_order)),
    ];
    let ordering_options = |ordering: Option<&LexOrdering>| {
        ordering
            .map(|ordering| ordering.iter().map(|expr| expr.options).collect::<Vec<_>>())
    };

    for join_type in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::LeftSemi,
        JoinType::RightSemi,
        JoinType::LeftMark,
        JoinType::RightMark,
    ] {
        for null_equality in [
            NullEquality::NullEqualsNothing,
            NullEquality::NullEqualsNull,
        ] {
            for filter in [None, Some(filter.clone())] {
                for partition_mode in [
                    StreamJoinPartitionMode::Partitioned,
                    StreamJoinPartitionMode::SinglePartition,
                ] {
                    for (left_order, right_order) in &ordering_cases {
                        let result = roundtrip_test_and_return(
                            Arc::new(SymmetricHashJoinExec::try_new(
                                Arc::new(EmptyExec::new(schema_left.clone())),
                                Arc::new(EmptyExec::new(schema_right.clone())),
                                on.clone(),
                                filter.clone(),
                                &join_type,
                                null_equality,
                                left_order.clone(),
                                right_order.clone(),
                                partition_mode,
                            )?),
                            &ctx,
                            &codec,
                            &proto_converter,
                        )?;
                        let result =
                            result.downcast_ref::<SymmetricHashJoinExec>().unwrap();
                        assert_eq!(result.join_type(), &join_type);
                        assert_eq!(result.null_equality(), null_equality);
                        assert_eq!(result.partition_mode(), partition_mode);
                        assert_eq!(
                            ordering_options(result.left_sort_exprs()),
                            ordering_options(left_order.as_ref())
                        );
                        assert_eq!(
                            ordering_options(result.right_sort_exprs()),
                            ordering_options(right_order.as_ref())
                        );
                        assert_eq!(
                            result.filter().map(JoinFilter::column_indices),
                            filter.as_ref().map(JoinFilter::column_indices)
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn roundtrip_sort_merge_join() -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let field_a = Field::new("col_a", DataType::Int64, false);
    let field_b = Field::new("col_b", DataType::Int64, false);
    let schema_left = Schema::new(vec![field_a.clone()]);
    let schema_right = Schema::new(vec![field_b.clone()]);
    let on = vec![(
        Arc::new(Column::new("col_a", schema_left.index_of("col_a")?)) as _,
        Arc::new(Column::new("col_b", schema_right.index_of("col_b")?)) as _,
    )];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("col_a", 1)),
            Operator::Gt,
            Arc::new(Column::new("col_b", 0)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![field_a, field_b])),
    );

    let schema_left = Arc::new(schema_left);
    let schema_right = Arc::new(schema_right);
    let sort_options = vec![SortOptions {
        descending: true,
        nulls_first: false,
    }];
    for null_equality in [
        NullEquality::NullEqualsNothing,
        NullEquality::NullEqualsNull,
    ] {
        for filter in [None, Some(filter.clone())] {
            for join_type in [
                JoinType::Inner,
                JoinType::Left,
                JoinType::Right,
                JoinType::Full,
                JoinType::LeftAnti,
                JoinType::RightAnti,
                JoinType::LeftSemi,
                JoinType::RightSemi,
                JoinType::LeftMark,
                JoinType::RightMark,
            ] {
                let result = roundtrip_test_and_return(
                    Arc::new(SortMergeJoinExec::try_new(
                        Arc::new(EmptyExec::new(schema_left.clone())),
                        Arc::new(EmptyExec::new(schema_right.clone())),
                        on.clone(),
                        filter.clone(),
                        join_type,
                        sort_options.clone(),
                        null_equality,
                    )?),
                    &ctx,
                    &codec,
                    &proto_converter,
                )?;
                let result = result.downcast_ref::<SortMergeJoinExec>().unwrap();
                assert_eq!(result.join_type(), join_type);
                assert_eq!(result.null_equality(), null_equality);
                assert_eq!(result.sort_options(), sort_options);
                assert_eq!(
                    result.filter().as_ref().map(|f| f.column_indices()),
                    filter.as_ref().map(|f| f.column_indices())
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn roundtrip_logical_plan_sort_merge_join() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "t0",
        "tests/testdata/test.csv",
        datafusion::prelude::CsvReadOptions::default().has_header(true),
    )
    .await?;
    ctx.register_csv(
        "t1",
        "tests/testdata/test.csv",
        datafusion::prelude::CsvReadOptions::default().has_header(true),
    )
    .await?;

    ctx.sql("SET datafusion.optimizer.prefer_hash_join = false")
        .await?
        .show()
        .await?;

    let query = "SELECT t1.* FROM t0 join t1 on t0.a = t1.a";
    let plan = ctx.sql(query).await?.create_physical_plan().await?;
    roundtrip_test(plan)
}
