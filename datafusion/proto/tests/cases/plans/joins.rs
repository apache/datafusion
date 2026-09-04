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
use datafusion::physical_plan::expressions::{
    BinaryExpr, CastExpr, Column, Literal, PhysicalSortExpr,
};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    AsOfJoinExec, AsOfMatchExpr, HashJoinExec, NestedLoopJoinExec, PartitionMode,
    PiecewiseMergeJoinExec, SortMergeJoinExec, StreamJoinPartitionMode,
    SymmetricHashJoinExec,
};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinSide, NullEquality, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_proto_converter,
    physical_plan_to_bytes_with_proto_converter,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use prost::Message;
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
fn roundtrip_sort_merge_join_with_projection() -> Result<()> {
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let schema_left = Arc::new(Schema::new(vec![Field::new(
        "col_a",
        DataType::Int64,
        false,
    )]));
    let schema_right = Arc::new(Schema::new(vec![Field::new(
        "col_b",
        DataType::Int64,
        false,
    )]));
    let on = vec![(
        Arc::new(Column::new("col_a", 0)) as _,
        Arc::new(Column::new("col_b", 0)) as _,
    )];

    // An empty projection is not an absent one: it changes the output schema, and
    // proto3 cannot tell the two apart without a sentinel.
    for projection in [None, Some(vec![]), Some(vec![1, 0])] {
        let result = roundtrip_test_and_return(
            Arc::new(
                SortMergeJoinExec::try_new(
                    Arc::new(EmptyExec::new(Arc::clone(&schema_left))),
                    Arc::new(EmptyExec::new(Arc::clone(&schema_right))),
                    on.clone(),
                    None,
                    JoinType::Inner,
                    vec![SortOptions::default()],
                    NullEquality::NullEqualsNothing,
                )?
                .with_projection(projection.clone())?,
            ),
            &ctx,
            &codec,
            &proto_converter,
        )?;
        let result = result.downcast_ref::<SortMergeJoinExec>().unwrap();
        assert_eq!(result.projection, projection);
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

/// One `roundtrip_piecewise_merge_join_compound_on_exprs` case: the buffered and
/// streamed halves of the predicate, and the string each must display as after the
/// round trip.
type PiecewiseOnCase = (
    Arc<dyn PhysicalExpr>,
    Arc<dyn PhysicalExpr>,
    &'static str,
    &'static str,
);

/// A field of `PiecewiseMergeJoinExecNode` to clear, with its wire name.
type PiecewiseFieldClear = (&'static str, fn(&mut protobuf::PiecewiseMergeJoinExecNode));

/// Schemas for the `PiecewiseMergeJoinExec` tests.
///
/// The two sides differ in width, in column names, and in the *index* of the join
/// column (buffered `a` is at 2, streamed `b` is at 0). Matching indices on both
/// sides would let a decoder that resolved a predicate half against the wrong
/// schema still produce `Column { index: 0 }` and round-trip silently.
fn piecewise_schemas() -> (Arc<Schema>, Arc<Schema>) {
    (
        Arc::new(Schema::new(vec![
            Field::new("pad0", DataType::Utf8, true),
            Field::new("pad1", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
        ])),
        Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("pad2", DataType::Utf8, false),
        ])),
    )
}

/// A valid `PiecewiseMergeJoinExec` over [`piecewise_schemas`], for tests that then
/// corrupt one field of its encoding.
fn piecewise_join(
    schemas: &(Arc<Schema>, Arc<Schema>),
    num_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(PiecewiseMergeJoinExec::try_new(
        Arc::new(EmptyExec::new(Arc::clone(&schemas.0))),
        Arc::new(EmptyExec::new(Arc::clone(&schemas.1))),
        (
            Arc::new(Column::new("a", 2)) as _,
            Arc::new(Column::new("b", 0)) as _,
        ),
        Operator::Lt,
        JoinType::Inner,
        num_partitions,
    )?))
}

/// `PiecewiseMergeJoinExec` derives its schema, sort options, required input
/// orderings and plan properties inside `try_new`, so only the six constructor
/// arguments travel on the wire. Cover the full cartesian product of the two
/// enum-valued ones: every range operator against every supported join type --
/// the four classic ones plus the two left existence joins, whose output schema is
/// the buffered side alone. (Right existence joins and Mark joins are rejected by
/// `try_new`, so this is the complete set.)
#[test]
fn roundtrip_piecewise_merge_join() -> Result<()> {
    let (schema_buffered, schema_streamed) = piecewise_schemas();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    for operator in [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq] {
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ] {
            let result = roundtrip_test_and_return(
                Arc::new(PiecewiseMergeJoinExec::try_new(
                    Arc::new(EmptyExec::new(Arc::clone(&schema_buffered))),
                    Arc::new(EmptyExec::new(Arc::clone(&schema_streamed))),
                    (
                        Arc::new(Column::new("a", 2)) as _,
                        Arc::new(Column::new("b", 0)) as _,
                    ),
                    operator,
                    join_type,
                    7,
                )?),
                &ctx,
                &codec,
                &proto_converter,
            )?;
            let result = result.downcast_ref::<PiecewiseMergeJoinExec>().unwrap();

            assert_eq!(result.operator, operator);
            assert_eq!(result.join_type(), join_type);
            // Both the name and the index have to survive on the correct side.
            assert_eq!(result.on.0.to_string(), "a@2");
            assert_eq!(result.on.1.to_string(), "b@0");
            // The existence joins output the buffered side alone (3 fields) while the
            // classic ones output both sides (3 + 2). The before/after comparison
            // inside the helper already covers the schema; this pins the expected
            // width absolutely, so the existence output contract is stated rather
            // than merely preserved.
            let expected_fields =
                if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    3
                } else {
                    5
                };
            assert_eq!(
                result.schema().fields().len(),
                expected_fields,
                "unexpected output width for {join_type}"
            );
        }
    }
    Ok(())
}

/// The two halves of the range predicate are arbitrary `PhysicalExpr`s, not just
/// columns: `side_of` in the physical planner classifies a side by
/// `Expr::column_refs()`, so any expression whose columns all come from one input
/// qualifies and reaches `create_physical_expr`. `ON t0.a + 1 < t1.b * 2` therefore
/// puts a `BinaryExpr` tree on each side. Cover a nested tree and a `CastExpr` so
/// the `encode_expr` / `decode_required_expr` path is exercised beyond a bare
/// `Column`, including the literals inside it.
#[test]
fn roundtrip_piecewise_merge_join_compound_on_exprs() -> Result<()> {
    let (schema_buffered, schema_streamed) = piecewise_schemas();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    // (buffered half, streamed half, expected buffered string, expected streamed string)
    let cases: Vec<PiecewiseOnCase> = vec![
        // Nested arithmetic on both sides.
        (
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 2)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                )),
                Operator::Multiply,
                Arc::new(Column::new("pad1", 1)),
            )),
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("b", 0)),
                Operator::Minus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(2)))),
            )),
            "(a@2 + 1) * pad1@1",
            "b@0 - 2",
        ),
        // A cast on the buffered side, a plain column on the streamed side: the
        // two halves need not have the same shape.
        (
            Arc::new(CastExpr::new(
                Arc::new(Column::new("a", 2)),
                DataType::Int32,
                None,
            )),
            Arc::new(Column::new("b", 0)),
            "CAST(a@2 AS Int32)",
            "b@0",
        ),
    ];

    for (on_buffered, on_streamed, expected_buffered, expected_streamed) in cases {
        let result = roundtrip_test_and_return(
            Arc::new(PiecewiseMergeJoinExec::try_new(
                Arc::new(EmptyExec::new(Arc::clone(&schema_buffered))),
                Arc::new(EmptyExec::new(Arc::clone(&schema_streamed))),
                (Arc::clone(&on_buffered), Arc::clone(&on_streamed)),
                Operator::Lt,
                JoinType::Inner,
                7,
            )?),
            &ctx,
            &codec,
            &proto_converter,
        )?;
        let result = result.downcast_ref::<PiecewiseMergeJoinExec>().unwrap();

        assert_eq!(result.on.0.to_string(), expected_buffered);
        assert_eq!(result.on.1.to_string(), expected_streamed);
    }
    Ok(())
}

/// `num_partitions` crosses the wire as a `u64` and is read back with a checked
/// `usize::try_from`, matching how `HashJoinExec` handles `fetch`. Pin the
/// conversion at the boundaries rather than only at a small value.
///
/// Both `u32::MAX` and `usize::MAX` are representable on every target (on a 32-bit
/// target `usize::MAX` is simply `u32::MAX`), so this stays portable. The
/// truncating case -- a `u64` above `usize::MAX` -- is only reachable on a 32-bit
/// target and so is not exercised on a 64-bit host. `num_partitions` is only read
/// at execution time, so constructing these does not allocate.
#[test]
fn roundtrip_piecewise_merge_join_num_partitions_bounds() -> Result<()> {
    let schemas = piecewise_schemas();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    for num_partitions in [1, 7, u32::MAX as usize, usize::MAX] {
        // `roundtrip_test_and_return` compares the `Debug` output, which for
        // `PiecewiseMergeJoinExec` includes `num_partitions`.
        roundtrip_test_and_return(
            piecewise_join(&schemas, num_partitions)?,
            &ctx,
            &codec,
            &proto_converter,
        )?;
    }
    Ok(())
}

/// Every message field except `operator` and `join_type` is a proto3 `message`,
/// so "absent" is representable on the wire and a truncated or hand-built payload
/// can omit any of them. Each omission must name the field it is missing rather
/// than panicking on an `unwrap`.
#[test]
fn piecewise_merge_join_rejects_missing_fields() -> Result<()> {
    let schemas = piecewise_schemas();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    let valid = physical_plan_to_bytes_with_proto_converter(
        piecewise_join(&schemas, 7)?,
        &codec,
        &proto_converter,
    )?;

    let clear: [PiecewiseFieldClear; 4] = [
        ("buffered", |join| join.buffered = None),
        ("streamed", |join| join.streamed = None),
        ("on_buffered", |join| join.on_buffered = None),
        ("on_streamed", |join| join.on_streamed = None),
    ];

    for (field, clear_field) in clear {
        let mut node = protobuf::PhysicalPlanNode::decode(valid.as_ref())
            .expect("a plan encoded by try_to_proto must decode as a PhysicalPlanNode");
        let Some(PhysicalPlanType::PiecewiseMergeJoin(join)) =
            node.physical_plan_type.as_mut()
        else {
            panic!("expected a PiecewiseMergeJoin node");
        };
        clear_field(join);

        let Err(err) = physical_plan_from_bytes_with_proto_converter(
            &node.encode_to_vec(),
            ctx.task_ctx().as_ref(),
            &codec,
            &proto_converter,
        ) else {
            panic!("decoding must fail when {field} is absent");
        };
        // Match the quoted field name, so that clearing `buffered` cannot be
        // satisfied by an error that only mentions `on_buffered`.
        let expected = format!("missing required field '{field}'");
        assert!(
            err.to_string().contains(&expected),
            "missing {field}: expected an error containing {expected:?}, got: {err}"
        );
    }
    Ok(())
}

/// The operator travels as its `Operator` variant name, so the decoder has to
/// handle names that `try_to_proto` would never emit: a name with no `Operator`
/// counterpart, and a real `Operator` that is not one of the four range
/// operators. Both must surface as errors rather than panicking or silently
/// decoding into a different operator, since a malformed payload can arrive from
/// any peer.
#[test]
fn piecewise_merge_join_rejects_bad_operator_on_the_wire() -> Result<()> {
    let schemas = piecewise_schemas();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    // Start from a valid encoding so only the operator field is malformed.
    let valid = physical_plan_to_bytes_with_proto_converter(
        piecewise_join(&schemas, 7)?,
        &codec,
        &proto_converter,
    )?;
    let mut node = protobuf::PhysicalPlanNode::decode(valid.as_ref())
        .expect("a plan just encoded by try_to_proto must decode as a PhysicalPlanNode");

    for (operator, expected) in [
        ("NotAnOperator", "unknown Operator"),
        ("Eq", "non-range operator"),
    ] {
        let Some(PhysicalPlanType::PiecewiseMergeJoin(join)) =
            node.physical_plan_type.as_mut()
        else {
            panic!("expected a PiecewiseMergeJoin node");
        };
        join.operator = operator.to_string();

        let Err(err) = physical_plan_from_bytes_with_proto_converter(
            &node.encode_to_vec(),
            ctx.task_ctx().as_ref(),
            &codec,
            &proto_converter,
        ) else {
            panic!("decoding must fail for operator {operator}");
        };
        assert!(
            err.to_string().contains(expected),
            "operator {operator}: expected an error containing {expected:?}, got: {err}"
        );
    }
    Ok(())
}

/// `join_type` is a proto3 enum, so any `i32` is representable on the wire -- including
/// the existence joins `try_new` still rejects (right-sided and mark) and values that
/// map to no `JoinType` at all. `try_to_proto` can emit none of these, but a payload
/// from a newer peer or a hand-built one can, and each must surface as an error
/// rather than a panic or a silently different operator. The right-sided cases matter
/// most: `try_new` derives *reversed* sort options for them, so accepting one would
/// build a plan whose buffered side is sorted the wrong way.
#[test]
fn piecewise_merge_join_rejects_unsupported_join_type_on_the_wire() -> Result<()> {
    let schemas = piecewise_schemas();

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    let valid = physical_plan_to_bytes_with_proto_converter(
        piecewise_join(&schemas, 7)?,
        &codec,
        &proto_converter,
    )?;
    let mut node = protobuf::PhysicalPlanNode::decode(valid.as_ref())
        .expect("a plan just encoded by try_to_proto must decode as a PhysicalPlanNode");

    // (wire value, description, expected error fragment)
    let cases: [(i32, &str, &str); 5] = [
        (
            protobuf::JoinType::Rightsemi as i32,
            "RightSemi",
            "Existence join RightSemi is currently not supported",
        ),
        (
            protobuf::JoinType::Rightanti as i32,
            "RightAnti",
            "Existence join RightAnti is currently not supported",
        ),
        (
            protobuf::JoinType::Leftmark as i32,
            "LeftMark",
            "Existence join LeftMark is currently not supported",
        ),
        (
            protobuf::JoinType::Rightmark as i32,
            "RightMark",
            "Existence join RightMark is currently not supported",
        ),
        // Past the last tag, so it maps to no variant.
        (i32::MAX, "no variant", "unknown JoinType"),
    ];

    for (wire_value, description, expected) in cases {
        let Some(PhysicalPlanType::PiecewiseMergeJoin(join)) =
            node.physical_plan_type.as_mut()
        else {
            panic!("expected a PiecewiseMergeJoin node");
        };
        join.join_type = wire_value;

        let Err(err) = physical_plan_from_bytes_with_proto_converter(
            &node.encode_to_vec(),
            ctx.task_ctx().as_ref(),
            &codec,
            &proto_converter,
        ) else {
            panic!("decoding must fail for join_type {description}");
        };
        assert!(
            err.to_string().contains(expected),
            "join_type {description}: expected an error containing {expected:?}, got: {err}"
        );
    }
    Ok(())
}

/// Every other test here compares a plan against itself after a round trip, which is
/// blind to a *symmetric* mistake: if `try_to_proto` wrote the streamed side into the
/// `buffered` field and `try_from_proto` read it back the same way, before and after
/// would still match, and even the `on` assertions would hold, because each half is
/// resolved against whichever child the decoder paired it with. The bytes would still
/// be wrong for every other reader of this schema. So assert the wire layout
/// absolutely, once. A left existence join is the clearest case to do it on: its two
/// sides differ in width on the wire (3 fields buffered, 2 streamed), and its output
/// is the buffered side alone, so nothing downstream would reveal a swap either.
#[test]
fn piecewise_merge_join_existence_wire_layout() -> Result<()> {
    let (schema_buffered, schema_streamed) = piecewise_schemas();

    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};

    let bytes = physical_plan_to_bytes_with_proto_converter(
        Arc::new(PiecewiseMergeJoinExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&schema_buffered))),
            Arc::new(EmptyExec::new(Arc::clone(&schema_streamed))),
            (
                Arc::new(Column::new("a", 2)) as _,
                Arc::new(Column::new("b", 0)) as _,
            ),
            Operator::Gt,
            JoinType::LeftSemi,
            7,
        )?),
        &codec,
        &proto_converter,
    )?;

    let node = protobuf::PhysicalPlanNode::decode(bytes.as_ref())
        .expect("a plan encoded by try_to_proto must decode as a PhysicalPlanNode");
    let Some(PhysicalPlanType::PiecewiseMergeJoin(join)) = node.physical_plan_type else {
        panic!("expected a PiecewiseMergeJoin node");
    };

    // The two children, identified by the width of the schema each carries.
    let child_width = |child: Option<&protobuf::PhysicalPlanNode>, field: &str| {
        let Some(PhysicalPlanType::Empty(empty)) = child
            .expect("child must be present")
            .physical_plan_type
            .as_ref()
        else {
            panic!("expected an EmptyExec under {field}");
        };
        empty
            .schema
            .as_ref()
            .expect("schema must be present")
            .columns
            .len()
    };
    assert_eq!(child_width(join.buffered.as_deref(), "buffered"), 3);
    assert_eq!(child_width(join.streamed.as_deref(), "streamed"), 2);

    // Each half of the range predicate, against the side it belongs to.
    let column = |expr: Option<&protobuf::PhysicalExprNode>, field: &str| {
        let Some(protobuf::physical_expr_node::ExprType::Column(column)) =
            expr.expect("expr must be present").expr_type.as_ref()
        else {
            panic!("expected a Column under {field}");
        };
        (column.name.clone(), column.index)
    };
    assert_eq!(
        column(join.on_buffered.as_ref(), "on_buffered"),
        ("a".to_string(), 2)
    );
    assert_eq!(
        column(join.on_streamed.as_ref(), "on_streamed"),
        ("b".to_string(), 0)
    );

    assert_eq!(join.operator, "Gt");
    assert_eq!(join.join_type, protobuf::JoinType::Leftsemi as i32);
    assert_eq!(join.num_partitions, 7);
    Ok(())
}

/// End-to-end: a `PiecewiseMergeJoinExec` as the planner actually builds it, rather
/// than one hand-constructed by the test. This covers what the unit test above
/// cannot: the planner may swap the two inputs and reverse the operator (a range
/// predicate written right-to-left becomes a left-to-right one on a swapped plan),
/// and it takes `num_partitions` from `target_partitions`. If the encoding confused
/// the buffered and streamed sides, the swapped form is where it would show.
#[tokio::test]
async fn roundtrip_planned_piecewise_merge_join() -> Result<()> {
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

    ctx.sql("SET datafusion.optimizer.enable_piecewise_merge_join = true")
        .await?
        .collect()
        .await?;

    // Both predicate directions, so the input-swapping branch of the planner is
    // covered as well as the straight one, plus a compound predicate so the
    // planner emits a non-`Column` expression on each side. The last two
    // decorrelate to `LeftSemi` / `LeftAnti`, where the marked side must come back
    // as the buffered one.
    for query in [
        "SELECT t0.a FROM t0 JOIN t1 ON t0.a < t1.a",
        "SELECT t0.a FROM t0 JOIN t1 ON t1.a > t0.a",
        "SELECT t0.a FROM t0 JOIN t1 ON t0.a + 1 < t1.a * 2",
        "SELECT t0.a FROM t0 WHERE EXISTS (SELECT 1 FROM t1 WHERE t0.a > t1.a)",
        "SELECT t0.a FROM t0 WHERE NOT EXISTS (SELECT 1 FROM t1 WHERE t0.a > t1.a)",
    ] {
        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        let mut found = false;
        plan.apply(|node| {
            found |= node.downcast_ref::<PiecewiseMergeJoinExec>().is_some();
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(found, "expected a PiecewiseMergeJoinExec for: {query}");

        roundtrip_test(plan)?;
    }
    Ok(())
}
