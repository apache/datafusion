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

mod pruning_utils;

mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;

    use datafusion_expr::Operator;
    use datafusion_expr_common::columnar_value::ColumnarValue;

    use datafusion_physical_expr::PhysicalExpr;

    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, Column, NegativeExpr, NotExpr, lit,
    };
    use datafusion_physical_expr_common::physical_expr::{
        ColumnStats, NullPresence, PropagatedIntermediate, PruningContext, PruningOutcome,
    };

    use crate::pruning_utils::{
        ColumnPruningStatistics, DummyStats, MultiColumnPruningStatistics,
        single_column_stats,
    };

    /// Test statistics inference for a column reference expression. The source statistics
    /// only include a single column.
    ///
    /// Expr: a(column names)
    /// Target: Column expression propagates statistics from the source statistics.
    #[test]
    fn column_expr_single_col() {
        // Dummy stats: two containers with constant value 10 and 3 rows each.
        let pruning_stats = Arc::new(single_column_stats(
            "a",
            Some(Arc::new(Int32Array::from(vec![10, 20, 30]))),
            Some(Arc::new(Int32Array::from(vec![15, 25, 35]))),
            Some(Arc::new(UInt64Array::from(vec![0, 10, 5]))),
            Some(Arc::new(UInt64Array::from(vec![10, 10, 10]))),
        ));

        let context = Arc::new(PruningContext::new(pruning_stats));
        let column_expr = Column::new("a", 0);

        // Asserting the above stats are correctly propagated through the column reference
        match column_expr
            .evaluate_statistics_vectorized(context)
            .unwrap()
            .expect("pruning result")
        {
            PropagatedIntermediate::IntermediateStats(stats) => {
                let range_stats = stats.range_stats().expect("range stats");
                assert_eq!(range_stats.len(), 3);

                let (mins, maxs) = range_stats.normalize_to_arrays().unwrap();
                let mins = mins.expect("mins");
                let maxs = maxs.expect("maxs");

                let mins = mins.as_any().downcast_ref::<Int32Array>().unwrap();
                let maxs = maxs.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(mins, &Int32Array::from(vec![Some(10), Some(20), Some(30)]));
                assert_eq!(maxs, &Int32Array::from(vec![Some(15), Some(25), Some(35)]));

                let null_stats = stats.null_stats().expect("null stats");
                assert_eq!(null_stats.len(), 3);
                let presence: Vec<Option<bool>> = null_stats.presence().iter().collect();
                assert_eq!(presence, vec![Some(true), Some(false), None]);
            }
            other => panic!("expected stats, got {other:?}"),
        }
    }

    // The source statistics includes multiple columns, covering various edge cases
    // like single container missing stat, all containers missed a certain stat type,
    // etc.
    #[test]
    fn column_expr_multiple_col() {
        let num_containers = 3;
        // Column a has some missing stats for individual containers
        let mins_a = Arc::new(Int32Array::from(vec![None, Some(5), Some(7)]));
        let maxs_a = Arc::new(Int32Array::from(vec![Some(10), None, Some(12)]));
        let nulls_a = Arc::new(UInt64Array::from(vec![Some(0), Some(1), None]));
        let rows_a = Arc::new(UInt64Array::from(vec![Some(10), Some(10), Some(10)]));

        // Column b covers additional edge cases: partially missing min/max and no row counts.
        let mins_b = Arc::new(Int32Array::from(vec![Some(-5), None, Some(0)]));
        let maxs_b = Arc::new(Int32Array::from(vec![Some(-1), Some(2), None]));
        let nulls_b = Arc::new(UInt64Array::from(vec![None, Some(0), Some(5)]));
        let rows_b: Option<ArrayRef> = None;

        // Column c: some containers missing both min and max.
        let mins_c = Arc::new(Int32Array::from(vec![None, Some(1), None]));
        let maxs_c = Arc::new(Int32Array::from(vec![None, Some(4), None]));
        let nulls_c = Arc::new(UInt64Array::from(vec![Some(0), Some(2), None]));
        let rows_c = Arc::new(UInt64Array::from(vec![Some(5), Some(5), Some(5)]));

        // Column d: min entirely missing, max present.
        let mins_d: Option<ArrayRef> = None;
        let maxs_d = Arc::new(Int32Array::from(vec![Some(3), Some(6), Some(9)]));
        let nulls_d = Arc::new(UInt64Array::from(vec![Some(1), None, Some(0)]));
        let rows_d: Option<ArrayRef> = None;

        // Column e: everything missing.
        let mins_e: Option<ArrayRef> = None;
        let maxs_e: Option<ArrayRef> = None;
        let nulls_e: Option<ArrayRef> = None;
        let rows_e: Option<ArrayRef> = None;

        let stats = MultiColumnPruningStatistics::new(num_containers)
            .with_column(
                ColumnPruningStatistics::new("a")
                    .with_range(Some(mins_a.clone()), Some(maxs_a.clone()))
                    .with_nulls(Some(nulls_a.clone()), Some(rows_a.clone())),
            )
            .with_column(
                ColumnPruningStatistics::new("b")
                    .with_range(Some(mins_b.clone()), Some(maxs_b.clone()))
                    .with_nulls(Some(nulls_b.clone()), rows_b),
            )
            .with_column(
                ColumnPruningStatistics::new("c")
                    .with_range(Some(mins_c.clone()), Some(maxs_c.clone()))
                    .with_nulls(Some(nulls_c.clone()), Some(rows_c.clone())),
            )
            .with_column(
                ColumnPruningStatistics::new("d")
                    .with_range(mins_d.clone(), Some(maxs_d.clone()))
                    .with_nulls(Some(nulls_d.clone()), rows_d.clone()),
            )
            .with_column(
                ColumnPruningStatistics::new("e")
                    .with_range(mins_e.clone(), maxs_e.clone())
                    .with_nulls(nulls_e.clone(), rows_e.clone()),
            );

        let ctx = Arc::new(PruningContext::new(Arc::new(stats)));

        // Column a has full stats coverage for range and nulls.
        match Column::new("a", 0)
            .evaluate_statistics_vectorized(Arc::clone(&ctx))
            .unwrap()
            .expect("pruning result")
        {
            PropagatedIntermediate::IntermediateStats(stats) => {
                let range_stats = stats.range_stats().expect("range stats");
                assert_eq!(range_stats.len(), num_containers);
                let (mins, maxs) = range_stats.normalize_to_arrays().unwrap();
                let mins = mins.expect("mins");
                let maxs = maxs.expect("maxs");
                let mins = mins.as_any().downcast_ref::<Int32Array>().unwrap();
                let maxs = maxs.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(mins, &Int32Array::from(vec![None, Some(5), Some(7)]));
                assert_eq!(maxs, &Int32Array::from(vec![Some(10), None, Some(12)]));

                let null_stats = stats.null_stats().expect("null stats");
                assert_eq!(null_stats.len(), num_containers);
                let presence: Vec<Option<bool>> = null_stats.presence().iter().collect();
                assert_eq!(presence, vec![Some(true), None, None]);
            }
            other => panic!("expected stats, got {other:?}"),
        }

        // Column b has partial range stats and missing row counts, so null stats are absent.
        match Column::new("b", 1)
            .evaluate_statistics_vectorized(Arc::clone(&ctx))
            .unwrap()
            .expect("pruning result")
        {
            PropagatedIntermediate::IntermediateStats(stats) => {
                let range_stats = stats.range_stats().expect("range stats");
                assert_eq!(range_stats.len(), num_containers);
                let (mins, maxs) = range_stats.normalize_to_arrays().unwrap();
                let mins = mins.expect("mins");
                let maxs = maxs.expect("maxs");
                let mins = mins.as_any().downcast_ref::<Int32Array>().unwrap();
                let maxs = maxs.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(mins, &Int32Array::from(vec![Some(-5), None, Some(0)]));
                assert_eq!(maxs, &Int32Array::from(vec![Some(-1), Some(2), None]));

                assert!(stats.null_stats().is_none());
            }
            other => panic!("expected stats, got {other:?}"),
        }

        // Column c has min/max arrays with some containers missing both.
        match Column::new("c", 2)
            .evaluate_statistics_vectorized(Arc::clone(&ctx))
            .unwrap()
            .expect("pruning result")
        {
            PropagatedIntermediate::IntermediateStats(stats) => {
                let range_stats = stats.range_stats().expect("range stats");
                assert_eq!(range_stats.len(), num_containers);
                let (mins, maxs) = range_stats.normalize_to_arrays().unwrap();
                let mins = mins.expect("mins");
                let maxs = maxs.expect("maxs");
                let mins = mins.as_any().downcast_ref::<Int32Array>().unwrap();
                let maxs = maxs.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(mins, &Int32Array::from(vec![None, Some(1), None]));
                assert_eq!(maxs, &Int32Array::from(vec![None, Some(4), None]));

                let null_stats = stats.null_stats().expect("null stats");
                let presence: Vec<Option<bool>> = null_stats.presence().iter().collect();
                assert_eq!(presence, vec![Some(true), None, None]);
            }
            other => panic!("expected stats, got {other:?}"),
        }

        // Column d has max stats only and missing row counts, so null stats are absent.
        match Column::new("d", 3)
            .evaluate_statistics_vectorized(Arc::clone(&ctx))
            .unwrap()
            .expect("pruning result")
        {
            PropagatedIntermediate::IntermediateStats(stats) => {
                let range_stats = stats.range_stats().expect("range stats");
                assert_eq!(range_stats.len(), num_containers);
                let (mins, maxs) = range_stats.normalize_to_arrays().unwrap();
                assert!(mins.is_none(), "mins should be entirely missing");
                let maxs = maxs.expect("maxs");
                let maxs = maxs.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(maxs, &Int32Array::from(vec![Some(3), Some(6), Some(9)]));
                assert!(stats.null_stats().is_none());
            }
            other => panic!("expected stats, got {other:?}"),
        }

        // Column e has no stats at all; expect empty stats.
        match Column::new("e", 4)
            .evaluate_statistics_vectorized(ctx)
            .unwrap()
            .expect("pruning result")
        {
            PropagatedIntermediate::IntermediateStats(stats) => {
                assert!(stats.range_stats().is_none());
                assert!(stats.null_stats().is_none());
            }
            other => panic!("expected stats, got {other:?}"),
        }
    }

    #[test]
    fn lit_basic() {
        // expect range stat to be the scalar, and all container's null stat is the
        // value inside test cases
        let cases = vec![
            (ScalarValue::Int32(Some(5)), NullPresence::NoNull),
            (ScalarValue::Int32(None), NullPresence::AllNull),
        ];
        let num_containers = 3;

        for (scalar, expected_presence) in cases {
            let expected_value = scalar.clone();
            let lit_expr = lit(scalar);
            let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(
                num_containers,
            ))));
            let stat = lit_expr
                .evaluate_statistics_vectorized(ctx)
                .expect("pruning ok")
                .expect("pruning result");

            match stat {
                PropagatedIntermediate::IntermediateStats(ColumnStats {
                    range_stats: Some(range_stats),
                    null_stats,
                    num_containers: stats_num_containers,
                }) => {
                    assert_eq!(range_stats.len(), num_containers);
                    assert_eq!(stats_num_containers, num_containers);
                    let mins = range_stats.mins.as_ref().expect("mins");
                    let maxs = range_stats.maxs.as_ref().expect("maxs");
                    match (mins, maxs) {
                        (ColumnarValue::Scalar(min), ColumnarValue::Scalar(max)) => {
                            assert_eq!(min, &expected_value);
                            assert_eq!(max, &expected_value);
                        }
                        other => panic!("unexpected range stats: {other:?}"),
                    }

                    let presence: Vec<NullPresence> = null_stats
                        .expect("null stats")
                        .presence()
                        .iter()
                        .map(NullPresence::from_presence_item)
                        .collect();
                    let expected: Vec<NullPresence> =
                        std::iter::repeat_n(expected_presence, num_containers).collect();
                    assert_eq!(presence, expected);
                }
                other => panic!("unexpected pruning result: {other:?}"),
            }
        }
    }

    #[test]
    fn lit_various_types() {
        let cases = vec![
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Float64(Some(1.25)),
            ScalarValue::Decimal128(Some(12345), 10, 2),
        ];
        let num_containers = 2;

        for scalar in cases {
            let expected_value = scalar.clone();
            let lit_expr = lit(scalar);
            let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(
                num_containers,
            ))));
            let stat = lit_expr
                .evaluate_statistics_vectorized(ctx)
                .expect("pruning ok")
                .expect("pruning result");

            match stat {
                PropagatedIntermediate::IntermediateStats(ColumnStats {
                    range_stats: Some(range_stats),
                    null_stats,
                    num_containers: stats_num_containers,
                }) => {
                    assert_eq!(range_stats.len(), num_containers);
                    assert_eq!(stats_num_containers, num_containers);
                    let mins = range_stats.mins.as_ref().expect("mins");
                    let maxs = range_stats.maxs.as_ref().expect("maxs");
                    match (mins, maxs) {
                        (ColumnarValue::Scalar(min), ColumnarValue::Scalar(max)) => {
                            assert_eq!(min, &expected_value);
                            assert_eq!(max, &expected_value);
                        }
                        other => panic!("unexpected range stats: {other:?}"),
                    }

                    let presence: Vec<NullPresence> = null_stats
                        .expect("null stats")
                        .presence()
                        .iter()
                        .map(NullPresence::from_presence_item)
                        .collect();
                    assert_eq!(presence, vec![NullPresence::NoNull; num_containers]);
                }
                other => panic!("unexpected pruning result: {other:?}"),
            }
        }
    }

    // Such predicates should be impossible due to the constant folding optimizer
    // rule, but it's still worth some basic test coverage to ensure they have
    // consistent pruning result evaluation.
    #[test]
    fn compare_literal_literal_prunes() {
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(1))));

        // Cover supported operators and null combinations for literal-vs-literal
        let cases = vec![
            (
                ScalarValue::Int32(Some(5)),
                Operator::Gt,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Int32(Some(2)),
                Operator::Gt,
                ScalarValue::Int32(Some(10)),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(Some(3)),
                Operator::Lt,
                ScalarValue::Int32(Some(5)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Int32(Some(5)),
                Operator::Lt,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(Some(3)),
                Operator::GtEq,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Int32(Some(2)),
                Operator::GtEq,
                ScalarValue::Int32(Some(5)),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(Some(3)),
                Operator::LtEq,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Int32(Some(4)),
                Operator::LtEq,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(Some(3)),
                Operator::Eq,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Int32(Some(3)),
                Operator::NotEq,
                ScalarValue::Int32(Some(3)),
                PruningOutcome::SkipAll,
            ),
            // Null combinations
            (
                ScalarValue::Int32(None),
                Operator::Eq,
                ScalarValue::Int32(None),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(None),
                Operator::Eq,
                ScalarValue::Int32(Some(1)),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(Some(1)),
                Operator::Eq,
                ScalarValue::Int32(None),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Int32(None),
                Operator::NotEq,
                ScalarValue::Int32(Some(1)),
                PruningOutcome::SkipAll,
            ),
            // Different types
            (
                ScalarValue::Utf8(Some("abc".to_string())),
                Operator::Lt,
                ScalarValue::Utf8(Some("abd".to_string())),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Utf8(Some("abd".to_string())),
                Operator::Lt,
                ScalarValue::Utf8(Some("abc".to_string())),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Utf8(Some("abc".to_string())),
                Operator::Eq,
                ScalarValue::Utf8(Some("abc".to_string())),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Date32(Some(1)),
                Operator::Gt,
                ScalarValue::Date32(Some(0)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Date32(Some(1)),
                Operator::Lt,
                ScalarValue::Date32(Some(0)),
                PruningOutcome::SkipAll,
            ),
            (
                ScalarValue::Decimal128(Some(12345), 10, 2),
                Operator::Eq,
                ScalarValue::Decimal128(Some(12345), 10, 2),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Decimal128(Some(12345), 10, 2),
                Operator::NotEq,
                ScalarValue::Decimal128(Some(12346), 10, 2),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Float64(Some(1.5)),
                Operator::Gt,
                ScalarValue::Float64(Some(0.5)),
                PruningOutcome::KeepAll,
            ),
            (
                ScalarValue::Float64(Some(0.5)),
                Operator::Gt,
                ScalarValue::Float64(Some(1.5)),
                PruningOutcome::SkipAll,
            ),
        ];

        for (lhs, op, rhs, expected) in cases {
            let lhs_dbg = lhs.clone();
            let rhs_dbg = rhs.clone();
            let expr = BinaryExpr::new(lit(lhs), op, lit(rhs));
            let res = expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .unwrap()
                .expect("pruning result");
            match res {
                PropagatedIntermediate::IntermediateResult(results) => {
                    let arr = results.as_ref().expect("results");
                    let outcomes: Vec<PruningOutcome> =
                        arr.iter().map(PruningOutcome::from).collect();
                    assert_eq!(
                        outcomes,
                        vec![expected],
                        "case: lhs={lhs_dbg:?} op={op:?} rhs={rhs_dbg:?}"
                    );
                }
                other => panic!("unexpected pruning result: {other:?}"),
            }
        }
    }

    #[test]
    fn compare_literal_literal_prunes_mixed_types() {
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(1))));

        let cases = vec![
            // Different types outputs Unknown even if they're compatible
            (
                ScalarValue::Int32(Some(5)),
                Operator::Gt,
                ScalarValue::Int64(Some(3)),
                PruningOutcome::Unknown,
            ),
            // Incompatible types are treated as unknown for pruning
            (
                ScalarValue::Int32(Some(1)),
                Operator::Gt,
                ScalarValue::Utf8(Some("abc".to_string())),
                PruningOutcome::Unknown,
            ),
        ];

        for (lhs, op, rhs, expected) in cases {
            let lhs_dbg = lhs.clone();
            let rhs_dbg = rhs.clone();
            let expr = BinaryExpr::new(lit(lhs), op, lit(rhs));
            let res = expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .unwrap()
                .expect("pruning result");
            match res {
                PropagatedIntermediate::IntermediateResult(results) => {
                    let arr = results.as_ref().expect("results");
                    let outcomes: Vec<PruningOutcome> =
                        arr.iter().map(PruningOutcome::from).collect();
                    assert_eq!(
                        outcomes,
                        vec![expected],
                        "case: lhs={lhs_dbg:?} op={op:?} rhs={rhs_dbg:?}"
                    );
                }
                other => panic!("unexpected pruning result: {other:?}"),
            }
        }
    }

    #[test]
    fn default_pruning_predicate_ops_return_empty_results() {
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(1))));
        let ops = vec![
            Operator::IsDistinctFrom,
            Operator::IsNotDistinctFrom,
            Operator::LikeMatch,
            Operator::RegexMatch,
        ];

        for op in ops {
            let expr = BinaryExpr::new(
                lit(ScalarValue::Int32(Some(1))),
                op,
                lit(ScalarValue::Int32(Some(2))),
            );
            let res = expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .unwrap()
                .expect("pruning result");
            match res {
                PropagatedIntermediate::IntermediateResult(results) => {
                    assert!(
                        results.as_ref().is_none(),
                        "op {op:?} should return empty pruning result"
                    );
                }
                other => {
                    panic!("expected empty pruning result for {op:?}, got {other:?}")
                }
            }
        }
    }

    #[test]
    fn default_pruning_arith_ops_return_empty_stats() {
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(1))));
        let cases = vec![
            (
                Operator::Plus,
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
            ),
            (
                Operator::BitwiseAnd,
                ScalarValue::Int32(Some(5)),
                ScalarValue::Int32(Some(3)),
            ),
            (
                Operator::StringConcat,
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("b".to_string())),
            ),
        ];

        for (op, lhs, rhs) in cases {
            let lhs_dbg = lhs.clone();
            let rhs_dbg = rhs.clone();
            let expr = BinaryExpr::new(lit(lhs), op, lit(rhs));
            let res = expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .unwrap()
                .expect("pruning result");
            match res {
                PropagatedIntermediate::IntermediateStats(stats) => {
                    assert!(
                        stats.range_stats().is_none() && stats.null_stats().is_none(),
                        "op {op:?} with lhs={lhs_dbg:?} rhs={rhs_dbg:?} should return empty stats, got {stats:?}"
                    );
                }
                other => panic!("expected empty stats for {op:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn compare_column_literal_prunes_vectorized() {
        // Five containers with varying completeness/null patterns:
        // 0: [0, 20]       (fully known)
        // 1: [10, 30]      (fully known)
        // 2: [None, 40]    (min missing)
        // 3: [5, None]     (max missing)
        // 4: [None, None]  (both missing)
        let mins = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(10),
            None,
            Some(5),
            None,
        ]));
        let maxs = Arc::new(Int32Array::from(vec![
            Some(20),
            Some(30),
            Some(40),
            None,
            None,
        ]));
        let null_counts = Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
        ]));
        let row_counts = Arc::new(UInt64Array::from(vec![
            Some(10),
            Some(10),
            Some(10),
            Some(10),
            Some(10),
        ]));
        let stats = Arc::new(single_column_stats(
            "a",
            Some(mins),
            Some(maxs),
            Some(null_counts),
            Some(row_counts),
        ));
        let ctx = Arc::new(PruningContext::new(stats));

        let cases = vec![
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Gt,
                lit(-1),
                vec![
                    PruningOutcome::KeepAll,
                    PruningOutcome::KeepAll,
                    PruningOutcome::Unknown,
                    PruningOutcome::KeepAll,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Gt,
                lit(40),
                vec![
                    PruningOutcome::SkipAll,
                    PruningOutcome::SkipAll,
                    PruningOutcome::SkipAll,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Operator::GtEq,
                lit(10),
                vec![
                    PruningOutcome::Unknown,
                    PruningOutcome::KeepAll,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Operator::LtEq,
                lit(20),
                vec![
                    PruningOutcome::KeepAll,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Eq,
                lit(10),
                vec![
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Operator::NotEq,
                lit(10),
                vec![
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                lit(-1),
                Operator::Lt,
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                vec![
                    PruningOutcome::KeepAll,
                    PruningOutcome::KeepAll,
                    PruningOutcome::Unknown,
                    PruningOutcome::KeepAll,
                    PruningOutcome::Unknown,
                ],
            ),
            (
                lit(40),
                Operator::Lt,
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                vec![
                    PruningOutcome::SkipAll,
                    PruningOutcome::SkipAll,
                    PruningOutcome::SkipAll,
                    PruningOutcome::Unknown,
                    PruningOutcome::Unknown,
                ],
            ),
        ];

        for (lhs, op, rhs, expected) in cases {
            let expr = BinaryExpr::new(lhs.clone(), op, rhs.clone());
            let res = expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .unwrap()
                .expect("pruning result");
            match res {
                PropagatedIntermediate::IntermediateResult(results) => {
                    let arr = results.as_ref().expect("results");
                    let outcomes: Vec<PruningOutcome> =
                        arr.iter().map(PruningOutcome::from).collect();
                    assert_eq!(outcomes, expected);
                }
                other => panic!("unexpected pruning result: {other:?}"),
            }
        }
    }

    #[test]
    fn compare_column_literal_prunes_typed_columns() {
        let num_containers = 2;
        let nulls: ArrayRef = Arc::new(UInt64Array::from(vec![Some(0), Some(0)]));
        let rows: ArrayRef = Arc::new(UInt64Array::from(vec![Some(10), Some(10)]));

        let float_mins =
            Arc::new(Float64Array::from(vec![Some(1.0), Some(-5.0)])) as ArrayRef;
        let float_maxs =
            Arc::new(Float64Array::from(vec![Some(10.0), Some(0.0)])) as ArrayRef;

        let dec_min_vals = vec![
            ScalarValue::Decimal128(Some(1200), 10, 2), // 12.00
            ScalarValue::Decimal128(Some(3000), 10, 2), // 30.00
        ];
        let dec_max_vals = vec![
            ScalarValue::Decimal128(Some(1800), 10, 2), // 18.00
            ScalarValue::Decimal128(Some(4500), 10, 2), // 45.00
        ];
        let dec_mins = ScalarValue::iter_to_array(dec_min_vals).unwrap();
        let dec_maxs = ScalarValue::iter_to_array(dec_max_vals).unwrap();

        let date_mins = ScalarValue::iter_to_array([
            ScalarValue::Date32(Some(0)),
            ScalarValue::Date32(Some(10)),
        ])
        .unwrap();
        let date_maxs = ScalarValue::iter_to_array([
            ScalarValue::Date32(Some(0)),
            ScalarValue::Date32(Some(20)),
        ])
        .unwrap();

        let str_mins = ScalarValue::iter_to_array([
            ScalarValue::Utf8(Some("apple".to_string())),
            ScalarValue::Utf8(Some("orange".to_string())),
        ])
        .unwrap();
        let str_maxs = ScalarValue::iter_to_array([
            ScalarValue::Utf8(Some("banana".to_string())),
            ScalarValue::Utf8(Some("zebra".to_string())),
        ])
        .unwrap();

        let stats = MultiColumnPruningStatistics::new(num_containers)
            .with_column(
                ColumnPruningStatistics::new("f")
                    .with_range(Some(float_mins), Some(float_maxs))
                    .with_nulls(Some(Arc::clone(&nulls)), Some(Arc::clone(&rows))),
            )
            .with_column(
                ColumnPruningStatistics::new("d")
                    .with_range(Some(dec_mins), Some(dec_maxs))
                    .with_nulls(Some(Arc::clone(&nulls)), Some(Arc::clone(&rows))),
            )
            .with_column(
                ColumnPruningStatistics::new("dt")
                    .with_range(Some(date_mins), Some(date_maxs))
                    .with_nulls(Some(Arc::clone(&nulls)), Some(Arc::clone(&rows))),
            )
            .with_column(
                ColumnPruningStatistics::new("s")
                    .with_range(Some(str_mins), Some(str_maxs))
                    .with_nulls(Some(nulls), Some(rows)),
            );
        let ctx = Arc::new(PruningContext::new(Arc::new(stats)));

        type PruningCase = (
            Arc<dyn PhysicalExpr>,
            Operator,
            Arc<dyn PhysicalExpr>,
            Vec<PruningOutcome>,
        );

        let cases: Vec<PruningCase> = vec![
            (
                Arc::new(Column::new("f", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Gt,
                lit(ScalarValue::Float64(Some(0.0))),
                vec![PruningOutcome::KeepAll, PruningOutcome::SkipAll],
            ),
            (
                Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>,
                Operator::GtEq,
                lit(ScalarValue::Decimal128(Some(2500), 10, 2)), // 25.00
                vec![PruningOutcome::SkipAll, PruningOutcome::KeepAll],
            ),
            (
                Arc::new(Column::new("dt", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Lt,
                lit(ScalarValue::Date32(Some(1))),
                vec![PruningOutcome::KeepAll, PruningOutcome::SkipAll],
            ),
            (
                Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
                Operator::Eq,
                lit(ScalarValue::Utf8(Some("banana".to_string()))),
                vec![PruningOutcome::Unknown, PruningOutcome::SkipAll],
            ),
        ];

        for (lhs, op, rhs, expected) in cases {
            let expr = BinaryExpr::new(lhs.clone(), op, rhs.clone());
            let res = expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .unwrap()
                .expect("pruning result");
            match res {
                PropagatedIntermediate::IntermediateResult(results) => {
                    let arr = results.as_ref().expect("results");
                    let outcomes: Vec<PruningOutcome> =
                        arr.iter().map(PruningOutcome::from).collect();
                    assert_eq!(outcomes, expected);
                }
                other => panic!("unexpected pruning result: {other:?}"),
            }
        }
    }

    #[test]
    fn compare_pruning_combines_range_and_null_stats() {
        let num_containers = 9;
        let mins = Arc::new(Int32Array::from(vec![
            Some(1),  // KeepAll + NoNull
            Some(-5), // SkipAll + NoNull
            Some(-1), // Unknown + NoNull
            Some(1),  // KeepAll + Unknown nulls
            Some(-5), // SkipAll + Unknown nulls
            Some(-1), // Unknown + Unknown nulls
            Some(1),  // KeepAll + AllNull
            Some(-5), // SkipAll + AllNull
            Some(-1), // Unknown + AllNull
        ]));
        let maxs = Arc::new(Int32Array::from(vec![
            Some(5),
            Some(-1),
            Some(5),
            Some(5),
            Some(-1),
            Some(5),
            Some(5),
            Some(-1),
            Some(5),
        ]));
        let nulls = Arc::new(UInt64Array::from(vec![
            Some(0), // NoNull
            Some(0), // NoNull
            Some(0), // NoNull
            Some(1), // Unknown
            Some(1), // Unknown
            Some(1), // Unknown
            Some(5), // AllNull
            Some(5), // AllNull
            Some(5), // AllNull
        ]));
        let rows = Arc::new(UInt64Array::from(vec![Some(5); num_containers]));

        let stats = MultiColumnPruningStatistics::new(num_containers).with_column(
            ColumnPruningStatistics::new("a")
                .with_range(Some(mins), Some(maxs))
                .with_nulls(Some(nulls), Some(rows)),
        );
        let ctx = Arc::new(PruningContext::new(Arc::new(stats)));

        let expr = BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(ScalarValue::Int32(Some(0))),
        );

        let res = expr
            .evaluate_statistics_vectorized(ctx)
            .unwrap()
            .expect("pruning result");
        match res {
            PropagatedIntermediate::IntermediateResult(results) => {
                let arr = results.as_ref().expect("results");
                let outcomes: Vec<PruningOutcome> =
                    arr.iter().map(PruningOutcome::from).collect();
                assert_eq!(
                    outcomes,
                    vec![
                        PruningOutcome::KeepAll, // KeepAll + NoNull -> KeepAll
                        PruningOutcome::SkipAll, // SkipAll + NoNull -> SkipAll
                        PruningOutcome::Unknown, // Unknown + NoNull -> Unknown
                        PruningOutcome::Unknown, // KeepAll + Unknown -> Unknown
                        PruningOutcome::SkipAll, // SkipAll + Unknown -> SkipAll
                        PruningOutcome::Unknown, // Unknown + Unknown -> Unknown
                        PruningOutcome::SkipAll, // KeepAll + AllNull -> SkipAll
                        PruningOutcome::SkipAll, // SkipAll + AllNull -> SkipAll
                        PruningOutcome::SkipAll, // Unknown + AllNull -> SkipAll
                    ]
                );
            }
            other => panic!("unexpected pruning result: {other:?}"),
        }
    }

    #[test]
    fn compare_pruning_with_missing_null_stats_defaults_to_unknown() {
        let num_containers = 3;
        let mins = Arc::new(Int32Array::from(vec![Some(1), Some(-5), Some(-1)]));
        let maxs = Arc::new(Int32Array::from(vec![Some(5), Some(-1), Some(5)]));

        // Null statistics are fully missing for all containers.
        let stats = MultiColumnPruningStatistics::new(num_containers).with_column(
            ColumnPruningStatistics::new("a").with_range(Some(mins), Some(maxs)),
        );
        let ctx = Arc::new(PruningContext::new(Arc::new(stats)));

        let expr = BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(ScalarValue::Int32(Some(0))),
        );

        let res = expr
            .evaluate_statistics_vectorized(ctx)
            .unwrap()
            .expect("pruning result");
        match res {
            PropagatedIntermediate::IntermediateResult(results) => {
                let arr = results.as_ref().expect("results");
                let outcomes: Vec<PruningOutcome> =
                    arr.iter().map(PruningOutcome::from).collect();
                assert_eq!(
                    outcomes,
                    vec![
                        PruningOutcome::Unknown, // KeepAll range + missing nulls -> Unknown
                        PruningOutcome::SkipAll, // SkipAll range dominates
                        PruningOutcome::Unknown, // Unknown range + missing nulls -> Unknown
                    ]
                );
            }
            other => panic!("unexpected pruning result: {other:?}"),
        }
    }

    // Test missing stats at different level for cmp expr:
    // - column stat missing entirely
    // - range stat inside column stat missing
    // - either min/max bound inside range stat missing
    #[test]
    fn compare_pruning_expr_cmp_expr_missing_stats() {
        let num_containers = 1;
        let a_mins: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        let a_maxs: ArrayRef = Arc::new(Int32Array::from(vec![Some(10)]));
        let b_mins: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let b_maxs: ArrayRef = Arc::new(Int32Array::from(vec![Some(11)]));

        let make_ctx = |a_mins: Option<ArrayRef>,
                        a_maxs: Option<ArrayRef>,
                        b_mins: Option<ArrayRef>,
                        b_maxs: Option<ArrayRef>| {
            let stats = MultiColumnPruningStatistics::new(num_containers)
                .with_column(ColumnPruningStatistics::new("a").with_range(a_mins, a_maxs))
                .with_column(
                    ColumnPruningStatistics::new("b").with_range(b_mins, b_maxs),
                );
            Arc::new(PruningContext::new(Arc::new(stats)))
        };

        let ctx_full = make_ctx(
            Some(Arc::clone(&a_mins)),
            Some(Arc::clone(&a_maxs)),
            Some(Arc::clone(&b_mins)),
            Some(Arc::clone(&b_maxs)),
        );
        let ctx_left_range_missing = make_ctx(
            None,
            None,
            Some(Arc::clone(&b_mins)),
            Some(Arc::clone(&b_maxs)),
        );
        let ctx_right_range_missing = make_ctx(
            Some(Arc::clone(&a_mins)),
            Some(Arc::clone(&a_maxs)),
            None,
            None,
        );
        let ctx_left_min_missing = make_ctx(
            None,
            Some(Arc::clone(&a_maxs)),
            Some(Arc::clone(&b_mins)),
            Some(Arc::clone(&b_maxs)),
        );
        let ctx_right_min_missing = make_ctx(
            Some(Arc::clone(&a_mins)),
            Some(Arc::clone(&a_maxs)),
            None,
            Some(Arc::clone(&b_maxs)),
        );
        let ctx_left_max_missing = make_ctx(
            Some(Arc::clone(&a_mins)),
            None,
            Some(Arc::clone(&b_mins)),
            Some(Arc::clone(&b_maxs)),
        );
        let ctx_right_max_missing = make_ctx(
            Some(Arc::clone(&a_mins)),
            Some(Arc::clone(&a_maxs)),
            Some(Arc::clone(&b_mins)),
            None,
        );

        let col_a = || Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b = || Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let neg_a = || Arc::new(NegativeExpr::new(col_a())) as Arc<dyn PhysicalExpr>;
        let neg_b = || Arc::new(NegativeExpr::new(col_b())) as Arc<dyn PhysicalExpr>;

        enum Expected {
            NoPruning,
            EmptyResults,
        }

        let cases = vec![
            (
                "lhs_pruning_none",
                neg_a(),
                col_b(),
                Arc::clone(&ctx_full),
                Expected::NoPruning,
            ),
            (
                "rhs_pruning_none",
                col_a(),
                neg_b(),
                Arc::clone(&ctx_full),
                Expected::NoPruning,
            ),
            (
                "lhs_range_stats_none",
                col_a(),
                col_b(),
                Arc::clone(&ctx_left_range_missing),
                Expected::EmptyResults,
            ),
            (
                "rhs_range_stats_none",
                col_a(),
                col_b(),
                Arc::clone(&ctx_right_range_missing),
                Expected::EmptyResults,
            ),
            (
                "lhs_range_min_missing",
                col_a(),
                col_b(),
                Arc::clone(&ctx_left_min_missing),
                Expected::EmptyResults,
            ),
            (
                "rhs_range_min_missing",
                col_a(),
                col_b(),
                Arc::clone(&ctx_right_min_missing),
                Expected::EmptyResults,
            ),
            (
                "lhs_range_max_missing",
                col_a(),
                col_b(),
                Arc::clone(&ctx_left_max_missing),
                Expected::EmptyResults,
            ),
            (
                "rhs_range_max_missing",
                col_a(),
                col_b(),
                Arc::clone(&ctx_right_max_missing),
                Expected::EmptyResults,
            ),
        ];

        for (case, lhs, rhs, ctx, expected) in cases {
            let expr = BinaryExpr::new(lhs, Operator::Gt, rhs);
            let res = expr.evaluate_statistics_vectorized(ctx).unwrap();
            match expected {
                Expected::NoPruning => {
                    assert!(res.is_none(), "case {case}: expected None, got {res:?}");
                }
                Expected::EmptyResults => match res {
                    Some(PropagatedIntermediate::IntermediateResult(results)) => {
                        assert!(
                            results.as_ref().is_none(),
                            "case {case}: expected empty pruning results, got {results:?}"
                        );
                    }
                    Some(other) => {
                        panic!("case {case}: expected pruning results, got {other:?}")
                    }
                    None => panic!("case {case}: expected pruning results, got None"),
                },
            }
        }
    }

    #[test]
    fn compare_pruning_column_to_column_null_combinations() {
        let num_containers = 5;
        // Construct ranges for a > b.
        // idx 0-2: left always greater than right (KeepAll range)
        // idx 3: left always less/equal (SkipAll range)
        // idx 4: overlapping (Unknown range)
        let a_mins: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(1),
            Some(-5),
            Some(-1),
        ]));
        let a_maxs: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(5),
            Some(5),
            Some(5),
            Some(-1),
            Some(5),
        ]));
        let b_mins: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(-5),
            Some(-5),
            Some(-5),
            Some(5),
            Some(-5),
        ]));
        let b_maxs: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(-1),
            Some(-1),
            Some(-1),
            Some(10),
            Some(5),
        ]));

        // All containers are NoNull for both columns.
        let a_nulls: ArrayRef =
            Arc::new(UInt64Array::from(vec![Some(0); num_containers]));
        let b_nulls: ArrayRef =
            Arc::new(UInt64Array::from(vec![Some(0); num_containers]));
        let rows: ArrayRef = Arc::new(UInt64Array::from(vec![Some(5); num_containers]));

        let stats = MultiColumnPruningStatistics::new(num_containers)
            .with_column(
                ColumnPruningStatistics::new("a")
                    .with_range(Some(Arc::clone(&a_mins)), Some(Arc::clone(&a_maxs)))
                    .with_nulls(Some(Arc::clone(&a_nulls)), Some(Arc::clone(&rows))),
            )
            .with_column(
                ColumnPruningStatistics::new("b")
                    .with_range(Some(Arc::clone(&b_mins)), Some(Arc::clone(&b_maxs)))
                    .with_nulls(Some(Arc::clone(&b_nulls)), Some(Arc::clone(&rows))),
            );
        let ctx = Arc::new(PruningContext::new(Arc::new(stats)));

        let expr = BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Column::new("b", 0)),
        );

        let res = expr
            .evaluate_statistics_vectorized(ctx)
            .unwrap()
            .expect("pruning result");
        match res {
            PropagatedIntermediate::IntermediateResult(results) => {
                let arr = results.as_ref().expect("results");
                let outcomes: Vec<PruningOutcome> =
                    arr.iter().map(PruningOutcome::from).collect();
                assert_eq!(
                    outcomes,
                    vec![
                        PruningOutcome::KeepAll, // KeepAll range + NoNull
                        PruningOutcome::KeepAll, // KeepAll range + NoNull
                        PruningOutcome::KeepAll, // KeepAll range + NoNull
                        PruningOutcome::SkipAll, // SkipAll range dominates
                        PruningOutcome::Unknown, // Unknown range
                    ]
                );
            }
            other => panic!("unexpected pruning result: {other:?}"),
        }
    }

    /// Testing `PhysicalExpr`s that has not implemented the statistics propagation
    /// with `PhysicalExpr::evaluate_statistics_vectorized` API. They should return `None` from
    /// the default implementation, implementation output pruning statistics not
    /// available.
    #[test]
    fn pruning_unsupported_exprs_returns_none() {
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(3))));

        // Predicate expression (NotExpr) should surface lack of pruning info.
        let res_bool = NotExpr::new(lit(true))
            .evaluate_statistics_vectorized(Arc::clone(&ctx))
            .expect("pruning");
        assert!(res_bool.is_none());

        // Non-predicate expression (NegativeExpr) should surface lack of pruning info.
        let res_int = NegativeExpr::new(lit(1i32))
            .evaluate_statistics_vectorized(ctx)
            .expect("pruning");
        assert!(res_int.is_none());
        // Logical binary operator pruning is unimplemented; expect no pruning info.
        let binary_expr = BinaryExpr::new(lit(true), Operator::And, lit(false));
        let res_bin = binary_expr
            .evaluate_statistics_vectorized(Arc::new(PruningContext::new(Arc::new(
                DummyStats::new(2),
            ))))
            .expect("pruning");
        assert!(res_bin.is_none());

        // UDFs should also yield no pruning info.
        let num_containers = 4;
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats::new(
            num_containers,
        ))));
        let schema_str = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let bool_args = vec![
            Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>,
            lit("prefix"),
        ];
        // Boolean-returning built-in UDF (regexp_match) should yield no pruning results.
        let bool_udf = datafusion_functions::string::starts_with();
        let bool_expr = ScalarFunctionExpr::try_new(
            bool_udf,
            bool_args,
            &schema_str,
            Arc::new(datafusion_common::config::ConfigOptions::new()),
        )
        .expect("bool udf");
        assert!(
            bool_expr
                .evaluate_statistics_vectorized(Arc::clone(&ctx))
                .expect("pruning")
                .is_none()
        );

        // Non-boolean built-in UDF (abs) should yield no statistics.
        let schema_int = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let int_args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
        let int_udf = datafusion_functions::math::abs();
        let int_expr = ScalarFunctionExpr::try_new(
            int_udf,
            int_args,
            &schema_int,
            Arc::new(datafusion_common::config::ConfigOptions::new()),
        )
        .expect("int udf");
        assert!(
            int_expr
                .evaluate_statistics_vectorized(ctx)
                .expect("pruning")
                .is_none()
        );
    }
}
