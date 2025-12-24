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

    use arrow::array::{Int32Array, Int64Array, UInt64Array};
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, is_null, lit};
    use datafusion_physical_expr_common::physical_expr::{
        ColumnStats, PruningContext, PruningIntermediate, PruningResult, RangeStats,
    };

    use crate::pruning_utils::{DummyStats, MockPruningStatistics};

    #[test]
    fn column_pruning_uses_parquet_stats() {
        // Dummy stats: two containers with constant value 10 and 3 rows each.
        let pruning_stats = Arc::new(MockPruningStatistics::from_scalar(
            "a",
            ScalarValue::Int32(Some(10)),
            2,
            3,
        ));

        let context = Arc::new(PruningContext::new(pruning_stats));
        let column_expr = Column::new("a", 0);

        match column_expr.evaluate_pruning(context).unwrap() {
            PruningIntermediate::IntermediateStats(stats) => {
                let range_stats = stats.range_stats().expect("range stats");
                assert_eq!(range_stats.len(), 2);

                let (mins, maxs) = match range_stats {
                    RangeStats::Values {
                        mins: Some(mins),
                        maxs: Some(maxs),
                        ..
                    } => (mins.clone(), maxs.clone()),
                    RangeStats::Scalar { value, length } => {
                        let arr = ScalarValue::iter_to_array(
                            std::iter::repeat(value.clone()).take(*length),
                        )
                        .unwrap();
                        (arr.clone(), arr)
                    }
                    _ => panic!("missing min/max stats"),
                };

                let mins = mins.as_any().downcast_ref::<Int32Array>().unwrap();
                let maxs = maxs.as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(mins, &Int32Array::from(vec![Some(10), Some(10)]));
                assert_eq!(maxs, &Int32Array::from(vec![Some(10), Some(10)]));

                let null_stats = stats.null_stats().expect("null stats");
                assert_eq!(null_stats.len(), 2);

                let null_counts = null_stats
                    .null_counts()
                    .expect("null counts")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                assert_eq!(null_counts, &UInt64Array::from(vec![Some(0), Some(0)]));

                let row_counts = null_stats
                    .row_counts()
                    .expect("row counts")
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                assert_eq!(row_counts, &UInt64Array::from(vec![Some(3), Some(3)]));
            }
            other => panic!("expected stats, got {other:?}"),
        }
    }

    #[test]
    fn lit_basic() {
        let lit_expr = lit(5);
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats)));
        let stat = lit_expr.evaluate_pruning(ctx).expect("pruning ok");

        match stat {
            PruningIntermediate::IntermediateStats(ColumnStats {
                range_stats: Some(RangeStats::Scalar { value, length }),
                null_stats,
            }) => {
                assert_eq!(value, ScalarValue::Int32(Some(5)));
                assert_eq!(length, 0);
                assert!(null_stats.is_none());
            }
            other => panic!("unexpected pruning result: {other:?}"),
        }
    }

    #[test]
    fn range_stats_scalar_variant() {
        let stats = RangeStats::new_scalar(ScalarValue::Int64(Some(42)), 3).unwrap();
        assert_eq!(stats.len(), 3);

        let (mins, maxs) = match &stats {
            RangeStats::Scalar { value, length } => {
                let arr = ScalarValue::iter_to_array(
                    std::iter::repeat(value.clone()).take(*length),
                )
                .unwrap();
                (arr.clone(), arr)
            }
            RangeStats::Values { mins, maxs, .. } => {
                (mins.clone().expect("mins"), maxs.clone().expect("maxs"))
            }
        };

        let mins = mins.as_any().downcast_ref::<Int64Array>().unwrap();
        let maxs = maxs.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(mins, &Int64Array::from(vec![Some(42), Some(42), Some(42)]));
        assert_eq!(maxs, &Int64Array::from(vec![Some(42), Some(42), Some(42)]));
    }

    #[test]
    fn compare_literal_literal_prunes() {
        let expr = BinaryExpr::new(lit(5), Operator::Gt, lit(3));
        let ctx = Arc::new(PruningContext::new(Arc::new(DummyStats)));
        let res = expr.evaluate_pruning(ctx).unwrap();
        match res {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysTrue])
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn compare_column_literal_prunes() {
        let stats = Arc::new(MockPruningStatistics::from_scalar(
            "a",
            ScalarValue::Int32(Some(10)),
            2,
            3,
        ));
        let ctx = Arc::new(PruningContext::new(stats));
        let expr = BinaryExpr::new(Arc::new(Column::new("a", 0)), Operator::Lt, lit(5));
        let res = expr.evaluate_pruning(ctx).unwrap();
        match res {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysFalse; 2])
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn compare_column_ranges_prunes() {
        // All ranges strictly greater than the literal -> AlwaysTrue
        let mins = Arc::new(Int32Array::from(vec![Some(10), Some(8)]));
        let maxs = Arc::new(Int32Array::from(vec![Some(20), Some(12)]));
        let zeros = Arc::new(UInt64Array::from(vec![Some(0), Some(0)]));
        let rows = Arc::new(UInt64Array::from(vec![Some(3), Some(3)]));
        let stats = Arc::new(MockPruningStatistics::new(
            "a",
            mins.clone(),
            maxs.clone(),
            zeros.clone(),
            Some(rows.clone()),
        ));
        let ctx = Arc::new(PruningContext::new(stats));
        let expr = BinaryExpr::new(Arc::new(Column::new("a", 0)), Operator::Gt, lit(5));
        let res = expr.evaluate_pruning(ctx).unwrap();
        match res {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysTrue; 2])
            }
            other => panic!("unexpected result: {other:?}"),
        }

        // Mixed range that overlaps the literal -> Unknown
        let mins = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let maxs = Arc::new(Int32Array::from(vec![Some(9), Some(4)]));
        let stats = Arc::new(MockPruningStatistics::new(
            "a",
            mins,
            maxs,
            zeros,
            Some(rows),
        ));
        let ctx = Arc::new(PruningContext::new(stats));
        let res = expr.evaluate_pruning(ctx).unwrap();
        match res {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(
                    results,
                    vec![PruningResult::Unknown, PruningResult::AlwaysFalse]
                )
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn multiply_range_prunes() {
        let mins = Arc::new(Int32Array::from(vec![Some(10)]));
        let maxs = Arc::new(Int32Array::from(vec![Some(20)]));
        let zeros = Arc::new(UInt64Array::from(vec![Some(0)]));
        let rows = Arc::new(UInt64Array::from(vec![Some(1)]));
        let stats = Arc::new(MockPruningStatistics::new(
            "a",
            mins,
            maxs,
            zeros,
            Some(rows),
        ));
        let ctx = Arc::new(PruningContext::new(stats));

        let mult_expr =
            BinaryExpr::new(Arc::new(Column::new("a", 0)), Operator::Multiply, lit(7));
        let mult_stats = mult_expr
            .evaluate_pruning(Arc::clone(&ctx))
            .expect("multiplication pruning ok");
        match mult_stats {
            PruningIntermediate::IntermediateStats(ColumnStats {
                range_stats:
                    Some(RangeStats::Values {
                        mins: Some(mins),
                        maxs: Some(maxs),
                        length,
                    }),
                null_stats,
            }) => {
                assert_eq!(length, 1);
                let min_scalar =
                    ScalarValue::try_from_array(mins.as_ref(), 0).expect("min scalar");
                let max_scalar =
                    ScalarValue::try_from_array(maxs.as_ref(), 0).expect("max scalar");
                let min_scalar = min_scalar.cast_to(&DataType::Int64).unwrap();
                let max_scalar = max_scalar.cast_to(&DataType::Int64).unwrap();
                assert_eq!(min_scalar, ScalarValue::Int64(Some(70)));
                assert_eq!(max_scalar, ScalarValue::Int64(Some(140)));
                assert!(null_stats.is_none());
            }
            other => panic!("unexpected pruning result: {other:?}"),
        }

        // (a*7) < 10
        let expr = BinaryExpr::new(Arc::new(mult_expr), Operator::Lt, lit(10));

        let res = expr.evaluate_pruning(ctx).unwrap();
        match res {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysFalse])
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn is_null_prunes_when_no_nulls() {
        let stats = Arc::new(MockPruningStatistics::from_scalar(
            "b",
            ScalarValue::Int32(Some(1)),
            1,
            3,
        ));
        let ctx = Arc::new(PruningContext::new(stats));
        let expr = is_null(Arc::new(Column::new("b", 0))).unwrap();

        match expr.evaluate_pruning(ctx).unwrap() {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysFalse])
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
