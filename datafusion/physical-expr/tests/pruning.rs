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
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::pruning::PruningStatistics;
    use datafusion_expr::Operator;
    use datafusion_functions::string;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, Column, in_list, is_null, lit,
    };
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
    fn column_set_stats_present() {
        let mins =
            ScalarValue::iter_to_array(vec![ScalarValue::Utf8(Some("foo".to_string()))])
                .unwrap();
        let maxs =
            ScalarValue::iter_to_array(vec![ScalarValue::Utf8(Some("foo".to_string()))])
                .unwrap();
        let null_counts =
            ScalarValue::iter_to_array(vec![ScalarValue::UInt64(Some(0))]).unwrap();
        let row_counts =
            ScalarValue::iter_to_array(vec![ScalarValue::UInt64(Some(1))]).unwrap();

        let mut set = HashSet::new();
        set.insert(ScalarValue::Utf8(Some("foo".to_string())));
        set.insert(ScalarValue::Utf8(Some("bar".to_string())));

        let pruning_stats = Arc::new(MockPruningStatistics::new_with_sets(
            "c",
            mins,
            maxs,
            null_counts,
            Some(row_counts),
            Some(vec![Some(set.clone())]),
        ));

        let context = Arc::new(PruningContext::new(pruning_stats));
        let column_expr = Column::new("c", 0);

        match column_expr.evaluate_pruning(context).unwrap() {
            PruningIntermediate::IntermediateStats(stats) => {
                let set_stats = stats.set_stats().expect("set stats");
                assert_eq!(set_stats.len(), 1);
                let container_set = set_stats.value_sets()[0]
                    .as_ref()
                    .expect("value set present");
                assert_eq!(container_set, &set);
            }
            other => panic!("expected stats, got {other:?}"),
        }
    }

    #[derive(Clone)]
    struct LenOnlyStats(usize);

    impl PruningStatistics for LenOnlyStats {
        fn min_values(&self, _: &datafusion_common::Column) -> Option<ArrayRef> {
            None
        }

        fn max_values(&self, _: &datafusion_common::Column) -> Option<ArrayRef> {
            None
        }

        fn num_containers(&self) -> usize {
            self.0
        }

        fn null_counts(&self, _: &datafusion_common::Column) -> Option<ArrayRef> {
            None
        }

        fn row_counts(&self, _: &datafusion_common::Column) -> Option<ArrayRef> {
            None
        }

        fn contained(
            &self,
            _: &datafusion_common::Column,
            _: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
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
                set_stats,
            }) => {
                assert_eq!(value, ScalarValue::Int32(Some(5)));
                assert_eq!(length, 0);
                assert!(null_stats.is_none());
                let set_stats = set_stats.expect("set stats");
                assert_eq!(set_stats.len(), 0);
            }
            other => panic!("unexpected pruning result: {other:?}"),
        }
    }

    #[test]
    fn literal_set_stats_present() {
        let lit_expr = lit("foo");
        let ctx = Arc::new(PruningContext::new(Arc::new(LenOnlyStats(2))));
        let stat = lit_expr.evaluate_pruning(ctx).expect("pruning ok");

        match stat {
            PruningIntermediate::IntermediateStats(ColumnStats { set_stats, .. }) => {
                let set_stats = set_stats.expect("set stats");
                assert_eq!(set_stats.len(), 2);
                for values in set_stats.value_sets() {
                    let set = values.as_ref().expect("value set");
                    assert_eq!(set.len(), 1);
                    assert!(set.contains(&ScalarValue::Utf8(Some("foo".to_string()))));
                }
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
                set_stats,
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
                assert!(set_stats.is_none());
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

    #[test]
    fn in_list_literal_set_prunes() {
        let ctx = Arc::new(PruningContext::new(Arc::new(LenOnlyStats(1))));
        let schema = Schema::new(Vec::<FieldRef>::new());
        let expr =
            in_list(lit("foo"), vec![lit("foo"), lit("bar")], &false, &schema).unwrap();

        match expr.evaluate_pruning(ctx).unwrap() {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysTrue])
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn in_list_prunes_with_set_stats() {
        let mins =
            ScalarValue::iter_to_array(vec![ScalarValue::Utf8(Some("foo".to_string()))])
                .unwrap();
        let maxs =
            ScalarValue::iter_to_array(vec![ScalarValue::Utf8(Some("foo".to_string()))])
                .unwrap();
        let null_counts =
            ScalarValue::iter_to_array(vec![ScalarValue::UInt64(Some(0))]).unwrap();

        let mut value_set = HashSet::new();
        value_set.insert(ScalarValue::Utf8(Some("foo".to_string())));
        value_set.insert(ScalarValue::Utf8(Some("bar".to_string())));

        let stats = Arc::new(MockPruningStatistics::new_with_sets(
            "c",
            mins,
            maxs,
            null_counts,
            None,
            Some(vec![Some(value_set)]),
        ));

        let ctx = Arc::new(PruningContext::new(stats));
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8, true)]);
        let expr = in_list(
            Arc::new(Column::new("c", 0)),
            vec![lit("toy"), lit("book")],
            &false,
            &schema,
        )
        .unwrap();

        match expr.evaluate_pruning(ctx).unwrap() {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(results, vec![PruningResult::AlwaysFalse])
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn in_list_prunes_upper_with_set_stats() {
        let mins = ScalarValue::iter_to_array(vec![
            ScalarValue::Utf8(Some("electronic".to_string())),
            ScalarValue::Utf8(Some("chair".to_string())),
            ScalarValue::Utf8(Some("book".to_string())),
        ])
        .unwrap();
        let maxs = ScalarValue::iter_to_array(vec![
            ScalarValue::Utf8(Some("electronic".to_string())),
            ScalarValue::Utf8(Some("chair".to_string())),
            ScalarValue::Utf8(Some("pencil".to_string())),
        ])
        .unwrap();
        let null_counts = ScalarValue::iter_to_array(vec![
            ScalarValue::UInt64(Some(0)),
            ScalarValue::UInt64(Some(0)),
            ScalarValue::UInt64(Some(0)),
        ])
        .unwrap();

        let mut set_true = HashSet::new();
        set_true.insert(ScalarValue::Utf8(Some("electronic".to_string())));

        let mut set_false = HashSet::new();
        set_false.insert(ScalarValue::Utf8(Some("chair".to_string())));

        let mut set_unknown = HashSet::new();
        set_unknown.insert(ScalarValue::Utf8(Some("book".to_string())));
        set_unknown.insert(ScalarValue::Utf8(Some("pencil".to_string())));

        let stats = Arc::new(MockPruningStatistics::new_with_sets(
            "c",
            mins,
            maxs,
            null_counts,
            None,
            Some(vec![Some(set_true), Some(set_false), Some(set_unknown)]),
        ));

        let ctx = Arc::new(PruningContext::new(stats));
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8, true)]);
        let upper_udf = string::upper();
        let upper_expr = ScalarFunctionExpr::try_new(
            upper_udf,
            vec![Arc::new(Column::new("c", 0)) as Arc<dyn PhysicalExpr>],
            &schema,
            Arc::new(ConfigOptions::new()),
        )
        .unwrap();

        let expr = in_list(
            Arc::new(upper_expr),
            vec![lit("ELECTRONIC"), lit("BOOK")],
            &false,
            &schema,
        )
        .unwrap();

        match expr.evaluate_pruning(ctx).unwrap() {
            PruningIntermediate::IntermediateResult(results) => {
                assert_eq!(
                    results,
                    vec![
                        PruningResult::AlwaysTrue,
                        PruningResult::AlwaysFalse,
                        PruningResult::Unknown
                    ]
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
