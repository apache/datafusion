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

use std::sync::Arc;

use arrow::array::{Int32Array, UInt64Array};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{Column, lit};
use datafusion_physical_expr_common::physical_expr::{
    ColumnStats, PruningContext, PruningIntermediate, RangeStats,
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
