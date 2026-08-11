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

//! The window execs and their window functions.

use super::roundtrip_test;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_window::nth_value::nth_value_udwf;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::window::{SlidingAggregateWindowExpr, StandardWindowExpr};
use datafusion::physical_plan::InputOrderMode;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{PhysicalSortExpr, cast, col, lit};
use datafusion::physical_plan::windows::{
    BoundedWindowAggExec, PlainAggregateWindowExpr, WindowAggExec,
    create_udwf_window_expr,
};
use datafusion::scalar::ScalarValue;
use datafusion_common::Result;
use datafusion_expr::{WindowFrame, WindowFrameBound};
use datafusion_functions_aggregate::average::avg_udaf;
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_udwf() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let udwf_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(
            &row_number_udwf(),
            &[],
            &schema,
            "row_number() PARTITION BY [a] ORDER BY [b] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string(),
            false,
        )?,
        &[
            col("a", &schema)?
        ],
        &[
            PhysicalSortExpr::new(col("b", &schema)?, SortOptions::new(true, true))
        ],
        Arc::new(WindowFrame::new(None)),
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(BoundedWindowAggExec::try_new(
        vec![udwf_expr],
        input,
        InputOrderMode::Sorted,
        true,
    )?))
}

#[test]
fn roundtrip_window() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    let window_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Range,
        WindowFrameBound::Preceding(ScalarValue::Int64(None)),
        WindowFrameBound::CurrentRow,
    );

    let nth_value_window =
        create_udwf_window_expr(
            &nth_value_udwf(),
            &[col("a", &schema)?,
                lit(2)], schema.as_ref(),
            "NTH_VALUE(a, 2) PARTITION BY [b] ORDER BY [a ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string(),
            false,
        )?;
    let udwf_expr = Arc::new(StandardWindowExpr::new(
        nth_value_window,
        &[col("b", &schema)?],
        &[PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }],
        Arc::new(window_frame),
    ));

    let plain_aggr_window_expr = Arc::new(PlainAggregateWindowExpr::new(
        AggregateExprBuilder::new(
            avg_udaf(),
            vec![cast(col("b", &schema)?, &schema, DataType::Float64)?],
        )
        .schema(Arc::clone(&schema))
        .alias("avg(b)")
        .build()
        .map(Arc::new)?,
        &[],
        &[],
        Arc::new(WindowFrame::new(None)),
        None,
    ));

    let window_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Range,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Preceding(ScalarValue::Int64(None)),
    );

    let args = vec![cast(col("a", &schema)?, &schema, DataType::Float64)?];
    let sum_expr = AggregateExprBuilder::new(sum_udaf(), args)
        .schema(Arc::clone(&schema))
        .alias("SUM(a) RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING")
        .build()
        .map(Arc::new)?;

    let sliding_aggr_window_expr = Arc::new(SlidingAggregateWindowExpr::new(
        sum_expr,
        &[],
        &[],
        Arc::new(window_frame),
        None,
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(WindowAggExec::try_new(
        vec![plain_aggr_window_expr, sliding_aggr_window_expr, udwf_expr],
        input,
        false,
    )?))
}

#[test]
fn roundtrip_window_distinct() -> Result<()> {
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    // Create a distinct count window expression with unbounded frame (becomes PlainAggregateWindowExpr)
    let distinct_count_expr = Arc::new(PlainAggregateWindowExpr::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("count(DISTINCT a)")
            .distinct() // Enable distinct
            .build()
            .map(Arc::new)?,
        &[col("b", &schema)?],            // partition by b
        &[],                              // no order by
        Arc::new(WindowFrame::new(None)), // unbounded frame
        None,
    ));

    // Create a distinct sum window expression with bounded frame (becomes SlidingAggregateWindowExpr)
    let bounded_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        WindowFrameBound::CurrentRow,
    );

    let distinct_sum_expr = Arc::new(SlidingAggregateWindowExpr::new(
        AggregateExprBuilder::new(
            sum_udaf(),
            vec![cast(col("a", &schema)?, &schema, DataType::Float64)?],
        )
        .schema(Arc::clone(&schema))
        .alias("sum(DISTINCT a)")
        .distinct() // Enable distinct
        .with_ignore_nulls(true) // Enable ignore nulls
        .build()
        .map(Arc::new)?,
        &[],                     // no partition by
        &[],                     // no order by
        Arc::new(bounded_frame), // bounded frame
        None,
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(WindowAggExec::try_new(
        vec![distinct_count_expr, distinct_sum_expr],
        input,
        false,
    )?))
}

#[test]
fn test_distinct_window_serialization_end_to_end() -> Result<()> {
    // Create a more comprehensive test that verifies distinct window functions
    // work properly through the entire serialization/deserialization pipeline
    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    // Test 1: DISTINCT COUNT with IGNORE NULLS
    let distinct_count_ignore_nulls = Arc::new(PlainAggregateWindowExpr::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("a", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("count_distinct_ignore_nulls")
            .distinct()
            .with_ignore_nulls(true)
            .build()
            .map(Arc::new)?,
        &[col("b", &schema)?],
        &[],
        Arc::new(WindowFrame::new(None)),
        None,
    ));

    // Test 2: DISTINCT SUM (without ignore nulls)
    let bounded_frame = WindowFrame::new_bounds(
        datafusion_expr::WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
        WindowFrameBound::CurrentRow,
    );

    let distinct_sum = Arc::new(SlidingAggregateWindowExpr::new(
        AggregateExprBuilder::new(
            sum_udaf(),
            vec![cast(col("a", &schema)?, &schema, DataType::Float64)?],
        )
        .schema(Arc::clone(&schema))
        .alias("sum_distinct")
        .distinct()
        .build()
        .map(Arc::new)?,
        &[],
        &[],
        Arc::new(bounded_frame),
        None,
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    let window_exec = Arc::new(WindowAggExec::try_new(
        vec![distinct_count_ignore_nulls, distinct_sum],
        input,
        false,
    )?);

    // Perform the roundtrip test
    roundtrip_test(window_exec)
}

/// Tests that `lead` window function with offset and default value args
/// survives a protobuf round-trip. This is a regression test for a bug
/// where `expressions()` (used during serialization) returns only the
/// column expression for lead/lag, silently dropping the offset and
/// default value literal args.
#[test]
fn roundtrip_lead_with_default_value() -> Result<()> {
    use datafusion::functions_window::lead_lag::lead_udwf;

    let field_a = Field::new("a", DataType::Int64, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));

    // lead(a, 2, 42) — column a, offset 2, default value 42
    let lead_window = create_udwf_window_expr(
        &lead_udwf(),
        &[col("a", &schema)?, lit(2i64), lit(42i64)],
        schema.as_ref(),
        "test lead with default".to_string(),
        false,
    )?;

    let udwf_expr = Arc::new(StandardWindowExpr::new(
        lead_window,
        &[col("b", &schema)?],
        &[PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }],
        Arc::new(WindowFrame::new(None)),
    ));

    let input = Arc::new(EmptyExec::new(schema.clone()));

    roundtrip_test(Arc::new(BoundedWindowAggExec::try_new(
        vec![udwf_expr],
        input,
        InputOrderMode::Sorted,
        true,
    )?))
}
