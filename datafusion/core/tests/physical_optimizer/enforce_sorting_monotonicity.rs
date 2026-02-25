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

use crate::physical_optimizer::test_utils::{
    create_test_schema, parquet_exec_with_sort, sort_exec, sort_expr_options,
};
use arrow::datatypes::DataType;
use arrow_schema::SortOptions;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::WindowFrameBound;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion_expr::{WindowFrame, WindowFunctionDefinition};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::windows::{
    BoundedWindowAggExec, WindowAggExec, create_window_expr,
};
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode};
use insta::assert_snapshot;
use std::sync::{Arc, LazyLock};

// Function definition - Alias of the resulting column - Arguments of the function
#[derive(Clone)]
struct WindowFuncParam(
    WindowFunctionDefinition,
    &'static str,
    Vec<Arc<dyn PhysicalExpr>>,
);

fn function_arg_ordered() -> Vec<Arc<dyn PhysicalExpr>> {
    let input_schema = create_test_schema().unwrap();
    vec![col("nullable_col", &input_schema).unwrap()]
}
fn function_arg_unordered() -> Vec<Arc<dyn PhysicalExpr>> {
    let input_schema = create_test_schema().unwrap();
    vec![col("non_nullable_col", &input_schema).unwrap()]
}

fn fn_count_on_ordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count",
        function_arg_ordered(),
    )
}

fn fn_max_on_ordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(max_udaf()),
        "max",
        function_arg_ordered(),
    )
}

fn fn_min_on_ordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(min_udaf()),
        "min",
        function_arg_ordered(),
    )
}

fn fn_avg_on_ordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(avg_udaf()),
        "avg",
        function_arg_ordered(),
    )
}

fn fn_count_on_unordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count",
        function_arg_unordered(),
    )
}

fn fn_max_on_unordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(max_udaf()),
        "max",
        function_arg_unordered(),
    )
}
fn fn_min_on_unordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(min_udaf()),
        "min",
        function_arg_unordered(),
    )
}

fn fn_avg_on_unordered() -> WindowFuncParam {
    WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(avg_udaf()),
        "avg",
        function_arg_unordered(),
    )
}

struct TestWindowCase {
    partition_by: bool,
    window_frame: Arc<WindowFrame>,
    func: WindowFuncParam,
    required_sort: Vec<(&'static str, bool, bool)>, // (column name, ascending, nulls_first)
}
impl TestWindowCase {
    fn source() -> Arc<dyn ExecutionPlan> {
        static SOURCE: LazyLock<Arc<dyn ExecutionPlan>> = LazyLock::new(|| {
            let input_schema = create_test_schema().unwrap();
            let ordering = [sort_expr_options(
                "nullable_col",
                &input_schema,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )]
            .into();
            parquet_exec_with_sort(input_schema.clone(), vec![ordering])
        });
        Arc::clone(&SOURCE)
    }

    // runs the window test case and returns the string representation of the plan
    fn run(self) -> String {
        let input_schema = create_test_schema().unwrap();
        let source = Self::source();

        let Self {
            partition_by,
            window_frame,
            func: WindowFuncParam(func_def, func_name, func_args),
            required_sort,
        } = self;
        let partition_by_exprs = if partition_by {
            vec![col("nullable_col", &input_schema).unwrap()]
        } else {
            vec![]
        };

        let window_expr = create_window_expr(
            &func_def,
            func_name.to_string(),
            &func_args,
            &partition_by_exprs,
            &[],
            window_frame,
            Arc::clone(&input_schema),
            false,
            false,
            None,
        )
        .unwrap();

        let window_exec = if window_expr.uses_bounded_memory() {
            Arc::new(
                BoundedWindowAggExec::try_new(
                    vec![window_expr],
                    Arc::clone(&source),
                    InputOrderMode::Sorted,
                    partition_by,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>
        } else {
            Arc::new(
                WindowAggExec::try_new(
                    vec![window_expr],
                    Arc::clone(&source),
                    partition_by,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>
        };

        let output_schema = window_exec.schema();
        let sort_expr = required_sort.into_iter().map(|(col, asc, nulls_first)| {
            sort_expr_options(
                col,
                &output_schema,
                SortOptions {
                    descending: !asc,
                    nulls_first,
                },
            )
        });
        let ordering = LexOrdering::new(sort_expr).unwrap();
        let physical_plan = sort_exec(ordering, window_exec);

        crate::physical_optimizer::enforce_sorting::EnforceSortingTest::new(physical_plan)
            .with_repartition_sorts(true)
            .run()
    }
}
#[test]
fn test_window_partial_constant_and_set_monotonicity_0() {
    // ============================================REGION STARTS============================================
    // WindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on ordered column
    // Case 0:
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_1() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_2() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_3() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("avg", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_4() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("non_nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_5() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("non_nullable_col", false, false),
            ("max", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[non_nullable_col@1 DESC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    SortExec: expr=[non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_6() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("min", true, false),
            ("non_nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[min@2 ASC NULLS LAST, non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_7() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("avg", false, false),
            ("nullable_col", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
            );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================

#[test]
fn test_window_partial_constant_and_set_monotonicity_8() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_9() {
    assert_snapshot!(TestWindowCase  {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_10() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

#[test]
fn test_window_partial_constant_and_set_monotonicity_11() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("avg", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// WindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on unordered column
// Case 12:
#[test]
fn test_window_partial_constant_and_set_monotonicity_12() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("non_nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 13:
#[test]
fn test_window_partial_constant_and_set_monotonicity_13() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("non_nullable_col", true, false),
            ("max", false, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 14:
#[test]
fn test_window_partial_constant_and_set_monotonicity_14() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("min", false, false),
            ("non_nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC NULLS LAST, non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 15:
#[test]
fn test_window_partial_constant_and_set_monotonicity_15() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(None)),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("avg", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// WindowAggExec + Sliding(current row, unbounded following) + no partition_by + on ordered column
// Case 16:
#[test]
fn test_window_partial_constant_and_set_monotonicity_16() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 17:
#[test]
fn test_window_partial_constant_and_set_monotonicity_17() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("max", false, true),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[max@2 DESC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 18:
#[test]
fn test_window_partial_constant_and_set_monotonicity_18() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", true, true),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[min@2 ASC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 19:
#[test]
fn test_window_partial_constant_and_set_monotonicity_19() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("avg", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// WindowAggExec + Sliding(current row, unbounded following) + no partition_by + on unordered column
// Case 20:
#[test]
fn test_window_partial_constant_and_set_monotonicity_20() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 21:
#[test]
fn test_window_partial_constant_and_set_monotonicity_21() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", false, true),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 22:
#[test]
fn test_window_partial_constant_and_set_monotonicity_22() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("min", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 23:
#[test]
fn test_window_partial_constant_and_set_monotonicity_23() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("avg", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// WindowAggExec + Sliding(current row, unbounded following) + partition_by + on ordered column
// Case 24:
#[test]
fn test_window_partial_constant_and_set_monotonicity_24() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 25:
#[test]
fn test_window_partial_constant_and_set_monotonicity_25() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 26:
#[test]
fn test_window_partial_constant_and_set_monotonicity_26() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", false, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#);
}

// Case 27:
#[test]
fn test_window_partial_constant_and_set_monotonicity_27() {
    assert_snapshot!(
        TestWindowCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_avg_on_ordered(),
            required_sort: vec![
                ("avg", false, false),
            ],
        }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#);
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// WindowAggExec + Sliding(current row, unbounded following) + partition_by + on unordered column

// Case 28:
#[test]
fn test_window_partial_constant_and_set_monotonicity_28() {
    assert_snapshot!(
        TestWindowCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_count_on_unordered(),
            required_sort: vec![
                ("count", false, false),
                ("nullable_col", true, false),
            ],
        }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[count@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[count: Ok(Field { name: "count", data_type: Int64 }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 29:
#[test]
fn test_window_partial_constant_and_set_monotonicity_29() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", false, true),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC], preserve_partitioning=[false]
      WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    WindowAggExec: wdw=[max: Ok(Field { name: "max", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#)
}

// Case 30:
#[test]
fn test_window_partial_constant_and_set_monotonicity_30() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("min", false, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[min: Ok(Field { name: "min", data_type: Int32, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#);
}

// Case 31:
#[test]
fn test_window_partial_constant_and_set_monotonicity_31() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("avg", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]
      WindowAggExec: wdw=[avg: Ok(Field { name: "avg", data_type: Float64, nullable: true }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on ordered column

// Case 32:
#[test]
fn test_window_partial_constant_and_set_monotonicity_32() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 33:
#[test]
fn test_window_partial_constant_and_set_monotonicity_33() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("max", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[max@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 34:
#[test]
fn test_window_partial_constant_and_set_monotonicity_34() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}
// Case 35:
#[test]
fn test_window_partial_constant_and_set_monotonicity_35() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
           ("avg", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on unordered column

// Case 36:
#[test]
fn test_window_partial_constant_and_set_monotonicity_36() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, true),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 37:
#[test]
fn test_window_partial_constant_and_set_monotonicity_37() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("max", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[max@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 38:
#[test]
fn test_window_partial_constant_and_set_monotonicity_38() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("min", false, true),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 39:
#[test]
fn test_window_partial_constant_and_set_monotonicity_39() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("avg", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on ordered column

// Case 40:
#[test]
fn test_window_partial_constant_and_set_monotonicity_40() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 41:
#[test]
fn test_window_partial_constant_and_set_monotonicity_41() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("max", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[max@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 42:
#[test]
fn test_window_partial_constant_and_set_monotonicity_42() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 43:
#[test]
fn test_window_partial_constant_and_set_monotonicity_43() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("avg", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on unordered column

// Case 44:
#[test]
fn test_window_partial_constant_and_set_monotonicity_44() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("count", true, true),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[count@2 ASC], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 45:
#[test]
fn test_window_partial_constant_and_set_monotonicity_45() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", false, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 46:
#[test]
fn test_window_partial_constant_and_set_monotonicity_46() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("min", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 47:
#[test]
fn test_window_partial_constant_and_set_monotonicity_47() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new(Some(true))),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + no partition_by + on ordered column

// Case 48:
#[test]
fn test_window_partial_constant_and_set_monotonicity_48() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("count", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 49:
#[test]
fn test_window_partial_constant_and_set_monotonicity_49() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32).unwrap()))),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("max", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[max@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 50:
#[test]
fn test_window_partial_constant_and_set_monotonicity_50() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("min", false, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 51:
#[test]
fn test_window_partial_constant_and_set_monotonicity_51() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("avg", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + no partition_by + on unordered column

// Case 52:
#[test]
fn test_window_partial_constant_and_set_monotonicity_52() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32).unwrap()))),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("count", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 53:
#[test]
fn test_window_partial_constant_and_set_monotonicity_53() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 54:
#[test]
fn test_window_partial_constant_and_set_monotonicity_54() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("min", true, false),
            ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 55:
#[test]
fn test_window_partial_constant_and_set_monotonicity_55() {
    assert_snapshot!(TestWindowCase {
        partition_by: false,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32).unwrap()))),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + partition_by + on ordered column

// Case 56:
#[test]
fn test_window_partial_constant_and_set_monotonicity_56() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_count_on_ordered(),
        required_sort: vec![
            ("count", true, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 57:
#[test]
fn test_window_partial_constant_and_set_monotonicity_57() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32).unwrap()))),
        func: fn_max_on_ordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 58:
#[test]
fn test_window_partial_constant_and_set_monotonicity_58() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_min_on_ordered(),
        required_sort: vec![
            ("min", false, false),
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 59:
#[test]
fn test_window_partial_constant_and_set_monotonicity_59() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_avg_on_ordered(),
        required_sort: vec![
            ("avg", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// =============================================REGION ENDS=============================================
// = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
// ============================================REGION STARTS============================================
// BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + partition_by + on unordered column

// Case 60:
#[test]
fn test_window_partial_constant_and_set_monotonicity_60() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_count_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("count", true, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[count: Field { "count": Int64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 61:
#[test]
fn test_window_partial_constant_and_set_monotonicity_61() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_max_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("max", true, true),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[max: Field { "max": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 62:
#[test]
fn test_window_partial_constant_and_set_monotonicity_62() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_min_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
            ("min", false, false),
        ],
    }.run(),
        @ r#"
    Input / Optimized Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[min: Field { "min": nullable Int32 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}

// Case 63:
#[test]
fn test_window_partial_constant_and_set_monotonicity_63() {
    assert_snapshot!(TestWindowCase {
        partition_by: true,
        window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32).unwrap()), WindowFrameBound::CurrentRow)),
        func: fn_avg_on_unordered(),
        required_sort: vec![
            ("nullable_col", true, false),
        ],
    }.run(),
        @ r#"
    Input Plan:
    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]
      BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet

    Optimized Plan:
    BoundedWindowAggExec: wdw=[avg: Field { "avg": nullable Float64 }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]
      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet
    "#
    );
}
// =============================================REGION ENDS=============================================
