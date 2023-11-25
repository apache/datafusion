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

//! Tests for window queries
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use datafusion_common::{assert_batches_eq, Result, ScalarValue};
use datafusion_execution::config::SessionConfig;
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::expressions::{col, NthValue};
use datafusion_physical_expr::window::{BuiltInWindowExpr, BuiltInWindowFunctionExpr};
use datafusion_physical_plan::memory::MemoryExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, PartitionSearchMode};
use datafusion_physical_plan::{collect, get_plan_string, ExecutionPlan};

// Tests NTH_VALUE(negative index) with memoize feature.
// To be able to trigger memoize feature for NTH_VALUE we need to
// - feed BoundedWindowAggExec with batch stream data.
// - Window frame should contain UNBOUNDED PRECEDING.
// It hard to ensure these conditions are met, from the sql query.
#[tokio::test]
async fn test_window_nth_value_bounded_memoize() -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    // Create a new batch of data to insert into the table
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
    )?;

    let memory_exec = MemoryExec::try_new(
        &[vec![batch.clone(), batch.clone(), batch.clone()]],
        schema.clone(),
        None,
    )
    .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;
    let col_a = col("a", &schema)?;
    let nth_value_func1 =
        NthValue::nth("nth_value(-1)", col_a.clone(), DataType::Int32, 1)?
            .reverse_expr()
            .unwrap();
    let nth_value_func2 =
        NthValue::nth("nth_value(-2)", col_a.clone(), DataType::Int32, 2)?
            .reverse_expr()
            .unwrap();
    let last_value_func =
        Arc::new(NthValue::last("last", col_a.clone(), DataType::Int32)) as _;
    let window_exprs = vec![
        // LAST_VALUE(a)
        Arc::new(BuiltInWindowExpr::new(
            last_value_func,
            &[],
            &[],
            Arc::new(WindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                end_bound: WindowFrameBound::CurrentRow,
            }),
        )) as _,
        // NTH_VALUE(a, -1)
        Arc::new(BuiltInWindowExpr::new(
            nth_value_func1,
            &[],
            &[],
            Arc::new(WindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                end_bound: WindowFrameBound::CurrentRow,
            }),
        )) as _,
        // NTH_VALUE(a, -2)
        Arc::new(BuiltInWindowExpr::new(
            nth_value_func2,
            &[],
            &[],
            Arc::new(WindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                end_bound: WindowFrameBound::CurrentRow,
            }),
        )) as _,
    ];
    let physical_plan = BoundedWindowAggExec::try_new(
        window_exprs,
        memory_exec,
        vec![],
        PartitionSearchMode::Sorted,
    )
    .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;

    let batches = collect(physical_plan.clone(), ctx.task_ctx()).await?;

    let expected = vec![
        "BoundedWindowAggExec: wdw=[last: Ok(Field { name: \"last\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow }, nth_value(-1): Ok(Field { name: \"nth_value(-1)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow }, nth_value(-2): Ok(Field { name: \"nth_value(-2)\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow }], mode=[Sorted]",
        "  MemoryExec: partitions=1, partition_sizes=[3]",
    ];
    // Get string representation of the plan
    let actual = get_plan_string(&physical_plan);
    assert_eq!(
        expected, actual,
        "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = [
        "+---+------+---------------+---------------+",
        "| a | last | nth_value(-1) | nth_value(-2) |",
        "+---+------+---------------+---------------+",
        "| 1 | 1    | 1             |               |",
        "| 2 | 2    | 2             | 1             |",
        "| 3 | 3    | 3             | 2             |",
        "| 1 | 1    | 1             | 3             |",
        "| 2 | 2    | 2             | 1             |",
        "| 3 | 3    | 3             | 2             |",
        "| 1 | 1    | 1             | 3             |",
        "| 2 | 2    | 2             | 1             |",
        "| 3 | 3    | 3             | 2             |",
        "+---+------+---------------+---------------+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}
