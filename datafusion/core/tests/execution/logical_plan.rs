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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_common::{Column, DFSchema, Result, ScalarValue, Spans};
use datafusion_execution::TaskContext;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::logical_plan::{LogicalPlan, Values};
use datafusion_expr::{Aggregate, AggregateUDF, Expr};
use datafusion_functions_aggregate::count::Count;
use datafusion_physical_plan::collect;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

///! Logical plans need to provide stable semantics, as downstream projects
///! create them and depend on them. Test executable semantics of logical plans.

#[tokio::test]
async fn count_only_nulls() -> Result<()> {
    // Input: VALUES (NULL), (NULL), (NULL) AS _(col)
    let input_schema = Arc::new(DFSchema::from_unqualified_fields(
        vec![Field::new("col", DataType::Null, true)].into(),
        HashMap::new(),
    )?);
    let input = Arc::new(LogicalPlan::Values(Values {
        schema: input_schema,
        values: vec![
            vec![Expr::Literal(ScalarValue::Null)],
            vec![Expr::Literal(ScalarValue::Null)],
            vec![Expr::Literal(ScalarValue::Null)],
        ],
    }));
    let input_col_ref = Expr::Column(Column {
        relation: None,
        name: "col".to_string(),
        spans: Spans::new(),
    });

    // Aggregation: count(col) AS count
    let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        input,
        vec![],
        vec![Expr::AggregateFunction(AggregateFunction {
            func: Arc::new(AggregateUDF::new_from_impl(Count::new())),
            args: vec![input_col_ref],
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: None,
        })],
    )?);

    // Execute and verify results
    let session_state = SessionStateBuilder::new().build();
    let physical_plan = session_state.create_physical_plan(&aggregate).await?;
    let result =
        collect(physical_plan, Arc::new(TaskContext::from(&session_state))).await?;

    let result = only(result.as_slice());
    let result_schema = result.schema();
    let field = only(result_schema.fields().deref());
    let column = only(result.columns());

    assert_eq!(field.data_type(), &DataType::Int64); // TODO should be UInt64
    assert_eq!(column.deref(), &Int64Array::from(vec![0]));

    Ok(())
}

fn only<T>(elements: &[T]) -> &T
where
    T: Debug,
{
    let [element] = elements else {
        panic!("Expected exactly one element, got {:?}", elements);
    };
    element
}
