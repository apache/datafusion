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

//! Logical plans need to provide stable semantics, as downstream projects
//! create them and depend on them. Test executable semantics of logical plans.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::{provider_as_source, ViewTable};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_common::{Column, DFSchema, DFSchemaRef, Result, ScalarValue, Spans};
use datafusion_execution::TaskContext;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::logical_plan::{LogicalPlan, Values};
use datafusion_expr::{
    Aggregate, AggregateUDF, EmptyRelation, Expr, LogicalPlanBuilder, UNNAMED_TABLE,
};
use datafusion_functions_aggregate::count::Count;
use datafusion_physical_plan::collect;
use insta::assert_snapshot;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

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
            vec![Expr::Literal(ScalarValue::Null, None)],
            vec![Expr::Literal(ScalarValue::Null, None)],
            vec![Expr::Literal(ScalarValue::Null, None)],
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
            params: AggregateFunctionParams {
                args: vec![input_col_ref],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
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
        panic!("Expected exactly one element, got {elements:?}");
    };
    element
}

#[test]
fn inline_scan_projection_test() -> Result<()> {
    let name = UNNAMED_TABLE;
    let column = "a";

    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);
    let projection = vec![schema.index_of(column)?];

    let provider = ViewTable::new(
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
        }),
        None,
    );
    let source = provider_as_source(Arc::new(provider));

    let plan = LogicalPlanBuilder::scan(name, source, Some(projection))?.build()?;

    assert_snapshot!(
        format!("{plan}"),
        @r"
    SubqueryAlias: ?table?
      Projection: a
        EmptyRelation
    "
    );

    Ok(())
}
