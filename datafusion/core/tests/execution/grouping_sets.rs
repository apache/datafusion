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

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{assert_batches_eq, Result};

fn find_aggregate(plan: &dyn ExecutionPlan) -> Option<AggregateExec> {
    if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
        return Some(aggregate.clone());
    }

    for child in plan.children() {
        if let Some(aggregate) = find_aggregate(child.as_ref()) {
            return Some(aggregate);
        }
    }

    None
}

#[tokio::test]
async fn grouping_sets_with_empty_set_emits_grouping_id() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table))?;

    let df = ctx
        .sql("SELECT COUNT(*) FROM t GROUP BY GROUPING SETS (())")
        .await?;

    let physical_plan = df.clone().create_physical_plan().await?;
    let aggregate = find_aggregate(physical_plan.as_ref()).expect("aggregate plan");

    let group_fields: Vec<_> = aggregate
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(group_fields, vec!["__grouping_id", "count(Int64(1))"]);

    let results = df.collect().await?;
    let expected = vec![
        "+----------+",
        "| count(*) |",
        "+----------+",
        "| 3        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
