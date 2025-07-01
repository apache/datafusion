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

//! Tests for deeply nested plans causing stack overflows

#[cfg(test)]
mod tests {
    use crate::utils::test::add_plan_schemas_to_ctx;
    use datafusion::common::Result;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use futures::StreamExt;
    use serde_json::{json, Value};
    use substrait::proto::Plan;

    // The depth of the nested plan to generate (number of arguments in literal list)
    const DEPTH: usize = 1000;

    #[tokio::test]
    async fn test_stack_overflow_planning() -> Result<()> {
        let (ctx, plan) = setup().await?;
        ctx.state().create_physical_plan(&plan).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_stack_overflow_execution() -> Result<()> {
        let (ctx, plan) = setup().await?;
        let plan = ctx.state().create_physical_plan(&plan).await?;
        let mut records =
            datafusion::physical_plan::execute_stream(plan, ctx.task_ctx().clone())?;
        while let Some(record) = records.next().await {
            record?;
        }

        Ok(())
    }

    /// Setup returns a session context and a logical plan for a deeply nested substrait plan.
    async fn setup() -> Result<(SessionContext, LogicalPlan)> {
        let proto = generate_deep_plan(DEPTH);

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        Ok((ctx, plan))
    }

    /// Generate a deeply nested substrait plan by extending the arguments of the scalar function
    /// in deeply_nested_tpl.json. This avoids committing a large json file to the repo.
    fn generate_deep_plan(depth: usize) -> Plan {
        let template = include_str!("../testdata/test_plans/deeply_nested_tpl.json");
        let mut data: Value =
            serde_json::from_str(template).expect("failed to parse json");

        // Locate the `arguments` array we want to extend
        let args = data
        .pointer_mut("/relations/0/root/input/project/input/aggregate/input/filter/condition/scalarFunction/arguments/2/value/scalarFunction/arguments")
        .and_then(Value::as_array_mut)
        .expect("couldn't find the arguments array");

        // Insert N new arguments
        for i in 1..depth {
            let new_arg = json!(  {
              "value": {
                "scalarFunction": {
                  "functionReference": 2,
                  "outputType": {
                    "bool": {
                      "nullability": "NULLABILITY_NULLABLE"
                    }
                  },
                  "arguments": [
                    {
                      "value": {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 2
                            }
                          },
                          "rootReference": {}
                        }
                      }
                    },
                    {
                      "value": {
                        "literal": {
                          "string": format!("VALUE_{}", i)
                        }
                      }
                    }
                  ]
                }
              }
            });
            args.push(new_arg);
        }

        serde_json::from_value(data).expect("failed to deserialize from value")
    }
}
