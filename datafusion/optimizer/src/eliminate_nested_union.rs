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

//! Optimizer rule [TODO]
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::logical_plan::{LogicalPlan, Union};

use crate::optimizer::ApplyOrder;
use std::sync::Arc;

#[derive(Default)]
/// [TODO] add description
pub struct EliminateNestedUnion;

impl EliminateNestedUnion {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateNestedUnion {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Union(union) => {
                let Union { inputs, schema } = union;

                let inputs = inputs
                    .into_iter()
                    .flat_map(|plan| match Arc::as_ref(plan) {
                        LogicalPlan::Union(Union { inputs, .. }) => inputs.clone(),
                        _ => vec![Arc::clone(plan)],
                    })
                    .map(|plan| Ok(plan))
                    .collect::<Result<Vec<_>>>()?;

                let schema = schema.clone();

                Ok(Some(LogicalPlan::Union(Union { inputs, schema })))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_nested_union"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::Union;

    use super::*;
    use crate::test::*;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateNestedUnion::new()), plan, expected)
    }

    #[test]
    fn eliminate_nothing() -> Result<()> {
        let table_1 = test_table_scan_with_name("table_1")?;
        let table_2 = test_table_scan_with_name("table_2")?;

        let schema = table_1.schema().clone();

        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(table_1), Arc::new(table_2)],
            schema,
        });

        let expected = "\
        Union\
        \n  TableScan: table_1\
        \n  TableScan: table_2";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_nested_union() -> Result<()> {
        let table_1 = Arc::new(test_table_scan_with_name("table_1")?);
        let table_2 = Arc::new(test_table_scan_with_name("table_2")?);
        let table_3 = Arc::new(test_table_scan_with_name("table_3")?);
        let table_4 = Arc::new(test_table_scan_with_name("table_4")?);

        let schema = table_1.schema().clone();

        let plan = LogicalPlan::Union(Union {
            inputs: vec![
                table_1,
                Arc::new(LogicalPlan::Union(Union {
                    inputs: vec![
                        table_2,
                        Arc::new(LogicalPlan::Union(Union {
                            inputs: vec![table_3, table_4],
                            schema: schema.clone(),
                        })),
                    ],
                    schema: schema.clone(),
                })),
            ],
            schema: schema.clone(),
        });
        let expected = "\
        Union\
        \n  TableScan: table_1\
        \n  TableScan: table_2\
        \n  TableScan: table_3\
        \n  TableScan: table_4";
        assert_optimized_plan_equal(&plan, expected)
    }
}
