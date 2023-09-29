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

//! Optimizer rule to replace nested unions to single union.
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{
    builder::project_with_column_index,
    expr_rewriter::coerce_plan_expr_for_schema,
    logical_plan::{LogicalPlan, Projection, Union},
};

use crate::optimizer::ApplyOrder;
use std::sync::Arc;

#[derive(Default)]
/// An optimization rule that replaces nested unions with a single union.
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
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Union(union) => {
                let Union { inputs, schema } = union;

                let union_schema = schema.clone();

                let inputs = inputs
                    .into_iter()
                    .flat_map(|plan| match Arc::as_ref(plan) {
                        LogicalPlan::Union(Union { inputs, .. }) => inputs.clone(),
                        _ => vec![Arc::clone(plan)],
                    })
                    .map(|plan| {
                        let plan = coerce_plan_expr_for_schema(&plan, &union_schema)?;
                        match plan {
                            LogicalPlan::Projection(Projection {
                                expr, input, ..
                            }) => Ok(Arc::new(project_with_column_index(
                                expr,
                                input,
                                union_schema.clone(),
                            )?)),
                            _ => Ok(Arc::new(plan)),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Some(LogicalPlan::Union(Union {
                    inputs,
                    schema: union_schema,
                })))
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
    use super::*;
    use crate::test::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::logical_plan::table_scan;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ])
    }

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateNestedUnion::new()), plan, expected)
    }

    #[test]
    fn eliminate_nothing() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .build()?;

        let expected = "\
        Union\
        \n  TableScan: table\
        \n  TableScan: table";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_nested_union() -> Result<()> {
        let plan_builder = table_scan(Some("table"), &schema(), None)?;

        let plan = plan_builder
            .clone()
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .union(plan_builder.clone().build()?)?
            .build()?;

        let expected = "\
        Union\
        \n  TableScan: table\
        \n  TableScan: table\
        \n  TableScan: table\
        \n  TableScan: table";
        assert_optimized_plan_equal(&plan, expected)
    }
}
