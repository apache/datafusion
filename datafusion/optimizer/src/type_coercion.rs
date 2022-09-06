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

//! Optimizer rule for type validation and coercion

use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::binary_rule::coerce_types;
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion};
use datafusion_expr::logical_plan::builder::build_join_schema;
use datafusion_expr::logical_plan::JoinType;
use datafusion_expr::utils::from_plan;
use datafusion_expr::ExprSchemable;
use datafusion_expr::{Expr, LogicalPlan};

#[derive(Default)]
pub struct TypeCoercion {}

impl TypeCoercion {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "TypeCoercion"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // optimize child plans first
        let new_inputs = plan
            .inputs()
            .iter()
            .map(|p| self.optimize(p, optimizer_config))
            .collect::<Result<Vec<_>>>()?;

        let schema = match new_inputs.len() {
            1 => new_inputs[0].schema().clone(),
            2 => DFSchemaRef::new(build_join_schema(
                new_inputs[0].schema(),
                new_inputs[1].schema(),
                &JoinType::Inner,
            )?),
            _ => DFSchemaRef::new(DFSchema::empty()),
        };

        let mut expr_rewrite = TypeCoercionRewriter { schema };

        let new_expr = plan
            .expressions()
            .into_iter()
            .map(|expr| expr.rewrite(&mut expr_rewrite))
            .collect::<Result<Vec<_>>>()?;

        from_plan(plan, &new_expr, &new_inputs)
    }
}

struct TypeCoercionRewriter {
    schema: DFSchemaRef,
}

impl ExprRewriter for TypeCoercionRewriter {
    fn pre_visit(&mut self, _expr: &Expr) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match &expr {
            Expr::BinaryExpr { left, op, right } => {
                let left_type = left.get_type(&self.schema)?;
                let right_type = right.get_type(&self.schema)?;
                let coerced_type = coerce_types(&left_type, op, &right_type)?;
                Ok(Expr::BinaryExpr {
                    left: Box::new(
                        left.as_ref().clone().cast_to(&coerced_type, &self.schema)?,
                    ),
                    op: *op,
                    right: Box::new(
                        right
                            .as_ref()
                            .clone()
                            .cast_to(&coerced_type, &self.schema)?,
                    ),
                })
            }
            Expr::Like { pattern, .. }
            | Expr::ILike { pattern, .. }
            | Expr::SimilarTo { pattern, .. } => match pattern.get_type(&self.schema)? {
                DataType::Utf8 => Ok(expr.clone()),
                other => Err(DataFusionError::Plan(format!(
                    "Expected pattern in Like, ILike, or SimilarTo to be Utf8 but was {}",
                    other
                ))),
            },
            expr => Ok(expr.clone()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::type_coercion::TypeCoercion;
    use crate::{OptimizerConfig, OptimizerRule};
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::logical_plan::{EmptyRelation, Projection};
    use datafusion_expr::{lit, LogicalPlan};
    use std::sync::Arc;

    #[test]
    fn simple_case() -> Result<()> {
        let expr = lit(1.2_f64).lt(lit(2_u32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: Float64(1.2) < CAST(UInt32(2) AS Float64)\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn nested_case() -> Result<()> {
        let expr = lit(1.2_f64).lt(lit(2_u32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone().or(expr)],
            empty,
            None,
        )?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!("Projection: Float64(1.2) < CAST(UInt32(2) AS Float64) OR Float64(1.2) < CAST(UInt32(2) AS Float64)\
            \n  EmptyRelation", &format!("{:?}", plan));
        Ok(())
    }
}
