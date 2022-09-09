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
use datafusion_expr::binary_rule::{coerce_types, comparison_coercion};
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion};
use datafusion_expr::type_coercion::data_types;
use datafusion_expr::utils::from_plan;
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_expr::{ExprSchemable, Signature};
use std::sync::Arc;

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

        // get schema representing all available input fields. This is used for data type
        // resolution only, so order does not matter here
        let schema = new_inputs.iter().map(|input| input.schema()).fold(
            DFSchema::empty(),
            |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            },
        );

        let mut expr_rewrite = TypeCoercionRewriter {
            schema: Arc::new(schema),
        };

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
        match expr {
            Expr::BinaryExpr {
                ref left,
                op,
                ref right,
            } => {
                let left_type = left.get_type(&self.schema)?;
                let right_type = right.get_type(&self.schema)?;
                match (&left_type, &right_type) {
                    (
                        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                        &DataType::Interval(_),
                    ) => {
                        // Arrow `can_cast_types` says we cannot cast an Interval to
                        // Date32/Date64/Timestamp, which contradicts DataFusion's `coerce_types`
                        Ok(expr.clone())
                    }
                    _ => {
                        let coerced_type = coerce_types(&left_type, &op, &right_type)?;
                        Ok(Expr::BinaryExpr {
                            left: Box::new(
                                left.clone().cast_to(&coerced_type, &self.schema)?,
                            ),
                            op,
                            right: Box::new(
                                right.clone().cast_to(&coerced_type, &self.schema)?,
                            ),
                        })
                    }
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr_type = expr.get_type(&self.schema)?;
                let low_type = low.get_type(&self.schema)?;
                let coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {} and {} in BETWEEN expression",
                            expr_type, low_type
                        ))
                    })?;
                Ok(Expr::Between {
                    expr: Box::new(expr.cast_to(&coerced_type, &self.schema)?),
                    negated,
                    low: Box::new(low.cast_to(&coerced_type, &self.schema)?),
                    high: Box::new(high.cast_to(&coerced_type, &self.schema)?),
                })
            }
            Expr::ScalarUDF { fun, args } => {
                let new_expr = coerce_arguments_for_signature(
                    args.as_slice(),
                    &self.schema,
                    &fun.signature,
                )?;
                Ok(Expr::ScalarUDF {
                    fun,
                    args: new_expr,
                })
            }
            expr => Ok(expr),
        }
    }
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature(
    expressions: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(vec![]);
    }

    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types(&current_types, signature)?;

    expressions
        .iter()
        .enumerate()
        .map(|(i, expr)| expr.clone().cast_to(&new_types[i], schema))
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod test {
    use crate::type_coercion::TypeCoercion;
    use crate::{OptimizerConfig, OptimizerRule};
    use arrow::datatypes::DataType;
    use datafusion_common::{DFSchema, Result, ScalarValue};
    use datafusion_expr::{
        lit,
        logical_plan::{EmptyRelation, Projection},
        Expr, LogicalPlan, Operator, ReturnTypeFunction, ScalarFunctionImplementation,
        ScalarUDF, Signature, Volatility,
    };
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

    #[test]
    fn scalar_udf() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
        let fun: ScalarFunctionImplementation = Arc::new(move |_| unimplemented!());
        let udf = Expr::ScalarUDF {
            fun: Arc::new(ScalarUDF::new(
                "TestScalarUDF",
                &Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
                &return_type,
                &fun,
            )),
            args: vec![lit(123_i32)],
        };
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: TestScalarUDF(CAST(Int32(123) AS Float32))\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn scalar_udf_invalid_input() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
        let fun: ScalarFunctionImplementation = Arc::new(move |_| unimplemented!());
        let udf = Expr::ScalarUDF {
            fun: Arc::new(ScalarUDF::new(
                "TestScalarUDF",
                &Signature::uniform(1, vec![DataType::Int32], Volatility::Stable),
                &return_type,
                &fun,
            )),
            args: vec![lit("Apple")],
        };
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).err().unwrap();
        assert_eq!(
            "Plan(\"Coercion from [Utf8] to the signature Uniform(1, [Int32]) failed.\")",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn binary_op_date32_add_interval() -> Result<()> {
        //CAST(Utf8("1998-03-18") AS Date32) + IntervalDayTime("386547056640")
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Cast {
                expr: Box::new(lit("1998-03-18")),
                data_type: DataType::Date32,
            }),
            op: Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::IntervalDayTime(Some(
                386547056640,
            )))),
        };
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: CAST(Utf8(\"1998-03-18\") AS Date32) + IntervalDayTime(\"386547056640\")\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    fn empty() -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }))
    }
}
