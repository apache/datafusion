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

use arrow::datatypes::Field;
use datafusion_common::Result;
use datafusion_common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawDictionaryExpr};
use datafusion_expr::{lit, Expr};

use super::named_struct;

#[derive(Default, Debug)]
pub struct CoreFunctionPlanner {}

impl ExprPlanner for CoreFunctionPlanner {
    fn plan_dictionary_literal(
        &self,
        expr: RawDictionaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawDictionaryExpr>> {
        let mut args = vec![];
        for (k, v) in expr.keys.into_iter().zip(expr.values.into_iter()) {
            args.push(k);
            args.push(v);
        }
        Ok(PlannerResult::Planned(named_struct().call(args)))
    }

    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        is_named_struct: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(
                if is_named_struct {
                    crate::core::named_struct()
                } else {
                    crate::core::r#struct()
                },
                args,
            ),
        )))
    }

    fn plan_overlay(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::string::overlay(), args),
        )))
    }

    fn plan_compound_identifier(
        &self,
        field: &Field,
        qualifier: Option<&TableReference>,
        nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        let col = Expr::Column(Column::from((qualifier, field)));

        // Start with the base column expression
        let mut expr = col;

        // Iterate over nested_names and create nested get_field expressions
        for nested_name in nested_names {
            let get_field_args = vec![expr, lit(ScalarValue::from(nested_name.clone()))];
            expr = Expr::ScalarFunction(ScalarFunction::new_udf(
                crate::core::get_field(),
                get_field_args,
            ));
        }

        Ok(PlannerResult::Planned(expr))
    }
}
