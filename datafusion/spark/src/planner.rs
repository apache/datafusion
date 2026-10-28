// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult};

#[derive(Default, Debug)]
pub struct SparkFunctionPlanner;

impl ExprPlanner for SparkFunctionPlanner {
    fn plan_extract(
        &self,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::function::datetime::date_part(), args),
        )))
    }

    fn plan_substring(
        &self,
        args: Vec<Expr>,
    ) -> datafusion_common::Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::function::string::substring(), args),
        )))
    }
}
