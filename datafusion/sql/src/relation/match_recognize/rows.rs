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

//! ROWS PER MATCH planning utilities for MATCH_RECOGNIZE.
//!
//! This module translates the SQL-facing `ROWS PER MATCH` and
//! `AFTER MATCH SKIP` specifications into their internal representations and
//! applies the final projection for the selected mode. It also post-processes
//! the projection to normalize `CLASSIFIER()` values when exclusions created
//! shadow symbols, ensuring user-visible output remains consistent.

use crate::planner::{ContextProvider, SqlToRel};
use crate::relation::match_recognize::exclusions::ExclusionContext;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::logical_plan::NamedExpr;
use datafusion_expr::match_recognize::{
    rows_projection_expr, AfterMatchSkip, EmptyMatchesMode, RowsPerMatch,
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    AfterMatchSkip as SQLAfterMatchSkip, EmptyMatchesMode as SQLEmptyMatchesMode,
    RowsPerMatch as SQLRowsPerMatch,
};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn convert_rows_per_match(
        rows_per_match: Option<SQLRowsPerMatch>,
    ) -> RowsPerMatch {
        match rows_per_match {
            None | Some(SQLRowsPerMatch::OneRow) => RowsPerMatch::OneRow,
            Some(SQLRowsPerMatch::AllRows(mode)) => {
                let converted_mode = match mode {
                    Some(SQLEmptyMatchesMode::Show) | None => EmptyMatchesMode::Show,
                    Some(SQLEmptyMatchesMode::Omit) => EmptyMatchesMode::Omit,
                    Some(SQLEmptyMatchesMode::WithUnmatched) => {
                        EmptyMatchesMode::WithUnmatched
                    }
                };
                RowsPerMatch::AllRows(converted_mode)
            }
        }
    }

    pub(super) fn convert_after_match_skip(
        after_match_skip: Option<SQLAfterMatchSkip>,
    ) -> AfterMatchSkip {
        match after_match_skip {
            None | Some(SQLAfterMatchSkip::PastLastRow) => AfterMatchSkip::PastLastRow,
            Some(SQLAfterMatchSkip::ToNextRow) => AfterMatchSkip::ToNextRow,
            Some(SQLAfterMatchSkip::ToFirst(symbol)) => {
                AfterMatchSkip::ToFirst(symbol.value)
            }
            Some(SQLAfterMatchSkip::ToLast(symbol)) => {
                AfterMatchSkip::ToLast(symbol.value)
            }
        }
    }

    pub(super) fn apply_rows_per_match_semantics(
        plan: LogicalPlan,
        rows_per_match: RowsPerMatch,
        table_schema: &DFSchemaRef,
        partition_by_exprs: &[Expr],
        measures_exprs: &[NamedExpr],
        ctx: Option<&ExclusionContext>,
    ) -> Result<LogicalPlan> {
        let plan_builder = LogicalPlanBuilder::from(plan);
        let mut projection_exprs = rows_projection_expr(
            &rows_per_match,
            table_schema,
            partition_by_exprs,
            measures_exprs,
        );
        if let Some(ctx) = ctx {
            projection_exprs = projection_exprs
                .into_iter()
                .map(|e| ctx.normalize_classifier_expr(e))
                .collect::<Result<Vec<_>>>()?;
        }
        let final_plan = plan_builder.project(projection_exprs)?.build()?;
        Ok(final_plan)
    }
}
