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

//! Built-in [`RelationPlanner`] for `TABLESAMPLE SYSTEM(p%)`.
//!
//! Auto-registered via [`SessionStateDefaults::default_relation_planners`]
//! so SQL `TABLESAMPLE SYSTEM (10) [REPEATABLE (n)]` works out of the
//! box on any default `SessionContext`. Other `TABLESAMPLE` flavours
//! (`BERNOULLI`, `ROW`, `BUCKET ... OUT OF ...`, `OFFSET`) are rejected
//! at planning time — implementing those is left to a downstream
//! `RelationPlanner` (see `datafusion-examples/examples/relation_planner/`).
//!
//! `SessionStateBuilder::register_relation_planner` inserts new planners
//! at the front of the chain, so a downstream planner that returns
//! `Planned` for the same `TABLESAMPLE` syntax wins. Returning
//! `Original` falls through to this default.
//!
//! [`SessionStateDefaults::default_relation_planners`]: ../../datafusion/execution/session_state/struct.SessionStateDefaults.html

use std::sync::Arc;

use datafusion_common::{Result, not_impl_err, plan_datafusion_err, plan_err};
use datafusion_expr::logical_plan::sample::{SampleMethod, sample_plan};
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use sqlparser::ast::{
    self, TableFactor, TableSampleKind, TableSampleMethod, TableSampleUnit,
};

/// Built-in `RelationPlanner` that lifts `TABLESAMPLE SYSTEM(p%)`
/// (with optional `REPEATABLE(seed)`) into the core
/// [`Sample`](datafusion_expr::logical_plan::sample::Sample) extension
/// node so the `SamplePushdown` optimizer rule can absorb the sample
/// into the scan.
///
/// Rejects every other form of `TABLESAMPLE` with a `not_impl_err`. To
/// support `BERNOULLI`, row counts, or `BUCKET`, register your own
/// `RelationPlanner` ahead of this one — `register_relation_planner`
/// pushes to the front and the first `Planned` wins.
#[derive(Debug, Default)]
pub struct TableSampleSystemPlanner;

impl RelationPlanner for TableSampleSystemPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        // Only act on Table relations carrying a `TABLESAMPLE` clause.
        // Everything else (derived, function, unnest, join) falls
        // through to the next planner / DataFusion's default logic.
        let TableFactor::Table {
            sample: Some(sample),
            alias,
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            index_hints,
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let ts = match sample {
            TableSampleKind::BeforeTableAlias(s)
            | TableSampleKind::AfterTableAlias(s) => *s,
        };

        if ts.bucket.is_some() {
            return not_impl_err!(
                "TABLESAMPLE BUCKET is not supported (only SYSTEM PERCENT). \
                 Register a custom RelationPlanner before the built-in \
                 TableSampleSystemPlanner to handle other forms."
            );
        }
        if ts.offset.is_some() {
            return not_impl_err!(
                "TABLESAMPLE OFFSET is not supported (only SYSTEM PERCENT)"
            );
        }
        match ts.name {
            // The built-in planner only handles SYSTEM (and BLOCK as an
            // alias for SYSTEM, matching Hive). An unspecified method
            // is rejected rather than silently picking SYSTEM, since
            // the right default differs by engine (PostgreSQL requires
            // an explicit method; Spark defaults to block-level).
            // Anything else is a semantics commitment we don't want to
            // make in core.
            Some(TableSampleMethod::System) | Some(TableSampleMethod::Block) => {}
            None => {
                return not_impl_err!(
                    "TABLESAMPLE without an explicit method is not supported; \
                     write TABLESAMPLE SYSTEM (...) (or register a custom \
                     RelationPlanner before the built-in TableSampleSystemPlanner \
                     to define a default)."
                );
            }
            Some(other) => {
                return not_impl_err!(
                    "TABLESAMPLE method {other} is not supported (only SYSTEM). \
                     Register a custom RelationPlanner before the built-in \
                     TableSampleSystemPlanner to handle other methods."
                );
            }
        }

        let quantity = ts.quantity.ok_or_else(|| {
            plan_datafusion_err!("TABLESAMPLE without a quantity is not supported")
        })?;
        let raw = match &quantity.value {
            ast::Expr::Value(vs) => match &vs.value {
                ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| {
                    plan_datafusion_err!("invalid TABLESAMPLE quantity: {n}")
                })?,
                v => return plan_err!("TABLESAMPLE quantity must be numeric; got {v:?}"),
            },
            other => {
                return plan_err!("TABLESAMPLE quantity must be a literal; got {other}");
            }
        };
        let fraction = match quantity.unit {
            Some(TableSampleUnit::Percent) | None => raw / 100.0,
            Some(TableSampleUnit::Rows) => {
                return not_impl_err!(
                    "TABLESAMPLE with ROWS count is not supported (only SYSTEM PERCENT)"
                );
            }
        };

        let seed = ts
            .seed
            .map(|s| match s.value {
                ast::Value::Number(n, _) => n
                    .parse::<u64>()
                    .map_err(|_| plan_datafusion_err!("invalid REPEATABLE seed: {n}")),
                v => Err(plan_datafusion_err!(
                    "REPEATABLE seed must be an integer; got {v:?}"
                )),
            })
            .transpose()?;

        // Replan the bare table without the sample clause, then wrap
        // the resulting plan in a `Sample` extension node.
        let bare = TableFactor::Table {
            sample: None,
            alias: alias.clone(),
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            index_hints,
        };
        let input = context.plan(bare)?;
        let plan = sample_plan(Arc::new(input), SampleMethod::System, fraction, seed)?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}
