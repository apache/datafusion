// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Checks that the logical optimizer reaches a fixed point: within `MAX`
//! passes, a pass leaves the plan unchanged.
//!
//! The optimizer also stops when a pass produces a plan that an earlier pass
//! produced (a cycle), so comparing the results of `max_passes = N` and
//! `max_passes = N + 1` can not tell a fixed point from a cycle. The plan after
//! each pass is recorded instead, and the last two must be equal.

use std::fs;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use test_utils::tpcds::tpcds_schemas;

const MAX: usize = 10;

fn ctx() -> SessionContext {
    let mut config = SessionConfig::default();
    config.options_mut().optimizer.max_passes = MAX;
    SessionContext::new_with_config(config)
}

/// The plan before the first optimizer pass, followed by the plan after each
/// pass that the optimizer ran.
fn pass_outputs(state: &SessionState, plan: LogicalPlan) -> Result<Vec<String>> {
    let plan =
        state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})?;
    let optimizer = state.optimizer();
    let last_rule = optimizer.rules.last().map(Arc::as_ptr);
    let mut out = vec![plan.display_indent().to_string()];
    optimizer.optimize(plan, state, |plan, rule| {
        if last_rule.is_some_and(|last| std::ptr::addr_eq(last, rule)) {
            out.push(plan.display_indent().to_string());
        }
    })?;
    Ok(out)
}

/// The number of passes after which a pass left the plan unchanged, or `None`
/// when the last pass still changed it (the plans still change at `MAX`, or
/// the optimizer stopped at a cycle).
fn passes_to_fixed_point(outputs: &[String]) -> Option<usize> {
    let [.., before, last] = outputs else {
        return None;
    };
    (before == last).then(|| outputs.len() - 2)
}

async fn outputs_for(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
    let (state, plan) = ctx.sql(sql).await?.into_parts();
    pass_outputs(&state, plan)
}

async fn check(setup: &[&str], sql: &str) -> Result<()> {
    let ctx = ctx();
    for s in setup {
        ctx.sql(s).await?.collect().await?;
    }
    let outputs = outputs_for(&ctx, sql).await?;
    let n = passes_to_fixed_point(&outputs);
    assert!(
        n.is_some(),
        "did not converge:\n{}",
        outputs.last().unwrap()
    );
    Ok(())
}

const SHAPE1_SETUP: &[&str] = &["CREATE TABLE test_data (id INT, float_val DOUBLE, \
     int_val INT, decimal_val DECIMAL(10,2)) AS VALUES (1, 5.3, 100, 100.00)"];
const SHAPE1: &str = "SELECT * FROM test_data WHERE floor(int_val) = 100";

const SHAPE2_SETUP: &[&str] = &["CREATE TABLE t(a INT) AS VALUES (1),(2)"];
const SHAPE2: &str = "SELECT a FROM t WHERE CASE WHEN arrow_field(a) IS NOT NULL \
     THEN arrow_field(a)['nullable'] IS NULL END";

#[tokio::test]
async fn converges_floor_preimage() -> Result<()> {
    check(SHAPE1_SETUP, SHAPE1).await
}

#[tokio::test]
async fn converges_issue_25329() -> Result<()> {
    check(SHAPE2_SETUP, SHAPE2).await
}

/// Checks that every TPC-DS query reaches a fixed point within `MAX` passes.
#[tokio::test]
async fn converges_tpcds() -> Result<()> {
    let ctx = ctx();
    for table in &tpcds_schemas() {
        ctx.register_table(
            table.name.as_str(),
            Arc::new(
                MemTable::try_new(Arc::new(table.schema.clone()), vec![vec![]])?
                    .with_constraints(table.constraints.clone()),
            ),
        )?;
    }
    let mut not_converged = vec![];
    for q in 1..=99 {
        let sql = fs::read_to_string(format!("tests/tpc-ds/{q}.sql"))
            .expect("Could not read query");
        // some queries have multiple statements
        for (i, sql) in sql.split(';').filter(|s| !s.trim().is_empty()).enumerate() {
            let outputs = outputs_for(&ctx, sql).await?;
            if passes_to_fixed_point(&outputs).is_none() {
                not_converged.push(format!("q{q}.{i}"));
            }
        }
    }
    assert!(
        not_converged.is_empty(),
        "not converged within {MAX} passes: {not_converged:?}"
    );
    Ok(())
}
