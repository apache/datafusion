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

//! Tests for [`LimitedDistinctAggregation`] physical optimizer rule
//!
//! Note these tests are not in the same module as the optimizer pass because
//! they rely on `ParquetExec` which is in the core crate.

use arrow_schema::DataType;
use datafusion_common::Result;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{cast, col};
use datafusion_physical_optimizer::test_utils::{
    assert_plan_matches_expected, build_group_by, mock_data, TestAggregate,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode},
    expressions,
    limit::LocalLimitExec,
    ExecutionPlan,
};
use std::sync::Arc;

#[test]
fn test_no_group_by() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT <aggregate with no expressions> FROM MemoryExec LIMIT 10;`, Single AggregateExec
    let single_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema, vec![]),
        vec![], /* aggr_expr */
        vec![], /* filter_expr */
        source, /* input */
        schema, /* input_schema */
    )?;
    let limit_exec = LocalLimitExec::new(
        Arc::new(single_agg),
        10, // fetch
    );
    // expected not to push the limit to the AggregateExec
    let expected = [
        "LocalLimitExec: fetch=10",
        "AggregateExec: mode=Single, gby=[], aggr=[]",
        "MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    Ok(())
}

#[test]
fn test_has_aggregate_expression() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_star();

    // `SELECT <aggregate with no expressions> FROM MemoryExec LIMIT 10;`, Single AggregateExec
    let single_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema, vec!["a".to_string()]),
        vec![Arc::new(agg.count_expr(&schema))], /* aggr_expr */
        vec![None],                              /* filter_expr */
        source,                                  /* input */
        schema.clone(),                          /* input_schema */
    )?;
    let limit_exec = LocalLimitExec::new(
        Arc::new(single_agg),
        10, // fetch
    );
    // expected not to push the limit to the AggregateExec
    let expected = [
        "LocalLimitExec: fetch=10",
        "AggregateExec: mode=Single, gby=[a@0 as a], aggr=[COUNT(*)]",
        "MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    Ok(())
}

#[test]
fn test_has_filter() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT a FROM MemoryExec WHERE a > 1 GROUP BY a LIMIT 10;`, Single AggregateExec
    // the `a > 1` filter is applied in the AggregateExec
    let filter_expr = Some(expressions::binary(
        col("a", &schema)?,
        Operator::Gt,
        cast(expressions::lit(1u32), &schema, DataType::Int32)?,
        &schema,
    )?);
    let agg = TestAggregate::new_count_star();
    let single_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema.clone(), vec!["a".to_string()]),
        vec![Arc::new(agg.count_expr(&schema))], /* aggr_expr */
        vec![filter_expr],                       /* filter_expr */
        source,                                  /* input */
        schema.clone(),                          /* input_schema */
    )?;
    let limit_exec = LocalLimitExec::new(
        Arc::new(single_agg),
        10, // fetch
    );
    // expected not to push the limit to the AggregateExec
    // TODO(msirek): open an issue for `filter_expr` of `AggregateExec` not printing out
    let expected = [
        "LocalLimitExec: fetch=10",
        "AggregateExec: mode=Single, gby=[a@0 as a], aggr=[COUNT(*)]",
        "MemoryExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    Ok(())
}
