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

//! Integration tests for [`LimitedDistinctAggregation`] physical optimizer rule

use std::sync::Arc;

use crate::physical_optimizer::test_utils::{
    assert_plan_matches_expected, build_group_by, mock_data, parquet_exec_with_sort,
    schema, TestAggregate,
};

use arrow::{compute::SortOptions, util::pretty::pretty_format_batches};
use arrow_schema::DataType;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::cast;
use datafusion_physical_expr::{expressions, expressions::col, PhysicalSortExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode},
    collect,
    limit::{GlobalLimitExec, LocalLimitExec},
    ExecutionPlan,
};

async fn assert_results_match_expected(
    plan: Arc<dyn ExecutionPlan>,
    expected: &str,
) -> Result<()> {
    let cfg = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(cfg);
    let batches = collect(plan, ctx.task_ctx()).await?;
    let actual = format!("{}", pretty_format_batches(&batches)?);
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn test_partial_final() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT a FROM DataSourceExec GROUP BY a LIMIT 4;`, Partial/Final AggregateExec
    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        build_group_by(&schema.clone(), vec!["a".to_string()]),
        vec![],         /* aggr_expr */
        vec![],         /* filter_expr */
        source,         /* input */
        schema.clone(), /* input_schema */
    )?;
    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        build_group_by(&schema.clone(), vec!["a".to_string()]),
        vec![],                /* aggr_expr */
        vec![],                /* filter_expr */
        Arc::new(partial_agg), /* input */
        schema.clone(),        /* input_schema */
    )?;
    let limit_exec = LocalLimitExec::new(
        Arc::new(final_agg),
        4, // fetch
    );
    // expected to push the limit to the Partial and Final AggregateExecs
    let expected = [
        "LocalLimitExec: fetch=4",
        "AggregateExec: mode=Final, gby=[a@0 as a], aggr=[], lim=[4]",
        "AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[], lim=[4]",
        "DataSourceExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    let expected = r#"
+---+
| a |
+---+
| 1 |
| 2 |
|   |
| 4 |
+---+
"#
    .trim();
    assert_results_match_expected(plan, expected).await?;
    Ok(())
}

#[tokio::test]
async fn test_single_local() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT a FROM DataSourceExec GROUP BY a LIMIT 4;`, Single AggregateExec
    let single_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema.clone(), vec!["a".to_string()]),
        vec![],         /* aggr_expr */
        vec![],         /* filter_expr */
        source,         /* input */
        schema.clone(), /* input_schema */
    )?;
    let limit_exec = LocalLimitExec::new(
        Arc::new(single_agg),
        4, // fetch
    );
    // expected to push the limit to the AggregateExec
    let expected = [
        "LocalLimitExec: fetch=4",
        "AggregateExec: mode=Single, gby=[a@0 as a], aggr=[], lim=[4]",
        "DataSourceExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    let expected = r#"
+---+
| a |
+---+
| 1 |
| 2 |
|   |
| 4 |
+---+
"#
    .trim();
    assert_results_match_expected(plan, expected).await?;
    Ok(())
}

#[tokio::test]
async fn test_single_global() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT a FROM DataSourceExec GROUP BY a LIMIT 4;`, Single AggregateExec
    let single_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema.clone(), vec!["a".to_string()]),
        vec![],         /* aggr_expr */
        vec![],         /* filter_expr */
        source,         /* input */
        schema.clone(), /* input_schema */
    )?;
    let limit_exec = GlobalLimitExec::new(
        Arc::new(single_agg),
        1,       // skip
        Some(3), // fetch
    );
    // expected to push the skip+fetch limit to the AggregateExec
    let expected = [
        "GlobalLimitExec: skip=1, fetch=3",
        "AggregateExec: mode=Single, gby=[a@0 as a], aggr=[], lim=[4]",
        "DataSourceExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    let expected = r#"
+---+
| a |
+---+
| 2 |
|   |
| 4 |
+---+
"#
    .trim();
    assert_results_match_expected(plan, expected).await?;
    Ok(())
}

#[tokio::test]
async fn test_distinct_cols_different_than_group_by_cols() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT distinct a FROM DataSourceExec GROUP BY a, b LIMIT 4;`, Single/Single AggregateExec
    let group_by_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema.clone(), vec!["a".to_string(), "b".to_string()]),
        vec![],         /* aggr_expr */
        vec![],         /* filter_expr */
        source,         /* input */
        schema.clone(), /* input_schema */
    )?;
    let distinct_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema.clone(), vec!["a".to_string()]),
        vec![],                 /* aggr_expr */
        vec![],                 /* filter_expr */
        Arc::new(group_by_agg), /* input */
        schema.clone(),         /* input_schema */
    )?;
    let limit_exec = LocalLimitExec::new(
        Arc::new(distinct_agg),
        4, // fetch
    );
    // expected to push the limit to the outer AggregateExec only
    let expected = [
        "LocalLimitExec: fetch=4",
        "AggregateExec: mode=Single, gby=[a@0 as a], aggr=[], lim=[4]",
        "AggregateExec: mode=Single, gby=[a@0 as a, b@1 as b], aggr=[]",
        "DataSourceExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    let expected = r#"
+---+
| a |
+---+
| 1 |
| 2 |
|   |
| 4 |
+---+
"#
    .trim();
    assert_results_match_expected(plan, expected).await?;
    Ok(())
}

#[test]
fn test_has_order_by() -> Result<()> {
    let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
        expr: col("a", &schema()).unwrap(),
        options: SortOptions::default(),
    }]);
    let source = parquet_exec_with_sort(vec![sort_key]);
    let schema = source.schema();

    // `SELECT a FROM DataSourceExec WHERE a > 1 GROUP BY a LIMIT 10;`, Single AggregateExec
    // the `a > 1` filter is applied in the AggregateExec
    let single_agg = AggregateExec::try_new(
        AggregateMode::Single,
        build_group_by(&schema, vec!["a".to_string()]),
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
        "AggregateExec: mode=Single, gby=[a@0 as a], aggr=[], ordering_mode=Sorted",
        "DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    Ok(())
}

#[test]
fn test_no_group_by() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT <aggregate with no expressions> FROM DataSourceExec LIMIT 10;`, Single AggregateExec
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
        "DataSourceExec: partitions=1, partition_sizes=[1]",
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

    // `SELECT <aggregate with no expressions> FROM DataSourceExec LIMIT 10;`, Single AggregateExec
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
        "DataSourceExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    Ok(())
}

#[test]
fn test_has_filter() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();

    // `SELECT a FROM DataSourceExec WHERE a > 1 GROUP BY a LIMIT 10;`, Single AggregateExec
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
        "DataSourceExec: partitions=1, partition_sizes=[1]",
    ];
    let plan: Arc<dyn ExecutionPlan> = Arc::new(limit_exec);
    assert_plan_matches_expected(&plan, &expected)?;
    Ok(())
}
