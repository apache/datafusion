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

//! Tests for the cost-based join order enumeration in [`JoinSelection`].

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    ColumnStatistics, JoinSide, JoinType, NullEquality, Result, ScalarValue,
};
use datafusion_common::{Statistics, stats::Precision};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::join_enumeration::JoinEnumeration;
use datafusion_physical_optimizer::join_selection::JoinSelection;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::{ExecutionPlan, displayable};
use insta::assert_snapshot;

use crate::physical_optimizer::join_selection::StatisticsExec;

/// A table of `rows` rows with `(name, distinct_count)` columns. Each gets a
/// `[0, distinct_count)` range, as a real scan with min/max statistics would.
fn table(rows: usize, columns: &[(&str, usize)]) -> (Statistics, Schema) {
    let column_statistics = columns
        .iter()
        .map(|(_, distinct)| ColumnStatistics {
            distinct_count: Precision::Inexact(*distinct),
            min_value: Precision::Inexact(ScalarValue::Int32(Some(0))),
            max_value: Precision::Inexact(ScalarValue::Int32(Some(*distinct as i32 - 1))),
            ..Default::default()
        })
        .collect();
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int32, false))
            .collect::<Vec<_>>(),
    );
    (
        Statistics {
            num_rows: Precision::Inexact(rows),
            total_byte_size: Precision::Absent,
            column_statistics,
        },
        schema,
    )
}

fn scan(rows: usize, columns: &[(&str, usize)]) -> Arc<dyn ExecutionPlan> {
    let (statistics, schema) = table(rows, columns);
    Arc::new(StatisticsExec::new(statistics, schema))
}

fn scan_without_statistics(columns: &[&str]) -> Arc<dyn ExecutionPlan> {
    let schema = Schema::new(
        columns
            .iter()
            .map(|name| Field::new(*name, DataType::Int32, false))
            .collect::<Vec<_>>(),
    );
    let statistics = Statistics {
        num_rows: Precision::Absent,
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics::new_unknown(); columns.len()],
    };
    Arc::new(StatisticsExec::new(statistics, schema))
}

fn join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: &[(&str, &str)],
) -> Result<Arc<dyn ExecutionPlan>> {
    join_of_type(left, right, on, JoinType::Inner, None)
}

/// The same shape as [`late_reducer_plan`], joined by sort merge instead of hash.
fn sort_merge_late_reducer_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let fact = scan(1_000_000, &[("f_id", 1_000_000), ("f_type", 1_000)]);
    let other = scan(1_000_000, &[("o_id", 1_000_000)]);
    let types = scan(10, &[("t_type", 10)]);

    let joined = sort_merge_join(fact, other, &[("f_id", "o_id")])?;
    sort_merge_join(joined, types, &[("f_type", "t_type")])
}

fn sort_merge_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: &[(&str, &str)],
) -> Result<Arc<dyn ExecutionPlan>> {
    let keys = on
        .iter()
        .map(|(left_key, right_key)| {
            Ok((
                Arc::new(Column::new_with_schema(left_key, &left.schema())?) as _,
                Arc::new(Column::new_with_schema(right_key, &right.schema())?) as _,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(SortMergeJoinExec::try_new(
        left,
        right,
        keys,
        None,
        JoinType::Inner,
        vec![SortOptions::default(); on.len()],
        NullEquality::NullEqualsNothing,
    )?))
}

fn join_of_type(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: &[(&str, &str)],
    join_type: JoinType,
    filter: Option<JoinFilter>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let keys = on
        .iter()
        .map(|(left_key, right_key)| {
            Ok((
                Arc::new(Column::new_with_schema(left_key, &left.schema())?) as _,
                Arc::new(Column::new_with_schema(right_key, &right.schema())?) as _,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        keys,
        filter,
        &join_type,
        None,
        PartitionMode::Auto,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}

/// A `left_col > right_col` filter over one column of each side.
fn greater_than_filter(
    left_col: (&str, usize),
    right_col: (&str, usize),
) -> Result<JoinFilter> {
    let schema = Schema::new(vec![
        Field::new(left_col.0, DataType::Int32, false),
        Field::new(right_col.0, DataType::Int32, false),
    ]);
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(Column::new(left_col.0, 0)),
        Operator::Gt,
        Arc::new(Column::new(right_col.0, 1)),
    ));
    Ok(JoinFilter::new(
        expression,
        vec![
            ColumnIndex {
                index: left_col.1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: right_col.1,
                side: JoinSide::Right,
            },
        ],
        Arc::new(schema),
    ))
}

/// A three way join in its expensive `FROM` order: the two large tables first
/// produce a million rows, where either with the tiny table first gives ten
/// thousand.
fn late_reducer_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let fact = scan(1_000_000, &[("f_id", 1_000_000), ("f_type", 1_000)]);
    let other = scan(1_000_000, &[("o_id", 1_000_000)]);
    let types = scan(10, &[("t_type", 10)]);

    let fact_other = join(fact, other, &[("f_id", "o_id")])?;
    join(fact_other, types, &[("f_type", "t_type")])
}

/// Both rules in pipeline order: shape first, then build side and partition mode.
fn optimize(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = JoinEnumeration::new().optimize(plan, config)?;
    JoinSelection::new().optimize(plan, config)
}

fn formatted(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

#[test]
fn reorders_a_late_reducer() -> Result<()> {
    let plan = late_reducer_plan()?;
    // The planner's order: the two million row tables are joined first.
    assert_snapshot!(formatted(&plan), @r"
    HashJoinExec: mode=Auto, join_type=Inner, on=[(f_type@1, t_type@0)]
      HashJoinExec: mode=Auto, join_type=Inner, on=[(f_id@0, o_id@0)]
        StatisticsExec: col_count=2, row_count=Inexact(1000000)
        StatisticsExec: col_count=1, row_count=Inexact(1000000)
      StatisticsExec: col_count=1, row_count=Inexact(10)
    ");

    // The reducing join moves down so the large tables never join directly.
    assert_snapshot!(formatted(&optimize(plan, &ConfigOptions::new())?), @r"
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(f_id@0, o_id@0)], projection=[f_id@0, f_type@1, o_id@3, t_type@2]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(t_type@0, f_type@1)], projection=[f_id@1, f_type@2, t_type@0]
        StatisticsExec: col_count=1, row_count=Inexact(10)
        StatisticsExec: col_count=2, row_count=Inexact(1000000)
      StatisticsExec: col_count=1, row_count=Inexact(1000000)
    ");
    Ok(())
}

#[test]
fn respects_the_config_flag() -> Result<()> {
    let mut config = ConfigOptions::new();
    config.optimizer.join_enumeration = false;
    let optimized = optimize(late_reducer_plan()?, &config)?;
    // Without enumeration the large tables still join first, for a million rows.
    assert_snapshot!(formatted(&optimized), @r"
    ProjectionExec: expr=[f_id@1 as f_id, f_type@2 as f_type, o_id@3 as o_id, t_type@0 as t_type]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(t_type@0, f_type@1)]
        StatisticsExec: col_count=1, row_count=Inexact(10)
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(f_id@0, o_id@0)]
          StatisticsExec: col_count=2, row_count=Inexact(1000000)
          StatisticsExec: col_count=1, row_count=Inexact(1000000)
    ");
    Ok(())
}

#[test]
fn leaves_plans_without_statistics_alone() -> Result<()> {
    let fact = scan_without_statistics(&["f_id", "f_type"]);
    let other = scan_without_statistics(&["o_id"]);
    let types = scan_without_statistics(&["t_type"]);
    let plan = join(
        join(fact, other, &[("f_id", "o_id")])?,
        types,
        &[("f_type", "t_type")],
    )?;

    let optimized = optimize(Arc::clone(&plan), &ConfigOptions::new())?;
    assert_snapshot!(formatted(&optimized), @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(f_type@1, t_type@0)]
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(f_id@0, o_id@0)]
        StatisticsExec: col_count=2, row_count=Absent
        StatisticsExec: col_count=1, row_count=Absent
      StatisticsExec: col_count=1, row_count=Absent
    ");
    Ok(())
}

#[test]
fn keeps_an_already_optimal_order() -> Result<()> {
    // Already in the cheap order, so the enumerator must not churn the plan.
    let fact = scan(1_000_000, &[("f_id", 1_000_000), ("f_type", 1_000)]);
    let other = scan(1_000_000, &[("o_id", 1_000_000)]);
    let types = scan(10, &[("t_type", 10)]);

    let reduced = join(fact, types, &[("f_type", "t_type")])?;
    let plan = join(reduced, other, &[("f_id", "o_id")])?;

    let mut disabled = ConfigOptions::new();
    disabled.optimizer.join_enumeration = false;
    assert_eq!(
        formatted(&optimize(Arc::clone(&plan), &ConfigOptions::new())?),
        formatted(&optimize(plan, &disabled)?),
    );
    Ok(())
}

/// A session over four in-memory tables shaped like a small star schema.
fn star_schema_context(
    join_enumeration: bool,
    prefer_hash_join: bool,
) -> Result<SessionContext> {
    let mut config = SessionConfig::new();
    config.options_mut().optimizer.join_enumeration = join_enumeration;
    config.options_mut().optimizer.prefer_hash_join = prefer_hash_join;
    let ctx = SessionContext::new_with_config(config);

    let ints = |name: &str, values: Vec<i32>| -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )?)
    };

    // fact(f_id, f_type, f_region), 240 rows.
    let fact = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("f_id", DataType::Int32, false),
            Field::new("f_type", DataType::Int32, false),
            Field::new("f_region", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from((0..240).collect::<Vec<_>>())),
            Arc::new(Int32Array::from(
                (0..240).map(|i| i % 12).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                (0..240).map(|i| i % 5).collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("fact", fact)?;
    ctx.register_batch("ids", ints("o_id", (0..240).rev().collect())?)?;
    ctx.register_batch("types", ints("t_type", vec![3, 7])?)?;
    ctx.register_batch("regions", ints("r_region", vec![1, 2, 4])?)?;
    Ok(ctx)
}

/// A plain join tree, `EXISTS`, `NOT EXISTS`, and a non-equi predicate.
const STAR_QUERIES: [&str; 4] = [
    "select f_id, f_type, f_region from fact, ids, types, regions \
     where f_id = o_id and f_type = t_type and f_region = r_region order by f_id",
    "select f_id, f_type from fact where exists \
     (select 1 from types where t_type = f_type) \
     and f_id in (select o_id from ids) order by f_id",
    "select f_id, f_type from fact where not exists \
     (select 1 from types where t_type = f_type) \
     and f_id in (select o_id from ids) order by f_id",
    "select f_id, f_type, t_type from fact, ids, types \
     where f_id = o_id and f_type = t_type and f_id > t_type order by f_id",
];

#[tokio::test]
async fn reordering_returns_the_same_rows() -> Result<()> {
    for prefer_hash_join in [true, false] {
        reordering_returns_the_same_rows_with(prefer_hash_join).await?;
    }
    Ok(())
}

async fn reordering_returns_the_same_rows_with(prefer_hash_join: bool) -> Result<()> {
    let enumerated = star_schema_context(true, prefer_hash_join)?;
    let baseline = star_schema_context(false, prefer_hash_join)?;
    let mut reordered_any = false;
    for query in STAR_QUERIES {
        let enumerated_plan = enumerated.sql(query).await?.create_physical_plan().await?;
        let baseline_plan = baseline.sql(query).await?.create_physical_plan().await?;
        reordered_any |= formatted(&enumerated_plan) != formatted(&baseline_plan);

        let enumerated_rows = enumerated.sql(query).await?.collect().await?;
        let baseline_rows = baseline.sql(query).await?.collect().await?;
        assert_eq!(
            pretty_format_batches(&enumerated_rows)?.to_string(),
            pretty_format_batches(&baseline_rows)?.to_string(),
            "rows differ for: {query}"
        );
        assert!(enumerated_rows.iter().map(|b| b.num_rows()).sum::<usize>() > 0);
    }
    // Rows matching would prove nothing if no plan had changed.
    assert!(reordered_any);
    Ok(())
}

/// A selective semi join sitting above a join of two large tables, as TPC-H q18
/// has it.
fn late_semi_join_plan(anti: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let fact = scan(1_000_000, &[("f_id", 1_000_000), ("f_type", 1_000)]);
    let other = scan(1_000_000, &[("o_id", 1_000_000)]);
    // Sized so the reducer keeps one percent either way round: ten of the thousand
    // types match for the semi join, all but ten for the anti join. A reducer that
    // kept most of its input would not be worth moving.
    let wanted = if anti {
        scan(990, &[("w_type", 990)])
    } else {
        scan(10, &[("w_type", 10)])
    };

    let joined = join(fact, other, &[("f_id", "o_id")])?;
    let join_type = if anti {
        JoinType::LeftAnti
    } else {
        JoinType::LeftSemi
    };
    join_of_type(joined, wanted, &[("f_type", "w_type")], join_type, None)
}

#[test]
fn applies_a_selective_semi_join_first() -> Result<()> {
    let plan = late_semi_join_plan(false)?;
    assert_snapshot!(formatted(&plan), @r"
    HashJoinExec: mode=Auto, join_type=LeftSemi, on=[(f_type@1, w_type@0)]
      HashJoinExec: mode=Auto, join_type=Inner, on=[(f_id@0, o_id@0)]
        StatisticsExec: col_count=2, row_count=Inexact(1000000)
        StatisticsExec: col_count=1, row_count=Inexact(1000000)
      StatisticsExec: col_count=1, row_count=Inexact(10)
    ");

    // A `RightSemi` filtering the fact table before the inner join.
    assert_snapshot!(formatted(&optimize(plan, &ConfigOptions::new())?), @r"
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(f_id@0, o_id@0)]
      HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(w_type@0, f_type@1)]
        StatisticsExec: col_count=1, row_count=Inexact(10)
        StatisticsExec: col_count=2, row_count=Inexact(1000000)
      StatisticsExec: col_count=1, row_count=Inexact(1000000)
    ");
    Ok(())
}

#[test]
fn applies_an_anti_join_first() -> Result<()> {
    let optimized = optimize(late_semi_join_plan(true)?, &ConfigOptions::new())?;
    // The anti join is pushed down the same way.
    assert_snapshot!(formatted(&optimized), @r"
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(f_id@0, o_id@0)]
      HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(w_type@0, f_type@1)]
        StatisticsExec: col_count=1, row_count=Inexact(990)
        StatisticsExec: col_count=2, row_count=Inexact(1000000)
      StatisticsExec: col_count=1, row_count=Inexact(1000000)
    ");
    Ok(())
}

#[test]
fn moves_a_non_equi_filter_with_its_join() -> Result<()> {
    // `f_type > t_type` rides on the fact/types join, which moves below the join
    // with the second large table.
    let fact = scan(1_000_000, &[("f_id", 1_000_000), ("f_type", 1_000)]);
    let other = scan(1_000_000, &[("o_id", 1_000_000)]);
    let types = scan(10, &[("t_type", 10)]);

    let joined = join(fact, other, &[("f_id", "o_id")])?;
    let plan = join_of_type(
        joined,
        types,
        &[("f_type", "t_type")],
        JoinType::Inner,
        Some(greater_than_filter(("f_type", 1), ("t_type", 0))?),
    )?;

    // Re-attached to the join that now brings its two columns together.
    assert_snapshot!(formatted(&optimize(plan, &ConfigOptions::new())?), @r"
    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(f_id@0, o_id@0)], projection=[f_id@0, f_type@1, o_id@3, t_type@2]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(t_type@0, f_type@1)], filter=f_type@1 > t_type@0, projection=[f_id@1, f_type@2, t_type@0]
        StatisticsExec: col_count=1, row_count=Inexact(10)
        StatisticsExec: col_count=2, row_count=Inexact(1000000)
      StatisticsExec: col_count=1, row_count=Inexact(1000000)
    ");
    Ok(())
}

#[test]
fn reorders_sort_merge_joins() -> Result<()> {
    // A sort merge join carries no projection, so the columns the subtree used to
    // emit are restored by one projection above it.
    assert_snapshot!(
        formatted(&optimize(sort_merge_late_reducer_plan()?, &ConfigOptions::new())?),
        @r"
    ProjectionExec: expr=[f_id@1 as f_id, f_type@2 as f_type, o_id@3 as o_id, t_type@0 as t_type]
      SortMergeJoinExec: join_type=Inner, on=[(f_id@1, o_id@0)]
        SortMergeJoinExec: join_type=Inner, on=[(t_type@0, f_type@1)]
          StatisticsExec: col_count=1, row_count=Inexact(10)
          StatisticsExec: col_count=2, row_count=Inexact(1000000)
        StatisticsExec: col_count=1, row_count=Inexact(1000000)
    "
    );
    Ok(())
}
